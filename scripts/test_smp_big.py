#!/usr/bin/env python3
"""
test_smp_big.py - Large file SMP stress test with SHA-1 integrity.

Generates 256MB, 512MB, 1GB random files, then:
  Direction A: wimage compress (threads=1,8,16) -> wimlib decompress -> SHA-1 check
  Direction B: wimlib compress -> wimage decompress -> SHA-1 check

Every test verifies SHA-1 of the original file matches SHA-1 of the extracted file.
"""

import hashlib
import os
import shutil
import subprocess
import sys
import time

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"
TMPDIR = "/tmp/wimage_smp_big"

SIZES = [
    ("256MB", 256 * 1024 * 1024),
    ("512MB", 512 * 1024 * 1024),
    ("1GB",  1024 * 1024 * 1024),
]

THREAD_COUNTS = [1, 8, 16]


def run(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"FAILED ({r.returncode}): {' '.join(cmd)}\n{err}")


def sha1_file(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)  # 1MB
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fmt(n):
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f}GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f}MB"
    return f"{n / 1024:.0f}KB"


def generate_file(path, size):
    """Generate a file with pseudo-random but reproducible content."""
    written = 0
    # Mix of random-ish and patterned blocks so compression has something to work with
    block = 1 << 20  # 1MB blocks
    with open(path, "wb") as f:
        i = 0
        while written < size:
            chunk_sz = min(block, size - written)
            # Alternate: 75% pseudo-random via hashlib, 25% patterned
            if i % 4 == 0:
                # Patterned block (compressible)
                pat = (f"Block-{i:08d}-ABCDEFGHIJKLMNOP\n" * 100).encode()
                data = (pat * ((chunk_sz // len(pat)) + 1))[:chunk_sz]
            else:
                # Pseudo-random via SHA-256 chain (deterministic)
                parts = []
                seed = hashlib.sha256(f"seed-{i}-{written}".encode()).digest()
                remaining = chunk_sz
                while remaining > 0:
                    seed = hashlib.sha256(seed).digest()
                    take = min(32, remaining)
                    parts.append(seed[:take])
                    remaining -= take
                data = b"".join(parts)
            f.write(data)
            written += chunk_sz
            i += 1


def main():
    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found", file=sys.stderr)
        return 1

    os.makedirs(TMPDIR, exist_ok=True)
    total_pass = 0
    total_fail = 0

    for size_name, size_bytes in SIZES:
        print(f"\n{'='*70}")
        print(f"  FILE SIZE: {size_name} ({size_bytes:,} bytes)")
        print(f"{'='*70}")

        src_file = os.path.join(TMPDIR, f"source_{size_name}.bin")

        # Generate source file
        if not os.path.isfile(src_file) or os.path.getsize(src_file) != size_bytes:
            print(f"\nGenerating {size_name} test file...", end=" ", flush=True)
            t0 = time.monotonic()
            generate_file(src_file, size_bytes)
            print(f"{time.monotonic() - t0:.1f}s")

        # Compute source SHA-1
        print(f"Computing source SHA-1...", end=" ", flush=True)
        t0 = time.monotonic()
        src_sha1 = sha1_file(src_file)
        t_hash = time.monotonic() - t0
        print(f"{src_sha1}  ({t_hash:.2f}s)")

        # Create source directory with the single file
        src_dir = os.path.join(TMPDIR, f"srcdir_{size_name}")
        os.makedirs(src_dir, exist_ok=True)
        src_in_dir = os.path.join(src_dir, f"data_{size_name}.bin")
        if not os.path.isfile(src_in_dir):
            os.link(src_file, src_in_dir)  # hard link to avoid copy

        # ============================================================
        # Direction A: wimage compress -> wimlib decompress
        # ============================================================
        print(f"\n--- Direction A: wimage compress -> wimlib decompress ---")

        for threads in THREAD_COUNTS:
            label = f"SMP{threads}"
            wim_path = os.path.join(TMPDIR, f"a_{size_name}_{label}.wim")
            out_dir = os.path.join(TMPDIR, f"a_{size_name}_{label}_out")

            # Compress
            t0 = time.monotonic()
            run([WIMAGE, "capture", src_dir, wim_path, f"Test-{label}",
                 "--compress=xpress", f"--threads={threads}"])
            t_comp = time.monotonic() - t0

            wim_sz = os.path.getsize(wim_path)
            ratio = wim_sz / size_bytes * 100
            throughput = size_bytes / t_comp / (1 << 20)
            print(f"  {label}: compress {t_comp:.2f}s ({throughput:.0f} MB/s)  "
                  f"WIM={fmt(wim_sz)} ({ratio:.1f}%)", end="")

            # Decompress with wimlib
            if os.path.isdir(out_dir):
                shutil.rmtree(out_dir)
            os.makedirs(out_dir)
            t0 = time.monotonic()
            run([WIMLIB, "apply", wim_path, "1", out_dir])
            t_decomp = time.monotonic() - t0

            # SHA-1 verify
            out_file = os.path.join(out_dir, f"data_{size_name}.bin")
            if not os.path.isfile(out_file):
                print(f"  \033[31mFAIL\033[0m: extracted file missing")
                total_fail += 1
                continue

            t0 = time.monotonic()
            out_sha1 = sha1_file(out_file)
            t_verify = time.monotonic() - t0

            if src_sha1 == out_sha1:
                print(f"  decompress {t_decomp:.2f}s  "
                      f"SHA-1 \033[32mMATCH\033[0m ({t_verify:.2f}s)")
                total_pass += 1
            else:
                print(f"  \033[31mFAIL\033[0m: SHA-1 mismatch!")
                print(f"    src: {src_sha1}")
                print(f"    out: {out_sha1}")
                total_fail += 1

            # Cleanup extracted to save disk
            shutil.rmtree(out_dir, ignore_errors=True)

        # ============================================================
        # Direction B: wimlib compress -> wimage decompress
        # ============================================================
        print(f"\n--- Direction B: wimlib compress -> wimage decompress ---")

        wim_path = os.path.join(TMPDIR, f"b_{size_name}.wim")

        # wimlib compress
        t0 = time.monotonic()
        run([WIMLIB, "capture", src_dir, wim_path, "WimlibTest", "--compress=XPRESS"])
        t_comp = time.monotonic() - t0
        wim_sz = os.path.getsize(wim_path)
        ratio = wim_sz / size_bytes * 100
        throughput = size_bytes / t_comp / (1 << 20)
        print(f"  wimlib compress: {t_comp:.2f}s ({throughput:.0f} MB/s)  "
              f"WIM={fmt(wim_sz)} ({ratio:.1f}%)")

        # wimage decompress
        out_dir = os.path.join(TMPDIR, f"b_{size_name}_out")
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)
        t0 = time.monotonic()
        run([WIMAGE, "apply", wim_path, "1", out_dir])
        t_decomp = time.monotonic() - t0

        out_file = os.path.join(out_dir, f"data_{size_name}.bin")
        if not os.path.isfile(out_file):
            print(f"  \033[31mFAIL\033[0m: extracted file missing")
            total_fail += 1
        else:
            t0 = time.monotonic()
            out_sha1 = sha1_file(out_file)
            t_verify = time.monotonic() - t0

            if src_sha1 == out_sha1:
                print(f"  wimage decompress: {t_decomp:.2f}s  "
                      f"SHA-1 \033[32mMATCH\033[0m ({t_verify:.2f}s)")
                total_pass += 1
            else:
                print(f"  \033[31mFAIL\033[0m: SHA-1 mismatch!")
                print(f"    src: {src_sha1}")
                print(f"    out: {out_sha1}")
                total_fail += 1

        shutil.rmtree(out_dir, ignore_errors=True)

        # Cleanup WIM files for this size to save disk
        for f in os.listdir(TMPDIR):
            if f.endswith(".wim") and size_name in f:
                os.unlink(os.path.join(TMPDIR, f))

    # Summary
    print(f"\n{'='*70}")
    total = total_pass + total_fail
    if total_fail == 0:
        print(f"\033[32m  ALL {total} TESTS PASSED\033[0m  "
              f"(3 sizes x 3 SMP levels + 3 inverse = {total} checks)")
    else:
        print(f"\033[31m  {total_fail}/{total} FAILED\033[0m")
    print(f"{'='*70}")

    # Cleanup source files
    shutil.rmtree(TMPDIR, ignore_errors=True)

    return total_fail


if __name__ == "__main__":
    sys.exit(main())
