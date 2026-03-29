#!/usr/bin/env python3
"""
test_smp_bench.py - SMP timing benchmark table.

Generates 256MB, 512MB, 1GB files, benchmarks compress/decompress
for SMP1/2/4/8/16 (wimage) and wimlib, prints comparison table.
"""

import hashlib
import os
import shutil
import subprocess
import sys
import time

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"
TMPDIR = "/tmp/wimage_bench"

SIZES = [
    ("32MB",  32 * 1024 * 1024),
    ("64MB",  64 * 1024 * 1024),
    ("128MB", 128 * 1024 * 1024),
]

SMP = [1, 4, 8, 16]


def run(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"FAILED: {' '.join(cmd)}\n{err}")


def sha1_file(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def generate_file(path, size):
    written = 0
    block = 1 << 20
    with open(path, "wb") as f:
        i = 0
        while written < size:
            chunk_sz = min(block, size - written)
            if i % 4 == 0:
                pat = (f"Block-{i:08d}-ABCDEFGHIJKLMNOP\n" * 100).encode()
                data = (pat * ((chunk_sz // len(pat)) + 1))[:chunk_sz]
            else:
                parts = []
                seed = hashlib.sha256(f"seed-{i}-{written}".encode()).digest()
                remaining = chunk_sz
                while remaining > 0:
                    seed = hashlib.sha256(seed).digest()
                    parts.append(seed[:min(32, remaining)])
                    remaining -= min(32, remaining)
                data = b"".join(parts)
            f.write(data)
            written += chunk_sz
            i += 1


def bench_compress_wimage(src_dir, wim_path, threads):
    t0 = time.monotonic()
    run([WIMAGE, "capture", src_dir, wim_path, "Bench",
         "--compress=xpress", f"--threads={threads}"])
    return time.monotonic() - t0


def bench_compress_wimlib(src_dir, wim_path):
    t0 = time.monotonic()
    run([WIMLIB, "capture", src_dir, wim_path, "Bench", "--compress=XPRESS"])
    return time.monotonic() - t0


def bench_decompress_wimage(wim_path, out_dir):
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    t0 = time.monotonic()
    run([WIMAGE, "apply", wim_path, "1", out_dir])
    return time.monotonic() - t0


def bench_decompress_wimlib(wim_path, out_dir):
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    t0 = time.monotonic()
    run([WIMLIB, "apply", wim_path, "1", out_dir])
    return time.monotonic() - t0


def main():
    os.makedirs(TMPDIR, exist_ok=True)

    for size_name, size_bytes in SIZES:
        src_file = os.path.join(TMPDIR, f"src_{size_name}.bin")
        src_dir = os.path.join(TMPDIR, f"srcdir_{size_name}")
        fname = f"data_{size_name}.bin"
        src_in_dir = os.path.join(src_dir, fname)

        # Generate
        if not os.path.isfile(src_file) or os.path.getsize(src_file) != size_bytes:
            print(f"Generating {size_name}...", end=" ", flush=True)
            t0 = time.monotonic()
            generate_file(src_file, size_bytes)
            print(f"{time.monotonic() - t0:.1f}s")
        os.makedirs(src_dir, exist_ok=True)
        if not os.path.isfile(src_in_dir):
            os.link(src_file, src_in_dir)

        # Source SHA-1
        src_sha1 = sha1_file(src_file)

        # ----------------------------------------------------------
        # Compress benchmarks
        # ----------------------------------------------------------
        comp_times = {}  # key -> seconds
        comp_sizes = {}

        # wimage at each SMP level
        for t in SMP:
            wim = os.path.join(TMPDIR, f"rs_{size_name}_smp{t}.wim")
            comp_times[f"wimage-smp{t}"] = bench_compress_wimage(src_dir, wim, t)
            comp_sizes[f"wimage-smp{t}"] = os.path.getsize(wim)

        # wimlib
        wim_wl = os.path.join(TMPDIR, f"wl_{size_name}.wim")
        comp_times["wimlib"] = bench_compress_wimlib(src_dir, wim_wl)
        comp_sizes["wimlib"] = os.path.getsize(wim_wl)

        # ----------------------------------------------------------
        # Decompress benchmarks: each tool decompresses its own WIM
        # AND cross-decompresses the other tool's WIM
        # ----------------------------------------------------------
        decomp_times = {}

        # wimage decompresses its own SMP1 WIM
        wim_rs = os.path.join(TMPDIR, f"rs_{size_name}_smp1.wim")
        out = os.path.join(TMPDIR, "dec_tmp")
        decomp_times["wimage->own"] = bench_decompress_wimage(wim_rs, out)
        # Verify
        out_sha1 = sha1_file(os.path.join(out, fname))
        assert src_sha1 == out_sha1, f"wimage decomp SHA-1 mismatch for {size_name}"
        shutil.rmtree(out, ignore_errors=True)

        # wimlib decompresses wimage's SMP1 WIM
        decomp_times["wimlib->wimage"] = bench_decompress_wimlib(wim_rs, out)
        out_sha1 = sha1_file(os.path.join(out, fname))
        assert src_sha1 == out_sha1, f"wimlib decomp of wimage SHA-1 mismatch for {size_name}"
        shutil.rmtree(out, ignore_errors=True)

        # wimage decompresses wimlib's WIM
        decomp_times["wimage->wimlib"] = bench_decompress_wimage(wim_wl, out)
        out_sha1 = sha1_file(os.path.join(out, fname))
        assert src_sha1 == out_sha1, f"wimage decomp of wimlib SHA-1 mismatch for {size_name}"
        shutil.rmtree(out, ignore_errors=True)

        # wimlib decompresses its own WIM
        decomp_times["wimlib->own"] = bench_decompress_wimlib(wim_wl, out)
        out_sha1 = sha1_file(os.path.join(out, fname))
        assert src_sha1 == out_sha1, f"wimlib decomp SHA-1 mismatch for {size_name}"
        shutil.rmtree(out, ignore_errors=True)

        # Cleanup WIMs
        for f in os.listdir(TMPDIR):
            if f.endswith(".wim"):
                os.unlink(os.path.join(TMPDIR, f))

        # ----------------------------------------------------------
        # Print tables
        # ----------------------------------------------------------
        mb = size_bytes / (1 << 20)

        print(f"\n{'='*78}")
        print(f"  {size_name} ({size_bytes:,} bytes)   SHA-1: {src_sha1}")
        print(f"{'='*78}")

        # Compression table
        print(f"\n  COMPRESSION (XPRESS Huffman)")
        print(f"  {'Tool':<20} {'Time':>8} {'MB/s':>8} {'WIM Size':>10} {'Ratio':>8}")
        print(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*10} {'-'*8}")
        for key in [f"wimage-smp{t}" for t in SMP] + ["wimlib"]:
            t = comp_times[key]
            sz = comp_sizes[key]
            mbs = mb / t if t > 0 else 0
            ratio = sz / size_bytes * 100
            sz_str = f"{sz / (1 << 20):.0f}MB"
            print(f"  {key:<20} {t:>7.2f}s {mbs:>7.0f} {sz_str:>10} {ratio:>7.1f}%")

        # Speedup vs SMP1
        base = comp_times.get("wimage-smp1", 1)
        print(f"\n  SMP Speedup vs SMP1:")
        print(f"  {'Threads':<10}", end="")
        for t in SMP:
            speedup = base / comp_times.get(f"wimage-smp{t}", base)
            print(f" {f'SMP{t}':>8}", end="")
        print()
        print(f"  {'Speedup':<10}", end="")
        for t in SMP:
            speedup = base / comp_times.get(f"wimage-smp{t}", base)
            print(f" {speedup:>7.2f}x", end="")
        print()

        # Decompression table
        print(f"\n  DECOMPRESSION")
        print(f"  {'Direction':<25} {'Time':>8} {'MB/s':>8}")
        print(f"  {'-'*25} {'-'*8} {'-'*8}")
        for key in ["wimage->own", "wimlib->wimage", "wimage->wimlib", "wimlib->own"]:
            t = decomp_times[key]
            mbs = mb / t if t > 0 else 0
            print(f"  {key:<25} {t:>7.2f}s {mbs:>7.0f}")

        print(f"\n  All SHA-1 checksums verified: {src_sha1}")

    # Cleanup
    shutil.rmtree(TMPDIR, ignore_errors=True)
    print(f"\n{'='*78}")
    print(f"  BENCHMARK COMPLETE - All checksums verified")
    print(f"{'='*78}")


if __name__ == "__main__":
    main()
