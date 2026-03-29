#!/usr/bin/env python3
"""
test_random250.py - 250 random file stress test.

Generates 250 files with random sizes (0 to 256KB) and random content,
then verifies perfect round-trip in both directions:

  Direction A: wimage compress -> wimlib-imagex decompress -> SHA-256 check
  Direction B: wimlib-imagex compress -> wimage decompress -> SHA-256 check

Every single byte of every file is verified via SHA-256 checksums.
"""

import hashlib
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"


def run(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Command failed ({r.returncode}): {' '.join(cmd)}\n{err}")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_tree(root):
    result = {}
    for dp, _, fns in os.walk(root):
        for fn in fns:
            full = os.path.join(dp, fn)
            if os.path.isfile(full):
                result[os.path.relpath(full, root)] = sha256(full)
    return result


def fmt(n):
    if n >= 1_048_576:
        return f"{n/1_048_576:.1f}MB"
    if n >= 1024:
        return f"{n/1024:.1f}KB"
    return f"{n}B"


def main():
    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found. Run 'make' first.", file=sys.stderr)
        return 1

    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    rng = random.Random(seed)
    tmpdir = tempfile.mkdtemp(prefix="wimage_r250_")
    src = os.path.join(tmpdir, "source")
    failed = 0

    try:
        # ============================================================
        # Generate 250 random files
        # ============================================================
        print(f"Generating 250 random files (seed={seed})...")
        os.makedirs(src)

        total_bytes = 0
        sizes = []
        for i in range(250):
            # Mix of sizes: ~30% tiny (<1KB), ~40% medium (1-32KB), ~20% large (32-128KB), ~10% huge (128-256KB)
            r = rng.random()
            if r < 0.05:
                sz = 0                              # empty
            elif r < 0.30:
                sz = rng.randint(1, 1023)           # tiny
            elif r < 0.70:
                sz = rng.randint(1024, 32767)       # medium
            elif r < 0.90:
                sz = rng.randint(32768, 131071)     # large (multi-chunk)
            else:
                sz = rng.randint(131072, 262144)    # huge

            # Some files go into subdirectories
            if i < 50:
                relpath = f"file_{i:03d}.bin"
            elif i < 100:
                os.makedirs(os.path.join(src, "subA"), exist_ok=True)
                relpath = f"subA/file_{i:03d}.bin"
            elif i < 150:
                os.makedirs(os.path.join(src, "subB", "deep"), exist_ok=True)
                relpath = f"subB/deep/file_{i:03d}.bin"
            elif i < 200:
                os.makedirs(os.path.join(src, "subC"), exist_ok=True)
                relpath = f"subC/file_{i:03d}.bin"
            else:
                os.makedirs(os.path.join(src, "subD", "nested", "dirs"), exist_ok=True)
                relpath = f"subD/nested/dirs/file_{i:03d}.bin"

            data = rng.randbytes(sz)
            with open(os.path.join(src, relpath), "wb") as f:
                f.write(data)
            total_bytes += sz
            sizes.append(sz)

        # Also add some empty directories
        for d in ["emptyA", "emptyB", "subA/emptyC"]:
            os.makedirs(os.path.join(src, d), exist_ok=True)

        sizes.sort()
        print(f"  250 files: {fmt(total_bytes)} total")
        print(f"  sizes: min={fmt(sizes[0])} median={fmt(sizes[125])} max={fmt(sizes[-1])}")
        print(f"  empty files: {sum(1 for s in sizes if s == 0)}")
        print(f"  multi-chunk (>32KB): {sum(1 for s in sizes if s > 32768)}")
        print()

        # Compute source checksums
        print("Computing source SHA-256 checksums...")
        t0 = time.monotonic()
        src_hashes = hash_tree(src)
        t_hash = time.monotonic() - t0
        print(f"  {len(src_hashes)} files hashed in {t_hash:.2f}s")
        print()

        # ============================================================
        # Direction A: wimage compress -> wimlib decompress
        # ============================================================
        for compress in ["none", "xpress"]:
            print(f"=== Direction A ({compress}): wimage compress -> wimlib decompress ===")

            wim = os.path.join(tmpdir, f"a_{compress}.wim")
            out = os.path.join(tmpdir, f"a_{compress}_out")

            t0 = time.monotonic()
            run([WIMAGE, "capture", src, wim, "Random250", f"--compress={compress}"])
            t_cap = time.monotonic() - t0
            wsz = os.path.getsize(wim)
            ratio = wsz / total_bytes * 100 if total_bytes > 0 else 0
            print(f"  Capture: {fmt(wsz)} ({ratio:.1f}%) in {t_cap:.2f}s")

            os.makedirs(out)
            t0 = time.monotonic()
            run([WIMLIB, "apply", wim, "1", out])
            t_app = time.monotonic() - t0
            print(f"  wimlib extract: {t_app:.2f}s")

            t0 = time.monotonic()
            out_hashes = hash_tree(out)
            t_chk = time.monotonic() - t0

            if src_hashes == out_hashes:
                print(f"  SHA-256 verify: \033[32mALL {len(out_hashes)} FILES MATCH\033[0m ({t_chk:.2f}s)")
            else:
                missing = set(src_hashes) - set(out_hashes)
                extra = set(out_hashes) - set(src_hashes)
                differ = {k for k in src_hashes if k in out_hashes and src_hashes[k] != out_hashes[k]}
                print(f"  SHA-256 verify: \033[31mFAILED\033[0m")
                if missing:
                    print(f"    Missing files: {len(missing)}")
                    for f in sorted(missing)[:5]:
                        print(f"      {f}")
                if extra:
                    print(f"    Extra files: {len(extra)}")
                if differ:
                    print(f"    Content differs: {len(differ)}")
                    for f in sorted(differ)[:5]:
                        print(f"      {f}: src={src_hashes[f][:16]}... out={out_hashes[f][:16]}...")
                failed += 1
            print()

        # ============================================================
        # Direction B: wimlib compress -> wimage decompress
        # ============================================================
        for compress_arg, compress_name in [("NONE", "none"), ("XPRESS", "xpress")]:
            print(f"=== Direction B ({compress_name}): wimlib compress -> wimage decompress ===")

            wim = os.path.join(tmpdir, f"b_{compress_name}.wim")
            out = os.path.join(tmpdir, f"b_{compress_name}_out")

            t0 = time.monotonic()
            run([WIMLIB, "capture", src, wim, "Random250", f"--compress={compress_arg}"])
            t_cap = time.monotonic() - t0
            wsz = os.path.getsize(wim)
            ratio = wsz / total_bytes * 100 if total_bytes > 0 else 0
            print(f"  wimlib capture: {fmt(wsz)} ({ratio:.1f}%) in {t_cap:.2f}s")

            os.makedirs(out)
            t0 = time.monotonic()
            run([WIMAGE, "apply", wim, "1", out])
            t_app = time.monotonic() - t0
            print(f"  wimage extract: {t_app:.2f}s")

            t0 = time.monotonic()
            out_hashes = hash_tree(out)
            t_chk = time.monotonic() - t0

            if src_hashes == out_hashes:
                print(f"  SHA-256 verify: \033[32mALL {len(out_hashes)} FILES MATCH\033[0m ({t_chk:.2f}s)")
            else:
                missing = set(src_hashes) - set(out_hashes)
                extra = set(out_hashes) - set(src_hashes)
                differ = {k for k in src_hashes if k in out_hashes and src_hashes[k] != out_hashes[k]}
                print(f"  SHA-256 verify: \033[31mFAILED\033[0m")
                if missing:
                    print(f"    Missing files: {len(missing)}")
                if extra:
                    print(f"    Extra files: {len(extra)}")
                if differ:
                    print(f"    Content differs: {len(differ)}")
                    for f in sorted(differ)[:5]:
                        print(f"      {f}: src={src_hashes[f][:16]}... out={out_hashes[f][:16]}...")
                failed += 1
            print()

        # ============================================================
        # Summary
        # ============================================================
        total_tests = 4
        passed = total_tests - failed
        if failed == 0:
            print(f"\033[32m=== ALL {total_tests} TESTS PASSED === 250 files x 4 directions, every byte verified\033[0m")
        else:
            print(f"\033[31m=== {failed}/{total_tests} TESTS FAILED ===\033[0m")

        return failed

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
