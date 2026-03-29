#!/usr/bin/env python3
"""
test_bench_compare.py - Consolidated repeated benchmark and comparison table.

Runs repeated compression benchmarks for:
  - mixed data
  - highly patterned data
  - pseudo-random data

For each profile and size:
  - benchmark wimage at SMP1/4/8/16
  - benchmark wimlib-imagex
  - report median and best throughput
  - cross-verify SHA-1 by extracting our SMP16 WIM with wimlib and
    the wimlib WIM with wimage
"""

import argparse
import hashlib
import os
import shutil
import statistics
import subprocess
import sys
import time

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"
TMPDIR = "/tmp/wimage_bench_compare"

SIZES = [
    ("32MB", 32 * 1024 * 1024),
    ("64MB", 64 * 1024 * 1024),
    ("128MB", 128 * 1024 * 1024),
]

THREADS = [1, 4, 8, 16]
PROFILES = ["mixed", "patterned", "random"]


def run(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"FAILED ({r.returncode}): {' '.join(cmd)}\n{err}")


def sha1_file(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def patterned_block(index, chunk_sz):
    pat = (f"Pattern-{index:08d}-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n".encode() * 2048)
    return (pat * ((chunk_sz // len(pat)) + 1))[:chunk_sz]


def random_block(index, written, chunk_sz):
    parts = []
    seed = hashlib.sha256(f"seed-{index}-{written}".encode()).digest()
    remaining = chunk_sz
    while remaining > 0:
        seed = hashlib.sha256(seed).digest()
        take = min(32, remaining)
        parts.append(seed[:take])
        remaining -= take
    return b"".join(parts)


def mixed_block(index, written, chunk_sz):
    if index % 4 == 0:
        return patterned_block(index, chunk_sz)
    return random_block(index, written, chunk_sz)


def generate_file(path, size, profile):
    written = 0
    block = 1 << 20
    with open(path, "wb") as f:
        index = 0
        while written < size:
            chunk_sz = min(block, size - written)
            if profile == "patterned":
                data = patterned_block(index, chunk_sz)
            elif profile == "random":
                data = random_block(index, written, chunk_sz)
            elif profile == "mixed":
                data = mixed_block(index, written, chunk_sz)
            else:
                raise ValueError(f"unknown profile: {profile}")
            f.write(data)
            written += chunk_sz
            index += 1


def ensure_source(profile, size_name, size_bytes):
    os.makedirs(TMPDIR, exist_ok=True)
    src_file = os.path.join(TMPDIR, f"{profile}_{size_name}.bin")
    src_dir = os.path.join(TMPDIR, f"{profile}_{size_name}_dir")
    src_in_dir = os.path.join(src_dir, f"data_{profile}_{size_name}.bin")

    if not os.path.isfile(src_file) or os.path.getsize(src_file) != size_bytes:
        print(f"Generating {profile}/{size_name}...", end=" ", flush=True)
        t0 = time.monotonic()
        generate_file(src_file, size_bytes, profile)
        print(f"{time.monotonic() - t0:.1f}s")

    os.makedirs(src_dir, exist_ok=True)
    if os.path.exists(src_in_dir):
        if os.path.samefile(src_file, src_in_dir):
            pass
        else:
            os.unlink(src_in_dir)
            os.link(src_file, src_in_dir)
    else:
        os.link(src_file, src_in_dir)

    return src_file, src_dir, os.path.basename(src_in_dir)


def bench_capture(cmd, wim_path, runs, warmups):
    samples = []
    size_bytes = 0
    for i in range(warmups + runs):
        if os.path.exists(wim_path):
            os.unlink(wim_path)
        t0 = time.monotonic()
        run(cmd)
        dt = time.monotonic() - t0
        size_bytes = os.path.getsize(wim_path)
        if i >= warmups:
            samples.append(dt)
    return {
        "samples": samples,
        "median": statistics.median(samples),
        "best": min(samples),
        "size": size_bytes,
    }


def bench_apply(cmd, out_dir):
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    t0 = time.monotonic()
    run(cmd)
    return time.monotonic() - t0


def summarize_row(label, stats, total_mb, input_size):
    median_mbs = total_mb / stats["median"] if stats["median"] > 0 else 0
    best_mbs = total_mb / stats["best"] if stats["best"] > 0 else 0
    ratio = stats["size"] / input_size * 100
    sz_str = f"{stats['size'] / (1 << 20):.0f}MB"
    return (
        f"  {label:<16} {stats['median']:>7.2f}s {median_mbs:>7.0f} "
        f"{best_mbs:>7.0f} {sz_str:>9} {ratio:>7.1f}%"
    )


def main():
    parser = argparse.ArgumentParser(description="Repeated wimage vs wimlib benchmark")
    parser.add_argument("--runs", type=int, default=3, help="timed runs per tool/profile/size")
    parser.add_argument("--warmup", type=int, default=1, help="warmup runs per tool/profile/size")
    args = parser.parse_args()

    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found. Run 'make' first.", file=sys.stderr)
        return 1

    try:
        run([WIMLIB, "--version"])
    except Exception:
        print(f"ERROR: {WIMLIB} not found. Install wimtools.", file=sys.stderr)
        return 1

    os.makedirs(TMPDIR, exist_ok=True)
    summary = []

    print(f"Repeated runs: {args.runs} timed + {args.warmup} warmup")

    for profile in PROFILES:
        print(f"\n{'#' * 78}")
        print(f"# Profile: {profile}")
        print(f"{'#' * 78}")

        for size_name, size_bytes in SIZES:
            src_file, src_dir, fname = ensure_source(profile, size_name, size_bytes)
            src_sha1 = sha1_file(src_file)
            total_mb = size_bytes / (1 << 20)

            results = {}
            artifacts = {}

            for threads in THREADS:
                wim = os.path.join(TMPDIR, f"{profile}_{size_name}_rs_smp{threads}.wim")
                cmd = [WIMAGE, "capture", src_dir, wim, "Bench",
                       "--compress=xpress", f"--threads={threads}"]
                stats = bench_capture(cmd, wim, args.runs, args.warmup)
                key = f"wimage-smp{threads}"
                results[key] = stats
                artifacts[key] = wim

            wim_wl = os.path.join(TMPDIR, f"{profile}_{size_name}_wimlib.wim")
            stats = bench_capture(
                [WIMLIB, "capture", src_dir, wim_wl, "Bench", "--compress=XPRESS"],
                wim_wl, args.runs, args.warmup)
            results["wimlib"] = stats
            artifacts["wimlib"] = wim_wl

            # Cross-verify artifacts using the strongest wimage output and the wimlib output.
            out_a = os.path.join(TMPDIR, f"{profile}_{size_name}_out_wimlib")
            out_b = os.path.join(TMPDIR, f"{profile}_{size_name}_out_wimage")
            t_a = bench_apply([WIMLIB, "apply", artifacts["wimage-smp16"], "1", out_a], out_a)
            t_b = bench_apply([WIMAGE, "apply", artifacts["wimlib"], "1", out_b], out_b)
            out_a_sha1 = sha1_file(os.path.join(out_a, fname))
            out_b_sha1 = sha1_file(os.path.join(out_b, fname))
            assert out_a_sha1 == src_sha1, f"wimlib apply mismatch: {profile}/{size_name}"
            assert out_b_sha1 == src_sha1, f"wimage apply mismatch: {profile}/{size_name}"
            shutil.rmtree(out_a, ignore_errors=True)
            shutil.rmtree(out_b, ignore_errors=True)

            print(f"\n{'=' * 78}")
            print(f"  {profile} / {size_name} ({size_bytes:,} bytes)  SHA-1: {src_sha1}")
            print(f"{'=' * 78}")
            print(f"  {'Tool':<16} {'Median':>8} {'MedMB/s':>8} {'BestMB/s':>8} {'WIM Size':>9} {'Ratio':>8}")
            print(f"  {'-' * 16} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 9} {'-' * 8}")
            for key in [f"wimage-smp{t}" for t in THREADS] + ["wimlib"]:
                print(summarize_row(key, results[key], total_mb, size_bytes))

            winner_key = max(results, key=lambda k: total_mb / results[k]["median"])
            winner_mbs = total_mb / results[winner_key]["median"]
            wl_mbs = total_mb / results["wimlib"]["median"]
            print(f"\n  Cross-apply SHA-1 verified. wimlib apply: {t_a:.2f}s, wimage apply: {t_b:.2f}s")
            print(f"  Winner by median throughput: {winner_key} ({winner_mbs:.0f} MB/s)")
            if winner_key != "wimlib":
                print(f"  Margin vs wimlib: {winner_mbs / wl_mbs:.2f}x")
            else:
                best_wimage = max(total_mb / results[f'wimage-smp{t}']['median'] for t in THREADS)
                print(f"  WIMLIB lead vs best wimage: {wl_mbs / best_wimage:.2f}x")

            summary.append({
                "profile": profile,
                "size": size_name,
                "winner": winner_key,
                "winner_mbs": winner_mbs,
                "wimlib_mbs": wl_mbs,
                "wimage16_mbs": total_mb / results["wimage-smp16"]["median"],
            })

            for wim in artifacts.values():
                if os.path.exists(wim):
                    os.unlink(wim)

    print(f"\n{'#' * 78}")
    print("# Summary")
    print(f"{'#' * 78}")
    print(f"  {'Profile':<12} {'Size':<8} {'Winner':<16} {'Winner MB/s':>10} {'RS16 MB/s':>10} {'WIMLIB MB/s':>12}")
    print(f"  {'-' * 12} {'-' * 8} {'-' * 16} {'-' * 10} {'-' * 10} {'-' * 12}")
    for row in summary:
        print(f"  {row['profile']:<12} {row['size']:<8} {row['winner']:<16} "
              f"{row['winner_mbs']:>10.0f} {row['wimage16_mbs']:>10.0f} {row['wimlib_mbs']:>12.0f}")

    shutil.rmtree(TMPDIR, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
