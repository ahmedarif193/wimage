#!/usr/bin/env python3
"""
test_regression_matrix.py - Heavier regression matrix for the threaded writer path.

This is intentionally broader than the unit-style comparison suite:
  - builds a mixed stress tree with large compressible, incompressible, mixed,
    duplicate, empty, and many-small-file cases
  - captures with wimage at SMP1/4/8/16 using XPRESS + integrity
  - verifies each WIM, then applies it with both wimage and wimlib
  - captures the same tree with wimlib and applies it with wimage

The goal is to validate safety across the optimized writer path, not just report
one benchmark winner.
"""

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"
THREADS = [1, 4, 8, 16]


def run(cmd):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        out = r.stdout.decode("utf-8", errors="replace")
        err = r.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(
            f"FAILED ({r.returncode}): {' '.join(cmd)}\nSTDOUT:\n{out[:1000]}\nSTDERR:\n{err[:1000]}"
        )
    return (
        r.stdout.decode("utf-8", errors="replace"),
        r.stderr.decode("utf-8", errors="replace"),
    )


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_tree(root):
    result = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for fn in sorted(filenames):
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root)
            result[rel] = sha256_file(full)
    return result


def dir_tree(root):
    paths = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        rel = os.path.relpath(dirpath, root)
        if rel != ".":
            paths.append(rel + "/")
        for fn in sorted(filenames):
            paths.append(os.path.relpath(os.path.join(dirpath, fn), root))
    return sorted(paths)


def tree_fingerprint(root):
    h = hashlib.sha1()
    for rel, digest in sorted(sha256_tree(root).items()):
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(digest.encode("ascii"))
        h.update(b"\n")
    for rel in dir_tree(root):
        h.update(b"D:")
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def deterministic_bytes(label, size):
    parts = []
    seed = hashlib.sha256(label.encode("utf-8")).digest()
    remaining = size
    while remaining > 0:
        seed = hashlib.sha256(seed).digest()
        take = min(len(seed), remaining)
        parts.append(seed[:take])
        remaining -= take
    return b"".join(parts)


def repeated_bytes(label, size):
    pat = (f"{label}-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n".encode("ascii") * 2048)
    return (pat * ((size // len(pat)) + 1))[:size]


def write_file(root, relpath, data):
    full = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as f:
        f.write(data)


def build_stress_tree(root):
    os.makedirs(root, exist_ok=True)

    write_file(root, "empty/zero.bin", b"")
    os.makedirs(os.path.join(root, "empty/dir"), exist_ok=True)

    write_file(root, "large/pattern_2m.bin", repeated_bytes("pattern-2m", 2 * (1 << 20)))
    dup = repeated_bytes("dup-2m", 2 * (1 << 20))
    write_file(root, "dups/dup_a_2m.bin", dup)
    write_file(root, "dups/dup_b_2m.bin", dup)
    write_file(root, "large/random_2m.bin", deterministic_bytes("random-2m", 2 * (1 << 20)))

    mixed = []
    for i in range(32):
        if i % 3 == 0:
            mixed.append(repeated_bytes(f"mixed-pattern-{i}", 1 << 16))
        else:
            mixed.append(deterministic_bytes(f"mixed-random-{i}", 1 << 16))
    write_file(root, "large/mixed_2m.bin", b"".join(mixed))

    for i in range(256):
        rel = f"many/set{i % 16:02d}/file{i:03d}.dat"
        if i % 5 == 0:
            data = repeated_bytes(f"small-pattern-{i}", 8192)
        else:
            data = deterministic_bytes(f"small-random-{i}", 8192)
        write_file(root, rel, data)

    depth = root
    for i in range(12):
        depth = os.path.join(depth, f"deep{i:02d}")
        os.makedirs(depth, exist_ok=True)
    with open(os.path.join(depth, "tail.txt"), "wb") as f:
        f.write(b"deep tail\n")

    write_file(root, "meta/readme.txt", b"regression-matrix\n")


def assert_tree_equal(src, ref_files, ref_paths, label):
    got_files = sha256_tree(src)
    got_paths = dir_tree(src)
    if got_files != ref_files:
        missing = sorted(set(ref_files) - set(got_files))[:5]
        extra = sorted(set(got_files) - set(ref_files))[:5]
        raise AssertionError(f"{label}: file hash mismatch, missing={missing}, extra={extra}")
    if got_paths != ref_paths:
        raise AssertionError(f"{label}: directory listing mismatch")


def main():
    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found. Run 'make' first.", file=sys.stderr)
        return 1

    try:
        run([WIMLIB, "--version"])
    except Exception:
        print(f"ERROR: {WIMLIB} not found. Install wimtools.", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="wimage_regression_") as tmpdir:
        src = os.path.join(tmpdir, "src")
        build_stress_tree(src)
        ref_files = sha256_tree(src)
        ref_paths = dir_tree(src)
        ref_fp = tree_fingerprint(src)

        print("Regression source tree ready")
        print(f"  entries: {len(ref_paths)}")
        print(f"  fingerprint: {ref_fp}")
        print("")
        print("wimage threaded writer matrix")
        print("  threads   wim_mb   verify   wimage_apply   wimlib_apply")
        print("  -------   ------   ------   ------------   -----------")

        for threads in THREADS:
            wim = os.path.join(tmpdir, f"wimage_smp{threads}.wim")
            run([WIMAGE, "capture", src, wim, "Stress", "--compress=xpress", "--check", f"--threads={threads}"])
            run([WIMAGE, "verify", wim])
            run([WIMAGE, "info", wim])
            run([WIMLIB, "info", wim])

            out_rs = os.path.join(tmpdir, f"out_rs_{threads}")
            out_wl = os.path.join(tmpdir, f"out_wl_{threads}")
            os.makedirs(out_rs, exist_ok=True)
            os.makedirs(out_wl, exist_ok=True)
            run([WIMAGE, "apply", wim, "1", out_rs])
            run([WIMLIB, "apply", wim, "1", out_wl])
            assert_tree_equal(out_rs, ref_files, ref_paths, f"wimage apply SMP{threads}")
            assert_tree_equal(out_wl, ref_files, ref_paths, f"wimlib apply SMP{threads}")
            print(f"  {threads:>7}   {os.path.getsize(wim) / (1 << 20):>6.1f}   yes      yes            yes")

        print("")
        print("wimlib writer compatibility")
        print("  writer    wim_mb   wimage_apply   wimlib_apply")
        print("  ------    ------   ------------   -----------")

        wim_wl = os.path.join(tmpdir, "wimlib_xpress.wim")
        run([WIMLIB, "capture", src, wim_wl, "Stress", "--compress=XPRESS"])
        run([WIMAGE, "info", wim_wl])
        run([WIMLIB, "info", wim_wl])

        out_from_wl_rs = os.path.join(tmpdir, "out_from_wl_rs")
        out_from_wl_wl = os.path.join(tmpdir, "out_from_wl_wl")
        os.makedirs(out_from_wl_rs, exist_ok=True)
        os.makedirs(out_from_wl_wl, exist_ok=True)
        run([WIMAGE, "apply", wim_wl, "1", out_from_wl_rs])
        run([WIMLIB, "apply", wim_wl, "1", out_from_wl_wl])
        assert_tree_equal(out_from_wl_rs, ref_files, ref_paths, "wimage apply wimlib")
        assert_tree_equal(out_from_wl_wl, ref_files, ref_paths, "wimlib apply wimlib")
        print(f"  wimlib    {os.path.getsize(wim_wl) / (1 << 20):>6.1f}   yes            yes")

        print("")
        print("All regression matrix checks passed")
        print(f"  source fingerprint: {ref_fp}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
