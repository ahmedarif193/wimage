#!/usr/bin/env python3
"""
test_realworld.py - Real-world WIM stress test using actual build artifacts.

Uses the ReactOS build output (DLLs, .obj, source files) as test data to
verify wimage handles real binary content at scale, matching wimlib
behavior exactly.

Tests:
  - Compress a real directory tree (HAL DLLs, host tools, etc.) with both tools
  - Cross-extract and verify SHA-256 of every file
  - Multi-chunk large files (> 32KB)
  - Mixed binary/text content
  - Compression ratio comparison
  - Round-trip integrity on real data
"""

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"
BUILD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

# Find real directories to test with
CANDIDATES = [
    os.path.join(BUILD_DIR, "hal", "halx86"),                     # DLLs + obj
    os.path.join(BUILD_DIR, "host-tools", "bin"),                  # native binaries
    os.path.join(BUILD_DIR, "..", "sdk", "include", "host"),       # headers
    os.path.join(BUILD_DIR, "..", "boot", "freeldr", "freeldr", "include"),  # freeldr headers
    os.path.join(BUILD_DIR, "..", "sdk", "tools", "cabman"),       # cabman source
]


def run(cmd, check=True):
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and r.returncode != 0:
        err = r.stderr.decode("utf-8", errors="replace")
        raise subprocess.CalledProcessError(r.returncode, cmd, r.stdout, err)
    return r.returncode, r.stdout.decode("utf-8", errors="replace"), r.stderr.decode("utf-8", errors="replace")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_tree(root):
    result = {}
    for dp, dns, fns in os.walk(root):
        dns.sort()
        for fn in sorted(fns):
            full = os.path.join(dp, fn)
            if os.path.isfile(full) and not os.path.islink(full):
                result[os.path.relpath(full, root)] = sha256_file(full)
    return result


def tree_stats(root):
    files = 0
    dirs = 0
    total_bytes = 0
    for dp, dns, fns in os.walk(root):
        dirs += len(dns)
        for fn in fns:
            full = os.path.join(dp, fn)
            if os.path.isfile(full) and not os.path.islink(full):
                files += 1
                total_bytes += os.path.getsize(full)
    return files, dirs, total_bytes


def find_test_dir():
    """Find a suitable real directory with > 1MB of content."""
    for cand in CANDIDATES:
        if os.path.isdir(cand):
            _, _, sz = tree_stats(cand)
            if sz > 100_000:  # at least 100KB
                return cand
    return None


def fmt_size(n):
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


class RealWorldBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="wimage_rw_")
        self.test_dir = find_test_dir()
        if not self.test_dir:
            self.skipTest("No suitable test directory found in build output")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def p(self, *parts):
        return os.path.join(self.tmpdir, *parts)


class TestRealDirNone(RealWorldBase):
    """Uncompressed WIM of real build output."""

    def test_wimage_capture_wimlib_extract(self):
        """wimage captures real dir, wimlib extracts, files match."""
        wim = self.p("test.wim")
        out = self.p("out")
        nf, nd, nb = tree_stats(self.test_dir)
        print(f"\n  Source: {self.test_dir} ({nf} files, {nd} dirs, {fmt_size(nb)})")

        t0 = time.monotonic()
        run([WIMAGE, "capture", self.test_dir, wim, "RealTest", "--compress=none"])
        t_cap = time.monotonic() - t0

        wsz = os.path.getsize(wim)
        print(f"  WIM: {fmt_size(wsz)} (capture {t_cap:.2f}s)")

        t0 = time.monotonic()
        os.makedirs(out)
        run([WIMLIB, "apply", wim, "1", out])
        t_app = time.monotonic() - t0
        print(f"  wimlib extract: {t_app:.2f}s")

        src_h = sha256_tree(self.test_dir)
        out_h = sha256_tree(out)
        self.assertEqual(src_h, out_h, "File content mismatch after wimlib extract")

    def test_wimlib_capture_wimage_extract(self):
        """wimlib captures real dir, wimage extracts, files match."""
        wim = self.p("test.wim")
        out = self.p("out")

        run([WIMLIB, "capture", self.test_dir, wim, "RealTest", "--compress=none"])
        os.makedirs(out)
        run([WIMAGE, "apply", wim, "1", out])

        src_h = sha256_tree(self.test_dir)
        out_h = sha256_tree(out)
        self.assertEqual(src_h, out_h, "File content mismatch after wimage extract")


class TestRealDirXpress(RealWorldBase):
    """XPRESS-compressed WIM of real build output."""

    def test_wimage_capture_wimlib_extract(self):
        """wimage XPRESS captures real dir, wimlib extracts, files match."""
        wim = self.p("test.wim")
        out = self.p("out")
        nf, nd, nb = tree_stats(self.test_dir)
        print(f"\n  Source: {self.test_dir} ({nf} files, {nd} dirs, {fmt_size(nb)})")

        t0 = time.monotonic()
        run([WIMAGE, "capture", self.test_dir, wim, "RealTest", "--compress=xpress"])
        t_cap = time.monotonic() - t0

        wsz = os.path.getsize(wim)
        ratio = wsz / nb * 100 if nb > 0 else 0
        print(f"  WIM: {fmt_size(wsz)} ({ratio:.1f}% of source, capture {t_cap:.2f}s)")

        t0 = time.monotonic()
        os.makedirs(out)
        run([WIMLIB, "apply", wim, "1", out])
        t_app = time.monotonic() - t0
        print(f"  wimlib extract: {t_app:.2f}s")

        src_h = sha256_tree(self.test_dir)
        out_h = sha256_tree(out)
        self.assertEqual(src_h, out_h, "File content mismatch after wimlib extract")

    def test_wimlib_capture_wimage_extract(self):
        """wimlib XPRESS captures real dir, wimage extracts, files match."""
        wim = self.p("test.wim")
        out = self.p("out")
        nf, nd, nb = tree_stats(self.test_dir)
        print(f"\n  Source: {self.test_dir} ({nf} files, {nd} dirs, {fmt_size(nb)})")

        t0 = time.monotonic()
        run([WIMLIB, "capture", self.test_dir, wim, "RealTest", "--compress=XPRESS"])
        t_cap = time.monotonic() - t0

        wsz = os.path.getsize(wim)
        print(f"  WIM: {fmt_size(wsz)} (capture {t_cap:.2f}s)")

        t0 = time.monotonic()
        os.makedirs(out)
        run([WIMAGE, "apply", wim, "1", out])
        t_app = time.monotonic() - t0
        print(f"  wimage extract: {t_app:.2f}s")

        src_h = sha256_tree(self.test_dir)
        out_h = sha256_tree(out)
        self.assertEqual(src_h, out_h, "File content mismatch after wimage extract")

    def test_cross_extract_identical(self):
        """Both tools extract our XPRESS WIM identically."""
        wim = self.p("test.wim")
        run([WIMAGE, "capture", self.test_dir, wim, "RealTest", "--compress=xpress"])
        out_a = self.p("a")
        out_b = self.p("b")
        os.makedirs(out_a)
        os.makedirs(out_b)
        run([WIMLIB, "apply", wim, "1", out_a])
        run([WIMAGE, "apply", wim, "1", out_b])
        self.assertEqual(sha256_tree(out_a), sha256_tree(out_b))


class TestCompressionRealWorld(RealWorldBase):
    """Compression effectiveness on real data."""

    def test_xpress_smaller_than_none(self):
        """XPRESS WIM of real data should be smaller than uncompressed."""
        wim_none = self.p("none.wim")
        wim_xp = self.p("xp.wim")
        run([WIMAGE, "capture", self.test_dir, wim_none, "T", "--compress=none"])
        run([WIMAGE, "capture", self.test_dir, wim_xp, "T", "--compress=xpress"])
        sz_none = os.path.getsize(wim_none)
        sz_xp = os.path.getsize(wim_xp)
        print(f"\n  none={fmt_size(sz_none)} xpress={fmt_size(sz_xp)} ratio={sz_xp/sz_none*100:.1f}%")
        self.assertLess(sz_xp, sz_none)

    def test_ratio_vs_wimlib(self):
        """Our XPRESS ratio should be reasonable vs wimlib."""
        wim_ours = self.p("ours.wim")
        wim_theirs = self.p("theirs.wim")
        run([WIMAGE, "capture", self.test_dir, wim_ours, "T", "--compress=xpress"])
        run([WIMLIB, "capture", self.test_dir, wim_theirs, "T", "--compress=XPRESS"])
        sz_ours = os.path.getsize(wim_ours)
        sz_theirs = os.path.getsize(wim_theirs)
        ratio = sz_ours / sz_theirs
        print(f"\n  ours={fmt_size(sz_ours)} wimlib={fmt_size(sz_theirs)} ratio={ratio:.2f}x")
        self.assertLess(ratio, 3.0, "Our WIM >3x wimlib's - compression too poor")


class TestIntegrityRealWorld(RealWorldBase):
    """Integrity table on real data."""

    def test_integrity_roundtrip(self):
        """Capture with --check, verify passes."""
        wim = self.p("test.wim")
        run([WIMAGE, "capture", self.test_dir, wim, "T", "--compress=xpress", "--check"])
        rc, out, _ = run([WIMAGE, "verify", wim], check=False)
        self.assertEqual(rc, 0, "Integrity verification failed on real data")

    def test_integrity_then_extract(self):
        """WIM with integrity table can still be extracted correctly."""
        wim = self.p("test.wim")
        out = self.p("out")
        run([WIMAGE, "capture", self.test_dir, wim, "T", "--compress=xpress", "--check"])
        os.makedirs(out)
        run([WIMLIB, "apply", wim, "1", out])
        self.assertEqual(sha256_tree(self.test_dir), sha256_tree(out))


class TestMultipleDirs(unittest.TestCase):
    """Test with multiple real directories if available."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="wimage_multi_")
        self.dirs = [d for d in CANDIDATES if os.path.isdir(d)]
        if len(self.dirs) < 2:
            self.skipTest("Need at least 2 real directories")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_multiple_dirs_sequential(self):
        """Capture and verify multiple different real directories."""
        for i, src in enumerate(self.dirs[:3]):
            nf, nd, nb = tree_stats(src)
            if nb < 1000:
                continue
            wim = os.path.join(self.tmpdir, f"dir{i}.wim")
            out = os.path.join(self.tmpdir, f"out{i}")
            print(f"\n  Dir {i}: {os.path.basename(src)} ({nf} files, {fmt_size(nb)})")
            run([WIMAGE, "capture", src, wim, f"Dir{i}", "--compress=xpress"])
            os.makedirs(out)
            run([WIMLIB, "apply", wim, "1", out])
            self.assertEqual(sha256_tree(src), sha256_tree(out),
                             f"Content mismatch for {src}")


if __name__ == "__main__":
    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found. Run 'make' first.", file=sys.stderr)
        sys.exit(1)
    rc, _, _ = run([WIMLIB, "--version"], check=False)
    if rc != 0:
        print(f"ERROR: {WIMLIB} not found.", file=sys.stderr)
        sys.exit(1)

    # Show what test data we'll use
    td = find_test_dir()
    if td:
        nf, nd, nb = tree_stats(td)
        print(f"Primary test dir: {td} ({nf} files, {nd} dirs, {fmt_size(nb)})")
    else:
        print("WARNING: No suitable test directory found, some tests will be skipped")

    print(f"Available dirs: {sum(1 for d in CANDIDATES if os.path.isdir(d))}/{len(CANDIDATES)}\n")

    unittest.main(verbosity=2)
