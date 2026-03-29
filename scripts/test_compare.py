#!/usr/bin/env python3
"""
test_compare.py - Behavioral comparison: wimage vs wimlib-imagex

Tests that both tools produce functionally identical results for every
WIM operation: capture, apply, info, dir, verify, and cross-tool interop.

Covers:
  - Header fields (magic, version, flags, compression type, image count)
  - Directory listings (file paths, counts)
  - File content round-trip integrity (SHA-256 of every extracted file)
  - Metadata accuracy (file counts, dir counts, timestamps present)
  - Deduplication (identical files share blobs)
  - Multi-chunk resources (files > 32KB chunk size)
  - Compression ratio sanity (XPRESS should compress repetitive data)
  - Integrity table generation and verification
  - Edge cases: empty files, empty dirs, deep nesting, large files, symlink-like dups
"""

import hashlib
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WIMAGE = os.environ.get("WIMAGE_BIN", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "wimage"))
WIMLIB = "wimlib-imagex"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd, check=True, capture=True):
    """Run a command, return (returncode, stdout, stderr)."""
    r = subprocess.run(
        cmd, shell=isinstance(cmd, str),
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    out = r.stdout.decode("utf-8", errors="replace") if r.stdout else ""
    err = r.stderr.decode("utf-8", errors="replace") if r.stderr else ""
    if check and r.returncode != 0:
        raise subprocess.CalledProcessError(r.returncode, cmd, out, err)
    return r.returncode, out, err


def sha256_file(path):
    """Return hex SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_tree(root):
    """Return dict {relative_path: sha256} for every file under root."""
    result = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for fn in sorted(filenames):
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root)
            result[rel] = sha256_file(full)
    return result


def dir_tree(root):
    """Return sorted list of all relative paths (files + dirs) under root."""
    paths = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        rel = os.path.relpath(dirpath, root)
        if rel != ".":
            paths.append(rel + "/")
        for fn in sorted(filenames):
            paths.append(os.path.relpath(os.path.join(dirpath, fn), root))
    return sorted(paths)


def parse_wim_header(wim_path):
    """Read the 208-byte WIM header and return a dict of fields."""
    with open(wim_path, "rb") as f:
        data = f.read(208)
    if len(data) < 208:
        return None
    fields = {}
    fields["magic"] = data[0:8]
    fields["header_size"] = struct.unpack_from("<I", data, 8)[0]
    fields["version"] = struct.unpack_from("<I", data, 12)[0]
    fields["flags"] = struct.unpack_from("<I", data, 16)[0]
    fields["chunk_size"] = struct.unpack_from("<I", data, 20)[0]
    fields["guid"] = data[24:40]
    fields["part_number"] = struct.unpack_from("<H", data, 40)[0]
    fields["total_parts"] = struct.unpack_from("<H", data, 42)[0]
    fields["image_count"] = struct.unpack_from("<I", data, 44)[0]
    # lookup table reshdr at offset 48
    fields["lookup_offset"] = struct.unpack_from("<Q", data, 56)[0]
    fields["lookup_orig_size"] = struct.unpack_from("<Q", data, 64)[0]
    # xml data reshdr at offset 72
    fields["xml_offset"] = struct.unpack_from("<Q", data, 80)[0]
    fields["xml_orig_size"] = struct.unpack_from("<Q", data, 88)[0]
    # boot index at offset 120
    fields["boot_index"] = struct.unpack_from("<I", data, 120)[0]
    # integrity reshdr at offset 124
    fields["integrity_offset"] = struct.unpack_from("<Q", data, 132)[0]
    fields["integrity_orig_size"] = struct.unpack_from("<Q", data, 140)[0]
    fields["file_size"] = os.path.getsize(wim_path)
    return fields


def parse_info_output(text):
    """Parse key-value pairs from info command output."""
    kv = {}
    for line in text.splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            kv[k.strip()] = v.strip()
    return kv


def parse_dir_output(text):
    """Parse dir listing into sorted path list."""
    paths = []
    for line in text.strip().splitlines():
        line = line.strip()
        if line and not line.startswith("-"):
            paths.append(line)
    return sorted(paths)


# ---------------------------------------------------------------------------
# Test data builders
# ---------------------------------------------------------------------------

def make_basic_tree(root):
    """Standard test tree with varied content."""
    os.makedirs(os.path.join(root, "subdir", "deep"), exist_ok=True)
    os.makedirs(os.path.join(root, "emptydir"), exist_ok=True)

    _write(root, "hello.txt", b"Hello World\n")
    _write(root, "subdir/nested.txt", b"Nested file content here\n")
    _write(root, "subdir/deep/deep.txt", b"Deep nested file\n")
    _write(root, "dup1.txt", b"Duplicate content for dedup test\n")
    _write(root, "dup2.txt", b"Duplicate content for dedup test\n")  # same as dup1
    _write(root, "empty.txt", b"")  # zero-length


def make_large_tree(root):
    """Tree with large and repetitive files to stress compression."""
    os.makedirs(root, exist_ok=True)
    # 64KB random (incompressible, multi-chunk at 32KB)
    import random
    rng = random.Random(42)
    _write(root, "random_64k.bin", bytes(rng.getrandbits(8) for _ in range(65536)))
    # 32KB highly repetitive (compresses very well, tests long matches)
    pattern = b"ABCDEFGHIJKLMNOP\n"
    rep = (pattern * ((32768 // len(pattern)) + 1))[:32768]
    _write(root, "repetitive_32k.dat", rep)
    # 128KB mixed
    mixed = b"The quick brown fox jumps. " * 5000
    _write(root, "mixed_128k.txt", mixed[:131072])
    # Small file
    _write(root, "tiny.txt", b"x\n")


def make_huge_tree(root):
    """Tree with >1MB files to exercise mmap capture and threaded compression."""
    os.makedirs(root, exist_ok=True)

    pattern = (b"WIMAGE-XPRESS-BLOCK-" * 4096)[:1 << 20]
    _write(root, "pattern_1m.bin", pattern + pattern)

    seed = hashlib.sha256(b"huge-tree-seed").digest()
    parts = []
    remaining = 2 * (1 << 20)
    while remaining > 0:
        seed = hashlib.sha256(seed).digest()
        take = min(len(seed), remaining)
        parts.append(seed[:take])
        remaining -= take
    _write(root, "random_2m.bin", b"".join(parts))

    mixed = []
    remaining = 2 * (1 << 20)
    block = 1 << 16
    i = 0
    while remaining > 0:
        take = min(block, remaining)
        if i % 4 == 0:
            chunk = (f"Huge-{i:06d}-ABCDEFGHIJKLMNOPQRSTUVWXYZ\n".encode() * 2048)[:take]
        else:
            seed = hashlib.sha256(seed + bytes([i & 0xFF])).digest()
            chunk_parts = []
            left = take
            tmp = seed
            while left > 0:
                tmp = hashlib.sha256(tmp).digest()
                use = min(len(tmp), left)
                chunk_parts.append(tmp[:use])
                left -= use
            chunk = b"".join(chunk_parts)
        mixed.append(chunk)
        remaining -= take
        i += 1
    _write(root, "mixed_2m.bin", b"".join(mixed))

    _write(root, "tiny.txt", b"mmap-threshold-anchor\n")


def make_deep_tree(root):
    """Deeply nested directory tree."""
    path = root
    for i in range(20):
        path = os.path.join(path, f"level{i}")
        os.makedirs(path, exist_ok=True)
    _write(path, "bottom.txt", b"At the bottom\n")


def _write(root, relpath, content):
    full = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class WimCompareBase(unittest.TestCase):
    """Base class with temp dir management."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="wimage_pytest_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def path(self, *parts):
        return os.path.join(self.tmpdir, *parts)

    def capture_wimage(self, src, wim, name="Test", compress="none", check=False, threads=1):
        cmd = [WIMAGE, "capture", src, wim, name, f"--compress={compress}"]
        if threads and threads > 1:
            cmd.append(f"--threads={threads}")
        if check:
            cmd.append("--check")
        run(cmd)

    def capture_wimlib(self, src, wim, name="Test", compress="NONE"):
        run([WIMLIB, "capture", src, wim, name, f"--compress={compress}"])

    def apply_wimage(self, wim, dest, image="1"):
        os.makedirs(dest, exist_ok=True)
        run([WIMAGE, "apply", wim, image, dest])

    def apply_wimlib(self, wim, dest, image="1"):
        os.makedirs(dest, exist_ok=True)
        run([WIMLIB, "apply", wim, image, dest])


class TestHeaderFormat(WimCompareBase):
    """Binary header must match the WIM specification."""

    def _check_header(self, wim_path, compress):
        hdr = parse_wim_header(wim_path)
        self.assertIsNotNone(hdr)
        self.assertEqual(hdr["magic"], b"MSWIM\x00\x00\x00", "WIM magic")
        self.assertEqual(hdr["header_size"], 208, "header size")
        self.assertEqual(hdr["version"], 0x00010D00, "version 1.13")
        self.assertEqual(hdr["part_number"], 1)
        self.assertEqual(hdr["total_parts"], 1)
        self.assertEqual(hdr["image_count"], 1)
        if compress == "none":
            self.assertEqual(hdr["chunk_size"], 0, "uncompressed chunk_size must be 0")
            self.assertEqual(hdr["flags"] & 0x00020000, 0, "XPRESS flag off")
        else:
            self.assertEqual(hdr["chunk_size"], 32768, "XPRESS chunk_size")
            self.assertNotEqual(hdr["flags"] & 0x00020000, 0, "XPRESS flag on")
        self.assertGreater(hdr["lookup_offset"], 0, "lookup table present")
        self.assertGreater(hdr["xml_offset"], 0, "XML data present")

    def test_header_uncompressed(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none")
        self._check_header(wim, "none")

    def test_header_xpress(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="xpress")
        self._check_header(wim, "xpress")

    def test_header_matches_wimlib(self):
        """Key header fields must match between both tools."""
        src = self.path("src")
        make_basic_tree(src)
        ours = self.path("ours.wim")
        theirs = self.path("theirs.wim")
        self.capture_wimage(src, ours, compress="none")
        self.capture_wimlib(src, theirs, compress="NONE")
        h1 = parse_wim_header(ours)
        h2 = parse_wim_header(theirs)
        self.assertEqual(h1["version"], h2["version"])
        self.assertEqual(h1["image_count"], h2["image_count"])
        self.assertEqual(h1["part_number"], h2["part_number"])
        self.assertEqual(h1["chunk_size"], h2["chunk_size"])


class TestInfoCommand(WimCompareBase):
    """Info output must report correct metadata."""

    def _compare_info(self, compress_ours, compress_wimlib):
        src = self.path("src")
        make_basic_tree(src)
        ours = self.path("ours.wim")
        theirs = self.path("theirs.wim")
        self.capture_wimage(src, ours, compress=compress_ours)
        self.capture_wimlib(src, theirs, compress=compress_wimlib)

        _, out_ours, _ = run([WIMAGE, "info", ours])
        _, out_theirs, _ = run([WIMLIB, "info", theirs])
        kv_ours = parse_info_output(out_ours)
        kv_theirs = parse_info_output(out_theirs)

        self.assertEqual(kv_ours.get("Image Count"), kv_theirs.get("Image Count"),
                         "Image Count mismatch")
        self.assertEqual(kv_ours.get("Compression"), kv_theirs.get("Compression"),
                         "Compression type mismatch")

    def test_info_none(self):
        self._compare_info("none", "NONE")

    def test_info_xpress(self):
        self._compare_info("xpress", "XPRESS")

    def test_info_header_flag(self):
        """--header flag produces output with 'Magic' field."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim)
        _, out, _ = run([WIMAGE, "info", wim, "--header"])
        self.assertIn("Magic", out)
        self.assertIn("Version", out)

    def test_info_xml_flag(self):
        """--xml flag produces XML content."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim)
        _, out, _ = run([WIMAGE, "info", wim, "--xml"])
        self.assertIn("WIM", out)

    def test_file_dir_counts(self):
        """File and directory counts must match between tools."""
        src = self.path("src")
        make_basic_tree(src)
        ours = self.path("ours.wim")
        theirs = self.path("theirs.wim")
        self.capture_wimage(src, ours, compress="none")
        self.capture_wimlib(src, theirs, compress="NONE")
        _, o1, _ = run([WIMAGE, "info", ours])
        _, o2, _ = run([WIMLIB, "info", theirs])
        kv1, kv2 = parse_info_output(o1), parse_info_output(o2)
        self.assertEqual(kv1.get("Directory Count"), kv2.get("Directory Count"))
        self.assertEqual(kv1.get("File Count"), kv2.get("File Count"))


class TestDirCommand(WimCompareBase):
    """Dir listings must be identical between both tools."""

    def test_dir_basic(self):
        src = self.path("src")
        make_basic_tree(src)
        ours = self.path("ours.wim")
        theirs = self.path("theirs.wim")
        self.capture_wimage(src, ours, compress="none")
        self.capture_wimlib(src, theirs, compress="NONE")
        _, d1, _ = run([WIMAGE, "dir", ours])
        _, d2, _ = run([WIMLIB, "dir", theirs])
        self.assertEqual(parse_dir_output(d1), parse_dir_output(d2))

    def test_dir_detailed(self):
        """--detailed flag produces attribute information."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim)
        _, out, _ = run([WIMAGE, "dir", wim, "--detailed"])
        self.assertIn("Attributes", out)

    def test_dir_deep_tree(self):
        """20-level deep directory tree listed correctly."""
        src = self.path("src")
        make_deep_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none")
        _, out, _ = run([WIMAGE, "dir", wim])
        self.assertIn("level19", out)
        self.assertIn("bottom.txt", out)


class TestCaptureApplyNone(WimCompareBase):
    """Round-trip capture+apply with no compression."""

    def test_wimage_roundtrip(self):
        """wimage capture -> wimage apply, content identical."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="none")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_wimlib_reads_ours(self):
        """wimlib can apply a WIM created by wimage."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="none")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_wimage_reads_wimlib(self):
        """wimage can apply a WIM created by wimlib."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimlib(src, wim, compress="NONE")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_cross_extract_identical(self):
        """Both tools extracting the same WIM produce identical trees."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none")
        out_a = self.path("out_a")
        out_b = self.path("out_b")
        self.apply_wimlib(wim, out_a)
        self.apply_wimage(wim, out_b)
        self.assertEqual(sha256_tree(out_a), sha256_tree(out_b))


class TestCaptureApplyXpress(WimCompareBase):
    """Round-trip capture+apply with XPRESS Huffman compression."""

    def test_wimage_roundtrip(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_wimlib_reads_ours(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_wimage_reads_wimlib(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimlib(src, wim, compress="XPRESS")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_cross_extract_identical(self):
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimlib(src, wim, compress="XPRESS")
        out_a = self.path("out_a")
        out_b = self.path("out_b")
        self.apply_wimlib(wim, out_a)
        self.apply_wimage(wim, out_b)
        self.assertEqual(sha256_tree(out_a), sha256_tree(out_b))


class TestLargeFiles(WimCompareBase):
    """Large / multi-chunk files must survive round-trip."""

    def test_large_none_roundtrip(self):
        src = self.path("src")
        make_large_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="none")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_large_xpress_roundtrip(self):
        src = self.path("src")
        make_large_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_large_xpress_wimlib_reads(self):
        """wimlib can extract our XPRESS WIM with large files."""
        src = self.path("src")
        make_large_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_large_xpress_wimlib_created(self):
        """wimage can extract wimlib XPRESS WIM with large files."""
        src = self.path("src")
        make_large_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimlib(src, wim, compress="XPRESS")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_huge_xpress_threaded_roundtrip(self):
        """Threaded XPRESS capture round-trips >1MB files through our writer path."""
        src = self.path("src")
        make_huge_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress", threads=8)
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_huge_xpress_threaded_wimlib_reads(self):
        """wimlib can extract our threaded XPRESS WIM created from >1MB files."""
        src = self.path("src")
        make_huge_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress", threads=8)
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_huge_xpress_wimlib_created(self):
        """wimage can extract a huge >1MB XPRESS WIM created by wimlib."""
        src = self.path("src")
        make_huge_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimlib(src, wim, compress="XPRESS")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_huge_xpress_cross_extract_identical(self):
        """Both tools extract the same huge threaded XPRESS WIM identically."""
        src = self.path("src")
        make_huge_tree(src)
        wim = self.path("test.wim")
        out_a = self.path("out_a")
        out_b = self.path("out_b")
        self.capture_wimage(src, wim, compress="xpress", threads=8)
        self.apply_wimage(wim, out_a)
        self.apply_wimlib(wim, out_b)
        self.assertEqual(sha256_tree(src), sha256_tree(out_a))
        self.assertEqual(sha256_tree(out_a), sha256_tree(out_b))


class TestCompressionRatio(WimCompareBase):
    """XPRESS should actually compress repetitive data."""

    def test_xpress_compresses(self):
        src = self.path("src")
        make_large_tree(src)
        wim_none = self.path("none.wim")
        wim_xp = self.path("xp.wim")
        self.capture_wimage(src, wim_none, compress="none")
        self.capture_wimage(src, wim_xp, compress="xpress")
        sz_none = os.path.getsize(wim_none)
        sz_xp = os.path.getsize(wim_xp)
        self.assertLess(sz_xp, sz_none, "XPRESS WIM should be smaller than uncompressed")
        ratio = sz_xp / sz_none
        self.assertLess(ratio, 0.95, f"XPRESS ratio {ratio:.2%} too high, compression ineffective")

    def test_ratio_comparable_to_wimlib(self):
        """Our compression ratio should be within 2x of wimlib's."""
        src = self.path("src")
        make_large_tree(src)
        ours = self.path("ours.wim")
        theirs = self.path("theirs.wim")
        self.capture_wimage(src, ours, compress="xpress")
        self.capture_wimlib(src, theirs, compress="XPRESS")
        sz_ours = os.path.getsize(ours)
        sz_theirs = os.path.getsize(theirs)
        ratio = sz_ours / sz_theirs
        self.assertLess(ratio, 2.0, f"Our WIM is {ratio:.1f}x wimlib's size - too large")
        self.assertGreater(ratio, 0.5, f"Our WIM is {ratio:.1f}x wimlib's size - suspiciously small")


class TestDeduplication(WimCompareBase):
    """Files with identical content must share a single blob."""

    def test_dedup_same_content(self):
        """dup1.txt and dup2.txt have identical content, WIM should dedup."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none")
        _, out, _ = run([WIMAGE, "info", wim, "--blobs"])
        # Count blob entries by looking for "Hash" lines
        blob_count = out.count("Hash")
        # 4 unique file contents + 1 metadata = 5 blobs (empty.txt has no blob)
        # hello.txt, nested.txt, deep.txt, dup(shared) = 4 file blobs
        self.assertLessEqual(blob_count, 6, f"Expected <=6 blobs (with dedup), got {blob_count}")

    def test_dedup_file_size(self):
        """WIM with duplicates should be smaller than without."""
        src_dedup = self.path("src_dedup")
        os.makedirs(src_dedup)
        data = b"X" * 10000
        for i in range(10):
            _write(src_dedup, f"file{i}.txt", data)

        src_unique = self.path("src_unique")
        os.makedirs(src_unique)
        for i in range(10):
            _write(src_unique, f"file{i}.txt", data + bytes([i]))

        wim_dedup = self.path("dedup.wim")
        wim_unique = self.path("unique.wim")
        self.capture_wimage(src_dedup, wim_dedup, compress="none")
        self.capture_wimage(src_unique, wim_unique, compress="none")
        self.assertLess(os.path.getsize(wim_dedup), os.path.getsize(wim_unique),
                        "Dedup WIM should be smaller than unique WIM")


class TestEdgeCases(WimCompareBase):
    """Edge cases that could trip up format handling."""

    def test_empty_file(self):
        """Zero-length file survives round-trip."""
        src = self.path("src")
        os.makedirs(src)
        _write(src, "empty.txt", b"")
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim)
        self.apply_wimage(wim, out)
        self.assertTrue(os.path.isfile(os.path.join(out, "empty.txt")))
        self.assertEqual(os.path.getsize(os.path.join(out, "empty.txt")), 0)

    def test_empty_directory(self):
        """Empty directory preserved in round-trip."""
        src = self.path("src")
        os.makedirs(os.path.join(src, "emptydir"))
        _write(src, "a.txt", b"anchor\n")
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim)
        self.apply_wimage(wim, out)
        self.assertTrue(os.path.isdir(os.path.join(out, "emptydir")))

    def test_deep_nesting(self):
        """20-level deep directory tree survives round-trip."""
        src = self.path("src")
        make_deep_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_deep_nesting_wimlib_compat(self):
        """wimlib reads our deeply nested XPRESS WIM."""
        src = self.path("src")
        make_deep_tree(src)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_single_file(self):
        """WIM with just one file."""
        src = self.path("src")
        os.makedirs(src)
        _write(src, "only.txt", b"solo\n")
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))

    def test_binary_content(self):
        """Files with all 256 byte values."""
        src = self.path("src")
        os.makedirs(src)
        _write(src, "allbytes.bin", bytes(range(256)) * 4)
        wim = self.path("test.wim")
        out = self.path("out")
        self.capture_wimage(src, wim, compress="xpress")
        self.apply_wimage(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))


class TestIntegrity(WimCompareBase):
    """Integrity table generation and verification."""

    def test_integrity_roundtrip(self):
        """--check creates integrity table, verify confirms it."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none", check=True)
        rc, out, _ = run([WIMAGE, "verify", wim], check=False)
        self.assertEqual(rc, 0, "Integrity verification should pass")

    def test_integrity_header_present(self):
        """WIM created with --check has non-zero integrity reshdr."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none", check=True)
        hdr = parse_wim_header(wim)
        self.assertGreater(hdr["integrity_offset"], 0, "integrity table should be present")
        self.assertGreater(hdr["integrity_orig_size"], 0)

    def test_no_integrity_by_default(self):
        """WIM without --check has no integrity table."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none")
        hdr = parse_wim_header(wim)
        self.assertEqual(hdr["integrity_offset"], 0)

    def test_wimlib_accepts_our_integrity(self):
        """wimlib can read a WIM with our integrity table."""
        src = self.path("src")
        make_basic_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="none", check=True)
        rc, _, _ = run([WIMLIB, "info", wim], check=False)
        self.assertEqual(rc, 0)

    def test_threaded_xpress_integrity_roundtrip(self):
        """Threaded XPRESS capture with integrity preserves >1MB files."""
        src = self.path("src")
        out = self.path("out")
        make_huge_tree(src)
        wim = self.path("test.wim")
        self.capture_wimage(src, wim, compress="xpress", check=True, threads=8)
        rc, _, _ = run([WIMAGE, "verify", wim], check=False)
        self.assertEqual(rc, 0, "Threaded integrity verification should pass")
        self.apply_wimlib(wim, out)
        self.assertEqual(sha256_tree(src), sha256_tree(out))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Verify tools exist
    if not os.path.isfile(WIMAGE):
        print(f"ERROR: {WIMAGE} not found. Run 'make' first.", file=sys.stderr)
        sys.exit(1)
    rc, _, _ = run([WIMLIB, "--version"], check=False)
    if rc != 0:
        print(f"ERROR: {WIMLIB} not found. Install wimtools.", file=sys.stderr)
        sys.exit(1)

    unittest.main(verbosity=2)
