#!/bin/bash
set -e
WIMAGE="${WIMAGE_BIN:-../build/wimage}"
TESTDIR=/tmp/wimage_test_$$
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
PASS=0
FAIL=0

cleanup() { rm -rf "$TESTDIR"; }
trap cleanup EXIT

pass() { echo -e "${GREEN}PASS${NC}: $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}FAIL${NC}: $1 - $2"; FAIL=$((FAIL+1)); }

mkdir -p "$TESTDIR/source/subdir/deep"
echo "Hello World" > "$TESTDIR/source/test.txt"
echo "Another file" > "$TESTDIR/source/subdir/nested.txt"
echo "Deep file" > "$TESTDIR/source/subdir/deep/deep.txt"
echo "Duplicate" > "$TESTDIR/source/dup1.txt"
cp "$TESTDIR/source/dup1.txt" "$TESTDIR/source/dup2.txt"  # dedup test
touch "$TESTDIR/source/empty.txt"  # zero-length file
mkdir -p "$TESTDIR/source/emptydir"  # empty directory

echo "=== wimage Test Suite ==="
echo ""

# Test 1: Capture with no compression
echo "--- Test 1: Capture (no compression) ---"
$WIMAGE capture "$TESTDIR/source" "$TESTDIR/none.wim" "TestNone" "No compression test" --compress=none && pass "capture --compress=none" || fail "capture --compress=none" "exit code $?"

# Test 2: Our WIM readable by wimlib
echo "--- Test 2: wimlib-imagex reads our WIM ---"
wimlib-imagex info "$TESTDIR/none.wim" > /dev/null 2>&1 && pass "wimlib reads our none.wim" || fail "wimlib reads our none.wim" "wimlib-imagex info failed"
wimlib-imagex dir "$TESTDIR/none.wim" > /dev/null 2>&1 && pass "wimlib dir our none.wim" || fail "wimlib dir our none.wim" "wimlib-imagex dir failed"

# Test 3: wimlib extracts our WIM correctly
echo "--- Test 3: wimlib extracts our WIM ---"
mkdir -p "$TESTDIR/out_wimlib"
wimlib-imagex apply "$TESTDIR/none.wim" 1 "$TESTDIR/out_wimlib" 2>/dev/null && pass "wimlib apply our none.wim" || fail "wimlib apply our none.wim" "wimlib-imagex apply failed"
diff -r "$TESTDIR/source" "$TESTDIR/out_wimlib" > /dev/null 2>&1 && pass "wimlib extract matches source" || fail "wimlib extract matches source" "files differ"

# Test 4: Capture with XPRESS compression
echo "--- Test 4: Capture (XPRESS compression) ---"
$WIMAGE capture "$TESTDIR/source" "$TESTDIR/xpress.wim" "TestXpress" "XPRESS test" --compress=xpress && pass "capture --compress=xpress" || fail "capture --compress=xpress" "exit code $?"

# Test 5: wimlib reads XPRESS WIM
echo "--- Test 5: wimlib reads our XPRESS WIM ---"
wimlib-imagex info "$TESTDIR/xpress.wim" > /dev/null 2>&1 && pass "wimlib reads our xpress.wim" || fail "wimlib reads our xpress.wim" "wimlib-imagex info failed"
mkdir -p "$TESTDIR/out_xpress"
wimlib-imagex apply "$TESTDIR/xpress.wim" 1 "$TESTDIR/out_xpress" 2>/dev/null && pass "wimlib apply our xpress.wim" || fail "wimlib apply our xpress.wim" "failed"
diff -r "$TESTDIR/source" "$TESTDIR/out_xpress" > /dev/null 2>&1 && pass "xpress extract matches source" || fail "xpress extract matches source" "files differ"

# Test 6: wimlib creates WIM, our tool reads it
echo "--- Test 6: Read wimlib-created WIM ---"
wimlib-imagex capture "$TESTDIR/source" "$TESTDIR/ref.wim" "RefImage" --compress=none 2>/dev/null
$WIMAGE info "$TESTDIR/ref.wim" > /dev/null 2>&1 && pass "wimage info wimlib WIM" || fail "wimage info wimlib WIM" "info failed"
$WIMAGE dir "$TESTDIR/ref.wim" > /dev/null 2>&1 && pass "wimage dir wimlib WIM" || fail "wimage dir wimlib WIM" "dir failed"

# Test 7: Our tool extracts wimlib WIM
echo "--- Test 7: Extract wimlib-created WIM ---"
mkdir -p "$TESTDIR/out_wimage"
$WIMAGE apply "$TESTDIR/ref.wim" 1 "$TESTDIR/out_wimage" 2>/dev/null && pass "wimage apply wimlib WIM" || fail "wimage apply wimlib WIM" "apply failed"
diff -r "$TESTDIR/source" "$TESTDIR/out_wimage" > /dev/null 2>&1 && pass "wimage extract matches source" || fail "wimage extract matches source" "files differ"

# Test 8: Read XPRESS-compressed wimlib WIM
echo "--- Test 8: Read XPRESS wimlib WIM ---"
wimlib-imagex capture "$TESTDIR/source" "$TESTDIR/ref_xpress.wim" "RefXpress" --compress=XPRESS 2>/dev/null
mkdir -p "$TESTDIR/out_wimage_xp"
$WIMAGE apply "$TESTDIR/ref_xpress.wim" 1 "$TESTDIR/out_wimage_xp" 2>/dev/null && pass "wimage apply XPRESS wimlib WIM" || fail "wimage apply XPRESS wimlib WIM" "apply failed"
diff -r "$TESTDIR/source" "$TESTDIR/out_wimage_xp" > /dev/null 2>&1 && pass "XPRESS extract matches source" || fail "XPRESS extract matches source" "files differ"

# Test 9: Info output
echo "--- Test 9: Info command ---"
$WIMAGE info "$TESTDIR/none.wim" 2>&1 | grep -q "Image Count:" && pass "info shows image count" || fail "info shows image count" "missing"
$WIMAGE info "$TESTDIR/none.wim" --header 2>&1 | grep -q "Magic" && pass "info --header" || fail "info --header" "missing"
$WIMAGE info "$TESTDIR/none.wim" --xml 2>&1 | grep -q "WIM" && pass "info --xml" || fail "info --xml" "missing"

# Test 10: Dir output
echo "--- Test 10: Dir command ---"
$WIMAGE dir "$TESTDIR/none.wim" 2>&1 | grep -q "test.txt" && pass "dir lists files" || fail "dir lists files" "missing"
$WIMAGE dir "$TESTDIR/none.wim" --detailed 2>&1 | grep -q "Attributes" && pass "dir --detailed" || fail "dir --detailed" "missing"

# Test 11: Integrity check
echo "--- Test 11: Integrity ---"
$WIMAGE capture "$TESTDIR/source" "$TESTDIR/integrity.wim" "IntTest" --check --compress=none && pass "capture --check" || fail "capture --check" "exit code $?"
$WIMAGE verify "$TESTDIR/integrity.wim" && pass "verify passes" || fail "verify passes" "verification failed"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL
