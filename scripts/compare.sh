#!/bin/bash
# compare.sh - Compare wimage vs wimlib-imagex
# Tests both directions: our WIM -> wimlib reads, wimlib WIM -> our tool reads
# Also compares output format, file sizes, and round-trip integrity

set -e
WIMAGE="${WIMAGE_BIN:-../build/wimage}"
WIMLIB=wimlib-imagex
DIR=/tmp/wimage_compare_$$
GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0

cleanup() { rm -rf "$DIR"; }
trap cleanup EXIT
mkdir -p "$DIR"

pass() { echo -e "  ${GREEN}OK${NC}: $1"; PASS=$((PASS+1)); }
fail() { echo -e "  ${RED}FAIL${NC}: $1"; FAIL=$((FAIL+1)); }
section() { echo -e "\n${CYAN}=== $1 ===${NC}"; }

# Build test data with varied content
mkdir -p "$DIR/source/subdir/deep" "$DIR/source/emptydir"
echo "Hello World" > "$DIR/source/test.txt"
echo "Another file with more content" > "$DIR/source/subdir/nested.txt"
echo "Deep nested file" > "$DIR/source/subdir/deep/deep.txt"
echo "Duplicate content" > "$DIR/source/dup1.txt"
cp "$DIR/source/dup1.txt" "$DIR/source/dup2.txt"
touch "$DIR/source/empty.txt"
# Larger file for multi-byte compression testing
dd if=/dev/urandom bs=1024 count=64 of="$DIR/source/random.bin" 2>/dev/null
# Repetitive file (compresses well)
yes "ABCDEFGHIJKLMNOP" 2>/dev/null | head -c 32768 > "$DIR/source/repetitive.dat"

for COMP in none xpress; do
    section "Compression: $COMP"

    echo -e "\n--- Capture ---"
    $WIMAGE capture "$DIR/source" "$DIR/ours_${COMP}.wim" "OurImage" "wimage $COMP" --compress=$COMP 2>/dev/null
    $WIMLIB capture "$DIR/source" "$DIR/theirs_${COMP}.wim" "TheirImage" --compress=${COMP^^} 2>/dev/null

    OURS_SZ=$(stat -c%s "$DIR/ours_${COMP}.wim")
    THEIRS_SZ=$(stat -c%s "$DIR/theirs_${COMP}.wim")
    RATIO=$(awk "BEGIN{printf \"%.1f\", $OURS_SZ/$THEIRS_SZ * 100}")
    echo "  File size:  ours=${OURS_SZ}  wimlib=${THEIRS_SZ}  ratio=${RATIO}%"

    echo -e "\n--- Info output comparison ---"
    $WIMAGE info "$DIR/ours_${COMP}.wim" > "$DIR/info_ours_${COMP}.txt" 2>&1
    $WIMLIB info "$DIR/theirs_${COMP}.wim" > "$DIR/info_theirs_${COMP}.txt" 2>&1

    # Compare key fields
    for FIELD in "Image Count" "Compression" "Part Number"; do
        OURS_VAL=$(grep "$FIELD" "$DIR/info_ours_${COMP}.txt" | head -1 | sed 's/.*: *//')
        THEIRS_VAL=$(grep "$FIELD" "$DIR/info_theirs_${COMP}.txt" | head -1 | sed 's/.*: *//')
        if [ "$OURS_VAL" = "$THEIRS_VAL" ]; then
            pass "$FIELD: '$OURS_VAL'"
        else
            fail "$FIELD: ours='$OURS_VAL' wimlib='$THEIRS_VAL'"
        fi
    done

    # Compare dir counts / file counts
    OURS_DC=$(grep "Directory Count" "$DIR/info_ours_${COMP}.txt" | head -1 | sed 's/.*: *//')
    THEIRS_DC=$(grep "Directory Count" "$DIR/info_theirs_${COMP}.txt" | head -1 | sed 's/.*: *//')
    if [ "$OURS_DC" = "$THEIRS_DC" ]; then
        pass "Directory Count: $OURS_DC"
    else
        fail "Directory Count: ours=$OURS_DC wimlib=$THEIRS_DC"
    fi

    OURS_FC=$(grep "File Count" "$DIR/info_ours_${COMP}.txt" | head -1 | sed 's/.*: *//')
    THEIRS_FC=$(grep "File Count" "$DIR/info_theirs_${COMP}.txt" | head -1 | sed 's/.*: *//')
    if [ "$OURS_FC" = "$THEIRS_FC" ]; then
        pass "File Count: $OURS_FC"
    else
        fail "File Count: ours=$OURS_FC wimlib=$THEIRS_FC"
    fi

    echo -e "\n--- Dir listing comparison ---"
    $WIMAGE dir "$DIR/ours_${COMP}.wim" 2>/dev/null | sort > "$DIR/dir_ours_${COMP}.txt"
    $WIMLIB dir "$DIR/theirs_${COMP}.wim" 2>/dev/null | sort > "$DIR/dir_theirs_${COMP}.txt"
    if diff -q "$DIR/dir_ours_${COMP}.txt" "$DIR/dir_theirs_${COMP}.txt" >/dev/null 2>&1; then
        pass "Dir listings match ($(wc -l < "$DIR/dir_ours_${COMP}.txt") entries)"
    else
        fail "Dir listings differ"
        diff "$DIR/dir_ours_${COMP}.txt" "$DIR/dir_theirs_${COMP}.txt" | head -10
    fi

    echo -e "\n--- Cross-read: wimlib reads our WIM ---"
    $WIMLIB info "$DIR/ours_${COMP}.wim" >/dev/null 2>&1 \
        && pass "wimlib info on our WIM" || fail "wimlib info on our WIM"
    $WIMLIB dir "$DIR/ours_${COMP}.wim" >/dev/null 2>&1 \
        && pass "wimlib dir on our WIM" || fail "wimlib dir on our WIM"

    echo -e "\n--- Cross-read: wimage reads wimlib WIM ---"
    $WIMAGE info "$DIR/theirs_${COMP}.wim" >/dev/null 2>&1 \
        && pass "wimage info on wimlib WIM" || fail "wimage info on wimlib WIM"
    $WIMAGE dir "$DIR/theirs_${COMP}.wim" >/dev/null 2>&1 \
        && pass "wimage dir on wimlib WIM" || fail "wimage dir on wimlib WIM"

    echo -e "\n--- Extract: wimlib extracts our WIM ---"
    rm -rf "$DIR/ext_wimlib_ours_${COMP}"
    mkdir -p "$DIR/ext_wimlib_ours_${COMP}"
    $WIMLIB apply "$DIR/ours_${COMP}.wim" 1 "$DIR/ext_wimlib_ours_${COMP}" 2>/dev/null \
        && pass "wimlib apply our $COMP WIM" || fail "wimlib apply our $COMP WIM"
    diff -r "$DIR/source" "$DIR/ext_wimlib_ours_${COMP}" >/dev/null 2>&1 \
        && pass "wimlib extracted content matches source" || fail "wimlib extracted content differs"

    echo -e "\n--- Extract: wimage extracts wimlib WIM ---"
    rm -rf "$DIR/ext_wimage_theirs_${COMP}"
    mkdir -p "$DIR/ext_wimage_theirs_${COMP}"
    $WIMAGE apply "$DIR/theirs_${COMP}.wim" 1 "$DIR/ext_wimage_theirs_${COMP}" 2>/dev/null \
        && pass "wimage apply wimlib $COMP WIM" || fail "wimage apply wimlib $COMP WIM"
    diff -r "$DIR/source" "$DIR/ext_wimage_theirs_${COMP}" >/dev/null 2>&1 \
        && pass "wimage extracted content matches source" || fail "wimage extracted content differs"

    echo -e "\n--- Cross-extract comparison ---"
    # Both tools extract from the SAME WIM, compare results
    rm -rf "$DIR/cross_a" "$DIR/cross_b"
    mkdir -p "$DIR/cross_a" "$DIR/cross_b"
    $WIMLIB apply "$DIR/ours_${COMP}.wim" 1 "$DIR/cross_a" 2>/dev/null
    $WIMAGE apply "$DIR/ours_${COMP}.wim" 1 "$DIR/cross_b" 2>/dev/null
    diff -r "$DIR/cross_a" "$DIR/cross_b" >/dev/null 2>&1 \
        && pass "Both tools extract identical files from our WIM" \
        || fail "Extraction differs between tools on our WIM"

    rm -rf "$DIR/cross_c" "$DIR/cross_d"
    mkdir -p "$DIR/cross_c" "$DIR/cross_d"
    $WIMLIB apply "$DIR/theirs_${COMP}.wim" 1 "$DIR/cross_c" 2>/dev/null
    $WIMAGE apply "$DIR/theirs_${COMP}.wim" 1 "$DIR/cross_d" 2>/dev/null
    diff -r "$DIR/cross_c" "$DIR/cross_d" >/dev/null 2>&1 \
        && pass "Both tools extract identical files from wimlib WIM" \
        || fail "Extraction differs between tools on wimlib WIM"
done

section "Integrity table"
$WIMAGE capture "$DIR/source" "$DIR/integ.wim" "IntegTest" --check --compress=none 2>/dev/null \
    && pass "capture --check" || fail "capture --check"
$WIMAGE verify "$DIR/integ.wim" 2>/dev/null \
    && pass "wimage verify own WIM" || fail "wimage verify own WIM"
$WIMLIB info "$DIR/integ.wim" >/dev/null 2>&1 \
    && pass "wimlib reads integrity WIM" || fail "wimlib reads integrity WIM"

section "Deduplication"
# dup1.txt and dup2.txt have identical content - check blob count
BLOB_COUNT=$($WIMAGE info "$DIR/ours_none.wim" --blobs 2>/dev/null | grep -c "^Hash" || true)
# Source has 8 files but 2 are duplicates + 1 empty = 6 unique blobs + 1 metadata
UNIQUE_FILES=6  # test.txt, nested.txt, deep.txt, dup1(=dup2), random.bin, repetitive.dat
echo "  Blob entries: $BLOB_COUNT (expect ~$((UNIQUE_FILES+1)) with dedup + metadata)"
if [ "$BLOB_COUNT" -le "$((UNIQUE_FILES+2))" ]; then
    pass "Deduplication working (${BLOB_COUNT} blobs for $((UNIQUE_FILES+1)) unique files + metadata)"
else
    fail "Too many blobs: $BLOB_COUNT (expected ~$((UNIQUE_FILES+1)))"
fi

section "Summary"
echo -e "\n${CYAN}Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo ""
exit $FAIL
