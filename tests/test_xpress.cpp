extern "C" {
#include "xpress_huff.h"
}
#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include <cstdlib>

/* ================================================================
 * Compress helpers
 * ================================================================ */

TEST(Xpress, CompressEmpty)
{
    uint8_t out[16];
    uint32_t out_len = 0xDEAD;
    XpressStatus st = xpress_huff_compress(nullptr, 0, out, sizeof(out), &out_len);
    EXPECT_EQ(st, XPRESS_OK);
    EXPECT_EQ(out_len, 0u);
}

TEST(Xpress, CompressOneByte)
{
    uint8_t in = 0x42;
    std::vector<uint8_t> out(512);
    uint32_t out_len = 0;
    XpressStatus st = xpress_huff_compress(&in, 1, out.data(),
                                           (uint32_t)out.size(), &out_len);
    /* Single byte may or may not compress, both OK and BUFFER_TOO_SMALL are valid */
    EXPECT_TRUE(st == XPRESS_OK || st == XPRESS_BUFFER_TOO_SMALL);
}

/* ================================================================
 * Round-trip tests
 * ================================================================ */

static void roundtrip(const uint8_t* data, uint32_t len)
{
    /* Compress */
    std::vector<uint8_t> comp(len + 4096);
    uint32_t comp_len = 0;
    XpressStatus st = xpress_huff_compress(data, len, comp.data(),
                                           (uint32_t)comp.size(), &comp_len);
    if (st == XPRESS_BUFFER_TOO_SMALL) {
        /* Data is incompressible, skip round-trip check */
        return;
    }
    ASSERT_EQ(st, XPRESS_OK);
    ASSERT_GT(comp_len, 0u);

    /* Decompress */
    std::vector<uint8_t> decomp(len);
    st = xpress_huff_decompress(comp.data(), comp_len,
                                decomp.data(), len);
    ASSERT_EQ(st, XPRESS_OK);
    EXPECT_EQ(memcmp(data, decomp.data(), len), 0);
}

TEST(Xpress, RoundTrip_Zeros)
{
    std::vector<uint8_t> data(32768, 0);
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, RoundTrip_Random)
{
    std::vector<uint8_t> data(32768);
    srand(12345);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, RoundTrip_Repetitive)
{
    const char* pattern = "ABCDEFGHIJKLMNOP\n";
    size_t plen = strlen(pattern);
    std::vector<uint8_t> data(32768);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)pattern[i % plen];
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, RoundTrip_Mixed)
{
    std::vector<uint8_t> data(32768);
    /* First half: random */
    srand(54321);
    for (size_t i = 0; i < data.size() / 2; i++)
        data[i] = (uint8_t)(rand() & 0xFF);
    /* Second half: pattern */
    for (size_t i = data.size() / 2; i < data.size(); i++)
        data[i] = (uint8_t)(i & 0xFF);
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, RoundTrip_AllBytes)
{
    std::vector<uint8_t> data(32768);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)(i & 0xFF);
    roundtrip(data.data(), (uint32_t)data.size());
}

/* ================================================================
 * Incompressible data
 * ================================================================ */

TEST(Xpress, CompressIncompressible)
{
    /* Truly random data should not compress */
    std::vector<uint8_t> data(32768);
    srand(99999);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);

    std::vector<uint8_t> comp(data.size() + 4096);
    uint32_t comp_len = 0;
    XpressStatus st = xpress_huff_compress(data.data(), (uint32_t)data.size(),
                                           comp.data(), (uint32_t)comp.size(),
                                           &comp_len);
    EXPECT_EQ(st, XPRESS_BUFFER_TOO_SMALL);
}

/* ================================================================
 * Decompression error handling
 * ================================================================ */

TEST(Xpress, DecompressInvalidShort)
{
    /* Less than 256 bytes (Huffman table) -> BAD_DATA */
    uint8_t buf[200];
    memset(buf, 0, sizeof(buf));
    uint8_t out[256];
    XpressStatus st = xpress_huff_decompress(buf, sizeof(buf), out, sizeof(out));
    EXPECT_EQ(st, XPRESS_BAD_DATA);
}

TEST(Xpress, DecompressInvalidTable)
{
    /* 256 bytes of 0xFF means all code lengths 15 for all 512 symbols -
     * The decompressor may accept or reject this table depending on
     * implementation. Verify it does not crash. */
    uint8_t buf[512];
    memset(buf, 0xFF, 256);
    memset(buf + 256, 0, 256);
    uint8_t out[256];
    XpressStatus st = xpress_huff_decompress(buf, sizeof(buf), out, sizeof(out));
    /* Either BAD_DATA or OK is acceptable; the key is no crash */
    EXPECT_TRUE(st == XPRESS_BAD_DATA || st == XPRESS_OK);
}

TEST(Xpress, DecompressTruncated)
{
    /* Build a valid compressed buffer, then truncate the bitstream */
    const char* pattern = "ABCDEFGHIJKLMNOP\n";
    size_t plen = strlen(pattern);
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)pattern[i % plen];

    std::vector<uint8_t> comp(data.size() + 4096);
    uint32_t comp_len = 0;
    XpressStatus st = xpress_huff_compress(data.data(), (uint32_t)data.size(),
                                           comp.data(), (uint32_t)comp.size(),
                                           &comp_len);
    if (st != XPRESS_OK) return; /* skip if incompressible */

    /* Truncate: keep only Huffman table + a few bytes.
     * The decompressor may still produce output without error if
     * the partial bitstream happens to decode. Verify no crash. */
    uint32_t trunc_len = 256 + 4;
    uint8_t out[4096];
    st = xpress_huff_decompress(comp.data(), trunc_len, out, 4096);
    /* Either failure or success is acceptable; the key is no crash */
    (void)st;
}

/* ================================================================
 * Scratch allocation
 * ================================================================ */

TEST(Xpress, ScratchCreateDestroy)
{
    XpressCompressScratch* s = xpress_huff_create_scratch(65536);
    ASSERT_NE(s, nullptr);
    xpress_huff_destroy_scratch(s);
}

TEST(Xpress, ScratchDestroyNull)
{
    /* destroy(NULL) must not crash */
    xpress_huff_destroy_scratch(nullptr);
}

TEST(Xpress, CompressWithScratch)
{
    /* Compress with scratch should produce same output as without */
    const char* pattern = "ABCDEFGHIJKLMNOP\n";
    size_t plen = strlen(pattern);
    std::vector<uint8_t> data(8192);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)pattern[i % plen];

    /* Without scratch */
    std::vector<uint8_t> comp1(data.size() + 4096);
    uint32_t len1 = 0;
    XpressStatus st1 = xpress_huff_compress(data.data(), (uint32_t)data.size(),
                                            comp1.data(), (uint32_t)comp1.size(),
                                            &len1);

    /* With scratch */
    XpressCompressScratch* s = xpress_huff_create_scratch((uint32_t)data.size());
    ASSERT_NE(s, nullptr);
    std::vector<uint8_t> comp2(data.size() + 4096);
    uint32_t len2 = 0;
    XpressStatus st2 = xpress_huff_compress_with_scratch(
        data.data(), (uint32_t)data.size(),
        comp2.data(), (uint32_t)comp2.size(), &len2, s);
    xpress_huff_destroy_scratch(s);

    EXPECT_EQ(st1, st2);
    if (st1 == XPRESS_OK && st2 == XPRESS_OK) {
        EXPECT_EQ(len1, len2);
        EXPECT_EQ(memcmp(comp1.data(), comp2.data(), len1), 0);
    }
}

/* ================================================================
 * chunk_may_compress heuristic
 * ================================================================ */

TEST(Xpress, ChunkMayCompress_Random)
{
    /* Random data: likely_incompressible returns 1, so chunk_may_compress returns 0 */
    std::vector<uint8_t> data(32768);
    srand(77777);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);
    int result = xpress_huff_chunk_may_compress(data.data(), (uint32_t)data.size());
    /* Random data -> likely incompressible -> returns 0 (should not compress) */
    EXPECT_EQ(result, 0);
}

TEST(Xpress, ChunkMayCompress_Pattern)
{
    /* Repeating pattern with varied content - the heuristic detects
     * hash matches at distances > stride, indicating compressibility. */
    const char* pattern = "ABCDEFGHIJKLMNOP\n";
    size_t plen = strlen(pattern);
    std::vector<uint8_t> data(32768);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)pattern[i % plen];
    int result = xpress_huff_chunk_may_compress(data.data(), (uint32_t)data.size());
    EXPECT_NE(result, 0);
}

TEST(Xpress, ChunkMayCompress_Small)
{
    /* Data < 2048 bytes -> always returns non-zero (skip heuristic) */
    std::vector<uint8_t> data(1024);
    srand(11111);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);
    int result = xpress_huff_chunk_may_compress(data.data(), (uint32_t)data.size());
    EXPECT_NE(result, 0);
}

TEST(Xpress, ChunkMayCompress_AllZeros)
{
    /* All-zeros data must be detected as compressible (not incompressible).
     * Regression: the heuristic hash collision check used > instead of >=,
     * causing uniform data to be falsely classified as incompressible. */
    std::vector<uint8_t> data(32768, 0);
    int result = xpress_huff_chunk_may_compress(data.data(), (uint32_t)data.size());
    EXPECT_NE(result, 0);
}

TEST(Xpress, ChunkMayCompress_UniformByte)
{
    /* Any single-byte fill should be compressible */
    std::vector<uint8_t> data(32768, 0xAA);
    int result = xpress_huff_chunk_may_compress(data.data(), (uint32_t)data.size());
    EXPECT_NE(result, 0);
}

TEST(Xpress, RoundTrip_AllZeros_32K)
{
    /* All-zeros must compress and round-trip correctly */
    std::vector<uint8_t> data(32768, 0);
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, RoundTrip_SmallBuffer)
{
    /* Small buffers near h3 boundary (3 bytes needed for hash).
     * Regression: HC_INSERT had no bounds check, causing heap-buffer-overflow
     * when compressing buffers where pos+2 >= in_len. */
    for (uint32_t sz = 1; sz <= 16; sz++) {
        std::vector<uint8_t> data(sz);
        for (uint32_t i = 0; i < sz; i++)
            data[i] = (uint8_t)(i * 37);
        roundtrip(data.data(), sz);
    }
}

TEST(Xpress, RoundTrip_HighEntropy32K)
{
    /* 32KB of high-entropy data with all 256 byte values active and sparse
     * matches. This triggers the Huffman tree depth > MAXCL case where
     * the Kraft inequality repair can overflow if not properly validated.
     * Regression: compressor produced invalid Huffman codes for certain
     * binary PE data patterns, causing decompressor heap-buffer-overflow. */
    std::vector<uint8_t> data(32768);
    srand(0xDEAD);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);
    /* Inject sparse short matches to create many active match symbols */
    for (size_t i = 256; i < data.size() - 4; i += 97) {
        size_t src = (rand() % 200) + 3;
        if (src < i) { data[i] = data[src]; data[i+1] = data[src+1]; data[i+2] = data[src+2]; }
    }
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, KraftOverflow_ManySymbols)
{
    /* Create data that activates many of the 512 XPRESS symbols (256 literals
     * + many match length/offset combinations) to stress the Huffman tree
     * depth limit. The compressor must either produce valid codes or fall
     * back to raw storage -- never produce corrupt output. */
    std::vector<uint8_t> data(32768);
    srand(0xBEEF);
    for (auto& b : data) b = (uint8_t)(rand() & 0xFF);
    /* Many short matches at various offsets */
    for (size_t i = 64; i < data.size() - 8; i += 31) {
        size_t off = (rand() % 60) + 3;
        if (off < i) {
            for (int k = 0; k < 3 + (rand() % 5); k++)
                if (i + k < data.size()) data[i+k] = data[i - off + k];
        }
    }
    roundtrip(data.data(), (uint32_t)data.size());
}

/* ================================================================
 * Larger round-trip tests
 * ================================================================ */

TEST(Xpress, LargeRoundTrip_64KB)
{
    std::vector<uint8_t> data(65536);
    /* Compressible pattern */
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)((i * 7 + i / 256) & 0xFF);
    roundtrip(data.data(), (uint32_t)data.size());
}

TEST(Xpress, LargeRoundTrip_256KB)
{
    /* 256KB of compressible data */
    std::vector<uint8_t> data(256 * 1024);
    for (size_t i = 0; i < data.size(); i++)
        data[i] = (uint8_t)((i % 200) + (i / 1000));

    /* Process in 64KB chunks since XPRESS chunk size is 64KB max */
    uint32_t chunk_size = 65536;
    for (uint32_t off = 0; off < (uint32_t)data.size(); off += chunk_size) {
        uint32_t len = chunk_size;
        if (off + len > (uint32_t)data.size())
            len = (uint32_t)data.size() - off;
        roundtrip(data.data() + off, len);
    }
}

TEST(Xpress, VariousSizes)
{
    /* Loop through powers of 2 from 1 to 65536, compress+decompress */
    for (uint32_t sz = 1; sz <= 65536; sz *= 2) {
        std::vector<uint8_t> data(sz);
        /* Use a compressible pattern */
        for (uint32_t i = 0; i < sz; i++)
            data[i] = (uint8_t)(i & 0xFF);
        roundtrip(data.data(), sz);
    }
}
