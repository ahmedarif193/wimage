/*
 * test_wim_readwrite.cpp - Unit tests for WIM create/open/capture/extract round-trips
 */

/* Map C11 _Static_assert to C++ static_assert for shared headers */
#define _Static_assert static_assert

extern "C" {
#include "wim_io.h"
#include "wim_read.h"
#include "wim_write.h"
#include "wim_capture.h"
#include "wim_types.h"
#include "sha1.h"
}
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <sys/stat.h>
#include <unistd.h>

/* ================================================================
 *  Test fixture with temp directory management
 * ================================================================ */

class WimTestFixture : public ::testing::Test {
protected:
    char tmpdir[256];

    void SetUp() override {
        snprintf(tmpdir, sizeof(tmpdir), "/tmp/wimage_test_XXXXXX");
        ASSERT_NE(nullptr, mkdtemp(tmpdir));
    }

    void TearDown() override {
        char cmd[300];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
        system(cmd);
    }

    std::string path(const char* rel) {
        return std::string(tmpdir) + "/" + rel;
    }

    void write_file(const char* rel, const void* data, size_t len) {
        std::string p = path(rel);
        std::string dir = p.substr(0, p.rfind('/'));
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", dir.c_str());
        system(cmd);
        FILE* f = fopen(p.c_str(), "wb");
        if (f) { fwrite(data, 1, len, f); fclose(f); }
    }

    std::string read_file(const char* rel) {
        FILE* f = fopen(path(rel).c_str(), "rb");
        if (!f) return "";
        fseek(f, 0, SEEK_END);
        long sz = ftell(f);
        fseek(f, 0, SEEK_SET);
        std::string s(sz, 0);
        fread(&s[0], 1, sz, f);
        fclose(f);
        return s;
    }

    void make_source_dir(const char* rel) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", path(rel).c_str());
        system(cmd);
    }

    /* Compute SHA-1 of a file for comparison */
    void sha1_of_file(const char* rel, uint8_t digest[20]) {
        std::string content = read_file(rel);
        sha1_hash((const uint8_t*)content.data(), (uint32_t)content.size(), digest);
    }
};

/* ================================================================
 *  Basic create / open tests
 * ================================================================ */

TEST_F(WimTestFixture, CreateEmpty)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    /* Reopen and verify header */
    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    EXPECT_EQ(0u, ctx2.image_count);
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, CreateAndReopen)
{
    make_source_dir("src");
    write_file("src/hello.txt", "hello\n", 6);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Image1", "desc"));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    EXPECT_EQ(1u, ctx2.image_count);
    wim_ctx_free(&ctx);
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, CaptureTreeSyncWriteFailureStopsCleanly)
{
    make_source_dir("src");
    write_file("src/file.txt", "data", 4);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));

    FILE* failing = fopen("/dev/full", "w+b");
    ASSERT_NE(nullptr, failing);
    fclose(ctx.file);
    ctx.file = failing;
    setvbuf(ctx.file, NULL, _IONBF, 0);

    EXPECT_EQ(-1, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    wim_ctx_free(&ctx);
}

TEST_F(WimTestFixture, CaptureTreeAsyncWriteFailureStopsCleanly)
{
    make_source_dir("src");
    char payload[1024];
    memset(payload, 'A', sizeof(payload));
    for (int i = 0; i < 128; i++) {
        char rel[64];
        snprintf(rel, sizeof(rel), "src/file_%03d.bin", i);
        write_file(rel, payload, sizeof(payload));
    }

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ctx.num_threads = 4;
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));

    FILE* failing = fopen("/dev/full", "w+b");
    ASSERT_NE(nullptr, failing);
    fclose(ctx.file);
    ctx.file = failing;
    setvbuf(ctx.file, NULL, _IONBF, 0);

    int ret = wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL);
    if (ret == 0)
        EXPECT_EQ(-1, wim_finalize(&ctx, 0));
    else
        EXPECT_EQ(-1, ret);
    wim_ctx_free(&ctx);
}

/* ================================================================
 *  Round-trip tests (uncompressed)
 * ================================================================ */

TEST_F(WimTestFixture, RoundTrip_Uncompressed_SingleFile)
{
    make_source_dir("src");
    write_file("src/hello.txt", "hello\n", 6);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));
    EXPECT_EQ("hello\n", read_file("out/hello.txt"));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, RoundTrip_Uncompressed_MultiFile)
{
    make_source_dir("src");
    const char* names[] = { "a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };
    const char* data[]  = { "aaa",   "bbb",   "ccc",   "ddd",   "eee"   };

    for (int i = 0; i < 5; i++) {
        std::string rel = std::string("src/") + names[i];
        write_file(rel.c_str(), data[i], strlen(data[i]));
    }

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    for (int i = 0; i < 5; i++) {
        std::string rel = std::string("out/") + names[i];
        uint8_t orig_sha[20], ext_sha[20];
        sha1_hash((const uint8_t*)data[i], (uint32_t)strlen(data[i]), orig_sha);
        std::string content = read_file(rel.c_str());
        sha1_hash((const uint8_t*)content.data(), (uint32_t)content.size(), ext_sha);
        EXPECT_EQ(0, memcmp(orig_sha, ext_sha, 20)) << "SHA-1 mismatch for " << names[i];
    }
    wim_close(&ctx2);
}

/* ================================================================
 *  Round-trip tests (xpress)
 * ================================================================ */

TEST_F(WimTestFixture, RoundTrip_Xpress_SingleFile)
{
    make_source_dir("src");
    /* Repeating data to give xpress something to compress */
    char buf[1024];
    memset(buf, 'X', sizeof(buf));
    write_file("src/data.bin", buf, sizeof(buf));

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/data.bin");
    ASSERT_EQ(sizeof(buf), content.size());
    EXPECT_EQ(0, memcmp(buf, content.data(), sizeof(buf)));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, RoundTrip_Xpress_MultiFile)
{
    make_source_dir("src");
    const char* names[] = { "a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };

    for (int i = 0; i < 5; i++) {
        char buf[256];
        memset(buf, 'A' + i, sizeof(buf));
        std::string rel = std::string("src/") + names[i];
        write_file(rel.c_str(), buf, sizeof(buf));
    }

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    for (int i = 0; i < 5; i++) {
        char expected[256];
        memset(expected, 'A' + i, sizeof(expected));
        std::string rel = std::string("out/") + names[i];
        std::string content = read_file(rel.c_str());
        ASSERT_EQ(sizeof(expected), content.size()) << "Size mismatch for " << names[i];
        EXPECT_EQ(0, memcmp(expected, content.data(), sizeof(expected)))
            << "Content mismatch for " << names[i];
    }
    wim_close(&ctx2);
}

/* ================================================================
 *  Edge cases
 * ================================================================ */

TEST_F(WimTestFixture, RoundTrip_EmptyFile)
{
    make_source_dir("src");
    write_file("src/empty.txt", "", 0);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/empty.txt");
    EXPECT_EQ(0u, content.size());
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, RoundTrip_EmptyDir)
{
    make_source_dir("src/subdir");

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    struct stat st;
    ASSERT_EQ(0, stat(path("out/subdir").c_str(), &st));
    EXPECT_TRUE(S_ISDIR(st.st_mode));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, RoundTrip_DeepNesting)
{
    /* Create 10 levels deep: src/d0/d1/.../d9/leaf.txt */
    std::string dir = "src";
    for (int i = 0; i < 10; i++) {
        char level[16];
        snprintf(level, sizeof(level), "/d%d", i);
        dir += level;
    }
    make_source_dir(dir.c_str());
    std::string leaf = dir + "/leaf.txt";
    write_file(leaf.c_str(), "deep", 4);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    /* Build expected extracted path */
    std::string out_leaf = "out";
    for (int i = 0; i < 10; i++) {
        char level[16];
        snprintf(level, sizeof(level), "/d%d", i);
        out_leaf += level;
    }
    out_leaf += "/leaf.txt";
    EXPECT_EQ("deep", read_file(out_leaf.c_str()));
    wim_close(&ctx2);
}

/* ================================================================
 *  Deduplication
 * ================================================================ */

TEST_F(WimTestFixture, Deduplication)
{
    make_source_dir("src");
    const char* content = "identical content for dedup test";
    size_t len = strlen(content);
    write_file("src/file1.txt", content, len);
    write_file("src/file2.txt", content, len);
    write_file("src/file3.txt", content, len);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));

    /* Count non-metadata blobs: should be 1 (deduplication) */
    size_t data_blobs = 0;
    for (size_t i = 0; i < ctx.blob_count; i++) {
        if (!(ctx.blobs[i].flags & WIM_RESHDR_FLAG_METADATA))
            data_blobs++;
    }
    EXPECT_EQ(1u, data_blobs);

    ASSERT_EQ(0, wim_finalize(&ctx, 0));
}

/* ================================================================
 *  Integrity table
 * ================================================================ */

TEST_F(WimTestFixture, Integrity_CreateAndVerify)
{
    make_source_dir("src");
    write_file("src/data.txt", "integrity test data", 19);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 1));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    EXPECT_EQ(0, wim_verify_integrity(&ctx2));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, Integrity_Corruption)
{
    make_source_dir("src");
    write_file("src/data.txt", "integrity corruption test", 25);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 1));

    /* Flip a byte in the WIM body (after header, before integrity) */
    FILE* f = fopen(path("test.wim").c_str(), "r+b");
    ASSERT_NE(nullptr, f);
    fseek(f, WIM_HEADER_SIZE + 10, SEEK_SET);
    uint8_t byte;
    fread(&byte, 1, 1, f);
    byte ^= 0xFF;
    fseek(f, WIM_HEADER_SIZE + 10, SEEK_SET);
    fwrite(&byte, 1, 1, f);
    fclose(f);

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    EXPECT_EQ(-1, wim_verify_integrity(&ctx2));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, Integrity_XpressRoundTrip)
{
    /* Integrity with XPRESS compression: verifies hash range excludes XML. */
    make_source_dir("src");
    char buf[40000];
    for (size_t i = 0; i < sizeof(buf); i++)
        buf[i] = (char)(i % 251);
    write_file("src/data.bin", buf, sizeof(buf));

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 1));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    EXPECT_EQ(0, wim_verify_integrity(&ctx2));

    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);
    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/data.bin");
    ASSERT_EQ(sizeof(buf), content.size());
    EXPECT_EQ(0, memcmp(buf, content.data(), sizeof(buf)));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, RoundTrip_AllZerosFile)
{
    /* All-zeros file: tests XPRESS compression of uniform data and
     * the raw fallback when compression doesn't save space. */
    make_source_dir("src");
    std::vector<char> zeros(100000, 0);
    write_file("src/zeros.bin", zeros.data(), zeros.size());

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);
    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/zeros.bin");
    ASSERT_EQ(zeros.size(), content.size());
    EXPECT_EQ(0, memcmp(zeros.data(), content.data(), zeros.size()));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, CompressionFallback_IncompressibleMultiChunk)
{
    /* Random data spanning multiple chunks: compressed output should not
     * be larger than raw. The blob should be stored uncompressed. */
    make_source_dir("src");
    std::vector<uint8_t> rnd(65536);
    srand(42);
    for (auto& b : rnd) b = (uint8_t)(rand() & 0xFF);
    write_file("src/random.bin", rnd.data(), rnd.size());

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));

    /* Find the data blob - its compressed_size should equal original_size
     * (stored raw) because random data is incompressible. */
    for (size_t i = 0; i < ctx.blob_count; i++) {
        if (ctx.blobs[i].flags & WIM_RESHDR_FLAG_METADATA) continue;
        EXPECT_LE(ctx.blobs[i].compressed_size, ctx.blobs[i].original_size)
            << "Compressed size must not exceed original for incompressible data";
    }

    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    /* Verify round-trip */
    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);
    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/random.bin");
    ASSERT_EQ(rnd.size(), content.size());
    EXPECT_EQ(0, memcmp(rnd.data(), content.data(), rnd.size()));
    wim_close(&ctx2);
}

/* ================================================================
 *  Error handling
 * ================================================================ */

TEST_F(WimTestFixture, OpenInvalidMagic)
{
    char garbage[256];
    memset(garbage, 'X', sizeof(garbage));
    write_file("bad.wim", garbage, sizeof(garbage));

    WimCtx ctx;
    wim_ctx_init(&ctx);
    EXPECT_EQ(-1, wim_open(&ctx, path("bad.wim").c_str()));
    wim_close(&ctx);
}

TEST_F(WimTestFixture, OpenTruncated)
{
    char data[100];
    memset(data, 0, sizeof(data));
    write_file("trunc.wim", data, sizeof(data));

    WimCtx ctx;
    wim_ctx_init(&ctx);
    EXPECT_EQ(-1, wim_open(&ctx, path("trunc.wim").c_str()));
    wim_close(&ctx);
}

TEST_F(WimTestFixture, OpenNonexistent)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);
    EXPECT_EQ(-1, wim_open(&ctx, path("does_not_exist.wim").c_str()));
    wim_close(&ctx);
}

TEST_F(WimTestFixture, SelectImage_OutOfRange)
{
    make_source_dir("src");
    write_file("src/f.txt", "x", 1);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));

    /* Index 0 is out of range (1-based) */
    EXPECT_EQ(-1, wim_select_image(&ctx2, 0));
    /* Index > image_count is out of range */
    EXPECT_EQ(-1, wim_select_image(&ctx2, (int)ctx2.image_count + 1));
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, GetRoot_BeforeSelect)
{
    make_source_dir("src");
    write_file("src/f.txt", "x", 1);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));

    /* Without calling wim_select_image, get_root returns NULL */
    const WimDentry* root = wim_get_root(&ctx2, 1);
    EXPECT_EQ(nullptr, root);
    wim_close(&ctx2);
}

TEST_F(WimTestFixture, ReadBlob_UnknownSha1)
{
    make_source_dir("src");
    write_file("src/f.txt", "x", 1);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 0));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));

    uint8_t fake_sha[20];
    memset(fake_sha, 0xAB, 20);
    uint8_t* data = NULL;
    size_t data_size = 0;
    EXPECT_EQ(-1, wim_read_blob(&ctx2, fake_sha, &data, &data_size));
    wim_close(&ctx2);
}

/* ================================================================
 *  Multi-thread determinism
 * ================================================================ */

TEST_F(WimTestFixture, MultiThread_SameOutput)
{
    make_source_dir("src");
    /* Create a file large enough to have multiple chunks (>32k) */
    char buf[40000];
    for (size_t i = 0; i < sizeof(buf); i++)
        buf[i] = (char)(i % 251);
    write_file("src/data.bin", buf, sizeof(buf));

    /* Capture with 1 thread */
    {
        WimCtx ctx;
        wim_ctx_init(&ctx);
        ctx.num_threads = 1;
        ASSERT_EQ(0, wim_create(&ctx, path("wim1.wim").c_str(), 1));
        ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
        ASSERT_EQ(0, wim_finalize(&ctx, 0));
    }

    /* Capture with 4 threads */
    {
        WimCtx ctx;
        wim_ctx_init(&ctx);
        ctx.num_threads = 4;
        ASSERT_EQ(0, wim_create(&ctx, path("wim4.wim").c_str(), 1));
        ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
        ASSERT_EQ(0, wim_finalize(&ctx, 0));
    }

    /* Both should extract to identical content */
    WimCtx ctx1, ctx4;
    wim_ctx_init(&ctx1);
    wim_ctx_init(&ctx4);
    ASSERT_EQ(0, wim_open(&ctx1, path("wim1.wim").c_str()));
    ASSERT_EQ(0, wim_open(&ctx4, path("wim4.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx1, 1));
    ASSERT_EQ(0, wim_select_image(&ctx4, 1));

    const WimDentry* r1 = wim_get_root(&ctx1, 1);
    const WimDentry* r4 = wim_get_root(&ctx4, 1);
    ASSERT_NE(nullptr, r1);
    ASSERT_NE(nullptr, r4);

    make_source_dir("out1");
    make_source_dir("out4");
    ASSERT_EQ(0, wim_extract_tree(&ctx1, r1, path("out1").c_str()));
    ASSERT_EQ(0, wim_extract_tree(&ctx4, r4, path("out4").c_str()));

    std::string c1 = read_file("out1/data.bin");
    std::string c4 = read_file("out4/data.bin");
    ASSERT_EQ(c1.size(), c4.size());
    EXPECT_EQ(0, memcmp(c1.data(), c4.data(), c1.size()));

    wim_close(&ctx1);
    wim_close(&ctx4);
}

/* ================================================================
 *  Large file round-trip
 * ================================================================ */

TEST_F(WimTestFixture, LargeFile_RoundTrip)
{
    make_source_dir("src");
    size_t size = 1024 * 1024; /* 1 MB */
    uint8_t* big = (uint8_t*)malloc(size);
    ASSERT_NE(nullptr, big);
    for (size_t i = 0; i < size; i++)
        big[i] = (uint8_t)(i * 37 + i / 256);
    write_file("src/big.bin", big, size);

    uint8_t orig_sha[20];
    sha1_hash(big, (uint32_t)size, orig_sha);

    WimCtx ctx;
    wim_ctx_init(&ctx);
    ASSERT_EQ(0, wim_create(&ctx, path("test.wim").c_str(), 1));
    ASSERT_EQ(0, wim_capture_tree(&ctx, path("src").c_str(), "Img", NULL));
    ASSERT_EQ(0, wim_finalize(&ctx, 0));

    WimCtx ctx2;
    wim_ctx_init(&ctx2);
    ASSERT_EQ(0, wim_open(&ctx2, path("test.wim").c_str()));
    ASSERT_EQ(0, wim_select_image(&ctx2, 1));
    const WimDentry* root = wim_get_root(&ctx2, 1);
    ASSERT_NE(nullptr, root);

    make_source_dir("out");
    ASSERT_EQ(0, wim_extract_tree(&ctx2, root, path("out").c_str()));

    std::string content = read_file("out/big.bin");
    ASSERT_EQ(size, content.size());

    uint8_t ext_sha[20];
    sha1_hash((const uint8_t*)content.data(), (uint32_t)content.size(), ext_sha);
    EXPECT_EQ(0, memcmp(orig_sha, ext_sha, 20));

    free(big);
    wim_close(&ctx2);
}
