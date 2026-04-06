/*
 * test_wim_capture.cpp - Unit tests for the directory capture walker
 */

/* Map C11 _Static_assert to C++ static_assert for shared headers */
#define _Static_assert static_assert

extern "C" {
#include "wim_io.h"
#include "wim_capture.h"
#include "wim_types.h"
#include "sha1.h"
}
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

/* ================================================================
 *  Mock blob writer
 * ================================================================ */

struct MockWriter {
    std::vector<std::vector<uint8_t>> blobs;
    int fail_at = -1; /* -1 = never fail */

    static int callback(uint8_t* data, uint64_t size,
                        void (*free_fn)(void*, size_t), void* free_arg,
                        uint8_t sha1_out[20], void* user)
    {
        MockWriter* self = (MockWriter*)user;
        if (self->fail_at == (int)self->blobs.size())
            return -1; /* caller will free */
        self->blobs.push_back(std::vector<uint8_t>(data, data + size));
        sha1_hash(data, (uint32_t)size, sha1_out);
        if (free_fn)
            free_fn(free_arg, (size_t)size);
        return 0;
    }
};

/* ================================================================
 *  Test fixture
 * ================================================================ */

class WimCaptureFixture : public ::testing::Test {
protected:
    char tmpdir[256];

    void SetUp() override {
        snprintf(tmpdir, sizeof(tmpdir), "/tmp/wimage_cap_XXXXXX");
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

    void make_dir(const char* rel) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", path(rel).c_str());
        system(cmd);
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
};

/* ================================================================
 *  Tests
 * ================================================================ */

TEST_F(WimCaptureFixture, CaptureEmptyDir)
{
    make_dir("src");

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));
    EXPECT_EQ(0u, root.child_count);
    EXPECT_TRUE(root.attributes & WIM_FILE_ATTRIBUTE_DIRECTORY);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureSingleFile)
{
    make_dir("src");
    write_file("src/hello.txt", "hello", 5);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));
    ASSERT_EQ(1u, root.child_count);
    EXPECT_STREQ("hello.txt", root.children[0].name_utf8);
    EXPECT_TRUE(root.children[0].attributes & WIM_FILE_ATTRIBUTE_ARCHIVE);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureNestedDirs)
{
    make_dir("src/sub");
    write_file("src/sub/file.txt", "data", 4);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));

    /* root -> sub -> file.txt */
    ASSERT_EQ(1u, root.child_count);
    const WimDentry* sub = &root.children[0];
    EXPECT_STREQ("sub", sub->name_utf8);
    EXPECT_TRUE(sub->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY);

    ASSERT_EQ(1u, sub->child_count);
    const WimDentry* file = &sub->children[0];
    EXPECT_STREQ("file.txt", file->name_utf8);
    EXPECT_TRUE(file->attributes & WIM_FILE_ATTRIBUTE_ARCHIVE);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureMultipleFiles)
{
    make_dir("src");
    const char* names[] = { "a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };
    for (int i = 0; i < 5; i++) {
        std::string rel = std::string("src/") + names[i];
        write_file(rel.c_str(), "x", 1);
    }

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));
    EXPECT_EQ(5u, root.child_count);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureSortedChildren)
{
    make_dir("src");
    /* Create files in non-alphabetical order */
    write_file("src/zebra.txt", "z", 1);
    write_file("src/apple.txt", "a", 1);
    write_file("src/mango.txt", "m", 1);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));
    ASSERT_EQ(3u, root.child_count);

    /* Children should be sorted alphabetically */
    EXPECT_STREQ("apple.txt", root.children[0].name_utf8);
    EXPECT_STREQ("mango.txt", root.children[1].name_utf8);
    EXPECT_STREQ("zebra.txt", root.children[2].name_utf8);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureTimestamps)
{
    make_dir("src");
    write_file("src/file.txt", "test", 4);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));
    /* Root and child should have non-zero timestamps */
    EXPECT_NE(0u, root.creation_time);
    ASSERT_EQ(1u, root.child_count);
    EXPECT_NE(0u, root.children[0].creation_time);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureEmptyFileNoBlob)
{
    make_dir("src");
    write_file("src/empty.txt", "", 0);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));

    /* Empty file should not trigger the blob writer */
    EXPECT_EQ(0u, writer.blobs.size());

    ASSERT_EQ(1u, root.child_count);
    const WimDentry* child = &root.children[0];

    /* SHA-1 should be all zeros for empty file */
    uint8_t zero_sha[20] = {0};
    EXPECT_EQ(0, memcmp(child->sha1, zero_sha, 20));
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, MockWriterFails)
{
    make_dir("src");
    write_file("src/file.txt", "data", 4);

    WimDentry root;
    MockWriter writer;
    writer.fail_at = 0; /* Fail on first blob */

    int ret = wim_capture_dir(path("src").c_str(), &root,
                               MockWriter::callback, &writer);
    EXPECT_EQ(-1, ret);
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, MockWriterReceivesContent)
{
    make_dir("src");
    const char* content = "test payload data";
    size_t len = strlen(content);
    write_file("src/payload.txt", content, len);

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));

    /* Writer should have received exactly 1 blob with the file content */
    ASSERT_EQ(1u, writer.blobs.size());
    ASSERT_EQ(len, writer.blobs[0].size());
    EXPECT_EQ(0, memcmp(content, writer.blobs[0].data(), len));
    wim_dentry_free(&root);
}

TEST_F(WimCaptureFixture, CaptureSkipsSymlinks)
{
    make_dir("src");
    write_file("src/real.txt", "data", 4);

    /* Create a symlink */
    std::string link_path = path("src/link.txt");
    std::string target = path("src/real.txt");
    symlink(target.c_str(), link_path.c_str());

    WimDentry root;
    MockWriter writer;
    ASSERT_EQ(0, wim_capture_dir(path("src").c_str(), &root,
                                  MockWriter::callback, &writer));

    /*
     * Symlinks are captured but get NORMAL attribute (not DIRECTORY or ARCHIVE).
     * Verify the symlink entry is not treated as a regular file (no blob written for it).
     */
    ASSERT_EQ(2u, root.child_count);

    /* Find the symlink entry */
    const WimDentry* link_entry = NULL;
    const WimDentry* real_entry = NULL;
    for (size_t i = 0; i < root.child_count; i++) {
        if (strcmp(root.children[i].name_utf8, "link.txt") == 0)
            link_entry = &root.children[i];
        else if (strcmp(root.children[i].name_utf8, "real.txt") == 0)
            real_entry = &root.children[i];
    }

    ASSERT_NE(nullptr, link_entry);
    ASSERT_NE(nullptr, real_entry);

    /* Symlink gets NORMAL attribute, not ARCHIVE or DIRECTORY */
    EXPECT_TRUE(link_entry->attributes & WIM_FILE_ATTRIBUTE_NORMAL);
    EXPECT_FALSE(link_entry->attributes & WIM_FILE_ATTRIBUTE_ARCHIVE);

    /* Only 1 blob written (the real file), symlink doesn't produce a blob */
    EXPECT_EQ(1u, writer.blobs.size());
    wim_dentry_free(&root);
}
