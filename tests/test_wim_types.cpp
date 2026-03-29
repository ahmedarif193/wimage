#define _Static_assert static_assert
extern "C" {
#include "wim_types.h"
}
#include <gtest/gtest.h>
#include <cstring>

/* ================================================================
 * WimSha1Key comparisons
 * ================================================================ */

TEST(WimTypes, Sha1Cmp_Equal)
{
    WimSha1Key a, b;
    memset(&a, 0x42, sizeof(a));
    memcpy(&b, &a, sizeof(a));
    EXPECT_EQ(wim_sha1_cmp(&a, &b), 0);
}

TEST(WimTypes, Sha1Cmp_Less)
{
    WimSha1Key a, b;
    memset(&a, 0x00, sizeof(a));
    memset(&b, 0xFF, sizeof(b));
    EXPECT_LT(wim_sha1_cmp(&a, &b), 0);
}

TEST(WimTypes, Sha1Cmp_Greater)
{
    WimSha1Key a, b;
    memset(&a, 0xFF, sizeof(a));
    memset(&b, 0x00, sizeof(b));
    EXPECT_GT(wim_sha1_cmp(&a, &b), 0);
}

TEST(WimTypes, Sha1IsZero_True)
{
    WimSha1Key k;
    memset(&k, 0, sizeof(k));
    EXPECT_NE(wim_sha1_is_zero(&k), 0);
}

TEST(WimTypes, Sha1IsZero_False)
{
    WimSha1Key k;
    memset(&k, 0, sizeof(k));
    k.hash[10] = 0x01; /* one bit set */
    EXPECT_EQ(wim_sha1_is_zero(&k), 0);
}

/* ================================================================
 * WimDentry
 * ================================================================ */

TEST(WimTypes, DentryInit)
{
    WimDentry d;
    wim_dentry_init(&d);
    EXPECT_EQ(d.security_id, -1);
    EXPECT_EQ(d.name_utf8, nullptr);
    EXPECT_EQ(d.name_utf16, nullptr);
    EXPECT_EQ(d.name_utf16_len, 0u);
    EXPECT_EQ(d.attributes, 0u);
    EXPECT_EQ(d.file_size, 0u);
    EXPECT_EQ(d.child_count, 0u);
    EXPECT_EQ(d.child_cap, 0u);
    EXPECT_EQ(d.children, nullptr);

    /* SHA1 should be all zeros */
    uint8_t zero[20] = {0};
    EXPECT_EQ(memcmp(d.sha1, zero, 20), 0);
}

TEST(WimTypes, DentryAddChild_First)
{
    WimDentry parent;
    wim_dentry_init(&parent);

    WimDentry child;
    wim_dentry_init(&child);
    child.attributes = 0x10; /* directory */

    wim_dentry_add_child(&parent, child);
    EXPECT_EQ(parent.child_count, 1u);
    EXPECT_GE(parent.child_cap, 1u);
    EXPECT_EQ(parent.children[0].attributes, 0x10u);

    wim_dentry_free(&parent);
}

TEST(WimTypes, DentryAddChild_Grow)
{
    WimDentry parent;
    wim_dentry_init(&parent);

    for (int i = 0; i < 20; i++) {
        WimDentry child;
        wim_dentry_init(&child);
        child.attributes = (uint32_t)i;
        wim_dentry_add_child(&parent, child);
    }

    EXPECT_EQ(parent.child_count, 20u);
    EXPECT_GE(parent.child_cap, 20u);

    /* Verify each child's attributes */
    for (int i = 0; i < 20; i++)
        EXPECT_EQ(parent.children[i].attributes, (uint32_t)i);

    wim_dentry_free(&parent);
}

TEST(WimTypes, DentryFree_Leaf)
{
    WimDentry d;
    wim_dentry_init(&d);
    wim_dentry_free(&d); /* no crash */
    EXPECT_EQ(d.child_count, 0u);
}

TEST(WimTypes, DentryFree_Tree)
{
    /* Parent -> children -> grandchildren */
    WimDentry root;
    wim_dentry_init(&root);

    for (int i = 0; i < 3; i++) {
        WimDentry child;
        wim_dentry_init(&child);
        for (int j = 0; j < 2; j++) {
            WimDentry grandchild;
            wim_dentry_init(&grandchild);
            grandchild.attributes = (uint32_t)(i * 10 + j);
            wim_dentry_add_child(&child, grandchild);
        }
        wim_dentry_add_child(&root, child);
    }

    EXPECT_EQ(root.child_count, 3u);
    EXPECT_EQ(root.children[0].child_count, 2u);

    wim_dentry_free(&root); /* should free entire tree without crash */
    EXPECT_EQ(root.child_count, 0u);
    EXPECT_EQ(root.children, nullptr);
}

/* ================================================================
 * WimCtx
 * ================================================================ */

TEST(WimTypes, CtxInit)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);
    EXPECT_EQ(ctx.file, nullptr);
    EXPECT_EQ(ctx.blob_count, 0u);
    EXPECT_EQ(ctx.blob_cap, 0u);
    EXPECT_EQ(ctx.blobs, nullptr);
    EXPECT_EQ(ctx.image_count, 0u);
    EXPECT_EQ(ctx.writing, 0);
}

TEST(WimTypes, CtxAddBlob_Single)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    WimBlob blob;
    memset(&blob, 0, sizeof(blob));
    blob.sha1.hash[0] = 0xAA;
    blob.original_size = 1234;

    int rc = wim_ctx_add_blob(&ctx, &blob);
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(ctx.blob_count, 1u);
    EXPECT_EQ(ctx.blobs[0].sha1.hash[0], 0xAA);
    EXPECT_EQ(ctx.blobs[0].original_size, 1234u);

    wim_ctx_free(&ctx);
}

TEST(WimTypes, CtxAddBlob_Many)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    for (int i = 0; i < 100; i++) {
        WimBlob blob;
        memset(&blob, 0, sizeof(blob));
        blob.sha1.hash[0] = (uint8_t)(i & 0xFF);
        blob.sha1.hash[1] = (uint8_t)((i >> 8) & 0xFF);
        blob.original_size = (uint64_t)i * 100;
        int rc = wim_ctx_add_blob(&ctx, &blob);
        EXPECT_EQ(rc, 0);
    }

    EXPECT_EQ(ctx.blob_count, 100u);
    EXPECT_GE(ctx.blob_cap, 100u);

    /* Spot check */
    EXPECT_EQ(ctx.blobs[50].original_size, 5000u);
    EXPECT_EQ(ctx.blobs[99].sha1.hash[0], 99u);

    wim_ctx_free(&ctx);
}

TEST(WimTypes, CtxFindBlob_Empty)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    uint8_t sha1[20] = {0};
    EXPECT_EQ(wim_ctx_find_blob(&ctx, sha1), -1);

    wim_ctx_free(&ctx);
}

TEST(WimTypes, CtxFindBlob_Found)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    WimBlob blob;
    memset(&blob, 0, sizeof(blob));
    blob.sha1.hash[0] = 0xDE;
    blob.sha1.hash[1] = 0xAD;
    wim_ctx_add_blob(&ctx, &blob);

    uint8_t search[20] = {0};
    search[0] = 0xDE;
    search[1] = 0xAD;
    int idx = wim_ctx_find_blob(&ctx, search);
    EXPECT_EQ(idx, 0);

    wim_ctx_free(&ctx);
}

TEST(WimTypes, CtxFindBlob_NotFound)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    WimBlob blob;
    memset(&blob, 0, sizeof(blob));
    blob.sha1.hash[0] = 0x01;
    wim_ctx_add_blob(&ctx, &blob);

    uint8_t search[20] = {0};
    search[0] = 0xFF; /* different hash */
    EXPECT_EQ(wim_ctx_find_blob(&ctx, search), -1);

    wim_ctx_free(&ctx);
}

TEST(WimTypes, CtxFree)
{
    WimCtx ctx;
    wim_ctx_init(&ctx);

    /* Add some blobs */
    for (int i = 0; i < 10; i++) {
        WimBlob blob;
        memset(&blob, 0, sizeof(blob));
        blob.sha1.hash[0] = (uint8_t)i;
        wim_ctx_add_blob(&ctx, &blob);
    }

    wim_ctx_free(&ctx); /* no crash */
    EXPECT_EQ(ctx.blob_count, 0u);
    EXPECT_EQ(ctx.blobs, nullptr);
}
