extern "C" {
#include "sha1.h"
}
#include <gtest/gtest.h>
#include <cstring>
#include <vector>

/* Helper: format a 20-byte digest as a 40-char hex string */
static std::string hex(const uint8_t d[20])
{
    char buf[41];
    for (int i = 0; i < 20; i++)
        snprintf(buf + 2 * i, 3, "%02x", d[i]);
    return std::string(buf, 40);
}

/* Helper: parse a 40-char hex string into a 20-byte digest */
static void from_hex(const char* s, uint8_t d[20])
{
    for (int i = 0; i < 20; i++) {
        unsigned v;
        sscanf(s + 2 * i, "%02x", &v);
        d[i] = (uint8_t)v;
    }
}

TEST(SHA1, Init)
{
    Sha1Ctx ctx;
    sha1_init(&ctx);
    /* RFC 3174 initial state */
    EXPECT_EQ(ctx.state[0], 0x67452301u);
    EXPECT_EQ(ctx.state[1], 0xEFCDAB89u);
    EXPECT_EQ(ctx.state[2], 0x98BADCFEu);
    EXPECT_EQ(ctx.state[3], 0x10325476u);
    EXPECT_EQ(ctx.state[4], 0xC3D2E1F0u);
    EXPECT_EQ(ctx.count[0], 0u);
    EXPECT_EQ(ctx.count[1], 0u);
}

TEST(SHA1, Hash_Empty)
{
    uint8_t d[20];
    sha1_hash((const uint8_t*)"", 0, d);
    EXPECT_EQ(hex(d), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
}

TEST(SHA1, Hash_Abc)
{
    uint8_t d[20];
    sha1_hash((const uint8_t*)"abc", 3, d);
    EXPECT_EQ(hex(d), "a9993e364706816aba3e25717850c26c9cd0d89d");
}

TEST(SHA1, Hash_448bit)
{
    const char* msg = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    uint8_t d[20];
    sha1_hash((const uint8_t*)msg, (uint32_t)strlen(msg), d);
    EXPECT_EQ(hex(d), "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
}

TEST(SHA1, Hash_MillionA)
{
    std::vector<uint8_t> buf(1000000, 'a');
    uint8_t d[20];
    sha1_hash(buf.data(), (uint32_t)buf.size(), d);
    EXPECT_EQ(hex(d), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}

TEST(SHA1, Incremental)
{
    /* Split "abc" across two updates */
    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, (const uint8_t*)"a", 1);
    sha1_update(&ctx, (const uint8_t*)"bc", 2);
    uint8_t d_inc[20];
    sha1_final(&ctx, d_inc);

    uint8_t d_one[20];
    sha1_hash((const uint8_t*)"abc", 3, d_one);

    EXPECT_EQ(memcmp(d_inc, d_one, 20), 0);
}

TEST(SHA1, BlockBoundary63)
{
    /* 63 bytes: partial block, no padding wrap */
    std::vector<uint8_t> buf(63, 'X');
    uint8_t d_one[20];
    sha1_hash(buf.data(), 63, d_one);

    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, buf.data(), 63);
    uint8_t d_inc[20];
    sha1_final(&ctx, d_inc);
    EXPECT_EQ(memcmp(d_one, d_inc, 20), 0);
}

TEST(SHA1, BlockBoundary64)
{
    /* 64 bytes: exact one block */
    std::vector<uint8_t> buf(64, 'Y');
    uint8_t d_one[20];
    sha1_hash(buf.data(), 64, d_one);

    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, buf.data(), 64);
    uint8_t d_inc[20];
    sha1_final(&ctx, d_inc);
    EXPECT_EQ(memcmp(d_one, d_inc, 20), 0);
}

TEST(SHA1, BlockBoundary65)
{
    /* 65 bytes: crosses block boundary */
    std::vector<uint8_t> buf(65, 'Z');
    uint8_t d_one[20];
    sha1_hash(buf.data(), 65, d_one);

    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, buf.data(), 65);
    uint8_t d_inc[20];
    sha1_final(&ctx, d_inc);
    EXPECT_EQ(memcmp(d_one, d_inc, 20), 0);
}

TEST(SHA1, ZeroLength)
{
    uint8_t d[20];
    sha1_hash(nullptr, 0, d);
    EXPECT_EQ(hex(d), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
}

TEST(SHA1, OneByte)
{
    /* Hash of single byte 0x00 */
    uint8_t byte = 0x00;
    uint8_t d[20];
    sha1_hash(&byte, 1, d);
    /* Known: SHA1("\x00") = 5ba93c9db0cff93f52b521d7420e43f6eda2784f */
    EXPECT_EQ(hex(d), "5ba93c9db0cff93f52b521d7420e43f6eda2784f");

    /* Hash of single byte 0xFF */
    byte = 0xFF;
    sha1_hash(&byte, 1, d);
    /* Verify incremental matches one-shot */
    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, &byte, 1);
    uint8_t d2[20];
    sha1_final(&ctx, d2);
    EXPECT_EQ(memcmp(d, d2, 20), 0);
}

TEST(SHA1, LargeBuffer)
{
    /* 1 MB buffer with pseudo-random content */
    std::vector<uint8_t> buf(1024 * 1024);
    uint32_t state = 0xDEADBEEF;
    for (size_t i = 0; i < buf.size(); i++) {
        state = state * 1103515245 + 12345;
        buf[i] = (uint8_t)(state >> 16);
    }

    /* One-shot */
    uint8_t d_one[20];
    sha1_hash(buf.data(), (uint32_t)buf.size(), d_one);

    /* Incremental: init + update + final */
    Sha1Ctx ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, buf.data(), (uint32_t)buf.size());
    uint8_t d_inc[20];
    sha1_final(&ctx, d_inc);

    EXPECT_EQ(memcmp(d_one, d_inc, 20), 0);
}

TEST(SHA1, MultiUpdate)
{
    /* 100 separate 1-byte updates == single 100-byte hash */
    std::vector<uint8_t> buf(100);
    for (int i = 0; i < 100; i++)
        buf[i] = (uint8_t)i;

    uint8_t d_one[20];
    sha1_hash(buf.data(), 100, d_one);

    Sha1Ctx ctx;
    sha1_init(&ctx);
    for (int i = 0; i < 100; i++)
        sha1_update(&ctx, &buf[i], 1);
    uint8_t d_multi[20];
    sha1_final(&ctx, d_multi);

    EXPECT_EQ(memcmp(d_one, d_multi, 20), 0);
}
