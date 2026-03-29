/*
 * test_wim_io.cpp - Unit tests for UTF conversion and time helpers
 */

/* Map C11 _Static_assert to C++ static_assert for shared headers */
#define _Static_assert static_assert

extern "C" {
#include "wim_io.h"
#include "wim_types.h"
}
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>

/* ================================================================
 *  utf8_to_utf16le tests
 * ================================================================ */

TEST(Utf8ToUtf16, Empty)
{
    uint16_t* out = NULL;
    size_t len = 999;
    ASSERT_EQ(0, utf8_to_utf16le("", &out, &len));
    EXPECT_EQ(0u, len);
    free(out);
}

TEST(Utf8ToUtf16, Ascii)
{
    uint16_t* out = NULL;
    size_t len = 0;
    ASSERT_EQ(0, utf8_to_utf16le("hello", &out, &len));
    ASSERT_EQ(5u, len);
    EXPECT_EQ((uint16_t)'h', out[0]);
    EXPECT_EQ((uint16_t)'e', out[1]);
    EXPECT_EQ((uint16_t)'l', out[2]);
    EXPECT_EQ((uint16_t)'l', out[3]);
    EXPECT_EQ((uint16_t)'o', out[4]);
    free(out);
}

TEST(Utf8ToUtf16, TwoByte)
{
    /* "e-acute" = U+00E9 = UTF-8 0xC3 0xA9 */
    uint16_t* out = NULL;
    size_t len = 0;
    ASSERT_EQ(0, utf8_to_utf16le("\xC3\xA9", &out, &len));
    ASSERT_EQ(1u, len);
    EXPECT_EQ((uint16_t)0x00E9, out[0]);
    free(out);
}

TEST(Utf8ToUtf16, ThreeByte)
{
    /* Euro sign = U+20AC = UTF-8 0xE2 0x82 0xAC */
    uint16_t* out = NULL;
    size_t len = 0;
    ASSERT_EQ(0, utf8_to_utf16le("\xE2\x82\xAC", &out, &len));
    ASSERT_EQ(1u, len);
    EXPECT_EQ((uint16_t)0x20AC, out[0]);
    free(out);
}

TEST(Utf8ToUtf16, Null)
{
    uint16_t* out = NULL;
    size_t len = 0;
    EXPECT_EQ(-1, utf8_to_utf16le(NULL, &out, &len));
}

/* ================================================================
 *  utf16le_to_utf8 tests
 * ================================================================ */

TEST(Utf16ToUtf8, Empty)
{
    uint16_t dummy = 0;
    char* result = utf16le_to_utf8(&dummy, 0);
    ASSERT_NE(nullptr, result);
    EXPECT_STREQ("", result);
    free(result);
}

TEST(Utf16ToUtf8, Ascii)
{
    uint16_t data[] = { 'A', 'B', 'C' };
    char* result = utf16le_to_utf8(data, 3);
    ASSERT_NE(nullptr, result);
    EXPECT_STREQ("ABC", result);
    free(result);
}

TEST(Utf16ToUtf8, RoundTrip)
{
    const char* strings[] = {
        "hello world",
        "\xC3\xA9\xC3\xA0\xC3\xBC",       /* e-acute, a-grave, u-umlaut */
        "\xE2\x82\xAC 100",                  /* Euro sign + " 100" */
        "",
        NULL,
    };

    for (int i = 0; strings[i] != NULL; i++) {
        uint16_t* u16 = NULL;
        size_t u16_len = 0;
        ASSERT_EQ(0, utf8_to_utf16le(strings[i], &u16, &u16_len))
            << "Failed to convert string index " << i;

        char* back = utf16le_to_utf8(u16, u16_len);
        ASSERT_NE(nullptr, back) << "Failed round-trip for string index " << i;
        EXPECT_STREQ(strings[i], back) << "Mismatch at string index " << i;
        free(u16);
        free(back);
    }
}

/* ================================================================
 *  unix_to_filetime / filetime_to_unix tests
 * ================================================================ */

TEST(UnixToFiletime, Epoch)
{
    /* Unix epoch = 1970-01-01 00:00:00 = FILETIME 116444736000000000 */
    uint64_t ft = unix_to_filetime(0);
    EXPECT_EQ(116444736000000000ULL, ft);
}

TEST(UnixToFiletime, OneSecond)
{
    uint64_t ft = unix_to_filetime(1);
    EXPECT_EQ(116444736010000000ULL, ft);
}

TEST(FiletimeToUnix, Zero)
{
    /* FILETIME 0 is before Unix epoch, should clamp to 0 */
    time_t t = filetime_to_unix(0);
    EXPECT_EQ((time_t)0, t);
}

TEST(FiletimeToUnix, RoundTrip)
{
    time_t values[] = { 0, 1, 1000000, 1700000000 };
    for (time_t v : values) {
        uint64_t ft = unix_to_filetime(v);
        time_t back = filetime_to_unix(ft);
        EXPECT_EQ(v, back) << "Round-trip failed for t=" << v;
    }
}

/* ================================================================
 *  filetime_to_string tests
 * ================================================================ */

TEST(FiletimeToString, KnownDate)
{
    /* 2023-11-15T11:50:00Z = Unix 1700049000 */
    uint64_t ft = unix_to_filetime(1700049000);
    char* s = filetime_to_string(ft);
    ASSERT_NE(nullptr, s);
    EXPECT_STREQ("2023-11-15T11:50:00Z", s);
    free(s);
}

TEST(FiletimeToString, Epoch)
{
    uint64_t ft = unix_to_filetime(0);
    char* s = filetime_to_string(ft);
    ASSERT_NE(nullptr, s);
    EXPECT_STREQ("1970-01-01T00:00:00Z", s);
    free(s);
}

TEST(FiletimeToString, NullSafe)
{
    /* ft=0 is before epoch, filetime_to_unix clamps to 0 */
    char* s = filetime_to_string(0);
    ASSERT_NE(nullptr, s);
    /* Should produce a valid date string (length > 0) */
    EXPECT_GT(strlen(s), 0u);
    free(s);
}
