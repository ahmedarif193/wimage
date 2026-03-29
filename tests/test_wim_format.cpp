#define _Static_assert static_assert
extern "C" {
#include "wim_format.h"
}
#include <gtest/gtest.h>
#include <cstring>

/* ================================================================
 * Structure sizes (matches _Static_assert in header)
 * ================================================================ */

TEST(WimFormat, SizeofWimResHdr)
{
    EXPECT_EQ(sizeof(WimResHdr), 24u);
}

TEST(WimFormat, SizeofWimHeader)
{
    EXPECT_EQ(sizeof(WimHeader), 208u);
}

TEST(WimFormat, SizeofWimLookupEntry)
{
    EXPECT_EQ(sizeof(WimLookupEntry), 50u);
}

/* ================================================================
 * reshdr_get_size / reshdr_get_flags
 * ================================================================ */

TEST(WimFormat, ReshdrGetSize_Zero)
{
    WimResHdr r;
    memset(&r, 0, sizeof(r));
    EXPECT_EQ(reshdr_get_size(&r), 0u);
}

TEST(WimFormat, ReshdrGetSize_Small)
{
    WimResHdr r;
    memset(&r, 0, sizeof(r));
    r.size_and_flags[0] = 0xFF;
    EXPECT_EQ(reshdr_get_size(&r), 255u);
}

TEST(WimFormat, ReshdrGetSize_Max7Byte)
{
    WimResHdr r;
    memset(&r, 0, sizeof(r));
    /* Set low 7 bytes to 0xFF, byte 7 (flags) stays 0 */
    for (int i = 0; i < 7; i++)
        r.size_and_flags[i] = 0xFF;
    EXPECT_EQ(reshdr_get_size(&r), 0x00FFFFFFFFFFFFFFull);
}

TEST(WimFormat, ReshdrGetFlags)
{
    WimResHdr r;
    memset(&r, 0, sizeof(r));
    r.size_and_flags[7] = 0x06;
    EXPECT_EQ(reshdr_get_flags(&r), 0x06);
}

/* ================================================================
 * reshdr_set + get round-trip
 * ================================================================ */

TEST(WimFormat, ReshdrSetGet_RoundTrip)
{
    WimResHdr r;
    uint64_t comp_size = 0x0000123456789ABCull;
    uint8_t flags = WIM_RESHDR_FLAG_COMPRESSED | WIM_RESHDR_FLAG_METADATA;
    uint64_t offset = 0xDEADBEEFCAFEull;
    uint64_t orig_size = 0x0000FFFFFFFFFFFF;

    reshdr_set(&r, comp_size, flags, offset, orig_size);

    EXPECT_EQ(reshdr_get_size(&r), comp_size);
    EXPECT_EQ(reshdr_get_flags(&r), flags);
    EXPECT_EQ(r.offset, offset);
    EXPECT_EQ(r.original_size, orig_size);
}

/* ================================================================
 * Constants
 * ================================================================ */

TEST(WimFormat, Constants_Magic)
{
    EXPECT_EQ(memcmp(WIM_MAGIC, "MSWIM\0\0\0", 8), 0);
    EXPECT_EQ(WIM_MAGIC_LEN, 8u);
}

TEST(WimFormat, Constants_Version)
{
    EXPECT_EQ(WIM_VERSION, 0x00010D00u);
}

TEST(WimFormat, Constants_Flags)
{
    EXPECT_EQ(WIM_HDR_FLAG_COMPRESSION,     0x00000002u);
    EXPECT_EQ(WIM_HDR_FLAG_READONLY,        0x00000004u);
    EXPECT_EQ(WIM_HDR_FLAG_SPANNED,         0x00000008u);
    EXPECT_EQ(WIM_HDR_FLAG_RP_FIX,          0x00000080u);
    EXPECT_EQ(WIM_HDR_FLAG_COMPRESS_XPRESS, 0x00020000u);
    EXPECT_EQ(WIM_HDR_FLAG_COMPRESS_LZX,    0x00040000u);
    EXPECT_EQ(WIM_HDR_FLAG_COMPRESS_LZMS,   0x00080000u);

    EXPECT_EQ(WIM_RESHDR_FLAG_FREE,       0x01u);
    EXPECT_EQ(WIM_RESHDR_FLAG_METADATA,   0x02u);
    EXPECT_EQ(WIM_RESHDR_FLAG_COMPRESSED, 0x04u);
    EXPECT_EQ(WIM_RESHDR_FLAG_SPANNED,    0x08u);
}
