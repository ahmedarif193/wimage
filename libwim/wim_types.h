#ifndef WIM_TYPES_H
#define WIM_TYPES_H

#include "wim_format.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* SHA-1 key for blob lookup */
typedef struct {
    uint8_t hash[20];
} WimSha1Key;

static inline int wim_sha1_cmp(const WimSha1Key* a, const WimSha1Key* b) {
    return memcmp(a->hash, b->hash, 20);
}
static inline int wim_sha1_is_zero(const WimSha1Key* k) {
    uint8_t z[20] = {0};
    return memcmp(k->hash, z, 20) == 0;
}

/* Blob entry */
typedef struct {
    WimSha1Key sha1;
    uint64_t original_size;
    uint64_t compressed_size;
    uint64_t offset;
    uint32_t ref_count;
    uint8_t  flags;
} WimBlob;

/* Directory entry (tree node) */
typedef struct WimDentry {
    char*     name_utf8;       /* heap-allocated */
    uint16_t* name_utf16;      /* heap-allocated */
    size_t    name_utf16_len;  /* count of uint16_t */
    uint32_t  attributes;
    uint64_t  creation_time;
    uint64_t  last_access_time;
    uint64_t  last_write_time;
    uint8_t   sha1[20];
    uint64_t  file_size;
    int32_t   security_id;
    uint64_t  subdir_offset;

    struct WimDentry* children;   /* heap-allocated array */
    size_t child_count;
    size_t child_cap;
} WimDentry;

/* Image info (from XML) */
typedef struct {
    char     name[256];
    char     description[256];
    uint32_t dir_count;
    uint32_t file_count;
    uint64_t total_bytes;
    uint64_t creation_time;
    uint64_t modification_time;
} WimImageInfo;

/* Per-image data */
typedef struct {
    WimDentry root;
    int       metadata_loaded;
    WimBlob   metadata_blob;
} WimImageData;

/* Main WIM context (replaces CWimImage class) */
typedef struct {
    FILE*       file;
    WimHeader   header;
    int         writing;
    int         use_xpress;
    int         num_threads; /* 0 or 1 = single-threaded */

    /* Blob table */
    WimBlob*    blobs;
    size_t      blob_count;
    size_t      blob_cap;

    /* Images */
    WimImageData* images;
    size_t        image_count;

    /* Image infos */
    WimImageInfo* image_infos;
    size_t        image_info_count;

    /* XML */
    char*    xml_utf8;
    uint8_t* xml_raw;
    size_t   xml_raw_size;
} WimCtx;

/* Dynamic array helpers */
static inline void wim_dentry_init(WimDentry* d) {
    memset(d, 0, sizeof(*d));
    d->security_id = -1;
}

static inline int wim_dentry_add_child(WimDentry* parent, WimDentry child) {
    if (parent->child_count >= parent->child_cap) {
        size_t newcap = parent->child_cap ? parent->child_cap * 2 : 8;
        WimDentry* tmp = (WimDentry*)realloc(parent->children, newcap * sizeof(WimDentry));
        if (!tmp) return -1;
        parent->children = tmp;
        parent->child_cap = newcap;
    }
    parent->children[parent->child_count++] = child;
    return 0;
}

static inline void wim_dentry_free(WimDentry* d) {
    free(d->name_utf8);
    free(d->name_utf16);
    for (size_t i = 0; i < d->child_count; i++)
        wim_dentry_free(&d->children[i]);
    free(d->children);
    memset(d, 0, sizeof(*d));
}

static inline void wim_ctx_init(WimCtx* ctx) {
    memset(ctx, 0, sizeof(*ctx));
}

/* Blob table: add entry, find by SHA-1 */
static inline int wim_ctx_add_blob(WimCtx* ctx, const WimBlob* blob) {
    if (ctx->blob_count >= ctx->blob_cap) {
        size_t newcap = ctx->blob_cap ? ctx->blob_cap * 2 : 64;
        WimBlob* tmp = (WimBlob*)realloc(ctx->blobs, newcap * sizeof(WimBlob));
        if (!tmp) return -1;
        ctx->blobs = tmp;
        ctx->blob_cap = newcap;
    }
    ctx->blobs[ctx->blob_count++] = *blob;
    return 0;
}

static inline int wim_ctx_find_blob(const WimCtx* ctx, const uint8_t sha1[20]) {
    for (size_t i = 0; i < ctx->blob_count; i++) {
        if (memcmp(ctx->blobs[i].sha1.hash, sha1, 20) == 0)
            return (int)i;
    }
    return -1;
}

static inline void wim_ctx_free(WimCtx* ctx) {
    if (ctx->file) { fclose(ctx->file); ctx->file = NULL; }
    free(ctx->blobs);
    if (ctx->images) {
        for (size_t i = 0; i < ctx->image_count; i++)
            wim_dentry_free(&ctx->images[i].root);
        free(ctx->images);
    }
    free(ctx->image_infos);
    free(ctx->xml_utf8);
    free(ctx->xml_raw);
    memset(ctx, 0, sizeof(*ctx));
}

#endif /* WIM_TYPES_H */
