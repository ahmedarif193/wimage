/*
 * Copyright (C) 2026 Ahmed ARIF <arif.ing@outlook.com>
 *
 * wim_read.c - WIM reading implementation (pure C)
 *
 */

#include "wim_read.h"
#include "wim_io.h"
#include "sha1.h"
#include "xpress_huff.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#include <unistd.h>

/* ================================================================
 *  Internal helpers
 * ================================================================ */

static int read_header(WimCtx* ctx)
{
    if (fseek(ctx->file, 0, SEEK_SET) != 0)
        return -1;

    if (fread(&ctx->header, sizeof(WimHeader), 1, ctx->file) != 1)
        return -1;

    if (memcmp(ctx->header.magic, WIM_MAGIC, WIM_MAGIC_LEN) != 0) {
        fprintf(stderr, "Error: Invalid WIM magic\n");
        return -1;
    }

    if (ctx->header.header_size != WIM_HEADER_SIZE) {
        fprintf(stderr, "Error: Unexpected header size %u\n", ctx->header.header_size);
        return -1;
    }

    if (ctx->header.version != WIM_VERSION) {
        fprintf(stderr, "Error: Unsupported WIM version 0x%08X\n", ctx->header.version);
        return -1;
    }

    return 0;
}

static int read_lookup_table(WimCtx* ctx)
{
    uint64_t tbl_offset = ctx->header.lookup_table.offset;
    uint64_t tbl_comp_size = reshdr_get_size(&ctx->header.lookup_table);
    uint64_t tbl_orig_size = ctx->header.lookup_table.original_size;
    uint8_t tbl_flags = reshdr_get_flags(&ctx->header.lookup_table);

    if (tbl_orig_size == 0)
        return 0;

    if (fseek(ctx->file, (long)tbl_offset, SEEK_SET) != 0)
        return -1;

    uint8_t* raw = (uint8_t*)malloc((size_t)tbl_comp_size);
    if (!raw)
        return -1;
    if (fread(raw, 1, (size_t)tbl_comp_size, ctx->file) != (size_t)tbl_comp_size) {
        free(raw);
        return -1;
    }

    uint8_t* data;

    if ((tbl_flags & WIM_RESHDR_FLAG_COMPRESSED) && tbl_comp_size != tbl_orig_size) {
        data = (uint8_t*)malloc((size_t)tbl_orig_size);
        if (!data) { free(raw); return -1; }

        XpressStatus st = xpress_huff_decompress(raw, (uint32_t)tbl_comp_size,
                                                  data, (uint32_t)tbl_orig_size);
        free(raw);
        if (st != XPRESS_OK) {
            fprintf(stderr, "Error: Failed to decompress lookup table\n");
            free(data);
            return -1;
        }
    } else {
        data = raw;
    }

    size_t count = (size_t)tbl_orig_size / WIM_LOOKUP_ENTRY_SIZE;
    const uint8_t* ptr = data;

    for (size_t i = 0; i < count; i++) {
        WimLookupEntry entry;
        memcpy(&entry, ptr + i * WIM_LOOKUP_ENTRY_SIZE, WIM_LOOKUP_ENTRY_SIZE);

        WimBlob blob;
        memset(&blob, 0, sizeof(blob));
        memcpy(blob.sha1.hash, entry.sha1, 20);
        blob.compressed_size = reshdr_get_size(&entry.reshdr);
        blob.offset = entry.reshdr.offset;
        blob.original_size = entry.reshdr.original_size;
        blob.ref_count = entry.ref_count;
        blob.flags = reshdr_get_flags(&entry.reshdr);

        wim_ctx_add_blob(ctx, &blob);
    }

    free(data);
    return 0;
}

static int read_xml_data(WimCtx* ctx)
{
    uint64_t xml_offset = ctx->header.xml_data.offset;
    uint64_t xml_size = reshdr_get_size(&ctx->header.xml_data);

    if (xml_size == 0)
        return 0;

    if (fseek(ctx->file, (long)xml_offset, SEEK_SET) != 0)
        return -1;

    ctx->xml_raw = (uint8_t*)malloc((size_t)xml_size);
    if (!ctx->xml_raw)
        return -1;
    ctx->xml_raw_size = (size_t)xml_size;

    if (fread(ctx->xml_raw, 1, (size_t)xml_size, ctx->file) != (size_t)xml_size)
        return -1;

    /* Convert UTF-16LE to UTF-8 */
    const uint8_t* p = ctx->xml_raw;
    size_t byte_len = ctx->xml_raw_size;

    /* Skip BOM if present */
    if (byte_len >= 2 && p[0] == 0xFF && p[1] == 0xFE) {
        p += 2;
        byte_len -= 2;
    }

    size_t u16_len = byte_len / 2;
    const uint16_t* u16 = (const uint16_t*)p;
    ctx->xml_utf8 = utf16le_to_utf8(u16, u16_len);

    return 0;
}

static int read_resource(WimCtx* ctx, const WimBlob* blob,
                         uint8_t** out_data, size_t* out_size)
{
    if (fseek(ctx->file, (long)blob->offset, SEEK_SET) != 0)
        return -1;

    int compressed = (blob->flags & WIM_RESHDR_FLAG_COMPRESSED) != 0 &&
                     blob->compressed_size != blob->original_size;

    if (!compressed) {
        *out_data = (uint8_t*)malloc((size_t)blob->original_size);
        if (!*out_data)
            return -1;
        *out_size = (size_t)blob->original_size;
        if (fread(*out_data, 1, *out_size, ctx->file) != *out_size) {
            free(*out_data);
            *out_data = NULL;
            return -1;
        }
        return 0;
    }

    /* Compressed resource with chunk table */
    uint32_t chunk_size = ctx->header.chunk_size;
    if (chunk_size == 0)
        chunk_size = 32768;

    uint64_t num_chunks = (blob->original_size + chunk_size - 1) / chunk_size;
    if (num_chunks == 0)
        return -1;

    /* Read chunk offset table: (num_chunks - 1) entries */
    int use_64bit = (blob->original_size > 0xFFFFFFFFULL);
    size_t entry_size = use_64bit ? 8 : 4;
    size_t table_size = (size_t)((num_chunks - 1) * entry_size);

    uint8_t* table_buf = NULL;
    if (table_size > 0) {
        table_buf = (uint8_t*)malloc(table_size);
        if (!table_buf)
            return -1;
        if (fread(table_buf, 1, table_size, ctx->file) != table_size) {
            free(table_buf);
            return -1;
        }
    }

    /* Build chunk offsets (relative to after the table) */
    uint64_t* chunk_offsets = (uint64_t*)malloc(((size_t)num_chunks + 1) * sizeof(uint64_t));
    if (!chunk_offsets) {
        free(table_buf);
        return -1;
    }

    chunk_offsets[0] = 0;
    for (uint64_t i = 0; i < num_chunks - 1; i++) {
        if (use_64bit) {
            uint64_t val;
            memcpy(&val, table_buf + i * 8, 8);
            chunk_offsets[i + 1] = val;
        } else {
            uint32_t val;
            memcpy(&val, table_buf + i * 4, 4);
            chunk_offsets[i + 1] = val;
        }
    }
    /* Last entry = total compressed data size (after table) */
    chunk_offsets[num_chunks] = blob->compressed_size - table_size;
    free(table_buf);

    /* Read all compressed data after the table */
    uint64_t comp_data_size = blob->compressed_size - table_size;
    uint8_t* comp_data = (uint8_t*)malloc((size_t)comp_data_size);
    if (!comp_data) {
        free(chunk_offsets);
        return -1;
    }
    if (fread(comp_data, 1, (size_t)comp_data_size, ctx->file) != (size_t)comp_data_size) {
        free(comp_data);
        free(chunk_offsets);
        return -1;
    }

    *out_data = (uint8_t*)malloc((size_t)blob->original_size);
    if (!*out_data) {
        free(comp_data);
        free(chunk_offsets);
        return -1;
    }
    *out_size = (size_t)blob->original_size;
    uint64_t out_offset = 0;

    for (uint64_t i = 0; i < num_chunks; i++) {
        uint64_t c_off = chunk_offsets[i];
        uint64_t c_end = chunk_offsets[i + 1];
        uint64_t c_len = c_end - c_off;

        uint64_t remaining = blob->original_size - out_offset;
        uint32_t this_chunk = (remaining < chunk_size) ?
                              (uint32_t)remaining : chunk_size;

        if (c_len == this_chunk) {
            /* Stored uncompressed */
            memcpy(*out_data + out_offset, comp_data + c_off, this_chunk);
        } else {
            XpressStatus st = xpress_huff_decompress(
                comp_data + c_off, (uint32_t)c_len,
                *out_data + out_offset, this_chunk);
            if (st != XPRESS_OK) {
                fprintf(stderr, "Error: Chunk decompress failed (chunk %lu)\n",
                        (unsigned long)i);
                free(*out_data);
                *out_data = NULL;
                free(comp_data);
                free(chunk_offsets);
                return -1;
            }
        }
        out_offset += this_chunk;
    }

    free(comp_data);
    free(chunk_offsets);
    return 0;
}

/* ================================================================
 *  Dentry deserialization
 * ================================================================ */

#define DENTRY_FIXED_SIZE 102

static int deserialize_one_dentry(const uint8_t* buf, uint64_t buf_len,
                                  uint64_t offset, WimDentry* d, uint64_t* next_offset)
{
    if (offset + 8 > buf_len)
        return 0;

    uint64_t entry_len;
    memcpy(&entry_len, buf + offset, 8);
    if (entry_len == 0) {
        *next_offset = 0;
        return 0; /* end-of-directory marker */
    }

    if (entry_len < DENTRY_FIXED_SIZE || offset + entry_len > buf_len)
        return 0;

    const uint8_t* p = buf + offset;
    size_t off = 8;

    memcpy(&d->attributes, p + off, 4);        off += 4;
    memcpy(&d->security_id, p + off, 4);       off += 4;
    memcpy(&d->subdir_offset, p + off, 8);     off += 8;
    off += 16; /* unused1, unused2 */
    memcpy(&d->creation_time, p + off, 8);     off += 8;
    memcpy(&d->last_access_time, p + off, 8);  off += 8;
    memcpy(&d->last_write_time, p + off, 8);   off += 8;
    memcpy(d->sha1, p + off, 20);              off += 20;
    off += 4; /* reparse_tag */
    off += 8; /* hard_link_group_id */

    uint16_t num_streams;
    memcpy(&num_streams, p + off, 2);           off += 2;
    uint16_t short_name_nbytes;
    memcpy(&short_name_nbytes, p + off, 2);     off += 2;
    uint16_t file_name_nbytes;
    memcpy(&file_name_nbytes, p + off, 2);      off += 2;

    /* File name */
    if (file_name_nbytes > 0 && off + file_name_nbytes <= entry_len) {
        size_t u16_count = file_name_nbytes / 2;
        d->name_utf16 = (uint16_t*)malloc(u16_count * sizeof(uint16_t));
        if (d->name_utf16) {
            memcpy(d->name_utf16, p + off, file_name_nbytes);
            d->name_utf16_len = u16_count;
            d->name_utf8 = utf16le_to_utf8(d->name_utf16, u16_count);
        }
    }

    *next_offset = offset + entry_len;
    return 1;
}

static int deserialize_dentry_tree(const uint8_t* buf, uint64_t len, WimDentry* root)
{
    if (len < 8)
        return -1;

    uint32_t sec_total_len;
    memcpy(&sec_total_len, buf, 4);

    uint64_t dentry_start = sec_total_len;
    if (dentry_start % 8 != 0)
        dentry_start += 8 - (dentry_start % 8);

    if (dentry_start >= len)
        return -1;

    /* Parse root dentry */
    uint64_t next;
    if (!deserialize_one_dentry(buf, len, dentry_start, root, &next))
        return -1;

    /* BFS: queue directories whose children need parsing */
    size_t q_cap = 64;
    size_t q_head = 0, q_tail = 0;
    WimDentry** queue = (WimDentry**)malloc(q_cap * sizeof(WimDentry*));
    if (!queue)
        return -1;

    if ((root->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) && root->subdir_offset > 0) {
        queue[q_tail++] = root;
    }

    while (q_head < q_tail) {
        WimDentry* parent = queue[q_head++];

        uint64_t off = parent->subdir_offset;
        if (off >= len)
            continue;

        while (off < len) {
            WimDentry child;
            wim_dentry_init(&child);
            uint64_t child_next;
            if (!deserialize_one_dentry(buf, len, off, &child, &child_next))
                break;
            if (wim_dentry_add_child(parent, child) != 0) {
                wim_dentry_free(&child);
                free(queue);
                return -1;
            }
            off = child_next;
        }

        for (size_t c = 0; c < parent->child_count; c++) {
            WimDentry* ch = &parent->children[c];
            if ((ch->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) && ch->subdir_offset > 0) {
                if (q_tail >= q_cap) {
                    q_cap *= 2;
                    WimDentry** tmp_q = (WimDentry**)realloc(queue, q_cap * sizeof(WimDentry*));
                    if (!tmp_q) {
                        free(queue);
                        return -1;
                    }
                    queue = tmp_q;
                }
                queue[q_tail++] = ch;
            }
        }
    }

    free(queue);
    return 0;
}

static int read_metadata(WimCtx* ctx, int image_idx)
{
    if (image_idx < 0 || image_idx >= (int)ctx->image_count)
        return -1;

    WimImageData* img = &ctx->images[image_idx];
    if (img->metadata_loaded)
        return 0;

    uint8_t* data = NULL;
    size_t data_size = 0;
    int ret = read_resource(ctx, &img->metadata_blob, &data, &data_size);
    if (ret != 0)
        return ret;

    ret = deserialize_dentry_tree(data, data_size, &img->root);
    free(data);
    if (ret != 0)
        return ret;

    img->metadata_loaded = 1;
    return 0;
}

/* ================================================================
 *  XML image info parsing
 * ================================================================ */

static const char* find_tag_value(const char* block, size_t block_len,
                                  const char* tag, size_t* val_len)
{
    char open[64], close[64];
    snprintf(open, sizeof(open), "<%s>", tag);
    snprintf(close, sizeof(close), "</%s>", tag);

    const char* s = strstr(block, open);
    if (!s || (size_t)(s - block) >= block_len)
        return NULL;
    s += strlen(open);

    const char* e = strstr(s, close);
    if (!e || (size_t)(e - block) >= block_len)
        return NULL;

    *val_len = (size_t)(e - s);
    return s;
}

static void parse_filetime_xml(const char* block, size_t block_len, uint64_t* out)
{
    size_t hp_len, lp_len;
    const char* hp = find_tag_value(block, block_len, "HIGHPART", &hp_len);
    const char* lp = find_tag_value(block, block_len, "LOWPART", &lp_len);
    if (hp && lp) {
        char buf[32];
        size_t n = hp_len < 31 ? hp_len : 31;
        memcpy(buf, hp, n); buf[n] = '\0';
        uint32_t hi = (uint32_t)strtoul(buf, NULL, 0);

        n = lp_len < 31 ? lp_len : 31;
        memcpy(buf, lp, n); buf[n] = '\0';
        uint32_t lo = (uint32_t)strtoul(buf, NULL, 0);

        *out = ((uint64_t)hi << 32) | lo;
    }
}

static void parse_xml_image_infos(WimCtx* ctx)
{
    if (!ctx->xml_utf8)
        return;

    const char* xml = ctx->xml_utf8;
    size_t xml_len = strlen(xml);
    const char* pos = xml;

    /* Count images first */
    size_t img_count = 0;
    const char* scan = xml;
    while ((scan = strstr(scan, "<IMAGE ")) != NULL) {
        img_count++;
        scan += 7;
    }

    if (img_count == 0)
        return;

    ctx->image_infos = (WimImageInfo*)calloc(img_count, sizeof(WimImageInfo));
    if (!ctx->image_infos)
        return;

    size_t idx = 0;
    while ((pos = strstr(pos, "<IMAGE ")) != NULL && idx < img_count) {
        const char* end = strstr(pos, "</IMAGE>");
        if (!end) break;
        size_t block_len = (size_t)(end - pos + 8);

        WimImageInfo* info = &ctx->image_infos[idx];

        size_t vlen;
        const char* val;

        val = find_tag_value(pos, block_len, "DIRCOUNT", &vlen);
        if (val) {
            char buf[32];
            size_t n = vlen < 31 ? vlen : 31;
            memcpy(buf, val, n); buf[n] = '\0';
            info->dir_count = (uint32_t)strtoul(buf, NULL, 10);
        }

        val = find_tag_value(pos, block_len, "FILECOUNT", &vlen);
        if (val) {
            char buf[32];
            size_t n = vlen < 31 ? vlen : 31;
            memcpy(buf, val, n); buf[n] = '\0';
            info->file_count = (uint32_t)strtoul(buf, NULL, 10);
        }

        val = find_tag_value(pos, block_len, "TOTALBYTES", &vlen);
        if (val) {
            char buf[32];
            size_t n = vlen < 31 ? vlen : 31;
            memcpy(buf, val, n); buf[n] = '\0';
            info->total_bytes = strtoull(buf, NULL, 10);
        }

        val = find_tag_value(pos, block_len, "NAME", &vlen);
        if (val) {
            size_t n = vlen < sizeof(info->name) - 1 ? vlen : sizeof(info->name) - 1;
            memcpy(info->name, val, n);
            info->name[n] = '\0';
        }

        val = find_tag_value(pos, block_len, "DESCRIPTION", &vlen);
        if (val) {
            size_t n = vlen < sizeof(info->description) - 1 ? vlen : sizeof(info->description) - 1;
            memcpy(info->description, val, n);
            info->description[n] = '\0';
        }

        /* Parse CREATIONTIME */
        val = find_tag_value(pos, block_len, "CREATIONTIME", &vlen);
        if (val) {
            parse_filetime_xml(val, vlen, &info->creation_time);
        }

        /* Parse LASTMODIFICATIONTIME */
        val = find_tag_value(pos, block_len, "LASTMODIFICATIONTIME", &vlen);
        if (val) {
            parse_filetime_xml(val, vlen, &info->modification_time);
        }

        idx++;
        pos = end + 8;
    }

    ctx->image_info_count = idx;
    (void)xml_len;
}

/* ================================================================
 *  Public API
 * ================================================================ */

int wim_open(WimCtx* ctx, const char* filename)
{
    wim_close(ctx);
    wim_ctx_init(ctx);

    ctx->file = fopen(filename, "rb");
    if (!ctx->file) {
        fprintf(stderr, "Error: Cannot open '%s'\n", filename);
        return -1;
    }

    int ret = read_header(ctx);
    if (ret != 0) { wim_close(ctx); return ret; }

    ret = read_lookup_table(ctx);
    if (ret != 0) { wim_close(ctx); return ret; }

    ret = read_xml_data(ctx);
    if (ret != 0) { wim_close(ctx); return ret; }

    parse_xml_image_infos(ctx);

    /* Identify metadata blobs and create image entries */
    size_t meta_count = 0;
    for (size_t i = 0; i < ctx->blob_count; i++) {
        if (ctx->blobs[i].flags & WIM_RESHDR_FLAG_METADATA)
            meta_count++;
    }

    if (meta_count > 0) {
        ctx->images = (WimImageData*)calloc(meta_count, sizeof(WimImageData));
        if (!ctx->images) { wim_close(ctx); return -1; }

        size_t idx = 0;
        for (size_t i = 0; i < ctx->blob_count; i++) {
            if (ctx->blobs[i].flags & WIM_RESHDR_FLAG_METADATA) {
                wim_dentry_init(&ctx->images[idx].root);
                ctx->images[idx].metadata_blob = ctx->blobs[i];
                ctx->images[idx].metadata_loaded = 0;
                idx++;
            }
        }
        ctx->image_count = meta_count;
    }

    ctx->writing = 0;
    return 0;
}

int wim_select_image(WimCtx* ctx, int index)
{
    if (index < 1 || index > (int)ctx->image_count)
        return -1;

    int idx = index - 1;
    if (!ctx->images[idx].metadata_loaded) {
        int ret = read_metadata(ctx, idx);
        if (ret != 0)
            return ret;
    }
    return 0;
}

const WimDentry* wim_get_root(const WimCtx* ctx, int index)
{
    if (index < 1 || index > (int)ctx->image_count)
        return NULL;
    int idx = index - 1;
    if (!ctx->images[idx].metadata_loaded)
        return NULL;
    return &ctx->images[idx].root;
}

int wim_read_blob(WimCtx* ctx, const uint8_t sha1[20], uint8_t** data, size_t* data_size)
{
    int idx = wim_ctx_find_blob(ctx, sha1);
    if (idx < 0)
        return -1;

    return read_resource(ctx, &ctx->blobs[idx], data, data_size);
}

int wim_extract_file(WimCtx* ctx, const WimDentry* d, const char* dest_path)
{
    WimSha1Key key;
    memcpy(key.hash, d->sha1, 20);

    if (wim_sha1_is_zero(&key)) {
        /* Empty file */
        FILE* f = fopen(dest_path, "wb");
        if (!f) return -1;
        fclose(f);
    } else {
        uint8_t* data = NULL;
        size_t data_size = 0;
        int ret = wim_read_blob(ctx, d->sha1, &data, &data_size);
        if (ret != 0)
            return ret;

        FILE* f = fopen(dest_path, "wb");
        if (!f) { free(data); return -1; }
        if (data_size > 0)
            fwrite(data, 1, data_size, f);
        fclose(f);
        free(data);
    }

    /* Set file times */
    if (d->last_write_time != 0 || d->last_access_time != 0) {
        struct utimbuf times;
        times.actime = filetime_to_unix(d->last_access_time);
        times.modtime = filetime_to_unix(d->last_write_time);
        utime(dest_path, &times);
    }

    return 0;
}

int wim_extract_tree(WimCtx* ctx, const WimDentry* d, const char* dest_dir)
{
    if (d->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) {
        mkdir(dest_dir, 0755);

        for (size_t i = 0; i < d->child_count; i++) {
            const WimDentry* child = &d->children[i];
            if (!child->name_utf8)
                continue;

            size_t path_len = strlen(dest_dir) + 1 + strlen(child->name_utf8) + 1;
            char* child_path = (char*)malloc(path_len);
            if (!child_path)
                return -1;
            snprintf(child_path, path_len, "%s/%s", dest_dir, child->name_utf8);

            int ret = wim_extract_tree(ctx, child, child_path);
            free(child_path);
            if (ret != 0)
                return ret;
        }
    } else {
        return wim_extract_file(ctx, d, dest_dir);
    }

    return 0;
}

const char* wim_get_xml(const WimCtx* ctx)
{
    return ctx->xml_utf8;
}

int wim_verify_integrity(WimCtx* ctx)
{
    if (!ctx->file)
        return -1;

    uint64_t integ_offset = ctx->header.integrity.offset;
    uint64_t integ_size = reshdr_get_size(&ctx->header.integrity);

    if (integ_size == 0) {
        fprintf(stderr, "No integrity table present\n");
        return -1;
    }

    fseek(ctx->file, (long)integ_offset, SEEK_SET);

    uint32_t table_size, num_entries, chunk_size;
    if (fread(&table_size, 4, 1, ctx->file) != 1) return -1;
    if (fread(&num_entries, 4, 1, ctx->file) != 1) return -1;
    if (fread(&chunk_size, 4, 1, ctx->file) != 1) return -1;

    uint8_t* stored_hashes = (uint8_t*)malloc(num_entries * 20);
    if (!stored_hashes)
        return -1;
    if (fread(stored_hashes, 1, num_entries * 20, ctx->file) != num_entries * 20) {
        free(stored_hashes);
        return -1;
    }

    /* Compute SHA-1 over chunks from offset 208 to start of XML data.
     * XML data and integrity table are excluded from the hash range. */
    uint64_t data_start = WIM_HEADER_SIZE;
    uint64_t data_size = ctx->header.xml_data.offset - data_start;

    uint8_t* read_buf = (uint8_t*)malloc(chunk_size);
    if (!read_buf) {
        free(stored_hashes);
        return -1;
    }

    fseek(ctx->file, (long)data_start, SEEK_SET);
    uint64_t remaining = data_size;

    for (uint32_t i = 0; i < num_entries; i++) {
        uint32_t this_chunk = (remaining < chunk_size) ?
                              (uint32_t)remaining : chunk_size;

        if (fread(read_buf, 1, this_chunk, ctx->file) != this_chunk) {
            free(read_buf);
            free(stored_hashes);
            return -1;
        }

        uint8_t computed[20];
        sha1_hash(read_buf, this_chunk, computed);

        if (memcmp(computed, stored_hashes + i * 20, 20) != 0) {
            fprintf(stderr, "Error: Integrity check failed at chunk %u\n", i);
            free(read_buf);
            free(stored_hashes);
            return -1;
        }

        remaining -= this_chunk;
    }

    free(read_buf);
    free(stored_hashes);
    return 0;
}

void wim_close(WimCtx* ctx)
{
    wim_ctx_free(ctx);
}
