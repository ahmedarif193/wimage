/*
 * Copyright (C) 2026 Ahmed ARIF <arif.ing@outlook.com>
 *
 * wim_write.c - WIM writing implementation (pure C)
 *
 */

#include "wim_write.h"
#include "wim_capture.h"
#include "wim_io.h"
#include "sha1.h"
#include "xpress_huff.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef _WIN32
#include <pthread.h>
#endif

/* ================================================================
 *  Multi-threaded chunk compression
 * ================================================================ */

typedef struct {
    const uint8_t* in;
    uint32_t in_len;
    uint8_t* out;
    uint32_t out_cap;
    uint32_t out_len;
    int      stored_raw;
    int      owns_out;
    int      failed;
} ChunkWork;

static void compress_one_chunk(ChunkWork* w, XpressCompressScratch* scratch)
{
    if (w->stored_raw && !w->owns_out)
        return;

    if (!w->out) {
        if (!xpress_huff_chunk_may_compress(w->in, w->in_len)) {
            w->out = (uint8_t*)w->in;
            w->out_cap = w->in_len;
            w->out_len = w->in_len;
            w->stored_raw = 1;
            return;
        }

        w->out = (uint8_t*)malloc(w->in_len + 4096);
        if (!w->out) {
            w->failed = 1;
            return;
        }
        w->out_cap = w->in_len + 4096;
        w->owns_out = 1;
    }

    XpressStatus st;
    if (scratch) {
        st = xpress_huff_compress_prechecked_with_scratch(
            w->in, w->in_len, w->out, w->out_cap, &w->out_len, scratch);
    } else {
        st = xpress_huff_compress(w->in, w->in_len,
                                  w->out, w->out_cap, &w->out_len);
    }
    if (st != XPRESS_OK || w->out_len >= w->in_len) {
        if (w->owns_out) {
            free(w->out);
            w->owns_out = 0;
        }
        w->out = (uint8_t*)w->in;
        w->out_cap = w->in_len;
        w->out_len = w->in_len;
        w->stored_raw = 1;
    } else {
        w->stored_raw = 0;
    }
}

#ifndef _WIN32
typedef struct {
    ChunkWork* work;
    uint64_t num_chunks;
    uint32_t max_chunk_size;
    int      start_index;
    int      stride;
} ChunkWorkerArgs;

static void compress_chunk_stripe(ChunkWork* work, uint64_t num_chunks,
                                  uint32_t max_chunk_size,
                                  int start_index, int stride)
{
    XpressCompressScratch* scratch =
        xpress_huff_create_scratch(max_chunk_size);

    for (uint64_t i = (uint64_t)start_index; i < num_chunks; i += (uint64_t)stride)
        compress_one_chunk(&work[i], scratch);

    xpress_huff_destroy_scratch(scratch);
}

static void* compress_worker(void* arg)
{
    const ChunkWorkerArgs* worker = (const ChunkWorkerArgs*)arg;
    compress_chunk_stripe(worker->work, worker->num_chunks,
                          worker->max_chunk_size,
                          worker->start_index, worker->stride);
    return NULL;
}
#endif

/* ================================================================
 *  Blob writing
 * ================================================================ */

static int write_blob(WimCtx* ctx, const uint8_t* data_ptr, uint64_t size,
                      uint8_t sha1_out[20], int is_metadata)
{
    uint64_t blob_offset = (uint64_t)ftell(ctx->file);
    uint64_t written_size;
    int hash_ready = 0;

    if (ctx->use_xpress && size > 0) {
        /* Compress in chunks */
        uint32_t chunk_size = ctx->header.chunk_size;
        uint64_t num_chunks = (size + chunk_size - 1) / chunk_size;
        int use_64bit = (size > 0xFFFFFFFFULL);
        size_t entry_size = use_64bit ? 8 : 4;
        size_t table_size = (size_t)((num_chunks - 1) * entry_size);

        /* Compress all chunks (parallel if num_threads > 1) */
        ChunkWork* work = (ChunkWork*)calloc((size_t)num_chunks, sizeof(ChunkWork));
        if (!work) {
            return -1;
        }

        /* Prepare work items */
        uint64_t in_offset = 0;
        for (uint64_t i = 0; i < num_chunks; i++) {
            uint64_t remaining = size - in_offset;
            uint32_t this_chunk = (remaining < chunk_size) ?
                                  (uint32_t)remaining : chunk_size;
            work[i].in = data_ptr + in_offset;
            work[i].in_len = this_chunk;
            in_offset += this_chunk;
        }

        /* Dispatch compression */
        int nthreads = ctx->num_threads > 1 ? ctx->num_threads : 1;
#ifndef _WIN32
        if (nthreads > 1 && num_chunks > 1) {
            int thread_count = nthreads;
            if ((uint64_t)thread_count > num_chunks)
                thread_count = (int)num_chunks;

            pthread_t* tids =
                (pthread_t*)malloc((size_t)thread_count * sizeof(pthread_t));
            ChunkWorkerArgs* args =
                (ChunkWorkerArgs*)calloc((size_t)thread_count, sizeof(ChunkWorkerArgs));
            int started = 0;

            if (tids && args) {
                for (int j = 0; j < thread_count; j++) {
                    args[j].work = work;
                    args[j].num_chunks = num_chunks;
                    args[j].max_chunk_size = chunk_size;
                    args[j].start_index = j;
                    args[j].stride = thread_count;
                    if (pthread_create(&tids[started], NULL, compress_worker, &args[j]) != 0)
                        break;
                    started++;
                }
            }

            if (started == thread_count) {
                sha1_hash(data_ptr, size, sha1_out);
                hash_ready = 1;
            } else {
                for (int j = started; j < thread_count; j++)
                    compress_chunk_stripe(work, num_chunks, chunk_size, j, thread_count);
            }

            for (int j = 0; j < started; j++)
                pthread_join(tids[j], NULL);

            free(tids);
            free(args);
        } else
#endif
        {
            XpressCompressScratch* scratch =
                xpress_huff_create_scratch(chunk_size);
            for (uint64_t i = 0; i < num_chunks; i++)
                compress_one_chunk(&work[i], scratch);
            xpress_huff_destroy_scratch(scratch);
        }

        if (!hash_ready) {
            sha1_hash(data_ptr, size, sha1_out);
            hash_ready = 1;
        }

        for (uint64_t i = 0; i < num_chunks; i++) {
            if (work[i].failed)
                goto comp_fail;
        }

        if (!is_metadata) {
            int existing = wim_ctx_find_blob(ctx, sha1_out);
            if (existing >= 0) {
                ctx->blobs[existing].ref_count++;
                for (uint64_t i = 0; i < num_chunks; i++)
                    free(work[i].owns_out ? work[i].out : NULL);
                free(work);
                return 0;
            }
        }

        /* Check if compression actually saves space (table + chunks < original).
         * If not, fall back to writing raw uncompressed data. */
        uint64_t total_comp = table_size;
        for (uint64_t i = 0; i < num_chunks; i++)
            total_comp += work[i].out_len;

        if (total_comp >= size) {
            /* Compression didn't help - write raw */
            for (uint64_t i = 0; i < num_chunks; i++)
                free(work[i].owns_out ? work[i].out : NULL);
            free(work);

            if (size > 0)
                fwrite(data_ptr, 1, (size_t)size, ctx->file);
            written_size = size;
            goto blob_done;
        }

        /* Build and write chunk offset table */
        if (table_size > 0) {
            uint8_t* table_buf = (uint8_t*)calloc(1, table_size);
            if (!table_buf) goto comp_fail;

            uint64_t running_offset = 0;
            for (uint64_t i = 0; i < num_chunks - 1; i++) {
                running_offset += work[i].out_len;
                if (use_64bit) {
                    memcpy(table_buf + i * 8, &running_offset, 8);
                } else {
                    uint32_t val = (uint32_t)running_offset;
                    memcpy(table_buf + i * 4, &val, 4);
                }
            }
            fwrite(table_buf, 1, table_size, ctx->file);
            free(table_buf);
        }

        /* Write compressed chunks */
        for (uint64_t i = 0; i < num_chunks; ) {
            if (work[i].stored_raw && !work[i].owns_out) {
                size_t run_size = work[i].in_len;
                uint64_t j = i + 1;
                while (j < num_chunks &&
                       work[j].stored_raw &&
                       !work[j].owns_out &&
                       work[j].in == work[j - 1].in + work[j - 1].in_len) {
                    run_size += work[j].in_len;
                    j++;
                }
                fwrite(work[i].in, 1, run_size, ctx->file);
                i = j;
            } else {
                fwrite(work[i].out, 1, work[i].out_len, ctx->file);
                i++;
            }
        }

        written_size = total_comp;

        for (uint64_t i = 0; i < num_chunks; i++)
            free(work[i].owns_out ? work[i].out : NULL);
        free(work);
        goto blob_done;

comp_fail:
        for (uint64_t i = 0; i < num_chunks; i++)
            free((work && work[i].owns_out) ? work[i].out : NULL);
        free(work);
        return -1;

    } else {
        sha1_hash(data_ptr, size, sha1_out);
        hash_ready = 1;

        if (!is_metadata) {
            int existing = wim_ctx_find_blob(ctx, sha1_out);
            if (existing >= 0) {
                ctx->blobs[existing].ref_count++;
                return 0;
            }
        }

        /* Write uncompressed */
        if (size > 0)
            fwrite(data_ptr, 1, (size_t)size, ctx->file);
        written_size = size;
    }

blob_done:;
    if (!hash_ready)
        return -1;
    WimBlob blob;
    memset(&blob, 0, sizeof(blob));
    memcpy(blob.sha1.hash, sha1_out, 20);
    blob.original_size = size;
    blob.compressed_size = written_size;
    blob.offset = blob_offset;
    blob.ref_count = 1;
    blob.flags = 0;
    if (is_metadata)
        blob.flags |= WIM_RESHDR_FLAG_METADATA;
    if (ctx->use_xpress && written_size != size)
        blob.flags |= WIM_RESHDR_FLAG_COMPRESSED;

    wim_ctx_add_blob(ctx, &blob);
    return 0;
}

/* Callback adapter for wim_capture_dir */
static int blob_writer_cb(const uint8_t* data, uint64_t size,
                          uint8_t sha1_out[20], void* user)
{
    WimCtx* ctx = (WimCtx*)user;
    return write_blob(ctx, data, size, sha1_out, 0);
}

/* ================================================================
 *  Dentry serialization
 * ================================================================ */

#define DENTRY_FIXED_SIZE 102

static size_t dentry_serialized_size(const WimDentry* d)
{
    uint16_t name_nbytes = (uint16_t)(d->name_utf16_len * 2);
    size_t total = DENTRY_FIXED_SIZE;
    if (name_nbytes > 0)
        total += name_nbytes + 2; /* name + null terminator */
    /* Pad to 8-byte alignment */
    if (total % 8 != 0)
        total += 8 - (total % 8);
    return total;
}

static void serialize_dentry(const WimDentry* d, uint64_t subdir_off_override,
                             uint8_t* buf, size_t padded_len)
{
    memset(buf, 0, padded_len);

    uint16_t name_nbytes = (uint16_t)(d->name_utf16_len * 2);
    size_t offset = 0;

    /* length */
    uint64_t len64 = padded_len;
    memcpy(buf + offset, &len64, 8);                offset += 8;
    /* attributes */
    memcpy(buf + offset, &d->attributes, 4);         offset += 4;
    /* security_id */
    memcpy(buf + offset, &d->security_id, 4);        offset += 4;
    /* subdir_offset */
    memcpy(buf + offset, &subdir_off_override, 8);   offset += 8;
    /* unused1, unused2 */
    offset += 16;
    /* creation_time */
    memcpy(buf + offset, &d->creation_time, 8);      offset += 8;
    /* last_access_time */
    memcpy(buf + offset, &d->last_access_time, 8);   offset += 8;
    /* last_write_time */
    memcpy(buf + offset, &d->last_write_time, 8);    offset += 8;
    /* sha1 */
    memcpy(buf + offset, d->sha1, 20);               offset += 20;
    /* reparse_tag (0) */
    offset += 4;
    /* hard_link_group_id (0) */
    offset += 8;
    /* num_streams */
    uint16_t zero16 = 0;
    memcpy(buf + offset, &zero16, 2);                offset += 2;
    /* short_name_nbytes */
    memcpy(buf + offset, &zero16, 2);                offset += 2;
    /* file_name_nbytes */
    memcpy(buf + offset, &name_nbytes, 2);           offset += 2;

    /* File name (UTF-16LE + null) */
    if (name_nbytes > 0) {
        memcpy(buf + offset, d->name_utf16, name_nbytes);
        offset += name_nbytes;
        memset(buf + offset, 0, 2);
    }
}

typedef struct {
    const WimDentry* parent;
    size_t parent_buf_offset;
} ChildGroup;

static int serialize_dentry_tree(const WimDentry* root, uint8_t** out_buf, size_t* out_size)
{
    /* Security data: total_length=8, num_entries=0 */
    size_t root_size = dentry_serialized_size(root);

    /* BFS: collect directory groups */
    size_t g_cap = 64, g_count = 0;
    ChildGroup* groups = (ChildGroup*)malloc(g_cap * sizeof(ChildGroup));
    if (!groups) return -1;

    size_t pos = 8; /* security data */
    size_t root_offset = pos;
    pos += root_size;

    if (root->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) {
        if (g_count >= g_cap) {
            g_cap *= 2;
            groups = (ChildGroup*)realloc(groups, g_cap * sizeof(ChildGroup));
            if (!groups) return -1;
        }
        groups[g_count].parent = root;
        groups[g_count].parent_buf_offset = root_offset;
        g_count++;
    }

    /* Compute group offsets */
    size_t go_cap = 64, go_count = 0;
    size_t* group_offsets = (size_t*)malloc(go_cap * sizeof(size_t));
    if (!group_offsets) { free(groups); return -1; }

    size_t gi = 0;
    while (gi < g_count) {
        const WimDentry* parent = groups[gi].parent;

        if (go_count >= go_cap) {
            go_cap *= 2;
            group_offsets = (size_t*)realloc(group_offsets, go_cap * sizeof(size_t));
            if (!group_offsets) { free(groups); return -1; }
        }
        group_offsets[go_count++] = pos;

        for (size_t c = 0; c < parent->child_count; c++) {
            const WimDentry* child = &parent->children[c];
            size_t child_size = dentry_serialized_size(child);
            size_t child_offset = pos;
            pos += child_size;

            if (child->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) {
                if (g_count >= g_cap) {
                    g_cap *= 2;
                    groups = (ChildGroup*)realloc(groups, g_cap * sizeof(ChildGroup));
                    if (!groups) { free(group_offsets); return -1; }
                }
                groups[g_count].parent = child;
                groups[g_count].parent_buf_offset = child_offset;
                g_count++;
            }
        }
        /* 8-byte terminator */
        pos += 8;
        gi++;
    }

    /* Serialize */
    uint8_t* buf = (uint8_t*)calloc(1, pos);
    if (!buf) { free(groups); free(group_offsets); return -1; }

    /* Security data */
    uint32_t sec_total = 8;
    uint32_t sec_count = 0;
    memcpy(buf, &sec_total, 4);
    memcpy(buf + 4, &sec_count, 4);

    /* Root dentry */
    uint64_t root_subdir = (g_count > 0) ? group_offsets[0] : 0;
    serialize_dentry(root, root_subdir, buf + root_offset, root_size);

    /* Child groups */
    gi = 0;
    size_t dir_index = 1; /* groups[0] is root's children */
    while (gi < go_count) {
        const WimDentry* parent = groups[gi].parent;
        size_t cpos = group_offsets[gi];

        for (size_t c = 0; c < parent->child_count; c++) {
            const WimDentry* child = &parent->children[c];
            size_t child_size = dentry_serialized_size(child);

            uint64_t child_subdir = child->subdir_offset;
            if (child->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) {
                if (dir_index < go_count)
                    child_subdir = group_offsets[dir_index];
                dir_index++;
            }

            serialize_dentry(child, child_subdir, buf + cpos, child_size);
            cpos += child_size;
        }
        /* 8-byte terminator already zero from calloc */
        gi++;
    }

    free(groups);
    free(group_offsets);

    *out_buf = buf;
    *out_size = pos;
    return 0;
}

/* ================================================================
 *  Write finalization helpers
 * ================================================================ */

static int write_metadata(WimCtx* ctx, int image_idx)
{
    if (image_idx < 0 || image_idx >= (int)ctx->image_count)
        return -1;

    uint8_t* meta_buf = NULL;
    size_t meta_size = 0;
    int ret = serialize_dentry_tree(&ctx->images[image_idx].root, &meta_buf, &meta_size);
    if (ret != 0)
        return ret;

    uint8_t sha1_out[20];
    ret = write_blob(ctx, meta_buf, meta_size, sha1_out, 1);
    free(meta_buf);
    if (ret != 0)
        return ret;

    /* Store metadata blob info */
    int blob_idx = wim_ctx_find_blob(ctx, sha1_out);
    if (blob_idx >= 0) {
        ctx->images[image_idx].metadata_blob = ctx->blobs[blob_idx];
    }

    return 0;
}

static int write_lookup_table(WimCtx* ctx)
{
    uint64_t offset = (uint64_t)ftell(ctx->file);

    for (size_t i = 0; i < ctx->blob_count; i++) {
        WimLookupEntry entry;
        memset(&entry, 0, sizeof(entry));

        reshdr_set(&entry.reshdr, ctx->blobs[i].compressed_size, ctx->blobs[i].flags,
                   ctx->blobs[i].offset, ctx->blobs[i].original_size);
        entry.part_number = 1;
        entry.ref_count = ctx->blobs[i].ref_count;
        memcpy(entry.sha1, ctx->blobs[i].sha1.hash, 20);

        fwrite(&entry, WIM_LOOKUP_ENTRY_SIZE, 1, ctx->file);
    }

    uint64_t size = (uint64_t)ctx->blob_count * WIM_LOOKUP_ENTRY_SIZE;
    reshdr_set(&ctx->header.lookup_table, size, 0, offset, size);

    return 0;
}

static char* generate_xml(WimCtx* ctx)
{
    size_t cap = 4096;
    size_t len = 0;
    char* xml = (char*)malloc(cap);
    if (!xml) return NULL;

#define XML_APPEND(s) do { \
    size_t _slen = strlen(s); \
    while (len + _slen + 1 > cap) { cap *= 2; xml = (char*)realloc(xml, cap); if (!xml) return NULL; } \
    memcpy(xml + len, s, _slen); len += _slen; xml[len] = '\0'; \
} while (0)

    char numbuf[64];

    XML_APPEND("<WIM>\n");

    /* Total bytes */
    uint64_t total = 0;
    for (size_t i = 0; i < ctx->blob_count; i++)
        total += ctx->blobs[i].compressed_size;

    snprintf(numbuf, sizeof(numbuf), "%llu", (unsigned long long)total);
    XML_APPEND("<TOTALBYTES>");
    XML_APPEND(numbuf);
    XML_APPEND("</TOTALBYTES>\n");

    for (size_t i = 0; i < ctx->image_count; i++) {
        snprintf(numbuf, sizeof(numbuf), "%u", (unsigned)(i + 1));
        XML_APPEND("<IMAGE INDEX=\"");
        XML_APPEND(numbuf);
        XML_APPEND("\">\n");

        WimImageInfo* info = NULL;
        WimImageInfo empty_info;
        if (i < ctx->image_info_count) {
            info = &ctx->image_infos[i];
        } else {
            memset(&empty_info, 0, sizeof(empty_info));
            info = &empty_info;
        }

        snprintf(numbuf, sizeof(numbuf), "%u", info->dir_count);
        XML_APPEND("<DIRCOUNT>");
        XML_APPEND(numbuf);
        XML_APPEND("</DIRCOUNT>\n");

        snprintf(numbuf, sizeof(numbuf), "%u", info->file_count);
        XML_APPEND("<FILECOUNT>");
        XML_APPEND(numbuf);
        XML_APPEND("</FILECOUNT>\n");

        snprintf(numbuf, sizeof(numbuf), "%llu", (unsigned long long)info->total_bytes);
        XML_APPEND("<TOTALBYTES>");
        XML_APPEND(numbuf);
        XML_APPEND("</TOTALBYTES>\n");

        XML_APPEND("<HARDLINKBYTES>0</HARDLINKBYTES>\n");

        /* CREATIONTIME */
        uint32_t hi = (uint32_t)(info->creation_time >> 32);
        uint32_t lo = (uint32_t)(info->creation_time & 0xFFFFFFFF);
        snprintf(numbuf, sizeof(numbuf), "0x%08X", hi);
        XML_APPEND("<CREATIONTIME><HIGHPART>");
        XML_APPEND(numbuf);
        XML_APPEND("</HIGHPART><LOWPART>");
        snprintf(numbuf, sizeof(numbuf), "0x%08X", lo);
        XML_APPEND(numbuf);
        XML_APPEND("</LOWPART></CREATIONTIME>\n");

        /* LASTMODIFICATIONTIME */
        hi = (uint32_t)(info->modification_time >> 32);
        lo = (uint32_t)(info->modification_time & 0xFFFFFFFF);
        snprintf(numbuf, sizeof(numbuf), "0x%08X", hi);
        XML_APPEND("<LASTMODIFICATIONTIME><HIGHPART>");
        XML_APPEND(numbuf);
        XML_APPEND("</HIGHPART><LOWPART>");
        snprintf(numbuf, sizeof(numbuf), "0x%08X", lo);
        XML_APPEND(numbuf);
        XML_APPEND("</LOWPART></LASTMODIFICATIONTIME>\n");

        if (info->name[0] != '\0') {
            XML_APPEND("<NAME>");
            XML_APPEND(info->name);
            XML_APPEND("</NAME>\n");
        }
        if (info->description[0] != '\0') {
            XML_APPEND("<DESCRIPTION>");
            XML_APPEND(info->description);
            XML_APPEND("</DESCRIPTION>\n");
        }

        XML_APPEND("</IMAGE>\n");
    }

    XML_APPEND("</WIM>\n");

#undef XML_APPEND

    return xml;
}

static int write_xml_data(WimCtx* ctx)
{
    char* xml = generate_xml(ctx);
    if (!xml) return -1;

    /* Convert to UTF-16LE with BOM */
    uint16_t* utf16 = NULL;
    size_t utf16_len = 0;
    if (utf8_to_utf16le(xml, &utf16, &utf16_len) != 0) {
        free(xml);
        return -1;
    }
    free(xml);

    uint64_t offset = (uint64_t)ftell(ctx->file);

    /* Write BOM */
    uint16_t bom = 0xFEFF;
    fwrite(&bom, 2, 1, ctx->file);

    /* Write UTF-16LE data */
    fwrite(utf16, 2, utf16_len, ctx->file);

    uint64_t size = 2 + utf16_len * 2;
    reshdr_set(&ctx->header.xml_data, size, 0, offset, size);

    free(utf16);
    return 0;
}

static int write_integrity_table(WimCtx* ctx)
{
    /* Integrity hashes cover from after the header to the start of XML data.
     * XML data and integrity table itself are excluded from the hash range. */
    uint64_t data_end = ctx->header.xml_data.offset;
    uint64_t data_start = WIM_HEADER_SIZE;
    uint64_t data_size = data_end - data_start;

    if (data_size == 0)
        return 0;

    uint32_t chunk_size = 10485760; /* 10 MB */
    uint32_t num_chunks = (uint32_t)((data_size + chunk_size - 1) / chunk_size);

    uint8_t* hashes = (uint8_t*)malloc(num_chunks * 20);
    uint8_t* read_buf = (uint8_t*)malloc(chunk_size);
    if (!hashes || !read_buf) {
        free(hashes);
        free(read_buf);
        return -1;
    }

    fseek(ctx->file, (long)data_start, SEEK_SET);
    uint64_t remaining = data_size;

    for (uint32_t i = 0; i < num_chunks; i++) {
        uint32_t this_chunk = (remaining < chunk_size) ?
                              (uint32_t)remaining : chunk_size;

        if (fread(read_buf, 1, this_chunk, ctx->file) != this_chunk) {
            free(hashes);
            free(read_buf);
            return -1;
        }

        sha1_hash(read_buf, this_chunk, hashes + i * 20);
        remaining -= this_chunk;
    }

    free(read_buf);

    /* Seek to end of file (after XML data) to write integrity table */
    fseek(ctx->file, 0, SEEK_END);
    uint64_t integ_offset = (uint64_t)ftell(ctx->file);

    /* Table header: size, num_entries, chunk_size */
    uint32_t table_size = 12 + num_chunks * 20;
    fwrite(&table_size, 4, 1, ctx->file);
    fwrite(&num_chunks, 4, 1, ctx->file);
    fwrite(&chunk_size, 4, 1, ctx->file);
    fwrite(hashes, 1, num_chunks * 20, ctx->file);

    free(hashes);

    reshdr_set(&ctx->header.integrity, table_size, 0, integ_offset, table_size);
    return 0;
}

static int write_header(WimCtx* ctx)
{
    fseek(ctx->file, 0, SEEK_SET);
    if (fwrite(&ctx->header, sizeof(WimHeader), 1, ctx->file) != 1)
        return -1;
    return 0;
}

static void count_dir_file(const WimDentry* d, uint32_t* dirs, uint32_t* files,
                           uint64_t* bytes)
{
    if (d->attributes & WIM_FILE_ATTRIBUTE_DIRECTORY) {
        (*dirs)++;
        for (size_t i = 0; i < d->child_count; i++)
            count_dir_file(&d->children[i], dirs, files, bytes);
    } else {
        (*files)++;
        *bytes += d->file_size;
    }
}

/* ================================================================
 *  Public API
 * ================================================================ */

int wim_create(WimCtx* ctx, const char* filename, int use_xpress)
{
    int num_threads = ctx->num_threads;

    wim_ctx_free(ctx);
    wim_ctx_init(ctx);
    ctx->num_threads = num_threads;

    ctx->file = fopen(filename, "w+b");
    if (!ctx->file) {
        fprintf(stderr, "Error: Cannot create '%s'\n", filename);
        return -1;
    }
    setvbuf(ctx->file, NULL, _IOFBF, 1 << 20);

    memset(&ctx->header, 0, sizeof(ctx->header));
    memcpy(ctx->header.magic, WIM_MAGIC, WIM_MAGIC_LEN);
    ctx->header.header_size = WIM_HEADER_SIZE;
    ctx->header.version = WIM_VERSION;
    ctx->header.flags = 0;
    if (use_xpress) {
        ctx->header.flags |= WIM_HDR_FLAG_COMPRESSION | WIM_HDR_FLAG_COMPRESS_XPRESS;
    }
    ctx->header.chunk_size = use_xpress ? 32768 : 0;

    /* Generate a pseudo-random GUID */
    srand((unsigned)time(NULL));
    for (int i = 0; i < 16; i++)
        ctx->header.guid[i] = (uint8_t)(rand() & 0xFF);

    ctx->header.part_number = 1;
    ctx->header.total_parts = 1;
    ctx->header.image_count = 0;

    ctx->writing = 1;
    ctx->use_xpress = use_xpress;

    /* Write placeholder header */
    uint8_t placeholder[WIM_HEADER_SIZE];
    memset(placeholder, 0, WIM_HEADER_SIZE);
    fwrite(placeholder, 1, WIM_HEADER_SIZE, ctx->file);

    return 0;
}

int wim_capture_tree(WimCtx* ctx, const char* source_dir,
                     const char* image_name, const char* image_desc)
{
    if (!ctx->writing || !ctx->file)
        return -1;

    /* Grow images array */
    size_t new_count = ctx->image_count + 1;
    WimImageData* new_images = (WimImageData*)realloc(ctx->images,
                                                       new_count * sizeof(WimImageData));
    if (!new_images) return -1;
    ctx->images = new_images;

    WimImageData* img = &ctx->images[ctx->image_count];
    memset(img, 0, sizeof(*img));
    wim_dentry_init(&img->root);
    img->metadata_loaded = 1;

    int ret = wim_capture_dir(source_dir, &img->root, blob_writer_cb, ctx);
    if (ret != 0)
        return ret;

    /* Grow image_infos array */
    WimImageInfo* new_infos = (WimImageInfo*)realloc(ctx->image_infos,
                                                      new_count * sizeof(WimImageInfo));
    if (!new_infos) return -1;
    ctx->image_infos = new_infos;

    WimImageInfo* info = &ctx->image_infos[ctx->image_count];
    memset(info, 0, sizeof(*info));
    if (image_name)
        snprintf(info->name, sizeof(info->name), "%s", image_name);
    if (image_desc)
        snprintf(info->description, sizeof(info->description), "%s", image_desc);

    info->dir_count = 0;
    info->file_count = 0;
    info->total_bytes = 0;
    count_dir_file(&img->root, &info->dir_count, &info->file_count, &info->total_bytes);
    info->creation_time = unix_to_filetime(time(NULL));
    info->modification_time = info->creation_time;

    ctx->image_count = new_count;
    ctx->image_info_count = new_count;
    ctx->header.image_count = (uint32_t)new_count;

    return 0;
}

int wim_finalize(WimCtx* ctx, int write_integrity)
{
    if (!ctx->writing || !ctx->file)
        return -1;

    /* Write metadata for each image */
    for (size_t i = 0; i < ctx->image_count; i++) {
        int ret = write_metadata(ctx, (int)i);
        if (ret != 0)
            return ret;
    }

    /* Write lookup table */
    int ret = write_lookup_table(ctx);
    if (ret != 0)
        return ret;

    /* Write XML data */
    ret = write_xml_data(ctx);
    if (ret != 0)
        return ret;

    /* Write integrity table if requested */
    if (write_integrity) {
        ret = write_integrity_table(ctx);
        if (ret != 0)
            return ret;
    }

    /* Write final header */
    ret = write_header(ctx);
    if (ret != 0)
        return ret;

    fclose(ctx->file);
    ctx->file = NULL;
    ctx->writing = 0;

    return 0;
}
