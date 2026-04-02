/*
 * Copyright (C) 2026 Ahmed ARIF <arif.ing@outlook.com>
 *
 * xpress_huff.c - Clean-room XPRESS Huffman (LZ77+Huffman) codec
 *
 * Written from scratch based on the WIM format analysis:
 *   - 512 Huffman symbols (0-255 literals, 256-511 match refs)
 *   - 256-byte packed code-length table (two 4-bit nibbles per byte)
 *   - Canonical Huffman codes, MSB-first bit consumption
 *   - 16-bit LE word refill when buffer < 16 bits
 *   - Inline length extension bytes share the stream pointer
 *
 */

#include "xpress_huff.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/*======================================================================
 * Byte-order helpers
 *======================================================================*/

static inline uint16_t rd16(const uint8_t* p)
{
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline uint32_t rd32(const uint8_t* p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline void wr16(uint8_t* p, uint16_t v)
{
    p[0] = (uint8_t)v;  p[1] = (uint8_t)(v >> 8);
}

/*======================================================================
 * Constants
 *======================================================================*/

enum {
    NSYM       = 512,
    MAXCL      = 15,
    TBLBITS    = 15,
    TBLSZ      = 1 << TBLBITS,   /* 32768 */
    HTBL_BYTES = 256
};

/*======================================================================
 * Canonical Huffman - shared by compressor and decompressor
 *======================================================================*/

/* Compute canonical starting codes per length (standard algorithm).
 * count[len] = number of symbols with that code length.
 * next[len]  = next code to assign for that length. */
static void canonical_starts(const uint8_t cl[NSYM], uint32_t next[MAXCL + 1])
{
    uint32_t cnt[MAXCL + 1] = {0};
    for (int i = 0; i < NSYM; i++)
        if (cl[i] > 0 && cl[i] <= MAXCL) cnt[cl[i]]++;

    uint32_t code = 0;
    for (int len = 1; len <= MAXCL; len++) {
        code = (code + cnt[len - 1]) << 1;
        next[len] = code;
    }
}

/*======================================================================
 * DECOMPRESSOR
 *
 * Bit buffer model (MSB-first):
 *   - 'bits' is a 32-bit register; valid data sits at the TOP
 *   - peek N bits:   bits >> (32 - N)
 *   - consume N:     bits <<= N;  nbits -= N
 *   - refill:        bits |= rd16(p) << (16 - nbits);  nbits += 16
 *
 * Decode table is indexed by the top TBLBITS bits of the buffer.
 * For a symbol with canonical code C (length L), fill all entries
 * where the top L bits of the index equal C.
 *======================================================================*/

typedef struct { uint16_t sym; uint8_t len; } DEntry;

static int build_dtable(const uint8_t cl[NSYM], DEntry tbl[TBLSZ])
{
    uint32_t next[MAXCL + 1];
    canonical_starts(cl, next);
    memset(tbl, 0, sizeof(DEntry) * TBLSZ);

    for (int s = 0; s < NSYM; s++) {
        int L = cl[s];
        if (L == 0) continue;
        if (L > MAXCL) return 0;
        uint32_t c = next[L]++;
        /* top-L-bit match in a TBLBITS-wide index */
        uint32_t base = c << (TBLBITS - L);
        uint32_t span = 1u << (TBLBITS - L);
        if (base + span > TBLSZ) return 0;
        for (uint32_t j = 0; j < span; j++) {
            tbl[base + j].sym = (uint16_t)s;
            tbl[base + j].len = (uint8_t)L;
        }
    }
    return 1;
}

/* Core decompressor: tbl must point to TBLSZ DEntry elements */
static XpressStatus decompress_core(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_len,
    DEntry* tbl)
{
    uint8_t cl[NSYM];
    for (int i = 0; i < HTBL_BYTES; i++) {
        cl[2 * i]     = in[i] & 0x0F;
        cl[2 * i + 1] = (in[i] >> 4) & 0x0F;
    }

    if (!build_dtable(cl, tbl)) return XPRESS_BAD_DATA;

    const uint8_t* p   = in + HTBL_BYTES;
    const uint8_t* end = in + in_len;

    uint32_t bits = 0;
    int nbits = 0;

#define ENSURE_BITS(n) do { \
    if (nbits < (n) && p + 1 < end) { \
        bits |= (uint32_t)rd16(p) << (16 - nbits); \
        p += 2; nbits += 16; \
    } \
} while(0)

    uint32_t opos = 0;
    while (opos < out_len) {
        ENSURE_BITS(MAXCL);

        uint32_t idx = bits >> (32 - TBLBITS);
        uint16_t sym = tbl[idx].sym;
        int slen     = tbl[idx].len;
        if (slen == 0) return XPRESS_BAD_DATA;
        bits <<= slen;
        nbits -= slen;

        if (sym < 256) {
            out[opos++] = (uint8_t)sym;
        } else {
            int mi = sym - 256;
            int lh = mi & 0xF;
            int ol = mi >> 4;

            ENSURE_BITS(16);

            uint32_t moff;
            if (ol == 0) {
                moff = 1;
            } else {
                moff = (1u << ol) | (bits >> (32 - ol));
                bits <<= ol;
                nbits -= ol;
            }

            uint32_t mlen;
            if (lh < 15) {
                mlen = (uint32_t)lh + 3;
            } else {
                if (p >= end) return XPRESS_BAD_DATA;
                uint8_t xb = *p++;
                if (xb < 255) {
                    mlen = 15 + 3 + xb;
                } else {
                    if (p + 1 >= end) return XPRESS_BAD_DATA;
                    mlen = rd16(p); p += 2;
                    if (mlen == 0) {
                        if (p + 3 >= end) return XPRESS_BAD_DATA;
                        mlen = rd32(p); p += 4;
                    }
                    mlen += 3;
                }
            }

            if (moff > opos) return XPRESS_BAD_DATA;
            for (uint32_t j = 0; j < mlen && opos < out_len; j++) {
                out[opos] = out[opos - moff];
                opos++;
            }
        }
    }

#undef ENSURE_BITS
    return XPRESS_OK;
}

/* Heap-allocating decompressor (original API) */
#ifndef XPRESS_HUFF_DECOMPRESS_ONLY
XpressStatus xpress_huff_decompress(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_len)
{
    if (in_len < HTBL_BYTES) return XPRESS_BAD_DATA;

    DEntry* tbl = (DEntry*)malloc(sizeof(DEntry) * TBLSZ);
    if (!tbl) return XPRESS_BAD_DATA;

    XpressStatus st = decompress_core(in, in_len, out, out_len, tbl);
    free(tbl);
    return st;
}
#endif

/* Static decompressor: no malloc, caller provides workspace.
 * workspace must be at least XPRESS_DECOMPRESS_WORKSPACE_SIZE bytes. */
XpressStatus xpress_huff_decompress_static(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_len,
    void* workspace)
{
    if (in_len < HTBL_BYTES || !workspace) return XPRESS_BAD_DATA;
    return decompress_core(in, in_len, out, out_len, (DEntry*)workspace);
}

/*======================================================================
 * COMPRESSOR
 *
 * 1) Greedy LZ77 parse with hash table
 * 2) Frequency count -> build length-limited Huffman codes
 * 3) Pack all Huffman codes + offset extra bits into MSB-first bit array
 * 4) Simulate decompressor refill pattern to interleave 16-bit words
 *    and inline length-extension bytes
 *======================================================================*/

#ifndef XPRESS_HUFF_DECOMPRESS_ONLY
/* --- Huffman tree for code-length assignment --- */

typedef struct { uint32_t f; int16_t l, r, sym; } HNode;

static void depths(const HNode* n, int i, uint8_t* cl, int d)
{
    if (i < 0) return;
    if (n[i].sym >= 0) { cl[n[i].sym] = (uint8_t)(d > MAXCL ? MAXCL : d); return; }
    depths(n, n[i].l, cl, d + 1);
    depths(n, n[i].r, cl, d + 1);
}

/* Comparator for sorting leaf nodes by frequency */
static int cmp_hnode(const void* a, const void* b)
{
    uint32_t fa = ((const HNode*)a)->f, fb = ((const HNode*)b)->f;
    if (fa < fb) return -1;
    if (fa > fb) return 1;
    return 0;
}

static void make_lengths(const uint32_t freq[NSYM], uint8_t cl[NSYM])
{
    memset(cl, 0, NSYM);
    int ns = 0;
    int16_t ss[NSYM];
    for (int i = 0; i < NSYM; i++) if (freq[i]) ss[ns++] = (int16_t)i;
    if (ns == 0) { cl[0] = 1; return; }
    if (ns == 1) { cl[ss[0]] = 1; return; }

    int mx = 2 * ns;
    HNode* nd = (HNode*)calloc(mx, sizeof(HNode));
    if (!nd) {
        for (int i = 0; i < ns; i++) cl[ss[i]] = MAXCL;
        return;
    }

    for (int i = 0; i < ns; i++) {
        nd[i].f = freq[ss[i]]; nd[i].sym = ss[i];
        nd[i].l = nd[i].r = -1;
    }
    /* Sort leaves by frequency for O(n) two-queue merge */
    qsort(nd, ns, sizeof(HNode), cmp_hnode);

    /* Two-queue Huffman: q1 = sorted leaves, q2 = internal nodes */
    int q1h = 0;                 /* head of leaf queue */
    int q2h = ns, q2t = ns;     /* head/tail of internal queue (stored after leaves) */
    int nc = ns;

    #define QPOP(idx) do { \
        if (q1h < ns && (q2h >= q2t || nd[q1h].f <= nd[q2h].f)) \
            idx = q1h++; \
        else \
            idx = q2h++; \
    } while(0)

    for (int m = 0; m < ns - 1; m++) {
        int a, b;
        QPOP(a); QPOP(b);
        int ni = nc++;
        nd[ni].f = nd[a].f + nd[b].f;
        nd[ni].l = (int16_t)a; nd[ni].r = (int16_t)b;
        nd[ni].sym = -1;
        q2t = nc; /* new internal node goes to tail of q2 */
    }
    #undef QPOP

    depths(nd, nc - 1, cl, 0);
    free(nd);

    /* Clamp & fix Kraft inequality */
    int fix = 0;
    for (int i = 0; i < NSYM; i++) if (cl[i] > MAXCL) { cl[i] = MAXCL; fix = 1; }
    if (fix) {
        for (int it = 0; it < 2000; it++) {
            uint32_t k = 0;
            for (int i = 0; i < NSYM; i++)
                if (cl[i]) k += 1u << (MAXCL - cl[i]);
            if (k <= (1u << MAXCL)) break;
            int best = -1;
            for (int i = 0; i < NSYM; i++)
                if (cl[i] > 0 && cl[i] < MAXCL)
                    if (best < 0 || cl[i] < cl[best]) best = i;
            if (best < 0) break;
            cl[best]++;
        }
        /* Final Kraft check: if still oversubscribed, fall back to MAXCL for all */
        uint32_t k = 0;
        for (int i = 0; i < NSYM; i++)
            if (cl[i]) k += 1u << (MAXCL - cl[i]);
        if (k > (1u << MAXCL)) {
            for (int i = 0; i < NSYM; i++)
                if (cl[i]) cl[i] = MAXCL;
        }
    }
}

/* Assign canonical codes (MSB-first, right-aligned in uint32_t) */
static void make_codes(const uint8_t cl[NSYM], uint32_t codes[NSYM])
{
    uint32_t next[MAXCL + 1];
    canonical_starts(cl, next);
    memset(codes, 0, NSYM * sizeof(uint32_t));
    for (int i = 0; i < NSYM; i++) {
        if (cl[i] == 0) continue;
        codes[i] = next[cl[i]]++;
    }
}

/* --- MSB-first bit array --- */

static void ba_put(uint32_t* a, int pos, uint32_t val, int nb)
{
    if (nb <= 0) return;
    int w = pos / 32, b = pos % 32;
    int sh = 32 - b - nb;
    if (sh >= 0) {
        a[w] |= val << sh;
    } else {
        a[w]     |= val >> (-sh);
        a[w + 1] |= val << (32 + sh);
    }
}

static uint16_t ba_get16(const uint32_t* a, int pos)
{
    int w = pos / 32, b = pos % 32;
    if (b <= 16)
        return (uint16_t)((a[w] >> (16 - b)) & 0xFFFF);
    else
        return (uint16_t)(((a[w] << (b - 16)) | (a[w + 1] >> (48 - b))) & 0xFFFF);
}

/* --- LZ77 token --- */
typedef struct {
    uint16_t is_match;
    union {
        uint8_t lit;
        struct { uint16_t off, len; } m;
    } u;
} Tok;

struct XpressCompressScratch {
    Tok* tok;
    uint32_t* ba;
    uint32_t token_cap;
    uint32_t bit_words;
    int32_t head[32768];
    int32_t prev[65536];
};

static inline int hbit(uint32_t v) { int r = 0; while (v > 1) { v >>= 1; r++; } return r; }

static inline uint16_t tok_to_sym(const Tok* t)
{
    if (!t->is_match) return t->u.lit;
    int ol = hbit(t->u.m.off);
    uint32_t lh = t->u.m.len - 3;
    return (uint16_t)(256 + (ol << 4) + (lh > 15 ? 15 : lh));
}

static inline uint32_t h3(const uint8_t* p)
{
    return (((uint32_t)p[0] << 10) ^ ((uint32_t)p[1] << 5) ^ p[2]) & 0x7FFF;
}

/* Fast match extension using 8-byte word comparisons */
static inline uint32_t match_len(const uint8_t* a, const uint8_t* b, uint32_t max_len)
{
    uint32_t len = 0;
    while (len + 8 <= max_len) {
        uint64_t va, vb;
        memcpy(&va, a + len, 8);
        memcpy(&vb, b + len, 8);
        if (va != vb) {
            uint64_t diff = va ^ vb;
            len += __builtin_ctzll(diff) / 8;
            return len < max_len ? len : max_len;
        }
        len += 8;
    }
    while (len < max_len && a[len] == b[len]) len++;
    return len;
}

/*
 * Quick reject for chunks that are very unlikely to benefit from XPRESS.
 * Random-looking chunks dominate the current benchmark and otherwise burn
 * most of the compressor time only to be stored raw in the caller anyway.
 */
static int xpress_huff_likely_incompressible(const uint8_t* in, uint32_t in_len)
{
    enum { HASH_BITS = 10, HASH_SIZE = 1 << HASH_BITS };
    uint16_t last[HASH_SIZE];
    uint32_t repeats = 0;
    uint32_t stride;
    uint32_t repeat_goal;

    if (in_len < 2048)
        return 0;

    stride = in_len >= 16384 ? 8u : 4u;
    repeat_goal = in_len >= 16384 ? 4u : 6u;

    memset(last, 0xFF, sizeof(last));

    for (uint32_t pos = 0; pos + 4 <= in_len; pos += stride) {
        uint32_t word;
        memcpy(&word, in + pos, sizeof(word));

        uint32_t hv = (word * 2654435761u) >> (32 - HASH_BITS);
        uint16_t prev = last[hv];
        last[hv] = (uint16_t)pos;

        if (prev != 0xFFFF && pos >= (uint32_t)prev + stride) {
            uint32_t prev_word;
            memcpy(&prev_word, in + prev, sizeof(prev_word));
            if (prev_word == word && ++repeats >= repeat_goal)
                return 0;
        }
    }

    return 1;
}

int xpress_huff_chunk_may_compress(const uint8_t* in, uint32_t in_len)
{
    return !xpress_huff_likely_incompressible(in, in_len);
}

static int xpress_huff_reserve_tokens(XpressCompressScratch* scratch, uint32_t input_len)
{
    uint32_t need = input_len ? input_len : 1;
    if (scratch->token_cap >= need)
        return 1;

    Tok* tok = (Tok*)realloc(scratch->tok, (size_t)need * sizeof(Tok));
    if (!tok)
        return 0;

    scratch->tok = tok;
    scratch->token_cap = need;
    return 1;
}

static int xpress_huff_reserve_bits(XpressCompressScratch* scratch, uint32_t max_bits)
{
    size_t need_words = ((size_t)max_bits + 31) / 32 + 2;
    if (need_words == 0)
        need_words = 2;
    if (scratch->bit_words >= need_words)
        return 1;

    uint32_t* ba = (uint32_t*)realloc(scratch->ba, need_words * sizeof(uint32_t));
    if (!ba)
        return 0;

    scratch->ba = ba;
    scratch->bit_words = (uint32_t)need_words;
    return 1;
}

XpressCompressScratch* xpress_huff_create_scratch(uint32_t max_input_len)
{
    XpressCompressScratch* scratch =
        (XpressCompressScratch*)calloc(1, sizeof(*scratch));
    if (!scratch)
        return NULL;

    if (!xpress_huff_reserve_tokens(scratch, max_input_len) ||
        !xpress_huff_reserve_bits(scratch, max_input_len * 32u + 64u)) {
        xpress_huff_destroy_scratch(scratch);
        return NULL;
    }

    return scratch;
}

void xpress_huff_destroy_scratch(XpressCompressScratch* scratch)
{
    if (!scratch)
        return;

    free(scratch->ba);
    free(scratch->tok);
    free(scratch);
}

XpressStatus xpress_huff_compress_prechecked_with_scratch(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_capacity,
    uint32_t* out_len,
    XpressCompressScratch* scratch)
{
    if (in_len == 0) { *out_len = 0; return XPRESS_OK; }
    if (!scratch)
        return XPRESS_BAD_DATA;

    /* === Pass 1: LZ77 with hash chains + lazy matching === */
    #define WSIZE 65536
    #define MAX_CHAIN 4
    #define GOOD_MATCH 16

    if (!xpress_huff_reserve_tokens(scratch, in_len))
        return XPRESS_BAD_DATA;

    Tok* tok = scratch->tok;
    int32_t* head = scratch->head;
    int32_t* prev = scratch->prev;
    for (int i = 0; i < 32768; i++) head[i] = -1;
    memset(prev, 0xFF, WSIZE * sizeof(int32_t)); /* -1 */

    /* Insert position into hash chain (guard: need 3 bytes for h3) */
    #define HC_INSERT(p) do { \
        if ((p) + 2 < in_len) { \
            uint32_t hv_ = h3(in + (p)); \
            prev[(p) & (WSIZE-1)] = head[hv_]; \
            head[hv_] = (int32_t)(p); \
        } \
    } while(0)

    /* Find best match at position p, store length in bl_, offset in bo_ */
    #define HC_FIND_BEST(p, bl_, bo_) do { \
        bl_ = 0; bo_ = 0; \
        if ((p) + 2 < in_len) { \
            uint32_t hv_ = h3(in + (p)); \
            int32_t cur_ = head[hv_]; \
            uint32_t mx_ = in_len - (p); \
            if (mx_ > 65535) mx_ = 65535; \
            for (int chain_ = 0; chain_ < MAX_CHAIN && cur_ >= 0; chain_++) { \
                uint32_t d_ = (p) - (uint32_t)cur_; \
                if (d_ > 65535) break; \
                if (d_ >= 1) { \
                    uint32_t ml_ = match_len(in + (p), in + (uint32_t)cur_, mx_); \
                    if (ml_ > bl_) { bl_ = ml_; bo_ = d_; \
                        if (bl_ >= GOOD_MATCH) break; } \
                } \
                cur_ = prev[(uint32_t)cur_ & (WSIZE-1)]; \
            } \
            if (bl_ < 3) { bl_ = 0; bo_ = 0; } \
        } \
    } while(0)

    uint32_t ntok = 0, pos = 0;
    while (pos < in_len) {
        uint32_t bl, bo;
        HC_FIND_BEST(pos, bl, bo);
        HC_INSERT(pos);

        if (bl >= 3) {
            /* Lazy matching: check if next position yields a better match */
            if (pos + 1 < in_len && bl < GOOD_MATCH) {
                uint32_t next_bl, next_bo;
                HC_FIND_BEST(pos + 1, next_bl, next_bo);
                if (next_bl > bl + 1) {
                    /* Emit literal for current pos, use next match instead */
                    Tok* t = &tok[ntok++];
                    t->is_match = 0; t->u.lit = in[pos];
                    HC_INSERT(pos + 1);
                    pos++;
                    bl = next_bl; bo = next_bo;
                }
            }

            Tok* t = &tok[ntok++];
            t->is_match = 1; t->u.m.off = (uint16_t)bo; t->u.m.len = (uint16_t)bl;

            /* Update hash chain for positions within the match (skip middle for long matches) */
            uint32_t update_limit = bl < 32 ? bl : 32;
            for (uint32_t i = 1; i < update_limit && pos + i + 2 < in_len; i++) {
                HC_INSERT(pos + i);
            }
            pos += bl;
        } else {
            Tok* t = &tok[ntok++];
            t->is_match = 0; t->u.lit = in[pos++];
        }
    }
    #undef HC_INSERT
    #undef HC_FIND_BEST
    #undef WSIZE
    #undef MAX_CHAIN

    /* === Pass 2: count frequencies (no sb[] allocation - recompute symbols) === */
    uint32_t freq[NSYM] = {0};
    uint32_t max_bits = 0;
    for (uint32_t i = 0; i < ntok; i++) {
        uint16_t s = tok_to_sym(&tok[i]);
        freq[s]++;
        max_bits += MAXCL; /* upper bound: code length */
        if (tok[i].is_match)
            max_bits += hbit(tok[i].u.m.off); /* offset extra bits */
    }

    uint8_t cl[NSYM];
    make_lengths(freq, cl);

    /* Verify Kraft inequality before encoding: if code lengths are invalid,
     * bail out and let the caller store the chunk raw. */
    {
        uint32_t kraft = 0;
        for (int i = 0; i < NSYM; i++)
            if (cl[i]) kraft += 1u << (MAXCL - cl[i]);
        if (kraft > (1u << MAXCL))
            return XPRESS_BUFFER_TOO_SMALL;
    }

    uint32_t codes[NSYM];
    make_codes(cl, codes);

    /* === Pass 3: MSB-first bit array (no lx[] allocation - emit inline) === */
    if (!xpress_huff_reserve_bits(scratch, max_bits))
        return XPRESS_BAD_DATA;
    uint32_t* ba = scratch->ba;
    memset(ba, 0, (((size_t)max_bits + 31) / 32 + 2) * sizeof(uint32_t));

    int tbits = 0;
    for (uint32_t i = 0; i < ntok; i++) {
        uint16_t s = tok_to_sym(&tok[i]);
        int clen = cl[s]; if (clen == 0) clen = 1;
        ba_put(ba, tbits, codes[s], clen);
        tbits += clen;

        if (tok[i].is_match) {
            int ol = hbit(tok[i].u.m.off);
            if (ol > 0) {
                uint32_t ex = tok[i].u.m.off & ((1u << ol) - 1);
                ba_put(ba, tbits, ex, ol);
                tbits += ol;
            }
        }
    }

    /* === Pass 4: simulate decompressor, emit interleaved output === */
    if (out_capacity < HTBL_BYTES + 4u) {
        return XPRESS_BUFFER_TOO_SMALL;
    }

    /* Write Huffman table */
    for (int i = 0; i < HTBL_BYTES; i++)
        out[i] = (uint8_t)(cl[2 * i] | (cl[2 * i + 1] << 4));

    uint8_t* wp = out + HTBL_BYTES;
    uint8_t* we = out + out_capacity;

    int brp = 0;   /* bit-read position in the bit array */
    int dn = 0;    /* simulated decompressor nbits */

    /*
     * Simulate demand-driven refill matching the decompressor:
     *   ENSURE_BITS(N): if nbits < N, emit one 16-bit word
     * The decompressor starts with bits=0, nbits=0 (no initial preload).
     */
#define CEMIT(need) do { \
    if (dn < (need)) { \
        if (wp + 2 > we) goto full; \
        wr16(wp, ba_get16(ba, brp)); \
        wp += 2; brp += 16; dn += 16; \
    } \
} while(0)

    {
        for (uint32_t i = 0; i < ntok; i++) {
            uint16_t s = tok_to_sym(&tok[i]);

            /* ENSURE_BITS(MAXCL=15) before Huffman decode */
            CEMIT(MAXCL);

            int clen = cl[s]; if (clen == 0) clen = 1;
            dn -= clen;

            if (tok[i].is_match) {
                int ol = hbit(tok[i].u.m.off);

                /* ENSURE_BITS(16) before offset extra bits */
                CEMIT(16);
                dn -= ol;

                /* Length extension bytes emitted inline (no lx[] needed) */
                uint32_t len = tok[i].u.m.len;
                if (len - 3 >= 15) {
                    uint32_t el = len - 3 - 15;
                    if (el < 255) {
                        if (wp + 1 > we) goto full;
                        *wp++ = (uint8_t)el;
                    } else {
                        /* MS-XCA: uint16 value is (match_length - 3), decompressor adds +3 */
                        uint32_t raw = len - 3;
                        if (wp + 3 > we) goto full;
                        *wp++ = 255;
                        *wp++ = (uint8_t)(raw & 0xFF);
                        *wp++ = (uint8_t)((raw >> 8) & 0xFF);
                    }
                }
            }
        }

        /* Flush remaining bits */
        while (brp < tbits) {
            if (wp + 2 > we) goto full;
            wr16(wp, ba_get16(ba, brp));
            wp += 2; brp += 16;
        }
    }

#undef CEMIT

    {
        uint32_t cs = (uint32_t)(wp - out);
        if (cs >= in_len) {
            *out_len = in_len;
            return XPRESS_BUFFER_TOO_SMALL; /* caller stores uncompressed */
        }
        *out_len = cs;
        return XPRESS_OK;
    }

full:
    return XPRESS_BUFFER_TOO_SMALL;
}

XpressStatus xpress_huff_compress_with_scratch(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_capacity,
    uint32_t* out_len,
    XpressCompressScratch* scratch)
{
    if (!xpress_huff_chunk_may_compress(in, in_len))
        return XPRESS_BUFFER_TOO_SMALL;

    return xpress_huff_compress_prechecked_with_scratch(
        in, in_len, out, out_capacity, out_len, scratch);
}

XpressStatus xpress_huff_compress(
    const uint8_t* in, uint32_t in_len,
    uint8_t* out, uint32_t out_capacity,
    uint32_t* out_len)
{
    XpressStatus st;
    XpressCompressScratch* scratch = xpress_huff_create_scratch(in_len);
    if (!scratch)
        return XPRESS_BAD_DATA;

    st = xpress_huff_compress_with_scratch(in, in_len, out, out_capacity,
                                           out_len, scratch);
    xpress_huff_destroy_scratch(scratch);
    return st;
}
#endif
