/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Extreamly fast multi-threaded malloc.
 */
#define _BSD_SOURCE // so we get MAP_ANON on linux
#include <sys/mman.h>
#include <stdio.h>
#include <errno.h>
#include "common.h"
#include "rlocal.h"
#include "lwt.h"

#define GET_SCALE(n) (sizeof(void *)*__CHAR_BIT__ - __builtin_clzl((n) - 1)) // log2 of <n>, rounded up
#ifndef NBD32
#define MAX_POINTER_BITS 48
#define REGION_SCALE 21 // 2mb regions
#else
#define MAX_POINTER_BITS 32
#define REGION_SCALE 12 // 4kb regions
#endif
#define REGION_SIZE (1 << REGION_SCALE)
#define HEADER_REGION_SCALE ((MAX_POINTER_BITS - REGION_SCALE) + GET_SCALE(sizeof(header_t)))
#define MAX_SCALE 31 // allocate blocks up to 4GB in size (arbitrary, could be bigger)

typedef struct block {
    struct block *next;
} block_t;

// region header
typedef struct header {
    uint8_t owner; // thread id of owner
    uint8_t scale; // log2 of the block size
} header_t;

typedef struct tl {
    block_t *free_blocks[MAX_SCALE+1];
    block_t *blocks_from[MAX_NUM_THREADS];
    block_t *blocks_to[MAX_NUM_THREADS];
} __attribute__((aligned(CACHE_LINE_SIZE))) tl_t ;

static header_t *headers_ = NULL;

static tl_t tl_[MAX_NUM_THREADS] = {};

static inline header_t *get_header (void *r) {
    return headers_ + ((size_t)r >> REGION_SCALE);
}

static void *get_new_region (int block_scale) {
    size_t sz = (1 << block_scale);
    if (sz < REGION_SIZE) {
        sz = REGION_SIZE;
    }
    void *region = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
    TRACE("m1", "get_new_region: mmapped new region %p (size %p)", region, sz);
    if (region == (void *)-1) {
        perror("get_new_region: mmap");
        exit(-1);
    }
    if ((size_t)region & (sz - 1)) {
        TRACE("m0", "get_new_region: region not aligned", 0, 0);
        munmap(region, sz);
        region = mmap(NULL, sz * 2, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
        if (region == (void *)-1) {
            perror("get_new_region: mmap");
            exit(-1);
        }
        TRACE("m0", "get_new_region: mmapped new region %p (size %p)", region, sz * 2);
        void *aligned = (void *)(((size_t)region + sz) & ~(sz - 1));
        size_t extra = (char *)aligned - (char *)region;
        if (extra) {
            munmap(region, extra);
            TRACE("m0", "get_new_region: unmapped extra memory %p (size %p)", region, extra);
        }
        extra = ((char *)region + sz) - (char *)aligned;
        if (extra) {
            munmap((char *)aligned + sz, extra);
            TRACE("m0", "get_new_region: unmapped extra memory %p (size %p)", (char *)aligned + sz, extra);
        }
        region = aligned;
    }
    assert(region);
    if (headers_ != NULL) {
        LOCALIZE_THREAD_LOCAL(tid_, int);
        header_t *h = get_header(region);
        TRACE("m1", "get_new_region: header %p (%p)", h, h - headers_);

        assert(h->scale == 0);
        h->scale = block_scale;
        h->owner = tid_;
    }

    return region;
}

__attribute__ ((constructor(101))) void mem_init (void) {
#ifdef USE_SYSTEM_MALLOC
    return;
#endif
    assert(headers_ == NULL);
    // Allocate a region for the region headers. This could be a big chunk of memory (256MB) on 64 bit systems,
    // but it just takes up virtual address space. Physical address space used by the headers is still proportional
    // to the amount of memory we alloc.
    headers_ = (header_t *)get_new_region(HEADER_REGION_SCALE); 
    TRACE("m1", "mem_init: header region %p", headers_, 0);
    memset(headers_, 0, (1 << HEADER_REGION_SCALE));
}

// Put <x> onto its owner's public free list (in the appropriate size bin).
//
// TODO: maybe we want to munmap() larger size blocks?
void nbd_free (void *x) {
#ifdef USE_SYSTEM_MALLOC
    TRACE("m1", "nbd_free: %p", x, 0);
#ifndef NDEBUG
    //memset(x, 0xcd, sizeof(void *)); // bear trap
#endif//NDEBUG
    free(x);
    return;
#endif//USE_SYSTEM_MALLOC
    TRACE("m1", "nbd_free: block %p region %p", x, (size_t)x & ~MASK(REGION_SCALE));

    assert(x);
    LOCALIZE_THREAD_LOCAL(tid_, int);
    block_t  *b = (block_t *)x;
    header_t *h = get_header(x);
    TRACE("m1", "nbd_free: header %p scale %llu", h, h->scale);
    assert(h->scale && h->scale <= MAX_SCALE);
#ifndef NDEBUG
    memset(b, 0xcd, (1 << h->scale)); // bear trap
#endif
    tl_t *tl = &tl_[tid_]; // thread-local data
    if (h->owner == tid_) {
        TRACE("m1", "nbd_free: private block, old free list head %p", tl->free_blocks[h->scale], 0);
        b->next = tl->free_blocks[h->scale];
        tl->free_blocks[h->scale] = b;
    } else {
        TRACE("m1", "nbd_free: owner %llu", h->owner, 0);
        // push <b> onto it's owner's queue
        VOLATILE(b->next) = NULL;
        if (EXPECT_FALSE(tl->blocks_to[h->owner] == NULL)) {
            VOLATILE(tl_[h->owner].blocks_from[tid_]) = b;
        } else {
            VOLATILE(tl->blocks_to[h->owner]->next) = b;
        }
        tl->blocks_to[h->owner] = b;
    }
}

// Allocate a block of memory at least size <n>. Blocks are binned in powers-of-two. Round up
// <n> to the nearest power-of-two. 
//
// First check the current thread's private free list for an available block. If no blocks are on
// the private free list, pull blocks off of the current thread's public free lists and put them
// on the private free list. If we didn't find any blocks on the public free lists, allocate a new
// region, break it up into blocks and put them on the private free list.
void *nbd_malloc (size_t n) {
#ifdef USE_SYSTEM_MALLOC
    TRACE("m1", "nbd_malloc: request size %llu (scale %llu)", n, GET_SCALE(n));
    void *x = malloc(n);
    TRACE("m1", "nbd_malloc: returning %p", x, 0);
    return x;
#endif
    if (EXPECT_FALSE(n == 0))
        return NULL;
    if (n < sizeof(block_t)) {
        n = sizeof(block_t);
    }
    int b_scale = GET_SCALE(n);
    assert(b_scale >= 2);
    assert(b_scale <= MAX_SCALE);
    TRACE("m1", "nbd_malloc: request size %llu (scale %llu)", n, b_scale);
    LOCALIZE_THREAD_LOCAL(tid_, int);
    tl_t *tl = &tl_[tid_]; // thread-local data

    // If our private free list is empty, try to find blocks on our public free list. If that fails,
    // allocate a new region.
    if (EXPECT_FALSE(tl->free_blocks[b_scale] == NULL)) {
        for (int i = 0; i < MAX_NUM_THREADS; ++ i) {
            block_t *x = tl->blocks_from[i];
            if (x != NULL) {
                block_t *next = x->next;
                if (next != NULL) {
                    do {
                        header_t *h = get_header(x);
                        x->next = tl->free_blocks[h->scale];
                        tl->free_blocks[h->scale] = x;
                        x = next;
                        next = x->next;
                    } while (next != NULL);
                    tl->blocks_from[i] = x;
                }
            }
        }
        // allocate a new region
        if (tl->free_blocks[b_scale] == NULL) {
            char  *region = get_new_region(b_scale);
            size_t b_size = 1 << b_scale;
            size_t region_size = (b_size < REGION_SIZE) ? REGION_SIZE : b_size;
            for (int i = region_size; i != 0; i -= b_size) {
                block_t *b = (block_t *)(region + i - b_size);
                b->next = tl->free_blocks[b_scale];
                tl->free_blocks[b_scale] = b;
            }
        }
        assert(tl->free_blocks[b_scale] != NULL);
    }

    // Pull a block off of our private free list.
    block_t *b = tl->free_blocks[b_scale];
    TRACE("m1", "nbd_malloc: returning block %p (region %p) from private list", b, (size_t)b & ~MASK(REGION_SCALE));
    ASSERT(b);
    ASSERT(get_header(b)->scale == b_scale);
    tl->free_blocks[b_scale] = b->next;
    return b;
}
