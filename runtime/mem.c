/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Extreamly fast multi-threaded malloc. 64 bit platforms only!
 */
#define _BSD_SOURCE // so we get MAP_ANON on linux
#include <sys/mman.h>
#include <stdio.h>
#include <errno.h>
#include "common.h"
#include "rlocal.h"
#include "lwt.h"

#define GET_SCALE(n) (sizeof(void *)*__CHAR_BIT__ - __builtin_clzl((n) - 1)) // log2 of <n>, rounded up
#define MAX_SCALE 31 // allocate blocks up to 4GB in size (arbitrary, could be bigger)
#define REGION_SCALE 22 // 4MB regions
#define REGION_SIZE (1 << REGION_SCALE)
#define HEADER_REGION_SCALE 22 // 4MB is space enough for headers for over 2,000,000 regions
#define HEADER_REGION_SIZE (1 << HEADER_REGION_SCALE)
#define HEADER_COUNT (HEADER_REGION_SIZE / sizeof(header_t))

typedef struct block {
    struct block *next;
} block_t;

// region header
typedef struct header {
    uint8_t owner; // thread id of owner
    uint8_t scale; // log2 of the block size
} header_t;

typedef struct private_list {
    block_t *head;
    uint32_t next_pub;
    uint32_t count;
} private_list_t;

static header_t *headers_ = NULL;

static block_t *pub_free_list_[MAX_NUM_THREADS][MAX_SCALE+1][MAX_NUM_THREADS] = {};
static private_list_t pri_free_list_[MAX_NUM_THREADS][MAX_SCALE+1] = {};

static inline header_t *get_header (void *r) {
    return headers_ + (((size_t)r >> REGION_SCALE) & (HEADER_COUNT - 1));
}

static void *get_new_region (int block_scale) {
    size_t sz = (1 << block_scale);
    if (sz < REGION_SIZE) {
        sz = REGION_SIZE;
    }
    void *region = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
    TRACE("m1", "get_new_region: mmap new region %p (size %p)", region, sz);
    if (region == (void *)-1) {
        perror("get_new_region: mmap");
        exit(-1);
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

void mem_init (void) {
#ifdef USE_SYSTEM_MALLOC
    return;
#endif
    assert(headers_ == NULL);
    headers_ = (header_t *)get_new_region(HEADER_REGION_SCALE);
    TRACE("m1", "mem_init: header region %p", headers_, 0);
    memset(headers_, 0, HEADER_REGION_SIZE);
}

// Put <x> onto its owner's public free list (in the appropriate size bin).
//
// TODO: maybe we want to munmap() larger size blocks to reclaim virtual address space?
void nbd_free (void *x) {
#ifdef USE_SYSTEM_MALLOC
    TRACE("m1", "nbd_free: %p", x, 0);
#ifndef NDEBUG
    memset(x, 0xcd, sizeof(void *)); // bear trap
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
    if (h->owner == tid_) {
        TRACE("m1", "nbd_free: private block, old free list head %p", pri_free_list_[tid_][h->scale].head, 0);
        b->next = pri_free_list_[tid_][h->scale].head;
        pri_free_list_[tid_][h->scale].head = b;
    } else {
        TRACE("m1", "nbd_free: owner %llu free list head %p", h->owner, pub_free_list_[h->owner][h->scale][tid_]);
        do {
            b->next = pub_free_list_[h->owner][h->scale][tid_];
        } while (SYNC_CAS(&pub_free_list_[h->owner][h->scale][tid_], b->next, b) != b->next);
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
    private_list_t *pri = &pri_free_list_[tid_][b_scale]; // our private free list

    // If our private free list is empty, try to find blocks on our public free list. If that fails,
    // allocate a new region.
    if (EXPECT_FALSE(pri->head == NULL)) {
        block_t **pubs = pub_free_list_[tid_][b_scale]; // our public free lists
        while (1) {
            // look for blocks on our public free lists round robin
            pri->next_pub = (pri->next_pub+1) & (MAX_NUM_THREADS-1);

            TRACE("m1", "nbd_malloc: searching public free list %llu", pri->next_pub, 0);
            if (pri->next_pub == tid_) {
                uint32_t count = pri->count;
                pri->count = 0;
                // If we haven't gotten at least half a region's worth of block's from our public lists
                // we allocate a new region. This guarentees that we amortize the cost of accessing our
                // public lists accross enough nbd_malloc() calls.
                uint32_t min_count = b_scale > REGION_SCALE ? 1 << (b_scale-REGION_SCALE-1) : 1;
                if (count < min_count) {
                    char  *region = get_new_region(b_scale);
                    size_t b_size = 1 << b_scale;
                    size_t region_size = (b_size < REGION_SIZE) ? REGION_SIZE : b_size;
                    for (int i = region_size; i != 0; i -= b_size) {
                        block_t *b = (block_t *)(region + i - b_size);
                        b->next = pri->head;
                        pri->head = b;
                    }
                    pri->count = 0;
                    break;
                }
            } else if (pubs[pri->next_pub] != NULL) {
                block_t *stolen = SYNC_SWAP(&pubs[pri->next_pub], NULL);
                TRACE("m1", "nbd_malloc: stole list %p", stolen, 0);
                if (stolen == NULL)
                    continue;
                pri->head = stolen;
                break;
            }
        }
        assert(pri->head);
    }

    // Pull a block off of our private free list.
    block_t *b = pri->head;
    TRACE("m1", "nbd_malloc: returning block %p (region %p) from private list", b, (size_t)b & ~MASK(REGION_SCALE));
    assert(b);
    ASSERT(get_header(b)->scale == b_scale);
    pri->head = b->next;
    pri->count++;
    return b;
}
