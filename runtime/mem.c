/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Extreamly fast multi-threaded malloc. 64 bit platforms only!
 */
#include <sys/mman.h>
#include <stdio.h>
#include <errno.h>
#include "common.h"
#include "runtime_local.h"
#include "lwt.h"

#define GET_SCALE(n) (sizeof(n)*8-__builtin_clzl((n)-1)) // log2 of <n>, rounded up
#define MAX_SCALE 31 // allocate blocks up to 4GB in size (arbitrary, could be bigger)
#define REGION_SCALE 22 // 4MB regions
#define REGION_SIZE (1 << REGION_SCALE)
#define HEADER_REGION_SCALE 22 // 4MB is space enough for headers for over 2,000,000 regions

typedef struct block {
    struct block *next;
} block_t;

// region header
typedef struct header {
    uint8_t owner; // thread id of owner
    uint8_t scale; // log2 of the block size
} header_t;

static header_t *region_header_ = NULL;

// TODO: experiment with different memory layouts (i.e. separate private and public lists)
static block_t free_list_[MAX_NUM_THREADS][MAX_SCALE+1][MAX_NUM_THREADS];

static void *get_new_region (int scale) {
    if (scale < REGION_SCALE) {
        scale = REGION_SCALE;
    }
    TRACE("m0", "get_new_region(): mmap new region scale: %llu", scale, 0);
    void *region = mmap(NULL, (1 << scale), PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
    if (region == (void *)-1) {
        perror("get_new_region: mmap");
        exit(-1);
    }
    assert(region);
    return region;
}

void mem_init (void) {
    assert(region_header_ == NULL);
    region_header_ = (header_t *)get_new_region(HEADER_REGION_SCALE);
    memset(region_header_, 0, REGION_SIZE);
}

// Put <x> onto its owner's public free list (in the appropriate size bin).
//
// TODO: maybe we want to munmap() larger size blocks to reclaim virtual address space?
void nbd_free (void *x) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    block_t  *b = (block_t *)x;
    assert(((size_t)b >> REGION_SCALE) < ((1 << HEADER_REGION_SCALE) / sizeof(header_t)));
    header_t *h = region_header_ + ((size_t)b >> REGION_SCALE);
    TRACE("m0", "nbd_free(): block %p scale %llu", x, h->scale);
    block_t  *l = &free_list_[h->owner][h->scale][tid_];
    TRACE("m0", "nbd_free(): free list %p first block %p", l, l->next);
    b->next = l->next;
    l->next = b;
}

// Allocate a block of memory at least size <n>. Blocks are binned in powers-of-two. Round up
// <n> to the nearest power-of-two. 
//
// First check the current thread's private free list for an available block. If no blocks are on
// the private free list, pull all the available blocks off of the current thread's public free 
// lists and put them on the private free list. If we didn't find any blocks on the public free 
// lists, open a new region, break it up into blocks and put them on the private free list.
void *nbd_malloc (size_t n) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    if (n < sizeof(block_t)) {
        n = sizeof(block_t);
    }
    int b_scale = GET_SCALE(n);
    assert(b_scale <= MAX_SCALE);
    TRACE("m0", "nbd_malloc(): size %llu scale %llu", n, b_scale);
    block_t *fls = free_list_[tid_][b_scale]; // our free lists
    block_t *pri = fls + tid_; // our private free list
    TRACE("m0", "nbd_malloc(): private free list %p first block %p", pri, pri->next);

    // If our private free list is empty, fill it up with blocks from our public free lists
    if (EXPECT_FALSE(pri->next == NULL)) {
        int cnt = 0;
        block_t *last = pri;
        for (int i = 0; i < MAX_NUM_THREADS; ++i) {
            TRACE("m0", "nbd_malloc(): searching public free lists (%llu)", i, 0);
            block_t *pub = fls + i; // one of our public free lists
            TRACE("m0", "nbd_malloc(): public free list %p first block %p", pub, pub->next);
            if (EXPECT_FALSE(pub == pri)) 
                continue;

            if (pub->next != NULL) {
                block_t *stolen = SYNC_SWAP(&pub->next, NULL);
                TRACE("m0", "nbd_malloc(): stole list %p first block %p", pub, stolen);
                if (stolen) {
                    last->next = stolen;
                    TRACE("m0", "nbd_malloc(): append to last block %p of private free list", last, 0);
                    while (last->next) {
                        ++cnt;
                        TRACE("m0", "nbd_malloc(): find last block in list: last %p last->next %p",
                              last, last->next);
                        last = last->next;
                    }
                }
            }
        }
        TRACE("m0", "nbd_malloc(): moved %llu blocks from public to private free lists", cnt, 0);

        if (b_scale >= REGION_SCALE) {
            if (cnt == 0) {
                assert(pri->next == NULL);
                pri->next = (block_t *)get_new_region(b_scale);
                assert(pri->next->next == NULL);
            }
            assert(pri->next);

        } else if (cnt < (1 << (REGION_SCALE - b_scale - 1))) {

            // Even if we took a few blocks from our public lists we still break open a new region.
            // This guarentees that we are amortizing the cost of accessing our public lists accross 
            // many nbd_malloc() calls.
            char *region = get_new_region(b_scale);
            size_t b_size = 1 << b_scale;
            for (int i = REGION_SIZE; i != 0; i -= b_size) {
                block_t *b = (block_t *)(region + i - b_size);
                b->next = pri->next;
                //TRACE("m1", "nbd_malloc(): put new block %p ahead of %p on private list", b, b->next);
                pri->next = b;
                *b = *b;
            }
        }

        assert(pri->next);
    }

    // Pull a block off of our private free list.
    block_t *b = pri->next;
    TRACE("m0", "nbd_malloc(): take block %p off of of private list (new head is %p)", b, pri->next);
    pri->next = b->next;

    assert(b);
    return b;
}
