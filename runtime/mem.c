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

static header_t *region_header_ = NULL;

static block_t *pub_free_list_[MAX_NUM_THREADS][MAX_SCALE+1][MAX_NUM_THREADS] = {};
static private_list_t pri_free_list_[MAX_NUM_THREADS][MAX_SCALE+1] = {};

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
    assert(x);
    LOCALIZE_THREAD_LOCAL(tid_, int);
    block_t  *b = (block_t *)x;
    assert(((size_t)b >> REGION_SCALE) < ((1 << HEADER_REGION_SCALE) / sizeof(header_t)));
    header_t *h = region_header_ + ((size_t)b >> REGION_SCALE);
#ifndef NDEBUG
    memset(b, 0xcd, (1 << h->scale));
#endif
    TRACE("m0", "nbd_free(): block %p scale %llu", b, h->scale);
    if (h->owner == tid_) {
        TRACE("m0", "nbd_free(): private block, free list head %p", 
                    h->owner, pri_free_list_[tid_][h->scale].head);
        b->next = pri_free_list_[tid_][h->scale].head;
        pri_free_list_[tid_][h->scale].head = b;
    } else {
        TRACE("m0", "nbd_free(): owner %llu free list head %p", 
                    h->owner, pub_free_list_[h->owner][h->scale][tid_]);
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
    assert(n);
    LOCALIZE_THREAD_LOCAL(tid_, int);
    if (n < sizeof(block_t)) {
        n = sizeof(block_t);
    }
    int b_scale = GET_SCALE(n);
    assert(b_scale <= MAX_SCALE);
    TRACE("m0", "nbd_malloc(): size %llu scale %llu", n, b_scale);
    private_list_t *pri = &pri_free_list_[tid_][b_scale]; // our private free list
    TRACE("m0", "nbd_malloc(): private free list first block %p", pri->head, 0);

    // If our private free list is empty, try to find blocks on our public free list. If that fails,
    // allocate a new region.
    if (EXPECT_FALSE(pri->head == NULL)) {
        block_t **pubs = pub_free_list_[tid_][b_scale]; // our public free lists
        while (1) {
            // look for blocks on our public free lists round robin
            pri->next_pub = (pri->next_pub+1) & (MAX_NUM_THREADS-1);

            TRACE("m0", "nbd_malloc(): searching public free list %llu", pri->next_pub, 0);
            if (pri->next_pub == tid_) {
                uint32_t count = pri->count;
                pri->count = 0;
                // If our private list is empty and we haven't gotten at least half a region's worth 
                // of block's from our public lists, we allocate a new region. This guarentees that
                // we amortize the cost of accessing our public lists accross enough nbd_malloc() 
                // calls.
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
                    break;
                }
                continue;
            }

            if (pubs[pri->next_pub] != NULL) {
                block_t *stolen = SYNC_SWAP(&pubs[pri->next_pub], NULL);
                TRACE("m0", "nbd_malloc(): stole list %p", stolen, 0);
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
    TRACE("m0", "nbd_malloc(): take block %p off of of private list (new head is %p)", b, b->next);
    assert(b);
    pri->head = b->next;
    pri->count++;
    return b;
}
