/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * C implementation of the lock-free skiplist data-structure created by Maurice Herlihy, 
 * Yossi Lev, and Nir Shavit. See "The Art of Multiprocessor Programming"
 * http://www.amazon.com/Art-Multiprocessor-Programming-Maurice-Herlihy/dp/0123705916/
 *
 * See also Kir Fraser's dissertation "Practical Lock Freedom"
 * www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
 *
 * This code depends on certain stores and loads being ordered. Be careful on non-x86 platforms
 * with weaker memory-models. This code probably won't work without adding some memory barriers.
 */
#include <stdio.h>
#include <string.h>

#include "common.h"
#include "struct.h"
#include "mem.h"

#define MAX_LEVEL 31

typedef struct node {
    uint64_t key;
    uint64_t value;
    int top_level;
    struct node *next[];
} node_t;

typedef struct skiplist {
    node_t *head;
    node_t *last;
    int top_level;
} skiplist_t;

static int random_level (int r) {
    if (r&1)
        return 0;
    int n = __builtin_ctz(r);
    if (n < MAX_LEVEL)
        return n;
    return MAX_LEVEL;
}

node_t *node_alloc (int top_level, uint64_t key, uint64_t value) {
    assert(top_level >= 0 && top_level <= MAX_LEVEL);
    size_t sz = sizeof(node_t) + (top_level + 1) * sizeof(node_t *);
    node_t *item = (node_t *)nbd_malloc(sz);
    memset(item, 0, sz);
    item->key   = key;
    item->value = value;
    item->top_level = top_level;
    return item;
}

skiplist_t *sl_alloc (void) {
    skiplist_t *skiplist = (skiplist_t *)nbd_malloc(sizeof(skiplist_t));
    skiplist->head = node_alloc(MAX_LEVEL, 0, 0);
    skiplist->last = node_alloc(MAX_LEVEL, (uint64_t)-1, 0);
    for (int level = 0; level <= MAX_LEVEL; ++level) {
        skiplist->head->next[level] = skiplist->last;
    }
    return skiplist;
}

static node_t *find_preds (node_t **preds, skiplist_t *skiplist, uint64_t key, int help_remove) {
    node_t *pred = skiplist->head;
    node_t *item = NULL;
    TRACE("s3", "find_preds: searching for key %p in skiplist (head is %p)", key, pred);
#ifndef NDEBUG
    int count = 0;
#endif

    // Traverse the levels of the skiplist from the top level to the bottom
    for (int level = MAX_LEVEL; level >= 0; --level) {
        TRACE("s3", "find_preds: level %llu", level, 0);
        item = pred->next[level];
        if (IS_TAGGED(item)) {
            TRACE("s3", "find_preds: pred %p is marked for removal (item %p); retry", pred, item);
            return find_preds(preds, skiplist, key, help_remove); // retry
        }
        do {
            node_t *next = item->next[level];
            TRACE("s3", "find_preds: visiting item %p (next %p)", item, next);
            TRACE("s3", "find_preds: key %p", item->key, 0);

            // Marked items are logically removed, but not fully unlinked yet.
            while (EXPECT_FALSE(IS_TAGGED(next))) {

                // Skip over partially removed items.
                if (!help_remove) {
                    item = (node_t *)STRIP_TAG(item->next);
                    next = item->next[level];
                    continue;
                }

                // Unlink partially removed items.
                node_t *other;
                if ((other = SYNC_CAS(&pred->next[level], item, STRIP_TAG(next))) == item) {
                    item = (node_t *)STRIP_TAG(next);
                    next = item->next[level];
                    TRACE("s3", "find_preds: unlinked item %p from pred %p", item, pred);
                    TRACE("s3", "find_preds: now item is %p next is %p", item, next);

                    // The thread that completes the unlink should free the memory.
                    if (level == 0) { nbd_defer_free(other); }
                } else {
                    TRACE("s3", "find_preds: lost race to unlink item from pred %p; its link changed to %p",pred,other);
                    if (IS_TAGGED(other))
                        return find_preds(preds, skiplist, key, help_remove); // retry
                    item = other;
                    next = item->next[level];
                }
            }

            // If we reached the key (or passed where it should be), we found a pred. Save it and continue down.
            if (item->key >= key) {
                TRACE("s3", "find_preds: found pred %p item %p", pred, item);
                if (preds != NULL) {
                    preds[level] = pred;
                }
                break;
            }

            assert(count++ < 18);
            pred = item;
            item = next;

        } while (1);
    }
    return item;
}

// Fast find that does not help unlink partially removed nodes and does not return the node's predecessors.
uint64_t sl_lookup (skiplist_t *skiplist, uint64_t key) {
    TRACE("s3", "sl_lookup: searching for key %p in skiplist %p", key, skiplist);
    node_t *item = find_preds(NULL, skiplist, key, FALSE);

    // If we found an <item> matching the <key> return its value.
    return (item->key == key) ? item->value : DOES_NOT_EXIST;
}

// Insert the <key> if it doesn't already exist in the <skiplist>
uint64_t sl_add_r (int r, skiplist_t *skiplist, uint64_t key, uint64_t value) {
    TRACE("s3", "sl_add: inserting key %p value %p", key, value);
    node_t *preds[MAX_LEVEL+1];
    node_t *item = NULL;
    do {
        node_t *next = find_preds(preds, skiplist, key, TRUE);

        // If a node matching <key> already exists in the skiplist, return its value.
        if (next->key == key) {
            TRACE("s3", "sl_add: there is already an item %p (value %p) with the same key", next, next->value);
            if (EXPECT_FALSE(item != NULL)) { nbd_free(item); }
            return next->value;
        }

        // First insert <item> into the bottom level.
        if (EXPECT_TRUE(item == NULL)) { item = node_alloc(random_level(r), key, value); }
        TRACE("s3", "sl_add: attempting to insert item between %p and %p", preds[0], next);
        item->next[0] = next;
        for (int level = 1; level <= item->top_level; ++level) {
            item->next[level] = preds[level]->next[level];
        }
        node_t *other = SYNC_CAS(&preds[0]->next[0], next, item);
        if (other == next) {
            TRACE("s3", "sl_add: successfully inserted item %p at level 0", item, 0);
            break; // success
        }
        TRACE("s3", "sl_add: failed to change pred's link: expected %p found %p", next, other);

    } while (1);

    // Insert <item> into the skiplist from the bottom level up.
    for (int level = 1; level <= item->top_level; ++level) {
        do {
            node_t *pred = preds[level];
            node_t *next = pred->next[level];
            while (EXPECT_FALSE(IS_TAGGED(next) || next->key < key)) {
                find_preds(preds, skiplist, key, TRUE);
                pred = preds[level];
                next = pred->next[level];
            }

            do {
                // There in no need to continue linking in the item if another thread removed it.
                node_t *old_next = ((volatile node_t *)item)->next[level];
                if (IS_TAGGED(old_next))
                    return DOES_NOT_EXIST; // success

                // Use a CAS so we to not inadvertantly remove a mark another thread placed on the item.
                if (next == old_next || SYNC_CAS(&item->next[level], old_next, next) == old_next)
                    break;
            } while (1);

            TRACE("s3", "sl_add: attempting to insert item between %p and %p", pred, next);
            node_t *other = SYNC_CAS(&pred->next[level], next, item);
            if (other == next) {
                TRACE("s3", "sl_add: successfully inserted item %p at level %llu", item, level);
                break; // success
            }
            TRACE("s3", "sl_add: failed to change pred's link: expected %p found %p", next, other);

        } while (1);
    }
    return value;
}

uint64_t sl_remove (skiplist_t *skiplist, uint64_t key) {
    TRACE("s3", "sl_remove: removing item with key %p from skiplist %p", key, skiplist);
    node_t *preds[MAX_LEVEL+1];
    node_t *item = find_preds(preds, skiplist, key, TRUE);
    if (item->key != key) {
        TRACE("s3", "sl_remove: remove failed, an item with a matching key does not exist in the skiplist", 0, 0);
        return DOES_NOT_EXIST;
    }

    // Mark <item> removed at each level of the skiplist from the top down. This must be atomic. If multiple threads
    // try to remove the same item only one of them should succeed. Marking the bottom level establishes which of 
    // them succeeds.
    for (int level = item->top_level; level >= 0; --level) {
        if (EXPECT_FALSE(IS_TAGGED(item->next[level]))) {
            TRACE("s3", "sl_remove: %p is already marked for removal by another thread", item, 0);
            if (level == 0)
                return DOES_NOT_EXIST;
            continue;
        }
        node_t *next = SYNC_FETCH_AND_OR(&item->next[level], TAG);
        if (EXPECT_FALSE(IS_TAGGED(next))) {
            TRACE("s3", "sl_remove: lost race -- %p is already marked for removal by another thread", item, 0);
            if (level == 0)
                return DOES_NOT_EXIST;
            continue;
        }
    }

    uint64_t value = item->value;

    // Unlink <item> from the top down.
    int level = item->top_level;
    while (level >= 0) {
        node_t *pred = preds[level];
        node_t *next = item->next[level];
        TRACE("s3", "sl_remove: link item's pred %p to it's successor %p", pred, STRIP_TAG(next));
        node_t *other = NULL;
        if ((other = SYNC_CAS(&pred->next[level], item, STRIP_TAG(next))) != item) {
            TRACE("s3", "sl_remove: unlink failed; pred's link changed from %p to %p", item, other);
            // By marking the item earlier, we logically removed it. It is safe to leave the item partially
            // unlinked. Another thread will finish physically removing it from the skiplist.
            return value;
        }
        --level; 
    }

    // The thread that completes the unlink should free the memory.
    nbd_defer_free(item); 
    return value;
}

void sl_print (skiplist_t *skiplist) {
    for (int level = MAX_LEVEL; level >= 0; --level) {
        node_t *item = skiplist->head;
        if (item->next[level] == skiplist->last)
            continue;
        printf("(%d) ", level);
        while (item) {
            node_t *next = item->next[level];
            printf("%s%p:0x%llx ", IS_TAGGED(next) ? "*" : "", item, item->key);
            item = (node_t *)STRIP_TAG(next);
        }
        printf("\n");
        fflush(stdout);
    }

    printf("\n");
    node_t *item = skiplist->head;
    while (item) {
        assert(item->top_level <= MAX_LEVEL);
        int is_marked = IS_TAGGED(item->next[0]);
        printf("%s%p:0x%llx (%d", is_marked ? "*" : "", item, item->key, item->top_level);
        for (int level = 1; level <= item->top_level; ++level) {
            node_t *next = (node_t *)STRIP_TAG(item->next[level]);
            printf(" %p:0x%llx", item->next[level], next ? next->key : 0);
            if (item == skiplist->head && item->next[level] == skiplist->last)
                break;
            if (item == skiplist->last && item->next[level] == NULL)
                break;
        }
        printf(")\n");
        fflush(stdout);
        item = (node_t *)STRIP_TAG(item->next[0]);
    }
}

#ifdef MAKE_skiplist_test
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "runtime.h"

#define NUM_ITERATIONS 10000000

static volatile int wait_;
static long num_threads_;
static skiplist_t *sl_;

void *worker (void *arg) {
    int id = (int)(size_t)arg;

    unsigned int rand_seed = id+1;//rdtsc_l();

    // Wait for all the worker threads to be ready.
    SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

    for (int i = 0; i < NUM_ITERATIONS/num_threads_; ++i) {
        int n = rand_r(&rand_seed);
        int key = (n & 0xF) + 1;
        if (n & (1 << 8)) {
            sl_add_r(n, sl_, key, 1);
        } else {
            sl_remove(sl_, key);
        }

        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    nbd_init();
    lwt_set_trace_level("s3");

    char* program_name = argv[0];
    pthread_t thread[MAX_NUM_THREADS];

    if (argc > 2) {
        fprintf(stderr, "Usage: %s num_threads\n", program_name);
        return -1;
    }

    num_threads_ = 2;
    if (argc == 2)
    {
        errno = 0;
        num_threads_ = strtol(argv[1], NULL, 10);
        if (errno) {
            fprintf(stderr, "%s: Invalid argument for number of threads\n", program_name);
            return -1;
        }
        if (num_threads_ <= 0) {
            fprintf(stderr, "%s: Number of threads must be at least 1\n", program_name);
            return -1;
        }
        if (num_threads_ > MAX_NUM_THREADS) {
            fprintf(stderr, "%s: Number of threads cannot be more than %d\n", program_name, MAX_NUM_THREADS);
            return -1;
        }
    }

    sl_ = sl_alloc();

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    wait_ = num_threads_;

    for (int i = 0; i < num_threads_; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void*)(size_t)i);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }

    for (int i = 0; i < num_threads_; ++i) {
        pthread_join(thread[i], NULL);
    }

    gettimeofday(&tv2, NULL);
    int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
    printf("Th:%ld Time:%dms\n", num_threads_, ms);
    sl_print(sl_);
    lwt_dump("lwt.out");

    return 0;
}
#endif//skiplist_test
