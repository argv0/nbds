/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Harris-Michael lock-free list-based set
 * http://www.research.ibm.com/people/m/michael/spaa-2002.pdf
 */
#include <stdio.h>
#include <string.h>

#include "common.h"
#include "struct.h"
#include "mem.h"

typedef struct node {
    uint64_t key;
    uint64_t value;
    struct node *next;
} node_t;

typedef struct list {
    node_t *head;
    node_t *last;
} list_t;

node_t *node_alloc (uint64_t key, uint64_t value) {
    node_t *item = (node_t *)nbd_malloc(sizeof(node_t));
    memset(item, 0, sizeof(node_t));
    item->key   = key;
    item->value = value;
    return item;
}

list_t *list_alloc (void) {
    list_t *list = (list_t *)nbd_malloc(sizeof(list_t));
    list->head = node_alloc(0, 0);
    list->last = node_alloc((uint64_t)-1, 0);
    list->head->next = list->last;
    return list;
}

static node_t *find_pred (node_t **pred_ptr, list_t *list, uint64_t key, int help_remove) {
    node_t *pred = list->head;
    node_t *item = pred->next;
    TRACE("l3", "find_pred: searching for key %p in list (head is %p)", key, pred);
#ifndef NDEBUG
    int count = 0;
#endif

    do {
        node_t *next = item->next;
        TRACE("l3", "find_pred: visiting item %p (next %p)", item, next);
        TRACE("l3", "find_pred: key %p", item->key, item->value);

        // Marked items are logically removed, but not unlinked yet.
        while (EXPECT_FALSE(IS_TAGGED(next))) {

            // Skip over partially removed items.
            if (!help_remove) {
                item = (node_t *)STRIP_TAG(item->next);
                next = item->next;
                continue;
            }

            // Unlink partially removed items.
            node_t *other;
            if ((other = SYNC_CAS(&pred->next, item, STRIP_TAG(next))) == item) {
                item = (node_t *)STRIP_TAG(next);
                next = item->next;
                TRACE("l3", "find_pred: unlinked item %p from pred %p", item, pred);
                TRACE("l3", "find_pred: now item is %p next is %p", item, next);

                // The thread that completes the unlink should free the memory.
                nbd_defer_free(other);
            } else {
                TRACE("l3", "find_pred: lost race to unlink item from pred %p; its link changed to %p", pred, other);
                if (IS_TAGGED(other))
                    return find_pred(pred_ptr, list, key, help_remove); // retry
                item = other;
                next = item->next;
            }
        }

        // If we reached the key (or passed where it should be), we found the right predesssor
        if (item->key >= key) {
            TRACE("l3", "find_pred: found pred %p item %p", pred, item);
            if (pred_ptr != NULL) {
                *pred_ptr = pred;
            }
            return item;
        }

        assert(count++ < 18);
        pred = item;
        item = next;

    } while (1);
}

// Fast find. Do not help unlink partially removed nodes and do not return the found item's predecessor.
uint64_t list_lookup (list_t *list, uint64_t key) {
    TRACE("l3", "list_lookup: searching for key %p in list %p", key, list);
    node_t *item = find_pred(NULL, list, key, FALSE);

    // If we found an <item> matching the <key> return its value.
    return (item->key == key) ? item->value : DOES_NOT_EXIST;
}

// Insert the <key>, if it doesn't already exist in the <list>
uint64_t list_add (list_t *list, uint64_t key, uint64_t value) {
    TRACE("l3", "list_add: inserting key %p value %p", key, value);
    node_t *pred;
    node_t *item = NULL;
    do {
        node_t *next = find_pred(&pred, list, key, TRUE);

        // If a node matching <key> already exists in the list, return its value.
        if (next->key == key) {
            TRACE("l3", "list_add: there is already an item %p (value %p) with the same key", next, next->value);
            if (EXPECT_FALSE(item != NULL)) { nbd_free(item); }
            return next->value;
        }

        TRACE("l3", "list_add: attempting to insert item between %p and %p", pred, next);
        if (EXPECT_TRUE(item == NULL)) { item = node_alloc(key, value); }
        item->next = next;
        node_t *other = SYNC_CAS(&pred->next, next, item);
        if (other == next) {
            TRACE("l3", "list_add: successfully inserted item %p", item, 0);
            return DOES_NOT_EXIST; // success
        }
        TRACE("l3", "list_add: failed to change pred's link: expected %p found %p", next, other);

    } while (1);
}

uint64_t list_remove (list_t *list, uint64_t key) {
    TRACE("l3", "list_remove: removing item with key %p from list %p", key, list);
    node_t *pred;
    node_t *item = find_pred(&pred, list, key, TRUE);
    if (item->key != key) {
        TRACE("l3", "list_remove: remove failed, an item with a matching key does not exist in the list", 0, 0);
        return DOES_NOT_EXIST;
    }

    // Mark <item> removed. This must be atomic. If multiple threads try to remove the same item
    // only one of them should succeed.
    if (EXPECT_FALSE(IS_TAGGED(item->next))) {
        TRACE("l3", "list_remove: %p is already marked for removal by another thread", item, 0);
        return DOES_NOT_EXIST;
    }
    node_t *next = SYNC_FETCH_AND_OR(&item->next, TAG);
    if (EXPECT_FALSE(IS_TAGGED(next))) {
        TRACE("l3", "list_remove: lost race -- %p is already marked for removal by another thread", item, 0);
        return DOES_NOT_EXIST;
    }

    uint64_t value = item->value;

    // Unlink <item> from the list.
    TRACE("l3", "list_remove: link item's pred %p to it's successor %p", pred, next);
    node_t *other;
    if ((other = SYNC_CAS(&pred->next, item, next)) != item) {
        TRACE("l3", "list_remove: unlink failed; pred's link changed from %p to %p", item, other);
        // By marking the item earlier, we logically removed it. It is safe to leave the item.
        // Another thread will finish physically removing it from the list.
        return value;
    } 

    // The thread that completes the unlink should free the memory.
    nbd_defer_free(item); 
    return value;
}

void list_print (list_t *list) {
    node_t *item;
    item = list->head;
    while (item) {
        printf("0x%llx ", item->key);
        fflush(stdout);
        item = item->next;
    }
    printf("\n");
}

#ifdef MAKE_list_test
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "runtime.h"

#define NUM_ITERATIONS 10000000

static volatile int wait_;
static long num_threads_;
static list_t *list_;

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
            list_add(list_, key, 1);
        } else {
            list_remove(list_, key);
        }

        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    nbd_init();
    //lwt_set_trace_level("m0l0");

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

    list_ = list_alloc();

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
    list_print(list_);
    lwt_dump("lwt.out");

    return 0;
}
#endif//list_test
