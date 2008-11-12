/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Harris-Michael lock-free list-based set
 * http://www.research.ibm.com/people/m/michael/spaa-2002.pdf
 */
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "common.h"
#include "lwt.h"
#include "mem.h"

#define NUM_ITERATIONS 10000000

#define PLACE_MARK(x) (((size_t)(x))|1)
#define CLEAR_MARK(x) (((size_t)(x))&~(size_t)1)
#define IS_MARKED(x)  ((size_t)(x))&1

typedef struct node {
    struct node *next;
    int key;
} node_t;

typedef struct list {
    node_t head[1];
    node_t last;
} list_t;

static void list_node_init (node_t *item, int key) {
    memset(item, 0, sizeof(node_t));
    item->key = key;
}

node_t *list_node_alloc (int key) {
    node_t *item = (node_t *)nbd_malloc(sizeof(node_t));
    list_node_init(item, key);
    return item;
}

list_t *list_alloc (void) {
    list_t *list = (list_t *)nbd_malloc(sizeof(list_t));
    list_node_init(list->head, INT_MIN);
    list_node_init(&list->last, INT_MAX);
    list->head->next = &list->last;
    return list;
}

static void find_pred_and_item (node_t **pred_ptr, node_t **item_ptr, list_t *list, int key) {
    node_t *pred = list->head;
    node_t *item = list->head->next; // head is never removed
    TRACE("l3", "find_pred_and_item: searching for key %llu in list (head is %p)", key, pred);
#ifndef NDEBUG
    int count = 0;
#endif
    do {
        // skip removed items
        node_t *other, *next = item->next;
        TRACE("l3", "find_pred_and_item: visiting item %p (next is %p)", item, next);
        while (EXPECT_FALSE(IS_MARKED(next))) {
            
            // assist in unlinking partially removed items
            if ((other = SYNC_CAS(&pred->next, item, CLEAR_MARK(next))) != item)
            {
                TRACE("l3", "find_pred_and_item: failed to unlink item from pred %p, pred's next pointer was changed to %p", pred, other);
                return find_pred_and_item(pred_ptr, item_ptr, list, key); // retry
            }

            assert(count++ < 18);
            item = (node_t *)CLEAR_MARK(next);
            next = item->next;
            TRACE("l3", "find_pred_and_item: unlinked item, %p is the new item (next is %p)", item, next);
        }

        if (item->key >= key) {
            *pred_ptr = pred;
            *item_ptr = item;
            TRACE("l3", "find_pred_and_item: key found, returning pred %p and item %p", pred, item);
            return;
        }

        assert(count++ < 18);
        pred = item;
        item = next;

    } while (1);
}

int list_insert (list_t *list, node_t *item) {
    TRACE("l3", "list_insert: inserting %p (with key %llu)", item, item->key);
    node_t *pred, *next, *other = (node_t *)-1;
    do {
        if (other != (node_t *)-1) {
            TRACE("l3", "list_insert: failed to swap item into list; pred's next was changed to %p", other, 0);
        }
        find_pred_and_item(&pred, &next, list, item->key);

        // fail if item already exists in list
        if (next->key == item->key)
        {
            TRACE("l3", "list_insert: insert failed item with key already exists %p", next, 0);
            return 0;
        }

        item->next = next;
        TRACE("l3", "list_insert: attempting to insert item between %p and %p", pred, next);

    } while ((other = __sync_val_compare_and_swap(&pred->next, next, item)) != next);

    TRACE("l3", "list_insert: insert was successful", 0, 0);

    // success
    return 1;
}

node_t *list_remove (list_t *list, int key) {
    node_t *pred, *item, *next;

    TRACE("l3", "list_remove: removing item with key %llu", key, 0);
    find_pred_and_item(&pred, &item, list, key);
    if (item->key != key)
    {
        TRACE("l3", "list_remove: remove failed, key does not exist in list", 0, 0);
        return NULL;
    }

    // Mark <item> removed, must be atomic. If multiple threads try to remove the 
    // same item only one of them should succeed
    next = item->next;
    node_t *other = (node_t *)-1;
    if (IS_MARKED(next) || (other = __sync_val_compare_and_swap(&item->next, next, PLACE_MARK(next))) != next) {
        if (other == (node_t *)-1) {
            TRACE("l3", "list_remove: retry; %p is already marked for removal (it's next pointer is %p)", item, next);
        } else {
            TRACE("l3", "list_remove: retry; failed to mark %p for removal; it's next pointer was %p, but changed to %p", next, other);
        }
        return list_remove(list, key); // retry
    }

    // Remove <item> from list
    TRACE("l3", "list_remove: link item's pred %p to it's successor %p", pred, next);
    if ((other = __sync_val_compare_and_swap(&pred->next, item, next)) != item) {
        TRACE("l3", "list_remove: link failed; pred's link changed from %p to %p", item, other);

        // make sure item gets unlinked before returning it
        node_t *d1, *d2;
        find_pred_and_item(&d1, &d2, list, key);
    } else {
        TRACE("l3", "list_remove: link succeeded; pred's link changed from %p to %p", item, next);
    }

    return item;
}

void list_print (list_t *list) {
    node_t *item;
    item = list->head;
    while (item) {
        printf("%d ", item->key);
        fflush(stdout);
        item = item->next;
    }
    printf("\n");
}

#ifdef MAKE_list_test
#include <errno.h>
#include <pthread.h>
#include "runtime.h"

static volatile int wait_;
static long num_threads_;
static list_t *list_;

void *worker (void *arg) {
    int id = (int)(size_t)arg;

    unsigned int rand_seed = id+1;//rdtsc_l();

    // Wait for all the worker threads to be ready.
    __sync_fetch_and_add(&wait_, -1);
    do {} while (wait_); 
    __asm__ __volatile__("lfence"); 

    int i;
    for (i = 0; i < NUM_ITERATIONS/num_threads_; ++i) {
        int n = rand_r(&rand_seed);
        int key = (n & 0xF) + 1;
        if (n & (1 << 8)) {
            node_t *item = list_node_alloc(key);
            int success = list_insert(list_, item);
            if (!success) {
                nbd_free(item); 
            }
        } else {
            node_t *item = list_remove(list_, key);
            if (item) {
                nbd_defer_free(item);
            }
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

    __asm__ __volatile__("sfence"); 
    wait_ = num_threads_;

    int i;
    for (i = 0; i < num_threads_; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void*)(size_t)i);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }

    for (i = 0; i < num_threads_; ++i) {
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
