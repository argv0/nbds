/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * hazard pointer test
 *
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include "common.h"
#include "mem.h"
#include "runtime.h"
#include "hazard.h"

#define NUM_ITERATIONS 10000000

typedef struct node {
    struct node *next;
} node_t;

typedef struct lifo {
    node_t *head;
} lifo_t;

static volatile int wait_;
static lifo_t *stk_;

void *worker (void *arg) {
    int id = (int)(size_t)arg;
    unsigned int r = (unsigned int)(id + 1) * 0x5bd1e995; // seed psuedo-random number generator
    haz_t *hp0 = haz_get_static(0);

    // Wait for all the worker threads to be ready.
    (void)SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

    int i;
    for (i = 0; i < NUM_ITERATIONS; ++ i) {
        r ^= r << 6; r ^= r >> 21; r ^= r << 7; // generate next psuedo-random number
        if (r & 0x1000) {
            // push
            node_t *new_head = (node_t *)nbd_malloc(sizeof(node_t));
            node_t *old_head = stk_->head;
            node_t *temp;
            do {
                temp = old_head;
                new_head->next = temp;
            } while ((old_head = SYNC_CAS(&stk_->head, temp, new_head)) != temp);
        } else {
            // pop
            node_t *temp;
            node_t *head = stk_->head;
            do {
                temp = head;
                if (temp == NULL)
                    break;
                haz_set(hp0, temp);
                head = VOLATILE_DEREF(stk_).head;
                if (temp != head)
                    continue;
            } while ((head = SYNC_CAS(&stk_->head, temp, temp->next)) != temp);

            if (temp != NULL) {
                haz_defer_free(temp, nbd_free);
            }
        }
    }

    return NULL;
}

int main (int argc, char **argv) {
    //lwt_set_trace_level("m0r0");

    int num_threads = MAX_NUM_THREADS;
    if (argc == 2)
    {
        errno = 0;
        num_threads = strtol(argv[1], NULL, 10);
        if (errno) {
            fprintf(stderr, "%s: Invalid argument for number of threads\n", argv[0]);
            return -1;
        }
        if (num_threads <= 0) {
            fprintf(stderr, "%s: Number of threads must be at least 1\n", argv[0]);
            return -1;
        }
    }

    stk_ = (lifo_t *)nbd_malloc(sizeof(lifo_t)); 
    memset(stk_, 0, sizeof(lifo_t));

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    wait_ = num_threads;

    pthread_t thread[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void *)(size_t)i);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }
    for (int i = 0; i < num_threads; ++i) {
        pthread_join(thread[i], NULL);
    }

    gettimeofday(&tv2, NULL);
    int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
    printf("Th:%d Time:%dms\n\n", num_threads, ms);
    fflush(stdout);

    return 0;
}
