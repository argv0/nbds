#define _POSIX_C_SOURCE 1 // for rand_r
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include "common.h"
#include "runtime.h"
#include "mem.h"
#include "rcu.h"

#define NUM_ITERATIONS 10000000

typedef struct node {
    struct node *next;
} node_t;

typedef struct lifo {
    node_t *head;
} lifo_t;

static volatile int wait_;
static lifo_t *stk_;

static lifo_t *lifo_alloc (void) {
    lifo_t *stk = (lifo_t *)nbd_malloc(sizeof(lifo_t)); 
    memset(stk, 0, sizeof(lifo_t));
    return stk;
}

static void lifo_aba_push (lifo_t *stk, node_t *x) {
    node_t *head;
    do {
        head = VOLATILE_DEREF(stk).head;
        VOLATILE_DEREF(x).next = head;
    } while (SYNC_CAS(&stk->head, head, x) != head);
}

node_t *lifo_aba_pop (lifo_t *stk) {
    node_t *head;
    do {
        head = VOLATILE_DEREF(stk).head;
        if (head == NULL)
            return NULL;
    } while (SYNC_CAS(&stk->head, head, head->next) != head);
    head->next = NULL;
    return head;
}

node_t *node_alloc (void) {
    node_t *node = (node_t *)nbd_malloc(sizeof(node_t));
    memset(node, 0, sizeof(node_t));
    return node;
}

void *worker (void *arg) {
    int id = (int)(size_t)arg;
    unsigned int rand_seed = (unsigned int)id + 1;

    // Wait for all the worker threads to be ready.
    (void)__sync_fetch_and_add(&wait_, -1);
    do {} while (wait_); 

    int i;
    for (i = 0; i < NUM_ITERATIONS; ++ i) {
        int n = rand_r(&rand_seed);
        if (n & 0x1) {
            lifo_aba_push(stk_, node_alloc());
        } else {
            node_t *x = lifo_aba_pop(stk_);
            if (x) {
                rcu_defer_free(x);
            }
        }
        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    lwt_set_trace_level("m3r3");

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

    stk_ = lifo_alloc();
    wait_ = num_threads;

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
