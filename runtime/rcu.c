/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * safe memory reclamation using a simple technique from rcu
 *
 * WARNING: not robust enough for real-world use
 */
#include <string.h>
#include "common.h"
#include "runtime_local.h"
#include "lwt.h"
#include "mem.h"
#include "tls.h"

#define RCU_POST_THRESHOLD 10
#define RCU_QUEUE_SCALE 20

typedef struct fifo {
    uint32_t head;
    uint32_t tail;
    uint32_t scale;
    void *x[0];
} fifo_t;

static uint64_t rcu_[MAX_NUM_THREADS][MAX_NUM_THREADS] = {};
static uint64_t rcu_last_posted_[MAX_NUM_THREADS][MAX_NUM_THREADS] = {};
static fifo_t *pending_[MAX_NUM_THREADS] = {};
static int num_threads_ = 0;

static fifo_t *fifo_alloc(int scale) {
    fifo_t *q = (fifo_t *)nbd_malloc(sizeof(fifo_t) + (1 << scale) * sizeof(void *)); 
    memset(q, 0, sizeof(fifo_t));
    q->scale = scale;
    q->head = 0;
    q->tail = 0;
    return q;
}

static uint32_t fifo_index (fifo_t *q, uint32_t i) {
    return i & MASK(q->scale);
}

static void fifo_enqueue (fifo_t *q, void *x) {
    assert(fifo_index(q, q->head + 1) != fifo_index(q, q->tail));
    uint32_t i = fifo_index(q, q->head++);
    q->x[i] = x;
}

static void *fifo_dequeue (fifo_t *q) {
    uint32_t i = fifo_index(q, q->tail++);
    return q->x[i];
}

void rcu_thread_init (int id) {
    assert(id < MAX_NUM_THREADS);
    if (pending_[id] == NULL) {
        pending_[id] = fifo_alloc(RCU_QUEUE_SCALE);
        SYNC_ADD(&num_threads_, 1);
    }
}

static void rcu_post (uint64_t x) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    if (x - rcu_last_posted_[tid_][tid_] < RCU_POST_THRESHOLD)
        return;

    int next_thread_id = (tid_ + 1) % num_threads_;

    TRACE("r0", "rcu_post: %llu", x, 0);
    rcu_[next_thread_id][tid_] = rcu_last_posted_[tid_][tid_] = x;
}

void rcu_update (void) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    assert(tid_ < num_threads_);
    int next_thread_id = (tid_ + 1) % num_threads_;
    int i;
    for (i = 0; i < num_threads_; ++i) {
        if (i == tid_)
            continue;

        // No need to post an update if the value hasn't changed
        if (rcu_[tid_][i] == rcu_last_posted_[tid_][i])
            continue;

        uint64_t x = rcu_[tid_][i];
        rcu_[next_thread_id][i] = rcu_last_posted_[tid_][i] = x;
    }

    // free
    while (pending_[tid_]->tail != rcu_[tid_][tid_]) {
        nbd_free(fifo_dequeue(pending_[tid_]));
    }
}

void nbd_defer_free (void *x) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    fifo_enqueue(pending_[tid_], x);
    TRACE("r0", "nbd_defer_free: put %p on queue at position %llu", x, pending_[tid_]->head);
    rcu_post(pending_[tid_]->head);
}

#ifdef MAKE_rcu_test
#include <errno.h>
#include <stdio.h>
#include "runtime.h"

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
        head = ((volatile lifo_t *)stk)->head;
        ((volatile node_t *)x)->next = head;
    } while (__sync_val_compare_and_swap(&stk->head, head, x) != head);
}

node_t *lifo_aba_pop (lifo_t *stk) {
    node_t *head;
    do {
        head = ((volatile lifo_t *)stk)->head;
        if (head == NULL)
            return NULL;
    } while (__sync_val_compare_and_swap(&stk->head, head, head->next) != head);
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
    __sync_fetch_and_add(&wait_, -1);
    do {} while (wait_); 

    int i;
    for (i = 0; i < NUM_ITERATIONS; ++ i) {
        int n = rand_r(&rand_seed);
        if (n & 0x1) {
            lifo_aba_push(stk_, node_alloc());
        } else {
            node_t *x = lifo_aba_pop(stk_);
            if (x) {
                nbd_defer_free(x);
            }
        }
        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    nbd_init();
    //lwt_set_trace_level("m0r0");

    int num_threads = 2;
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

    pthread_t thread[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void *)(size_t)i);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }
    for (int i = 0; i < num_threads; ++i) {
        pthread_join(thread[i], NULL);
    }

    return 0;
}
#endif//rcu_test
