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
#include "rlocal.h"
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
