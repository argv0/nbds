/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#define _POSIX_C_SOURCE 1 // for rand_r()
#include <stdlib.h>
#include <pthread.h>
#include "common.h"
#include "runtime.h"
#include "rlocal.h"
#include "mem.h"
#include "tls.h"

DECLARE_THREAD_LOCAL(tid_, int);
DECLARE_THREAD_LOCAL(rand_seed_, unsigned);

typedef struct thread_info {
    int thread_id;
    void *(*start_routine)(void *);
    void *restrict arg;
} thread_info_t;

__attribute__ ((constructor)) void nbd_init (void) {
    //sranddev();
    INIT_THREAD_LOCAL(rand_seed_);
    INIT_THREAD_LOCAL(tid_);
    SET_THREAD_LOCAL(tid_, 0);
    mem_init();
    lwt_thread_init(0);
    rcu_thread_init(0);
}

static void *worker (void *arg) {
    thread_info_t *ti = (thread_info_t *)arg;
    SET_THREAD_LOCAL(tid_, ti->thread_id);
    LOCALIZE_THREAD_LOCAL(tid_, int);
#ifndef NDEBUG
    SET_THREAD_LOCAL(rand_seed_, tid_+1);
#else
    SET_THREAD_LOCAL(rand_seed_, nbd_rand_seed(tid_+1));
#endif
    lwt_thread_init(ti->thread_id);
    rcu_thread_init(ti->thread_id);
    void *ret = ti->start_routine(ti->arg);
    nbd_free(ti);
    return ret;
}

int nbd_thread_create (pthread_t *restrict thread, int thread_id, void *(*start_routine)(void *), void *restrict arg) {
    thread_info_t *ti = (thread_info_t *)nbd_malloc(sizeof(thread_info_t));
    ti->thread_id = thread_id;
    ti->start_routine = start_routine;
    ti->arg = arg;
    return pthread_create(thread, NULL, worker, ti);
}

int nbd_rand (void) {
    LOCALIZE_THREAD_LOCAL(rand_seed_, unsigned);
    unsigned r = rand_r(&rand_seed_);
    SET_THREAD_LOCAL(rand_seed_, r);
    return r;
}

uint64_t nbd_rand_seed (int i) {
    return rdtsc() + -715159705 + i * 129;
}

// Fairly fast random numbers
int nbd_next_rand (uint64_t *r) {
    *r = (*r * 0x5DEECE66DLL + 0xBLL) & MASK(48);
    return (*r >> 17) & 0x7FFFFFFF;
}
