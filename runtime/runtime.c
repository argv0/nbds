/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include <pthread.h>
#include "common.h"
#include "runtime.h"
#include "runtime_local.h"
#include "mem.h"
#include "tls.h"

DECLARE_THREAD_LOCAL(tid_, int);

typedef struct thread_info {
    int thread_id;
    void *(*start_routine)(void *);
    void *restrict arg;
} thread_info_t;

void nbd_init (void) {
    INIT_THREAD_LOCAL(tid_);
    SET_THREAD_LOCAL(tid_, 0);
    mem_init();
    lwt_thread_init(0);
    rcu_thread_init(0);
}

static void *worker (void *arg) {
    thread_info_t *ti = (thread_info_t *)arg;
    SET_THREAD_LOCAL(tid_, ti->thread_id);
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
