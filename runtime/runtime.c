/*
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include <stdlib.h>
#include <pthread.h>
#include "common.h"
#include "runtime.h"
#include "rlocal.h"
#include "mem.h"
#include "tls.h"

DECLARE_THREAD_LOCAL(ThreadId, int);
static int ThreadIndex

static int MaxThreadId = 0;

__attribute__ ((constructor)) void nbd_init (void) {
    rnd_init();
    mem_init();
}

void nbd_thread_init (void) {
    LOCALIZE_THREAD_LOCAL(ThreadId, int);

    if (ThreadId == 0) {
        ++MaxThreadId; // TODO: reuse thread id's of threads that have been destroyed
        ASSERT(MaxThreadId <= MAX_NUM_THREADS);
        SET_THREAD_LOCAL(ThreadId, MaxThreadId);
        rnd_thread_init();
    } 

    lwt_thread_init();
    rcu_thread_init();
}
