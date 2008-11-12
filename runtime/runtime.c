/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include "common.h"
#include "rcu.h"
#include "lwt.h"
#include "mem.h"
#include "nbd.h"
#include "tls.h"

DECLARE_THREAD_LOCAL(tid_, int);

void nbd_init (void) {
    INIT_THREAD_LOCAL(tid_, NULL);
    mem_init();
    lwt_init();
}

void nbd_thread_init (int id) {
    SET_THREAD_LOCAL(tid_, id);
    lwt_thread_init(id);
    rcu_thread_init(id);
}
