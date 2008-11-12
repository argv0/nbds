/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef THREADS_H
#define THREADS_H

void nbd_init (void);

int nbd_thread_create (pthread_t *restrict thread, int thread_id, void *(*start_routine)(void *), void *restrict arg);

void rcu_update (void);

#endif//THREADS_H
