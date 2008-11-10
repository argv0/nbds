/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef RCU_H
#define RCU_H
void rcu_thread_init (int thread_id);
void rcu_update (void);
void nbd_defer_free (void *x);
#endif//RCU_H
