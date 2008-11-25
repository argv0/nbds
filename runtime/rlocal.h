#ifndef RLOCAL_H
#define RLOCAL_H
#include "tls.h"
DECLARE_THREAD_LOCAL(tid_, int);

void mem_init (void);

void rcu_thread_init (int thread_id);
void lwt_thread_init (int thread_id);
#endif//RLOCAL_H 
