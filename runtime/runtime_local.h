#ifndef RUNTIME_LOCAL_H
#define RUNTIME_LOCAL_H
#include "tls.h"
DECLARE_THREAD_LOCAL(tid_, int);

void mem_init (void);

void rcu_thread_init (int thread_id);
void lwt_thread_init (int thread_id);
#endif//RUNTIME_LOCAL_H 
