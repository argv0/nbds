#ifndef RLOCAL_H
#define RLOCAL_H

#include "runtime.h"
#include "tls.h"

extern DECLARE_THREAD_LOCAL(ThreadId, int);

#define GET_THREAD_INDEX() ({ LOCALIZE_THREAD_LOCAL(ThreadId, int); assert(ThreadId != 0); ThreadId - 1; })

void mem_init (void);
void rnd_init (void);

void rnd_thread_init (void);
void rcu_thread_init (void);
void lwt_thread_init (void);

#endif//RLOCAL_H 
