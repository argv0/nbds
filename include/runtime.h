/*
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef RUNTIME_H
#define RUNTIME_H

#include <pthread.h>
#include "tls.h"

void nbd_thread_init (void);
uint64_t nbd_rand (void);

#endif//RUNTIME_H
