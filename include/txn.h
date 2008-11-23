/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef TXN_H
#define TXN_H
#include "struct.h"

typedef enum { TXN_READ_WRITE, TXN_READ_ONLY, TXN_BLIND_WRITE } txn_access_t;
typedef enum { TXN_DIRTY_READ, TXN_READ_COMMITTED, TXN_REPEATABLE_READ } txn_isolation_t;

typedef struct txn txn_t;

txn_t *txn_begin (txn_access_t access, txn_isolation_t isolation, hash_table_t *ht);
#endif//TXN_H
