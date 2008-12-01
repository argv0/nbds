/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef TXN_H
#define TXN_H

#include "map.h"

typedef enum { TXN_READ_WRITE, TXN_READ_ONLY, TXN_BLIND_WRITE          } txn_access_e;
typedef enum { TXN_REPEATABLE_READ, TXN_READ_COMMITTED, TXN_DIRTY_READ } txn_isolation_e;
typedef enum { TXN_RUNNING, TXN_VALIDATING, TXN_VALIDATED, TXN_ABORTED } txn_state_e;

typedef struct txn txn_t;

txn_t *     txn_begin  (txn_access_e access, txn_isolation_e isolation, map_type_t map_type);
void        txn_abort  (txn_t *txn);
txn_state_e txn_commit (txn_t *txn);

uint64_t tm_get (txn_t *txn, const char *key, uint32_t key_len);
void     tm_set (txn_t *txn, const char *key, uint32_t key_len, uint64_t value);

#endif//TXN_H
