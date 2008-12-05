/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef TXN_H
#define TXN_H

#include "map.h"

typedef enum { TXN_REPEATABLE_READ, TXN_READ_COMMITTED, TXN_READ_ONLY  } txn_type_e;
typedef enum { TXN_RUNNING, TXN_VALIDATING, TXN_VALIDATED, TXN_ABORTED } txn_state_e;

typedef struct txn txn_t;

txn_t *     txn_begin  (txn_type_e type, map_t *map);
void        txn_abort  (txn_t *txn);
txn_state_e txn_commit (txn_t *txn);

uint64_t    tm_get (txn_t *txn, void *key);
void        tm_set (txn_t *txn, void *key, uint64_t value);

#endif//TXN_H
