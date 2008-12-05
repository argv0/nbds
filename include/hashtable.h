#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "datatype.h"

typedef struct ht hashtable_t;

hashtable_t *ht_alloc (const datatype_t *key_type);
uint64_t ht_cas    (hashtable_t *ht, void *key, uint64_t expected_val, uint64_t val);
uint64_t ht_get    (hashtable_t *ht, void *key);
uint64_t ht_remove (hashtable_t *ht, void *key);
uint64_t ht_count  (hashtable_t *ht);
void     ht_print  (hashtable_t *ht);
void     ht_free   (hashtable_t *ht);

#endif//HASHTABLE_H
