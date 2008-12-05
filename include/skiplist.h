#ifndef SKIPLIST_H
#define SKIPLIST_H

#include "datatype.h"
#include "map.h"

typedef struct sl skiplist_t;

extern map_type_t MAP_TYPE_SKIPLIST;

skiplist_t *sl_alloc (const datatype_t *key_type);
uint64_t sl_cas    (skiplist_t *sl, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t sl_lookup (skiplist_t *sl, void *key);
uint64_t sl_remove (skiplist_t *sl, void *key);
uint64_t sl_count  (skiplist_t *sl);
void     sl_print  (skiplist_t *sl);
void     sl_free   (skiplist_t *sl);

#endif//SKIPLIST_H
