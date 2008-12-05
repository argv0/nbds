#ifndef SKIPLIST_H
#define SKIPLIST_H

#include "map.h"

typedef struct sl skiplist_t;

skiplist_t *sl_alloc (const datatype_t *key_type);
uint64_t sl_cas    (skiplist_t *sl, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t sl_lookup (skiplist_t *sl, void *key);
uint64_t sl_remove (skiplist_t *sl, void *key);
uint64_t sl_count  (skiplist_t *sl);
void     sl_print  (skiplist_t *sl);
void     sl_free   (skiplist_t *sl);

static const map_impl_t sl_map_impl = { 
    (map_alloc_t)sl_alloc, (map_cas_t)sl_cas, (map_get_t)sl_lookup, (map_remove_t)sl_remove, 
    (map_count_t)sl_count, (map_print_t)sl_print, (map_free_t)sl_free
};

#endif//SKIPLIST_H
