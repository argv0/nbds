#ifndef LIST_H
#define LIST_H

#include "map.h"

typedef struct ll list_t;

list_t * ll_alloc  (const datatype_t *key_type);
uint64_t ll_cas    (list_t *ll, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t ll_lookup (list_t *ll, void *key);
uint64_t ll_remove (list_t *ll, void *key);
uint64_t ll_count  (list_t *ll);
void     ll_print  (list_t *ll);
void     ll_free   (list_t *ll);

static const map_impl_t ll_map_impl = { 
    (map_alloc_t)ll_alloc, (map_cas_t)ll_cas, (map_get_t)ll_lookup, (map_remove_t)ll_remove, 
    (map_count_t)ll_count, (map_print_t)ll_print, (map_free_t)ll_free
};

#endif//LIST_H
