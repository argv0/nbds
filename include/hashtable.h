#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "map.h"

typedef struct ht hashtable_t;

hashtable_t *ht_alloc (const datatype_t *key_type);
uint64_t ht_cas    (hashtable_t *ht, void *key, uint64_t expected_val, uint64_t val);
uint64_t ht_get    (hashtable_t *ht, void *key);
uint64_t ht_remove (hashtable_t *ht, void *key);
uint64_t ht_count  (hashtable_t *ht);
void     ht_print  (hashtable_t *ht);
void     ht_free   (hashtable_t *ht);

static const map_impl_t ht_map_impl = { 
    (map_alloc_t)ht_alloc, (map_cas_t)ht_cas, (map_get_t)ht_get, (map_remove_t)ht_remove, 
    (map_count_t)ht_count, (map_print_t)ht_print, (map_free_t)ht_free
};

#endif//HASHTABLE_H
