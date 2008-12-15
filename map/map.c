/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * generic interface for map-like data structures
 */

#include "common.h"
#include "map.h"
#include "mem.h"

struct map {
    const map_impl_t *impl;
    void *data;
};

struct map_iter {
    const map_impl_t *impl;
    void *state;
};

map_t *map_alloc (const map_impl_t *map_impl, const datatype_t *key_type) { 
    map_t *map = nbd_malloc(sizeof(map_t)); 
    map->impl  = map_impl;
    map->data  = map->impl->alloc(key_type);
    return map;
}

void map_free (map_t *map) {
    map->impl->free_(map->data);
}

void map_print (map_t *map) {
    map->impl->print(map->data);
}

uint64_t map_count (map_t *map) {
    return map->impl->count(map->data);
}

uint64_t map_get (map_t *map, void *key) {
    return map->impl->get(map->data, key);
}

uint64_t map_set (map_t *map, void *key, uint64_t new_val) {
    return map->impl->cas(map->data, key, CAS_EXPECT_WHATEVER, new_val);
}

uint64_t map_add (map_t *map, void *key, uint64_t new_val) {
    return map->impl->cas(map->data, key, CAS_EXPECT_DOES_NOT_EXIST, new_val);
}

uint64_t map_cas (map_t *map, void *key, uint64_t expected_val, uint64_t new_val) {
    return map->impl->cas(map->data, key, expected_val, new_val);
}

uint64_t map_replace(map_t *map, void *key, uint64_t new_val) {
    return map->impl->cas(map->data, key, CAS_EXPECT_EXISTS, new_val);
}

uint64_t map_remove (map_t *map, void *key) {
    return map->impl->remove(map->data, key);
}

map_iter_t * map_iter_begin (map_t *map, void *key) {
    map_iter_t *iter = nbd_malloc(sizeof(map_iter_t));
    iter->impl  = map->impl;
    iter->state = map->impl->iter_begin(map->data, key);
    return iter;
}

uint64_t map_iter_next (map_iter_t *iter, void **key_ptr) {
    return iter->impl->iter_next(iter->state, key_ptr);
}

void map_iter_free (map_iter_t *iter) {
    iter->impl->iter_free(iter->state);
    nbd_free(iter);
}
