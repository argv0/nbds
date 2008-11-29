/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * generic interface for map-like data structures
 */

#include "common.h"
#include "mlocal.h"
#include "mem.h"
#include "map.h"

struct map {
    map_type_e type;
    void *impl;
};

map_t *map_alloc (map_type_e map_type) {
    void *impl = NULL;
    switch (map_type) {
        case MAP_TYPE_HASHTABLE: impl = ht_alloc(); break;
        case MAP_TYPE_SKIPLIST:  impl = sl_alloc(); break;
        case MAP_TYPE_LIST:      impl = ll_alloc(); break;
    }
    map_t *map = NULL;
    if (impl) {
        map = nbd_malloc(sizeof(map_t));
        map->type = map_type;
        map->impl = impl;
    }
    return map;
}

void map_free (map_t *map) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: ht_free(map->impl); break;
        case MAP_TYPE_SKIPLIST:  sl_free(map->impl); break;
        case MAP_TYPE_LIST:      ll_free(map->impl); break;
    }
}

void map_print (map_t *map) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: ht_print(map->impl); break;
        case MAP_TYPE_SKIPLIST:  sl_print(map->impl); break;
        case MAP_TYPE_LIST:      ll_print(map->impl); break;
    }
}

uint64_t map_stat (map_t *map, map_stat_e stat_type) {
    switch (stat_type) {
        case MAP_STAT_COUNT: 
            if (map->type != MAP_TYPE_HASHTABLE)
                return ERROR_UNSUPPORTED_FEATURE;
            return ht_count(map->impl);
    }
    return ERROR_INVALID_OPTION;
}

uint64_t map_get (map_t *map, const void *key_data, uint32_t key_len) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_get(map->impl, key_data, key_len);
        case MAP_TYPE_SKIPLIST:  return sl_lookup(map->impl, key_data, key_len);
        case MAP_TYPE_LIST:      return ll_lookup(map->impl, key_data, key_len);
    }
    return ERROR_INVALID_ARGUMENT;
}

uint64_t map_set (map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_cas(map->impl, key_data, key_len, CAS_EXPECT_WHATEVER, new_val);
        case MAP_TYPE_SKIPLIST:  return sl_cas(map->impl, key_data, key_len, CAS_EXPECT_WHATEVER, new_val);
        case MAP_TYPE_LIST:      return ll_cas(map->impl, key_data, key_len, CAS_EXPECT_WHATEVER, new_val);
    }
    return ERROR_INVALID_ARGUMENT;
}

uint64_t map_add (map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_cas(map->impl, key_data, key_len, CAS_EXPECT_DOES_NOT_EXIST, new_val);
        case MAP_TYPE_SKIPLIST:  return sl_cas(map->impl, key_data, key_len, CAS_EXPECT_DOES_NOT_EXIST, new_val);
        case MAP_TYPE_LIST:      return ll_cas(map->impl, key_data, key_len, CAS_EXPECT_DOES_NOT_EXIST, new_val);
    }
    return ERROR_INVALID_ARGUMENT;
}

uint64_t map_cas (map_t *map, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_cas(map->impl, key_data, key_len, expected_val, new_val);
        case MAP_TYPE_SKIPLIST:  return sl_cas(map->impl, key_data, key_len, expected_val, new_val);
        case MAP_TYPE_LIST:      return ll_cas(map->impl, key_data, key_len, expected_val, new_val);
    }
    return ERROR_INVALID_ARGUMENT;
}

uint64_t map_replace(map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_cas(map->impl, key_data, key_len, CAS_EXPECT_EXISTS, new_val);
        case MAP_TYPE_SKIPLIST:  return sl_cas(map->impl, key_data, key_len, CAS_EXPECT_EXISTS, new_val);
        case MAP_TYPE_LIST:      return ll_cas(map->impl, key_data, key_len, CAS_EXPECT_EXISTS, new_val);
    }
    return ERROR_INVALID_ARGUMENT;
}

uint64_t map_remove (map_t *map, const void *key_data, uint32_t key_len) {
    switch (map->type) {
        case MAP_TYPE_HASHTABLE: return ht_remove(map->impl, key_data, key_len);
        case MAP_TYPE_SKIPLIST:  return sl_remove(map->impl, key_data, key_len);
        case MAP_TYPE_LIST:      return ll_remove(map->impl, key_data, key_len);
    }
    return ERROR_INVALID_ARGUMENT;
}
