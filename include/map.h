#ifndef MAP_H
#define MAP_H

typedef struct map map_t;
typedef enum stat { MAP_STAT_COUNT } map_stat_e;
typedef enum { MAP_TYPE_HASHTABLE, MAP_TYPE_SKIPLIST, MAP_TYPE_LIST } map_type_e;

map_t *  map_alloc  (map_type_e map_type);
void     map_free   (map_t *map);
void     map_print  (map_t *map);
uint64_t map_stat   (map_t *map, map_stat_e stat_type);

uint64_t map_get    (map_t *map, const void *key_data, uint32_t key_len);
uint64_t map_set    (map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val);
uint64_t map_add    (map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val);
uint64_t map_cas    (map_t *map, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val);
uint64_t map_replace(map_t *map, const void *key_data, uint32_t key_len, uint64_t new_val);
uint64_t map_remove (map_t *map, const void *key_data, uint32_t key_len);

#endif//MAP_H
