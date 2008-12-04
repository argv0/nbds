#ifndef MAP_H
#define MAP_H

typedef struct map map_t;

typedef const struct map_impl *map_type_t;

typedef int      (*cmp_fun_t)   (void *, void *);
typedef void *   (*clone_fun_t) (void *);
typedef uint32_t (*hash_fun_t)  (void *);

extern map_type_t MAP_TYPE_HASHTABLE;
extern map_type_t MAP_TYPE_SKIPLIST;
extern map_type_t MAP_TYPE_LIST;

map_t *  map_alloc  (map_type_t map_type, cmp_fun_t cmp_fun, hash_fun_t hash_fun, clone_fun_t clone_fun);
uint64_t map_get    (map_t *map, void *key);
uint64_t map_set    (map_t *map, void *key, uint64_t new_val);
uint64_t map_add    (map_t *map, void *key, uint64_t new_val);
uint64_t map_cas    (map_t *map, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t map_replace(map_t *map, void *key, uint64_t new_val);
uint64_t map_remove (map_t *map, void *key);
uint64_t map_count  (map_t *map);
void     map_print  (map_t *map);
void     map_free   (map_t *map);

#endif//MAP_H
