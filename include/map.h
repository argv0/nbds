#ifndef MAP_H
#define MAP_H

#include "datatype.h"

typedef struct map map_t;
typedef struct map_impl map_impl_t;

map_t *  map_alloc  (const map_impl_t *map_impl, const datatype_t *key_type);
uint64_t map_get    (map_t *map, void *key);
uint64_t map_set    (map_t *map, void *key, uint64_t new_val);
uint64_t map_add    (map_t *map, void *key, uint64_t new_val);
uint64_t map_cas    (map_t *map, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t map_replace(map_t *map, void *key, uint64_t new_val);
uint64_t map_remove (map_t *map, void *key);
uint64_t map_count  (map_t *map);
void     map_print  (map_t *map);
void     map_free   (map_t *map);

/////////////////////////////////////////////////////////////////////////////////////

#define CAS_EXPECT_DOES_NOT_EXIST ( 0)
#define CAS_EXPECT_EXISTS         (-1)
#define CAS_EXPECT_WHATEVER       (-2)

typedef void *   (*map_alloc_t)  (const datatype_t *);
typedef uint64_t (*map_cas_t)    (void *, void *, uint64_t, uint64_t);
typedef uint64_t (*map_get_t)    (void *, void *);
typedef uint64_t (*map_remove_t) (void *, void *);
typedef uint64_t (*map_count_t)  (void *);
typedef void     (*map_print_t)  (void *);
typedef void     (*map_free_t)   (void *);

struct map_impl {
    map_alloc_t  alloc;
    map_cas_t    cas;
    map_get_t    get;
    map_remove_t remove;
    map_count_t  count;
    map_print_t  print;
    map_free_t   free_;
};

#endif//MAP_H
