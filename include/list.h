#ifndef LIST_H
#define LIST_H

#include "datatype.h"
#include "map.h"

typedef struct ll list_t;

extern map_type_t MAP_TYPE_LIST;

list_t * ll_alloc  (const datatype_t *key_type);
uint64_t ll_cas    (list_t *ll, void *key, uint64_t expected_val, uint64_t new_val);
uint64_t ll_lookup (list_t *ll, void *key);
uint64_t ll_remove (list_t *ll, void *key);
uint64_t ll_count  (list_t *ll);
void     ll_print  (list_t *ll);
void     ll_free   (list_t *ll);

#endif//LIST_H
