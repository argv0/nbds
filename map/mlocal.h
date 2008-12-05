#ifndef MLOCAL_H
#define MLOCAL_H

#include "datatype.h"

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

typedef struct map_impl {
    map_alloc_t  alloc;
    map_cas_t    cas;
    map_get_t    get;
    map_remove_t remove;
    map_count_t  count;
    map_print_t  print;
    map_free_t   free_;
} map_impl_t;

#endif//MLOCAL_H
