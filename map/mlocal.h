#ifndef MLOCAL_H
#define MLOCAL_H

#define CAS_EXPECT_DOES_NOT_EXIST ( 0)
#define CAS_EXPECT_EXISTS         (-1)
#define CAS_EXPECT_WHATEVER       (-2)

typedef void *   (*map_alloc_t)  (void);
typedef uint64_t (*map_cas_t)    (void *, const void *, uint32_t, uint64_t, uint64_t);
typedef uint64_t (*map_get_t)    (void *, const void *, uint32_t);
typedef uint64_t (*map_remove_t) (void *, const void *, uint32_t);
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

typedef struct ht hashtable_t;
typedef struct sl skiplist_t;
typedef struct ll list_t;

hashtable_t * ht_alloc (void);
skiplist_t *  sl_alloc (void);
list_t *      ll_alloc (void);

uint64_t ht_cas    (hashtable_t *ht, const char *key, uint32_t key_len, uint64_t expected_val, uint64_t val);
uint64_t ht_get    (hashtable_t *ht, const char *key, uint32_t len);
uint64_t ht_remove (hashtable_t *ht, const char *key, uint32_t len);
uint64_t ht_count  (hashtable_t *ht);
void     ht_print  (hashtable_t *ht);
void     ht_free   (hashtable_t *ht);

uint64_t sl_cas    (skiplist_t *sl, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val);
uint64_t sl_lookup (skiplist_t *sl, const void *key_data, uint32_t key_len);
uint64_t sl_remove (skiplist_t *sl, const void *key_data, uint32_t key_len);
uint64_t sl_count  (skiplist_t *sl);
void     sl_print  (skiplist_t *sl);
void     sl_free   (skiplist_t *sl);

uint64_t ll_cas    (list_t *ll, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val);
uint64_t ll_lookup (list_t *ll, const void *key_data, uint32_t key_len);
uint64_t ll_remove (list_t *ll, const void *key_data, uint32_t key_len);
uint64_t ll_count  (list_t *ll);
void     ll_print  (list_t *ll);
void     ll_free   (list_t *ll);

#endif//MLOCAL_H
