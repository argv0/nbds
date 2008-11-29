#ifndef STRUCT_H
#define STRUCT_H

#define DOES_NOT_EXIST 0

#define EXPECT_DOES_NOT_EXIST ( 0)
#define EXPECT_EXISTS         (-1)
#define EXPECT_WHATEVER       (-2)

typedef struct ht hashtable_t;
hashtable_t *ht_alloc (void);
uint64_t ht_compare_and_set (hashtable_t *ht, const char *key, uint32_t key_len, uint64_t expected_val, uint64_t val);
uint64_t ht_get    (hashtable_t *ht, const char *key, uint32_t len);
uint64_t ht_remove (hashtable_t *ht, const char *key, uint32_t len);
uint64_t ht_count  (hashtable_t *ht);
void     ht_free   (hashtable_t *ht);

typedef struct ll list_t;
list_t * ll_alloc  (void);
uint64_t ll_lookup (list_t *ll, const void *key_data, uint32_t key_len);
uint64_t ll_cas    (list_t *ll, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val);
uint64_t ll_remove (list_t *ll, const void *key_data, uint32_t key_len);
void     ll_print  (list_t *ll);

typedef struct sl skiplist_t;
skiplist_t * sl_alloc (void);
uint64_t sl_lookup (skiplist_t *sl, const void *key_data, uint32_t key_len);
uint64_t sl_cas    (skiplist_t *sl, const void *key_data, uint32_t key_len, uint64_t expected_val, uint64_t new_val);
uint64_t sl_remove (skiplist_t *sl, const void *key_data, uint32_t key_len);
void     sl_print  (skiplist_t *sl);

#endif//STRUCT_H
