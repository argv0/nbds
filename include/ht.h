/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef HT_H
#define HT_H

#include "common.h"

#define DOES_NOT_EXIST 0

#define HT_EXPECT_NOT_EXISTS ( 0)
#define HT_EXPECT_EXISTS     (-1)
#define HT_EXPECT_WHATEVER   (-2)

typedef struct hash_table_i *hash_table_t;

hash_table_t *ht_alloc (void);
void ht_free (hash_table_t *ht);
uint64_t ht_get (hash_table_t *ht, const char *key, uint32_t len);
uint64_t ht_compare_and_set (hash_table_t *ht, const char *key, uint32_t key_len, uint64_t expected_val, uint64_t val);
uint64_t ht_remove (hash_table_t *ht, const char *key, uint32_t len);
uint64_t ht_count (hash_table_t *ht);

#endif//HT_H
