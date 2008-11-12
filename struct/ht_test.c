/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include <stdio.h>
#include <pthread.h>
#include "runtime.h"
#include "CuTest.h"
#include "common.h"
#include "ht.h"
#include "mem.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

typedef struct worker_data {
    int id;
    CuTest *tc;
    hash_table_t *ht;
    int *wait;
} worker_data_t;

int64_t ht_set (hash_table_t *ht, const char *key, uint32_t key_len, int64_t val) {
    return ht_compare_and_set(ht, key, key_len, HT_EXPECT_WHATEVER, val);
}

int64_t ht_add (hash_table_t *ht, const char *key, uint32_t key_len, int64_t val) {
    return ht_compare_and_set(ht, key, key_len, HT_EXPECT_NOT_EXISTS, val);
}

int64_t ht_replace (hash_table_t *ht, const char *key, uint32_t key_len, int64_t val) {
    return ht_compare_and_set(ht, key, key_len, HT_EXPECT_EXISTS, val);
}

// Test some basic stuff; add a few keys, remove a few keys
void basic_test (CuTest* tc) {

    hash_table_t *ht = ht_alloc();

    ASSERT_EQUAL( 0,              ht_count(ht)          );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_add(ht,"a",2,10)     );
    ASSERT_EQUAL( 1,              ht_count(ht)          );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_add(ht,"b",2,20)     );
    ASSERT_EQUAL( 2,              ht_count(ht)          );
    ASSERT_EQUAL( 20,             ht_get(ht,"b",2)        );
    ASSERT_EQUAL( 10,             ht_set(ht,"a",2,11)     );
    ASSERT_EQUAL( 20,             ht_set(ht,"b",2,21)     );
    ASSERT_EQUAL( 2,              ht_count(ht)          );
    ASSERT_EQUAL( 21,             ht_add(ht,"b",2,22)     );
    ASSERT_EQUAL( 11,             ht_remove(ht,"a",2)     );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"a",2)        );
    ASSERT_EQUAL( 1,              ht_count(ht)          );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_remove(ht,"a",2)     );
    ASSERT_EQUAL( 21,             ht_remove(ht,"b",2)     );
    ASSERT_EQUAL( 0,              ht_count(ht)          );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_remove(ht,"b",2)     );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_remove(ht,"c",2)     );
    ASSERT_EQUAL( 0,              ht_count(ht)          );
    
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_add(ht,"d",2,40)     );
    ASSERT_EQUAL( 40,             ht_get(ht,"d",2)        );
    ASSERT_EQUAL( 1,              ht_count(ht)          );
    ASSERT_EQUAL( 40,             ht_remove(ht,"d",2)     );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"d",2)        );
    ASSERT_EQUAL( 0,              ht_count(ht)          );

    ASSERT_EQUAL( DOES_NOT_EXIST, ht_replace(ht,"d",2,10) );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"d",2)        );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_set(ht,"d",2,40)     );
    ASSERT_EQUAL( 40,             ht_replace(ht,"d",2,41) );
    ASSERT_EQUAL( 41,             ht_get(ht,"d",2)        );
    ASSERT_EQUAL( 41,             ht_remove(ht,"d",2)     );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"d",2)        );
    ASSERT_EQUAL( 0,              ht_count(ht)          );

    ASSERT_EQUAL( DOES_NOT_EXIST, ht_replace(ht,"b",2,20) );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"b",2)        );
    // In the end, all members should be removed
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_set(ht,"b",2,20)     );
    ASSERT_EQUAL( 20,             ht_replace(ht,"b",2,21) );
    ASSERT_EQUAL( 21,             ht_get(ht,"b",2)        );
    ASSERT_EQUAL( 21,             ht_remove(ht,"b",2)     );
    ASSERT_EQUAL( DOES_NOT_EXIST, ht_get(ht,"b",2)        );
    ASSERT_EQUAL( 0,              ht_count(ht)          );

    ht_free(ht);

    // In a quiecent state; it is safe to free.
    rcu_update();
}

void *simple_worker (void *arg) {
    worker_data_t *wd = (worker_data_t *)arg;
    hash_table_t *ht = wd->ht;
    CuTest* tc = wd->tc;
    uint64_t d = wd->id;
    int iters = 20000;

    SYNC_ADD(wd->wait, -1);
    do { } while (*((volatile worker_data_t *)wd)->wait); // wait for all workers to be ready

    int i, j;
    for (j = 0; j < 10; ++j) {
        for (i = d; i < iters; i+=2) {
            char key[8];
            sprintf(key, "k%u", i); 
            ASSERT_EQUAL( DOES_NOT_EXIST, ht_add(ht, key, strlen(key)+1, d+1) );
        }
        for (i = d; i < iters; i+=2) {
            char key[8];
            sprintf(key, "k%u", i); 
            ASSERT_EQUAL( d+1, ht_remove(ht, key, strlen(key)+1) );
        }
        rcu_update();
    }
    return NULL;
}

// Do some simple concurrent testing
void simple_add_remove (CuTest* tc) {

    pthread_t thread[2];
    worker_data_t wd[2];
    int wait = 2;
    hash_table_t *ht = ht_alloc();

    // In 2 threads, add & remove even & odd elements concurrently
    int i;
    for (i = 0; i < 2; ++i) {
        wd[i].id = i;
        wd[i].tc = tc;
        wd[i].ht = ht;
        wd[i].wait = &wait;
        int rc = pthread_create(thread + i, NULL, simple_worker, wd + i);
        if (rc != 0) { perror("pthread_create"); return; }
    }
    for (i = 0; i < 2; ++i) {
        pthread_join(thread[i], NULL);
    }

    // In the end, all members should be removed
    ASSERT_EQUAL( 0, ht_count(ht) );

    // In a quiecent state; it is safe to free.
    ht_free(ht);
}


void *inserter_worker (void *arg) {
    //pthread_t thread[NUM_THREADS];

    //hash_table_t *ht = ht_alloc();
    return NULL;
}

// Concurrent insertion
void concurrent_insert (CuTest* tc) {
}

int main (void) {

    nbd_init();
    //lwt_set_trace_level("h4");

    // Create and run test suite
	CuString *output = CuStringNew();
	CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, basic_test);
    SUITE_ADD_TEST(suite, simple_add_remove);

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);

    return 0;
}
