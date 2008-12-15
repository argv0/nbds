/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * tests ported from high-scale-lib
 * http://sourceforge.net/projects/high-scale-lib
 */
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>

#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "nstring.h"
#include "map.h"
#include "list.h"
#include "skiplist.h"
#include "hashtable.h"
#include "lwt.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

//#define TEST_STRING_KEYS

typedef struct worker_data {
    int id;
    CuTest *tc;
    map_t *map;
    volatile int *wait;
} worker_data_t;

static const map_impl_t *map_type_;

static uint64_t iterator_size (map_t *map) {
    map_iter_t *iter = map_iter_begin(map, NULL);
    uint64_t count = 0;
    while (map_iter_next(iter, NULL) != DOES_NOT_EXIST) {
        count++;
    }
    map_iter_free(iter);
    return count;
}

// Test some basic stuff; add a few keys, remove a few keys
void basic_test (CuTest* tc) {

#ifdef TEST_STRING_KEYS
    map_t *map = map_alloc(map_type_, &DATATYPE_NSTRING);
    nstring_t *k1 = ns_alloc(3); strcpy(k1->data, "k1");
    nstring_t *k2 = ns_alloc(3); strcpy(k1->data, "k2");
    nstring_t *k3 = ns_alloc(3); strcpy(k1->data, "k3");
    nstring_t *k4 = ns_alloc(3); strcpy(k1->data, "k4");
#else
    map_t *map = map_alloc(map_type_, NULL);
    void *k1 = (void *)1;
    void *k2 = (void *)2;
    void *k3 = (void *)3;
    void *k4 = (void *)4;
#endif

    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add    (map, k1,10) );
    ASSERT_EQUAL( 1,              map_count  (map)        );
    ASSERT_EQUAL( 1,              iterator_size(map)      );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add    (map, k2,20) );
    ASSERT_EQUAL( 2,              map_count  (map)        );
    ASSERT_EQUAL( 2,              iterator_size(map)      );
    ASSERT_EQUAL( 20,             map_get    (map, k2)    );
    ASSERT_EQUAL( 10,             map_set    (map, k1,11) );
    ASSERT_EQUAL( 20,             map_set    (map, k2,21) );
    ASSERT_EQUAL( 2,              map_count  (map)        );
    ASSERT_EQUAL( 2,              iterator_size(map)      );
    ASSERT_EQUAL( 21,             map_add    (map, k2,22) );
    ASSERT_EQUAL( 11,             map_remove (map, k1)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k1)    );
    ASSERT_EQUAL( 1,              map_count  (map)        );
    ASSERT_EQUAL( 1,              iterator_size(map)      );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove (map, k1)    );
    ASSERT_EQUAL( 21,             map_remove (map, k2)    );
    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( 0,              iterator_size(map)      );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove (map, k2)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove (map, k3)    );
    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( 0,              iterator_size(map)      );
    
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add    (map, k4,40) );
    ASSERT_EQUAL( 40,             map_get    (map, k4)    );
    ASSERT_EQUAL( 1,              map_count  (map)        );
    ASSERT_EQUAL( 1,              iterator_size(map)      );
    ASSERT_EQUAL( 40,             map_remove (map, k4)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k4)    );
    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( 0,              iterator_size(map)      );

    ASSERT_EQUAL( DOES_NOT_EXIST, map_replace(map, k4,10) );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k4)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_set    (map, k4,40) );
    ASSERT_EQUAL( 40,             map_replace(map, k4,41) );
    ASSERT_EQUAL( 41,             map_get    (map, k4)    );
    ASSERT_EQUAL( 41,             map_remove (map, k4)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k4)    );
    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( 0,              iterator_size(map)      );

    ASSERT_EQUAL( DOES_NOT_EXIST, map_replace(map, k2,20) );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k2)    );

    // In the end, all entries should be removed
    ASSERT_EQUAL( DOES_NOT_EXIST, map_set    (map, k2,20) );
    ASSERT_EQUAL( 20,             map_replace(map, k2,21) );
    ASSERT_EQUAL( 21,             map_get    (map, k2)    );
    ASSERT_EQUAL( 21,             map_remove (map, k2)    );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get    (map, k2)    );
    ASSERT_EQUAL( 0,              map_count  (map)        );
    ASSERT_EQUAL( 0,              iterator_size(map)      );

    map_free(map);

    rcu_update(); // In a quiecent state.
#ifdef TEST_STRING_KEYS
    nbd_free(k1); nbd_free(k2); nbd_free(k3); nbd_free(k4);
#endif
}

void *add_remove_worker (void *arg) {
    worker_data_t *wd = (worker_data_t *)arg;
    map_t *map = wd->map;
    CuTest* tc = wd->tc;
    uint64_t d = wd->id;
    int iters = 10000;

    SYNC_ADD(wd->wait, -1);
    do { } while (*wd->wait); // wait for all workers to be ready

#ifdef TEST_STRING_KEYS
    nstring_t *key = ns_alloc(9);
#else
    void *key;
#endif

    for (int j = 0; j < 10; ++j) {
        for (uint64_t i = d+1; i < iters; i+=2) {
#ifdef TEST_STRING_KEYS
            memset(key->data, 0, key->len);
            snprintf(key->data, key->len, "%llu", i);
#else
            key = (void *)i;
#endif
            TRACE("t0", "test map_add() iteration (%llu, %llu)", j, i);
            ASSERT_EQUAL(DOES_NOT_EXIST, map_add(map, key, d+1) );
            rcu_update(); // In a quiecent state.
        }
        for (uint64_t i = d+1; i < iters; i+=2) {
#ifdef TEST_STRING_KEYS
            memset(key->data, 0, key->len);
            snprintf(key->data, key->len, "%llu", i);
#else
            key = (void *)i;
#endif
            TRACE("t0", "test map_remove() iteration (%llu, %llu)", j, i);
            ASSERT_EQUAL(d+1, map_remove(map, key) );
            rcu_update(); // In a quiecent state.
        }
    }
    return NULL;
}

// Do some simple concurrent testing
void concurrent_add_remove_test (CuTest* tc) {

    pthread_t thread[2];
    worker_data_t wd[2];
    volatile int wait = 2;
#ifdef TEST_STRING_KEYS
    map_t *map = map_alloc(map_type_, &DATATYPE_NSTRING);
#else
    map_t *map = map_alloc(map_type_, NULL);
#endif

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    // In 2 threads, add & remove even & odd elements concurrently
    int i;
    for (i = 0; i < 2; ++i) {
        wd[i].id = i;
        wd[i].tc = tc;
        wd[i].map = map;
        wd[i].wait = &wait;
        int rc = nbd_thread_create(thread + i, i, add_remove_worker, wd + i);
        if (rc != 0) { perror("nbd_thread_create"); return; }
    }

    for (i = 0; i < 2; ++i) {
        pthread_join(thread[i], NULL);
    }

    gettimeofday(&tv2, NULL);
    int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
    map_print(map);
    printf("Th:%d Time:%dms\n", 2, ms);
    fflush(stdout);

    // In the end, all members should be removed
    ASSERT_EQUAL( 0, map_count(map) );
    ASSERT_EQUAL( 0, iterator_size(map) );

    // In a quiecent state; it is safe to free.
    map_free(map);
}

void basic_iteration_test (CuTest* tc) {
#ifdef TEST_STRING_KEYS
    map_t *map = map_alloc(map_type_, &DATATYPE_NSTRING);
    nstring_t *k1 = ns_alloc(3); strcpy(k1->data, "k1");
    nstring_t *k2 = ns_alloc(3); strcpy(k1->data, "k2");
    nstring_t *x_k;
    nstring_t *y_k;
#else
    map_t *map = map_alloc(map_type_, NULL);
    void *k1 = (void *)1;
    void *k2 = (void *)2;
    void *x_k;
    void *y_k;
#endif

    ASSERT_EQUAL( DOES_NOT_EXIST, map_add    (map, k1,1) );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add    (map, k2,2) );

    uint64_t x_v, y_v;
    map_iter_t *iter = map_iter_begin(map, NULL);
    x_v = map_iter_next(iter, &x_k);
    y_v = map_iter_next(iter, &y_k);
    ASSERT_EQUAL( DOES_NOT_EXIST, map_iter_next(iter, NULL) );
    map_iter_free(iter);
#ifdef TEST_STRING_KEYS
    ASSERT_EQUAL( TRUE, (ns_cmp(x_k, k1) == 0 && x_v == 1) || (ns_cmp(y_k, k1) == 0 && y_v == 1) );
    ASSERT_EQUAL( TRUE, (ns_cmp(x_k, k2) == 0 && x_v == 2) || (ns_cmp(y_k, k2) == 0 && y_v == 2) );
    nbd_free(k1);
    nbd_free(k2);
#else
    ASSERT_EQUAL( TRUE, (x_k == k1 && x_v == 1) || (y_k == k1 && y_v == 1) );
    ASSERT_EQUAL( TRUE, (x_k == k2 && x_v == 2) || (y_k == k2 && y_v == 2) );
#endif

    map_free(map);
}

void big_iteration_test (CuTest* tc) {
    static const int n = 10000;
    
#ifdef TEST_STRING_KEYS
    map_t *map = map_alloc(map_type_, &DATATYPE_NSTRING);
    nstring_t *key = ns_alloc(9);
    nstring_t *k3 = ns_alloc(3); strcpy(k1->data, "k3");
    nstring_t *k4 = ns_alloc(3); strcpy(k1->data, "k4");
#else
    map_t *map = map_alloc(map_type_, NULL);
    void *k3 = (void *)3;
    void *k4 = (void *)4;
    void *key;
#endif

    for (size_t i = 1; i <= n; ++i) {
#ifdef TEST_STRING_KEYS
        memset(key->data, 0, key->len);
        snprintf(key->data, key->len, "k%llu", i);
#else
        key = (void *)i;
#endif
        ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, key)    );
        ASSERT_EQUAL( DOES_NOT_EXIST, map_set(map, key, i) );
        ASSERT_EQUAL( i,              map_get(map, key)    );
        rcu_update(); // In a quiecent state.
    }

    ASSERT_EQUAL( n, map_count(map) );
    ASSERT_EQUAL( n, iterator_size(map) );

    uint64_t sum = 0;
    uint64_t val;
    map_iter_t *iter = map_iter_begin(map, NULL);
    while ((val = map_iter_next(iter, NULL)) != DOES_NOT_EXIST) {
        sum += val;
    }
    map_iter_free(iter);
    ASSERT_EQUAL(n*(n+1)/2, sum);
    ASSERT_EQUAL(3, map_remove(map, k3));
    ASSERT_EQUAL(4, map_remove(map, k4));
    sum = 0;
    iter = map_iter_begin(map, NULL);
    while ((val = map_iter_next(iter, NULL)) != DOES_NOT_EXIST) {
        sum += val;
    }
    map_iter_free(iter);
    ASSERT_EQUAL(n*(n+1)/2 - (3+4), sum);
        
#ifdef TEST_STRING_KEYS
    nbd_free(key);
#endif
}

int main (void) {

    nbd_init();
    lwt_set_trace_level("h3");

    static const map_impl_t *map_types[] = { &ll_map_impl, &sl_map_impl, &ht_map_impl };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
        map_type_ = map_types[i];

        // Create and run test suite
        CuString *output = CuStringNew();
        CuSuite* suite = CuSuiteNew();

        SUITE_ADD_TEST(suite, basic_test);
        SUITE_ADD_TEST(suite, concurrent_add_remove_test);
        SUITE_ADD_TEST(suite, basic_iteration_test);
        SUITE_ADD_TEST(suite, big_iteration_test);

        CuSuiteRun(suite);
        CuSuiteDetails(suite, output);
        printf("%s\n", output->buffer);
    }

    return 0;
}
