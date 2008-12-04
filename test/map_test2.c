/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>

#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "map.h"
#include "lwt.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

typedef struct worker_data {
    int id;
    CuTest *tc;
    map_t *map;
    volatile int *wait;
} worker_data_t;

static map_type_t map_type_;

// Test some basic stuff; add a few keys, remove a few keys
void simple (CuTest* tc) {

    map_t *map = map_alloc(map_type_, NULL, NULL, NULL);

    ASSERT_EQUAL( 0,              map_count(map)            );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add(map, (void *)'a',10)     );
    ASSERT_EQUAL( 1,              map_count(map)            );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add(map, (void *)'b',20)     );
    ASSERT_EQUAL( 2,              map_count(map)            );
    ASSERT_EQUAL( 20,             map_get(map, (void *)'b')        );
    ASSERT_EQUAL( 10,             map_set(map, (void *)'a',11)     );
    ASSERT_EQUAL( 20,             map_set(map, (void *)'b',21)     );
    ASSERT_EQUAL( 2,              map_count(map)            );
    ASSERT_EQUAL( 21,             map_add(map, (void *)'b',22)     );
    ASSERT_EQUAL( 11,             map_remove(map, (void *)'a')     );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'a')        );
    ASSERT_EQUAL( 1,              map_count(map)            );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove(map, (void *)'a')     );
    ASSERT_EQUAL( 21,             map_remove(map, (void *)'b')     );
    ASSERT_EQUAL( 0,              map_count(map)            );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove(map, (void *)'b')     );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_remove(map, (void *)'c')     );
    ASSERT_EQUAL( 0,              map_count(map)            );
    
    ASSERT_EQUAL( DOES_NOT_EXIST, map_add(map, (void *)'d',40)     );
    ASSERT_EQUAL( 40,             map_get(map, (void *)'d')        );
    ASSERT_EQUAL( 1,              map_count(map)            );
    ASSERT_EQUAL( 40,             map_remove(map, (void *)'d')     );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'d')        );
    ASSERT_EQUAL( 0,              map_count(map)            );

    ASSERT_EQUAL( DOES_NOT_EXIST, map_replace(map, (void *)'d',10) );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'d')        );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_set(map, (void *)'d',40)     );
    ASSERT_EQUAL( 40,             map_replace(map, (void *)'d',41) );
    ASSERT_EQUAL( 41,             map_get(map, (void *)'d')        );
    ASSERT_EQUAL( 41,             map_remove(map, (void *)'d')     );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'d')        );
    ASSERT_EQUAL( 0,              map_count(map)            );

    ASSERT_EQUAL( DOES_NOT_EXIST, map_replace(map, (void *)'b',20) );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'b')        );

    // In the end, all entries should be removed
    ASSERT_EQUAL( DOES_NOT_EXIST, map_set(map, (void *)'b',20)     );
    ASSERT_EQUAL( 20,             map_replace(map, (void *)'b',21) );
    ASSERT_EQUAL( 21,             map_get(map, (void *)'b')        );
    ASSERT_EQUAL( 21,             map_remove(map, (void *)'b')     );
    ASSERT_EQUAL( DOES_NOT_EXIST, map_get(map, (void *)'b')        );
    ASSERT_EQUAL( 0,              map_count(map)            );

    map_free(map);

    // In a quiecent state; it is safe to free.
    rcu_update();
}

void *add_remove_worker (void *arg) {
    worker_data_t *wd = (worker_data_t *)arg;
    map_t *map = wd->map;
    CuTest* tc = wd->tc;
    uint64_t d = wd->id;
    int iters = (map_type_ == MAP_TYPE_LIST) ? 5000 : 500000;

    SYNC_ADD(wd->wait, -1);
    do { } while (*wd->wait); // wait for all workers to be ready

    for (int j = 0; j < 10; ++j) {
        for (uint64_t i = d+1; i < iters; i+=2) {
            TRACE("t0", "test map_add() iteration (%llu, %llu)", j, i);
            CuAssertIntEquals_Msg(tc, (void *)i, DOES_NOT_EXIST, map_add(map, (void *)i, d+1) );
            rcu_update();
        }
        for (uint64_t i = d+1; i < iters; i+=2) {
            TRACE("t0", "test map_remove() iteration (%llu, %llu)", j, i);
            CuAssertIntEquals_Msg(tc, (void *)i, d+1, map_remove(map, (void *)i) );
            rcu_update();
        }
    }
    return NULL;
}

// Do some simple concurrent testing
void add_remove (CuTest* tc) {

    pthread_t thread[2];
    worker_data_t wd[2];
    volatile int wait = 2;
    map_t *map = map_alloc(map_type_, NULL, NULL, NULL);

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

    // In a quiecent state; it is safe to free.
    map_free(map);
}

void *inserter_worker (void *arg) {
    //pthread_t thread[NUM_THREADS];

    //map_t *map = map_alloc(map_type_);
    return NULL;
}

// Concurrent insertion
void concurrent_insert (CuTest* tc) {
}

int main (void) {

    nbd_init();
    lwt_set_trace_level("h3");

    map_type_t map_types[] = { MAP_TYPE_LIST, MAP_TYPE_SKIPLIST, MAP_TYPE_HASHTABLE };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
        map_type_ = map_types[i];

        // Create and run test suite
        CuString *output = CuStringNew();
        CuSuite* suite = CuSuiteNew();

        SUITE_ADD_TEST(suite, simple);
        SUITE_ADD_TEST(suite, add_remove);

        CuSuiteRun(suite);
        CuSuiteDetails(suite, output);
        printf("%s\n", output->buffer);
    }

    return 0;
}
