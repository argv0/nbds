#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "common.h"
#include "nstring.h"
#include "runtime.h"
#include "map.h"
#include "rcu.h"
#include "list.h"
#include "skiplist.h"
#include "hashtable.h"

#define NUM_ITERATIONS 10000000

//#define TEST_STRING_KEYS

static volatile int wait_;
static long num_threads_;
static map_t *map_;

void *worker (void *arg) {

    // Wait for all the worker threads to be ready.
    (void)SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

#ifdef TEST_STRING_KEYS
    nstring_t *key_str = ns_alloc(10);
#endif

    for (int i = 0; i < NUM_ITERATIONS/num_threads_; ++i) {
        unsigned r = nbd_rand();
        int key = r & 0xF;
#ifdef TEST_STRING_KEYS
        key_str->len = sprintf(key_str->data, "%X", key) + 1;
        assert(key_str->len <= 10);
        if (r & (1 << 8)) {
            map_set(map_, (map_key_t)key_str, 1);
        } else {
            map_remove(map_, (map_key_t)key_str);
        }
#else
        if (r & (1 << 8)) {
            map_set(map_, (map_key_t)(key + 1), 1);
        } else {
            map_remove(map_, (map_key_t)(key + 1));
        }
#endif

        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    lwt_set_trace_level("r0m3s3");

    char* program_name = argv[0];
    pthread_t thread[MAX_NUM_THREADS];

    if (argc > 2) {
        fprintf(stderr, "Usage: %s num_threads\n", program_name);
        return -1;
    }

    num_threads_ = MAX_NUM_THREADS;
    if (argc == 2)
    {
        errno = 0;
        num_threads_ = strtol(argv[1], NULL, 10);
        if (errno) {
            fprintf(stderr, "%s: Invalid argument for number of threads\n", program_name);
            return -1;
        }
        if (num_threads_ <= 0) {
            fprintf(stderr, "%s: Number of threads must be at least 1\n", program_name);
            return -1;
        }
        if (num_threads_ > MAX_NUM_THREADS) {
            fprintf(stderr, "%s: Number of threads cannot be more than %d\n", program_name, MAX_NUM_THREADS);
            return -1;
        }
    }

    static const map_impl_t *map_types[] = { &MAP_IMPL_LL, &MAP_IMPL_SL, &MAP_IMPL_HT };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
#ifdef TEST_STRING_KEYS
        map_ = map_alloc(map_types[i], &DATATYPE_NSTRING);
#else
        map_ = map_alloc(map_types[i], NULL);
#endif

        struct timeval tv1, tv2;
        gettimeofday(&tv1, NULL);

        wait_ = num_threads_;

        for (int i = 0; i < num_threads_; ++i) {
            int rc = nbd_thread_create(thread + i, i, worker, (void*)(size_t)i);
            if (rc != 0) { perror("pthread_create"); return rc; }
        }

        for (int i = 0; i < num_threads_; ++i) {
            pthread_join(thread[i], NULL);
        }

        gettimeofday(&tv2, NULL);
        int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
        map_print(map_);
        printf("Th:%ld Time:%dms\n\n", num_threads_, ms);
        fflush(stdout);
    }

    return 0;
}
