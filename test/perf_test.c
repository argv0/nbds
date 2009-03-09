#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#include "common.h"
#include "nstring.h"
#include "runtime.h"
#include "map.h"
#include "rcu.h"
#include "mem.h"
#include "list.h"
#include "skiplist.h"
#include "hashtable.h"

//#define TEST_STRING_KEYS

static volatile int wait_;
static volatile int stop_;
static int num_threads_;
static int duration_;
static map_t *map_;
static int get_range_;
static int put_range_;
static size_t num_keys_;
static map_key_t *keys_ = NULL;
static int ops_[MAX_NUM_THREADS] = {};

#define FOO (1ULL << 20)

void *worker (void *arg) {
    int tid = (int)(size_t)arg;
    uint64_t s = nbd_rand_seed(tid);
    int get_ops = 0, put_ops = 0, del_ops = 0;

    // Wait for all the worker threads to be ready.
    (void)SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

    while (!stop_) {
        map_key_t key = keys_[ nbd_next_rand(&s) & (num_keys_ - 1) ];
        uint32_t x = nbd_next_rand(&s) & (FOO - 1);
        if (x < get_range_) {
#ifndef NDEBUG
            map_val_t val = 
#endif
                map_get(map_, key);
#ifdef TEST_STRING_KEYS
            ASSERT(val == DOES_NOT_EXIST || ns_cmp((nstring_t *)key, (nstring_t *)val) == 0);
#else
            ASSERT(val == DOES_NOT_EXIST || key == val);
#endif
            get_ops++;
        } else if (x < put_range_) {
            map_add(map_, key, key);
            put_ops++;
        } else {
            map_remove(map_, key);
            del_ops++;
        }
        rcu_update();
    }

    ops_[tid] = get_ops + put_ops + del_ops;

    return NULL;
}

int run_test (void) {
    int ops;
    wait_ = num_threads_ + 1;

    // Quicky sanity check
    int n = 100;
    if (num_keys_ < n) { n = num_keys_; }
    for (int i = 0; i < n; ++i) {
        map_set(map_, keys_[i], keys_[i]);
        for(int j = 0; j < i; ++j) {
#ifdef TEST_STRING_KEYS
            ASSERT(ns_cmp((nstring_t *)map_get(map_, keys_[i]), (nstring_t *)keys_[i]) == 0);
#else
            ASSERT(map_get(map_, keys_[i]) == keys_[i]);
#endif
        }
    }

    stop_ = 0;

    pthread_t thread[MAX_NUM_THREADS];
    for (int i = 0; i < num_threads_; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void*)(size_t)i);
        if (rc != 0) { perror("pthread_create"); exit(rc); }
    }

    do { /* nothing */ } while (wait_ != 1);

    wait_ = 0;
    sleep(duration_);
    stop_ = 1;

    for (int i = 0; i < num_threads_; ++i) {
        pthread_join(thread[i], NULL);
    }
    ops = 0;
    for (int i = 0; i < num_threads_; ++i) {
        ops += ops_[i];
    }
    return ops;
}

int main (int argc, char **argv) {
    char* program_name = argv[0];

    if (argc > 3) {
        fprintf(stderr, "Usage: %s num_threads\n", program_name);
        return -1;
    }

    num_threads_ = 2;
    if (argc > 1)
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

    int table_scale = 12;
    if (argc > 2) {
        table_scale = strtol(argv[2], NULL, 10);
        if (errno) {
            fprintf(stderr, "%s: Invalid argument for the scale of the collection\n", program_name);
            return -1;
        }
        table_scale = strtol(argv[2], NULL, 10);
        if (table_scale < 0 || table_scale > 31) {
            fprintf(stderr, "%s: The scale of the collection must be between 0 and 31\n", program_name);
            return -1;
        }
    }


    int read_ratio = 90;
    int put_ratio = 50;
    get_range_ = (int)((double)FOO / 100 * read_ratio);
    put_range_ = get_range_ + (int)(((double)FOO - get_range_) / 100 * put_ratio);

    static const map_impl_t *map_types[] = { &MAP_IMPL_SL };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
#ifdef TEST_STRING_KEYS
        map_ = map_alloc(map_types[i], &DATATYPE_NSTRING);
#else
        map_ = map_alloc(map_types[i], NULL);
#endif

        // Do some warmup
        num_keys_ = 1ULL << table_scale;
        keys_ = nbd_malloc(sizeof(map_key_t) * num_keys_);
        ASSERT(keys_ != NULL);
        for (uint64_t j = 0; j < num_keys_; ++j) {
#ifdef TEST_STRING_KEYS
            char tmp[64];
            snprintf(tmp, sizeof(tmp), "%dabc%d", j, j*17+123);
            int n = strlen(tmp);
            keys_[j] = ns_alloc(n);
            memcpy(keys_[j], tmp, n);
#else
            keys_[j] = j*17+123;
#endif
        }

        duration_ = 10;
        int num_trials = 1;
        int ops = 0;
        for (int i = 0; i < num_trials; ++i) {
            ops += run_test();
        }
        double ops_per_sec = ops / num_trials / duration_;

        //map_print(map_);
        printf("Threads:%-2d  Size:2^%-2d  Mops/Sec:%-4.3g  per-thread:%-4.3g\n\n", 
                num_threads_, table_scale, ops_per_sec/1000000, ops_per_sec/num_threads_/1000000);
        fflush(stdout);

        map_free(map_);
    }

    return 0;
}
