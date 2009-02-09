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
static long num_threads_;
static map_t *map_;
static int get_range_;
static int put_range_;
static int num_keys_;
static map_key_t *keys_ = NULL;
static uint64_t times_[MAX_NUM_THREADS] = {};
static int ops_[MAX_NUM_THREADS] = {};

void *worker (void *arg) {
    int tid = (int)(size_t)arg;
    uint64_t s = nbd_rand_seed(tid);
    int get_ops = 0, put_ops = 0, del_ops = 0;

    // Wait for all the worker threads to be ready.
    (void)SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

    uint64_t t1 = rdtsc();

    while (!stop_) {
        int r = nbd_next_rand(&s);
        int x = r & ( (1 << 20) - 1 );
        int i = nbd_next_rand(&s) & (num_keys_ - 1);
        map_key_t key = keys_[i];
        if (x < get_range_) {
            map_val_t val = map_get(map_, key);
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

    times_[tid] = rdtsc() - t1;
    ops_[tid] = get_ops + put_ops + del_ops;

    return NULL;
}

void run_test (void) {
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
    sleep(2);
    stop_ = 1;

    for (int i = 0; i < num_threads_; ++i) {
        pthread_join(thread[i], NULL);
    }
}

int main (int argc, char **argv) {
    char* program_name = argv[0];

    if (argc > 2) {
        fprintf(stderr, "Usage: %s num_threads\n", program_name);
        return -1;
    }

    num_threads_ = 2;
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


    int table_scale = 10;
    int read_ratio = 95;
    get_range_ = (read_ratio << 20) / 100;
    put_range_ = (((1 << 20) - get_range_) >> 1) + get_range_;

    static const map_impl_t *map_types[] = { &MAP_IMPL_HT };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
#ifdef TEST_STRING_KEYS
        map_ = map_alloc(map_types[i], &DATATYPE_NSTRING);
#else
        map_ = map_alloc(map_types[i], NULL);
#endif

        // Do some warmup
        num_keys_ = 1 << table_scale;
        keys_ = nbd_malloc(sizeof(map_key_t) * num_keys_);
        for (int j = 0; j < num_keys_; ++j) {
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

        struct timeval tv1, tv2;
        gettimeofday(&tv1, NULL);

        int num_trials = 1;
        for (int i = 0; i < num_trials; ++i) {
            run_test();
        }

        gettimeofday(&tv2, NULL);
        int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
        map_print(map_);
        printf("Th:%ld Time:%dms\n\n", num_threads_, ms);
        fflush(stdout);

        map_free(map_);
    }

    return 0;
}
