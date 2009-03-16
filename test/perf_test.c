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

static int num_threads_;
static volatile int start_, stop_, load_;
static map_t *map_;
static int get_range_, put_range_;
static size_t num_keys_;
static double load_time_;
static int duration_;

#define OP_SELECT_RANGE (1ULL << 20)

void *worker (void *arg) {
    volatile uint64_t ops = 0;

    // Wait for all the worker threads to be ready.
    (void)SYNC_ADD(&load_, -1);
    do {} while (load_);

    // Pre-load map
    int n = num_keys_ / 2 / num_threads_;
    for (int i = 0; i < n; ++i) {
        map_key_t key = (nbd_rand() & (num_keys_ - 1)) + 1;
        map_set(map_, key, key);
    }

    // Wait for all the worker threads to be done loading.
    (void)SYNC_ADD(&start_, -1);
    do {} while (start_);

    while (!stop_) {
        ++ops;
        map_key_t key = (nbd_rand() & (num_keys_ - 1)) + 1;
        map_key_t x = nbd_rand() & (OP_SELECT_RANGE - 1);
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
        } else if (x < put_range_) {
            map_add(map_, key, key);
        } else {
            map_remove(map_, key);
        }
        rcu_update();
    }

    return (void *)ops;
}

uint64_t run_test (void) {
    load_ = num_threads_ + 1;
    start_ = num_threads_ + 1;

    stop_ = 0;

    pthread_t thread[MAX_NUM_THREADS];
    for (int i = 0; i < num_threads_; ++i) {
        int rc = nbd_thread_create(thread + i, i, worker, (void*)(size_t)i);
        if (rc != 0) { perror("pthread_create"); exit(rc); }
    }

    do { /* nothing */ } while (load_ != 1);
    load_ = 0;

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    do { /* nothing */ } while (start_ != 1);

    gettimeofday(&tv2, NULL);
    load_time_ = (double)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000000;

    start_ = 0;
    sleep(duration_);
    stop_ = 1;

    uint64_t ops = 0;
    for (int i = 0; i < num_threads_; ++i) {
        void *count;
        pthread_join(thread[i], &count);
        ops += (size_t)count;
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
    if (num_threads_ > MAX_NUM_THREADS) { num_threads_ = MAX_NUM_THREADS; }
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
    }
    if (num_threads_ > MAX_NUM_THREADS) {
        fprintf(stderr, "%s: Number of threads cannot be more than %d\n", program_name, MAX_NUM_THREADS);
        return -1;
    }

    int table_scale = 12;
    if (argc > 2) {
        table_scale = strtol(argv[2], NULL, 10);
        if (errno) {
            fprintf(stderr, "%s: Invalid argument for the scale of the collection\n", program_name);
            return -1;
        }
        table_scale = strtol(argv[2], NULL, 10);
        if (table_scale < 0 || table_scale > 36) {
            fprintf(stderr, "%s: The scale of the collection must be between 0 and 36\n", program_name);
            return -1;
        }
    }

    int read_ratio = 90;
    int put_ratio = 50;
    get_range_ = (int)((double)OP_SELECT_RANGE / 100 * read_ratio);
    put_range_ = get_range_ + (int)(((double)OP_SELECT_RANGE - get_range_) / 100 * put_ratio);

    static const map_impl_t *map_types[] = { &MAP_IMPL_HT };
    for (int i = 0; i < sizeof(map_types)/sizeof(*map_types); ++i) {
#ifdef TEST_STRING_KEYS
        map_ = map_alloc(map_types[i], &DATATYPE_NSTRING);
#else
        map_ = map_alloc(map_types[i], NULL);
#endif

        num_keys_ = 1ULL << table_scale;

        duration_ = 1 + table_scale/4;
        double mops_per_sec = (double)run_test() / 1000000.0 / duration_;

        printf("Threads:%-2d  Size:2^%-2d  load time:%-4.2f  Mops/s:%-4.2f  per-thread:%-4.2f  ",
                num_threads_, table_scale, load_time_, mops_per_sec, mops_per_sec/num_threads_);
        map_print(map_, FALSE);
        fflush(stdout);

        map_free(map_);
    }

    return 0;
}
