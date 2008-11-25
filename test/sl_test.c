#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "common.h"
#include "runtime.h"
#include "struct.h"

#define NUM_ITERATIONS 10000000

static volatile int wait_;
static long num_threads_;
static skiplist_t *sl_;

void *worker (void *arg) {

    // Wait for all the worker threads to be ready.
    SYNC_ADD(&wait_, -1);
    do {} while (wait_); 

    for (int i = 0; i < NUM_ITERATIONS/num_threads_; ++i) {
        unsigned r = nbd_rand();
        int key = r & 0xF;
        if (r & (1 << 8)) {
            sl_add(sl_, key, 1);
        } else {
            sl_remove(sl_, key);
        }

        rcu_update();
    }

    return NULL;
}

int main (int argc, char **argv) {
    nbd_init();
    //lwt_set_trace_level("s3");

    char* program_name = argv[0];
    pthread_t thread[MAX_NUM_THREADS];

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

    sl_ = sl_alloc();

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
    sl_print(sl_);
    printf("Th:%ld Time:%dms\n", num_threads_, ms);

    return 0;
}
