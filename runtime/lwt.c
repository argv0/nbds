/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * lightweight tracing 
 */
#include <stdio.h>

#include "common.h"
#include "tls.h"
#include "lwt.h"
#include "mem.h"

#define LWT_BUFFER_SCALE 16
#define LWT_BUFFER_SIZE (1 << LWT_BUFFER_SCALE)
#define LWT_BUFFER_MASK (LWT_BUFFER_SIZE - 1)

typedef struct lwt_record {
    uint64_t timestamp;
    const char *format;
    size_t value1;
    size_t value2;
} lwt_record_t;

typedef struct lwt_buffer {
    uint32_t head;
    lwt_record_t x[0];
} lwt_buffer_t;

DECLARE_THREAD_LOCAL(tb_, int);

lwt_buffer_t *lwt_buf_[MAX_NUM_THREADS] = {};
uint64_t flag_mask_ = 0;
static int buf_count_ = 0;
static const char *flags_ = "";

void lwt_init (void)
{
    INIT_THREAD_LOCAL(tb_, NULL);
}

void lwt_thread_init (int thread_id)
{
    assert(thread_id < MAX_NUM_THREADS);
    if (lwt_buf_[thread_id] == NULL) {
        lwt_buf_[thread_id] = (lwt_buffer_t *)nbd_malloc(sizeof(lwt_buffer_t) + sizeof(lwt_record_t) * LWT_BUFFER_SIZE);
        SYNC_ADD(&buf_count_, 1);
        memset(lwt_buf_[thread_id], 0, sizeof(lwt_buffer_t));
        SET_THREAD_LOCAL(tb_, lwt_buf_[thread_id]);
    }
}

void lwt_set_trace_level (const char *flags)
{
    assert(strlen(flags) % 2 == 0); // a well formed <flags> should be an even number of characters long
    flags_ = flags;
    int i;
    for (i = 0; flags[i]; i+=2) {
        flag_mask_ |= 1 << (flags[i] - 'A');
    }
}

static inline void dump_record (FILE *file, int thread_id, lwt_record_t *r, uint64_t offset)
{
    // print the record if its trace category is enabled at a high enough level
    int flag  =  (size_t)r->format >> 56;
    int level = ((size_t)r->format >> 48) & 0xFF;
    const char *f = strchr(flags_, flag);
    if (f != NULL && level <= f[1]) {
        char s[3] = {flag, level, '\0'};
        fprintf(file, "%09llu %d %s ", ((uint64_t)r->timestamp - offset) >> 6, thread_id, s);
        const char *format = (const char *)(((uint64_t)r->format << 16) >> 16); // strip out the embedded flags
        fprintf(file, format, r->value1, r->value2);
        fprintf(file, "\n");
    }
}

static void dump_buffer (FILE *file, int thread_id, uint64_t offset)
{
    assert(thread_id < buf_count_);

    lwt_buffer_t *tb = lwt_buf_[thread_id]; 
    int i;
    if (tb->head > LWT_BUFFER_SIZE) {
        for (i = tb->head & LWT_BUFFER_MASK; i < LWT_BUFFER_SIZE; ++i) {
            dump_record(file, thread_id, tb->x + i, offset);
        }
    }

    for (i = 0; i < (tb->head & LWT_BUFFER_MASK); ++i) {
        dump_record(file, thread_id, tb->x + i, offset);
    }
}

void lwt_dump (const char *file_name)
{
    uint64_t offset = (uint64_t)-1;
    int i;

    for (i = 0; i < buf_count_; ++i) {
        if (lwt_buf_[i] != NULL && lwt_buf_[i]->head != 0) {
            uint64_t x = lwt_buf_[i]->x[0].timestamp;
            if (x < offset) {
                offset = x;
            }
            if (lwt_buf_[i]->head > LWT_BUFFER_SIZE)
            {
                x = lwt_buf_[i]->x[lwt_buf_[i]->head & LWT_BUFFER_MASK].timestamp;
                if (x < offset) {
                    offset = x;
                }
            }
        }
    }

    if (offset != (uint64_t)-1) {
        FILE *file = fopen(file_name, "w");
        assert(file);
        for (i = 0; i < buf_count_; ++i) {
            if (lwt_buf_[i] != NULL) {
                dump_buffer(file, i, offset);
            }
        }
        fflush(file);
        fclose(file);
    }
}

void lwt_trace_i (const char *format, size_t value1, size_t value2) {
    LOCALIZE_THREAD_LOCAL(tb_, lwt_buffer_t *);
    if (tb_) {
        unsigned int u, l;
        __asm__ __volatile__("rdtsc" : "=a" (l), "=d" (u)); 
        uint64_t timestamp = ((uint64_t)u << 32) | l; 
        lwt_record_t temp = { timestamp, format, value1, value2 };
        tb_->x[tb_->head++ & LWT_BUFFER_MASK] = temp;
    }
}
