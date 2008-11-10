/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * lightweight tracing
 */ 
#ifndef LWT_H
#define LWT_H
#include "tls.h"

#ifndef ENABLE_TRACE
#define TRACE(...) do { } while (0)
#else
#define TRACE(flag, format, v1, v2) lwt_trace(flag, format, (size_t)(v1), (size_t)(v2))
#endif

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

void lwt_init (void);
void lwt_thread_init (int thread_id);

void lwt_dump (const char *file_name) __attribute__ ((externally_visible));

// <flags> indicates what kind of trace messages should be included in the dump. <flags> is a sequence of letters
// followed by numbers (e.g. "x1c9n2g3"). The letters indicate trace categories and the numbers are trace levels 
// for each category. If a category appears in <flags>, then messages from that category will be included in the
// dump if they have a trace level less than or equal to the one specified in <flags>. Categories are case
// sensitive.
void lwt_set_trace_level (const char *flags);

// <flag> is a two character string containing a letter followed by a number (e.g. "f3"). The letter indicates a
// trace category, and the number a trace level. <flag> controls whether or not the trace message gets included in
// the dump. It is only included when its specified category is enabled at a trace level greater than or equal to
// the one in <flag>. Categories are case sensitive. 
static inline void lwt_trace (const char *flag, const char *format, size_t value1, size_t value2) {
    extern uint64_t flag_mask_;
    if (EXPECT_FALSE(flag_mask_ & (1 << (flag[0] - 'A')))) {
        LOCALIZE_THREAD_LOCAL(tb_, lwt_buffer_t *);
        unsigned int u, l;
        __asm__ __volatile__("rdtsc" : "=a" (l), "=d" (u)); 
        uint64_t timestamp = ((uint64_t)u << 32) | l; 
        // embed <flags> in <format> so we don't have to make the lwt_record_t any bigger than it already is
        format = (const char *)((size_t)format | ((uint64_t)flag[0] << 56) | ((uint64_t)flag[1] << 48));
        lwt_record_t temp = { timestamp, format, value1, value2 };
        if (tb_) {
            tb_->x[tb_->head++ & LWT_BUFFER_MASK] = temp;
        }
    }
}
#endif//LWT_H
