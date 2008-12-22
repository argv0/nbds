/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * hazard pointers
 *
 * www.research.ibm.com/people/m/michael/ieeetpds-2004.pdf
 *
 */
#include "common.h"
#include "lwt.h"
#include "mem.h"
#include "tls.h"
#include "runtime.h"
#include "hazard.h"

typedef struct pending {
    void * ptr; 
    free_t free_; 
} pending_t;

typedef struct haz_local {
    pending_t *pending; // to be freed
    int pending_size;
    int pending_count;

    haz_t static_haz[STATIC_HAZ_PER_THREAD];

    haz_t **dynamic;
    int dynamic_size;
    int dynamic_count;

} haz_local_t;

static haz_local_t haz_local_[MAX_NUM_THREADS] = {};

static void sort_hazards (haz_t *hazards, int n) {
    return;
}

static int search_hazards (void *p, haz_t *hazards, int n) {
    for (int i = 0; i < n; ++i) {
        if (hazards[i] == p) 
            return TRUE;
    }
    return FALSE;
}

static void resize_pending (void) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    haz_local_t *l = haz_local_ + tid_;
    pending_t *p = nbd_malloc(sizeof(pending_t) * l->pending_size * 2);
    memcpy(p, l->pending, l->pending_size);
    nbd_free(l->pending);
    l->pending = p;
    l->pending_size *= 2;
}

void haz_defer_free (void *d, free_t f) {
    assert(d);
    assert(f);
    LOCALIZE_THREAD_LOCAL(tid_, int);
    haz_local_t *l = haz_local_ + tid_;
    while (l->pending_count == l->pending_size) {

        if (l->pending_size == 0) {
            l->pending_size = MAX_NUM_THREADS * STATIC_HAZ_PER_THREAD;
            l->pending = nbd_malloc(sizeof(pending_t) * l->pending_size);
            break;
        }

        // scan for hazard pointers
        haz_t *hazards = nbd_malloc(sizeof(haz_t) * l->pending_size);
        int    hazard_count = 0;
        for (int i = 0; i < MAX_NUM_THREADS; ++i) {
            haz_local_t *h = haz_local_ + i;
            for (int j = 0; j < STATIC_HAZ_PER_THREAD; ++j) {
                if (h->static_haz[j] != NULL) {
                    if (hazard_count == l->pending_size) {
                        resize_pending();
                        nbd_free(hazards);
                        haz_defer_free(d, f);
                        return;
                    }
                    hazards[hazard_count++] = h->static_haz[j];
                }
            }
            for (int j = 0; j < h->dynamic_count; ++j) {
                if (h->dynamic[j] != NULL && *h->dynamic[j] != NULL) {
                    if (hazard_count == l->pending_size) {
                        resize_pending();
                        nbd_free(hazards);
                        haz_defer_free(d, f);
                        return;
                    }
                    hazards[hazard_count++] = *h->dynamic[j];
                }
            }
        }
        sort_hazards(hazards, hazard_count);

        // check for conflicts
        int  conflicts_count = 0;
        for (int i = 0; i < l->pending_count; ++i) {
            pending_t *p = l->pending + i;
            if (search_hazards(p->ptr, hazards, hazard_count)) {
                l->pending[conflicts_count++] = *p; // put conflicts back on the pending list
            } else {
                assert(p->free_);
                assert(p->ptr);
                p->free_(p->ptr); // free pending item
            }
        }
        l->pending_count = conflicts_count;
        nbd_free(hazards);
    }
    l->pending[ l->pending_count ].ptr  = d;
    l->pending[ l->pending_count ].free_ = f;
    l->pending_count++;
}

haz_t *haz_get_static (int i) {
    if (i >= STATIC_HAZ_PER_THREAD)
        return NULL;
    LOCALIZE_THREAD_LOCAL(tid_, int);
    return &haz_local_[tid_].static_haz[i];
}

void haz_register_dynamic (haz_t *haz) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    haz_local_t *l = haz_local_ + tid_;

    if (l->dynamic_size == 0) {
        l->dynamic_size = MAX_NUM_THREADS * STATIC_HAZ_PER_THREAD;
        l->dynamic = nbd_malloc(sizeof(haz_t *) * l->dynamic_size);
    }

    if (l->dynamic_count == l->dynamic_size) {
        haz_t **d = nbd_malloc(sizeof(haz_t *) * l->dynamic_size * 2);
        memcpy(d, l->dynamic, l->dynamic_size);
        nbd_free(l->dynamic);
        l->dynamic = d;
        l->dynamic_size *= 2;
    }

    l->dynamic[ l->dynamic_count++ ] = haz;
}

// assumes <haz> was registered in the same thread
void haz_unregister_dynamic (void **haz) {
    LOCALIZE_THREAD_LOCAL(tid_, int);
    haz_local_t *l = haz_local_ + tid_;

    for (int i = 0; i < l->dynamic_count; ++i) {
        if (l->dynamic[i] == haz) {
            if (i != l->dynamic_count - 1) {
                l->dynamic[i] = l->dynamic[ l->dynamic_count ];
            }
            l->dynamic_count--;
            return;
        }
    }
    assert(0);
}
