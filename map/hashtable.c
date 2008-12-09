/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * 
 * C implementation of Cliff Click's lock-free hash table from 
 * http://www.azulsystems.com/events/javaone_2008/2008_CodingNonBlock.pdf
 * http://sourceforge.net/projects/high-scale-lib
 *
 * Note: This is code uses synchronous atomic operations because that is all that x86 provides. 
 * Every atomic operation is also an implicit full memory barrier. The upshot is that it simplifies
 * the code a bit, but it won't be as fast as it could be on platforms like SPARC that provide 
 * weaker operations which would still do the job.
 */

#include <stdio.h>
#include "common.h"
#include "murmur.h"
#include "mem.h"
#include "hashtable.h"

#define GET_PTR(x) ((void *)((x) & MASK(48))) // low-order 48 bits is a pointer to a nstring_t

typedef struct entry {
    uint64_t key;
    uint64_t val;
} entry_t;

typedef struct hti {
    volatile entry_t *table;
    hashtable_t *ht; // parent ht;
    struct hti *next;
    unsigned scale;
    int max_probe;
    int references;
    int count; // TODO: make these counters distributed
    int num_entries_copied;
    int copy_scan;
} hti_t;

struct ht_iter {
    hti_t *  hti;
    int64_t  idx;
    uint64_t key;
    uint64_t val;
};

struct ht {
    hti_t *hti;
    const datatype_t *key_type;
};

static const uint64_t COPIED_VALUE           = -1;
static const uint64_t TOMBSTONE              = STRIP_TAG(-1, TAG1);

static const unsigned ENTRIES_PER_BUCKET     = CACHE_LINE_SIZE/sizeof(entry_t);
static const unsigned ENTRIES_PER_COPY_CHUNK = CACHE_LINE_SIZE/sizeof(entry_t)*2;
static const unsigned MIN_SCALE              = 4; // min 16 entries (4 buckets)
static const unsigned MAX_BUCKETS_TO_PROBE   = 250;

static int hti_copy_entry (hti_t *ht1, volatile entry_t *ent, uint32_t ent_key_hash, hti_t *ht2);

// Choose the next bucket to probe using the high-order bits of <key_hash>.
static inline int get_next_ndx(int old_ndx, uint32_t key_hash, int ht_scale) {
    int incr = (key_hash >> (32 - ht_scale));
    incr += !incr; // If the increment is 0, make it 1.
    return (old_ndx + incr) & MASK(ht_scale);
}

// Lookup <key> in <hti>. 
//
// Return the entry that <key> is in, or if <key> isn't in <hti> return the entry that it would be 
// in if it were inserted into <hti>. If there is no room for <key> in <hti> then return NULL, to 
// indicate that the caller should look in <hti->next>.
//
// Record if the entry being returned is empty. Otherwise the caller will have to waste time 
// re-comparing the keys to confirm that it did not lose a race to fill an empty entry.
static volatile entry_t *hti_lookup (hti_t *hti, void *key, uint32_t key_hash, int *is_empty) {
    TRACE("h2", "hti_lookup(key %p in hti %p)", key, hti);
    *is_empty = 0;

    // Probe one cache line at a time
    int ndx = key_hash & MASK(hti->scale); // the first entry to search
    for (int i = 0; i < hti->max_probe; ++i) {

        // The start of the bucket is the first entry in the cache line.
        volatile entry_t *bucket = hti->table + (ndx & ~(ENTRIES_PER_BUCKET-1)); 

        // Start searching at the indexed entry. Then loop around to the begining of the cache line.
        for (int j = 0; j < ENTRIES_PER_BUCKET; ++j) {
            volatile entry_t *ent = bucket + ((ndx + j) & (ENTRIES_PER_BUCKET-1));

            uint64_t ent_key = ent->key;
            if (ent_key == DOES_NOT_EXIST) {
                TRACE("h1", "hti_lookup: entry %p for key %p is empty", ent, 
                            (hti->ht->key_type == NULL) ? (void *)ent_key : GET_PTR(ent_key));
                *is_empty = 1; // indicate an empty so the caller avoids an expensive key compare
                return ent;
            }

            // Compare <key> with the key in the entry. 
            if (EXPECT_TRUE(hti->ht->key_type == NULL)) {
                // fast path for integer keys
                if (ent_key == (uint64_t)key) {
                    TRACE("h1", "hti_lookup: found entry %p with key %p", ent, ent_key);
                    return ent;
                }
            } else {
                // The key in <ent> is made up of two parts. The 48 low-order bits are a pointer. The
                // high-order 16 bits are taken from the hash. The bits from the hash are used as a
                // quick check to rule out non-equal keys without doing a complete compare.
                if ((key_hash >> 16) == (ent_key >> 48) && hti->ht->key_type->cmp(GET_PTR(ent_key), key) == 0) {
                    TRACE("h1", "hti_lookup: found entry %p with key %p", ent, GET_PTR(ent_key));
                    return ent;
                }
            }
        }

        ndx = get_next_ndx(ndx, key_hash, hti->scale);
    }

    // maximum number of probes exceeded
    TRACE("h1", "hti_lookup: maximum number of probes exceeded returning 0x0", 0, 0);
    return NULL;
}

// Allocate and initialize a hti_t with 2^<scale> entries.
static hti_t *hti_alloc (hashtable_t *parent, int scale) {
    hti_t *hti = (hti_t *)nbd_malloc(sizeof(hti_t));
    memset(hti, 0, sizeof(hti_t));

    size_t sz = sizeof(entry_t) * (1 << scale);
    entry_t *table = nbd_malloc(sz);
    memset(table, 0, sz);
    hti->table = table;

    hti->scale = scale;

    // When searching for a key probe a maximum of 1/4 of the buckets up to 1000 buckets.
    hti->max_probe = ((1 << (hti->scale - 2)) / ENTRIES_PER_BUCKET) + 4;
    if (hti->max_probe > MAX_BUCKETS_TO_PROBE) {
        hti->max_probe = MAX_BUCKETS_TO_PROBE;
    }

    hti->ht = parent;

    assert(hti->scale >= MIN_SCALE && hti->scale < 63); // size must be a power of 2
    assert(sizeof(entry_t) * ENTRIES_PER_BUCKET % CACHE_LINE_SIZE == 0); // divisible into cache
    assert((size_t)hti->table % CACHE_LINE_SIZE == 0); // cache aligned

    return hti;
}

// Called when <hti> runs out of room for new keys.
//
// Initiates a copy by creating a larger hti_t and installing it in <hti->next>.
static void hti_start_copy (hti_t *hti) {
    TRACE("h0", "hti_start_copy(hti %p scale %llu)", hti, hti->scale);

    // heuristics to determine the size of the new table
    uint64_t count = ht_count(hti->ht);
    unsigned int new_scale = hti->scale;
    new_scale += (count > (1 << (new_scale - 2))); // double size if more than 1/4 full
    new_scale += (count > (1 << (new_scale - 2))); // double size again if more than 1/2 full

    // Allocate the new table and attempt to install it.
    hti_t *next = hti_alloc(hti->ht, new_scale);
    hti_t *old_next = SYNC_CAS(&hti->next, NULL, next);
    if (old_next != NULL) {
        // Another thread beat us to it.
        TRACE("h0", "hti_start_copy: lost race to install new hti; found %p", old_next, 0);
        nbd_free(next);
        return;
    }
    TRACE("h0", "hti_start_copy: new hti %p scale %llu", next, next->scale);
}

// Copy the key and value stored in <ht1_ent> (which must be an entry in <ht1>) to <ht2>. 
//
// Return 1 unless <ht1_ent> is already copied (then return 0), so the caller can account for the total
// number of entries left to copy.
static int hti_copy_entry (hti_t *ht1, volatile entry_t *ht1_ent, uint32_t key_hash, hti_t *ht2) {
    TRACE("h2", "hti_copy_entry: entry %p to table %p", ht1_ent, ht2);
    assert(ht1);
    assert(ht1->next);
    assert(ht2);
    assert(ht1_ent >= ht1->table && ht1_ent < ht1->table + (1 << ht1->scale));
    assert(key_hash == 0 || ht1->ht->key_type == NULL || (key_hash >> 16) == (ht1_ent->key >> 48));

    uint64_t ht1_ent_val = ht1_ent->val;
    if (EXPECT_FALSE(ht1_ent_val == COPIED_VALUE)) {
        TRACE("h1", "hti_copy_entry: entry %p already copied to table %p", ht1_ent, ht2);
        return FALSE; // already copied
    }

    // Kill empty entries.
    if (EXPECT_FALSE(ht1_ent_val == DOES_NOT_EXIST)) {
        uint64_t ht1_ent_val = SYNC_CAS(&ht1_ent->val, DOES_NOT_EXIST, COPIED_VALUE);
        if (ht1_ent_val == DOES_NOT_EXIST) {
            TRACE("h1", "hti_copy_entry: empty entry %p killed", ht1_ent, 0);
            return TRUE;
        }
        if (ht1_ent_val == COPIED_VALUE) {
            TRACE("h0", "hti_copy_entry: lost race to kill empty entry %p; the entry is already killed", ht1_ent, 0);
            return FALSE; // another thread beat us to it
        }
        TRACE("h0", "hti_copy_entry: lost race to kill empty entry %p; the entry is not empty", ht1_ent, 0);
    }

    // Tag the value in the old entry to indicate a copy is in progress.
    ht1_ent_val = SYNC_FETCH_AND_OR(&ht1_ent->val, TAG_VALUE(0, TAG1));
    TRACE("h2", "hti_copy_entry: tagged the value %p in old entry %p", ht1_ent_val, ht1_ent);
    if (ht1_ent_val == COPIED_VALUE) {
        TRACE("h1", "hti_copy_entry: entry %p already copied to table %p", ht1_ent, ht2);
        return FALSE; // <value> was already copied by another thread.
    }

    // Install the key in the new table.
    uint64_t ht1_ent_key = ht1_ent->key;
    void *key = (ht1->ht->key_type == NULL) ? (void *)ht1_ent_key : GET_PTR(ht1_ent_key);

    // The old table's dead entries don't need to be copied to the new table, but their keys need to be freed.
    assert(COPIED_VALUE == TAG_VALUE(TOMBSTONE, TAG1));
    if (ht1_ent_val == TOMBSTONE) {
        TRACE("h1", "hti_copy_entry: entry %p old value was deleted, now freeing key %p", ht1_ent, key);
        if (EXPECT_FALSE(ht1->ht->key_type != NULL)) {
            nbd_defer_free(key);
        }
        return TRUE; 
    }

    // We use 0 to indicate that <key_hash> is uninitiallized. Occasionally the key's hash will really be 0 and we
    // waste time recomputing it every time. It is rare enough (1 in 65k) that it won't hurt performance. 
    if (key_hash == 0) { 
        key_hash = (ht1->ht->key_type == NULL) ? murmur32_8b(ht1_ent_key) : ht1->ht->key_type->hash(key);
    }

    int ht2_ent_is_empty;
    volatile entry_t *ht2_ent = hti_lookup(ht2, key, key_hash, &ht2_ent_is_empty);
    TRACE("h0", "hti_copy_entry: copy entry %p to entry %p", ht1_ent, ht2_ent);

    // It is possible that there isn't any room in the new table either.
    if (EXPECT_FALSE(ht2_ent == NULL)) {
        TRACE("h0", "hti_copy_entry: no room in table %p copy to next table %p", ht2, ht2->next);
        if (ht2->next == NULL) {
            hti_start_copy(ht2); // initiate nested copy, if not already started
        }
        return hti_copy_entry(ht1, ht1_ent, key_hash, ht2->next); // recursive tail-call
    }

    if (ht2_ent_is_empty) {
        uint64_t old_ht2_ent_key = SYNC_CAS(&ht2_ent->key, DOES_NOT_EXIST, ht1_ent_key);
        if (old_ht2_ent_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_copy_entry: lost race to CAS key %p into new entry; found %p",
                    ht1_ent_key, old_ht2_ent_key);
            return hti_copy_entry(ht1, ht1_ent, key_hash, ht2); // recursive tail-call
        }
    }

    // Copy the value to the entry in the new table.
    ht1_ent_val = STRIP_TAG(ht1_ent_val, TAG1);
    uint64_t old_ht2_ent_val = SYNC_CAS(&ht2_ent->val, DOES_NOT_EXIST, ht1_ent_val);

    // If there is a nested copy in progress, we might have installed the key into a dead entry.
    if (old_ht2_ent_val == COPIED_VALUE) {
        TRACE("h0", "hti_copy_entry: nested copy in progress; copy %p to next table %p", ht2_ent, ht2->next);
        return hti_copy_entry(ht1, ht1_ent, key_hash, ht2->next); // recursive tail-call
    }

    // Mark the old entry as dead.
    ht1_ent->val = COPIED_VALUE;

    // Update the count if we were the one that completed the copy.
    if (old_ht2_ent_val == DOES_NOT_EXIST) {
        TRACE("h0", "hti_copy_entry: key %p value %p copied to new entry", key, ht1_ent_val);
        SYNC_ADD(&ht1->count, -1);
        SYNC_ADD(&ht2->count, 1);
        return TRUE;
    }

    TRACE("h0", "hti_copy_entry: lost race to install value %p in new entry; found value %p", 
                ht1_ent_val, old_ht2_ent_val);
    return FALSE; // another thread completed the copy
}

// Compare <expected> with the existing value associated with <key>. If the values match then 
// replace the existing value with <new>. If <new> is DOES_NOT_EXIST, delete the value associated with 
// the key by replacing it with a TOMBSTONE.
//
// Return the previous value associated with <key>, or DOES_NOT_EXIST if <key> is not in the table
// or associated with a TOMBSTONE. If a copy is in progress and <key> has been copied to the next 
// table then return COPIED_VALUE. 
//
// NOTE: the returned value matches <expected> iff the set succeeds
//
// Certain values of <expected> have special meaning. If <expected> is CAS_EXPECT_EXISTS then any 
// real value matches (i.ent. not a TOMBSTONE or DOES_NOT_EXIST) as long as <key> is in the table. If
// <expected> is CAS_EXPECT_WHATEVER then skip the test entirely.
//
static uint64_t hti_cas (hti_t *hti, void *key, uint32_t key_hash, uint64_t expected, uint64_t new) {
    TRACE("h1", "hti_cas: hti %p key %p", hti, key);
    TRACE("h1", "hti_cas: value %p expect %p", new, expected);
    assert(hti);
    assert(!IS_TAGGED(new, TAG1));
    assert(key);

    int is_empty;
    volatile entry_t *ent = hti_lookup(hti, key, key_hash, &is_empty);

    // There is no room for <key>, grow the table and try again.
    if (ent == NULL) {
        if (hti->next == NULL) {
            hti_start_copy(hti);
        }
        return COPIED_VALUE;
    }

    // Install <key> in the table if it doesn't exist.
    if (is_empty) {
        TRACE("h0", "hti_cas: entry %p is empty", ent, 0);
        if (expected != CAS_EXPECT_WHATEVER && expected != CAS_EXPECT_DOES_NOT_EXIST)
            return DOES_NOT_EXIST;

        // No need to do anything, <key> is already deleted.
        if (new == DOES_NOT_EXIST)
            return DOES_NOT_EXIST;

        // Allocate <new_key>.
        uint64_t new_key = (uint64_t)((hti->ht->key_type == NULL) ? key : hti->ht->key_type->clone(key));
        if (EXPECT_FALSE(hti->ht->key_type != NULL)) {
            // Combine <new_key> pointer with bits from its hash 
            new_key = ((uint64_t)(key_hash >> 16) << 48) | new_key; 
        }

        // CAS the key into the table.
        uint64_t old_ent_key = SYNC_CAS(&ent->key, DOES_NOT_EXIST, new_key);

        // Retry if another thread stole the entry out from under us.
        if (old_ent_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_cas: lost race to install key %p in entry %p", new_key, ent);
            TRACE("h0", "hti_cas: found %p instead of NULL", 
                        (hti->ht->key_type == NULL) ? (void *)old_ent_key : GET_PTR(old_ent_key), 0);
            if (hti->ht->key_type != NULL) {
                nbd_free(GET_PTR(new_key));
            }
            return hti_cas(hti, key, key_hash, expected, new); // tail-call
        }
        TRACE("h2", "hti_cas: installed key %p in entry %p", new_key, ent);
    }

    TRACE("h0", "hti_cas: entry for key %p is %p", 
                (hti->ht->key_type == NULL) ? (void *)ent->key : GET_PTR(ent->key), ent);

    // If the entry is in the middle of a copy, the copy must be completed first.
    uint64_t ent_val = ent->val;
    if (EXPECT_FALSE(IS_TAGGED(ent_val, TAG1))) {
        if (ent_val != COPIED_VALUE) {
            int did_copy = hti_copy_entry(hti, ent, key_hash, ((volatile hti_t *)hti)->next);
            if (did_copy) {
                SYNC_ADD(&hti->num_entries_copied, 1);
            }
            TRACE("h0", "hti_cas: value in the middle of a copy, copy completed by %s", 
                        (did_copy ? "self" : "other"), 0);
        }
        TRACE("h0", "hti_cas: value copied to next table, retry on next table", 0, 0);
        return COPIED_VALUE;
    }

    // Fail if the old value is not consistent with the caller's expectation.
    int old_existed = (ent_val != TOMBSTONE && ent_val != DOES_NOT_EXIST);
    if (EXPECT_FALSE(expected != CAS_EXPECT_WHATEVER && expected != ent_val)) {
        if (EXPECT_FALSE(expected != (old_existed ? CAS_EXPECT_EXISTS : CAS_EXPECT_DOES_NOT_EXIST))) {
            TRACE("h1", "hti_cas: value %p expected by caller not found; found value %p",
                        expected, ent_val);
            return ent_val;
        }
    }

    // No need to update if value is unchanged.
    if ((new == DOES_NOT_EXIST && !old_existed) || ent_val == new) {
        TRACE("h1", "hti_cas: old value and new value were the same", 0, 0);
        return ent_val;
    }

    // CAS the value into the entry. Retry if it fails.
    uint64_t v = SYNC_CAS(&ent->val, ent_val, new == DOES_NOT_EXIST ? TOMBSTONE : new);
    if (EXPECT_FALSE(v != ent_val)) {
        TRACE("h0", "hti_cas: value CAS failed; expected %p found %p", ent_val, v);
        return hti_cas(hti, key, key_hash, expected, new); // recursive tail-call
    }

    // The set succeeded. Adjust the value count.
    if (old_existed && new == DOES_NOT_EXIST) {
        SYNC_ADD(&hti->count, -1);
    } else if (!old_existed && new != DOES_NOT_EXIST) {
        SYNC_ADD(&hti->count, 1);
    }

    // Return the previous value.
    TRACE("h0", "hti_cas: CAS succeeded; old value %p new value %p", ent_val, new);
    return ent_val;
}

//
static uint64_t hti_get (hti_t *hti, void *key, uint32_t key_hash) {
    int is_empty;
    volatile entry_t *ent = hti_lookup(hti, key, key_hash, &is_empty);

    // When hti_lookup() returns NULL it means we hit the reprobe limit while
    // searching the table. In that case, if a copy is in progress the key 
    // might exist in the copy.
    if (EXPECT_FALSE(ent == NULL)) {
        if (((volatile hti_t *)hti)->next != NULL)
            return hti_get(hti->next, key, key_hash); // recursive tail-call
        return DOES_NOT_EXIST;
    }

    if (is_empty)
        return DOES_NOT_EXIST;

    // If the entry is being copied, finish the copy and retry on the next table.
    uint64_t ent_val = ent->val;
    if (EXPECT_FALSE(IS_TAGGED(ent_val, TAG1))) {
        if (EXPECT_FALSE(ent_val != COPIED_VALUE)) {
            int did_copy = hti_copy_entry(hti, ent, key_hash, ((volatile hti_t *)hti)->next);
            if (did_copy) {
                SYNC_ADD(&hti->num_entries_copied, 1);
            }
        }
        return hti_get(((volatile hti_t *)hti)->next, key, key_hash); // tail-call
    }

    return (ent_val == TOMBSTONE) ? DOES_NOT_EXIST : ent_val;
}

//
uint64_t ht_get (hashtable_t *ht, void *key) {
    uint32_t hash = (ht->key_type == NULL) ? murmur32_8b((uint64_t)key) : ht->key_type->hash(key);
    return hti_get(ht->hti, key, hash);
}

// returns TRUE if copy is done
int hti_help_copy (hti_t *hti) {
    volatile entry_t *ent;
    uint64_t limit; 
    uint64_t total_copied = hti->num_entries_copied;
    int num_copied = 0;
    int x = hti->copy_scan; 

    TRACE("h1", "ht_cas: help copy. scan is %llu, size is %llu", x, 1<<hti->scale);
    if (total_copied == (1 << hti->scale)) {
        // Panic if we've been around the array twice and still haven't finished the copy.
        int panic = (x >= (1 << (hti->scale + 1))); 
        if (!panic) {
            limit = ENTRIES_PER_COPY_CHUNK;

            // Reserve some entries for this thread to copy. There is a race condition here because the
            // fetch and add isn't atomic, but that is ok.
            hti->copy_scan = x + ENTRIES_PER_COPY_CHUNK; 

            // <copy_scan> might be larger than the size of the table, if some thread stalls while 
            // copying. In that case we just wrap around to the begining and make another pass through
            // the table.
            ent = hti->table + (x & MASK(hti->scale));
        } else {
            TRACE("h1", "ht_cas: help copy panic", 0, 0);
            // scan the whole table
            limit = (1 << hti->scale);
            ent = hti->table;
        }

        // Copy the entries
        for (int i = 0; i < limit; ++i) {
            num_copied += hti_copy_entry(hti, ent++, 0, hti->next);
            assert(ent <= hti->table + (1 << hti->scale));
        }
        if (num_copied != 0) {
            total_copied = SYNC_ADD(&hti->num_entries_copied, num_copied);
        }
    }

    return (total_copied == (1 << hti->scale));
}

//
uint64_t ht_cas (hashtable_t *ht, void *key, uint64_t expected_val, uint64_t new_val) {

    TRACE("h2", "ht_cas: key %p ht %p", key, ht);
    TRACE("h2", "ht_cas: expected val %p new val %p", expected_val, new_val);
    assert(key != DOES_NOT_EXIST);
    assert(!IS_TAGGED(new_val, TAG1) && new_val != DOES_NOT_EXIST && new_val != TOMBSTONE);

    hti_t *hti = ht->hti;

    // Help with an ongoing copy.
    if (EXPECT_FALSE(hti->next != NULL)) {
        int done = hti_help_copy(hti);

        // Dispose of fully copied tables.
        if (done && hti->references == 0) {

            int r = SYNC_CAS(&hti->references, 0, -1);
            if (r == 0) {
                assert(hti->next);
                if (SYNC_CAS(&ht->hti, hti, hti->next) == hti) {
                    nbd_defer_free((void *)hti->table); 
                    nbd_defer_free(hti); 
                }
            }
        }
    }

    uint64_t old_val;
    uint32_t key_hash = (ht->key_type == NULL) ? murmur32_8b((uint64_t)key) : ht->key_type->hash(key);
    while ((old_val = hti_cas(hti, key, key_hash, expected_val, new_val)) == COPIED_VALUE) {
        assert(hti->next);
        hti = hti->next;
    }

    return old_val == TOMBSTONE ? DOES_NOT_EXIST : old_val;
}

// Remove the value in <ht> associated with <key>. Returns the value removed, or DOES_NOT_EXIST if there was
// no value for that key.
uint64_t ht_remove (hashtable_t *ht, void *key) {
    hti_t *hti = ht->hti;
    uint64_t val;
    uint32_t key_hash = (ht->key_type == NULL) ? murmur32_8b((uint64_t)key) : ht->key_type->hash(key);
    do {
        val = hti_cas(hti, key, key_hash, CAS_EXPECT_WHATEVER, DOES_NOT_EXIST);
        if (val != COPIED_VALUE)
            return val == TOMBSTONE ? DOES_NOT_EXIST : val;
        assert(hti->next);
        hti = hti->next;
        assert(hti);
    } while (1);
}

// Returns the number of key-values pairs in <ht>
uint64_t ht_count (hashtable_t *ht) {
    hti_t *hti = ht->hti;
    uint64_t count = 0;
    while (hti) {
        count += hti->count;
        hti = hti->next; 
    }
    return count;
}

// Allocate and initialize a new hash table.
hashtable_t *ht_alloc (const datatype_t *key_type) {
    hashtable_t *ht = nbd_malloc(sizeof(hashtable_t));
    ht->key_type = key_type;
    ht->hti = (hti_t *)hti_alloc(ht, MIN_SCALE);
    return ht;
}

// Free <ht> and its internal structures.
void ht_free (hashtable_t *ht) {
    hti_t *hti = ht->hti;
    do {
        for (uint32_t i = 0; i < (1 << hti->scale); ++i) {
            assert(hti->table[i].val == COPIED_VALUE || !IS_TAGGED(hti->table[i].val, TAG1));
            if (ht->key_type != NULL && hti->table[i].key != DOES_NOT_EXIST) {
                nbd_free(GET_PTR(hti->table[i].key));
            }
        }
        hti_t *next = hti->next;
        nbd_free((void *)hti->table);
        nbd_free(hti);
        hti = next;
    } while (hti);
    nbd_free(ht);
}

void ht_print (hashtable_t *ht) {
    hti_t *hti = ht->hti;
    while (hti) {
        printf("hti:%p scale:%u count:%d copied:%d\n", hti, hti->scale, hti->count, hti->num_entries_copied);
        for (int i = 0; i < (1 << hti->scale); ++i) {
            volatile entry_t *ent = hti->table + i;
            printf("[0x%x] %p:%p\n", i, (void *)ent->key, (void *)ent->val);
            if (i > 30) {
                printf("...\n");
                break;
            }
        }
        hti = hti->next;
    }
}

ht_iter_t *ht_iter_start (hashtable_t *ht, void *key) {
    hti_t *hti = ht->hti;
    int rcount;
    do {
        while (((volatile hti_t *)hti)->next != NULL) {
            do { } while (hti_help_copy(hti) != TRUE);
            hti = hti->next;
        }

        int old = hti->references;
        do {
            rcount = old;
            if (rcount != -1) {
                old = SYNC_CAS(&hti->references, rcount, rcount + 1);
            }
        } while (rcount != old);
    } while (rcount == -1);

    ht_iter_t *iter = nbd_malloc(sizeof(ht_iter_t));
    iter->hti = hti;
    iter->idx = -1;

    return iter;
}

ht_iter_t *ht_iter_next (ht_iter_t *iter) {
    volatile entry_t *ent;
    uint64_t key;
    uint64_t val;
    uint64_t table_size = (1 << iter->hti->scale);
    do {
        if (++iter->idx == table_size) {
            ht_iter_free(iter);
            return NULL;
        }
        ent = &iter->hti->table[++iter->idx];
        key = ent->key;
        val = ent->val;

    } while (key == DOES_NOT_EXIST || val == DOES_NOT_EXIST || val == TOMBSTONE);

    iter->key = key;
    if (val == COPIED_VALUE) {
        uint32_t hash = (iter->hti->ht->key_type == NULL) 
                      ? murmur32_8b(key)
                      : iter->hti->ht->key_type->hash((void *)key);
        iter->val = hti_get(iter->hti->next, (void *)ent->key, hash);
    } else {
        iter->val = val;
    }

    return iter;
}

uint64_t ht_iter_val (ht_iter_t *iter) {
    return iter->val;
}

uint64_t ht_iter_key (ht_iter_t *iter) {
    return iter->key;
}

void ht_iter_free (ht_iter_t *iter) {
    SYNC_ADD(&iter->hti->references, -1);
}

