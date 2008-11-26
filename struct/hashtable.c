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

#include "common.h"
#include "murmur.h"
#include "mem.h"
#include "struct.h"
#include "nstring.h"

#define GET_PTR(x) ((nstring_t *)((x) & MASK(48))) // low-order 48 bits is a pointer to a nstring_t

typedef struct ht_entry {
    uint64_t key; // ptr to nstring_t
    uint64_t value;
} entry_t;

typedef struct hti {
    volatile entry_t *table;
    hashtable_t *ht; // parent ht;
    struct hti *next;
    struct hti *next_free;
    unsigned int scale;
    int max_probe;
    int count; // TODO: make these counters distributed
    int num_entries_copied;
    int scan;
} hashtable_i_t;

static const uint64_t COPIED_VALUE           = -1;
static const uint64_t TOMBSTONE              = STRIP_TAG(-1);

static const unsigned ENTRIES_PER_BUCKET     = CACHE_LINE_SIZE/sizeof(entry_t);
static const unsigned ENTRIES_PER_COPY_CHUNK = CACHE_LINE_SIZE/sizeof(entry_t)*2;
static const unsigned MIN_SCALE              = 4; // min 16 entries (4 buckets)
static const unsigned MAX_BUCKETS_TO_PROBE   = 250;

static int hti_copy_entry (hashtable_i_t *ht1, volatile entry_t *e, uint32_t e_key_hash, hashtable_i_t *ht2);

// Choose the next bucket to probe using the high-order bits of <key_hash>.
static inline int get_next_ndx(int old_ndx, uint32_t key_hash, int ht_scale) {
    int incr = (key_hash >> (32 - ht_scale));
    incr += !incr; // If the increment is 0, make it 1.
    return (old_ndx + incr) & MASK(ht_scale);
}

// Compare two keys.
//
// A key is made up of two parts. The 48 low-order bits are a pointer to a null terminated string.
// The high-order 16 bits are taken from the hash of that string. The bits from the hash are used 
// as a quick check to rule out non-equal keys without doing a complete string compare.
static inline int ht_key_equals (uint64_t a, uint32_t b_hash, const char *b_value, uint32_t b_len) {
    if ((b_hash >> 16) != (a >> 48)) // high-order 16 bits are from the hash value
        return FALSE;
    return ns_cmp_raw(GET_PTR(a), b_value, b_len) == 0;
}

// Lookup <key> in <hti>. 
//
// Return the entry that <key> is in, or if <key> isn't in <hti> return the entry that it would be 
// in if it were inserted into <hti>. If there is no room for <key> in <hti> then return NULL, to 
// indicate that the caller should look in <hti->next>.
//
// Record if the entry being returned is empty. Otherwise the caller will have to waste time with
// ht_key_equals() to confirm that it did not lose a race to fill an empty entry.
static volatile entry_t *hti_lookup (hashtable_i_t *hti, uint32_t key_hash, const char *key_data, uint32_t key_len, int *is_empty) {
    TRACE("h2", "hti_lookup(key %p in hti %p)", key_data, hti);
    *is_empty = 0;

    // Probe one cache line at a time
    int ndx = key_hash & MASK(hti->scale); // the first entry to search
    for (int i = 0; i < hti->max_probe; ++i) {

        // The start of the bucket is the first entry in the cache line.
        volatile entry_t *bucket = hti->table + (ndx & ~(ENTRIES_PER_BUCKET-1)); 

        // Start searching at the indexed entry. Then loop around to the begining of the cache line.
        for (int j = 0; j < ENTRIES_PER_BUCKET; ++j) {
            volatile entry_t *e = bucket + ((ndx + j) & (ENTRIES_PER_BUCKET-1));

            uint64_t e_key = e->key;
            if (e_key == DOES_NOT_EXIST) {
                TRACE("h1", "hti_lookup: entry %p for key \"%s\" is empty", e, ns_data(GET_PTR(e_key)));
                *is_empty = 1; // indicate an empty so the caller avoids an expensive ht_key_equals
                return e;
            }

            if (ht_key_equals(e_key, key_hash, key_data, key_len)) {
                TRACE("h1", "hti_lookup: entry %p key \"%s\"", e, ns_data(GET_PTR(e_key)));
                TRACE("h2", "hti_lookup: entry key len %llu, value %p", ns_len(GET_PTR(e_key)), e->value);
                return e;
            }
        }

        ndx = get_next_ndx(ndx, key_hash, hti->scale);
    }

    // maximum number of probes exceeded
    TRACE("h1", "hti_lookup: maximum number of probes exceeded returning 0x0", 0, 0);
    return NULL;
}

// Allocate and initialize a hashtable_i_t with 2^<scale> entries.
static hashtable_i_t *hti_alloc (hashtable_t *parent, int scale) {
    // Include enough slop to align the actual table on a cache line boundry
    size_t n = sizeof(hashtable_i_t) 
             + sizeof(entry_t) * (1 << scale) 
             + (CACHE_LINE_SIZE - 1);
    hashtable_i_t *hti = (hashtable_i_t *)calloc(n, 1);

    // Align the table of hash entries on a cache line boundry.
    hti->table = (entry_t *)(((uint64_t)hti + sizeof(hashtable_i_t) + (CACHE_LINE_SIZE-1)) 
                            & ~(CACHE_LINE_SIZE-1));

    hti->scale = scale;

    // When searching for a key probe a maximum of 1/4 of the buckets up to 1000 buckets.
    hti->max_probe = ((1 << (hti->scale - 2)) / ENTRIES_PER_BUCKET) + 2;
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
// Initiates a copy by creating a larger hashtable_i_t and installing it in <hti->next>.
static void hti_start_copy (hashtable_i_t *hti) {
    TRACE("h0", "hti_start_copy(hti %p scale %llu)", hti, hti->scale);

    // heuristics to determine the size of the new table
    uint64_t count = ht_count(hti->ht);
    unsigned int new_scale = hti->scale;
    new_scale += (count > (1 << (new_scale - 2))); // double size if more than 1/4 full
    new_scale += (count > (1 << (new_scale - 2))); // double size again if more than 1/2 full

    // Allocate the new table and attempt to install it.
    hashtable_i_t *next = hti_alloc(hti->ht, new_scale);
    hashtable_i_t *old_next = SYNC_CAS(&hti->next, NULL, next);
    if (old_next != NULL) {
        // Another thread beat us to it.
        TRACE("h0", "hti_start_copy: lost race to install new hti; found %p", old_next, 0);
        nbd_free(next);
        return;
    }
    TRACE("h0", "hti_start_copy: new hti %p scale %llu", next, next->scale);
}

// Copy the key and value stored in <ht1_e> (which must be an entry in <ht1>) to <ht2>. 
//
// Return 1 unless <ht1_e> is already copied (then return 0), so the caller can account for the total
// number of entries left to copy.
static int hti_copy_entry (hashtable_i_t *ht1, volatile entry_t *ht1_e, uint32_t key_hash, 
                           hashtable_i_t *ht2) {
    TRACE("h2", "hti_copy_entry: entry %p to table %p", ht1_e, ht2);
    assert(ht1);
    assert(ht1->next);
    assert(ht2);
    assert(ht1_e >= ht1->table && ht1_e < ht1->table + (1 << ht1->scale));
    assert(key_hash == 0 || (key_hash >> 16) == (ht1_e->key >> 48));

    uint64_t ht1_e_value = ht1_e->value;
    if (EXPECT_FALSE(ht1_e_value == COPIED_VALUE)) {
        TRACE("h1", "hti_copy_entry: entry %p already copied to table %p", ht1_e, ht2);
        return FALSE; // already copied
    }

    // Kill empty entries.
    if (EXPECT_FALSE(ht1_e_value == DOES_NOT_EXIST)) {
        uint64_t ht1_e_value = SYNC_CAS(&ht1_e->value, DOES_NOT_EXIST, COPIED_VALUE);
        if (ht1_e_value == DOES_NOT_EXIST) {
            TRACE("h1", "hti_copy_entry: empty entry %p killed", ht1_e, 0);
            return TRUE;
        }
        if (ht1_e_value == COPIED_VALUE) {
            TRACE("h0", "hti_copy_entry: lost race to kill empty entry %p", ht1_e, 0);
            return FALSE; // another thread beat us to it
        }
        TRACE("h0", "hti_copy_entry: lost race to kill empty entry %p; the entry is now"
                    "in use and should be copied", ht1_e, 0);
    }

    // Tag the value in the old entry to indicate a copy is in progress.
    ht1_e_value = SYNC_FETCH_AND_OR(&ht1_e->value, TAG_VALUE(0));
    TRACE("h2", "hti_copy_entry: tagged the value %p in old entry %p", ht1_e_value, ht1_e);
    if (ht1_e_value == COPIED_VALUE) {
        TRACE("h1", "hti_copy_entry: entry %p already copied to table %p", ht1_e, ht2);
        return FALSE; // <value> was already copied by another thread.
    }

    // The old table's deleted entries don't need to be copied to the new table, but their keys need
    // to be freed.
    assert(COPIED_VALUE == TAG_VALUE(TOMBSTONE));
    if (ht1_e_value == TOMBSTONE) {
        TRACE("h1", "hti_copy_entry: entry %p old value was deleted, now freeing key %p", ht1_e, GET_PTR(ht1_e->key));
        nbd_defer_free(GET_PTR(ht1_e->key));
        return TRUE; 
    }

    // Install the key in the new table.
    uint64_t key = ht1_e->key;
    nstring_t *key_string = GET_PTR(key);
    uint64_t value = STRIP_TAG(ht1_e_value);

    // We use 0 to indicate that <key_hash> isn't initiallized. Occasionally the <key_hash> will
    // really be 0 and we will waste time recomputing it. That is rare enough that it is OK. 
    if (key_hash == 0) { 
        key_hash = murmur32(ns_data(key_string), ns_len(key_string));
    }

    int is_empty;
    volatile entry_t *ht2_e = hti_lookup(ht2, key_hash, ns_data(key_string), ns_len(key_string), &is_empty);
    TRACE("h0", "hti_copy_entry: copy entry %p to entry %p", ht1_e, ht2_e);

    // it is possible that there is not any room in the new table either
    if (EXPECT_FALSE(ht2_e == NULL)) {
        TRACE("h0", "hti_copy_entry: no room in table %p copy to next table %p", ht2, ht2->next);
        if (ht2->next == NULL) {
            hti_start_copy(ht2); // initiate nested copy, if not already started
        }
        return hti_copy_entry(ht1, ht1_e, key_hash, ht2->next); // recursive tail-call
    }

    // a tagged entry returned from hti_lookup() means it is either empty or has a new key
    if (is_empty) {
        uint64_t old_ht2_e_key = SYNC_CAS(&ht2_e->key, DOES_NOT_EXIST, key);
        if (old_ht2_e_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_copy_entry: lost race to CAS key %p into new entry; found %p",
                    key, old_ht2_e_key);
            return hti_copy_entry(ht1, ht1_e, key_hash, ht2); // recursive tail-call
        }
    }

    // Copy the value to the entry in the new table.
    uint64_t old_ht2_e_value = SYNC_CAS(&ht2_e->value, DOES_NOT_EXIST, value);

    // If there is a nested copy in progress, we might have installed the key into a dead entry.
    if (old_ht2_e_value == COPIED_VALUE) {
        TRACE("h0", "hti_copy_entry: nested copy in progress; copy %p to next table %p", ht2_e, ht2->next);
        return hti_copy_entry(ht1, ht1_e, key_hash, ht2->next); // recursive tail-call
    }

    // Mark the old entry as dead.
    ht1_e->value = COPIED_VALUE;

    // Update the count if we were the one that completed the copy.
    if (old_ht2_e_value == DOES_NOT_EXIST) {
        TRACE("h0", "hti_copy_entry: key \"%s\" value %p copied to new entry", ns_data(key_string), value);
        SYNC_ADD(&ht1->count, -1);
        SYNC_ADD(&ht2->count, 1);
        return TRUE;
    }

    TRACE("h0", "hti_copy_entry: lost race to install value %p in new entry; found value %p", 
                value, old_ht2_e_value);
    return FALSE; // another thread completed the copy
}

// Compare <expected> with the existing value associated with <key>. If the values match then 
// replace the existing value with <new>. If <new> is TOMBSTONE, delete the value associated with 
// the key by replacing it with a TOMBSTONE.
//
// Return the previous value associated with <key>, or DOES_NOT_EXIST if <key> is not in the table
// or associated with a TOMBSTONE. If a copy is in progress and <key> has been copied to the next 
// table then return COPIED_VALUE. 
//
// NOTE: the returned value matches <expected> iff the set succeeds
//
// Certain values of <expected> have special meaning. If <expected> is EXPECT_EXISTS then any 
// real value matches (i.e. not a TOMBSTONE or DOES_NOT_EXIST) as long as <key> is in the table. If
// <expected> is EXPECT_WHATEVER then skip the test entirely.
//
static uint64_t hti_compare_and_set (hashtable_i_t *hti, uint32_t key_hash, const char *key_data, 
                                    uint32_t key_len, uint64_t expected, uint64_t new) {
    TRACE("h1", "hti_compare_and_set: hti %p key %p", hti, key_data);
    TRACE("h1", "hti_compare_and_set: value %p expect %p", new, expected);
    assert(hti);
    assert(new != DOES_NOT_EXIST && !IS_TAGGED(new));
    assert(key_data);

    int is_empty;
    volatile entry_t *e = hti_lookup(hti, key_hash, key_data, key_len, &is_empty);

    // There is no room for <key>, grow the table and try again.
    if (e == NULL) {
        if (hti->next == NULL) {
            hti_start_copy(hti);
        }
        return COPIED_VALUE;
    }

    // Install <key> in the table if it doesn't exist.
    if (is_empty) {
        TRACE("h0", "hti_compare_and_set: entry %p is empty", e, 0);
        if (expected != EXPECT_WHATEVER && expected != EXPECT_DOES_NOT_EXIST)
            return DOES_NOT_EXIST;

        // No need to do anything, <key> is already deleted.
        if (new == TOMBSTONE)
            return DOES_NOT_EXIST;

        // Allocate <key>.
        nstring_t *key = ns_alloc(key_data, key_len);

        // Combine <key> pointer with bits from its hash, CAS it into the table. 
        uint64_t temp = ((uint64_t)(key_hash >> 16) << 48) | (uint64_t)key; 
        uint64_t e_key = SYNC_CAS(&e->key, DOES_NOT_EXIST, temp);

        // Retry if another thread stole the entry out from under us.
        if (e_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_compare_and_set: lost race to install key %p in entry %p", key, e);
            TRACE("h0", "hti_compare_and_set: found %p instead of NULL", GET_PTR(e_key), 0);
            nbd_free(key);
            return hti_compare_and_set(hti, key_hash, key_data, key_len, expected, new); // tail-call
        }
        TRACE("h2", "hti_compare_and_set: installed key %p in entry %p", key, e);
    }

    TRACE("h0", "hti_compare_and_set: entry for key \"%s\" is %p", ns_data(GET_PTR(e->key)), e);

    // If the entry is in the middle of a copy, the copy must be completed first.
    uint64_t e_value = e->value;
    if (EXPECT_FALSE(IS_TAGGED(e_value))) {
        if (e_value != COPIED_VALUE) {
            int did_copy = hti_copy_entry(hti, e, key_hash, ((volatile hashtable_i_t *)hti)->next);
            if (did_copy) {
                SYNC_ADD(&hti->num_entries_copied, 1);
            }
            TRACE("h0", "hti_compare_and_set: value in the middle of a copy, copy completed by %s", 
                        (did_copy ? "self" : "other"), 0);
        }
        TRACE("h0", "hti_compare_and_set: value copied to next table, retry on next table", 0, 0);
        return COPIED_VALUE;
    }

    // Fail if the old value is not consistent with the caller's expectation.
    int old_existed = (e_value != TOMBSTONE && e_value != DOES_NOT_EXIST);
    if (EXPECT_FALSE(expected != EXPECT_WHATEVER && expected != e_value)) {
        if (EXPECT_FALSE(expected != (old_existed ? EXPECT_EXISTS : EXPECT_DOES_NOT_EXIST))) {
            TRACE("h1", "hti_compare_and_set: value %p expected by caller not found; found value %p",
                        expected, e_value);
            return e_value;
        }
    }

    // No need to update if value is unchanged.
    if ((new == TOMBSTONE && !old_existed) || e_value == new) {
        TRACE("h1", "hti_compare_and_set: old value and new value were the same", 0, 0);
        return e_value;
    }

    // CAS the value into the entry. Retry if it fails.
    uint64_t v = SYNC_CAS(&e->value, e_value, new);
    if (EXPECT_FALSE(v != e_value)) {
        TRACE("h0", "hti_compare_and_set: value CAS failed; expected %p found %p", e_value, v);
        return hti_compare_and_set(hti, key_hash, key_data, key_len, expected, new); // recursive tail-call
    }

    // The set succeeded. Adjust the value count.
    if (old_existed && new == TOMBSTONE) {
        SYNC_ADD(&hti->count, -1);
    } else if (!old_existed && new != TOMBSTONE) {
        SYNC_ADD(&hti->count, 1);
    }

    // Return the previous value.
    TRACE("h0", "hti_compare_and_set: CAS succeeded; old value %p new value %p", e_value, new);
    return e_value;
}

//
static uint64_t hti_get (hashtable_i_t *hti, uint32_t key_hash, const char *key_data, uint32_t key_len) {
    assert(key_data);

    int is_empty;
    volatile entry_t *e = hti_lookup(hti, key_hash, key_data, key_len, &is_empty);

    // When hti_lookup() returns NULL it means we hit the reprobe limit while
    // searching the table. In that case, if a copy is in progress the key 
    // might exist in the copy.
    if (EXPECT_FALSE(e == NULL)) {
        if (((volatile hashtable_i_t *)hti)->next != NULL)
            return hti_get(hti->next, key_hash, key_data, key_len); // recursive tail-call
        return DOES_NOT_EXIST;
    }

    if (is_empty)
        return DOES_NOT_EXIST;

    // If the entry is being copied, finish the copy and retry on the next table.
    uint64_t e_value = e->value;
    if (EXPECT_FALSE(IS_TAGGED(e_value))) {
        if (EXPECT_FALSE(e_value != COPIED_VALUE)) {
            int did_copy = hti_copy_entry(hti, e, key_hash, ((volatile hashtable_i_t *)hti)->next);
            if (did_copy) {
                SYNC_ADD(&hti->num_entries_copied, 1);
            }
        }
        return hti_get(((volatile hashtable_i_t *)hti)->next, key_hash, key_data, key_len); // tail-call
    }

    return (e_value == TOMBSTONE) ? DOES_NOT_EXIST : e_value;
}

//
uint64_t ht_get (hashtable_t *ht, const char *key_data, uint32_t key_len) {
    return hti_get(*ht, murmur32(key_data, key_len), key_data, key_len);
}

//
uint64_t ht_compare_and_set (hashtable_t *ht, const char *key_data, uint32_t key_len, 
                            uint64_t expected_val, uint64_t new_val) {

    TRACE("h2", "ht_compare_and_set: key %p len %u", key_data, key_len);
    TRACE("h2", "ht_compare_and_set: expected val %p new val %p", expected_val, new_val);
    assert(key_data);
    assert(!IS_TAGGED(new_val) && new_val != DOES_NOT_EXIST);

    hashtable_i_t *hti = *ht;

    // Help with an ongoing copy.
    if (EXPECT_FALSE(hti->next != NULL)) {
        volatile entry_t *e;
        uint64_t limit; 
        int num_copied = 0;
        int x = hti->scan; 

        TRACE("h1", "ht_compare_and_set: help copy. scan is %llu, size is %llu", x, 1<<hti->scale);
        // Panic if we've been around the array twice and still haven't finished the copy.
        int panic = (x >= (1 << (hti->scale + 1))); 
        if (!panic) {
            limit = ENTRIES_PER_COPY_CHUNK;

            // Reserve some entries for this thread to copy. There is a race condition here because the
            // fetch and add isn't atomic, but that is ok.
            hti->scan = x + ENTRIES_PER_COPY_CHUNK; 

            // <hti->scan> might be larger than the size of the table, if some thread stalls while 
            // copying. In that case we just wrap around to the begining and make another pass through
            // the table.
            e = hti->table + (x & MASK(hti->scale));
        } else {
            TRACE("h1", "ht_compare_and_set: help copy panic", 0, 0);
            // scan the whole table
            limit = (1 << hti->scale);
            e = hti->table;
        }

        // Copy the entries
        for (int i = 0; i < limit; ++i) {
            num_copied += hti_copy_entry(hti, e++, 0, hti->next);
            assert(e <= hti->table + (1 << hti->scale));
        }
        if (num_copied != 0) {
            SYNC_ADD(&hti->num_entries_copied, num_copied);
        }

        // Dispose of fully copied tables.
        if (hti->num_entries_copied == (1 << hti->scale) || panic) {
            assert(hti->next);
            if (SYNC_CAS(ht, hti, hti->next) == hti) {
                nbd_defer_free(hti); 
            }
        }
    }

    uint64_t old_val;
    uint32_t key_hash = murmur32(key_data, key_len);
    while ((old_val = hti_compare_and_set(hti, key_hash, key_data, key_len, expected_val, new_val)) 
           == COPIED_VALUE) {
        assert(hti->next);
        hti = hti->next;
    }

    return old_val == TOMBSTONE ? DOES_NOT_EXIST : old_val;
}

// Remove the value in <ht> associated with <key_data>. Returns the value removed, or 
// DOES_NOT_EXIST if there was no value for that key.
uint64_t ht_remove (hashtable_t *ht, const char *key_data, uint32_t key_len) {
    hashtable_i_t *hti = *ht;
    uint64_t val;
    uint32_t key_hash = murmur32(key_data, key_len);
    do {
        val = hti_compare_and_set(hti, key_hash, key_data, key_len, EXPECT_WHATEVER, TOMBSTONE);
        if (val != COPIED_VALUE)
            return val == TOMBSTONE ? DOES_NOT_EXIST : val;
        assert(hti->next);
        hti = hti->next;
        assert(hti);
    } while (1);
}

// Returns the number of key-values pairs in <ht>
uint64_t ht_count (hashtable_t *ht) {
    hashtable_i_t *hti = *ht;
    uint64_t count = 0;
    while (hti) {
        count += hti->count;
        hti = hti->next; 
    }
    return count;
}

// Allocate and initialize a new hash table.
hashtable_t *ht_alloc (void) {
    hashtable_t *ht = nbd_malloc(sizeof(hashtable_t));
    *ht = (hashtable_i_t *)hti_alloc(ht, MIN_SCALE);
    return ht;
}

// Free <ht> and its internal structures.
void ht_free (hashtable_t *ht) {
    hashtable_i_t *hti = *ht;
    do {
        for (uint32_t i = 0; i < (1 << hti->scale); ++i) {
            assert(hti->table[i].value == COPIED_VALUE || !IS_TAGGED(hti->table[i].value));
            if (hti->table[i].key != DOES_NOT_EXIST) {
                nbd_free(GET_PTR(hti->table[i].key));
            }
        }
        hashtable_i_t *next = hti->next;
        nbd_free(hti);
        hti = next;
    } while (hti);
    nbd_free(ht);
}
