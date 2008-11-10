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
#include "ht.h"
#include "murmur.h"
#include "mem.h"
#include "rcu.h"

#define COPIED_VALUE            (-1)
#define TOMBSTONE               STRIP_TAG(COPIED_VALUE)

#define ENTRIES_PER_BUCKET      (CACHE_LINE_SIZE/sizeof(entry_t))
#define ENTRIES_PER_COPY_CHUNK  (ENTRIES_PER_BUCKET * 2)
#define MIN_SCALE               (__builtin_ctz(ENTRIES_PER_BUCKET) + 2) // min 4 buckets
#define MAX_BUCKETS_TO_PROBE    250

typedef struct ht_entry {
    int64_t key;
    int64_t value;
} entry_t;

typedef struct string {
    uint32_t len;
    char val[];
} string_t;

typedef struct hash_table_i {
    volatile entry_t *table;
    hash_table_t *ht; // parent ht;
    struct hash_table_i *next;
    struct hash_table_i *next_free;
    unsigned int scale;
    int max_probe;
    int count; // TODO: make these counters distributed
    int num_entries_copied;
    int scan;
} hash_table_i_t;

static int hti_copy_entry 
    (hash_table_i_t *old_hti, volatile entry_t *e, uint32_t e_key_hash, hash_table_i_t *new_hti);

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
    const string_t *a_key = (string_t *)(a & MASK(48)); // low-order 48 bits is a pointer 
    assert(a_key);
    return a_key->len == b_len && memcmp(a_key->val, b_value, b_len) == 0;
}

// Lookup <key> in <hti>. 
//
// Return the entry that <key> is in, or if <key> isn't in <hti> return the entry that it would be 
// in if it were inserted into <hti>. If there is no room for <key> in <hti> then return NULL, to 
// indicate that the caller should look in <hti->next>.
//
// Record if the entry being returned is empty. Otherwise the caller will have to waste time with
// ht_key_equals() to confirm that it did not lose a race to fill an empty entry.
static volatile entry_t *hti_lookup (hash_table_i_t *hti, uint32_t key_hash, const char *key_val, uint32_t key_len, int *is_empty) {
    TRACE("h0", "hti_lookup(key \"%s\" in hti %p)", key_val, hti);
    *is_empty = 0;

    // Probe one cache line at a time
    int ndx = key_hash & MASK(hti->scale); // the first entry to search
    int i;
    for (i = 0; i < hti->max_probe; ++i) {

        // The start of the bucket is the first entry in the cache line.
        volatile entry_t *bucket = hti->table + (ndx & ~(ENTRIES_PER_BUCKET-1)); 

        // Start searching at the indexed entry. Then loop around to the begining of the cache line.
        int j;
        for (j = 0; j < ENTRIES_PER_BUCKET; ++j) {
            volatile entry_t *e = bucket + ((ndx + j) & (ENTRIES_PER_BUCKET-1));

            uint64_t e_key = e->key;
            if (e_key == DOES_NOT_EXIST) {
                TRACE("h0", "hti_lookup: empty entry %p found on probe %d", e, i*ENTRIES_PER_BUCKET+j+1);
                // we tag the pointer so the caller can avoid an expensive ht_key_equals()
                *is_empty = 1;
                return e;
            }

            if (ht_key_equals(e_key, key_hash, key_val, key_len)) {
                TRACE("h0", "hti_lookup: entry %p found on probe %d", e, i*ENTRIES_PER_BUCKET+j+1);
                TRACE("h0", "hti_lookup: with key \"%s\" value %p", 
                            ((string_t *)(e_key & MASK(48)))->val, e->value);
                return e;
            }
        }

        ndx = get_next_ndx(ndx, key_hash, hti->scale);
    }

    // maximum number of probes exceeded
    TRACE("h0", "hti_lookup: maximum number of probes exceeded returning 0x0", 0, 0);
    return NULL;
}

// Allocate and initialize a hash_table_i_t with 2^<scale> entries.
static hash_table_i_t *hti_alloc (hash_table_t *parent, int scale) {
    // Include enough slop to align the actual table on a cache line boundry
    size_t n = sizeof(hash_table_i_t) 
             + sizeof(entry_t) * (1 << scale) 
             + (CACHE_LINE_SIZE - 1);
    hash_table_i_t *hti = (hash_table_i_t *)calloc(n, 1);

    // Align the table of hash entries on a cache line boundry.
    hti->table = (entry_t *)(((uint64_t)hti + sizeof(hash_table_i_t) + (CACHE_LINE_SIZE-1)) 
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
// Initiates a copy by creating a larger hash_table_i_t and installing it in <hti->next>.
static void hti_start_copy (hash_table_i_t *hti) {
    TRACE("h0", "hti_start_copy(hti %p hti->next %p)", hti, hti->next);
    if (hti->next) 
        return; // another thread beat us to it

    // heuristics to determine the size of the new table
    uint64_t count = ht_count(hti->ht);
    unsigned int new_scale = hti->scale;
    new_scale += (count > (1 << (new_scale - 2))); // double size if more than 1/4 full
    new_scale += (count > (1 << (new_scale - 2))); // double size again if more than 1/2 full

    // Allocate the new table and attempt to install it.
    hash_table_i_t *next = hti_alloc(hti->ht, hti->scale + 1);
    hash_table_i_t *old_next = SYNC_CAS(&hti->next, NULL, next);
    if (old_next != NULL) {
        TRACE("h0", "hti_start_copy: lost race to install new hti; found %p", old_next, 0);
        // Another thread beat us to it.
        nbd_free(next);
        return;
    }
    TRACE("h0", "hti_start_copy: new hti is %p", next, 0);
}

// Copy the key and value stored in <old_e> (which must be an entry in <old_hti>) to <new_hti>. 
//
// Return 1 unless <old_e> is already copied (then return 0), so the caller can account for the total
// number of entries left to copy.
static int hti_copy_entry (hash_table_i_t *old_hti, volatile entry_t *old_e, uint32_t key_hash, 
                           hash_table_i_t *new_hti) {
    TRACE("h0", "hti_copy_entry(copy entry from %p to %p)", old_hti, new_hti);
    assert(old_hti);
    assert(old_hti->next);
    assert(new_hti);
    assert(old_e >= old_hti->table && old_e < old_hti->table + (1 << old_hti->scale));

    int64_t old_e_value = old_e->value;
    TRACE("h0", "hti_copy_entry: entry %p current value %p", old_e, old_e_value);
    if (EXPECT_FALSE(old_e_value == COPIED_VALUE))
        return FALSE; // already copied

    // Kill empty entries.
    if (EXPECT_FALSE(old_e_value == DOES_NOT_EXIST)) {
        old_e_value = SYNC_CAS(&old_e->value, DOES_NOT_EXIST, COPIED_VALUE);
        if (old_e_value == DOES_NOT_EXIST) {
            TRACE("h0", "hti_copy_entry: old entry killed", 0, 0);
            return TRUE;
        }
        if (old_e_value == COPIED_VALUE) {
            TRACE("h0", "hti_copy_entry: lost race to kill empty entry in old hti", 0, 0);
            return FALSE; // another thread beat us to it
        }
        TRACE("h0", "hti_copy_entry: lost race to kill empty entry in old hti; "
                    "the entry is now being used", 0, 0);
    }

    // Tag the value in the old entry to indicate a copy is in progress.
    old_e_value = SYNC_FETCH_AND_OR(&old_e->value, TAG_VALUE(0));
    TRACE("h0", "hti_copy_entry: tagged the value %p in old entry %p", old_e_value, old_e);
    if (old_e_value == COPIED_VALUE) 
        return FALSE; // <value> was already copied by another thread.

    // Deleted entries don't need to be installed into to the new table, but their keys do need to
    // be freed.
    assert(COPIED_VALUE == TAG_VALUE(TOMBSTONE));
    if (old_e_value == TOMBSTONE) {
        nbd_free((string_t *)(old_e->key & MASK(48)));
        return TRUE; 
    }
    old_e_value = STRIP_TAG(old_e_value);

    // Install the key in the new table.
    uint64_t old_e_key = old_e->key;
    string_t *key = (string_t *)(old_e_key & MASK(48));
    TRACE("h0", "hti_copy_entry: key %p is %s", old_e_key, key->val);

    // We use 0 to indicate that <key_hash> isn't initiallized. Occasionally the <key_hash> will
    // really be 0 and we will waste time recomputing it. That is rare enough that it is OK. 
    if (key_hash == 0) { 
        key_hash = murmur32(key->val, key->len);
    }

    int is_empty;
    volatile entry_t *new_e = hti_lookup(new_hti, key_hash, key->val, key->len, &is_empty);

    // it is possible that there is not any room in the new table either
    if (EXPECT_FALSE(new_e == NULL)) {
        hti_start_copy(new_hti); // initiate nested copy, if not already started
        return hti_copy_entry(old_hti, old_e, key_hash, new_hti->next); // recursive tail-call
    }

    // a tagged entry returned from hti_lookup() means it is either empty or has a new key
    if (is_empty) {
        uint64_t new_e_key = SYNC_CAS(&new_e->key, DOES_NOT_EXIST, old_e_key);
        if (new_e_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_copy_entry: lost race to CAS key %p into new entry; found %p",
                    old_e_key, new_e_key);
            return hti_copy_entry(old_hti, old_e, key_hash, new_hti); // recursive tail-call
        }
    }
    assert(ht_key_equals(new_e->key, key_hash, key->val, key->len));
    TRACE("h0", "hti_copy_entry: key %p installed in new old hti %p", key->val, new_hti);

    // Copy the value to the entry in the new table.
    uint64_t new_e_value = SYNC_CAS(&new_e->value, DOES_NOT_EXIST, old_e_value);

    // If there is a nested copy in progress, we might have installed the key into a dead entry.
    if (new_e_value == COPIED_VALUE)
        return hti_copy_entry(old_hti, old_e, key_hash, new_hti->next); // recursive tail-call

    // Mark the old entry as dead.
    old_e->value = COPIED_VALUE;

    // Update the count if we were the one that completed the copy.
    if (new_e_value == DOES_NOT_EXIST) {
        TRACE("h0", "hti_copy_entry: value %p installed in new hti %p", old_e_value, new_hti);
        SYNC_ADD(&old_hti->count, -1);
        SYNC_ADD(&new_hti->count, 1);
        return TRUE;
    }

    TRACE("h0", "hti_copy_entry: lost race to CAS value %p in new hti; found %p", 
                old_e_value, new_e_value);
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
// Certain values of <expected> have special meaning. If <expected> is HT_EXPECT_EXISTS then any 
// real value matches (i.e. not a TOMBSTONE or DOES_NOT_EXIST) as long as <key> is in the table. If
// <expected> is HT_EXPECT_WHATEVER then skip the test entirely.
//
static int64_t hti_compare_and_set (hash_table_i_t *hti, uint32_t key_hash, const char *key_val, 
                                    uint32_t key_len, int64_t expected, int64_t new) {
    TRACE("h0", "hti_compare_and_set(hti %p key \"%s\")", hti, key_val);
    TRACE("h0", "hti_compare_and_set(new value %p; caller expects value %p)", new, expected);
    assert(hti);
    assert(new != DOES_NOT_EXIST && !IS_TAGGED(new));
    assert(key_val);

    int is_empty;
    volatile entry_t *e = hti_lookup(hti, key_hash, key_val, key_len, &is_empty);

    // There is no room for <key>, grow the table and try again.
    if (e == NULL) {
        hti_start_copy(hti);
        return COPIED_VALUE;
    }

    // Install <key> in the table if it doesn't exist.
    if (is_empty) {
        TRACE("h0", "hti_compare_and_set: entry %p is empty", e, 0);
        if (expected != HT_EXPECT_WHATEVER && expected != HT_EXPECT_NOT_EXISTS)
            return DOES_NOT_EXIST;

        // No need to do anything, <key> is already deleted.
        if (new == TOMBSTONE)
            return DOES_NOT_EXIST;

        // allocate <key>.
        string_t *key = nbd_malloc(sizeof(uint32_t) + key_len);
        key->len = key_len;
        memcpy(key->val, key_val, key_len);

        // CAS <key> into the table. 
        uint64_t e_key = SYNC_CAS(&e->key, DOES_NOT_EXIST, ((uint64_t)(key_hash >> 16) << 48) | (uint64_t)key);

        // Retry if another thread stole the entry out from under us.
        if (e_key != DOES_NOT_EXIST) {
            TRACE("h0", "hti_compare_and_set: key in entry %p is \"%s\"", e, e_key & MASK(48));
            TRACE("h0", "hti_compare_and_set: lost race to install key \"%s\" in %p", key->val, e);
            nbd_free(key);
            return hti_compare_and_set(hti, key_hash, key_val, key_len, expected, new); // tail-call
        }
        TRACE("h0", "hti_compare_and_set: installed key \"%s\" in entry %p", key_val, e);
    }

    // If the entry is in the middle of a copy, the copy must be completed first.
    int64_t e_value = e->value;
    TRACE("h0", "hti_compare_and_set: value in entry %p is %p", e, e_value);
    if (EXPECT_FALSE(IS_TAGGED(e_value))) {
        int did_copy = hti_copy_entry(hti, e, key_hash, ((volatile hash_table_i_t *)hti)->next);
        if (did_copy) {
            SYNC_ADD(&hti->num_entries_copied, 1);
        }
        return COPIED_VALUE;
    }

    // Fail if the old value is not consistent with the caller's expectation.
    int old_existed = (e_value != TOMBSTONE && e_value != DOES_NOT_EXIST);
    if (EXPECT_FALSE(expected != HT_EXPECT_WHATEVER && expected != e_value)) {
        if (EXPECT_FALSE(expected != (old_existed ? HT_EXPECT_EXISTS : HT_EXPECT_NOT_EXISTS))) {
            TRACE("h0", "hti_compare_and_set: value expected by caller for key \"%s\" not found; "
                        "found value %p", key_val, e_value);
            return e_value;
        }
    }

    // CAS the value into the entry. Retry if it fails.
    uint64_t v = SYNC_CAS(&e->value, e_value, new);
    if (EXPECT_FALSE(v != e_value)) {
        TRACE("h0", "hti_compare_and_set: value CAS failed; expected %p found %p", e_value, v);
        return hti_compare_and_set(hti, key_hash, key_val, key_len, expected, new); // recursive tail-call
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
static int64_t hti_get (hash_table_i_t *hti, uint32_t key_hash, const char *key_val, uint32_t key_len) {
    assert(key_val);

    int is_empty;
    volatile entry_t *e = hti_lookup(hti, key_hash, key_val, key_len, &is_empty);

    // When hti_lookup() returns NULL it means we hit the reprobe limit while
    // searching the table. In that case, if a copy is in progress the key 
    // might exist in the copy.
    if (EXPECT_FALSE(e == NULL)) {
        if (((volatile hash_table_i_t *)hti)->next != NULL)
            return hti_get(hti->next, key_hash, key_val, key_len); // recursive tail-call
        return DOES_NOT_EXIST;
    }

    if (is_empty)
        return DOES_NOT_EXIST;

    // If the entry is being copied, finish the copy and retry on the next table.
    int64_t e_value = e->value;
    if (EXPECT_FALSE(IS_TAGGED(e_value))) {
        if (EXPECT_FALSE(e_value != COPIED_VALUE)) {
            int did_copy = hti_copy_entry(hti, e, key_hash, ((volatile hash_table_i_t *)hti)->next);
            if (did_copy) {
                SYNC_ADD(&hti->num_entries_copied, 1);
            }
        }
        return hti_get(((volatile hash_table_i_t *)hti)->next, key_hash, key_val, key_len); // tail-call
    }

    return (e_value == TOMBSTONE) ? DOES_NOT_EXIST : e_value;
}

//
int64_t ht_get (hash_table_t *ht, const char *key_val, uint32_t key_len) {
    return hti_get(*ht, murmur32(key_val, key_len), key_val, key_len);
}

//
int64_t ht_compare_and_set (hash_table_t *ht, const char *key_val, uint32_t key_len, 
                            int64_t expected_val, int64_t new_val) {

    assert(key_val);
    assert(!IS_TAGGED(new_val) && new_val != DOES_NOT_EXIST);

    hash_table_i_t *hti = *ht;

    // Help with an ongoing copy.
    if (EXPECT_FALSE(hti->next != NULL)) {

        // Reserve some entries for this thread to copy. There is a race condition here because the
        // fetch and add isn't atomic, but that is ok.
        int x = hti->scan; 
        hti->scan = x + ENTRIES_PER_COPY_CHUNK; 

        // <hti->scan> might be larger than the size of the table, if some thread stalls while 
        // copying. In that case we just wrap around to the begining and make another pass through
        // the table.
        volatile entry_t *e = hti->table + (x & MASK(hti->scale));

        // Copy the entries
        int num_copied = 0, i;
        for (i = 0; i < ENTRIES_PER_COPY_CHUNK; ++i) {
            num_copied += hti_copy_entry(hti, e++, 0, hti->next);
            assert(e <= hti->table + (1 << hti->scale));
        }
        if (num_copied != 0) {
            SYNC_ADD(&hti->num_entries_copied, num_copied);
        }
    }

    // Dispose of fully copied tables.
    while (hti->num_entries_copied == (1 << hti->scale)) {
        assert(hti->next);
        if (SYNC_CAS(ht, hti, hti->next) == hti) {
            nbd_defer_free(hti); 
        }
        hti = *ht;
    }

    int64_t old_val;
    uint32_t key_hash = murmur32(key_val, key_len);
    while ((old_val = hti_compare_and_set(hti, key_hash, key_val, key_len, expected_val, new_val)) 
           == COPIED_VALUE) {
        assert(hti->next);
        hti = hti->next;
    }

    return old_val == TOMBSTONE ? DOES_NOT_EXIST : old_val;
}

// Remove the value in <ht> associated with <key_val>. Returns the value removed, or 
// DOES_NOT_EXIST if there was no value for that key.
int64_t ht_remove (hash_table_t *ht, const char *key_val, uint32_t key_len) {
    hash_table_i_t *hti = *ht;
    int64_t val;
    uint32_t key_hash = murmur32(key_val, key_len);
    do {
        val = hti_compare_and_set(hti, key_hash, key_val, key_len, HT_EXPECT_WHATEVER, TOMBSTONE);
        if (val != COPIED_VALUE)
            return val == TOMBSTONE ? DOES_NOT_EXIST : val;
        assert(hti->next);
        hti = hti->next;
        assert(hti);
    } while (1);
}

// Returns the number of key-values pairs in <ht>
uint64_t ht_count (hash_table_t *ht) {
    hash_table_i_t *hti = *ht;
    uint64_t count = 0;
    while (hti) {
        count += hti->count;
        hti = hti->next; 
    }
    return count;
}

// Allocate and initialize a new hash table.
hash_table_t *ht_alloc (void) {
    hash_table_t *ht = nbd_malloc(sizeof(hash_table_t));
    *ht = (hash_table_i_t *)hti_alloc(ht, MIN_SCALE);
    return ht;
}

// Free <ht> and its internal structures.
void ht_free (hash_table_t *ht) {
    hash_table_i_t *hti = *ht;
    do {
        hash_table_i_t *next = hti->next;
        nbd_free(hti);
        hti = next;
    } while (hti);
    nbd_free(ht);
}
