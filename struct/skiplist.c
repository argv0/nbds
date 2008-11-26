/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Implementation of the lock-free skiplist data-structure created by Maurice Herlihy, Yossi Lev,
 * and Nir Shavit. See Herlihy's and Shivit's book "The Art of Multiprocessor Programming".
 * http://www.amazon.com/Art-Multiprocessor-Programming-Maurice-Herlihy/dp/0123705916/
 *
 * See also Kir Fraser's dissertation "Practical Lock Freedom".
 * www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
 *
 * This code is written for the x86 memory-model. The algorithim depends on certain stores and
 * loads being ordered. Be careful, this code probably won't work correctly on platforms with
 * weaker memory models if you don't add memory barriers in the right places.
 */
#include <stdio.h>
#include <string.h>

#include "common.h"
#include "runtime.h"
#include "struct.h"
#include "nstring.h"
#include "mem.h"
#include "tls.h"

// Setting MAX_LEVEL to 0 essentially makes this data structure the Harris-Michael lock-free list
// in list.c
#define MAX_LEVEL 31

typedef struct node {
    nstring_t *key;
    uint64_t value;
    int top_level;
    struct node *next[];
} node_t;

struct sl {
    node_t *head;
};

static int random_level (void) {
    unsigned r = nbd_rand();
    if (r&1)
        return 0;
    int n = __builtin_ctz(r)-1;
#if MAX_LEVEL < 31
    if (n > MAX_LEVEL)
        return MAX_LEVEL;
#endif
    assert(n <= MAX_LEVEL);
    return n;
}

node_t *node_alloc (int level, const void *key_data, uint32_t key_len, uint64_t value) {
    assert(level >= 0 && level <= MAX_LEVEL);
    size_t sz = sizeof(node_t) + (level + 1) * sizeof(node_t *);
    node_t *item = (node_t *)nbd_malloc(sz);
    memset(item, 0, sz);
    item->key   = ns_alloc(key_data, key_len);
    item->value = value;
    item->top_level = level;
    return item;
}

skiplist_t *sl_alloc (void) {
    skiplist_t *sl = (skiplist_t *)nbd_malloc(sizeof(skiplist_t));
    sl->head = node_alloc(MAX_LEVEL, " ", 0, 0);
    memset(sl->head->next, 0, (MAX_LEVEL+1) * sizeof(skiplist_t *));
    return sl;
}

static node_t *find_preds (int *found, node_t *preds[MAX_LEVEL+1], int n, skiplist_t *sl, const void *key_data, uint32_t key_len, int help_remove) {
    node_t *pred = sl->head;
    node_t *item = NULL;
    TRACE("s3", "find_preds: searching for key %p in sl (head is %p)", key_data, pred);
    *found = 0;

    int start_level = MAX_LEVEL;
#if MAX_LEVEL > 2
    // Optimization for small lists. No need to traverse empty higher levels.
    start_level = 2;
    while (pred->next[start_level+1] != NULL) {
        start_level += start_level - 1;
        if (EXPECT_FALSE(start_level >= MAX_LEVEL)) {
            start_level = MAX_LEVEL;
            break;
        }
    }
    if (EXPECT_FALSE(start_level < n)) {
        start_level = n;
    }
#endif

    // Traverse the levels of <sl> from the top level to the bottom
    for (int level = start_level; level >= 0; --level) {
        TRACE("s3", "find_preds: level %llu", level, 0);
        item = pred->next[level];
        if (EXPECT_FALSE(IS_TAGGED(item))) {
            TRACE("s3", "find_preds: pred %p is marked for removal (item %p); retry", pred, item);
            return find_preds(found, preds, n, sl, key_data, key_len, help_remove); // retry
        }
        while (item != NULL) {
            node_t *next = item->next[level];
            TRACE("s3", "find_preds: visiting item %p (next %p)", item, next);
            TRACE("s3", "find_preds: key %p", item->key, 0);

            // A tag means an item is logically removed but not physically unlinked yet.
            while (EXPECT_FALSE(IS_TAGGED(next))) {

                // Skip over logically removed items.
                if (!help_remove) {
                    item = (node_t *)STRIP_TAG(item->next);
                    if (EXPECT_FALSE(item == NULL))
                        break;
                    next = item->next[level];
                    continue;
                }

                // Unlink logically removed items.
                node_t *other;
                if ((other = SYNC_CAS(&pred->next[level], item, STRIP_TAG(next))) == item) {
                    item = (node_t *)STRIP_TAG(next);
                    if (EXPECT_FALSE(item == NULL))
                        break;
                    next = item->next[level];
                    TRACE("s3", "find_preds: unlinked item %p from pred %p", item, pred);
                    TRACE("s3", "find_preds: now item is %p next is %p", item, next);

                    // The thread that completes the unlink should free the memory.
                    if (level == 0) { nbd_defer_free(other); }
                } else {
                    TRACE("s3", "find_preds: lost race to unlink from pred %p; its link changed to %p", pred, other);
                    if (IS_TAGGED(other))
                        return find_preds(found, preds, n, sl, key_data, key_len, help_remove); // retry
                    item = other;
                    if (EXPECT_FALSE(item == NULL))
                        break;
                    next = item->next[level];
                }
            }

            if (EXPECT_FALSE(item == NULL))
                break;

            // If we reached the key (or passed where it should be), we found a pred. Save it and continue down.
            int x = ns_cmp_raw(item->key, key_data, key_len);
            if (x >= 0) {
                if (level == 0 && x == 0) {
                    *found = 1;
                }
                TRACE("s3", "find_preds: found pred %p item %p", pred, item);
                break;
            }

            pred = item;
            item = next;
        }
        if (preds != NULL) {
            preds[level] = pred;
        }
    }
    if (n == -1 && item != NULL) {
        assert(preds != NULL);
        for (int level = start_level + 1; level <= item->top_level; ++level) {
            preds[level] = sl->head;
        }
    }
    return item;
}

// Fast find that does not help unlink partially removed nodes and does not return the node's predecessors.
uint64_t sl_lookup (skiplist_t *sl, const void *key_data, uint32_t key_len) {
    TRACE("s3", "sl_lookup: searching for key %p in sl %p", key, sl);
    int found;
    node_t *item = find_preds(&found, NULL, 0, sl, key_data, key_len, FALSE);

    // If we found an <item> matching the <key> return its value.
    return found ? item->value : DOES_NOT_EXIST;
}

// Insert the <key> if it doesn't already exist in <sl>
uint64_t sl_add (skiplist_t *sl, const void *key_data, uint32_t key_len, uint64_t value) {
    TRACE("s3", "sl_add: inserting key %p value %p", key, value);
    node_t *preds[MAX_LEVEL+1];
    node_t *item = NULL;
    int n = random_level();
    do {
        int found;
        node_t *next = find_preds(&found, preds, n, sl, key_data, key_len, TRUE);

        // If a node matching <key> already exists in <sl>, return its value.
        if (found) {
            TRACE("s3", "sl_add: there is already an item %p (value %p) with the same key", next, next->value);
            if (EXPECT_FALSE(item != NULL)) { nbd_free(item); }
            return next->value;
        }

        // First insert <item> into the bottom level.
        if (EXPECT_TRUE(item == NULL)) { item = node_alloc(n, key_data, key_len, value); }
        TRACE("s3", "sl_add: attempting to insert item between %p and %p", preds[0], next);
        item->next[0] = next;
        for (int level = 1; level <= item->top_level; ++level) {
            node_t *pred = preds[level];
            item->next[level] = pred->next[level];
        }
        node_t *pred = preds[0];
        node_t *other = SYNC_CAS(&pred->next[0], next, item);
        if (other == next) {
            TRACE("s3", "sl_add: successfully inserted item %p at level 0", item, 0);
            break; // success
        }
        TRACE("s3", "sl_add: failed to change pred's link: expected %p found %p", next, other);

    } while (1);

    // Insert <item> into <sl> from the bottom level up.
    for (int level = 1; level <= item->top_level; ++level) {
        do {
            node_t *pred;
            node_t *next;
            do {
                pred = preds[level];
                next = pred->next[level];
                if (next == NULL) // item goes at the end of the list
                    break;
                if (!IS_TAGGED(next) && ns_cmp_raw(next->key, key_data, key_len) > 0) // pred's link changed
                    break;
                int found;
                find_preds(&found, preds, item->top_level, sl, key_data, key_len, TRUE);
            } while (1);

            do {
                // There in no need to continue linking in the item if another thread removed it.
                node_t *old_next = ((volatile node_t *)item)->next[level];
                if (IS_TAGGED(old_next))
                    return DOES_NOT_EXIST; // success

                // Use a CAS so we to not inadvertantly remove a mark another thread placed on the item.
                if (next == old_next || SYNC_CAS(&item->next[level], old_next, next) == old_next)
                    break;
            } while (1);

            TRACE("s3", "sl_add: attempting to insert item between %p and %p", pred, next);
            node_t *other = SYNC_CAS(&pred->next[level], next, item);
            if (other == next) {
                TRACE("s3", "sl_add: successfully inserted item %p at level %llu", item, level);
                break; // success
            }
            TRACE("s3", "sl_add: failed to change pred's link: expected %p found %p", next, other);

        } while (1);
    }
    return value;
}

uint64_t sl_remove (skiplist_t *sl, const void *key_data, uint32_t key_len) {
    TRACE("s3", "sl_remove: removing item with key %p from sl %p", key_data, sl);
    int found;
    node_t *preds[MAX_LEVEL+1];
    node_t *item = find_preds(&found, preds, -1, sl, key_data, key_len, TRUE);
    if (!found) {
        TRACE("s3", "sl_remove: remove failed, an item with a matching key does not exist in the sl", 0, 0);
        return DOES_NOT_EXIST;
    }

    // Mark <item> removed at each level of <sl> from the top down. This must be atomic. If multiple threads
    // try to remove the same item only one of them should succeed. Marking the bottom level establishes which of 
    // them succeeds.
    for (int level = item->top_level; level >= 0; --level) {
        if (EXPECT_FALSE(IS_TAGGED(item->next[level]))) {
            TRACE("s3", "sl_remove: %p is already marked for removal by another thread", item, 0);
            if (level == 0)
                return DOES_NOT_EXIST;
            continue;
        }
        node_t *next = SYNC_FETCH_AND_OR(&item->next[level], TAG);
        if (EXPECT_FALSE(IS_TAGGED(next))) {
            TRACE("s3", "sl_remove: lost race -- %p is already marked for removal by another thread", item, 0);
            if (level == 0)
                return DOES_NOT_EXIST;
            continue;
        }
    }

    uint64_t value = item->value;

    // Unlink <item> from the top down.
    int level = item->top_level;
    while (level >= 0) {
        node_t *pred = preds[level];
        node_t *next = item->next[level];
        TRACE("s3", "sl_remove: link item's pred %p to it's successor %p", pred, STRIP_TAG(next));
        node_t *other = NULL;
        if ((other = SYNC_CAS(&pred->next[level], item, STRIP_TAG(next))) != item) {
            TRACE("s3", "sl_remove: unlink failed; pred's link changed from %p to %p", item, other);
            // By marking the item earlier, we logically removed it. It is safe to leave the item partially
            // unlinked. Another thread will finish physically removing it from <sl>.
            return value;
        }
        --level; 
    }

    // The thread that completes the unlink should free the memory.
    nbd_defer_free(item); 
    return value;
}

void sl_print (skiplist_t *sl) {
    for (int level = MAX_LEVEL; level >= 0; --level) {
        node_t *item = sl->head;
        if (item->next[level] == NULL)
            continue;
        printf("(%d) ", level);
        while (item) {
            node_t *next = item->next[level];
            printf("%s%p ", IS_TAGGED(next) ? "*" : "", item);
            item = (node_t *)STRIP_TAG(next);
        }
        printf("\n");
        fflush(stdout);
    }

    printf("\n");
    node_t *item = sl->head;
    while (item) {
        int is_marked = IS_TAGGED(item->next[0]);
        printf("%s%p:%s ", is_marked ? "*" : "", item, (char *)ns_data(item->key));
        if (item != sl->head) {
            printf("[%d]", item->top_level);
        } else {
            printf("[*]");
        }
        for (int level = 1; level <= item->top_level; ++level) {
            node_t *next = (node_t *)STRIP_TAG(item->next[level]);
            is_marked = IS_TAGGED(item->next[0]);
            printf(" %p%s", next, is_marked ? "*" : "");
            if (item == sl->head && item->next[level] == NULL)
                break;
        }
        printf("\n");
        fflush(stdout);
        item = (node_t *)STRIP_TAG(item->next[0]);
    }
}
