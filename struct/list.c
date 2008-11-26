/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Harris-Michael lock-free list-based set
 * http://www.research.ibm.com/people/m/michael/spaa-2002.pdf
 */
#include <stdio.h>
#include <string.h>

#include "common.h"
#include "struct.h"
#include "nstring.h"
#include "mem.h"

typedef struct node {
    nstring_t *key;
    uint64_t value;
    struct node *next;
} node_t;

struct ll {
    node_t *head;
};

static node_t *node_alloc (const void *key_data, uint32_t key_len, uint64_t value) {
    node_t *item = (node_t *)nbd_malloc(sizeof(node_t));
    memset(item, 0, sizeof(node_t));
    item->key   = key_data ? ns_alloc(key_data, key_len) : NULL;
    item->value = value;
    return item;
}

list_t *ll_alloc (void) {
    list_t *ll = (list_t *)nbd_malloc(sizeof(list_t));
    ll->head = node_alloc(" ", 0, 0);
    ll->head->next = NULL;
    return ll;
}

static node_t *find_pred (node_t **pred_ptr, list_t *ll, const void *key_data, uint32_t key_len, int help_remove) {
    node_t *pred = ll->head;
    node_t *item = pred->next;
    TRACE("l3", "find_pred: searching for key %p in ll (head is %p)", key_data, pred);

    while (item != NULL) {
        node_t *next = item->next;
        TRACE("l3", "find_pred: visiting item %p (next %p)", item, next);
        TRACE("l3", "find_pred: key \"%s\"", ns_data(item->key), item->value);

        // A tag means an item is logically removed but not physically unlinked yet.
        while (EXPECT_FALSE(IS_TAGGED(next))) {

            // Skip over logically removed items.
            if (!help_remove) {
                item = (node_t *)STRIP_TAG(item->next);
                if (EXPECT_FALSE(item == NULL))
                    break;
                next = item->next;
                continue;
            }

            // Unlink logically removed items.
            node_t *other;
            if ((other = SYNC_CAS(&pred->next, item, STRIP_TAG(next))) == item) {
                item = (node_t *)STRIP_TAG(next);
                if (EXPECT_FALSE(item == NULL))
                    break;
                next = item->next;
                TRACE("l3", "find_pred: unlinked item %p from pred %p", item, pred);
                TRACE("l3", "find_pred: now item is %p next is %p", item, next);

                // The thread that completes the unlink should free the memory.
                nbd_defer_free(other);
            } else {
                TRACE("l3", "find_pred: lost race to unlink from pred %p; its link changed to %p", pred, other);
                if (IS_TAGGED(other))
                    return find_pred(pred_ptr, ll, key_data, key_len, help_remove); // retry
                item = other;
                if (EXPECT_FALSE(item == NULL))
                    break;
                next = item->next;
            }
        }

        if (EXPECT_FALSE(item == NULL))
            break;

        // If we reached the key (or passed where it should be), we found the right predesssor
        int x = ns_cmp_raw(item->key, key_data, key_len);
        if (x >= 0) {
            TRACE("l3", "find_pred: found pred %p item %p", pred, item);
            if (pred_ptr != NULL) {
                *pred_ptr = pred;
            }
            return x == 0 ? item : NULL;
        }

        pred = item;
        item = next;

    }

    // <key> is not in <ll>.
    if (pred_ptr != NULL) {
        *pred_ptr = pred;
    }
    return NULL;
}

// Fast find. Do not help unlink partially removed nodes and do not return the found item's predecessor.
uint64_t ll_lookup (list_t *ll, const void *key_data, uint32_t key_len) {
    TRACE("l3", "ll_lookup: searching for key %p in ll %p", key_data, ll);
    node_t *item = find_pred(NULL, ll, key_data, key_len, FALSE);

    // If we found an <item> matching the key return its value.
    return item != NULL ? item->value : DOES_NOT_EXIST;
}

// Insert a new item if a matching key doesn't already exist in <ll>
uint64_t ll_add (list_t *ll, const void *key_data, uint32_t key_len, uint64_t value) {
    TRACE("l3", "ll_add: inserting key %p value %p", key_data, value);
    node_t *pred;
    node_t *item = NULL;
    do {
        node_t *next = find_pred(&pred, ll, key_data, key_len, TRUE);

        // If a node matching the key already exists in <ll> return its value.
        if (next != NULL) {
            TRACE("l3", "ll_add: there is already an item %p (value %p) with the same key", next, next->value);
            if (EXPECT_FALSE(item != NULL)) { nbd_free(item); }
            return next->value;
        }

        next = pred->next;
        TRACE("l3", "ll_add: attempting to insert item between %p and %p", pred, next);
        if (EXPECT_TRUE(item == NULL)) { item = node_alloc(key_data, key_len, value); }
        item->next = next;
        node_t *other = SYNC_CAS(&pred->next, next, item);
        if (other == next) {
            TRACE("l3", "ll_add: successfully inserted item %p", item, 0);
            return DOES_NOT_EXIST; // success
        }
        TRACE("l3", "ll_add: failed to change pred's link: expected %p found %p", next, other);

    } while (1);
}

uint64_t ll_remove (list_t *ll, const void *key_data, uint32_t key_len) {
    TRACE("l3", "ll_remove: removing item with key %p from ll %p", key_data, ll);
    node_t *pred;
    node_t *item = find_pred(&pred, ll, key_data, key_len, TRUE);
    if (item == NULL) {
        TRACE("l3", "ll_remove: remove failed, an item with a matching key does not exist in the ll", 0, 0);
        return DOES_NOT_EXIST;
    }

    // Mark <item> removed. This must be atomic. If multiple threads try to remove the same item
    // only one of them should succeed.
    if (EXPECT_FALSE(IS_TAGGED(item->next))) {
        TRACE("l3", "ll_remove: %p is already marked for removal by another thread", item, 0);
        return DOES_NOT_EXIST;
    }
    node_t *next = SYNC_FETCH_AND_OR(&item->next, TAG);
    if (EXPECT_FALSE(IS_TAGGED(next))) {
        TRACE("l3", "ll_remove: lost race -- %p is already marked for removal by another thread", item, 0);
        return DOES_NOT_EXIST;
    }

    uint64_t value = item->value;

    // Unlink <item> from <ll>.
    TRACE("l3", "ll_remove: link item's pred %p to it's successor %p", pred, next);
    node_t *other;
    if ((other = SYNC_CAS(&pred->next, item, next)) != item) {
        TRACE("l3", "ll_remove: unlink failed; pred's link changed from %p to %p", item, other);
        // By marking the item earlier, we logically removed it. It is safe to leave the item.
        // Another thread will finish physically removing it from the ll.
        return value;
    } 

    // The thread that completes the unlink should free the memory.
    nbd_defer_free(item); 
    return value;
}

void ll_print (list_t *ll) {
    node_t *item;
    item = ll->head->next;
    while (item) {
        printf("%s ", (char *)ns_data(item->key));
        fflush(stdout);
        item = item->next;
    }
    printf("\n");
}
