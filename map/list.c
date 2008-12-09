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
#include "list.h"
#include "mem.h"

typedef struct ll_iter node_t;

struct ll_iter {
    void *key;
    uint64_t val;
    node_t *next;
};

struct ll {
    node_t *head;
    const datatype_t *key_type;
};

static node_t *node_alloc (void *key, uint64_t val) {
    node_t *item = (node_t *)nbd_malloc(sizeof(node_t));
    item->key = key;
    item->val = val;
    return item;
}

list_t *ll_alloc (const datatype_t *key_type) {
    list_t *ll = (list_t *)nbd_malloc(sizeof(list_t));
    ll->key_type = key_type;
    ll->head = node_alloc(NULL, 0);
    ll->head->next = NULL;
    return ll;
}

void ll_free (list_t *ll) {
    node_t *item = ll->head->next;
    while (item) {
        node_t *next = (node_t *)STRIP_TAG(item->next, TAG1);
        nbd_free(item);
        item = next;
    }
}

uint64_t ll_count (list_t *ll) {
    uint64_t count = 0;
    node_t *item = ll->head->next;
    while (item) {
        if (!IS_TAGGED(item->next, TAG1)) {
            count++;
        }
        item = (node_t *)STRIP_TAG(item->next, TAG1);
    }
    return count;
}

static int find_pred (node_t **pred_ptr, node_t **item_ptr, list_t *ll, void *key, int help_remove) {
    node_t *pred = ll->head;
    node_t *item = pred->next;
    TRACE("l2", "find_pred: searching for key %p in list (head is %p)", key, pred);

    while (item != NULL) {
        node_t *next = item->next;

        // A tag means an item is logically removed but not physically unlinked yet.
        while (EXPECT_FALSE(IS_TAGGED(next, TAG1))) {

            // Skip over logically removed items.
            if (!help_remove) {
                item = (node_t *)STRIP_TAG(item->next, TAG1);
                if (EXPECT_FALSE(item == NULL))
                    break;
                TRACE("l3", "find_pred: skipping marked item %p (next is %p)", item, next);
                next = item->next;
                continue;
            }

            // Unlink logically removed items.
            node_t *other;
            TRACE("l3", "find_pred: unlinking marked item %p next is %p", item, next);
            if ((other = SYNC_CAS(&pred->next, item, STRIP_TAG(next, TAG1))) == item) {
                TRACE("l2", "find_pred: unlinked item %p from pred %p", item, pred);
                item = (node_t *)STRIP_TAG(next, TAG1);
                if (EXPECT_FALSE(item == NULL))
                    break;
                next = item->next;
                TRACE("l3", "find_pred: now current item is %p next is %p", item, next);

                // The thread that completes the unlink should free the memory.
                if (ll->key_type != NULL) {
                    nbd_defer_free((void*)other->key);
                }
                nbd_defer_free(other);
            } else {
                TRACE("l2", "find_pred: lost a race to unlink item %p from pred %p", item, pred);
                TRACE("l2", "find_pred: pred's link changed to %p", other, 0);
                if (IS_TAGGED(other, TAG1))
                    return find_pred(pred_ptr, item_ptr, ll, key, help_remove); // retry
                item = other;
                if (EXPECT_FALSE(item == NULL))
                    break;
                next = item->next;
            }
        }

        if (EXPECT_FALSE(item == NULL))
            break;

        TRACE("l3", "find_pred: visiting item %p (next is %p)", item, next);
        TRACE("l4", "find_pred: key %p val %p", item->key, item->val);

        int d;
        if (EXPECT_TRUE(ll->key_type == NULL)) {
            d = (uint64_t)item->key - (uint64_t)key;
        } else {
            d = ll->key_type->cmp(item->key, key);
        }

        // If we reached the key (or passed where it should be), we found the right predesssor
        if (d >= 0) {
            if (pred_ptr != NULL) {
                *pred_ptr = pred;
            }
            *item_ptr = item;
            if (d == 0) {
                TRACE("l2", "find_pred: found matching item %p in list, pred is %p", item, pred);
                return TRUE;
            } 
            TRACE("l2", "find_pred: found proper place for key %p in list, pred is %p", key, pred);
            return FALSE;
        }

        pred = item;
        item = next;
    }

    // <key> is not in <ll>.
    if (pred_ptr != NULL) {
        *pred_ptr = pred;
    }
    *item_ptr = NULL;
    TRACE("l2", "find_pred: reached end of list. last item is %p", pred, 0);
    return FALSE;
}

// Fast find. Do not help unlink partially removed nodes and do not return the found item's predecessor.
uint64_t ll_lookup (list_t *ll, void *key) {
    TRACE("l1", "ll_lookup: searching for key %p in list %p", key, ll);
    node_t *item;
    int found = find_pred(NULL, &item, ll, key, FALSE);

    // If we found an <item> matching the key return its value.
    if (found) {
        uint64_t val = item->val;
        if (val != DOES_NOT_EXIST) {
            TRACE("l1", "ll_lookup: found item %p. val %p. returning item", item, item->val);
            return val;
        }
    }

    TRACE("l1", "ll_lookup: no item in the list matched the key", 0, 0);
    return DOES_NOT_EXIST;
}

uint64_t ll_cas (list_t *ll, void *key, uint64_t expectation, uint64_t new_val) {
    TRACE("l1", "ll_cas: key %p list %p", key, ll);
    TRACE("l1", "ll_cas: expectation %p new value %p", expectation, new_val);
    ASSERT((int64_t)new_val > 0);

    do {
        node_t *pred, *old_item;
        int found = find_pred(&pred, &old_item, ll, key, TRUE);
        if (!found) {

            // There was not an item in the list that matches the key. 
            if (EXPECT_FALSE((int64_t)expectation > 0 || expectation == CAS_EXPECT_EXISTS)) {
                TRACE("l1", "ll_cas: the expectation was not met, the list was not changed", 0, 0);
                return DOES_NOT_EXIST; // failure
            }

            ASSERT(expectation == CAS_EXPECT_DOES_NOT_EXIST || expectation == CAS_EXPECT_WHATEVER);

            // Create a new item and insert it into the list.
            TRACE("l2", "ll_cas: attempting to insert item between %p and %p", pred, pred->next);
            void *new_key  = (ll->key_type == NULL) ? key : ll->key_type->clone(key);
            node_t *new_item = node_alloc(new_key, new_val);
            node_t *next = new_item->next = old_item;
            node_t *other = SYNC_CAS(&pred->next, next, new_item);
            if (other == next) {
                TRACE("l1", "ll_cas: successfully inserted new item %p", new_item, 0);
                return DOES_NOT_EXIST; // success
            }

            // Lost a race. Failed to insert the new item into the list.
            TRACE("l1", "ll_cas: lost a race. CAS failed. expected pred's link to be %p but found %p", next, other);
            if (ll->key_type != NULL) {
                nbd_free(new_key);
            }
            nbd_free(new_item);
            continue; // retry
        }

        // Found an item in the list that matches the key.
        uint64_t old_item_val = old_item->val;
        do {
            // If the item's value is DOES_NOT_EXIST it means another thread removed the node out from under us.
            if (EXPECT_FALSE(old_item_val == DOES_NOT_EXIST)) {
                TRACE("l2", "ll_cas: lost a race, found an item but another thread removed it. retry", 0, 0);
                break; // retry
            }

            if (EXPECT_FALSE(expectation == CAS_EXPECT_DOES_NOT_EXIST)) {
                TRACE("l1", "ll_cas: found an item %p in the list that matched the key. the expectation was "
                        "not met, the list was not changed", old_item, old_item_val);
                return old_item_val; // failure
            }

            // Use a CAS and not a SWAP. If the node is in the process of being removed and we used a SWAP, we could
            // replace DOES_NOT_EXIST with our value. Then another thread that is updating the value could think it
            // succeeded and return our value even though we indicated that the node has been removed. If the CAS 
            // fails it means another thread either removed the node or updated its value.
            uint64_t ret_val = SYNC_CAS(&old_item->val, old_item_val, new_val);
            if (ret_val == old_item_val) {
                TRACE("l1", "ll_cas: the CAS succeeded. updated the value of the item", 0, 0);
                return ret_val; // success
            }
            TRACE("l2", "ll_cas: lost a race. the CAS failed. another thread changed the item's value", 0, 0);

            old_item_val = ret_val;
        } while (1);
    } while (1);
}

uint64_t ll_remove (list_t *ll, void *key) {
    TRACE("l1", "ll_remove: removing item with key %p from list %p", key, ll);
    node_t *pred;
    node_t *item;
    int found = find_pred(&pred, &item, ll, key, TRUE);
    if (!found) {
        TRACE("l1", "ll_remove: remove failed, an item with a matching key does not exist in the list", 0, 0);
        return DOES_NOT_EXIST;
    }

    // Mark <item> removed. If multiple threads try to remove the same item only one of them should succeed.
    node_t *next;
    node_t *old_next = item->next;
    do {
        next = old_next;
        old_next = SYNC_CAS(&item->next, next, TAG_VALUE(next, TAG1));
        if (IS_TAGGED(old_next, TAG1)) {
            TRACE("l1", "ll_remove: lost a race -- %p is already marked for removal by another thread", item, 0);
            return DOES_NOT_EXIST;
        }
    } while (next != old_next);
    TRACE("l2", "ll_remove: logically removed item %p", item, 0);
    ASSERT(IS_TAGGED(item->next, TAG1));

    // Atomically swap out the item's value in case another thread is updating the item while we are 
    // removing it. This establishes which operation occurs first logically, the update or the remove. 
    uint64_t val = SYNC_SWAP(&item->val, DOES_NOT_EXIST); 
    TRACE("l2", "ll_remove: replaced item's val %p with DOES_NOT_EXIT", val, 0);

    // Unlink <item> from <ll>. If we lose a race to another thread just back off. It is safe to leave the
    // item logically removed for a later call (or some other thread) to physically unlink. By marking the
    // item earlier, we logically removed it. 
    TRACE("l2", "ll_remove: unlink the item by linking its pred %p to its successor %p", pred, next);
    node_t *other;
    if ((other = SYNC_CAS(&pred->next, item, next)) != item) {
        TRACE("l1", "ll_remove: unlink failed; pred's link changed from %p to %p", item, other);
        return val;
    } 

    // The thread that completes the unlink should free the memory.
    if (ll->key_type != NULL) {
        nbd_defer_free(item->key);
    }
    nbd_defer_free(item);
    TRACE("l1", "ll_remove: successfully unlinked item %p from the list", item, 0);
    return val;
}

void ll_print (list_t *ll) {
    node_t *item;
    item = ll->head->next;
    int i = 0;
    while (item) {
        node_t *next = item->next;
        if (IS_TAGGED(item, TAG1)) {
            printf("*");
        }
        printf("%p:%p ", item, item->key);
        fflush(stdout);
        item = (node_t *)STRIP_TAG(next, TAG1);
        if (i++ > 30) {
            printf("...");
            break;
        }
    }
    printf("\n");
}

ll_iter_t *ll_iter_start (list_t *ll, void *key) {
    node_t *item;
    find_pred(NULL, &item, ll, key, FALSE);
    return item;
}

ll_iter_t *ll_iter_next (ll_iter_t *iter) {
    assert(iter);
    if (EXPECT_FALSE(!iter))
        return NULL;

    node_t *next = iter->next;
    while (next != NULL && IS_TAGGED(next->next, TAG1)) {
        next = (node_t *)STRIP_TAG(next->next, TAG1);
    }

    return next;
}

uint64_t ll_iter_val (ll_iter_t *iter) {
    return iter->val;
}

void *ll_iter_key (ll_iter_t *iter) {
    return iter->key;
}
