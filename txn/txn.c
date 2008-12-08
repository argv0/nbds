/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include "common.h"
#include "txn.h"
#include "mem.h"
#include "skiplist.h"

#define UNDETERMINED_VERSION 0
#define ABORTED_VERSION      TAG_VALUE(0, TAG1)
#define INITIAL_WRITES_SIZE  4

typedef struct update_rec update_t;

struct update_rec {
    update_t *next; // an earlier update
    uint64_t version;
    uint64_t value;
};

typedef struct write_rec {
    void *key;
    update_t *rec; 
} write_rec_t;

struct txn {
    uint64_t rv;
    uint64_t wv;
    map_t *map;
    write_rec_t *writes;
    uint32_t writes_size;
    uint32_t writes_count;
    uint32_t writes_scan;
    txn_type_e type;
    txn_state_e state;
};

static uint64_t version_ = 1;

static txn_state_e txn_validate (txn_t *txn);

static skiplist_t *active_ = NULL;

void txn_init (void) {
    active_ = sl_alloc(NULL);
}

// Validate the updates for <key>. Validation fails if there is a write-write conflict. That is if after our 
// read version another transaction committed a change to an entry we are also trying to change.
//
// If we encounter a potential conflict with a transaction that is in the process of validating, we help it 
// complete validating. It must be finished before we can decide to rollback or commit.
//
static txn_state_e tm_validate_key (txn_t *txn, void *key) {
    
    update_t *update = (update_t *) map_get(txn->map, key);
    for (; update != NULL; update = update->next) {

        // If the update or its version is not tagged it means the update is committed.
        //
        // We can stop at the first committed record we find that is at least as old as our read version. All 
        // the other committed records following it will be older. And all the uncommitted records following it 
        // will eventually conflict with it and abort.
        if (!IS_TAGGED(update, TAG2))
            return TXN_VALIDATED;
        update = (update_t *)STRIP_TAG(update, TAG2);
        if (!IS_TAGGED(update->version, TAG1)) 
            return (update->version <= txn->rv) ? TXN_VALIDATED : TXN_ABORTED;

        // If the update's version is tagged then either the update was aborted or the the version number is 
        // actually a pointer to a running transaction's txn_t.

        // Skip aborted transactions.
        if (EXPECT_FALSE(update->version == ABORTED_VERSION))
            continue;

        // The update's transaction is still in progress. Access its txn_t.
        txn_t *writer = (txn_t *)STRIP_TAG(update->version, TAG1);
        if (writer == txn)
            continue; // Skip our own updates.
        txn_state_e writer_state = writer->state;

        // Any running transaction will only be able to acquire a wv greater than ours. A transaction changes its 
        // state to validating before aquiring a wv. We can ignore an unvalidated transaction if its version is
        // greater than ours. See next comment below for why. 
        if (writer_state == TXN_RUNNING)
            continue; 
        
        // If <writer> has a later version than us we can safely ignore its updates. It will not commit until
        // we have completed validation (in order to remain non-blocking it will help us validate if necessary). 
        // This protocol ensures a deterministic resolution to every conflict and avoids infinite ping-ponging 
        // between validating two conflicting transactions.
        if (writer_state == TXN_VALIDATING) {
            if (writer->wv > txn->wv)
                continue;
            // Help <writer> commit. We need to know if <writer> aborts or commits before we can decide what to
            // do. But we don't want to block, so we assist.
            writer_state = txn_validate(writer);
        }

        // Skip updates from aborted transactions.
        if (writer_state == TXN_ABORTED)
            continue;

        assert(writer_state == TXN_VALIDATED);
        return (writer->wv <= txn->rv) ? TXN_VALIDATED : TXN_ABORTED;
    }

    return TXN_VALIDATED;
}

static txn_state_e txn_validate (txn_t *txn) {
    int i;
    switch (txn->state) {

        case TXN_VALIDATING:
            if (txn->wv == UNDETERMINED_VERSION) {
                uint64_t wv = SYNC_ADD(&version_, 1);
                SYNC_CAS(&txn->wv, UNDETERMINED_VERSION, wv);
            }

            for (i = 0; i < txn->writes_count; ++i) {
                txn_state_e s = tm_validate_key(txn, txn->writes[i].key);
                if (s == TXN_ABORTED) {
                    txn->state = TXN_ABORTED;
                    break;
                }
            }
            if (txn->state == TXN_VALIDATING) {
                txn->state =  TXN_VALIDATED;
            }
            break;

        case TXN_VALIDATED:
        case TXN_ABORTED:
            break;

        default:
            assert(FALSE);
    }

    return txn->state;
}

static update_t *alloc_update_rec (void) {
    update_t *u = (update_t *)nbd_malloc(sizeof(update_t));
    memset(u, 0, sizeof(update_t));
    return u;
}

txn_t *txn_begin (txn_type_e type, map_t *map) {
    txn_t *txn = (txn_t *)nbd_malloc(sizeof(txn_t));
    memset(txn, 0, sizeof(txn_t));
    txn->type = type;
    txn->wv = UNDETERMINED_VERSION;
    txn->state = TXN_RUNNING;
    txn->map = map;
    if (type != TXN_READ_ONLY) {
        txn->writes = nbd_malloc(sizeof(*txn->writes) * INITIAL_WRITES_SIZE);
        txn->writes_size = INITIAL_WRITES_SIZE;
    }

    // acquire the read version for txn. must be careful to avoid a race
    do {
        txn->rv = version_;

        uint64_t old_count;
        uint64_t temp = 0;
        do {
            old_count = temp;
            temp = (uint64_t)sl_cas(active_, (void *)txn->rv, old_count, old_count + 1);
        } while (temp != old_count);

        if (txn->rv == version_)
            break;

        temp = 1;
        do {
            old_count = temp;
            temp = sl_cas(active_, (void *)txn->rv, old_count, old_count - 1);
        } while (temp != old_count);
    } while (1);

    return txn;
}

void txn_abort (txn_t *txn) {

    int i;
    for (i = 0; i < txn->writes_count; ++i) {
        update_t *update = (update_t *)txn->writes[i].rec;
        update->version = ABORTED_VERSION;
    }

    nbd_defer_free(txn->writes);
    nbd_defer_free(txn);
}

txn_state_e txn_commit (txn_t *txn) {

    assert(txn->state == TXN_RUNNING);
    txn->state = TXN_VALIDATING;
    txn_state_e state = txn_validate(txn);

    // Detach <txn> from its updates.
    uint64_t wv = (txn->state == TXN_ABORTED) ? ABORTED_VERSION : txn->wv;
    int i;
    for (i = 0; i < txn->writes_count; ++i) {
        update_t *update = (update_t *)txn->writes[i].rec;
        update->version = wv;
    }

    // Lower the reference count for <txn>'s read version
    uint64_t temp = 2;
    uint64_t old_count;
    do {
        old_count = temp;
        temp = sl_cas(active_, (void *)txn->rv, old_count, old_count - 1);
        if (temp == 1 && txn->rv != version_) {
            sl_remove(active_, (void *)txn->rv);
            break;
        }
    } while (old_count != temp);

    nbd_defer_free(txn->writes);
    nbd_defer_free(txn);

    return state;
}

// Get most recent committed version prior to our read version.
uint64_t tm_get (txn_t *txn, void *key) {

    update_t *newest_update = (update_t *) map_get(txn->map, key);
    if (!IS_TAGGED(newest_update, TAG2))
            return (uint64_t)newest_update;

    // Iterate through the update records to find the latest committed version prior to our read version. 
    update_t *update;
    for (update = newest_update; ; update = update->next) {

        if (!IS_TAGGED(update, TAG2))
            return (uint64_t)update;

        update = (update_t *)STRIP_TAG(update, TAG2);
        assert(update != NULL);

        // If the update's version is not tagged it means the update is committed.
        if (!IS_TAGGED(update->version, TAG1)) {
            if (update->version <= txn->rv)
                break; // success
            continue;
        }

        // If the update's version is tagged then either the update was aborted or the the version number is 
        // actually a pointer to a running transaction's txn_t.

        // Skip updates from aborted transactions.
        if (EXPECT_FALSE(update->version == ABORTED_VERSION))
            continue;

        // The update's transaction is still in progress. Access its txn_t.
        txn_t *writer = (txn_t *)STRIP_TAG(update->version, TAG1);
        if (writer == txn) // found our own update
            break; // success 

        txn_state_e writer_state = writer->state;
        if (writer_state == TXN_RUNNING)
            continue; 

        if (writer_state == TXN_VALIDATING) {
            if (writer->wv > txn->rv)
                continue;
            writer_state = txn_validate(writer);
        }

        // Skip updates from aborted transactions.
        if (writer_state == TXN_ABORTED)
            continue;

        assert(writer_state == TXN_VALIDATED);
        if (writer->wv > txn->rv)
            continue;
        break; // success
    }

    uint64_t value = update->value;

    // collect some garbage
    update_t *last = update;
    update_t *next = update->next;
    uint64_t min_active = 0;
    if (IS_TAGGED(next, TAG2)) {
        next = (update_t *)STRIP_TAG(next, TAG2);
        min_active = (uint64_t)sl_min_key(active_);
        if (next->version < min_active) {

            // Skip over aborted versions to verify the chain of updates is old enough for collection
            update_t *temp = next;
            while (temp->version == ABORTED_VERSION) {
                assert(!IS_TAGGED(temp->version, TAG1));
                update_t *temp = next->next;
                if (!IS_TAGGED(temp, TAG2))
                    break;
                temp = (update_t *)STRIP_TAG(temp, TAG2);
                if (temp->version >= min_active)
                    return value;
                temp = temp->next;
            }

            // collect <next> and all the update records following it
            do {
                next = SYNC_SWAP(&update->next, NULL);

                // if we find ourself in a race just back off and let the other thread take care of it
                if (next == NULL) 
                    return value;

                update = next;
                next = next->next;
                nbd_free(update);
            } while (IS_TAGGED(next, TAG2));
        }
    }

    // If there is one item left and it is visible by all active transactions we can merge it into the map itself.
    // There is no need for an update record.
    if (next == NULL && last == (update_t *)STRIP_TAG(newest_update, TAG2)) {
        if (min_active == UNDETERMINED_VERSION) {
            min_active = (uint64_t)sl_min_key(active_);
        }
        if (last->version <= min_active) {
            if (map_cas(txn->map, key, TAG_VALUE(last, TAG2), value) == TAG_VALUE(last, TAG2)) {
                nbd_defer_free(last);
            }
        }
    } 
    
    return value;
}

void tm_set (txn_t *txn, void *key, uint64_t value) {

    // create a new update record
    update_t *update = alloc_update_rec();
    update->value = value;
    update->version = TAG_VALUE(txn, TAG1);

    // push the new update record onto <key>'s update list
    uint64_t old_update;
    do {
        old_update = map_get(txn->map, key);
        update->next = (update_t *)old_update;
    } while (map_cas(txn->map, key, old_update, TAG_VALUE(update, TAG2)) != old_update);

    // add <key> to the write set for commit-time validation
    if (txn->writes_count == txn->writes_size) {
        write_rec_t *w = nbd_malloc(sizeof(write_rec_t) * txn->writes_size * 2);
        memcpy(w, txn->writes, txn->writes_size * sizeof(write_rec_t));
        txn->writes_size *= 2;
        nbd_free(txn->writes);
        txn->writes = w;
    }
    int i = txn->writes_count++;
    txn->writes[i].key = key;
    txn->writes[i].rec = update;
}
