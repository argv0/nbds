/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include "common.h"
#include "txn.h"
#include "mem.h"
#include "skiplist.h"

#define UNDETERMINED_VERSION 0
#define ABORTED_VERSION      TAG_VALUE(0)
#define INITIAL_WRITES_SIZE  4

typedef enum { UPDATE_TYPE_PUT, UPDATE_TYPE_DELETE } update_type_t;

typedef struct update_rec update_rec_t;

struct update_rec {
    update_type_t type;
    uint64_t value;
    uint64_t version;
    update_rec_t *next; // an earlier update
};

typedef struct write_rec {
    void *key;
    update_rec_t *rec; 
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
    
    update_rec_t *update = (update_rec_t *) map_get(txn->map, key);
    for (; update != NULL; update = update->next) {

        // If the update's version is not tagged it means the update is committed.
        //
        // We can stop at the first committed record we find that is at least as old as our read version. All 
        // the other committed records following it will be older. And all the uncommitted records following it 
        // will eventually conflict with it and abort.
        if (!IS_TAGGED(update->version)) 
            return (update->version <= txn->rv) ? TXN_VALIDATED : TXN_ABORTED;

        // If the update's version is tagged then either the update was aborted or the the version number is 
        // actually a pointer to a running transaction's txn_t.

        // Skip aborted transactions.
        if (EXPECT_FALSE(update->version == ABORTED_VERSION))
            continue;

        // The update's transaction is still in progress. Access its txn_t.
        txn_t *writer = (txn_t *)STRIP_TAG(update->version);
        if (writer == txn)
            continue; // Skip our own updates.
        txn_state_e writer_state = writer->state;

        // Any running transaction will only be able to aquire a wv greater than ours. A transaction changes its 
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

static update_rec_t *alloc_update_rec (void) {
    update_rec_t *u = (update_rec_t *)nbd_malloc(sizeof(update_rec_t));
    memset(u, 0, sizeof(update_rec_t));
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

    // aquire the read version for txn. must be careful to avoid a race
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
        update_rec_t *update = (update_rec_t *)txn->writes[i].rec;
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
        update_rec_t *update = (update_rec_t *)txn->writes[i].rec;
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

    // Iterate through update records associated with <key> to find the latest committed version prior to our
    // read version. 
    update_rec_t *update = (update_rec_t *) map_get(txn->map, key);
    for (; update != NULL; update = update->next) {

        // If the update's version is not tagged it means the update is committed.
        if (!IS_TAGGED(update->version)) {
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
        txn_t *writer = (txn_t *)STRIP_TAG(update->version);
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

    if (EXPECT_FALSE(update == NULL))
        return DOES_NOT_EXIST;

    // collect some garbage
    update_rec_t *next = update->next;
    if (next != NULL) {
        uint64_t min_active_version = (uint64_t)sl_min_key(active_);
        if (next->version < min_active_version) {
            next = SYNC_SWAP(&update->next, NULL);
            while (next != NULL) {
                update = next;
                next = NULL;
                if (update->next != NULL) {
                    next = SYNC_SWAP(&update->next, NULL);
                }
                nbd_free(update);
            }
        }
    }
    
    return update->value;
}

void tm_set (txn_t *txn, void *key, uint64_t value) {

    // create a new update record
    update_rec_t *update = alloc_update_rec();
    update->type = UPDATE_TYPE_PUT;
    update->value = value;
    update->version = TAG_VALUE((uint64_t)txn);

    // push the new update record onto <key>'s update list
    uint64_t update_prev;
    do {
        update->next = (update_rec_t *) map_get(txn->map, key);
        update_prev = (uint64_t)update->next;
    } while (map_cas(txn->map, key, update_prev, (uint64_t)update) != update_prev);

    // add <key> to the write set for commit-time validation
    if (txn->writes_count == txn->writes_size) {
        write_rec_t *w = nbd_malloc(sizeof(write_rec_t) * txn->writes_size * 2);
        memcpy(w, txn->writes, txn->writes_size * sizeof(write_rec_t));
        txn->writes_size *= 2;
    }
    int i = txn->writes_count++;
    txn->writes[i].key = key;
    txn->writes[i].rec = update;
}
