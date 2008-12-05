/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#include "common.h"
#include "txn.h"
#include "mem.h"
#include "skiplist.h"

#define UNDETERMINED_VERSION 0
#define INITIAL_WRITES_SIZE  4

typedef enum { UPDATE_TYPE_PUT, UPDATE_TYPE_DELETE } update_type_t;

typedef struct update_rec update_rec_t;

struct update_rec {
    update_type_t type;
    uint64_t value;
    uint64_t version;
    update_rec_t *prev; // a previous update
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

static map_t *active_ = NULL;

void txn_init (void) {
    active_ = map_alloc(&sl_map_impl, NULL);
}

// Validate the updates for <key>. Validation fails for a key we have written to if there is a 
// write committed newer than our read version.
static txn_state_e tm_validate_key (txn_t *txn, void *key) {
    
    update_rec_t *update = (update_rec_t *) map_get(txn->map, key);
    for (; update != NULL; update = update->prev) {
        uint64_t writer_version = update->version;
        if (writer_version <= txn->rv)
            return TXN_VALIDATED;

        // If the version is tagged, it means it is a pointer to a transaction in progress.
        if (IS_TAGGED(writer_version)) {

            // Skip aborted transactions.
            if (EXPECT_FALSE(writer_version == TAG_VALUE(0)))
                continue;

            // Skip our own updates.
            txn_t *writer = (txn_t *)STRIP_TAG(writer_version);
            if (writer == txn)
                continue;

            writer_version = writer->wv;
            if (writer_version <= txn->rv && writer_version != UNDETERMINED_VERSION)
                return TXN_VALIDATED;

            txn_state_e writer_state = writer->state;
            if (EXPECT_FALSE(writer_state == TXN_ABORTED))
                continue;

            // Help validate <writer> if it is a committing transaction that might cause us to 
            // abort. However, if the <writer> has a later version than us we can safely ignore its
            // updates. This protocol ensures a deterministic resolution to every conflict, and 
            // avoids infinite ping-ponging between validating two conflicting transactions.
            if (writer_state == TXN_VALIDATING && (writer_version < txn->wv || 
                                                   writer_version == UNDETERMINED_VERSION)) {
                writer_state = txn_validate(writer);
            }

            if (writer_state == TXN_VALIDATED)
                return TXN_ABORTED;
        }

        return TXN_ABORTED;
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

    // aquire the read version for txn.
    do {
        txn->rv = version_;

        uint64_t old_count;
        uint64_t temp = 0;
        do {
            old_count = temp;
            temp = (uint64_t)map_cas(active_, (void *)txn->rv, old_count, old_count + 1);
        } while (temp != old_count);

        if (txn->rv == version_)
            break;

        temp = 1;
        do {
            old_count = temp;
            temp = map_cas(active_, (void *)txn->rv, old_count, old_count - 1);
        } while (temp != old_count);
    } while (1);

    return txn;
}

void txn_abort (txn_t *txn) {

    int i;
    for (i = 0; i < txn->writes_count; ++i) {
        update_rec_t *update = (update_rec_t *)txn->writes[i].rec;
        update->version = TAG_VALUE(0);
    }

    nbd_defer_free(txn->writes);
    nbd_defer_free(txn);
}

txn_state_e txn_commit (txn_t *txn) {

    assert(txn->state == TXN_RUNNING);
    txn->state = TXN_VALIDATING;
    txn_state_e state = txn_validate(txn);

    // Detach <txn> from its updates.
    uint64_t wv = (txn->state == TXN_ABORTED) ? TAG_VALUE(0) : txn->wv;
    int i;
    for (i = 0; i < txn->writes_count; ++i) {
        update_rec_t *update = (update_rec_t *)txn->writes[i].rec;
        update->version = wv;
    }

    nbd_defer_free(txn->writes);
    nbd_defer_free(txn);

    return state;
}

// Get most recent committed version prior to our read version.
uint64_t tm_get (txn_t *txn, void *key) {

    // Iterate through update records associated with <key> to find the latest committed version. 
    // We can use the first matching version. Older updates always come later in the list.
    update_rec_t *update = (update_rec_t *) map_get(txn->map, key);
    for (; update != NULL; update = update->prev) {
        uint64_t writer_version = update->version;
        if (writer_version < txn->rv)
            return update->value;

        // If the version is tagged, it means that it is not a version number, but a pointer to an
        // in progress transaction.
        if (IS_TAGGED(update->version)) {
            txn_t *writer = (txn_t *)STRIP_TAG(writer_version);

            if (writer == txn)
                return update->type == UPDATE_TYPE_DELETE ? DOES_NOT_EXIST : update->value;

            // Skip updates from aborted transactions.
            txn_state_e writer_state = writer->state;
            if (EXPECT_FALSE(writer_state == TXN_ABORTED))
                continue;

            if (writer_state == TXN_VALIDATING) {
                writer_state = txn_validate(writer);
            }

            if (writer_state == TXN_VALIDATED && writer->wv <= txn->rv && writer->wv != UNDETERMINED_VERSION)
                return update->type == UPDATE_TYPE_DELETE ? DOES_NOT_EXIST : update->value;
        }
    }
    return DOES_NOT_EXIST;
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
        update->prev = (update_rec_t *) map_get(txn->map, key);
        update_prev = (uint64_t)update->prev;
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
