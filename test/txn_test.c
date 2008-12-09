#include <stdio.h>
#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "txn.h"
#include "map.h"
#include "hashtable.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

void test1 (CuTest* tc) {
    map_t *map = map_alloc(&ht_map_impl, NULL);
    txn_t *t1 = txn_begin(TXN_REPEATABLE_READ, map);
    txn_t *t2 = txn_begin(TXN_REPEATABLE_READ, map);
    void *k1 = (void *)1;
    tm_set(t1, k1, 2);
    tm_set(t1, k1, 3);
    ASSERT_EQUAL( DOES_NOT_EXIST, tm_get(t2, k1) );
    tm_set(t2, k1, 4);
    ASSERT_EQUAL( 3, tm_get(t1, k1) );
    ASSERT_EQUAL( 4, tm_get(t2, k1) );
    ASSERT_EQUAL( TXN_VALIDATED, txn_commit(t2));
    ASSERT_EQUAL( TXN_ABORTED,   txn_commit(t1));
}

int main (void) {

    nbd_init();
    txn_init();

    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test1);
    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return 0;
}
