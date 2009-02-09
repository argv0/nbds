#include <stdio.h>
#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "txn.h"
#include "map.h"
#include "hashtable.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

void test1 (CuTest* tc) {
    map_t *map = map_alloc(&MAP_IMPL_HT, NULL);
    txn_t *t1 = txn_begin(map);
    txn_t *t2 = txn_begin(map);
    map_key_t k1 = (map_key_t)1;
    txn_map_set(t1, k1, 2);
    txn_map_set(t1, k1, 3);
    ASSERT_EQUAL( DOES_NOT_EXIST, txn_map_get(t2, k1) );
    txn_map_set(t2, k1, 4);
    ASSERT_EQUAL( 3, txn_map_get(t1, k1) );
    ASSERT_EQUAL( 4, txn_map_get(t2, k1) );
    ASSERT_EQUAL( TXN_VALIDATED, txn_commit(t2));
    ASSERT_EQUAL( TXN_ABORTED,   txn_commit(t1));
}

int main (void) {

    lwt_set_trace_level("x3h3");

    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test1);
    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return 0;
}
