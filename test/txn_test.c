#include <stdio.h>
#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "txn.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

void test1 (CuTest* tc) {
    map_t *map = map_alloc(MAP_TYPE_LIST, NULL);
    txn_t *t1 = txn_begin(TXN_READ_WRITE, TXN_REPEATABLE_READ, map);
    txn_t *t2 = txn_begin(TXN_READ_WRITE, TXN_REPEATABLE_READ, map);
    tm_set(t1, "abc", 2);
    tm_set(t1, "abc", 3);
    ASSERT_EQUAL( DOES_NOT_EXIST, tm_get(t2, "abc") );
    tm_set(t2, "abc", 4);
    ASSERT_EQUAL( 3, tm_get(t1, "abc") );
    ASSERT_EQUAL( 4, tm_get(t2, "abc") );
    ASSERT_EQUAL( TXN_VALIDATED, txn_commit(t2));
    ASSERT_EQUAL( TXN_ABORTED, txn_commit(t1));
}

int main (void) {

    nbd_init();

    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test1);
    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return 0;
}

