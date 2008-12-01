#include <stdio.h>
#include "CuTest.h"

#include "common.h"
#include "runtime.h"
#include "txn.h"

#define ASSERT_EQUAL(x, y) CuAssertIntEquals(tc, x, y)

void test1 (CuTest* tc) {
    txn_t *tm = txn_begin(TXN_READ_WRITE, TXN_REPEATABLE_READ, MAP_TYPE_LIST);
    tm_set(tm, "abc", 4, 2);
    tm_set(tm, "abc", 4, 3);
    ASSERT_EQUAL( 3, tm_get(tm, "abc", 4) );
    ASSERT_EQUAL( TXN_VALIDATED, txn_commit(tm));
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

