###################################################################################################
# Written by Josh Dybnis and released to the public domain, as explained at
# http://creativecommons.org/licenses/publicdomain
###################################################################################################
# Makefile for building programs with whole-program interfile optimization
###################################################################################################
OPT	   := -fwhole-program -combine -03 #-DNDEBUG
CFLAGS := -g -Wall -Werror -std=c99 -m64 $(OPT) #-DENABLE_TRACE
INCS   := $(addprefix -I, include)
TESTS  := output/ll_test output/sl_test output/ht_test output/rcu_test
EXES   := $(TESTS)

RUNTIME_SRCS  := runtime/runtime.c runtime/rcu.c runtime/lwt.c runtime/mem.c
MAP_SRCS      := map/map.c map/nstring.c 
TEST_SRCS     := $(RUNTIME_SRCS) $(MAP_SRCS)
rcu_test_SRCS := $(TEST_SRCS) test/rcu_test.c
txn_test_SRCS := $(TEST_SRCS) txn/txn.c map/hashtable.c 
ll_test_SRCS  := $(TEST_SRCS) test/ll_test.c map/list.c 
sl_test_SRCS  := $(TEST_SRCS) test/sl_test.c map/skiplist.c 
ht_test_SRCS  := $(TEST_SRCS) test/ht_test.c map/hashtable.c test/CuTest.c

tests: $(TESTS)

###################################################################################################
# build and run tests
###################################################################################################
test: $(addsuffix .log, $(TESTS))
	@echo > /dev/null

$(addsuffix .log, $(TESTS)) : %.log : %
	@echo "Running $*" && $* | tee $*.log

###################################################################################################
# Rebuild an executable if any of it's source files need to be recompiled
#
# Note: Calculating dependencies as a side-effect of compilation is disabled. There is a bug in
# 		gcc. Compilation fails when -MM -MF is used and there is more than one source file.
#		Otherwise "-MM -MT $@.d -MF $@.d" should be part of the command line for the compile.
#
#       Also, when calculating dependencies -combine is removed from CFLAGS because of another bug 
# 		in gcc. It chokes when -MM is used with -combine.
###################################################################################################
$(EXES): output/% : output/%.d makefile
	gcc $(CFLAGS:-combine:) $(INCS) -MM -MT $@ $($*_SRCS) > $@.d
	gcc $(CFLAGS) $(INCS) -o $@ $($*_SRCS)

###################################################################################################
# tags file for vi
###################################################################################################
tags:
	ctags -R .

###################################################################################################
#
###################################################################################################
clean:
	rm -rfv output/*

###################################################################################################
# dummy rule for boostrapping dependency files
###################################################################################################
$(addsuffix .d, $(EXES)) : output/%.d :

-include $(addsuffix .d, $(EXES))

.PHONY: clean test tags
