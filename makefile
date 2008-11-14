###################################################################################################
# Written by Josh Dybnis and released to the public domain, as explained at
# http://creativecommons.org/licenses/publicdomain
#
###################################################################################################
# Makefile for building programs with whole-program interfile optimization 
###################################################################################################
OPT	   := -fwhole-program -combine -03 #-DNDEBUG
CFLAGS := -g -Wall -Werror -std=c99 -m64 -fnested-functions #$(OPT) #-DENABLE_TRACE 
INCS   := $(addprefix -I, include)
TESTS  := output/rcu_test output/list_test output/ht_test
EXES   := $(TESTS) output/txn_test

RUNTIME_SRCS   := runtime/runtime.c runtime/rcu.c runtime/lwt.c runtime/mem.c 
TEST_SRCS      := $(RUNTIME_SRCS) 
rcu_test_SRCS  := $(TEST_SRCS)
list_test_SRCS := $(TEST_SRCS) struct/list.c
ht_test_SRCS   := $(TEST_SRCS) struct/ht.c test/ht_test.c test/CuTest.c
txn_test_SRCS  := $(TEST_SRCS) struct/ht.c txn/txn.c

tests: $(TESTS) 

txn: output/txn_test

###################################################################################################
# Run the tests
###################################################################################################
test: $(addsuffix .log, $(TESTS))
	@echo > /dev/null

$(addsuffix .log, $(TESTS)) : %.log : % 
	@echo "Running $*" && $* | tee $*.log

###################################################################################################
# Rebuild an executable if any of it's source files need to be recompiled 
#
# Note: Calculating dependancies as a side-effect of compilation is disabled. There is a bug in 
# 		gcc. Compilation fails when -MM -MF is used and there is more than one source file.
#		-MM -MT $@.d -MF $@.d
###################################################################################################
$(EXES): output/% : output/%.d makefile
	gcc $(CFLAGS) $(INCS) -DMAKE_$* -o $@ $($*_SRCS)

###################################################################################################
# Build tags file for vi
###################################################################################################
tags:
	ctags -R .

###################################################################################################
# 
###################################################################################################
clean:
	rm -rfv output/*

.PHONY: clean test tags

###################################################################################################
# Generate the dependencies lists for each executable
#
# Note: -combine is removed from CFLAGS because of a bug in gcc. The compiler chokes when
# 		-MM is used with -combine.
###################################################################################################
$(addsuffix .d, $(EXES)) : output/%.d :
	gcc $(CFLAGS:-combine:) $(INCS) -DMAKE_$* -MM -MT $@ $($*_SRCS) > $@

-include $(addsuffix .d, $(EXES))
