#!/bin/sh
for ks in 28 24 20 16 12 8 4 0
do
    for th in 1 2 4 8 16
    do
        output/perf_test $th $ks
    done
done


