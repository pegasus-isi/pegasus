#!/bin/bash

KICKSTART=../pegasus-kickstart

function run_test {
    echo "Running" $KICKSTART "$@"
    $KICKSTART "$@" >/dev/null 2>&1
    return $?
}

run_test ./lotsofprocs.sh && echo "OK"
run_test -B 10000 -t ./lotsofprocs.sh && echo "OK"
run_test -B 10000 ./lotsofprocs.sh && echo "OK"

run_test -I bindate.arg && echo "OK"

# It should fail if we have anything after -I fn
run_test -I bindate.arg -B 2 || echo "OK"

