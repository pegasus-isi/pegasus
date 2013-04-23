#!/bin/bash

KICKSTART=../pegasus-kickstart

function run_test {
    echo "Running" $KICKSTART "$@"
    $KICKSTART "$@" > /dev/null
    if [ $? -eq 0 ]; then
        echo "OK"
    fi
}

run_test ./lotsofprocs.sh
run_test -B 10000 -t ./lotsofprocs.sh
run_test -B 10000 ./lotsofprocs.sh

