#!/bin/bash

KICKSTART=../pegasus-kickstart

function run_test {
    echo "Running" "$@"
    "$@"
    if [ $? -eq 0 ]; then
        echo "OK"
        return 0
    else
        echo "ERROR"
        exit 1
    fi
}

function kickstart {
    OUTPUT=$($KICKSTART "$@")
    RC=$?
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT" | xmllint - >/dev/null
        RC=$?
    fi
    return $RC
}

function lotsofprocs {
    kickstart ./lotsofprocs.sh
    return $?
}

function lotsofprocs_trace {
    kickstart -t ./lotsofprocs.sh
    return $?
}

function lotsofprocs_buffer {
    kickstart -B 10000 ./lotsofprocs.sh
    return $?
}

function lotsofprocs_trace_buffer {
    kickstart -B 10000 -t ./lotsofprocs.sh
    return $?
}

# This should succeed
function argfile {
    kickstart -I bindate.arg
    return $?
}

# kickstart should fail if we have anything after -I fn
function argfile_after {
    kickstart -I bindate.arg -B 2 2>/dev/null
    if [ $? -ne 0 ]; then
        return 0
    else
        return 1
    fi
}

# The ampersands should be propery quoted
function xmlquote_ampersand {
    kickstart -B 3000 /bin/cat ampersand.txt
    return $?
}

function test_full {
    kickstart -f /bin/date
    return $?
}

function test_flush {
    kickstart -F /bin/date 2>/dev/null
    return $?
}

function test_executable {
    kickstart -X /bin/date
    return $?
}

function test_longarg {
    A=$(for i in $(seq 1 5000); do echo -n 'c'; done)
    kickstart /bin/echo $A
    return $?
}

function test_longarg_file {
    kickstart -I long.arg
    return $?
}

# RUN THE TESTS
run_test lotsofprocs
run_test lotsofprocs_buffer
if [ `uname -s` == "Linux" ]; then
    run_test lotsofprocs_trace
    run_test lotsofprocs_trace_buffer
fi
run_test argfile
run_test argfile_after
run_test xmlquote_ampersand
run_test test_full
run_test test_flush
run_test test_executable
run_test test_longarg
run_test test_longarg_file

