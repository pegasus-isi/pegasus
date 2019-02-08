#!/bin/bash


function run_test {
    echo "Running" "$@"
    "$@"
    if [ $? -eq 0 ]; then
        echo "OK"
        rm test.err test.out
        return 0
    else
        cat test.err test.out
        echo "ERROR"
        rm test.err test.out
        exit 1
    fi
}

function integrity {
    $INTEGRITY_LOCATION "$@" >$TEST_DIR/test.out 2>$TEST_DIR/test.err
    RC=$?
    return $RC
}

function test_generate_single {
    if ! (integrity --generate=data.1); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_generate_multiple {
    if ! (integrity --generate=data.1:data.2); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_generate_xml_single {
    if ! (integrity --generate-xml=data.1); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_generate_xml_multiple {
    if ! (integrity --generate-xml=foo1=data.1:foo2=data.2); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_verify_single {
    if ! (integrity --verify=data.1); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_verify_multiple {
    if ! (integrity --print-timings --verify=data.1:foo.2=data.2); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

function test_verify_multiple_stdin {
    if ! (echo "data.1:foo.2=data.2" | integrity --print-timings --verify=stdin); then
        echo "ERROR: pegasus-integrity exited non-zero"
        return 1
    fi
    return 0
}

export TEST_DIR=`pwd`

export INTEGRITY_LOCATION="python "`cd ../../.. && pwd`/lib/pegasus/python/Pegasus/cli/pegasus-integrity.py

# RUN THE TESTS
run_test test_generate_single
run_test test_generate_multiple
run_test test_generate_xml_single
run_test test_generate_xml_multiple
run_test test_verify_single
run_test test_verify_multiple
run_test test_verify_multiple_stdin



