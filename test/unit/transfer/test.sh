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

function transfer {
    ../../../bin/pegasus-transfer "$@" >test.out 2>test.err
    RC=$?
    return $RC
}

function test_integrity {
    rm -f .pegasus-integrity-ks.xml
    if ! (transfer --file web-to-local.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    # was .pegasus-integrity-ks.xml created?
    if [ ! -e .pegasus-integrity-ks.xml ]; then
        echo "ERROR: expected file .pegasus-integrity-ks.xml was not created"
        return 1
    fi
    # make sure it has a statinfo entry
    if ! (grep statinfo .pegasus-integrity-ks.xml) >/dev/null 2>&1; then
        echo "ERROR: .pegasus-integrity-ks.xml does not contain a statinfo entry"
        return 1
    fi
    rm -f .pegasus-integrity-ks.xml
    return 0
}

function test_local_cp {
    transfer --file cp.in
    return $?
}

function test_integrity_local_cp {
    rm -f .pegasus-integrity-ks.xml
    if ! (transfer --file cp.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    # was .pegasus-integrity-ks.xml created?
    if [ ! -e .pegasus-integrity-ks.xml ]; then
        echo "ERROR: expected file .pegasus-integrity-ks.xml was not created"
        return 1
    fi
    ENTRY_COUNT=`cat .pegasus-integrity-ks.xml | grep "entry generated" | wc -l`
    if [ $ENTRY_COUNT != 1 ]; then
        echo "ERROR: .pegasus-integrity-ks.xml does not have 2 entries!"
        cat .pegasus-integrity-ks.xml
        return 1
    fi
    rm -f .pegasus-integrity-ks.xml data.txt
    return 0
}

function test_symlink {
    rm -rf deep
    transfer --symlink --file symlink.in 
    if [ ! -L deep/index.html ]; then
        echo "ERROR: deep/index.html is not a symlink"
        return 1
    fi
    rm -rf deep
    return 0
}

# make sure we start cleanly
rm -f .pegasus-integrity-ks.xml index.html data.txt

# RUN THE TESTS
run_test test_integrity
run_test test_local_cp
run_test test_integrity_local_cp
run_test test_symlink

# cleanup
rm -f .pegasus-integrity-ks.xml index.html data.txt


