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
    rm -f $KICKSTART_INTEGRITY_DATA
    if ! (transfer --file web-to-local.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    # was $KICKSTART_INTEGRITY_DATA created?
    if [ ! -e $KICKSTART_INTEGRITY_DATA ]; then
        echo "ERROR: expected file $KICKSTART_INTEGRITY_DATA was not created"
        return 1
    fi
    # make sure it has a statinfo entry
    if ! (grep statinfo $KICKSTART_INTEGRITY_DATA) >/dev/null 2>&1; then
        echo "ERROR: $KICKSTART_INTEGRITY_DATA does not contain a statinfo entry"
        return 1
    fi
    rm -f $KICKSTART_INTEGRITY_DATA
    return 0
}

function test_local_cp {
    transfer --file cp.in
    return $?
}

function test_integrity_local_cp {
    rm -f $KICKSTART_INTEGRITY_DATA
    if ! (transfer --file cp.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    # was $KICKSTART_INTEGRITY_DATA created?
    if [ ! -e $KICKSTART_INTEGRITY_DATA ]; then
        echo "ERROR: expected file $KICKSTART_INTEGRITY_DATA was not created"
        return 1
    fi
    ENTRY_COUNT=`cat $KICKSTART_INTEGRITY_DATA | grep "entry generated" | wc -l`
    if [ $ENTRY_COUNT != 1 ]; then
        echo "ERROR: $KICKSTART_INTEGRITY_DATA does not have 2 entries!"
        cat $KICKSTART_INTEGRITY_DATA
        return 1
    fi
    rm -f $KICKSTART_INTEGRITY_DATA data.txt
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

export KICKSTART_INTEGRITY_DATA=ks.integrity.$$
rm -f $KICKSTART_INTEGRITY_DATA

# RUN THE TESTS
run_test test_integrity
run_test test_local_cp
run_test test_integrity_local_cp
run_test test_symlink

# cleanup
rm -f $KICKSTART_INTEGRITY_DATA index.html data.txt


