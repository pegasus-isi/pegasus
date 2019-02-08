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

function skip_test {
    echo "Skipping" "$@"
}

function transfer {
    $TRANSFER_LOCATION --max-attempts=1 "$@" >$TEST_DIR/test.out 2>$TEST_DIR/test.err
    RC=$?
    return $RC
}

function transfer_with_kickstart {
    $KICKSTART_LOCATION $TRANSFER_LOCATION "$@" >$TEST_DIR/test.out 2>$TEST_DIR/test.err
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

function test_containers {
    rm -f *-image.tar.gz
    if ! (transfer --file containers.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    # check docker->singularity file
    if ! (file singularity-docker-image.tar.gz | grep "/usr/bin/env run-singularity") >/dev/null 2>&1; then
        echo "singularity-docker-image.tar.gz is not in Singularity format"
        return 1
    fi
    rm -f *-image.tar.gz
    return 0
}

function test_symlink {
    rm -rf deep
    if ! (transfer --symlink --file symlink.in); then
        echo "ERROR: pegasus-transfer exited non-zero"
        return 1
    fi
    if [ ! -L deep/index.html ]; then
        echo "ERROR: deep/index.html is not a symlink"
        return 1
    fi
    rm -rf deep
    return 0
}

function test_symlink_should_fail {
    rm -rf deep
    # we expect transfer to fail here
    if (transfer --symlink --file symlink-fail.in); then
        echo "ERROR: pegasus-transfer did not fail as expected"
        return 1
    fi
    rm -rf deep
    return 0
}

function test_pull_back_integrity {
    (cd pull_back_integrity && rm -f remote* && transfer --file pullback.in && rm -f remote*)
}

function test_integrity_kickstart_large {
    export NUM_TRANSFERS=1000
    rm -rf kickstart_large
    mkdir kickstart_large
    cd kickstart_large
    # generate some data
    dd if=/dev/urandom of=input.data bs=1k count=1 >/dev/null 2>&1
    # generate a large number of transfers
    echo "[" >transfers.in
    for TID in `seq 1 $NUM_TRANSFERS`; do
        cat <<EOF >>transfers.in
 { "type": "transfer",
   "lfn": "file_$TID.data",
   "linkage": "input",
   "generate_checksum": true, 
   "id": $TID,
   "src_urls": [
     { "site_label": "local", "url": "file://$PWD/input.data", "priority": 100 }
   ],
   "dest_urls": [
     { "site_label": "local", "url": "file://$PWD/file_$TID.data" }
   ] }
EOF
        if [ $TID -lt $NUM_TRANSFERS ]; then
            echo "," >>transfers.in
        fi
    done
    echo "]" >>transfers.in

    transfer_with_kickstart --threads=50 --file transfers.in

    # make a copy of the kickstart record
    cp ../test.out kickstart-record.txt

    # make sure we have a full record
    if ! (tail -n 1 kickstart-record.txt | grep '</invocation>') >/dev/null 2>&1; then
        echo "Incomplete kickstart record"
        cd ..
        return 1
    fi

    cd ..
    rm -rf kickstart_large
    return 0
}

export TEST_DIR=`pwd`

cd ../../../bin
rm -f pegasus-transfer pegasus-integrity
ln -s pegasus-python-wrapper pegasus-transfer
ln -s pegasus-python-wrapper pegasus-integrity

export PATH=`pwd`:$PATH
export TRANSFER_LOCATION=`pwd`/pegasus-transfer

cd $TEST_DIR

export KICKSTART_LOCATION=`cd ../../../src/tools/pegasus-kickstart && pwd`/pegasus-kickstart

# we require kickstart
(cd ../../../src/tools/pegasus-kickstart && make) >/dev/null 2>&1

export KICKSTART_INTEGRITY_DATA=ks.integrity.$$
rm -f $KICKSTART_INTEGRITY_DATA

# RUN THE TESTS
run_test test_integrity
run_test test_local_cp
run_test test_integrity_local_cp
#if (docker image list && singularity --version) >/dev/null 2>&1; then
#    run_test test_containers
#else
    skip_test test_containers
#fi
run_test test_symlink
run_test test_symlink_should_fail
run_test test_pull_back_integrity
run_test test_integrity_kickstart_large

# cleanup
rm -f $KICKSTART_INTEGRITY_DATA index.html data.txt


