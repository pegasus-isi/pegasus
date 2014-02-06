#!/bin/bash

cd `dirname $0`
cwd=`pwd`
tests=`dirname $cwd`
home=`dirname $tests`
bin=$home/bin

function stderr {
    echo "$@" >&2
}

function exitcode {
    echo "Testing $2..."
    result=`$bin/pegasus-exitcode --no-rename $2 2>&1`
    rc=$?
    if [ $rc -ne $1 ]; then
        stderr "$result"
        stderr "ERROR"
        exit 1
    else
        echo "OK"
    fi
}

function run_test {
    echo "Testing " "$@" "..."
    "$@"
    rc=$?
    if [ $rc -ne 0 ]; then
        stderr "ERROR"
        exit 1
    else
        stderr "OK"
    fi
}

function test_rename_noerrfile {
    result=$($bin/pegasus-exitcode ok.out 2>&1)
    rc=$?
    mv ok.out.000 ok.out
    if [ $rc -ne 0 ]; then
        stderr "$result"
        return 1
    fi
}

function test_failure_message_zero_exit {
    result=$($bin/pegasus-exitcode --no-rename --failure-message "Job failed" failure_message_zero_exit.out 2>&1)
    rc=$?
    if [ $rc -ne 1 ]; then
        stderr "$result"
        return 1
    fi
}

function test_success_message_failure_message {
    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" -f "Job failed" success_message_failure_message.out 2>&1)
    rc=$?
    if [ $rc -ne 1 ]; then
        echo "$result" >&2
        return 1
    fi
}

function test_success_message_missing {
    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" success_message_missing.out 2>&1)
    rc=$?
    if [ $rc -ne 1 ]; then
        echo "$result" >&2
        return 1
    fi
}

function test_success_message_present {
    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" success_message_zero_exit.out 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "$result" >&2
        return 1
    fi
}

function test_success_message_nonzero_exit {
    result=$($bin/pegasus-exitcode --no-rename --success-message "Job succeeded" success_message_nonzero_exit.out 2>&1)
    rc=$?
    if [ $rc -ne 1 ]; then
        echo "$result" >&2
        return 1
    fi
}

function test_all_success_messages_required {
    result=$($bin/pegasus-exitcode --no-rename -s "Job succeeded" -s "Successfully finished" success_message_zero_exit.out 2>&1)
    rc=$?
    if [ $rc -ne 1 ]; then
        echo "$result" >&2
        return 1
    fi
}

function unit_tests {
    echo "Running unit tests..."
    /usr/bin/env python tests.py $bin/pegasus-exitcode
    rc=$?
    if [ $rc -ne 0 ]; then
        exit 1
    fi
}

unit_tests
# exitcode expected_result outfile
exitcode 0 ok.out
exitcode 1 failed.out
exitcode 1 walltime.out
exitcode 1 zerolen.out
exitcode 0 zeromem.out
exitcode 0 cluster-none.out
exitcode 0 cluster-ok.out
exitcode 1 cluster-error.out
exitcode 1 nonzero.out
exitcode 1 signalled.out
exitcode 0 seqexec-ok.out
exitcode 1 largecode.out
exitcode 0 cluster_summary_ok.out
exitcode 1 cluster_summary_failed.out
exitcode 1 cluster_summary_stat.out
exitcode 1 cluster_summary_missing.out
exitcode 0 cluster_summary_notasks.out
exitcode 1 cluster_summary_nosucc.out
exitcode 0 cluster_summary_submitted.out
run_test test_rename_noerrfile
run_test test_failure_message_zero_exit
run_test test_success_message_failure_message
run_test test_success_message_missing
run_test test_success_message_present
run_test test_success_message_nonzero_exit
run_test test_all_success_messages_required

