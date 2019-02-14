#!/bin/bash

KICKSTART=../pegasus-kickstart

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

function kickstart {
    $KICKSTART "$@" >test.out 2>test.err
    RC=$?
    ../../../../release-tools/yaml-validator test.out >/dev/null
    if [ $? -ne 0 ]; then
        cat test.err test.out
        echo "Invalid YAML"
        exit 1
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
    kickstart /bin/date
    return $?
}

function test_longarg {
    A=$(for i in $(seq 1 5000); do echo -n 'c'; done)
    kickstart /bin/echo $A
    return $?
}

function test_longarg_file {
    cat > long.arg <<END
/bin/echo
$(dd if=/dev/zero of=/dev/stdout bs=127k count=1 2>/dev/null | tr '\0' 'c')
END
    kickstart -I long.arg
    return $?
}

function test_toolongarg_file {
    cat > toolong.arg <<END
/bin/echo
$(dd if=/dev/zero of=/dev/stdout bs=129k count=1 2>/dev/null | tr '\0' 'c')
END
    kickstart -I toolong.arg
    if [ $? -eq 0 ]; then
        return 1
    else
        return 0
    fi
}

function test_quiet {
    kickstart -q ./testquiet.sh
    RC=$?
    if [ $RC -ne 0 ]; then
        return $RC
    fi
    if [[ $(cat test.out) =~ "Some message on stderr" ]]; then
        echo "Expected no <data> for stderr"
        return 1
    fi
    if [[ $(cat test.out) =~ "Some message on stdout" ]]; then
        echo "Expected no <data> for stdout"
        return 1
    fi
    return 0
}

function test_quiet_fail {
    kickstart -q ./testquiet.sh fail
    RC=$?
    if [ $RC -eq 0 ]; then
        return 1
    fi
    if ! [[ $(cat test.out) =~ "Some message on stderr" ]]; then
        echo "Expected <data> for stderr"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "Some message on stdout" ]]; then
        echo "Expected <data> for stdout"
        return 1
    fi
    return 0
}

function test_missing_args {
    for a in i o e l n N R B L T I w W s S; do
        kickstart -$a
        if [ $? -ne 127 ]; then
            echo "ERROR on missing argument for -$a"
            return 1
        fi
    done
}

function test_quoting {
    kickstart cat xmlquote.txt
    rc=$?
    if ! [[ $(cat test.out) =~ "Jens Vöckler" ]]; then
        echo "Expected UTF-8 umlaut in output"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "\"Gideon Juve\"" ]]; then
        echo "Expected quotes to be escaped"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "<some xml=\"goes\">here</some>" ]]; then
        echo "Expected XML to be escaped"
        return 1
    fi
    return $rc
}

function test_all_stdio {
    kickstart -B all echo hello world gideon
    rc=$?
    if ! [[ $(cat test.out) =~ "hello world gideon" ]]; then
        echo "Expected all output"
        return 1
    fi
    return $rc
}

function test_bad_stdio {
    kickstart -B foo echo hello
    rc=$?
    if ! [[ $(cat test.err) =~ "Invalid -B argument" ]]; then
        echo "Expected invalid -B"
        return 1
    fi
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    return 0
}

function test_timeout_ok {
    kickstart -k 5 /bin/sleep 1
    return $?
}

function test_timeout_fail {
    kickstart -k 2 /bin/sleep 3
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    return 0
}

function test_timeout_kill {
    kickstart -k 5 -K 5 python ignoreterm.py 30
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGKILL" ]]; then
        echo "Expected SIGKILL"
        return 1
    fi
    return 0
}

function test_timeout_nokill {
    kickstart -k 1 -K 30 python ignoreterm.py 10
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if [[ $(cat test.err) =~ "sending SIGKILL" ]]; then
        echo "Did not expect SIGKILL"
        return 1
    fi
    return 0
}

function test_timeout_mainjob_cleanup {
    KICKSTART_CLEANUP="/bin/echo Cleanup job" kickstart -k 1 /bin/sleep 5
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "  cleanup:" ]]; then
        echo "Expected cleanup job to run"
        return 1
    fi
    return 0
}

function test_timeout_pre {
    KICKSTART_PREJOB="/bin/sleep 5" kickstart -k 1 /bin/echo Main job
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if [[ $(cat test.out) =~ "  mainjob:" ]]; then
        echo "Did not expect main job to run"
        return 1
    fi
    return 0
}

function test_timeout_post {
    KICKSTART_POSTJOB="/bin/sleep 5" kickstart -k 1 /bin/echo Main job
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "  mainjob:" ]]; then
        echo "Expected main job to run"
        return 1
    fi
    return 0
}

function test_timeout_cleanup {
    KICKSTART_CLEANUP="/bin/sleep 5" kickstart -k 1 /bin/echo Main job
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "Expected zero exit"
        return 1
    fi
    if [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Did not expect SIGTERM"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "  mainjob:" ]]; then
        echo "Expected main job to run"
        return 1
    fi
    return 0
}

function test_timeout_setup {
    KICKSTART_SETUP="/bin/sleep 5" KICKSTART_PREJOB="/bin/echo Pre job" kickstart -k 1 /bin/echo Main job
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.err) =~ "sending SIGTERM" ]]; then
        echo "Expected SIGTERM"
        return 1
    fi
    if [[ $(cat test.out) =~ ">Pre job" ]]; then
        echo "Did not expect pre job to run"
        return 1
    fi
    if [[ $(cat test.out) =~ ">Main job" ]]; then
        echo "Did not expect main job to run"
        return 1
    fi
    return 0
}

function test_failure_environment {
    kickstart -w /doesnotexistever /bin/echo Main job
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "Expected non-zero exit"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "  environment:" ]]; then
        echo "Expected environment"
        return 1
    fi
    return 0
}

function test_quote_env_var {
    KICKSTART_SAVE=$KICKSTART
    KICKSTART="env GIDEON\"=juve $KICKSTART"
    kickstart -f /bin/date
    rc=$?
    KICKSTART=$KICKSTART_SAVE

    if [ $rc -ne 0 ]; then
        echo "Expected job to succeed"
        return 1
    fi

    if ! [[ $(cat test.out) =~ "GIDEON\\\"\": " ]]; then
        echo "Expected environment variable name to be quoted"
        return 1
    fi

    return 0
}

function test_prepend_path {
    KICKSTART_SAVE=$KICKSTART
    KICKSTART="env PATH=/bar KICKSTART_PREPEND_PATH=/foo $KICKSTART"
    kickstart /usr/bin/env
    rc=$?
    KICKSTART=$KICKSTART_SAVE

    if [ $rc -ne 0 ]; then
        echo "Expected kickstart to succeed"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "PATH=/foo:/bar" ]]; then
        cat test.out
        echo "Expected PATH to be set with KS_PREPEND_PATH"
        return 1
    fi

    return 0
}

function test_missing_executable {
    kickstart frobnerbrob
    rc=$?

    if [ $rc -eq 0 ]; then
        echo "Expected job to fail"
        return 1
    fi

    if ! [[ $(cat test.out) =~ "No such file or directory" ]]; then
        echo "Expected no such file or directory error message"
        return 1
    fi

    return 0
}

function test_not_executable {
    kickstart long.arg
    rc=$?

    if [ $rc -eq 0 ]; then
        echo "Expected job to fail"
        return 1
    fi

    if ! [[ $(cat test.out) =~ "Permission denied" ]]; then
        echo "Expected permission denied error message"
        return 1
    fi

    return 0;
}

function test_wrapper {
    KICKSTART_WRAPPER=./wrapper.sh kickstart /bin/date
    rc=$?

    if [ $rc -ne 0 ]; then
        echo "Expected job to succeed"
        return 1
    fi

    if ! [[ $(cat test.out) =~ "Hello, Wrapper!" ]]; then
        echo "Expected wrapper output"
        return 1
    fi

    return 0;
}

function test_metadata {
    kickstart testmetadata.sh
    rc=$?

    if [ $rc -ne 0 ]; then
        echo "Expected job to succeed"
        return 1
    fi

    if ! [[ $(cat test.out) =~ "foo=bar" ]]; then
        echo "Expected metadata in output"
        return 1
    fi

    return 0;
}

function test_integrity {
    ./testintegrity.sh
    return $?
}

function test_integrity_failure {
    alias pegasus-integrity=/bin/false
    kickstart -s testintegrity.data touch testintegrity.data
    rc=$?
    unalias pegasus-integrity
    return $rc
}

function test_integrity_yaml_inc {
    # do this test multiple times
    for I in `seq 100`; do
    
        kickstart pegasus-integrity --generate-fullstat-yaml=testintegrity.data=testintegrity.data
        rc=$?
    
        if [ $rc -ne 0 ]; then
            echo "Kickstart failed to run"
            return 1
        fi
    
        # verify it has the right output
        if ! (grep '"testintegrity.data":' test.out) >/dev/null 2>&1; then
            echo "Unable to find the included integrity data in ks output"
            return 1
        fi
        if ! (grep ' sha256:' test.out) >/dev/null 2>&1; then
            echo "Unable to find the included integrity data in ks output"
            return 1
        fi
    done

    return 0
}

function test_w_with_rel_exec {
    mkdir -p subdir
    cp /bin/date subdir/my_unique_exe
    kickstart -w $PWD/subdir ./my_unique_exe
    ec=$?
    rm -rf subdir
    return $ec
}

export START_DIR=`pwd`

# make sure we start cleanly
rm -f .pegasus-integrity-ks.xml
rm -rf tempbin

# we require a PEGASUS_BIN_DIR as we depend on other Pegasus CLIs
if [ "x$PEGASUS_BIN_DIR" = "x" ]; then
    echo "Please define PEGASUS_BIN_DIR before running these tests" >&2
    exit 1
fi
export PATH=$PEGASUS_BIN_DIR:$PATH

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
run_test test_toolongarg_file
run_test test_quiet
run_test test_quiet_fail
run_test test_missing_args
run_test test_quoting
run_test test_all_stdio
run_test test_bad_stdio
run_test test_timeout_ok
run_test test_timeout_fail
run_test test_timeout_kill
run_test test_timeout_nokill
run_test test_timeout_mainjob_cleanup
run_test test_timeout_pre
run_test test_timeout_post
run_test test_timeout_cleanup
run_test test_timeout_setup
run_test test_failure_environment
run_test test_quote_env_var
run_test test_prepend_path
run_test test_missing_executable
run_test test_not_executable
run_test test_wrapper
run_test test_metadata
run_test test_integrity
run_test test_integrity_failure
run_test test_integrity_yaml_inc
run_test test_w_with_rel_exec


