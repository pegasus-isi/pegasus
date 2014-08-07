#!/bin/bash

KICKSTART=../pegasus-kickstart

if [ -z "$(which xmllint)" ]; then
    echo "ERROR: xmllint not found"
    exit 1
fi

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
    xmllint test.out >/dev/null
    if [ $? -ne 0 ]; then
        cat test.err test.out
        echo "Invalid XML"
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
    kickstart -X /bin/date
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

function test_xmlquote {
    kickstart cat xmlquote.txt
    rc=$?
    if ! [[ $(cat test.out) =~ "Jens Vöckler" ]]; then
        echo "Expected UTF-8 umlaut in output"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "&quot;Gideon Juve&quot;" ]]; then
        echo "Expected quotes to be escaped"
        return 1
    fi
    if ! [[ $(cat test.out) =~ "&lt;some xml=&quot;goes&quot;&gt;here&lt;/some&gt;" ]]; then
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
run_test test_xmlquote
run_test test_all_stdio
run_test test_bad_stdio

