#!/bin/bash

KICKSTART=../pegasus-kickstart

function run_test {
    echo "Running" $KICKSTART "$@"
    $KICKSTART "$@"
    return $?
}

($KICKSTART ./lotsofprocs.sh | xmllint - >/dev/null 2>&1) && echo "OK"
($KICKSTART -B 10000 -t ./lotsofprocs.sh | xmllint - >/dev/null 2>&1) && echo "OK"
($KICKSTART -B 10000 ./lotsofprocs.sh | xmllint - >/dev/null 2>&1) && echo "OK"

# This should succeed
($KICKSTART -I bindate.arg | xmllint - >/dev/null 2>&1) && echo "OK"

# It should fail if we have anything after -I fn
!($KICKSTART -I bindate.arg -B 2 >/dev/null 2>&1) && echo "OK"

# This should return properly escaped XML
($KICKSTART -B 3000 /bin/cat ampersand.txt | xmllint - >/dev/null 2>&1) && echo "OK"

