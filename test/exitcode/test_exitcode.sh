#!/bin/bash

cd `dirname $0`
cwd=`pwd`
tests=`dirname $cwd`
home=`dirname $tests`
bin=$home/bin

function exitcode {
	echo "Testing $2..."
	result=`$bin/pegasus-exitcode --no-rename $2 2>&1`
	rc=$?
	if [ $rc -ne $1 ]; then
		echo "$result" >&2
		echo "ERROR" >&2
		exit 1
	else
		echo "OK"
	fi
}

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
