#!/bin/bash
set -e

PMC=./pegasus-mpi-cluster

function run_test
{
	local test=$1
	echo "Running $test..."
	if $test; then 
		echo "OK"
	else
		echo "FAILED ($?)"
		exit 1
	fi
}

function test_help {
	# Should print message and exit with 1
	if ! mpiexec -n 2 $PMC 2>/dev/null; then
		return 0
	else
		return 1
	fi
}

# Make sure it requires at least one worker
function test_one_worker_required {
	result=$(mpiexec -n 1 $PMC test/diamond.dag 2>&1)
	if [ $? ] && [ "$result" == "At least one worker process is required" ]; then
		return 0
	else
		echo $result
		return 1
	fi
}

# Make sure it will run the simple diamond dag
function test_run_diamond {
	output=$(mpiexec -n 2 $PMC -s test/diamond.dag 2>&1)
	RC=$?
	
	rm -f test/diamond.dag.*
	
	if [ $RC -ne 0 ]; then
		echo "$output"
		return 1
	fi
	
	n=$(echo "$output" | grep "status=0" | wc -l)
	
	if [ $n -ne 4 ]; then
		echo "$output"
		return 1
	fi
	
	stat=$(echo "$output" | grep 'stat="ok"' | wc -l)
	if [ $stat -ne 1 ]; then
		echo "$output"
		return 1
	fi
	
	return 0
}

# Make sure we can redirect task stdout/stderr to /dev/null
function test_out_err {
	mpiexec -n 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null 2>/dev/null
	RC=$?
	
	rm -f test/diamond.dag.*
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	return 0
}

function test_rescue_file {
	RESCUE=$(mktemp rescue.XXXXXX)
	
	mpiexec -n 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null -r $RESCUE 2>/dev/null
	RC=$?
	
	LOG=$(cat $RESCUE)
	
	rm -f $RESCUE
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	CORRECT=$(printf "\nDONE A\nDONE B\nDONE C\nDONE D\n")
	
	if [ "$LOG" != "$CORRECT" ]; then
		echo "$LOG"
		echo "ERROR Rescue file was incorrect"
		return 1
	fi
	
	return 0
}

function test_host_script {
	OUTPUT=$(mpiexec -n 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null --host-script test/hostscript.sh 2>&1)
	RC=$?
	
	rm -f test/diamond.dag.*
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	if [ $(echo "$OUTPUT" | grep "Worker 1: Launching host script" | wc -l) -ne 1 ]; then
		echo "$OUTPUT"
		echo "ERROR: Host script was not launched"
		return 1
	fi
	
	if [ $(echo "$OUTPUT" | grep "HOSTSCRIPT std" | wc -l) -ne 2 ]; then
		echo "$OUTPUT"
		echo "ERROR: Host script did not generate the right output"
		return 1
	fi
	
	if [ $(echo "$OUTPUT" | grep "Host script exited with status 0" | wc -l) -ne 1 ]; then
		echo "$OUTPUT"
		echo "ERROR: Host script test failed"
		return 1
	fi
	
	return 0
}

function test_hang_script {
	OUTPUT=$(mpiexec -n 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null --host-script test/hangscript.sh 2>&1)
	RC=$?
	
	rm -f test/diamond.dag.*
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	if [ $(echo "$OUTPUT" | grep "Worker 1: Launching host script" | wc -l) -ne 1 ]; then
		echo "$OUTPUT"
		echo "ERROR: Host script was not launched"
		return 1
	fi
	
	if [ $(echo "$OUTPUT" | grep "Host script exited on signal 15" | wc -l) -ne 1 ]; then
		echo "$OUTPUT"
		echo "ERROR: Hang script test failed"
		return 1
	fi
	
	return 0
}

function test_memory_limit {
	OUTPUT=$(mpiexec -n 2 $PMC -s test/memory.dag -o /dev/null -e /dev/null --host-memory 100 2>&1)
	RC=$?
	
	rm -rf test/memory.dag.*
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	return 0
}

function test_insufficient_memory {
	OUTPUT=$(mpiexec -n 2 $PMC -s test/memory.dag -o /dev/null -e /dev/null --host-memory 99 2>&1)
	RC=$?
	
	rm -f test/memory.dag.*
	
	# This test should fail because 99 MB isn't enough to run the tasks in the DAG
	if [ $RC -ne 1 ]; then
		return 1
	fi
	
	if ! [[ "$OUTPUT" =~ "FATAL ERROR: No host is capable of running task" ]]; then
		echo "$OUTPUT"
		echo "ERROR: Insufficient memory test failed"
		return 1
	fi
	
	return 0
}

run_test ./test-strlib
run_test ./test-tools
run_test ./test-dag
run_test ./test-log
run_test ./test-engine
run_test test_help
run_test test_one_worker_required
run_test test_run_diamond
run_test test_out_err
run_test test_rescue_file
run_test test_host_script
run_test test_hang_script
run_test test_memory_limit
run_test test_insufficient_memory
