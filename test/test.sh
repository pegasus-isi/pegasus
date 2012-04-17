#!/bin/sh

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

# Run unit tests
run_test ./test-strlib
run_test ./test-dag
run_test ./test-log
run_test ./test-engine

PMC=./pegasus-mpi-cluster

function test_help {
	# Should print message and exit with 1
	if ! mpiexec -n 2 $PMC 2>/dev/null; then
		return 0
	else
		return 1
	fi
}

function test_two_workers_required {
	result=$(mpiexec -n 1 $PMC test/diamond.dag 2>&1)
	if [ $? ] && [ "$result" == "At least one worker process is required" ]; then
		return 0
	else
		echo $result
		return 1
	fi
}

function test_run_diamond {
	output=$(mpiexec -n 2 $PMC -s test/diamond.dag 2>/dev/null)
	RC=$?
	
	rm test/diamond.dag.rescue.???
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	n=$(echo "$output" | grep "status=0" | wc -l)
	
	if [ $n -ne 4 ]; then
		return 1
	else
		return 0
	fi
}

# Run integration tests
run_test test_help
run_test test_two_workers_required
run_test test_run_diamond