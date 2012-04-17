#!/bin/sh

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
	output=$(mpiexec -n 2 $PMC -s test/diamond.dag 2>/dev/null)
	RC=$?
	
	rm test/diamond.dag.rescue.???
	
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
	
	rm test/diamond.dag.rescue.???
	
	if [ $RC -ne 0 ]; then
		return 1
	fi
	
	return 0
}


run_test ./test-strlib
run_test ./test-dag
run_test ./test-log
run_test ./test-engine
run_test test_help
run_test test_one_worker_required
run_test test_run_diamond
run_test test_out_err
