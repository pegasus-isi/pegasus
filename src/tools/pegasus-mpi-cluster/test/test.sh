#!/bin/bash
set -e

PMC=./pegasus-mpi-cluster

function run_test {
    local test=$1
    echo "Running $test..."

    # Set up
    rm -f test/*.dag.*
    rm -rf test/scratch

    if $test; then
        echo "OK"
    else
        echo "FAILED ($?)"
        exit 1
    fi

    # Clean up
    rm -f test/*.dag.*
    rm -rf test/scratch
}

function test_help {
    # Should print message and exit with 1
    if ! mpiexec -np 2 $PMC >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

function test_help_no_mpi {
    # It should work even if we don't have an MPI context
    if ! $PMC >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Make sure it requires at least one worker
function test_one_worker_required {
    result=$(mpiexec -np 1 $PMC test/diamond.dag 2>&1)
    RC=$?
    if [ $RC -eq 0 ]; then
        echo "$result"
        return 1
    fi
    if ! [[ "$result" =~ "At least one worker process is required" ]]; then
        echo "$result"
        return 1
    fi
}

# Make sure it will run the simple diamond dag
function test_run_diamond {
    output=$(mpiexec -np 2 $PMC -s test/diamond.dag 2>&1)
    RC=$?
    
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
}

# Make sure we can redirect task stdout/stderr to /dev/null
function test_out_err {
    mpiexec -np 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null >/dev/null 2>&1
    RC=$?
    
    if [ $RC -ne 0 ]; then
        return 1
    fi
}

# Make sure the rescue file gets generated where we want it
function test_rescue_file {
    RESCUE=$(mktemp test/diamond.dag.rescue.XXXXXX)
    
    mpiexec -np 2 $PMC -s test/diamond.dag -o /dev/null -e /dev/null -r $RESCUE >/dev/null 2>&1
    RC=$?
    
    LOG=$(cat $RESCUE)
    
    if [ $RC -ne 0 ]; then
        return 1
    fi
    
    CORRECT=$(printf "\nDONE A\nDONE B\nDONE C\nDONE D\n")
    
    if [ "$LOG" != "$CORRECT" ]; then
        echo "$LOG"
        echo "ERROR Rescue file was incorrect"
        return 1
    fi
}

# Make sure we can run host scripts
function test_host_script {
    OUTPUT=$(mpiexec -np 2 $PMC -v -s test/sleep.dag -o /dev/null -e /dev/null --host-cpus 4 --host-script test/hostscript.sh 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Host script test failed"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Worker 1: Launching host script" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Host script was not launched"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "HOSTSCRIPT stdout" ]] && ! [[ "$OUTPUT" =~ "HOSTSCRIPT stderr" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Host script did not generate the right output"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Host script exited with status 0" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Host script test failed"
        return 1
    fi
}

# Make sure a failing host script causes the job to fail
function test_fail_script {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/sleep.dag -o /dev/null -e /dev/null --host-cpus 4 --host-script /bin/false 2>&1)
    RC=$?
    
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Fail script test failed"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Host script failed" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Fail script test failed"
        return 1
    fi
}

# Make sure we can kill the process group of the host script when it forks children
function test_fork_script {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/sleep.dag -o /dev/null -e /dev/null --host-cpus 4 --host-script test/forkscript.sh -v 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Fork script test failed"
        return 1
    fi
    
    if [[ "$OUTPUT" =~ "Unable to terminate host script process group" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Fork script test failed"
        return 1
    fi
}

# Make sure host scripts time out after 60 seconds
function test_hang_script {
    echo "This should take 60 seconds..."
    
    OUTPUT=$(mpiexec -np 2 $PMC -s test/sleep.dag -o /dev/null -e /dev/null --host-cpus 4 --host-script test/hangscript.sh -v 2>&1)
    RC=$?
    
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Hang script test failed"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Host script timed out" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Hang script test failed"
        return 1
    fi
}

# Make sure memory scheduling works in the normal case
function test_memory_limit {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/memory.dag -o /dev/null -e /dev/null --host-memory 100 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Host memory test failed"
        return 1
    fi
}

# Make sure a failure occurs if there isn't enough memory to run a task
function test_insufficient_memory {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/memory.dag -o /dev/null -e /dev/null --host-memory 99 2>&1)
    RC=$?
    
    # This test should fail because 99 MB isn't enough to run the tasks in the DAG
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Insufficient memory test failed (1)"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "FATAL ERROR: No host is capable of running task" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Insufficient memory test failed (2)"
        return 1
    fi
}

# Make sure strict limits work in the normal case
function test_strict_limits {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/memory.dag --strict-limits 2>&1)
    RC=$?
    
    # This test should pass because 100 MB should be enough to run the tasks in the DAG
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Memory limit test failed"
        return 1
    fi
}

# Test to make sure a failure occurs if a task uses more memory than it is allowed
function test_strict_limits_failure {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/limit.dag --strict-limits 2>&1)
    RC=$?
    
    # This test should fail because 1 MB isn't enough to run the task in the DAG
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Memory limit test failed"
        return 1
    fi
}

# Test to make sure cpus are scheduled properly
function test_cpus_limit {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/cpus.dag -o /dev/null -e /dev/null --host-memory 100 --host-cpus 2 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Host CPUs test failed"
        return 1
    fi
}

# Make sure a failure occurs if no host can run one of the tasks
function test_insufficient_cpus {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/cpus.dag -o /dev/null -e /dev/null --host-cpus 1 2>&1)
    RC=$?
    
    # This test should fail because 1 CPU isn't enough to run the tasks in the DAG
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Insufficient CPUs test failed (1)"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "FATAL ERROR: No host is capable of running task" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Insufficient CPUs test failed (2)"
        return 1
    fi
}

# Make sure retries work
function test_tries {
    OUTPUT=$(mpiexec -np 2 $PMC -s test/tries.dag -o /dev/null -e /dev/null -t 3 2>&1)
    RC=$?
    
    # This test should fail because task B will fail twice
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Tries test failed"
        return 1
    fi
    
    if [ $(echo "$OUTPUT" | grep "Task B failed" | wc -l) -ne 5 ]; then
        echo "$OUTPUT"
        echo "ERROR: Tries test failed"
        return 1
    fi
    
    if [ $(echo "$OUTPUT" | grep "Task C failed" | wc -l) -ne 3 ]; then
        echo "$OUTPUT"
        echo "ERROR: Tries test failed"
        return 1
    fi
}

# Make sure that task priorities are honored
function test_priority {
    OUTPUT=$(mpiexec -np 2 $PMC -v -v -s test/priority.dag -o /dev/null -e /dev/null --host-cpus 2 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Priority test failed"
        return 1
    fi
    
    DESIRED="[trace] Scheduling task G
[trace] Scheduling task I
[trace] Scheduling task D
[trace] Scheduling task E
[trace] Scheduling task O
[trace] Scheduling task N"
    
    ACTUAL=$(echo "$OUTPUT" | grep "Scheduling task ")
    
    if [ "$ACTUAL" != "$DESIRED" ]; then
        echo "$OUTPUT"
        echo "Actual: $ACTUAL"
        echo "Desired: $DESIRED"
        echo "ERROR: Priority test failed"
        return 1
    fi
}

# Make sure that PMC aborts if the workflow takes too long
function test_max_wall_time {
    OUTPUT=$(mpiexec -np 3 $PMC -s test/walltime.dag --host-cpus 2 --max-wall-time 0.05 2>&1)
    RC=$?
    
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Max wall time test failed on exitcode"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Aborting workflow" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Max wall time test failed on aborting"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "TASK stdout" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Max wall time test failed on task stdout"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "TASK stderr" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Max wall time test failed on task stderr"
        return 1
    fi
    
    ELAPSED=$(echo "$OUTPUT" | grep "Wall time:" | cut -d" " -f4)
    if [ $(echo "$ELAPSED > 4.0" | bc -q) -eq 1 ]; then
        echo "$OUTPUT"
        echo "Ran in $ELAPSED seconds"
        echo "ERROR: Max wall time test took too long"
        return 1
    fi
}

# Make sure that the resource log gets generated correctly
function test_resource_log {
    OUTPUT=$(mpiexec -np 3 $PMC -s test/sleep.dag --host-cpus 4 --host-memory 100 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Resource log test failed"
        return 1
    fi
    
    LINES=$(cat test/sleep.dag.resource | wc -l)
    
    if [ $LINES -ne 3 ]; then
        echo "ERROR: Expected 3 lines in the resource log got $LINES"
        return 1
    fi
}

# Make sure that stdio is appended to existing files
function test_append_stdio {
    echo "onefish my stdout" > test/diamond.dag.out.1
    echo "twofish my stderr" > test/diamond.dag.err.1
    
    OUTPUT=$(mpiexec -np 2 $PMC -v test/diamond.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Append test failed"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "onefish my stdout" ]]; then
        echo "$OUTPUT"
        echo "ERROR: stdout not appended"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "twofish my stderr" ]]; then
        echo "$OUTPUT"
        echo "ERROR: stderr not appended"
        return 1
    fi
}

# Make sure I/O forwarding works
function test_forward {
    OUTPUT=$(mpiexec -np 2 $PMC -v test/forward.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Forward test failed"
        return 1
    fi
    
    FOO=$(grep "Variable FOO" test/forward.dag.foo | wc -l)
    if [ $? -ne 0 ] || [ $FOO -ne 2 ]; then
        echo "$OUTPUT"
        echo "ERROR: Forward test failed (foo problem)"
        return 1
    fi
    
    BAR=$(grep "Variable BAR" test/forward.dag.bar | wc -l)
    if [ $? -ne 0 ] || [ $BAR -ne 2 ]; then
        echo "$OUTPUT"
        echo "ERROR: Forward test failed (bar problem)"
        return 1
    fi
}

# Make sure I/O forwarding failures cause task to fail
function test_forward_fail {
    OUTPUT=$(mpiexec -np 2 $PMC -v test/forward_fail.dag 2>&1)
    RC=$?

    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Forward failure test failed"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Task A failed due to collective I/O errors" ]]; then
        echo "$OUTPUT"
        echo "ERROR: Forward failure test failed"
        return 1
    fi
}

# Make sure I/O forwarding works with files
function test_file_forward {
    OUTPUT=$(mpiexec -np 2 $PMC -v test/file_forward.dag 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: File forward test failed"
        return 1
    fi
    
    FOO=$(grep "foo" test/forward.dag.foo | wc -l)
    if [ $? -ne 0 ] || [ $FOO -ne 2 ]; then
        echo "$OUTPUT"
        echo "ERROR: File forward test failed (foo problem)"
        return 1
    fi
    
    BAR=$(grep "bar" test/forward.dag.bar | wc -l)
    if [ $? -ne 0 ] || [ $BAR -ne 2 ]; then
        echo "$OUTPUT"
        echo "ERROR: File forward test failed (bar problem)"
        return 1
    fi
}

# Make sure file forwarding fails properly
function test_file_forward_fail {
    OUTPUT=$(mpiexec -np 2 $PMC -v test/file_forward_fail.dag 2>&1)
    RC=$?
    
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: File forward fail test failed"
        return 1
    fi
    
    if ! [[ "$OUTPUT" =~ "Task A: ./test is not a file" ]]; then
        echo "$OUTPUT"
        echo "ERROR: File forward fail test failed"
        return 1
    fi
}

function test_per_task_stdio {
    mkdir -p test/scratch
    cp test/diamond.dag test/scratch/
    
    OUTPUT=$(mpiexec -np 2 $PMC -v --per-task-stdio test/scratch/diamond.dag 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: Per task stdio test failed"
        return 1
    fi
    
    NFILES=$(ls test/scratch/{A,B,C,D}.{out,err}.000 | wc -l)
    if [ $NFILES -ne 8 ]; then
        echo "$OUTPUT"
        echo "ERROR: Per task stdio test failed"
        echo "NFILES=$NFILES"
        return 1
    fi
}

function test_jobstate_log {
    mkdir -p test/scratch
    cp test/diamond.dag test/scratch/
    
    OUTPUT=$(mpiexec -np 2 $PMC -v --jobstate-log test/scratch/diamond.dag 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: jobstate.log test failed"
        return 1
    fi
    
    if ! [ -f "test/scratch/jobstate.log" ]; then
        echo "$OUTPUT"
        echo "ERROR: jobstate.log file was not created"
        return 1
    fi
}

function test_monitord_hack {
    mkdir -p test/scratch
    cp test/diamond.dag test/scratch/
    
    OUTPUT=$(mpiexec -np 2 $PMC -v --monitord-hack test/scratch/diamond.dag 2>&1)
    RC=$?
    
    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: monitord hack test failed"
        return 1
    fi
    
    if [ $(ls test/scratch/*.out.000 | wc -l) -ne 4 ]; then
        echo "$OUTPUT"
        echo "ERROR: --monitord-hack did not cause --per-task-stdio"
        return 1
    fi
    
    if ! [ -f "test/scratch/diamond.dag.dagman.out" ]; then
        echo "$OUTPUT"
        echo "ERROR: .dagman.out file was not created"
        return 1
    fi
}

function test_monitord_hack_failure {
    mkdir -p test/scratch
    cp test/fail.dag test/scratch/
    
    OUTPUT=$(mpiexec -np 2 $PMC -v --monitord-hack test/scratch/fail.dag 2>&1)
    RC=$?
    
    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: monitord hack failure test failed"
        return 1
    fi
    
    if ! [ -f "test/scratch/fail.dag.dagman.out" ]; then
        echo "$OUTPUT"
        echo "ERROR: .dagman.out file was not created"
        return 1
    fi
}

function test_maxfds {
    OUTPUT=$(mpiexec -np 2 $PMC --maxfds 10 -v test/maxfd.dag 2>&1)
    RC=$?

    if [ $RC -eq 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: maxfds test failed"
        return 1
    fi
}

function test_keep_affinity {
    OUTPUT=$(mpiexec -np 2 -bind-to core $PMC --keep-affinity test/cpuset.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: --keep-affinity test failed"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "physcpubind: 1" ]]; then
        echo "ERROR: clear_affinity test failed"
        return 1
    fi
}


function test_clear_affinity {
    OUTPUT=$(mpiexec -np 2 -bind-to core $PMC test/cpuset.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: clear_affinity test failed"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "physcpubind: 0 1" ]]; then
        echo "ERROR: clear_affinity test failed"
        return 1
    fi
}

function test_complex_args {
    OUTPUT=$(mpiexec -np 2 $PMC test/complexargs.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: complex_args test failed"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "I count 3 arguments" ]]; then
        echo "ERROR: complex_args test failed"
        return 1
    fi
}

function test_PM848 {
    OUTPUT=$(mpiexec -n 4 $PMC --host-cpus 8 test/PM848.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: PM848 test failed"
        return 1
    fi
}

function test_affinity_env {
    OUTPUT=$(mpiexec -n 8 $PMC --host-cpus 8 test/PM953.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: affinity test failed"
        return 1
    fi

    if ! [[ "$OUTPUT" =~ "eight 0,1,2,3,4,5,6,7" ]]; then
        echo "$OUTPUT"
        echo "ERROR: affinity test did not contain the right output"
        return 1
    fi
}

function test_PM954 {
    OUTPUT=$(mpiexec -n 2 $PMC test/PM954.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: PM954 test failed"
        return 1
    fi

    for var in "PMC_CPUS=3" "PMC_MEMORY=17" "PMC_TASK=foobar" "PMC_RANK=1" "PMC_HOST_RANK=0"; do
        if ! [[ "$OUTPUT" =~ "$var" ]]; then
            echo "$OUTPUT"
            echo "ERROR: Missing '$var' in output"
            return 1
        fi
    done
}

function test_PM953 {
    OUTPUT=$(mpiexec -n 2 $PMC --set-affinity test/PM953-2.dag 2>&1)
    RC=$?

    if [ $RC -ne 0 ]; then
        echo "$OUTPUT"
        echo "ERROR: PM953 test failed"
        return 1
    fi

    if ! echo "$OUTPUT" | grep -E "^physcpubind: 0 1 2 $" >/dev/null; then
        echo "$OUTPUT"
        echo "ERROR: PM953 affinity test did not contain the right output"
        return 1
    fi
}

# If a test name was specified, then run just that test
if ! [ -z "$*" ]; then
    run_test "$@"
    exit 0
fi

run_test ./test-strlib
run_test ./test-tools
run_test ./test-dag
run_test ./test-log
run_test ./test-engine
run_test ./test-fdcache
run_test ./test-protocol
run_test ./test-scheduler
run_test test_PM954
run_test test_help
run_test test_help_no_mpi
run_test test_one_worker_required
run_test test_run_diamond
run_test test_out_err
run_test test_rescue_file
run_test test_memory_limit
run_test test_insufficient_memory
run_test test_strict_limits
run_test test_cpus_limit
run_test test_insufficient_cpus
run_test test_tries
run_test test_priority
run_test test_host_script
run_test test_fail_script
run_test test_fork_script
run_test test_resource_log
run_test test_append_stdio
run_test test_forward
run_test test_forward_fail
run_test test_file_forward
run_test test_file_forward_fail
run_test test_per_task_stdio
run_test test_jobstate_log
run_test test_monitord_hack
run_test test_monitord_hack_failure
run_test test_max_wall_time
run_test test_hang_script
run_test test_maxfds
run_test test_complex_args
run_test test_PM848
run_test test_affinity_env

# setrlimit is broken on Darwin, so the strict limits test won't work
if [ $(uname -s) != "Darwin" ]; then
    run_test test_strict_limits_failure
fi

function count_cpus {
    if [ -f /proc/cpuinfo ]; then
        grep processor /proc/cpuinfo | wc -l
    else
        echo 1
    fi
}

# For the CPU affinity tests we need numactl
if ! [ -z "$(which numactl)" ]; then

    # For these two tests we need 2 cpus
    if [ $(count_cpus) -gt 1 ]; then
        run_test test_clear_affinity
        run_test test_keep_affinity
    fi

    # For this test we need 4 or more
    if [ $(count_cpus) -ge 4 ]; then
        run_test test_PM953
    fi
fi

