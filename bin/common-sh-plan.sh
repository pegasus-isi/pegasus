#!/bin/bash
#
# Common shell script that is sourced by Pegasus generated shell script, 
# when the SHELL code generator is used.
# $Id$
#

check_predefined_variables() {
    #purpose: checks for variables that need to be predefined.
    #         The variables are PEGASUS_SUBMIT_DIR and JOBSTATE_LOG

    if [ "X${PEGASUS_SUBMIT_DIR}" = "X" ]; then
	echo "ERROR: Set your PEGASUS_SUBMIT_DIR variable" 1>&2
	exit 1
    fi

    if [ "X${JOBSTATE_LOG}" = "X" ]; then
	echo "ERROR: Set your JOBSTATE_LOG variable" 1>&2
	exit 1
    fi

}


create_jobstate_log_entry() {
    # purpose: creates a jobstate log entry
    # paramtr: $jobname (IN): the name of the job to execute
    #          $state (IN):    the state in which the job is
    #         
    #         
    # returns: the entry for the jobstate.log file in ENTRY variable
    

    #1239666049 create_dir_blackdiamond_0_isi_viz SUBMIT 3758.0 isi_viz -
    jobstate=$JOBSTATE_LOG
    jobname=$1
    state=$2
    iso_date=`date "+%s"`
    ENTRY="$iso_date  $jobname  $state - local"
    echo $ENTRY >> $jobstate
}


execute_job() {
    # purpose: executes a job in a subshell
    # paramtr: $jobname (IN): the name of the job to execute
    #          $dir (IN):     the directory in which to execute the job
    #          $exec (IN):    the executable to be invoked
    #          $args (IN):    the arg string for the executable
    #          $stdin (IN):   the stdin for the job. Pass "" if no stdin.
    #          ...   (IN):    key=value pair for the evnvironment
    #                         variable to set for the job
    #         
    # returns:

    #sanity check
    check_predefined_variables
    if [ $# -lt 5 ] ; then
	echo "execute_job requires at a minimum 5 arguments"
	exit 1
    fi
      
    jobname=$1
    dir=$2
    exec=$3
    args=$4
    stdin=$5

    shift 5

    create_jobstate_log_entry  $jobname SUBMIT
    create_jobstate_log_entry  $jobname EXECUTE
    
    #execute each job in a sub shell
    #we dont want environment being clobbered
    (
	cd $dir
	
	#go through all the environment variables passed
	#as arguments and set them in the environment for
	#the executable to be invoked
	while [ $1 ]; do
	    env=$1
	    #echo "Env passed is $env"
	    key=`echo $env | awk -F"="  '{print $1}'`;
	    value=`echo $env | awk -F"=" '{print $2}'`;
	    
	    export $key=$value
	    #echo "key is $key value is $value"
	    shift
	done;

	echo "DEBUG: Executing JOB $exec $args" 1>&2
	jobout="${PEGASUS_SUBMIT_DIR}/${jobname}.out"
	joberr="${PEGASUS_SUBMIT_DIR}/${jobname}.err"

	if [ "X${stdin}" = "X" ]; then
	    #execute the job without setting the stdin
	    $exec $args 1> $jobout 2> $joberr

	else
	    #execute the job without setting the stdin
	    $exec $args 0<$stdin 1> $jobout 2> $joberr
	fi


    )
    status=$?
    
    echo "DEBUG: JOB $jobname Returned with $status" 1>&2
    return $status
    #exitcode $status
}


check_exitcode() {
    # purpose: checks a job exitcode and creates appropriate jobstate entries.
    #          On error exits the program
    # paramtr: $jobname (IN): the name of the job 
    #          $status (IN):  the status with which job executed

    #sanity check
    check_predefined_variables

    jobstate=$JOBSTATE_LOG
    jobname=$1
    status=$2

    create_jobstate_log_entry $jobname JOB_TERMINATED


    if [ $status -ne 0 ] ; then
        create_jobstate_log_entry $jobname JOB_FAILURE
	echo "INTERNAL *** SHELL_SCRIPT_FINISHED $status ***" >> $jobstate
	echo "ERROR: JOB $jobname failed with status $status" 1>&2
	exit $status
    else
	create_jobstate_log_entry  $jobname JOB_SUCCESS
    fi
    return
}

#if [ "X${PEGASUS_HOME}" = "X" ]; then
#    echo "ERROR: Set your PEGASUS_HOME variable" 1>&2
#    exit 1
#fi
