#!/bin/bash

set -e 
#set -x 

##
#  Copyright 2007-2017 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

#
# This file contains is a launcher script for launching Pegasus jobs via
# AWS Batch, and is launched by the fetch_and_run script provided by 
# Batch
#
# Author: Karan Vahi <vahi@isi.edu>
#

TASK_STDERR_SEPARATOR="########################[AWS BATCH] TASK STDERR ########################"

BASENAME="${0##*/}"

# Standard function to print an error and exit with a failing return code
function error_exit () {
  echo "${BASENAME} - ${1}" >&2
  exit 1
}

function pegasus_batch_log()
{
    TS=`/bin/date +'%F %H:%M:%S'`
    echo "$TS: $1"  1>&2
}


# Sets up the task stderr to point to a file
function setup_task_stderr()
{
    # Close STDERR FD
    exec 2>&-

    # Open STDERR to file for writes
    exec 2>$task_stderr_file

    
}
# Check that necessary programs are available
which aws >/dev/null 2>&1 || error_exit "Unable to find AWS CLI executable."
which unzip >/dev/null 2>&1 || error_exit "Unable to find unzip executable."

PEGASUS_LITE_COMMON_FILE=pegasus-lite-common.sh
PEGASUS_LITE_JOB_SCRIPT_FILE=sample_pegasus_lite.sh

if [ "X${task_stderr_file}" = "X" ]; then
    # compute from the job name
    if [ "X${PEGASUS_JOB_NAME}" = "X" ]; then
	error_exit "The env variable PEGASUS_JOB_NAME not specified"
    fi
    task_stderr_file=${PEGASUS_JOB_NAME}.err
fi

setup_task_stderr
pegasus_batch_log "task stderr set to file $task_stderr_file"

if [ "X${PEGASUS_AWS_BATCH_BUCKET}" = "X" ]; then
    error_exit "The env variable PEGASUS_AWS_BATCH_BUCKET not specified"
fi

aws s3 cp "${PEGASUS_AWS_BATCH_BUCKET}/${PEGASUS_LITE_COMMON_FILE}" - > "./${PEGASUS_LITE_COMMON_FILE}" || error_exit "Failed to download S3 file  ${PEGASUS_LITE_COMMON_FILE}"
aws s3 cp "${PEGASUS_AWS_BATCH_BUCKET}/${PEGASUS_LITE_JOB_SCRIPT_FILE}" - > "./${PEGASUS_LITE_JOB_SCRIPT_FILE}" || error_exit "Failed to download S3 file ${PEGASUS_LITE_JOB_SCRIPT_FILE}"
chmod +x ${PEGASUS_LITE_COMMON_FILE_FILE} ${PEGASUS_LITE_JOB_SCRIPT_FILE}


set +e
./${PEGASUS_LITE_JOB_SCRIPT_FILE} "${@}" 
task_ec=$?
echo "PegasusAWSBatchLaunch: exitcode $task_ec" 1>&2

# push out the stderr back to the bucket
# aws s3 cp "./${task_stderr_file}" "${PEGASUS_AWS_BATCH_BUCKET}/${task_stderr_file}" || echo "Unable to transfer stderr file ${task_stderr_file}"

# cat the stderr back to stdout
echo $TASK_STDERR_SEPARATOR 
cat ${task_stderr_file}

exit $task_ec