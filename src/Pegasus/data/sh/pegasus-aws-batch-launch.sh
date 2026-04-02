#!/bin/bash

set -e 


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

function usage () {
  if [ "${#@}" -ne 0 ]; then
    echo "* ${*}"
    echo
  fi
  cat <<ENDUSAGE
Usage:
Launches a script or executable in the container.
The script:executable is pulled down from a S3 bucket
identified by the evn variable PEGASUS_AWS_BATCH_BUCKET

Required environment varialbes to be set:
export PEGASUS_AWS_BATCH_BUCKET=s3://my-bucket
export PEGASUS_JOB_NAME="my-job"

${BASENAME} exec-basename[ <script arguments> ]

ENDUSAGE
  exit 2

}

function pegasus_batch_log()
{
    TS=`/bin/date +'%F %H:%M:%S'`
    echo "$TS: $1"  1>&2
}

function pegasus_batch_final_exit()
{
    rc=1

    # the exit code of the lite script should reflect the exit code 
    if [ "x$task_ec" = "x" ];then
        pegasus_batch_log "task_ec is missing - did the user task fail?"
    else
        if [ $task_ec != 0 ];then
            pegasus_batch_log "User task failed with exitcode $task_ec"
        fi
        rc=$task_ec
    fi

    echo "PegasusAWSBatchLaunch: exitcode $rc" 1>&2

    # cat the stderr back to stdout
    echo $TASK_STDERR_SEPARATOR 
    if (test -e  ${task_stderr_file}) ; then
	cat ${task_stderr_file}
    fi

    exit $rc
}


# Sets up the task stderr to point to a file
function setup_task_stderr()
{
    # Close STDERR FD
    exec 2>&-

    # Open STDERR to file for writes
    exec 2>$task_stderr_file

    
}

start_dir=`pwd`

# Check that necessary programs are available
which aws >/dev/null 2>&1 || error_exit "Unable to find AWS CLI executable in the container."
which unzip >/dev/null 2>&1 || error_exit "Unable to find unzip executable in the container."

PEGASUS_LITE_COMMON_FILE=pegasus-lite-common.sh

if [ "X${task_stderr_file}" = "X" ]; then
    # compute from the job name
    if [ "X${PEGASUS_JOB_NAME}" = "X" ]; then
	error_exit "The env variable PEGASUS_JOB_NAME not specified"
    fi
    task_stderr_file=${PEGASUS_JOB_NAME}.err
fi

setup_task_stderr
echo "########################[Pegasus AWS Batch] Starting up ########################" 1>&2
pegasus_batch_log "task stderr set to file $task_stderr_file"

# sanity checks
if [ "X${PEGASUS_AWS_BATCH_BUCKET}" = "X" ]; then
    usage "The env variable PEGASUS_AWS_BATCH_BUCKET not specified"
fi
# strip out trailing / from bucket name if any
PEGASUS_AWS_BATCH_BUCKET=`echo ${PEGASUS_AWS_BATCH_BUCKET} | sed -E "s/(.*)(\/)$/\1/"`

scheme="$(echo "${PEGASUS_AWS_BATCH_BUCKET}" | cut -d: -f1)"
if [ "${scheme}" != "s3" ]; then
  usage "PEGASUS_AWS_BATCH_BUCKET environment variable value should start with s3://"
fi

if [ $# -eq 0 ]; then
    usage "Need to pass the name of executable to run"
fi

# single exit function for success and error
trap 'pegasus_batch_final_exit' EXIT HUP INT QUIT TERM

# Use first argument as script name and pass the rest to the script
pegasus_batch_log "Number of args passed to pegasus-aws-batch - $# "
script="${1}"; shift

# for staged credentials, download the credentials from s3 if specified, and set
# the credential env variable to downloaded path 
for base in X509_USER_PROXY PEGASUS_CREDENTIALS S3CFG BOTO_CONFIG SSH_PRIVATE_KEY IRODS_ENVIRONMENT_FILE GOOGLE_PKCS12 ; do
    for key in `(env | grep -i ^$base | sed 's/=.*//') 2>/dev/null`; do
        eval val="\$$key"
	scheme="$(echo "${val}" | cut -d: -f1)"
	if [ "${scheme}" == "s3" ]; then
	    # pull down the credential from S3
	    cred=`echo ${val} | sed -E "s/s3:\/\/.*\/(.*)$/\1/"`
	    aws s3 cp "${val}" "./${cred}" || error_exit "Failed to download credential ${val} to ${cred}"

	    eval $key=`pwd`/"${cred}"
            eval val="\$$key"
            pegasus_batch_log "Expanded \$$key to $val"
        fi
        if [ -e "$val" ]; then
            chmod 0600 "$val"
        fi
    done
done


aws s3 cp "${PEGASUS_AWS_BATCH_BUCKET}/${PEGASUS_LITE_COMMON_FILE}"  "./${PEGASUS_LITE_COMMON_FILE}" 1>&2 || error_exit "Failed to download S3 file  ${PEGASUS_LITE_COMMON_FILE} from bucket ${PEGASUS_AWS_BATCH_BUCKET}"
aws s3 cp "${PEGASUS_AWS_BATCH_BUCKET}/${script}"  "./${script}" 1>&2 || error_exit "Failed to download S3 file ${script} from bucket ${PEGASUS_AWS_BATCH_BUCKET}"
chmod +x ${PEGASUS_LITE_COMMON_FILE_FILE} ${script}

pegasus_batch_log "Launching ${script} with $# arguments from directory ${start_dir}"

set +e
./${script} "${@}"
task_ec=$?
