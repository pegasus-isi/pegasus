#!/bin/bash
#
# File:     moab_status.sh
#
# Author:   Gideon Juve (gideon@isi.edu)
# Author:   David Rebatto (David.Rebatto@mi.infn.it)
#
# Description: Return a classad describing the status of a Moab job
#
# Copyright 2015 University of Southern California.
# Copyright (c) Members of the EGEE Collaboration. 2004.
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

. $(dirname $0)/blah_load_config.sh

if [ -n "$job_registry" ]; then
   ${blah_sbin_directory}/blah_job_registry_lkup $@
   exit 0
fi

getwn=""
getcreamport=""
while getopts "wn" arg
do
    case "$arg" in
        w) getwn="yes" ;;
        n) getcreamport="yes" ;;
        -) break ;;
        ?) echo "Usage: $0 [-w] [-n]"
           exit 1 ;;
    esac
done

shift $(($OPTIND - 1))

for job in "$@" ; do
    jobid=$(echo $job | cut -d/ -f3)
    staterr=/tmp/${jobid}_staterr
    result=$(${moab_binpath}/checkjob -v $jobid 2>$staterr)
    checkjob_exit_code=$?
    result=$(echo "$result" | awk '
BEGIN {
    current_job = ""
    current_wn = ""
    current_js = ""
    exitcode = 0
}

/SystemJID: / {
    current_job = $2
}

/Task Distribution: / {
    current_wn = $3
}

/State: / {
    current_js = $2
}

/Completion Code:/ {
    exitcode = $3
}

END {
    if (current_js ~ "Deferred") {jobstatus = 1}
    if (current_js ~ "NotQueued") {jobstatus = 1}
    if (current_js ~ "Unknown") {jobstatus = 1}
    if (current_js ~ "Idle") {jobstatus = 1}
    if (current_js ~ "Starting") {jobstatus = 2}
    if (current_js ~ "Suspended") {jobstatus = 2}
    if (current_js ~ "Running") {jobstatus = 2}
    if (current_js ~ "Removed") {jobstatus = 3}
    if (current_js ~ "Completed") {jobstatus = 4}
    if (current_js ~ "Vacated") {jobstatus = 4}
    if (current_js ~ "Hold") {jobstatus = 5}

    print "[BatchJobId=\"" current_job "\";"
    if (jobstatus == 2 || jobstatus == 4) {
        print "WorkerNode=\"" current_wn "\";"
    }
    print "JobStatus=" jobstatus ";"
    if (jobstatus == 4) {
        print "ExitCode=" exitcode ";"
    }
    print "]"
}
')
    errout=$(cat $staterr)
    rm -f $staterr 2>/dev/null

    if [ -z "$errout" ] ; then
        echo "0"$result
        retcode=0
    elif [ "$checkjob_exit_code" -eq "153" ] ; then
        # If the job has disappeared, assume it's completed 
        # (same as globus)
        echo "0[BatchJobId=\"$jobid\";JobStatus=4;ExitCode=0]"
        retcode=0
    else
        echo "1ERROR: Job not found"
        retcode=1
    fi
done

exit 0
