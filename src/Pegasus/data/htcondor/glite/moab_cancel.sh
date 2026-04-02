#!/bin/bash
#
# File:     moab_cancel.sh
#
# Author:   Gideon Juve (gideon@isi.edu)
# Author:   David Rebatto (David.Rebatto@mi.infn.it)
#
# Description: Cancel a Moab job
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

jnr=$#
jc=0
for job in "$@"; do
    jobid=$(echo $job | cut -d/ -f3)
    cmdout=$(${moab_binpath}/mjobctl -c $jobid 2>&1)
    retcode=$?
    # If the job is already completed or no longer in the queue,
    # treat it as successfully deleted.
    if echo "$cmdout" | grep -q 'Unknown Job' ; then
        retcode=0
    fi
    if [ "$retcode" == "0" ]; then
        if [ "$jnr" == "1" ]; then
            echo " 0 No\\ error"
        else
            echo .$jc" 0 No\\ error"
        fi
    else
        escaped_cmdout=$(echo $cmdout|sed "s/ /\\\\\ /g")
        if [ "$jnr" == "1" ]; then
            echo " $retcode $escaped_cmdout"
        else
            echo .$jc" $retcode $escaped_cmdout"
        fi
    fi
    jc=$(($jc+1))
done

