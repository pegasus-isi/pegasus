#!/bin/bash
#
# File:     moab_hold.sh
#
# Author:   Gideon Juve (gideon@isi.edu)
# Author:   David Rebatto (David.Rebatto@mi.infn.it)
#
# Description: Place a Moab job on hold
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

jobid=$(echo $1 | cut -d/ -f3)
result=$(${moab_binpath}/checkjob $jobid | awk '
/State:/ {
    print $2
}
')
#currently only holding idle or waiting jobs is supported
if [ "$2" ==  "1" ] ; then # I guess $2 is 1 when you want to force?
    ${moab_binpath}/mjobctl -h $jobid
elif [ "$result" == "Idle" ] ; then
    ${moab_binpath}/mjobctl -h $jobid
else
    echo "unsupported for this job status" >&2
    exit 1
fi

