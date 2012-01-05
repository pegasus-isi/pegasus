#!/bin/bash

##
#  Copyright 2007-2011 University Of Southern California
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
#
# This is a launcher script for pegasus lite local jobs
#
# Author: Karan Vahi <vahi@isi.edu>
# Author: Mats Rynge <rynge@isi.edu>
#

set -e 

check_predefined_variables() {
    #purpose: checks for variables that need to be predefined.
    #         The variables are PEGASUS_SUBMIT_DIR and JOBSTATE_LOG

    if [ "X${_CONDOR_SCRATCH_DIR}" = "X" ]; then
	echo "ERROR: _CONDOR_SCRATCH_DIR was not set" 1>&2
	exit 1
    fi

}

check_predefined_variables

dir=$_CONDOR_SCRATCH_DIR

#sanity check on arguments
if [ $# -lt 1 ] ; then
    echo "pegasus-lite-local requires path to executable followed by arguments";
    exit 1
fi
     
executable=$1
cd $dir
shift 
args=$@

#transfer any input files if required
if [ "X${_PEGASUS_TRANSFER_INPUT_FILES}" != "X" ]; then
    #split files on ,
    IFS=, read -a FILES <<< "$_PEGASUS_TRANSFER_INPUT_FILES" 

    for file in "${FILES[@]}";do
	#echo "FILES NEED TO BE TRANSFERRED $file"
	cp $file $dir
    done
    
fi

#execute the executable with the args
#cat is used to connect the stdin
cat - | $executable "$@"

