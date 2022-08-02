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
    
    path_addon="/bin:/usr/bin:/usr/local/bin:/opt/local/bin"
    if [ "X${PATH}" = "X" ]; then
	echo "WARNING: PATH not set. Adding default values $path_addon" 1>&2
	export PATH=$path_addon
    else
	echo "DEBUG: Adding default values to PATH - $path_addon" 1>&2
	export PATH=$PATH:$path_addon
    fi
}

check_predefined_variables

dir=$_CONDOR_SCRATCH_DIR

if [ "${_PEGASUS_EXECUTE_IN_INITIAL_DIR}" = "true" ];then
    dir=$_PEGASUS_INITIAL_DIR
fi

# PM-1029 prepend PATH with PEGASUS_BIN_DIR 
if [ "x$PEGASUS_BIN_DIR" != "x" ]; then
    export PATH="$PEGASUS_BIN_DIR:$PATH"
fi


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
	if [[ $file == /* ]] ; then
	    #file starts with /
	    cp $file $dir
	else
	    #file is relative grab from initialdir
	    #check for initialdir
	    if [ "X${_PEGASUS_INITIAL_DIR}" = "X" ]; then
		echo "ERROR: _PEGASUS_INITIAL_DIR not populated" 1>&2
		exit 1;
	    fi
	    file=$_PEGASUS_INITIAL_DIR/$file
	    cp $file $dir
	fi
	
    done
    
fi

#execute the executable with the args
if [ "X${_PEGASUS_CONNECT_STDIN}" = "X" ]; then
    #dont connect stdin
    $executable "$@"
else
    #cat is used to connect the stdin
    cat - | $executable "$@"
fi

# transfer any output files back to the Pegasus initial dir 
if [ "X${_PEGASUS_TRANSFER_OUTPUT_FILES}" != "X" ]; then

    #check for initialdir
    if [ "X${_PEGASUS_INITIAL_DIR}" = "X" ]; then
	echo "ERROR: _PEGASUS_INITIAL_DIR not populated" 1>&2
	exit 1;
    fi

    outputdir=$_PEGASUS_INITIAL_DIR

    #split files on ,
    IFS=, read -a FILES <<< "$_PEGASUS_TRANSFER_OUTPUT_FILES" 

    for file in "${FILES[@]}";do
	#echo "FILES NEED TO BE TRANSFERRED $file"
	cp $file $outputdir
    done
    
fi

# PM-1875, PM-1877 handle output remaps for deep LFN's
if [ "X${_PEGASUS_TRANSFER_OUTPUT_REMAPS}" != "X" ]; then
    dir=$_PEGASUS_INITIAL_DIR

    #split files on ;
    echo $_PEGASUS_TRANSFER_OUTPUT_REMAPS
    IFS=';' read -a FILE_PAIRS <<< "$_PEGASUS_TRANSFER_OUTPUT_REMAPS"
    for filepair in "${FILE_PAIRS[@]}";do
        files=($(echo $filepair | tr "=" "\n"))
	# copy the lfn basename to the deep lfn
        mv  $dir/${files[0]}  $dir/${files[1]}
    done
fi
