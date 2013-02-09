#!/bin/sh

# this file goes to the bin directory in the directory 
# pointed to by
# condor_config_val GLITE_LOCATION
#
# e.g 
#  [vahi@ccg-testing2 run0007]$ condor_config_val  GLITE_LOCATION
#  /usr/libexec/condor/glite
#
#  [vahi@ccg-testing2 run0007]$ ls -lh /usr/libexec/condor/glite/
#
# [vahi@ccg-testing2 run0007]$ ls -lh /usr/libexec/condor/glite/bin/pbs_local_submit_attributes.sh
# -rwxr-xr-x 1 root root 112 Feb  7 17:24 /usr/libexec/condor/glite/bin/pbs_local_submit_attributes.sh
# [vahi@ccg-testing2 run0007]$ 
#
#

if [ ! -z $NODES ]; then
echo "#PBS -l nodes=${NODES}"
fi
if [ ! -z $PROCS ]; then
echo "#PBS -l ncpus=${PROCS}"
fi
if [ ! -z $WALLTIME ]; then
echo "#PBS -l walltime=${WALLTIME}"
fi
if [ ! -z $PROJECT ]; then
echo "#PBS -A ${PROJECT}"
fi
if [ ! -z $JOBNAME ]; then
echo "#PBS -N ${JOBNAME}"
fi
if [ ! -z $PASSENV ]  && [ $PASSENV == 1 ]; then
echo "#PBS -V"
fi
if [ ! -z $MYENV ] ; then
echo "#PBS -v ${MYENV}"
fi

