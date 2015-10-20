#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 DAXFILE"
	exit 1
fi

DAXFILE=$1

# This command tells Pegasus to plan the workflow contained in 
# "diamond.dax" using the config file "pegasus.conf". The planned
# workflow will be stored in a relative directory named "submit".
# The execution site is "local" and the output directory where the
# outputs are placed ./outputs .
# --force tells Pegasus not to prune anything from the workflow, and
# --nocleanup tells Pegasus not to generate cleanup jobs.
pegasus-plan --conf pegasus.conf -d $DAXFILE --dir submit \
	--force --sites local --output-dir ./outputs  --cleanup none \
        --submit
