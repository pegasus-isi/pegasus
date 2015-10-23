#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 DAXFILE"
	exit 1
fi

DAXFILE=$1


# This command tells Pegasus to plan the workflow contained in 
# dax file passed . The planned  workflow will be stored in a relative directory
# starting with your username. The execution site is "PegasusVM" 
# --input-dir tells Pegasus to pick up inputs for the workflow from that directory
# --output-dir tells Pegasus to place the outputs in that directory
pegasus-plan  --dax $DAXFILE  --output-dir ./outputs --input-dir ./input \
             --sites PegasusVM --submit
