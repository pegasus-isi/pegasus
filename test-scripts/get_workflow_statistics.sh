#!/bin/sh
# Script that is used for running the workflow statistics for a completed workflow.
#
# args:
# 1: workflow directory
# 2: workflow ID
#
WORKFLOW_DIR=$1

WORKFLOW_ID=$2

pegasus-statistics -s all $WORKFLOW_DIR > /dev/null 2>&1 
if [ $? -eq 0 ]; then

  TRANSFER_TIME=`sed -n "/# $WORKFLOW_ID/,/# All/p" $WORKFLOW_DIR/statistics/breakdown.txt | grep "pegasus::pegasus-transfer" | awk '{print $8}'`
  CLEANUP_TIME=`sed -n "/# $WORKFLOW_ID/,/# All/p" $WORKFLOW_DIR/statistics/breakdown.txt | grep "pegasus::cleanup" | awk '{print $8}'`

#  echo "Transfer Time=$TRANSFER_TIME"
#  echo "Cleanup Time=$CLEANUP_TIME"
   echo "$TRANSFER_TIME,$CLEANUP_TIME"
else
#  echo "Error generating statistics."
  exit 1
fi

