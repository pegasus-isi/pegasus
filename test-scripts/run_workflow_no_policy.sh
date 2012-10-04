#!/bin/sh
# Runs the workflow without the policy enabled 5 times.
# Make sure the policy server is disabled in the montage-nocluster
# script.
#
# args:
# --file-size: size of the file that is attached to stage-in jobs (make 
# sure the host and montage-nocluster are actually configured to test this
# size.
#

a=

while [ $# -gt 1 ]; do
  if [ "$1" = "--file-size" ]; then
    a="${a} --file-size $2"
  else
    echo "Usage: $0 [args]"
    echo " where:"
    echo "--file-size <file size>: file size attached"
    exit 1
  fi
  shift 2
done
for i in 1 2 3 4 5
do
  ./workflow_test.sh ${a}
  if [ $? -ne 0 ]; then
    echo "Running workflow with default streams $i failed!"
    exit 1
  fi
done
echo "Successfully ran workflows."

