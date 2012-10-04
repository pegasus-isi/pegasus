#!/bin/sh
# Runs a montage workflow with a specified policy and additional file size
# for each default parallel stream setting.
#
# args:
# --max-streams: max streams used in the policy
# --file-size: size of the additional staged-in file
#
a=

while [ $# -gt 1 ]; do
  if [ "$1" = "--max-streams" ]; then
    a="${a} --max-streams $2"
  elif [ "$1" = "--file-size" ]; then
    a="${a} --file-size $2"
  else
    echo "Usage: $0 [args]"
    echo " where:"
    echo "--max-streams <max-streams>: default max streams in policy"
    echo "--file-size <file size>: file size attached"
    exit 1
  fi
  shift 2
done
for i in 4 6 8 10 12
do
  ./workflow_test.sh --default-streams $i ${a}
  if [ $? -ne 0 ]; then
    echo "Running workflow with default streams $i failed!"
    exit 1
  fi
done
echo "Successfully ran workflows."

