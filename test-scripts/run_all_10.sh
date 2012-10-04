#!/bin/sh
# Runs each default stream test for each policy type
# for 10M additional staged files.
# 
# Please make sure the montage-nocluster script
# has the proper LARGE_FILENAME_HOST specified and that
# this host has symlinks to the 10M file in the /opt/stagein/stagein
# directory.

for i in 50 100 200; do
  ./run_workflow_parallel_streams.sh --file-size 10M --max-streams $i
done

