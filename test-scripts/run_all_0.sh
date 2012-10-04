#!/bin/sh
# Script that runs a test for each default parallel stream
# and policy type for no additional staged files.
# Please make sure the LARGE_FILE_HOSTNAME is left blank
# in the montage-nocluster script when testing this.
#
for i in 50 100 200; do
  ./run_workflow_parallel_streams.sh --max-streams $i
done

