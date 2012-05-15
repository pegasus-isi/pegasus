#!/bin/bash

# This script hangs for a long time so that PMC has
# to kill it when the workflow finishes.

sleep 300

echo "FAILURE: Did not get killed"
