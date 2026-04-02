#!/bin/bash

# This script forks a process that runs for a long time and should
# get killed by killpg()

sleep 300 &
