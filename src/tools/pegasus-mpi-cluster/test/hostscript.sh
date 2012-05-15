#!/bin/bash

# This script just prints some messages and exits.
# We test to make sure that both messages end up
# in the stderr of PMC.

echo "HOSTSCRIPT stdout"
echo "HOSTSCRIPT stderr" >&2
