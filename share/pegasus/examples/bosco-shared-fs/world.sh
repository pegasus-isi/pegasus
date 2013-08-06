#!/bin/bash

set -e

# output some thing to stdout
echo "Hello world!"

# check that we got the input file
cat f.b

# in the DAX, this job is specified to have an f.c output file
echo "Hello world!" >f.c

