#!/bin/bash

set -e

# output some thing to stdout
echo "Hello world!"
input=f.b
if [ ! -e $input ]; then
    echo "ERROR: input file $input does not exist" 1>&2
    exit 1
fi

# check that we got the input file
cat $input

# in the DAX, this job is specified to have an f.c output file
echo "Hello world!" >f.c

