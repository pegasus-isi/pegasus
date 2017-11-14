#!/bin/bash
set -e

input=f.a
if [ ! -e $input ]; then
    echo "ERROR: input file $input does not exist" 1>&2
    exit 1
fi

# check that we got the input file                                                                                                                                                                                                                                       
cat $input

# output something on stdout
echo "Hello!"

# in the DAX, this job is specified to have an f.b output file
echo "Hello!" >f.b

