#!/bin/bash

# Print messages on stdio streams
echo "Some message on stdout"
echo "Some message on stderr" >&2

if [ $1 == "fail" ]; then
    exit 1
fi

exit 0

