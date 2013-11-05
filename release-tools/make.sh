#!/bin/bash

MAKE=`which gmake || which make`

if [ "$MAKE" == "" ]; then
    echo "make not found"
    exit 127
fi

exec $MAKE "$@"

