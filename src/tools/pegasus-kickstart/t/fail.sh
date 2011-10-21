#!/bin/sh
#
# $Id$
#
if [ -z "$1" ]; then
    exit 1
else
    kill -$1 $$
fi
