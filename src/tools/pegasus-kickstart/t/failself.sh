#!/bin/sh
#
# $Id$
#
if [ "X$1" = 'X' ]; then
    exit 1
else
    kill -$1 $$
fi
