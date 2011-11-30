#!/bin/sh
#
# $Id$
#

# sanity check
if [ "X$GRIDSTART_CHANNEL" = 'X' ]; then
    echo 'Warning: no feedback channel - using stdout' 1>&2
    GRIDSTART_CHANNEL=/dev/fd/1
    test -w $GRIDSTART_CHANNEL || exit 42
fi

/usr/bin/env | sort

i=0
while [[ $i -lt 5 ]]; do 
    i=$(( $i + 1 ))
    /bin/echo -n "testing #$i and <&>... " >> $GRIDSTART_CHANNEL
    sleep 1 
done
exit 0
