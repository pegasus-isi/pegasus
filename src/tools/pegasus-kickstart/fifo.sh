#!/bin/sh
#

# sanity check
if [ "X$GRIDSTART_CHANNEL" = 'X' ]; then
    echo 'no feedback channel - no kickstart?' 1>&2
    exit 42
fi

/usr/bin/env | sort

i=0
while [[ $i -lt 5 ]]; do 
    i=$(( $i + 1 ))
    /bin/echo -n "testing #$i" >> $GRIDSTART_CHANNEL
    sleep 1 
done
exit 0
