#!/bin/sh
#
# test all exit codes from 0 .. 127
#
# $Id$
#
TEMPFILE=`mktemp` || exit 1

i=0
while [[ $i -lt 128 ]]; do 
    ../pegasus-kickstart /bin/sh -c "exit $i" > $TEMPFILE
    fgrep '<status ' $TEMPFILE
    i=$(( $i + 1 ))
done

rm $TEMPFILE
