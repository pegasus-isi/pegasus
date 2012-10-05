#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: $0 infile outfile"
	exit 1
fi

INFILE=$1
OUTFILE=$2

if ! [ -f "$INFILE" ]; then
	echo "No such file: $INFILE"
	exit 1
fi

if [ -f "$OUTFILE" ]; then
	echo "File exists: $OUTFILE"
	exit 1
fi

sleep 1

cat $INFILE > $OUTFILE

