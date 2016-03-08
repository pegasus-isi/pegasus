#!/bin/bash

DEST_DIR=$1
LOG_FILE=build.log

type R >/dev/null 2>&1 || { echo >&2 "R is not available. Skipping R compilation."; exit 0; }

echo "Building R DAX API..."
rm -rf $DEST_DIR/*.tar.gz

R CMD build DAX &> $LOG_FILE

if [ $? -ne 0 ]; then
    cat $LOG_FILE >&2
    exit 1
fi

PKG_FILE=`ls *.tar.gz`
mv *.tar.gz $DEST_DIR/pegasus-r-$PKG_FILE
rm -rf $LOG_FILE

echo "R DAX API successfully built."

