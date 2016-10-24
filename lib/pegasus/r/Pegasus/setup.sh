#!/bin/bash

DEST_DIR=$1
LOG_FILE=build.log

if ! type R >/dev/null 2>&1; then
    if [ "x$PEGASUS_BUILD_R_MODULES" == "x0" ]; then
        echo "R is not available, but user has overridden with PEGASUS_BUILD_R_MODULES=0. Skipping R compilation."
        exit 0
    else
        echo "R is not available. Either install the R development packages, or disable this part of the build by setting PEGASUS_BUILD_R_MODULES=0 in your environment before executing ant."
        exit 1
    fi
fi

echo "Building R DAX API..."
rm -rf $DEST_DIR/*.tar.gz

R CMD build DAX &> $LOG_FILE

if [ $? -ne 0 ]; then
    cat $LOG_FILE >&2
    exit 1
fi

mv *.tar.gz $DEST_DIR/$PKG_FILE
rm -rf $LOG_FILE

echo "R DAX API successfully built."

