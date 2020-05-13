#!/bin/bash

# this test verifies that kickstart fails if pegasus-integrity callout fails

# need a directory with only pegasus-kickstart in it
rm -rf testintegrity-callout-failure
mkdir testintegrity-callout-failure

cp $PEGASUS_BIN_DIR/pegasus-kickstart testintegrity-callout-failure

./testintegrity-callout-failure/pegasus-kickstart -s testintegrity.data ls >test.out 2>test.err
RC=$?

rm -rf testintegrity-callout-failure

if [ $RC = 0 ]; then
    cat test.out test.err
    echo "kickstart exited 0, even with a callout failure!"
    exit 1
fi

exit 0


