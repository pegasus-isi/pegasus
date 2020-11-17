#!/bin/bash

set -e

TEST_DIR=$PWD

echo "Generating the dax..."
export PYTHONPATH=`pegasus-config --python`
python daxgen.py dax.xml

cat > sites.yml <<END
pegasus: "5.0"
sites:
 -
  name: "local"
  arch: "x86_64"
  os.type: "linux"
  directories:
   -
    type: "sharedStorage"
    path: "$TEST_DIR/outputs"
    fileServers:
     -
      operation: "all"
      url: "file://$TEST_DIR/outputs"
   -
    type: "sharedScratch"
    path: "$TEST_DIR/work"
    fileServers:
     -
      operation: "all"
      url: "file://$TEST_DIR/work"
END

echo "Planning the workflow..."
pegasus-plan \
    --conf pegasusrc \
    --dir submit \
    --dax dax.xml \
    --sites local \
    --output-sites local \
    --cleanup leaf

exit $?
