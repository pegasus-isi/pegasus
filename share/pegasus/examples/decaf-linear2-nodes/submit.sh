#!/bin/bash

set -e

# this variable is expanded by the planner when
# parsing in the sites.xml file in the conf directory
TOPDIR=`pwd`
export TOPDIR

# pegasus bin directory is needed to find keg
BIN_DIR=`pegasus-config --bin`
PEGASUS_LOCAL_BIN_DIR=$BIN_DIR
export PEGASUS_LOCAL_BIN_DIR

# Check if Decaf is found
if [ -z "$DECAF_PREFIX" ]; then
    echo "[ERROR]: DECAF_PREFIX must be set to the DECAF install path."
    exit -1
fi

# Check if MPI is found
MPI_VERSION=$(mpirun -V)
if [ $? -eq 127 ]; then
    echo "[ERROR]: mpirun must be in the PATH."
    exit -1
fi

# generate the input file
echo "This is sample input to KEG" >f.a

cat > ./conf/rc.data <<EOF
f.a file:///${TOPDIR}/f.a site="condorpool"
EOF

# generate the dax
if [ -z "$DYLD_LIBRARY_PATH" ]; then
    export DYLD_LIBRARY_PATH=$LD_LIBRARY_PATH
fi

# plan and submit the  workflow
pegasus-plan \
    --conf pegasus.properties \
    --sites condorpool \
    --output-site local \
    --dir dags \
    --dax decaf-v4.dax \
    --force \
    --submit \
	-v 
