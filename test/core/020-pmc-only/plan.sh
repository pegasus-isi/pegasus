#!/bin/bash

set -e

TEST_DIR=$PWD

echo "Generating the dax..."
export PYTHONPATH=`pegasus-config --python`
python daxgen.py dax.xml

cat > sites.xml <<END
<?xml version="1.0" encoding="UTF-8"?>
<sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-3.0.xsd" version="3.0">
    <site handle="local" arch="x86_64" os="LINUX" osrelease="rhel" osversion="7">
        <head-fs>
            <scratch>
                <shared>
                    <file-server protocol="file" url="file://" mount-point="$TEST_DIR/work"/>
                    <internal-mount-point mount-point="$TEST_DIR/work"/>
                </shared>
            </scratch>
            <storage>
                <shared>
                    <file-server protocol="file" url="file://" mount-point="$TEST_DIR/outputs"/>
                    <internal-mount-point mount-point="$TEST_DIR/outputs"/>
                </shared>
            </storage>
        </head-fs>
    </site>
</sitecatalog>
END

echo "Planning the workflow..."
pegasus-plan \
    --conf pegasusrc \
    --dir submit \
    --dax dax.xml \
    --sites local \
    --output-site local \
    --cleanup leaf

exit $?
