#!/bin/sh
#
# submittor for nested workflows
# $Id$
#
if [ "X$1" = 'X' ]; then
    depth=3
else
    depth=$(( $1 + 0 ))
fi

#
# OVERWRITE conf CONTENTS
#
test -d conf && rm -rf conf
mkdir -p conf work output
date +"# file-based replica catalog: %FT%T%z" > conf/rc.data
cp /dev/null conf/tc.text
pegasus-config --full-local $PWD/output > conf/sites.xml 
cat <<EOF > conf/properties
pegasus.catalog.replica=SimpleFile
pegasus.catalog.replica.file=conf/rc.data
pegasus.catalog.site=XML3
pegasus.catalog.site.file=conf/sites.xml
pegasus.catalog.transformation=Text
pegasus.catalog.transformation.file=conf/tc.text

pegasus.dir.useTimestamp=false
pegasus.dir.storage.deep=false

pegasus.monitord.events=false
pegasus.monitord.output=file:///tmp/throwaway.tmp

pegasus.data.configuration=Condor
EOF

fstype=`stat -f -L -c %T $PWD`
if [ "X$fstype" = 'Xnfs' ]; then
    echo "pegasus.condor.logs.symlink=true"  >> conf/properties
else
    echo "pegasus.condor.logs.symlink=false" >> conf/properties
fi

#
# create workflow of workflows
#
perl deepthought.pl $depth || exit 42

#
# plan and run
#
base=`date +"%Y%m%dT%H%M"`
test -d work/$base && rm -rf work/$base
dax="level-${depth}.dax"

pegasus-plan \
    --conf $PWD/conf/properties \
    -vvv \
    --dir work \
    --relative-submit-dir $base \
    --sites local \
    --output local \
    --dax $dax \
    --nocleanup \
    --submit
