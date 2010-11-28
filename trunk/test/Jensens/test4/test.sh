#!/bin/sh
#
# larger workflow of 291 nodes - tree of diamonds
#
save=`/bin/pwd`
cd `dirname $0`
here=`/bin/pwd`
cd $save
last=`dirname $here`
poolconfig="$last/pool.config"
tcdata="$last/tc.data"
hostname="`hostname --long`"

if [ -r "$last/helpers.sh" ]; then
    source "$last/helpers.sh"
else
    echo "Error! Unable to source helpers.sh from $last."
    exit 1
fi
check_setup || exit 1

#
# parse commandline
#
parse_commandline "$@"

#
# run substitutions on input files
#
check_template "$poolconfig"
check_template "$tcdata"
check_template --force big.vdl

#
# validate CLI arguments
#
check_arguments "$LOGNAME::process" "$LOGNAME::generate" \
    "$LOGNAME::combine" "$LOGNAME::multi" || exit $?

#
# check user proxy certificate ttl
#
check_proxy_ttl || exit $?

#
# remove left-overs from previous run
#
echo ""
if [ -d big ]; then
    echo "Warning: Removing old DAG directory big"
    rm -rf big 
fi

#
# create temporary property file entries
#
echo "creating my own $prop"
/bin/rm -f $here/vds.db.* >> /dev/null 2>&1
java="${JAVA_HOME}/bin/java -Dvds.home=${VDS_HOME} `create_properties`"
prop1=`create_chimera_properties`
prop2=`create_pegasus_properties multiple`

#
# run the abstract planner pipeline
#
echo ">>"
echo ">> running abstract planner"
echo ">>"
if [ -r big.xml -a big.xml -nt big.vdl.in ]; then
    # cheating ;-P
    cp big.xml $here/vds.db.8
    echo 8 > $here/vds.db.nr
else
    # go through the motions
    $VDS_HOME/bin/vdlt2vdlx $prop1 -n $LOGNAME big.vdl big.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to convert VDLt into VDLx"
	exit 4
    fi
    
    $VDS_HOME/bin/insertvdc $prop1 big.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to insert new definitions into database"
	exit 4
    fi
fi
    
$VDS_HOME/bin/gendax $prop1 --label big --lfn "$LOGNAME.final" \
    --output big.dax 
if [ $? -ne 0 ]; then
    echo "Error! Unable to create DAX for output file $LOGNAME.final"
    exit 4
else
    # don't need the dbase any more
    rm -f $here/vds.db.*
fi

#
# remove possibly existing products from RC (enforce build-style cDAG)
#
echo ">>"
echo ">> preparing replica catalog"
echo ">>"
echo "RC: Removing data product $LOGNAME.final from $dstsite (even if non-existent)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$LOGNAME.final" "$dst_location$LOGNAME.final" $catalog
test $? -eq 0 || echo "Warning: RC complained, please see error message above"

## remove possibly existing products from RC (enforce build DAG)
#tmpfile=`mktemp /tmp/rcdel.XXXXXX || exit 10`
#perl -e 'for ($i=0; $i<0x118; $i++) { printf "'$LOGNAME'f.%05x\n", $i }' > $tmpfile
#perl -e 'for ($i=0; $i<0x100; $i+=0x1C) { printf "'$LOGNAME'r.f.%05x\n", $i }' >> $tmpfile
#rm -f $tmpfile
## post-condition: Locations $srcsite and $dstsite exist

# create DAG
echo ">>"
echo ">> starting concrete planner"
echo ">>"
set -x
$VDS_HOME/bin/gencdag $prop2 --dax big.dax --pools $runsite \
    --output $dstsite --dir big --force
rc=$?
set +x
if [ $rc -ne 0 ]; then
    echo "Error! The concrete planner reported an error condition"
    exit 5
fi

# test for stopping now (manual submission)
if [ "$stop_after_cplan" -eq 1 ]; then
    echo "--"
    echo "You chose to submit the DAG manually. Please run the following commands:"
    echo -e "\tcd big"
    echo -e "\tvds-submit-dag big?0.dag"
    echo ""
    exit 0
fi

# run DAG and wait for finish
echo ">>"
echo ">> starting Condor DAGMan"
echo ">>"
cd big
vds-submit-dag -n ERROR big?0.dag
wait_for_dagman big?0.dag.dagman.log

check () {
    test -r "$1" || return 1
    /bin/ls -la $1
    cat $1
}

# results
grep -i termination big?0.dag.dagman.log >> /dev/null 2>&1
if [ $? -eq 0 ]; then 
    # show results
    echo "RC: files registered"
    $GLOBUS_LOCATION/bin/globus-rls-cli query rli lfn "$LOGNAME.final" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc lfn "$LOGNAME.final" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc pfn "$dst_location$LOGNAME.final" $catalog

    echo "-- "
    check "$HOME/vdldemo/$LOGNAME.final"
else
    # failure
    echo "Job was aborted."
    exit 11
fi

exit 0
