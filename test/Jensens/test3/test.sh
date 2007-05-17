#!/bin/sh
#
# diamond example - split and converge test
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
check_template --force blackdiamond.vdl

#
# validate CLI arguments
#
check_arguments "$LOGNAME::findrange" "$LOGNAME::preprocess" \
    "$LOGNAME::analyze" || exit $?

#
# check user proxy certificate ttl
#
check_proxy_ttl || exit $?

#
# remove left-overs from previous run
#
echo ""
if [ -d blackdiamond ]; then
    echo "Warning: Removing old DAG directory blackdiamond"
    rm -rf blackdiamond 
fi

#
# create the initial input data for hello world example (always here == local)
#
test -d "$HOME/vdldemo" || mkdir "$HOME/vdldemo"
date | tr '\012' '\011' > "$HOME/vdldemo/$LOGNAME.f.a"
hostname -f >> "$HOME/vdldemo/$LOGNAME.f.a"

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
if [ -r blackdiamond.xml -a blackdiamond.xml -nt blackdiamond.vdl.in ]; then
    # cheating ;-P
    cp blackdiamond.xml $here/vds.db.8
    echo 8 > $here/vds.db.nr
else
    # go through the motions
    $VDS_HOME/bin/vdlt2vdlx $prop1 -n $LOGNAME blackdiamond.vdl blackdiamond.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to convert VDLt into VDLx"
	exit 4
    fi
    
    $VDS_HOME/bin/insertvdc $prop1 blackdiamond.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to insert new definitions into database"
	exit 4
    fi
fi

$VDS_HOME/bin/gendax $prop1 --label black-diamond \
    --lfn "$LOGNAME.f.d" --output blackdiamond.dax 
if [ $? -ne 0 ]; then
    echo "Error! Unable to create DAX for output file $LOGNAME.f.d"
    exit 4
else
    # don't need the dbase any more
    rm -f $here/vds.db.*
fi

#
# insert the input file into the RLS
#
echo ">>"
echo ">> preparing replica catalog"
echo ">>"
echo "RC: Removing old $LOGNAME.f.a to $srcsite (even if duplicate)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$LOGNAME.f.a" "$src_location$LOGNAME.f.a" $catalog
test $? -eq 0 || echo "Warning: RC complained, please see error message above"
echo "RC: Adding new $LOGNAME.f.a to $srcsite (even if duplicate)"
$VDS_HOME/bin/rls-client $prop2 --lrc=$catalog --pool=$srcsite --delimiter=@ \
 --mappings=$LOGNAME.f.a,$src_location/$LOGNAME.f.a --verbose
test $? -eq 0 || echo "Warning: RC complained, please see error message above"

#
# remove possibly existing products from RC (enforce build-style cDAG)
#
echo "RC: Removing data product $LOGNAME.f.d from $dstsite (even if non-existent)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$LOGNAME.f.d" "$dst_location$LOGNAME.f.d" $catalog
test $? -eq 0 || echo "Warning: RC complained, please see error message above"

# create DAG
echo ">>"
echo ">> starting concrete planner"
echo ">>"
set -x
$VDS_HOME/bin/gencdag $prop2 --dax blackdiamond.dax --pools $runsite \
    --output $dstsite --dir blackdiamond --force
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
    echo -e "\tcd blackdiamond"
    echo -e "\tvds-submit-dag black-diamond?0.dag"
    echo ""
    exit 0
fi

# run DAG and wait for finish
echo ">>"
echo ">> starting Condor DAGMan"
echo ">>"
cd blackdiamond
$VDS_HOME/bin/vds-submit-dag -n ERROR black-diamond?0.dag
wait_for_dagman black-diamond?0.dag.dagman.log

check () {
    test -r "$1" || return 1
    /bin/ls -la $1
    cat $1
}

# results
grep -i termination black-diamond?0.dag.dagman.log >> /dev/null 2>&1
if [ $? -eq 0 ]; then 
    # show results
    echo "RC: files registered"
    $GLOBUS_LOCATION/bin/globus-rls-cli query rli lfn "$LOGNAME.f.d" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc lfn "$LOGNAME.f.d" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc pfn "$dst_location$LOGNAME.f.d" $catalog

    echo "-- "
    check "$HOME/vdldemo/$LOGNAME.f.a"
    check "$HOME/vdldemo/$LOGNAME.f.d"
else
    # failure
    echo "Job was aborted."
    exit 11
fi

exit 0
