#!/bin/sh
#
# hello world - minimum test
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
check_template --force helloworld.vdl

#
# validate CLI arguments
#
check_arguments "$LOGNAME::world" || exit $?

#
# check user proxy certificate ttl
#
check_proxy_ttl || exit $?

#
# remove left-overs from previous run
#
echo ""
if [ -d helloworld ]; then
    echo "Warning: Removing old DAG directory helloworld"
    rm -rf helloworld 
fi

#
# create the initial input data for hello world example (always here == local)
#
test -d "$HOME/vdldemo" || mkdir "$HOME/vdldemo"
date | tr '\012' '\011' > "$HOME/vdldemo/$LOGNAME.data.in"
hostname -f >> "$HOME/vdldemo/$LOGNAME.data.in"

#
# create temporary property file entries
#
echo "creating my own $prop"
/bin/rm -f $here/vds.db.* >> /dev/null 2>&1
java="${JAVA_HOME}/bin/java -Dvds.home=${VDS_HOME} `create_properties`"
prop1=`create_chimera_properties`
prop2=`create_pegasus_properties`

#
# run the abstract planner pipeline
#
echo ">>"
echo ">> running abstract planner"
echo ">>"
if [ -r helloworld.xml -a helloworld.xml -nt helloworld.vdl.in ]; then
    # cheating ;-P
    cp helloworld.xml $here/vds.db.8
    echo 8 > $here/vds.db.nr
else
    # go through the motions
    $VDS_HOME/bin/vdlt2vdlx $prop1 -n $LOGNAME helloworld.vdl helloworld.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to convert VDLt into VDLx"
	exit 4
    fi
    
    $VDS_HOME/bin/insertvdc $prop1 helloworld.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to insert new definitions into database"
	exit 4
    fi
fi

$VDS_HOME/bin/gendax $prop1 --label hello-world \
    --dv "$LOGNAME::hello:1.0" --output helloworld.dax 
rc=$?
if [ $rc -ne 0 ]; then
    echo "Error! Unable to create DAX for DV $LOGNAME::hello:1.0"
    exit 4
else
    # don't need the dbase any more
    rm -f $here/vds.db.*
fi

#
# insert the input file into the RLS -- delete before insert
#
echo ">>"
echo ">> preparing replica catalog"
echo ">>"
set -x
echo "RC: Adding $i to $srcsite (even if duplicate)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$LOGNAME.data.in" "$src_location/$LOGNAME.data.in" $catalog 
sleep 1
$VDS_HOME/bin/rls-client $prop2 --lrc=$catalog --pool=$srcsite --delimiter=@ \
 --mappings="$LOGNAME.data.in,$src_location/$LOGNAME.data.in" --verbose
$GLOBUS_LOCATION/bin/globus-rls-cli query lrc lfn "$LOGNAME.data.in" $catalog 
#
echo "RC: Removing data product $i from $dstsite (even if non-existent)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$LOGNAME.data.out" "$dst_location/data.out" $catalog
set +x
sleep 1

# create DAG
echo ">>"
echo ">> starting concrete planner"
echo ">>"
set -x
$VDS_HOME/bin/gencdag $prop2 --dax helloworld.dax --pools $runsite \
    --output $dstsite --dir helloworld --force
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
    echo -e "\tcd helloworld"
    echo -e "\tvds-submit-dag hello-world?0.dag"
    echo ""
    exit 0
fi

# run DAG and wait for finish
echo ">>"
echo ">> starting Condor DAGMan"
echo ">>"
cd helloworld
$VDS_HOME/bin/vds-submit-dag -n ERROR hello-world?0.dag
wait_for_dagman hello-world?0.dag.dagman.log

check () {
    test -r "$1" || return 1
    /bin/ls -la $1
    cat $1
}

# results
grep -i termination hello-world?0.dag.dagman.log >> /dev/null 2>&1
if [ $? -eq 0 ]; then 
    # show results
    echo "RC: files that were registered"
    $GLOBUS_LOCATION/bin/globus-rls-cli query rli lfn "$LOGNAME.data.out" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc lfn "$LOGNAME.data.out" $catalog

    echo "RC: files registered for src $srcsite"
    $GLOBUS_LOCATION/bin/globus-rls-cli query wildcard lrc pfn "$src_location/*" $catalog

    echo "RC: files registered for dst $dstsite"
    $GLOBUS_LOCATION/bin/globus-rls-cli query wildcard lrc pfn "$dst_location/*" $catalog

    echo "-- "
    check $HOME/vdldemo/$LOGNAME.data.in
    check $HOME/vdldemo/$LOGNAME.data.out
else
    # failure
    echo "Job was aborted."
    exit 11
fi

exit 0
