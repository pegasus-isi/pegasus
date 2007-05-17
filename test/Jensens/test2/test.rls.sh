#!/bin/sh
#
if [ "${JAVA_HOME}" = "" ]; then
    echo "Error! Please set your JAVA_HOME variable."
    exit 1
fi

if [ "${VDS_HOME}" = "" ]; then
    echo "Error! Please set your VDS_HOME variable."
    exit 1
fi

if [ "${CLASSPATH}" = "" ]; then
    echo "Warning! Your CLASSPATH variable is suspiciously empty."
    echo "I will source the set-user-env script for you."
    source $VDS_HOME/set-user-env.sh || exit 1
fi

if [ "${GLOBUS_LOCATION}" = "" ]; then
    echo "Error! Your GLOBUS_LOCATION is not set. Please set it, and"
    echo "source the globus-user-env script."
    exit 1
fi

if [ ! -x ${GLOBUS_LOCATION}/bin/grid-proxy-info ]; then
    echo "Error! Please make sure that your G2 installation is complete."
    exit 1
fi

if [ ! -x ${GLOBUS_LOCATION}/bin/globus-rls-cli ]; then
    echo "Error! Please make sure that you installed the RLS client in Globus."
    exit 1
fi

if [ ! "`type -p condor_submit_dag`" ]; then
    echo "Error! Please add the Condor tools to your PATH."
    exit 1
fi

save=`/bin/pwd`
cd `dirname $0`
here=`/bin/pwd`
cd $save
last=`dirname $here`
poolconfig="$last/pool.config.rls"
if [ ! -r "$poolconfig" ]; then
    echo "Error! Please create a pool.config file in $poolconfig."
    exit 1
fi

# let's start
TEMP=`getopt -l rls:,src:,source:,run:,exec:,dst:,destination:,help,stop-after-cplan -o c:r:s:d:e:h -- "$@"`
test $? -ne 0 && exit 1
eval set -- "$TEMP"

# defaults (the latter one for lazy me)
stop_after_cplan=0
srcsite=local
dstsite=local
test "$REPLICA_CATALOG" && catalog=${REPLICA_CATALOG}
test "$EXECUTION_POOL" && runsite=${EXECUTION_POOL}
while true; do
    case "$1" in 
	--rls|-r)
	    shift
	    catalog=$1
	    shift
	    ;;
	--src|--source|-s)
	    shift
	    srcsite=$1
	    shift
	    ;;
	--run|--exec|-e)
	    shift
	    runsite=$1
	    shift
	    ;;
	--dst|--destination|-d)
	    shift
	    dstsite=$1
	    shift
	    ;;
	--stop-after-cplan)
	    shift;
	    stop_after_cplan=1
	    ;;
	--help|-h)
	    echo "$0 --rls RC [--src S] --run E [--dst D] | --help"
	    echo " --rls RC  names your replica catalog RC, e.g. rls://sheveled.mcs.anl.gov"
	    echo " --src S   names the source pool S where files are picked up, default \"$srcsite\""
	    echo " --run E   names the execution pool E where things are run"
	    echo " --dst D   names the final resting place D of stage-out data, default \"$dstsite\""
	    echo " --stop-after-cplan  will stop this script after the gencdag ran"
	    exit 0
	    ;;
	--)
	    shift
	    break
	    ;;
	*)
	    echo "Error in arguments, see --help"
	    exit 1
	    ;;
    esac
done

#
# run substitutions on input files
#
$last/substitute.pl "USER=$LOGNAME" "VDS=${VDS_HOME}" "JAVA=${JAVA_HOME}" \
    "GL=${GLOBUS_LOCATION}" hw.vdl.in tc.data.in

#
# validate CLI arguments
#
error=0
if [ -z "$srcsite" ]; then
    echo "Error! You must specify a host as source to pick up the input file, see --src"
    error=2
else
    src_location=`$last/get-contact.pl $srcsite $poolconfig`
    if [ $? -ne 0 ]; then
	echo "Error! Probably an entry for $srcsite is missing in $poolconfig"
	error=3
    else 
	echo -e "Using src: $srcsite\t$src_location"
    fi
fi

if [ -z "$dstsite" ]; then
    echo "Error! You must specify a host as final resting place for data, see --dst"
    error=2
else
    dst_location=`$last/get-contact.pl $dstsite $poolconfig`
    if [ $? -ne 0 ]; then
	echo "Error! Probably an entry for $dstsite is missing in $poolconfig"
	error=3
    else
	echo -e "Using dst: $dstsite\t$dst_location"
    fi
fi

if [ -z "$runsite" ]; then
    echo "Error! You must specify a run pool (by its handle) using --run"
    error=1
else 
    run_location=`$last/get-contact.pl $runsite $poolconfig`
    if [ $? -ne 0 ]; then
	echo "Error! Probably an entry for $runsite is missing in $poolconfig"
	error=3
    else
	echo -e "Using run: $runsite\t$run_location"
    fi

    if [ "`grep \"^$i\" $here/tc.data | grep hw | wc -l`" -lt 1 ]; then
	echo "Error! There is no findrange entry for $i in $here/tc.data"
	error=3
    fi
fi

if [ -z "$catalog" ]; then
    echo "You must specify an RLS to use with --rls"
    error=1
fi
test "$error" -gt 0 && exit $error
unset error

#
# check user proxy certificate ttl
#
remain=`grid-proxy-info -all | tail -1 | cut -c 12-` >> /dev/null 2>&1
if [ $? -ne 0 -o -z "$remain" ]; then
    echo "Error! Unable to determine the time remaining on your certificate."
    exit 1
fi

left=`echo $remain | awk -F: '{ print $1*3600+$2*60+$3 }'`
if [ $left -lt 7200 ]; then
    echo "Error! There is too little time remaining on your proxy certificate."
    echo "Please run grid-proxy-init, and restart."
    exit 1
else
    echo "OK: $left s remaining on proxy certificate."
fi

#
# remove left-overs from previous run
#
echo ""
if [ -d hw ]; then
    echo "Warning: Removing old DAG directory hw"
    rm -rf hw 
fi

#
# create the initial input data for hello world example (always here == local)
#
stdin="$LOGNAME.hwi.txt"
stdout="$LOGNAME.hwo.txt"
stderr="$LOGNAME.hwe.txt"

test -d "$HOME/vdldemo" || mkdir "$HOME/vdldemo"
date | tr '\012' '\011' > "$HOME/vdldemo/$stdin"
hostname -f >> "$HOME/vdldemo/$stdin"

#
# create temporary property file entries
#
echo "creating my own $prop"
/bin/rm -f $here/vds.db.* >> /dev/null 2>&1
prop=" -Dvds.properties=/dev/null -Dvds.user.properties=/dev/null"
prop="${prop} -Dvds.db.vdc.schema=SingleFileSchema" 
prop="${prop} -Dvds.db.vdc.schema.file.store=$here/vds.db"
prop="${prop} -Dvds.tc.file=$here/tc.data"
prop="${prop} -Dvds.pool.file=$poolconfig"
prop="${prop} -Dvds.rls.url=$catalog"
prop="${prop} -Dvds.replica.mode=rls"
java="${JAVA_HOME}/bin/java -Dvds.home=${VDS_HOME} ${prop}"

#
# run the abstract planner pipeline
#
echo ">>"
echo ">> running abstract planner"
echo ">>"
if [ -r hw.xml -a hw.xml -nt hw.vdl.in ]; then
    # cheating ;-P
    cp hw.xml $here/vds.db.8
    echo 8 > $here/vds.db.nr
else
    # go through the motions
    $VDS_HOME/bin/vdlt2vdlx $prop -n $LOGNAME hw.vdl hw.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to convert VDLt into VDLx"
	exit 4
    fi
    
    $VDS_HOME/bin/insertvdc $prop hw.xml 
    if [ $? -ne 0 ]; then
	echo "Error! Unable to insert new definitions into database"
	exit 4
    fi
fi

$VDS_HOME/bin/gendax $prop -l hw -f "$stdout" -o hw.dax 
if [ $? -ne 0 ]; then
    echo "Error! Unable to create DAX for output file $stdin"
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
echo "RC: Removing old $stdin to $srcsite (even if duplicate)"
$GLOBUS_LOCATION/bin/globus-rls-cli delete "$stdin" "$src_location$stdin" $catalog
test $? -eq 0 || echo "Warning: RC complained, please see error message above"

echo "RC: Adding new $stdin to $srcsite (even if duplicate)"
$VDS_HOME/bin/rls-client $prop --lrc=$catalog --pool=$srcsite --delimiter=@ \
 --mappings=$stdin,$src_location$stdin
test $? -eq 0 || echo "Warning: RC complained, please see error message above"

#
# remove possibly existing products from RC (enforce build-style cDAG)
#
for i in $stdout $stderr; do
    echo "RC: Removing data product $i from $dstsite (even if non-existent)"
    $GLOBUS_LOCATION/bin/globus-rls-cli delete "$i" "$dst_location$i" $catalog
    test $? -eq 0 || echo "Warning: RC complained, please see error message above"
done

# create DAG
echo ">>"
echo ">> starting concrete planner"
echo ">>"
set -x
$VDS_HOME/bin/gencdag $prop --o $dstsite --p $runsite --dir hw --dax hw.dax --force
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
    echo -e "\tchdir hw"
    echo -e "\tcondor_submit_dag hw-0.dag"
    echo ""
    exit 0
fi

# run DAG and wait for finish
echo ">>"
echo ">> starting Condor DAGMan"
echo ">>"
cd hw
condor_submit_dag -notification ERROR hw-0.dag
while [ true ]; do
    date | tr '\012' '\011'
    echo "checking status of DAG..."
    egrep -i '(termination|Job was aborted by the user)' hw-0.dag.dagman.log >> /dev/null 
    test $? -eq 0 && break
    sleep 5
done

check () {
    test -r "$1" || return 1
    /bin/ls -la $1
    cat $1
}

# results
grep -i termination hw-0.dag.dagman.log >> /dev/null 2>&1
if [ $? -eq 0 ]; then 
    # show results
    echo "RC: files registered"
    $GLOBUS_LOCATION/bin/globus-rls-cli query rli lfn "$stdout" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc lfn "$stdout" $catalog
    $GLOBUS_LOCATION/bin/globus-rls-cli query lrc pfn "$dst_location$stdout" $catalog

    echo "-- "
    check "$HOME/vdldemo/$stdin"
    check "$HOME/vdldemo/$stdout"
    check "$HOME/vdldemo/$stderr"
else
    # failure
    echo "Job was aborted."
    exit 11
fi

exit 0
