#!/bin/sh
#
# helpful common functions of the test scripts in the test[1-4] subdirs.
#
check_setup () {
    # purpose: check basic setup early
    # returns: 0: all is well
    #          1: error in setup
    if [ "${JAVA_HOME}" = "" ]; then
	echo "Error! Please set your JAVA_HOME variable."
	return 1
    fi

    if [ "${VDS_HOME}" = "" ]; then
	echo "Error! Please set your VDS_HOME variable."
	return 1
    fi

    if [ "${CLASSPATH}" = "" ]; then
	echo "Warning! Your CLASSPATH variable is suspiciously empty."
	echo "I will source the set-user-env script for you."
	source $VDS_HOME/set-user-env.sh || return 1
    fi

    if [ "${GLOBUS_LOCATION}" = "" ]; then
	echo "Error! Your GLOBUS_LOCATION is not set. Please set it, and"
	echo "source the globus-user-env script."
	return 1
    fi

    if [ ! -x ${GLOBUS_LOCATION}/bin/grid-proxy-info ]; then
	echo "Error! Please make sure that your G2 installation is complete."
	return 1
    fi

    if [ ! -x ${GLOBUS_LOCATION}/bin/globus-rls-cli ]; then
	echo "Error! Please make sure that you installed the RLS client in Globus."
	return 1
    fi

    if [ ! "`type -p vds-submit-dag`" ]; then
	echo "Error! Please make sure that vds-submit-dag is in your PATH."
	return 1
    fi

    if [ ! "`type -p condor_submit_dag`" ]; then
	echo "Error! Please add the Condor tools to your PATH."
	return 1
    fi

    return 0
}



parse_commandline () {
    # purpose: parses commandline argument via GNU getopt
    # paramtr: the main script's "$@" argument vector
    # globals: $catalog (OUT): RLS URI
    #          $srcsite (OUT): 
    #          $runsite (OUT): 
    #          $dstsite (OUT): 
    #          $userprops (OUT): user properties file
    #          $stop_after_cplan (OUT): set to 1 if to stop early
    local TEMP=`getopt -l rls:,src:,run:,dst:,user:,help,stop-after-cplan -o c:r:s:d:e:u:h -- "$@"`
    test $? -ne 0 && exit 1
    eval set -- "$TEMP"

    stop_after_cplan=0
    srcsite=local
    dstsite=local
    userprops='/dev/null'
    test "$REPLICA_CATALOG" && catalog=${REPLICA_CATALOG}
    test "$EXECUTION_POOL" && runsite=${EXECUTION_POOL}
    while true; do
	case "$1" in 
	    --rls|-r)
		shift
		catalog=$1
		shift
		;;
	    --src|-s)
		shift
		srcsite=$1
		shift
		;;
	    --run|-e)
		shift
		runsite=$1
		shift
		;;
	    --dst|-d)
		shift
		dstsite=$1
		shift
		;;
	    --user|-u)
		shift
		userprops=$1
		shift
		;;
	    --stop-after-cplan)
		shift;
		stop_after_cplan=1
		;;
	    --help|-h)
		echo "`basename $0` --rls R [--src S] --run E [--dst D] [--user P] | --help"
		echo ""
		echo " --rls R   names your replica catalog, e.g. rls://terminable.uchicago.edu"
		echo " --src S   names the source site S where files are picked up, default \"$srcsite\""
		echo " --run E   names the execution pool E where things are run"
		echo " --dst D   names the final resting place D of stage-out data, default \"$dstsite\""
		echo " --user P  uses the specified user property file P, default $userprops"
		echo " --stop-after-cplan  will stop this script after the gencdag ran"
		echo ""
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

    return 0
}

check_template () {
    # purpose: check and substitute one file from template files.
    # paramtr: '--force' (opt. IN): force substitution (build)
    #          $filename (IN): full path of file to check
    if [ "$1" = "--force" ]; then
	force=1
	shift
    else 
	force=0
    fi

    # update, if template is newer
    test "$1.in" -nt "$1" && force=1

    if [ ! -r "$1" -o $force -eq 1 ]; then
	if [ ! -r "$1.in" ]; then
	    echo "Error! Missing template file $1.in"
	    exit 1
	else
	    echo "# generating $1..."
	    $last/substitute.pl "USER=$LOGNAME" "VDS=${VDS_HOME}" \
		"JAVA=${JAVA_HOME}" "GL=${GLOBUS_LOCATION}" \
		"HOST=${hostname}" "$1.in"
	fi
    else
	echo "# Warning: Recycling $1..."
    fi

    return 0
}    



check_arguments () {
    # purpose: check arguments for src, run, dst sites
    # paramtr: ... transformations to check for
    # checked: $srcsite (IN): src site
    #          $runsite (IN): run site
    #          $dstsite (IN): dst site
    #          $catalog (IN): RLS URI
    # generat: $src_location (OUT): gridftp URI src
    #          $dst_location (OUT): gridftp URI dst
    #          $run_location (OUT): gridftp URI run
    # globals: $last (IN): parent directory
    #          $poolconfig (IN): pool.config file (old textual)
    #          $tcdata (IN): tc.data file (old textual)
    # returns: 0: all is well
    #          1: missing RLS spec
    #          2: missing site argument
    #          3: invalid site argument
    local error=0
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

	while [ "$1" != "" ]; do
	    if [ "`grep \"^$runsite\" $tcdata | grep \"$1\" | wc -l`" -lt 1 ]; then
		echo "Error! There is no \"$1\" entry for $runsite in $tcdata"
		error=3
	    fi
	    shift
        done
    fi

    if [ -z "$catalog" ]; then
	echo "You must specify an RLS to use with --rls"
	error=1
    fi

    return $error
}



check_proxy_ttl () {
    # purpose: checks the runtime setup
    # returns: 0: all is well
    #          1: proxy is expired, non-existant, or too small
    left=`grid-proxy-info -timeleft 2> /dev/null`
    local rc=$?
    if [ $rc -ne 0 ]; then
	echo "Error! grid-proxy-info returned exit code $rc"
	return 2
    elif [ $left -lt 7200 ]; then
	echo "Error! There is too little time ($left s) remaining on your."
	echo "proxy certificate. Please run grid-proxy-init, and restart."
	return 1
    else
	echo "OK: $left s remaining on proxy certificate."
    fi
}


create_properties () {
    # purpose: create a set of properties to use for tests
    # paramtr: $mode (opt. IN): transfer mode to use, default single
    # globals: $poolconfig (IN): location of pool.config file
    #          $tcdata (IN): location of tc.data file
    #          $catalog (IN): RLS to use
    #          $userprops (IN): overwrite for user properties
    # returns: prints property string and returns
    local prop=" -Dvds.properties=/dev/null -Dvds.user.properties=${userprops}"
    prop="${prop} -Dvds.db.vdc.schema=SingleFileSchema" 
    prop="${prop} -Dvds.db.vdc.schema.file.store=$here/vds.db"
    prop="${prop} -Dvds.tc.file=$tcdata"
    prop="${prop} -Dvds.pool.file=$poolconfig"
    [ "$1" ] && prop="${prop} -Dvds.transfer.mode=$1"
    prop="${prop} -Dvds.rls.url=$catalog"
    prop="${prop} -Dvds.replica.mode=rls"

    echo "$prop"
    return 0
}

create_chimera_properties () {
    # purpose: create a set of properties to use for tests
    # globals: $userprops (IN): overwrite for user properties
    # returns: prints property string and returns
    local prop=" -Dvds.properties=/dev/null -Dvds.user.properties=${userprops}"
    prop="${prop} -Dvds.db.vdc.schema=SingleFileSchema" 
    prop="${prop} -Dvds.db.vdc.schema.file.store=$here/vds.db"

    echo "$prop"
    return 0
}

create_pegasus_properties () {
    # purpose: create a set of properties to use for tests
    # paramtr: $mode (opt. IN): transfer mode to use, default single
    # globals: $poolconfig (IN): location of pool.config file
    #          $tcdata (IN): location of tc.data file
    #          $catalog (IN): RLS to use
    #          $userprops (IN): overwrite for user properties
    # returns: prints property string and returns
    local prop=" -Dvds.properties=/dev/null -Dvds.user.properties=${userprops}"
    prop="${prop} -Dvds.tc.file=$tcdata"
    prop="${prop} -Dvds.pool.file=$poolconfig"
    [ "$1" ] && prop="${prop} -Dvds.transfer.mode=$1"
    prop="${prop} -Dvds.rls.url=$catalog"
    prop="${prop} -Dvds.replica.mode=rls"

    echo "$prop"
    return 0
}


wait_for_dagman () {
    # purpose: wait for DAGMan to finish
    # paramtr: $log (IN): DAGMan's own log file
    while true; do
	date | tr '\012' '\011'
	echo "checking status of DAG..."
	egrep -i '(termination|Job was aborted by the user)' $1 >> /dev/null 
	test $? -eq 0 && break
	sleep 5
    done

    return 0
}
