#!/bin/bash

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

#
# This file contains a set of common bash funtions to be used by 
# Pegasus Lite jobs
#
# Author: Mats Rynge <rynge@isi.edu>
#


# a default used if no other worker packages can be found
pegasus_lite_default_system="x86_64_rhel_7"


function pegasus_lite_setup_log()
{
    # PM-1132 set up the log explicitly to a file     
    if [ "X${pegasus_lite_log_file}" != "X" ]; then

        # rename the log file with approprite suffix
        # to ensure they are not ovewritten
        count="000"
        for count in `seq -f "%03g" 0 999`;
        do
            if [ ! -e ${pegasus_lite_log_file}.${count} ] ; then
                break
            fi
        done    
        pegasus_lite_log_file=${pegasus_lite_log_file}.${count}

        # Close STDOUT file descriptor
        exec 1>&-

        # Close STDERR FD
        exec 2>&-

        # Open STDERR to file for writes
        exec 2>$pegasus_lite_log_file

        exec 1>&2
    fi

}

function pegasus_lite_log()
{
    TS=`/bin/date +'%F %H:%M:%S'`
    echo "$TS: $1"  1>&2
}


function pegasus_lite_worker_package()
{
    # many ways of providing worker package
    if pegasus_lite_internal_wp_shipped || pegasus_lite_internal_wp_in_env || pegasus_lite_internal_wp_download; then
        return 0
    fi
    return 1
}


function pegasus_lite_internal_wp_shipped()
{
    # was the job shipped with a Pegasus worker package?
    if ls $pegasus_lite_start_dir/pegasus-worker-*.tar.gz >/dev/null 2>&1; then
        pegasus_lite_log "The job contained a Pegasus worker package"
    
        if [ "X$pegasus_lite_enforce_strict_wp_check" = "Xtrue" ]; then
            # make sure the provided worker package provided is for the this platform
            system=$(pegasus_lite_get_system)
            if [ $? = 0 ]; then
                wp_name=`(cd $pegasus_lite_start_dir && ls pegasus-worker-*.tar.gz | head -n 1) 2>/dev/null`
                if ! (echo "x$wp_name" | grep "$system") >/dev/null 2>&1 ; then
                    pegasus_lite_log "Warning: worker package $wp_name does not seem to match the system $system"
                    return 1
                fi 
            fi
        else
            pegasus_lite_log "Skipping sanity check of included worker package because pegasus.transfer.worker.package.strict=$pegasus_lite_enforce_strict_wp_check"
        fi

        tar xzf $pegasus_lite_start_dir/pegasus-worker-*.tar.gz
        unset PEGASUS_HOME
        export PATH=${pegasus_lite_work_dir}/pegasus-${pegasus_lite_full_version}/bin:$PATH
        return 0
    fi
    return 1
}


function pegasus_lite_internal_wp_in_env()
{
    old_path=$PATH

    # use PEGASUS_HOME if set
    if [ "x$PEGASUS_HOME" != "x" ]; then
        PATH="$PEGASUS_HOME/bin:$PATH"
        export PATH
    fi

    # is there already a pegasus install in our path?
    detected_pegasus_bin=`which pegasus-config 2>/dev/null || /bin/true`
    if [ "x$detected_pegasus_bin" != "x" ]; then
        detected_pegasus_bin=`dirname $detected_pegasus_bin`

        # does the version match?
        if $detected_pegasus_bin/pegasus-config --version 2>/dev/null | grep -E "${pegasus_lite_version_major}\.${pegasus_lite_version_minor}\.${pegasus_lite_version_patch}\$" >/dev/null 2>/dev/null; then
            pegasus_lite_log "Using existing Pegasus binaries in $detected_pegasus_bin"
            return 0
        else
            pegasus_lite_log "Warning: Pegasus binaries in $detected_pegasus_bin do not match Pegasus version used for current workflow"
        fi
    fi

    # back out env changes
    unset PEGASUS_HOME
    PATH=$old_path
    export PATH

    return 1
}


function pegasus_lite_internal_wp_download() 
{
    # fall back - download a worker package from download.pegasus.isi.edu

    if [ "X$pegasus_lite_version_allow_wp_auto_download" != "Xtrue" ]; then
        pegasus_lite_log "Not downloading a worker package because pegasus.transfer.worker.package.autodownload=$pegasus_lite_version_allow_wp_auto_download"
        return 1
    fi

    system=$(pegasus_lite_get_system)
    if [ $? != 0 ]; then
        # not sure what system we are on - try the default package
        system="x86_64_rhel_6"
    fi

    # Before we download from the Pegasus server, see if we can find a version
    # deployed on the infrastructure, for example on CVMFS on OSG
    cvmfs_base="/cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/worker/${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}/$system"
    if [ -f "$cvmfs_base/bin/pegasus-config" ]; then
        pegasus_lite_log "Using ${cvmfs_base} as worker package"
        unset PEGASUS_HOME
        export PATH="${cvmfs_base}/bin:$PATH"
        return 0
    fi
    
    # Nevermind, download directly from Pegasus server
    url="http://download.pegasus.isi.edu/pegasus/${pegasus_lite_version_major}"
    url="${url}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}"
    url="${url}/pegasus-worker"
    url="${url}-${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}"
    url="${url}-${system}.tar.gz"
    pegasus_lite_log "Downloading Pegasus worker package from $url"
    curl -s -S --insecure -o pegasus-worker.tar.gz "$url" || wget -q -O pegasus-worker.tar.gz "$url"
    if ! (test -e pegasus-worker.tar.gz && tar xzf pegasus-worker.tar.gz); then
        pegasus_lite_log "ERROR: Unable to download a worker package for this platform ($system)."
        pegasus_lite_log "If you want to use the same package as on the submit host, try the following setting in your properties file:"
        pegasus_lite_log "   pegasus.transfer.worker.package.strict = false"

        # try the default worker package
        pegasus_lite_log "Will try the default worker package ($pegasus_lite_default_system)"
        url="http://download.pegasus.isi.edu/pegasus/${pegasus_lite_version_major}"
        url="${url}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}"
        url="${url}/pegasus-worker"
        url="${url}-${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}"
        url="${url}-${pegasus_lite_default_system}.tar.gz"
        pegasus_lite_log "Downloading Pegasus worker package from $url"
        curl -s -S --insecure -o pegasus-worker.tar.gz "$url" || wget -q -O pegasus-worker.tar.gz "$url"
        if ! (test -e pegasus-worker.tar.gz && tar xzf pegasus-worker.tar.gz); then
            pegasus_lite_log "ERROR: Unable to download the default worker package."
            return 1
        fi
    fi

    rm -f pegasus-worker.tar.gz

    unset PEGASUS_HOME
    export PATH="${pegasus_lite_work_dir}/pegasus-${pegasus_lite_full_version}/bin:$PATH"
}


function pegasus_lite_setup_work_dir()
{
    # remember where we started from
    pegasus_lite_start_dir=`pwd`

    #check if there are any lof files to transfer
    set +e
    ls $pegasus_lite_start_dir/*lof > /dev/null 2>&1
    if [ "$?" = "0" ]; then
        found_lof="true"
    fi
    set -e

    if [ "x$pegasus_lite_work_dir" != "x" ]; then
        pegasus_lite_log "Not creating a new work directory as it is already set to $pegasus_lite_work_dir"
        
        if [ "x$found_lof" != "x" ]; then 
            if [ ! $pegasus_lite_start_dir -ef $pegasus_lite_work_dir ]; then
                 #PM-1021 copy all lof files from Condor scratch dir to directory where pegasus lite runs the job
                pegasus_lite_log "Copying lof files from $pegasus_lite_start_dir to $pegasus_lite_work_dir"
                cp $pegasus_lite_start_dir/*lof $pegasus_lite_work_dir
            fi
        fi

        return
    fi

    targets="$PEGASUS_WN_TMP $_CONDOR_SCRATCH_DIR $OSG_WN_TMP $TG_NODE_SCRATCH $TG_CLUSTER_SCRATCH $SCRATCH $TMPDIR $TMP /tmp"
    unset TMPDIR

    if [ "x$PEGASUS_WN_TMP_MIN_SPACE" = "x" ]; then
        PEGASUS_WN_TMP_MIN_SPACE=1000000
    fi

    for d in $targets; do

        pegasus_lite_log "Checking $d for potential use as work space... " 

        # does the target exist?
        if [ ! -e $d ]; then
            pegasus_lite_log "  Workdir: $d does not exist"
            continue
        fi

        # make sure there is enough available diskspace
        cd $d
        free=`df -kP . | awk '{if (NR==2) print $4}'`
        free_human=`df --si . | awk '{if (NR==2) print $4}'`
        if [ "x$free" == "x" -o $free -lt $PEGASUS_WN_TMP_MIN_SPACE ]; then
            pegasus_lite_log "  Workdir: not enough disk space available in $d"
            continue
        fi

        if touch $d/.dirtest.$$ >/dev/null 2>&1; then
            rm -f $d/.dirtest.$$ >/dev/null 2>&1
            d=`mktemp -d $d/pegasus.XXXXXXXXX`
            export pegasus_lite_work_dir=$d
            export pegasus_lite_work_dir_created=1
            pegasus_lite_log "  Workdir is $d - $free_human available"

            # PM-968 if provided, copy lof files from the HTCondor iwd to the PegasusLite work dir
            find $pegasus_lite_start_dir -name \*.lof -exec cp {} $pegasus_lite_work_dir/ \; >/dev/null 2>&1

	    # PM-1190 if provided, copy meta files from the HTCondor iwd to the PegasusLite work dir
            find $pegasus_lite_start_dir -name \*.meta -exec cp {} $pegasus_lite_work_dir/ \; >/dev/null 2>&1

            pegasus_lite_log "Changing cwd to $pegasus_lite_work_dir"
            cd $pegasus_lite_work_dir
           
            if [ "x$found_lof" != "x" ]; then
                #PM-1021 make sure pegasus_lite_work_dir and start dir are not same
                if [ ! $pegasus_lite_start_dir -ef $pegasus_lite_work_dir ]; then
                    # copy all lof files from Condor scratch dir to directory where pegasus lite runs the job
                    pegasus_lite_log "Copying lof files from $pegasus_lite_start_dir to $pegasus_lite_work_dir"
                    cp $pegasus_lite_start_dir/*lof $pegasus_lite_work_dir
                fi
            fi

            return 0
        fi
        pegasus_lite_log "  Workdir: not allowed to write to $d"
    done
    return 1
}

function container_init()
{
    # setup common variables
    cont_userid=`id -u`
    cont_user=`whoami`
    cont_groupid=`id -g`
    cont_group=`id -g -n $cont_user` 
    cont_name=${PEGASUS_DAG_JOB_ID}-`date -u +%s`

}

function docker_init()
{
    set -e

    container_init
    
    if [ $# -ne 1 ]; then 
	pegasus_lite_log "docker_init should be passed a docker url or a file"
	return 1
    fi

    # check if an image file was passed
    image_file=$1
    cont_image=""

    if [ "X${image_file}" != "X" ] ; then
		
	if [ -e ${image_file} ] ; then
	    pegasus_lite_log "container file is ${image_file}"
	    # try and load the image
	    images=`docker load -i ${image_file} | sed -E "s/^Loaded image:(.*)$/\1/"`

	    #docker load can list multiple images, which might be aliases for same image
	    for image in $images ; do
		cont_image=$image
	    done
	fi
	
    fi
    
    if [ "X${cont_image}" = "X" ]; then
	pegasus_lite_log "Unable to load image from $image_file"
	return 1
    else
	pegasus_lite_log "Loaded docker image $cont_image"
    fi

    set +e
}

function singularity_init()
{
    set -e

    # set the common variables used in the pegasus lite job.sh files
    container_init
    
    if [ $# -ne 1 ]; then 
	pegasus_lite_log "singularity_init should be passed a docker url or a file"
	return 1
    fi

    # for singularity we don't need to load anything like in docker.
    
}

function pegasus_lite_init()
{
    pegasus_lite_full_version=${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}

    # setup pegasus lite log
    pegasus_lite_setup_log

    # announce version - we do this so pegasus-exitcode and other tools
    # can tell the job was a PegasusLite job
    pegasus_lite_log "PegasusLite: version ${pegasus_lite_full_version}" 1>&2

    # PM-1134 - provide some details on where we are running
    # PM-1144 - do not use HOSTNAME from env, as it might have come form getenv=true
    out="Executing on"
    if hostname -f >/dev/null 2>&1; then
        out="$out host "`hostname -f`
    fi
    if [ "x$OSG_SITE_NAME" != "x" ]; then
        out="$out OSG_SITE_NAME=${OSG_SITE_NAME}"
    fi
    if [ "x$GLIDEIN_Site" != "x" ]; then
        out="$out GLIDEIN_Site=${GLIDEIN_Site}"
    fi
    if [ "x$GLIDEIN_ResourceName" != "x" ]; then
        out="$out GLIDEIN_ResourceName=${GLIDEIN_ResourceName}"
    fi
    pegasus_lite_log "$out"

    # for staged credentials, expand the paths and set strict permissions
    for base in X509_USER_PROXY S3CFG BOTO_CONFIG SSH_PRIVATE_KEY irodsEnvFile GOOGLE_PKCS12 ; do
        for key in `(env | grep -i ^$base | sed 's/=.*//') 2>/dev/null`; do
            eval val="\$$key"
            # expand the path
            if ! (echo $val | grep "^/") >/dev/null 2>&1; then
                eval $key=`pwd`/"$val"
                eval val="\$$key"
                pegasus_lite_log "Expanded \$$key to $val"
            fi
            if [ -e "$val" ]; then
                chmod 0600 "$val"
            fi
        done
    done

}


function pegasus_lite_signal_int()
{
    # remember the fact until we call the EXIT function
    caught_signal_name="INT"
    caught_signal_num=2
}


function pegasus_lite_signal_term()
{
    # remember the fact until we call the EXIT function
    caught_signal_name="TERM"
    caught_signal_num=15
}


function pegasus_lite_unexpected_exit()
{
    # note that there are two exit() functions, one for final
    # exit and one for unexepected. The final one is only called
    # at the last step of the lite script. The unexpected one
    # can be called anytime if the script exists as a result 
    # of for example signals
    rc=$?
    if [ "x$caught_signal_name" != "x" ]; then
        # if we got a signal, always fail the job
        pegasus_lite_log "Caught $caught_signal_name signal! Aborting..."
        rc=$(($caught_signal_num + 128))
    elif [ "x$rc" = "x" ]; then
        # when would this actually happen?
        pegasus_lite_log "Unable to determine real exit code!"
        rc=1
    elif [ $rc -gt 0 ]; then
        pegasus_lite_log "Last command exited with $rc"
    fi

    # never allow the script to exit with 0 in case of 
    # unexpected termination
    if [ "x$rc" = "x0" ]; then
        pegasus_lite_log "Unable to determine why the script was forced to exit!"
        rc=1
    fi

    if [ "x$pegasus_lite_work_dir_created" = "x1" ]; then
        cd /
        rm -rf $pegasus_lite_work_dir
        pegasus_lite_log "$pegasus_lite_work_dir cleaned up"
    fi

    echo "PegasusLite: exitcode $rc" 1>&2

    exit $rc
}


function pegasus_lite_final_exit()
{
    # note that there are two exit() functions, one for final
    # exit and one for unexepected. The final one is only called
    # at the last step of the lite script. The unexpected one
    # can be called anytime if the script exists as a result 
    # of for example signals
    rc=1
    
    # the exit code of the lite script should reflect the exit code
    # from the user task    
    if [ "x$job_ec" = "x" ];then
        pegasus_lite_log "job_ec is missing - did the user task fail?"
    else
        if [ $job_ec != 0 ];then
            pegasus_lite_log "User task failed with exitcode $job_ec"
        fi
        rc=$job_ec
    fi

    if [ "x$pegasus_lite_work_dir_created" = "x1" ]; then
        cd /
        rm -rf $pegasus_lite_work_dir
        pegasus_lite_log "$pegasus_lite_work_dir cleaned up"
    fi

    echo "PegasusLite: exitcode $rc" 1>&2

    exit $rc
}


function pegasus_lite_get_system()
{
    # PM-781
    # This function is a replacement of the old release-tools/getsystem
    # and was moved here because we need the getsystem functionallity not
    # only at build time, but at runtime from the jobs so that the jobs
    # can determine what worker package is required.

    # The goal is to get a triple identify the system:
    # arch _ osname _ osversion
    # for example: x86_64_deb_7

    arch=`uname -m 2>&1` || arch="UNKNOWN"
    osname=`uname -s 2>&1` || osname="UNKNOWN"
    osversion=`uname -r 2>&1` || osversion="UNKNOWN"
        
    if (echo $arch | grep -E '^i[0-9]86$') >/dev/null 2>&1; then
        arch="x86" 
    fi

    if [ "$osname" = "Linux" ]; then

        # /etc/issue works most of the time, but there are exceptions
        osname=`cat /etc/issue | head -n1 | awk '{print $1;}' | tr '[:upper:]' '[:lower:]'`

        if [ "X$osname" = "Xubuntu" ]; then
            osversion=`cat /etc/issue | head -n1 | awk '{print $2;}'` 
            # 18 LTS
            if (grep -i "bionic" /etc/issue) >/dev/null 2>&1; then
                osversion="18"
            fi
        elif [ -e /etc/debian_version ]; then
            osname="deb"
            osversion=`cat /etc/debian_version`
            # yet to be released Debian 10
            if (echo "$osversion" | grep "buster") >/dev/null 2>&1; then
                osversion="10"
            fi
        elif [ -e /etc/fedora-release ]; then
            osname="fedora"
            osversion=`cat /etc/fedora-release | grep -o -E '[0-9]+'`
        elif [ -e /etc/redhat-release ]; then
            osname="rhel"
            osversion=`cat /etc/redhat-release | grep -o -E ' [0-9]+.[0-9]+'`
        elif [ -e /etc/rocks-release ]; then
            osname="rhel"
            osversion=`cat /etc/rocks-release | grep -o -E ' [0-9]+.[0-9]+'`
        elif [ -e /etc/SuSE-release ]; then
            osname="suse"
            osversion=`cat /etc/SuSE-release | grep VERSION | grep -o -E ' [0-9]+'`
        fi
        
        # remove spaces/tabs in the version
        osversion=`echo $osversion | sed 's/[ \t]//g'`

        # remove / in the version
        osversion=`echo $osversion | sed 's/\//_/g'`

        # we only want major version numbers
        osversion=`echo $osversion | sed 's/[\.-].*//'`

        echo "${arch}_${osname}_${osversion}"
        return 0
    fi

    if [ "$osname" = "Darwin" ]; then
        osname="macos"
        osversion=`/usr/bin/sw_vers -productVersion`
        
        # we only want major version numbers
        osversion=`echo $osversion | sed 's/[\.-].*//'`

        echo "${arch}_${osname}_${osversion}"
        return 0
    fi
    
    if [ "$osname" = "FreeBSD" ]; then
        osname="freebsd"
        
        # we only want major version numbers
        osversion=`echo $osversion | sed 's/[\.-].*//'`

        echo "${arch}_${osname}_${osversion}"
        return 0
    fi
        
    # unable to determine detailed system information
    echo "${arch}_${osname}_${osversion}"
    return 1
}

