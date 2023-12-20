#!/bin/sh

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

# ensure we have a good working environment
if [ "X$PATH" = "X" ]; then
    PATH=/usr/bin:/bin
    export PATH
fi

# a default used if no other worker packages can be found
pegasus_lite_default_system="x86_64_rhel_7"

# remember where we started from
pegasus_lite_start_dir=`pwd`

pegasus_lite_setup_log()
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

pegasus_lite_log()
{
    TS=`/bin/date +'%F %H:%M:%S'`
    echo "$TS: $1"  1>&2
}


pegasus_lite_worker_package()
{
    # many ways of providing worker package
    if pegasus_lite_internal_wp_shipped || pegasus_lite_wp_untarred || pegasus_lite_internal_wp_in_env || pegasus_lite_internal_wp_download; then
        return 0
    fi
    return 1
}


pegasus_lite_internal_wp_shipped()
{
    system=$(pegasus_lite_get_system)

    # was the job shipped with a Pegasus worker package?
    if ls $pegasus_lite_start_dir/pegasus-worker-*.tar.gz >/dev/null 2>&1; then
        pegasus_lite_log "The job contained a Pegasus worker package"
    
        if [ "X$pegasus_lite_enforce_strict_wp_check" = "Xtrue" ]; then
            # make sure the provided worker package provided is for the this platform
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

        # determine the path of the worker package - this might not match the planner/lite versions
        # as the user might have specified a different worker package
        worker_package_dir=$(tar tzf $pegasus_lite_start_dir/pegasus-worker-*.tar.gz | head | sed 's;/.*;;' | sort | uniq)

        mv ${worker_package_dir} ${worker_package_dir}-${system}
        unset PEGASUS_HOME
        PATH=${pegasus_lite_work_dir}/${worker_package_dir}-${system}/bin:$PATH
        export PATH
        return 0
    fi
    return 1
}

pegasus_lite_wp_untarred()
{
    system=$(pegasus_lite_get_system)

    # PM-1894 does the job have access to an untarred pegasus worker.
    # can happen in case when job runs in application container
    # and worker package has already been untarred in pegasuslite
    # on the HOSTOS
    pegasus_lite_log "Checking for untar worker package in $pegasus_lite_start_dir"
    untar_dir=$(ls -d $pegasus_lite_start_dir/* | grep -E 'pegasus-[0-9]+\.[0-9]+\.[0-9]+[a-zA-Z0-9]*-*') >/dev/null 2>&1
    if [ "X$untar_dir" != "X" ]; then
        pegasus_lite_log "The job contained a Pegasus worker package that was already untarred in $untar_dir"
    
        if [ "X$pegasus_lite_enforce_strict_wp_check" = "Xtrue" ]; then
	    # make sure the provided worker package provided is for the this platform
	    if [ $? = 0 ]; then
                wp_name=`(cd $pegasus_lite_start_dir && ls -d $untar_dir | head -n 1) 2>/dev/null`
                if ! (echo "x$wp_name" | grep "$system") >/dev/null 2>&1 ; then
		    pegasus_lite_log "Warning: worker package $wp_name does not seem to match the system $system"
		    return 1
                fi 
	    fi
        else
	    pegasus_lite_log "Skipping sanity check of included worker package because pegasus.transfer.worker.package.strict=$pegasus_lite_enforce_strict_wp_check"
        fi

        # determine the path of the worker package - this might not match the planner/lite versions
        # as the user might have specified a different worker package
        worker_package_dir=$untar_dir

	# extra failsafe . attempt to see if we can run pegasus-kickstart out of untar dir
	if ! (${worker_package_dir}/bin/pegasus-kickstart true) >/dev/null 2>&1 ; then
	    pegasus_lite_log "Warning: unable to execute pegasus-kickstart out of $untar_dir . Will not use this install."
	    return 1
	fi
	
        unset PEGASUS_HOME
        PATH=${worker_package_dir}/bin:$PATH
        export PATH
        return 0
    fi

    return 1
}


pegasus_lite_internal_wp_in_env()
{
    old_path=$PATH

    # use PEGASUS_HOME if set
    if [ "x$PEGASUS_HOME" != "x" ]; then
        PATH="$PEGASUS_HOME/bin:$PATH"
        export PATH
    fi

    # is there already a pegasus install in our path?
    detected_pegasus_bin=`which pegasus-config 2>/dev/null || true`
    if [ "x$detected_pegasus_bin" != "x" ]; then
        detected_pegasus_bin=`dirname $detected_pegasus_bin`

        # does the version match?
        if $detected_pegasus_bin/pegasus-config --version 2>/dev/null | grep -E "${pegasus_lite_version_major}\.${pegasus_lite_version_minor}\.${pegasus_lite_version_patch}\$" >/dev/null 2>/dev/null; then
            pegasus_lite_log "Using existing Pegasus binaries in $detected_pegasus_bin"
            return 0
        else
            pegasus_lite_log "Warning: Pegasus binaries in $detected_pegasus_bin do not match Pegasus version used for current workflow"
        fi
    else
        # catch the case where a user has specified a faulty PEGASUS_HOME
        if [ "x$PEGASUS_HOME" != "x" ]; then
            pegasus_lite_log "Warning: PEGASUS_HOME was specified, but did not contain a Pegasus install. Unsetting PEGASUS_HOME."
        fi
    fi

    # back out env changes
    unset PEGASUS_HOME
    PATH=$old_path
    export PATH

    return 1
}


pegasus_lite_internal_wp_download() 
{
    # fall back - download a worker package from download.pegasus.isi.edu

    if [ "X$pegasus_lite_version_allow_wp_auto_download" != "Xtrue" ]; then
        pegasus_lite_log "Not downloading a worker package because pegasus.transfer.worker.package.autodownload=$pegasus_lite_version_allow_wp_auto_download"
        return 1
    fi

    system=$(pegasus_lite_get_system)
    if [ $? != 0 ]; then
        # not sure what system we are on - try the default package
        system="x86_64_rhel_7"
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
    curl -f -s -S --insecure -o pegasus-worker.tar.gz "$url" || wget -q -O pegasus-worker.tar.gz "$url"
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
        curl -f -s -S --insecure -o pegasus-worker.tar.gz "$url" || wget -q -O pegasus-worker.tar.gz "$url"
        if ! (test -e pegasus-worker.tar.gz && tar xzf pegasus-worker.tar.gz); then
            pegasus_lite_log "ERROR: Unable to download the default worker package."
            return 1
        fi
    fi

    mv pegasus-${pegasus_lite_full_version} pegasus-${pegasus_lite_full_version}-${system}
    rm -f pegasus-worker.tar.gz

    unset PEGASUS_HOME
    PATH="${pegasus_lite_work_dir}/pegasus-${pegasus_lite_full_version}-${system}/bin:$PATH"
    export PATH
}


pegasus_lite_setup_work_dir()
{
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
        free=`(df -kP . | awk '{if (NR==2) print $4}') 2>/dev/null`
        free_human=`(df --si . | awk '{if (NR==2) print $4}') 2>/dev/null`
        if [ "x$free" != "x" ]; then
            if [ $free -lt $PEGASUS_WN_TMP_MIN_SPACE ]; then
                pegasus_lite_log "  Workdir: not enough disk space available in $d"
                continue
            fi
        fi

        if touch $d/.dirtest.$$ >/dev/null 2>&1; then
            rm -f $d/.dirtest.$$ >/dev/null 2>&1
            d=`mktemp -d $d/pegasus.XXXXXXXXX`
            pegasus_lite_work_dir=$d
            export pegasus_lite_work_dir
            pegasus_lite_work_dir_created=1
            export pegasus_lite_work_dir_created
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

container_env()
{
    # This function will grab environment variables and update them for use inside the container.
    # Updated variables will be echoed to stdout, so the result can be redirected into the 
    # container script.

    inside_work_dir=$1

    # copy credentials into the pwd as this will become the container directory
    for base in PEGASUS_CREDENTIALS X509_USER_PROXY S3CFG BOTO_CONFIG SSH_PRIVATE_KEY IRODS_ENVIRONMENT_FILE GOOGLE_PKCS12 _CONDOR_CREDS ; do
        for key in `(env | grep -i ^${base} | sed 's/=.*//') 2>/dev/null`; do
            eval val="\$$key"
            if [ "X${val}" = "X" ]; then
                pegasus_lite_log "Credential $key evaluated to empty"
                continue
            fi
            cred="`basename ${val}`"
            dest="`pwd`/$cred"
            dest_inside="$inside_work_dir/$cred"
            if [ ! -e $dest ] ; then
                cp -R $val $dest
                chmod 600 $dest
                pegasus_lite_log "Copied credential \$$key to $dest"
                echo "export $key=\"$dest_inside\""
                pegasus_lite_log "Set \$$key to $dest_inside (for inside the container)"
            fi
        done
    done
        
    echo "export PEGASUS_MULTIPART_DIR=$inside_work_dir/.pegasus.mulitpart.d"
}

container_init()
{
    # setup common variables
    cont_userid=`id -u`
    cont_user=`whoami`
    cont_groupid=`id -g`
    cont_group=`id -g -n $cont_user`
    cont_name="${PEGASUS_DAG_JOB_ID}-${PEGASUS_WF_UUID}"
}

docker_init()
{
    set -e

    if [ $# -ne 1 ]; then 
        pegasus_lite_log "docker_init should be passed a docker url or a file"
        return 1
    fi
    
    container_init

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

singularity_init()
{
    set -e

    if [ $# -ne 1 ]; then 
        pegasus_lite_log "singularity_init should be passed a docker url or a file"
        return 1
    fi

    # prefer apptainer executable if it exists
    singularity_exec=`which apptainer 2>/dev/null || true`
    if [ "X$singularity_exec" = "X" ]; then
	singularity_exec=`which singularity 2>/dev/null || true`
    fi

    if [ "X$singularity_exec" = "X" ]; then
	pegasus_lite_log "Unable to find apptainer or singularity executable"	
	return 1
    fi
    pegasus_lite_log "Using $singularity_exec to run the container"
    export singularity_exec
    
    container_init

    # for singularity we don't need to load anything like in docker.    
}

shifter_init()
{
    set -e

    if [ $# -ne 1 ]; then
        pegasus_lite_log "shifter_init should be passed a docker url or a file"
	return 1
    fi

    container_init

    # for shifter we don't need to load anything like in docker.
}


pegasus_lite_init()
{
    pegasus_lite_full_version=${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}

    # setup pegasus lite log
    pegasus_lite_setup_log

    if [ "X$pegasus_lite_inside_container" != "Xtrue" ]; then
        # announce version - we do this so pegasus-exitcode and other tools
        # can tell the job was a PegasusLite job
        pegasus_lite_log "PegasusLite: version ${pegasus_lite_full_version}" 1>&2
    
        # PM-1134 - provide some details on where we are running
        # PM-1144 - do not use HOSTNAME from env, as it might have come form getenv=true
        out="Executing on"
        my_hostname=`hostname -f 2>/dev/null || true`
        if [ "x$my_hostname" != "x" ]; then
            out="$out host $my_hostname"

            # also IP if we can figure it out
            my_ip=`(host $my_hostname | grep "has address" | sed 's/.* has address //') 2>/dev/null || true`
            if [ "x$my_ip" = "x" ]; then
                # can also try hostname -I
                my_ip=`(hostname -I | sed 's/ .*//') 2>/dev/null || true`
            fi
            if [ "x$my_ip" != "x" ]; then
                out="$out IP=$my_ip"
            fi
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
    fi

    # for staged credentials, expand the paths and set strict permissions
    for base in PEGASUS_CREDENTIALS X509_USER_PROXY S3CFG BOTO_CONFIG SSH_PRIVATE_KEY IRODS_ENVIRONMENT_FILE GOOGLE_PKCS12 ; do
        for key in `(env | grep -i ^$base | sed 's/=.*//') 2>/dev/null`; do
            eval val="\$$key"
            # expand the path
            if ! (echo $val | grep "^/") >/dev/null 2>&1; then
                # the key is passed as a relative path so prepend the pwd
                eval $key=`pwd`/"$val"
                eval val="\$$key"
                pegasus_lite_log "Expanded \$$key to $val"
            else
                # check that the key can be found at the absolute path
                # otherwise look for it in the current directory
                if [ ! -e "$val" ]; then
                    eval $key=`pwd`/`basename $val`
                    eval val="\$$key"
                    pegasus_lite_log "Set \$$key to $val"
                fi
            fi
            if [ -e "$val" ]; then
                chmod 0600 "$val"
            fi
        done
    done

    export PEGASUS_MULTIPART_DIR=`pwd`/.pegasus.mulitpart.d
    mkdir -p $PEGASUS_MULTIPART_DIR
}


pegasus_lite_signal_int()
{
    # remember the fact until we call the EXIT function
    caught_signal_name="INT"
    caught_signal_num=2
}


pegasus_lite_signal_term()
{
    # remember the fact until we call the EXIT function
    caught_signal_name="TERM"
    caught_signal_num=15
}


pegasus_lite_unexpected_exit()
{
    # note that there are two exit() functions, one for final
    # exit and one for unexepected. The final one is only called
    # at the last step of the lite script. The unexpected one
    # can be called anytime if the script exists as a result 
    # of for example signals
    rc=$?

    pegasus_include_multipart || true

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


pegasus_lite_final_exit()
{
    # note that there are two exit() functions, one for final
    # exit and one for unexepected. The final one is only called
    # at the last step of the lite script. The unexpected one
    # can be called anytime if the script exists as a result 
    # of for example signals
    rc=1

    pegasus_include_multipart || true
    
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


pegasus_include_multipart()
{
    if [ "x$PEGASUS_MULTIPART_DIR" != "x" -a -d $PEGASUS_MULTIPART_DIR ]; then
        for entry in `ls $PEGASUS_MULTIPART_DIR/ | sort`; do
            echo
            echo "---------------pegasus-multipart"
            cat $PEGASUS_MULTIPART_DIR/$entry
        done
    fi
}


pegasus_lite_get_system()
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

    # align macos arm arch with linux (->aarch64)
    if [ "X$arch" = "Xarm64" ]; then
        arch="aarch64"
    fi

    if [ "$osname" = "Linux" ]; then

        # /etc/issue and /etc/os-release works most of the time, but there are exceptions
        if [ -e /etc/os-release ]; then
            osname=`grep -w ID /etc/os-release | head -n 1 | tr -d '"' | cut -d '=' -f 2`
            osversion=`grep -w VERSION_ID /etc/os-release | head -n 1 | tr -d '"' | cut -d '=' -f 2`
            
            case $osname in
                "debian") osname="deb" ;;
                "centos"|"rocky"|"scientific") osname="rhel" ;;
                "fedora") osname="fc" ;;
                "sles"|"opensuse-leap") osname="suse" ;;
                *) osname="$osname" ;;
            esac

            # sometimes VERSION_CODENAME is set, but not VERSION_ID
            if [ "X$osversion" = "X" ]; then
                oscodename=$(grep -w VERSION_CODENAME /etc/os-release | head -n 1 | tr -d '"' | cut -d '=' -f 2)
                case $oscodename in
                    "bullseye")  osversion="11" ;;
                    "bookworm")  osversion="12" ;;
                    "trixie")    osversion="13" ;;
                esac
            fi
        elif [ -e /etc/issue ]; then
            osname=`cat /etc/issue | head -n1 | awk '{print $1;}' | tr '[:upper:]' '[:lower:]'`

            if [ "X$osname" = "Xubuntu" ]; then
                osversion=`cat /etc/issue | head -n1 | awk '{print $2;}'` 
                # 18 LTS
                if (grep -i "bionic" /etc/issue) >/dev/null 2>&1; then
                    osversion="18"
                fi
                # 20 LTS
                if (grep -i "focal" /etc/issue) >/dev/null 2>&1; then
                    osversion="20"
                fi
            elif [ -e /etc/debian_version ]; then
                osname="deb"
                osversion=`cat /etc/debian_version | cut -d/ -f1`
                case $oscodename in
                    "buster")    osversion="10" ;;
                    "bullseye")  osversion="11" ;;
                    "bookworm")  osversion="12" ;;
                    "trixie")    osversion="13" ;;
                esac
            elif [ -e /etc/fedora-release ]; then
                osname="fc"
                osversion=`cat /etc/fedora-release | grep -o -E '[0-9]+'`
            elif [ -e /etc/redhat-release ]; then
                osname="rhel"
                osversion=`cat /etc/redhat-release | grep -o -E ' [0-9]+.[0-9]+'`
            elif [ -e /etc/rocks-release ]; then
                osname="rhel"
                osversion=`cat /etc/rocks-release | grep -o -E ' [0-9]+.[0-9]+'`
            elif [ -e /etc/rocky-release ]; then
                osname="rhel"
                osversion=`cat /etc/rocky-release | grep -o -E ' [0-9]+.[0-9]+'`
            elif [ -e /etc/SuSE-release ]; then
                osname="suse"
                osversion=`cat /etc/SuSE-release | grep VERSION | grep -o -E ' [0-9]+'`
            fi
        fi

        # remove spaces/tabs in the version
        osversion=`echo $osversion | sed 's/[ \t]//g'`

        # remove / in the version
        osversion=`echo $osversion | sed 's/\//_/g'`

        # we only want major version numbers
        osversion=`echo $osversion | sed 's/[\.-].*//'`
        
        if [ "X$osname" = "X" -o "X$osversion" = "X" ]; then
            echo "PegasusLite: 1 failed to get system info"
            exit 1
        fi

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


pegasus_lite_section_start()
{
    # stage_in, task_execute, stage_out
    section=$1
    ts=`/bin/date +%s 2>/dev/null`

    # for now, we only want to keep track of the start ts
    eval "pegasus_${section}_start"=$ts
}


pegasus_lite_section_end()
{
    # stage_in, task_execute, stage_out
    section=$1
    ts=`/bin/date +%s 2>/dev/null`

    # calculate duration if we can
    var_name=pegasus_${section}_start
    start_ts=${!var_name}

    if [ "X$start_ts" != "X" ]; then
        duration=$(($ts - $start_ts))
        pegasus_lite_chirp Chirp_pegasus_${section}_start $start_ts
        pegasus_lite_chirp Chirp_pegasus_${section}_duration $duration
    fi
}


pegasus_lite_chirp()
{
    key=$1
    value=$2

    # find/test chirp once here
    if [ "X$pegasus_lite_chirp_path" = "X" ]; then
        condor_libexec=`condor_config_val LIBEXEC 2>/dev/null || true`
        pegasus_lite_chirp_path=`(export PATH=$condor_libexec:$PATH ; which condor_chirp) 2>/dev/null || true`
        if [ "X$pegasus_lite_chirp_path" = "X" ]; then
            pegasus_lite_log "Unable to find condor_chirp - disabling chirping"
            pegasus_lite_chirp_path="none"
            return
        fi
    fi
    if [ "X$pegasus_lite_chirp_path" = "Xnone" ]; then
        # chirp fails - do nothing
        return
    fi

    #pegasus_lite_log "Chirping: $pegasus_lite_chirp_path set_job_attr_delayed $key $value"
    if ! $pegasus_lite_chirp_path set_job_attr_delayed $key $value ; then
        pegasus_lite_log "condor_chirp test failed - disabling chirping"
        pegasus_lite_chirp_path="none"
    fi
}


