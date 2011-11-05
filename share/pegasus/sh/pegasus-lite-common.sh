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


function pegasus_lite_log()
{
    TS=`/bin/date +'%F %H:%M:%S'`
    echo "$TS: $1" 1>&2
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
        pegasus_lite_log "The job contained a Pegasus worker package, installing to $pegasus_lite_work_dir/pegasus-worker"
        tar xzf $pegasus_lite_start_dir/pegasus-worker-*.tar.gz
        mv pegasus-worker-* pegasus-worker
        unset PEGASUS_HOME
        export PATH=$pegasus_lite_work_dir/pegasus-worker/bin:$PATH
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
    detected_pegasus_bin=`which pegasus-version 2>&1 || /bin/true`
    if [ "x$detected_pegasus_bin" != "x" ]; then
        detected_pegasus_bin=`dirname $detected_pegasus_bin`

        # does the version match?
        if $detected_pegasus_bin/pegasus-version 2>/dev/null | grep -E "^${pegasus_lite_version_major}\.${pegasus_lite_version_minor}\." >/dev/null 2>/dev/null; then
            pegasus_lite_log "Using existing Pegasus binaries in $detected_pegasus_bin"
            return 0
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
    # fall back - download a worker package from pegasus.isi.edu
    os=rhel
    major=5

    arch=`uname -m`
    if [ $arch != "x86_64" ]; then
        arch="x86"
    fi

    if [ -e /etc/redhat-release ]; then
        os=rhel
        major=`cat /etc/redhat-release | sed 's/.*release //' | sed 's/[\. ].*//'`
    else
        if [ -e /etc/debian_version ]; then
            os=deb
            major=`cat /etc/debian_version | sed 's/\..*//'`
        fi
    fi
    
    url="http://pegasus.isi.edu/wms/download/${pegasus_lite_version_major}"
    url="${url}.${pegasus_lite_version_minor}"
    if echo ${pegasus_lite_version_patch} | grep cvs >/dev/null 2>/dev/null; then
        url="${url}/nightly"
    fi
    url="${url}/pegasus-worker"
    url="${url}-${pegasus_lite_version_major}.${pegasus_lite_version_minor}.${pegasus_lite_version_patch}"
    url="${url}-${arch}_${os}_${major}.tar.gz"
    pegasus_lite_log "Downloading Pegasus worker package from $url"
    wget -q -O pegasus-worker.tar.gz "$url"
    tar xzf pegasus-worker.tar.gz
    rm -f pegasus-worker.tar.gz
    mv pegasus-* pegasus-worker

    unset PEGASUS_HOME
    export PATH=$pegasus_lite_work_dir/pegasus-worker/bin:$PATH
}


function pegasus_lite_setup_work_dir()
{
    # remember where we started from
    pegasus_lite_start_dir=`pwd`

    if [ "x$pegasus_lite_work_dir" != "x" ]; then
        pegasus_lite_log "Not creating a new work directory as it is already set to $pegasus_lite_work_dir"
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
        if [ "x$free" == "x" -o $free -lt $PEGASUS_WN_TMP_MIN_SPACE ]; then
            pegasus_lite_log "  Workdir: not enough disk space available in $d"
            continue
        fi

        if touch $d/.dirtest.$$ >/dev/null 2>&1; then
            rm -f $d/.dirtest.$$ >/dev/null 2>&1
            d=`mktemp -d $d/pegasus.XXXXXX`
            export pegasus_lite_work_dir=$d
            export pegasus_lite_work_dir_created=1
            pegasus_lite_log "  Work dir is $d"
            cd $pegasus_lite_work_dir
            return 0
        fi
        pegasus_lite_log "  Workdir: not allowed to write to $d"
    done
    return 1
}


function pegasus_lite_exit()
{
    rc=$?
    if [ "x$rc" = "x" ]; then
        rc=0
    fi
    if [ $rc != 0 ]; then
        pegasus_lite_log "FAILURE: Last command exited with $rc"
    fi

    if [ "x$pegasus_lite_work_dir_created" = "x1" ]; then
        cd /
        rm -rf $pegasus_lite_work_dir
        pegasus_lite_log "$pegasus_lite_work_dir cleaned up"
    fi

    exit $rc
}


