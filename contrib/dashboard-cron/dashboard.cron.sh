#!/bin/bash

pushd /localhome/bamboo/dashboard-cron > /dev/null
DIR='pegasus'

PID_FILE=dashboard.pid
LOG_FILE=dashboard.log
REV_FILE=dashboard.rev

OLD_REV='r'

if [ -e $REV_FILE ]; then
    OLD_REV=`cat $REV_FILE`
fi

if [[ ! -d $DIR ]]; then
    mkdir $DIR
    pushd $DIR > /dev/null
    git clone git://github.com/pegasus-isi/pegasus.git .
else
    pushd $DIR > /dev/null
    git pull
fi

HEAD_REV=`git rev-list --reverse HEAD | tail -1`

if [[ ! $OLD_REV = $HEAD_REV ]]; then
    /ccg/software/ant/default/bin/ant clean dist
    EC=$?
    popd > /dev/null

    if [ $EC -eq 0 ]; then
        echo $HEAD_REV > $REV_FILE
    else
        popd > /dev/null
        exit 0
    fi

    export PATH=`pwd`/`find $DIR/dist/ -mindepth 1 -maxdepth 1 -type d`/bin:$PATH

    if [ -e $PID_FILE ]; then
        kill `cat $PID_FILE`
    fi

    pegasus-service --host 0.0.0.0 --port 5000 -d &>> $LOG_FILE &

    PID=$!

    echo $PID > $PID_FILE
fi

popd > /dev/null
