#!/bin/bash

strip_quotes() {
    echo ${1//\"/}
}

if [ -n "${NODES}" ]; then
    nodes="#MSUB -l nodes=$(strip_quotes $NODES)"
    if [ -n "${PROCS}" ]; then
        nodes="${nodes}:ppn=$(strip_quotes $PROCS)"
    fi
    echo $nodes
fi

if [ -n "${WALLTIME}" ]; then
    echo "#MSUB -l walltime=$(strip_quotes $WALLTIME)"
fi

if [ -n "${PER_PROCESS_MEMORY}" ]; then
    echo "#MSUB -l pmem=$(strip_quotes $PER_PROCESS_MEMORY)"
fi

if [ -n "${TOTAL_MEMORY}" ]; then
    echo "#MSUB -l mem=$(strip_quotes $TOTAL_MEMORY)"
fi

if [ -n "${QUEUE}" ]; then
    echo "#MSUB -q ${QUEUE}"
fi

if [ -n "${PROJECT}" ]; then
    echo "#MSUB -A ${PROJECT}"
fi

if [ -n "${JOBNAME}" ]; then
    echo "#MSUB -N ${JOBNAME}"
fi

if [ "${PASSENV}" == "1" ]; then
    echo "#MSUB -V"
fi

# if a user passed any extra arguments set them in the end
# for example "-N testjob -l walltime=01:23:45 -l nodes=2"
if [ -n "${EXTRA_ARGUMENTS}" ]; then
    echo "#MSUB $(strip_quotes "$EXTRA_ARGUMENTS")"
fi

