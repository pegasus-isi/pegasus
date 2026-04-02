#!/bin/bash

strip_quotes() {
    # purpose: strip quotes from a variable value
    # paramtr: $str: the variable that needs to be stripped of quotes
    #         
    #         
    # returns: stripped out value
    echo ${1//\"/}
}

if [ -n "$PROJECT" ]; then
    # strip any quotes . e.g 16:IB condor expects it to be quoted
    # however lsf does not like it
    echo "#BSUB -P $(strip_quotes $PROJECT)"
fi

#if [ -n "$QUEUE" ]; then
#    echo "#BSUB -q $(strip_quotes $QUEUE)"
#fi

if [ -n "$JOBNAME" ]; then
    echo "#BSUB -J $(strip_quotes $JOBNAME)"
fi

if [ -n "$WALLTIME" ]; then
    echo "#BSUB -W $(strip_quotes $WALLTIME)"
fi

#### STRIPPED_REQUEST value strips out the first and last single-quote
#### LSF EXPERT MODE
#### -R "select[selection_string] order[order_string] rusage[usage_string [, usage_string][|| usage_string] ...] span[span_string] same[same_string] cu[cu_string]] affinity[affinity_string]"
#### eg. -R "select[hname!='host01'] rusage[mem=1024]"
expert=""
if [ -n "$REQUEST" ]; then
    #### This is required to enter Expert Mode on Summit ####
    expert="expert"
    echo "#BSUB -csm y"
    ####
    echo "#BSUB -R $(strip_quotes $REQUEST)"
fi

if [ -n "$ALLOC_FLAGS" ]; then
    echo "#BSUB -alloc_flags $(strip_quotes $ALLOC_FLAGS)"
fi

if [[ -n "$NODES" && !(-n "$expert") ]]; then
    echo "#BSUB -nnodes $(strip_quotes $NODES)"
fi

#### OLCF Summit doesn't support requests specifying amount of cores ####
#### If you want to parse requests for cores, uncomment the next segment. ####
#if [[ -n "$CORES" && !(-n "$expert") ]]; then
#    echo "#BSUB -n $(strip_quotes $CORES)"
#fi

if [[ -n "$TOTAL_MEMORY" && !(-n "$expert") ]]; then
    echo "#BSUB -M $(strip_quotes $TOTAL_MEMORY)"
fi

if [ -n "$EXTRA_ARGUMENTS" ]; then
    value=$(strip_quotes "$EXTRA_ARGUMENTS")
    #Split into pairs
    value_arr=${value}
    for i in `seq 0 2 $((${#value_arr[@]}-1))`; do
        echo "#BSUB ${value_arr[$i]} ${value_arr[$(($i+1))]}"
    done
fi
