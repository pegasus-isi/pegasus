#!/bin/bash

strip_quotes() {
    echo ${1//\"/}
}

if [ -n "${WALLTIME}" ]; then
    echo "#FLUX: --time-limit=$(strip_quotes $WALLTIME)"
fi

# Add if/when Flux allows users to specify memory
# if [ -n "${PER_PROCESS_MEMORY}" ]; then
#     echo "#SBATCH --mem-per-cpu=$(strip_quotes $PER_PROCESS_MEMORY)"
# fi

# Add if/when Flux allows users to specify memory
# if [ -n "${TOTAL_MEMORY}" ]; then
#     echo "#SBATCH --mem=$(strip_quotes $TOTAL_MEMORY)"
# fi

if [ -n "${JOBNAME}" ]; then
    echo "#FLUX: --job-name=${JOBNAME}"
fi

if [ -n "${PROJECT}" ]; then
    echo "#FLUX: --bank=${PROJECT}"
fi

if [ "x${EXCLUSIVE}" == "xyes" ]; then
    echo "#FLUX: --exclusive"
fi

# if a user passed any extra arguments set them in the end
# for example "-N testjob -l walltime=01:23:45 -l nodes=2"
if [ -n "${EXTRA_ARGUMENTS}" ]; then
    echo "#FLUX: $(strip_quotes "$EXTRA_ARGUMENTS")"
fi

# This logic is all duplicated from 'flux_submit.sh'. The only difference
# is the naming of the inputs. Namely, inputs to this script map to inputs in
# 'flux_submit.sh' as follows:
#   * NODES -> bls_opt_hostnumber
#   * PROCS -> bls_opt_smpgranularity
#   * CORES -> bls_opt_mpinodes
#   * GPUS -> bls_opt_gpunumber
__flux_nnodes=
__flux_nslots=
__flux_ncores_per_slot=
__flux_ngpus_per_slot=

if [ -n "${NODES}" ] || ([ -n "${PROCS}" ] && [ -n "${CORES}" ]); then
    if [ -n "${NODES}" ]; then
        __flux_nnodes=${NODES}
    else
        __flux_nnodes=$(( (CORES + PROCS - 1) / PROCS ))
    fi
    __cores_per_slot_from_CORES=
    if [ -n "${CORES}" ]; then
        __cores_per_slot_from_CORES=$(( (CORES + __flux_nnodes - 1) / __flux_nnodes ))
    fi
    if [ ! -z "${__cores_per_slot_from_CORES}" ] && [ -n "${PROCS}" ]; then
        if [ "${__cores_per_slot_from_CORES}" -ge "${PROCS}" ]; then
            __flux_ncores_per_slot=${__cores_per_slot_from_CORES}
        else
            __flux_ncores_per_slot=${PROCS}
        fi
    elif [ ! -z "${__cores_per_slot_from_CORES}" ]; then
        __flux_ncores_per_slot=${__cores_per_slot_from_CORES}
    elif [ -n "${PROCS}" ]; then
        __flux_ncores_per_slot=${PROCS}
    fi
    __flux_nslots=${__flux_nnodes}
elif [ -n "${CORES}" ]; then
    __flux_nslots=${CORES}
    __flux_ncores_per_slot=1
fi

if [ -n "${NODES}" ]; then
    nodes="#FLUX: --nodes=$(strip_quotes $NODES)"
    echo $nodes
fi

if [ ! -z "${__flux_nnodes}" ]; then
    echo "#FLUX: --nodes=$(strip_quotes ${__flux_nnodes})"
fi
if [ ! -z "${__flux_nslots}" ]; then
    echo "#FLUX: --nslots=$(strip_quotes ${__flux_nslots})"
fi
if [ ! -z "${__flux_ncores_per_slot}" ]; then
    echo "#FLUX: --cores-per-slot=$(strip_quotes ${__flux_ncores_per_slot})"
fi
if [ ! -z "${__flux_nnodes}" ] && [ -n "${GPUS}" ]; then
    echo "#FLUX: --gpus-per-slot=$(strip_quotes ${GPUS})"
fi

