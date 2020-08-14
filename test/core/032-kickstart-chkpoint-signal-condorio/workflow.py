#!/usr/bin/env python3

"""
Test to ensure that pegasus-kickstart sends SIGTERM and then SIGKILL to a job
executable when checkpoint.time and maxwalltime pegasus profiles are given.
Additionally, tests that checkpoint files are handled correctly for condorio.
"""

import logging

from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# Properties
props = Properties()
props["pegasus.data.configuration"] = "condorio"
props["dagman.retry"] = "2"
props.write()

# Replicas
rc = ReplicaCatalog()\
        .add_replica("local", "saved_state.txt", Path(".").resolve() / "saved_state.txt")\
        .write()


# Transformation
exe = Transformation(
        "checkpoint_program.py",
        site="local",
        pfn=Path(".").resolve() / "checkpoint_program.py",
        is_stageable=True
    )

tc = TransformationCatalog().add_transformations(exe).write()

# Workflow
job = Job(exe)\
        .add_args(180)\
        .add_checkpoint(File("saved_state.txt"), stage_out=True)\
        .set_stdout("output.txt")\
        .add_profiles(Namespace.PEGASUS, key="checkpoint.time", value=1)\
        .add_profiles(Namespace.PEGASUS, key="maxwalltime", value=2)

'''
KILL signal is sent at (checkpoint.time + (maxwalltime-checkpoint.time)/2) minutes. (hence -K 30)
.add_profiles(Namespace.PEGASUS, key="checkpoint.time", value=1)\
.add_profiles(Namespace.PEGASUS, key="maxwalltime", value=2)
# Kickstart args (note checkpoint.time converted to seconds)
pegasus-kickstart \ 
    -n checkpoint_program.py \
    -N ID0000001 \
    -o output.txt \
    -R condorpool  \
    -s output.txt=output.txt \
    -s saved_state.txt=saved_state.txt \
    -L checkpoint-wf \
    -T 2020-07-07T05:35:37+00:00 \
    -k 60 \
    -K 30 \
    ./checkpoint_program_py 180
'''
try:
    Workflow("checkpoint-wf").add_jobs(job).plan(submit=True)
except PegasusClientError as e:
    print(e.output)