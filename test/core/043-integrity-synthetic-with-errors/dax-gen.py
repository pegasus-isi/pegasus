#!/usr/bin/env python3

import os

from Pegasus.api import *

# globals
wf = Workflow("synthetic", infer_dependencies=True)

rc = ReplicaCatalog()
tc = TransformationCatalog()
wf.add_replica_catalog(rc)
wf.add_transformation_catalog(tc)

# keg is an input
keg = File("keg")
keg.add_metadata({"checksum.type": "sha256"})
keg.add_metadata(
    {
        "checksum.value": "d21f06ba2dfb7fe80ff13d5e0d3d4ef3eaa41b1a4941b8947859bce3164eb223"
    }
)
rc.add_replica("local", keg.lfn, "file://" + os.getcwd() + "/keg")


def add_file_checksum(f):
    # TODO
    f.metadata("checksum.type", "sha256")
    f.metadata(
        "checksum.value",
        "630408689fceb0d34f68ab5b47a8be5ca554d4e257055c4829479f5de886970a",
    )


def add_level(level_id, num_jobs, input_files, final_outputs):

    inputs_per_job = int(len(input_files) / num_jobs)
    output_files = []

    print("Level " + str(level_id) + ": " + str(inputs_per_job) + " inputs per job")

    exe = Transformation(
        name="level" + str(level_id),
        is_stageable=True,
        arch=Arch.X86_64,
        site="local",
        pfn="file://" + os.getcwd() + "/job.sh",
    )
    tc.add_transformations(exe)

    for i in range(num_jobs):
        job = Job(exe)

        # executable
        job.add_inputs(keg)

        # inputs
        for j in range(inputs_per_job):
            job.add_inputs(input_files.pop(0))

        f = File("%d-%d-1.data" % (level_id, i))
        job.add_outputs(f, register_replica=final_outputs)
        job.add_args(f)
        output_files.append(f)

        f = File("%d-%d-2.data" % (level_id, i))
        job.add_outputs(f, register_replica=final_outputs)
        job.add_args(f)
        output_files.append(f)

        wf.add_jobs(job)

    return output_files


l1_outputs = add_level(1, 50, [], False)
l2_outputs = add_level(2, 10, l1_outputs, False)
l2_outputs = add_level(3, 20, l2_outputs, True)

f = open("dax.yml", "w")
wf.write(f)
f.close()
