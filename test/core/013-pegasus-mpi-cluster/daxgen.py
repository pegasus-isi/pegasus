#!/usr/bin/env python3

import sys

from Pegasus.api import *

if len(sys.argv) != 2:
    print("Usage: python daxgen.py DAXFILE")
    sys.exit(1)

daxfile = sys.argv[1]


def create_job(cluster):
    j = Job("sleep")
    j.add_args("1")
    j.add_pegasus_profile(label="dag_%d" % cluster)
    j.add_pegasus_profile(pmc_request_memory="1")
    j.add_pegasus_profile(pmc_request_cpus="2")
    j.add_pegasus_profile(pmc_priority="1")
    j.add_pegasus_profile(pmc_task_arguments="--tries 3")
    return j


dax = Workflow("mpi-cluster")

rc = ReplicaCatalog()
tc = TransformationCatalog()
dax.add_replica_catalog(rc)
dax.add_transformation_catalog(tc)

sleep = Transformation(
    name="sleep",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    os_release="rhel",
    os_version="7",
    is_stageable=False,
    site="local",
    pfn="file:///bin/sleep",
)

tc.add_transformations(sleep)

# Create 48 jobs in 4 clusters of 12
for i in range(4):
    parent = create_job(i)
    dax.add_jobs(parent)

    child = create_job(i)
    dax.add_jobs(child)

    for j in range(10):
        j = create_job(i)
        dax.add_jobs(j)

        dax.add_dependency(parent, children=[j])
        dax.add_dependency(j, children=[child])

f = open(daxfile, "w")
dax.write(f)
f.close()
