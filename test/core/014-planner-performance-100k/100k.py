#!/usr/bin/env python3

import os
import shutil
import sys

from Pegasus.api import *

# Create a abstract dag
dax = Workflow("100k")

rc = ReplicaCatalog()
tc = TransformationCatalog()
dax.add_replica_catalog(rc)
dax.add_transformation_catalog(tc)

# Add input file to the DAX-level replica catalog
a = File("f.a")
rc.add_replica("local", a.lfn, "file://" + os.getcwd() + "/f.a")

# Add executables to the DAX-level replica catalog
exe = Transformation(
    name="mymodel",
    is_stageable=True,
    arch=Arch.X86_64,
    site="local",
    pfn="file://" + shutil.which("pegasus-keg"),
)
tc.add_transformations(exe)

for i in range(100000):
    job = Job(exe)
    job.add_args("-T", "5", "-i", a)
    job.add_inputs(a)
    dax.add_jobs(job)

# Write the DAX to stdout
dax.write(sys.stdout)
