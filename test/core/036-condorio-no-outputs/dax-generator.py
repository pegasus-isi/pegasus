#!/usr/bin/env python3

import os

from Pegasus.api import *

base_dir = os.getcwd()

dax = Workflow("test")

rc = ReplicaCatalog()
tc = TransformationCatalog()
dax.add_replica_catalog(rc)
dax.add_transformation_catalog(tc)

# Add executables to the DAX-level replica catalog
exe = Transformation(
    "test.sh",
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + base_dir + "/test.sh",
)
tc.add_transformations(exe)

j = Job(exe)
dax.add_jobs(j)

# Write the DAX
f = open("dax.xml", "w")
dax.write(f)
f.close()
