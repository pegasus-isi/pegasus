#!/usr/bin/env python3

import os
import sys

from Pegasus.api import *

if len(sys.argv) != 2:
    print("Usage: python daxgen.py DAXFILE")
    sys.exit(1)

daxfile = sys.argv[1]

dax = Workflow("pmc-only")

rc = ReplicaCatalog()
tc = TransformationCatalog()
dax.add_replica_catalog(rc)
dax.add_transformation_catalog(tc)

# Create a mapping for the test script
test = Transformation(
    name="test",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=False,
    site="local",
    pfn="file://%s/test.sh" % os.getcwd(),
)
tc.add_transformations(test)

# Create input dir if it doesn't exist
if not os.path.exists("inputs"):
    os.mkdir("inputs")

# Create a bunch of parallel chains
for i in range(20):
    input = File("input_%d.txt" % i)
    inter = File("inter_%d.txt" % i)
    output = File("output_%d.txt" % i)

    # Create input file
    f = open("inputs/%s" % input.lfn, "w")
    f.write("Hello, World! #%d\n" % i)
    f.close()

    # Register mapping for input file
    rc.add_replica("local", input.lfn, f"file://{os.getcwd()}/inputs/{input.lfn}")

    parent = Job(test)
    parent.add_args(input, inter)
    parent.add_inputs(input)
    parent.add_outputs(inter, register_replica=False, stage_out=False)
    dax.add_jobs(parent)

    child = Job(test)
    child.add_args(inter, output)
    child.add_inputs(inter)
    child.add_outputs(output, register_replica=False, stage_out=True)
    dax.add_jobs(child)

    dax.add_dependency(parent, children=[child])

f = open(daxfile, "w")
dax.write(f)
f.close()
