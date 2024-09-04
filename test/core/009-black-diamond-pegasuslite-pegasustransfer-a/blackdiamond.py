#!/usr/bin/env python3

import os
import sys

from Pegasus.api import *

if len(sys.argv) != 2:
    print("Usage: %s PEGASUS_HOME" % (sys.argv[0]))
    sys.exit(1)

# Create a abstract dag
diamond = Workflow("diamond")

rc = ReplicaCatalog()
tc = TransformationCatalog()
diamond.add_replica_catalog(rc)
diamond.add_transformation_catalog(tc)

# Add input file to the DAX-level replica catalog
a = File("f.a")
rc.add_replica("local", a.lfn, "file://" + os.getcwd() + "/f.a")

# Add executables to the DAX-level replica catalog
# In this case the binary is pegasus-keg, which is shipped with Pegasus, so we use
# the remote PEGASUS_HOME to build the path.
e_preprocess = Transformation(
    namespace="diamond",
    name="preprocess",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/bin/pegasus-keg",
)

e_findrange = Transformation(
    namespace="diamond",
    name="findrange",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/bin/pegasus-keg",
)

e_analyze = Transformation(
    namespace="diamond",
    name="analyze",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/bin/pegasus-keg",
)

tc.add_transformations(e_preprocess, e_findrange, e_analyze)

# Add a preprocess job
preprocess = Job(e_preprocess)
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.add_args("-a preprocess", "-T5", "-i", a, "-o", b1, b2)
preprocess.add_inputs(a)
preprocess.add_outputs(
    b1,
)
preprocess.add_outputs(
    b2,
)
diamond.add_jobs(preprocess)

# Add left Findrange job
frl = Job(e_findrange)
c1 = File("f.c1")
frl.add_args("-a findrange", "-T5", "-i", b1, "-o", c1)
frl.add_inputs(b1)
frl.add_outputs(
    c1,
)
diamond.add_jobs(frl)

# Add right Findrange job
frr = Job(e_findrange)
c2 = File("f.c2")
frr.add_args("-a findrange", "-T5", "-i", b2, "-o", c2)
frr.add_inputs(b2)
frr.add_outputs(
    c2,
)
diamond.add_jobs(frr)

# Add Analyze job
analyze = Job(e_analyze)
d = File("f.d")
analyze.add_args("-a analyze", "-T5", "-i", c1, c2, "-o", d)
analyze.add_inputs(c1)
analyze.add_inputs(c2)
analyze.add_outputs(d, register_replica=True)
diamond.add_jobs(analyze)

# Add control-flow dependencies
diamond.add_dependency(preprocess, children=[frl])
diamond.add_dependency(preprocess, children=[frr])
diamond.add_dependency(frl, children=[analyze])
diamond.add_dependency(frr, children=[analyze])

# Write the DAX to stdout
diamond.write(sys.stdout)
