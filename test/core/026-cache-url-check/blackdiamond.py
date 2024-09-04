#!/usr/bin/env python3

import os
import sys

from Pegasus.api import *

if len(sys.argv) != 3:
    print("Usage: %s PEGASUS_HOME char" % (sys.argv[0]))
    sys.exit(1)

char = ord(sys.argv[2][0])

# Create a abstract dag
diamond = Workflow("diamond-" + chr(char))

rc = ReplicaCatalog()
tc = TransformationCatalog()
diamond.add_replica_catalog(rc)
diamond.add_transformation_catalog(tc)


# Add input file to the DAX-level replica catalog
a = File("f." + chr(char))
if chr(char) == "a":
    rc.add_replica("local", a.lfn, "file://" + os.getcwd() + "/f.a")

char = char + 1

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
b1 = File("f." + chr(char) + "1")
b2 = File("f." + chr(char) + "2")
preprocess.add_args("-a preprocess", "-T10", "-i", a, "-o", b1, b2)
preprocess.add_inputs(a)
preprocess.add_outputs(
    b1,
)
preprocess.add_outputs(
    b2,
)
diamond.add_jobs(preprocess)

char = char + 1

# Add left Findrange job
frl = Job(e_findrange)
c1 = File("f." + chr(char) + "1")
frl.add_args("-a findrange", "-T5", "-i", b1, "-o", c1)
frl.add_inputs(b1)
frl.add_outputs(
    c1,
)
diamond.add_jobs(frl)

# Add right Findrange job
frr = Job(e_findrange)
c2 = File("f." + chr(char) + "2")
frr.add_args("-a findrange", "-T5", "-i", b2, "-o", c2)
frr.add_inputs(b2)
frr.add_outputs(
    c2,
)
diamond.add_jobs(frr)

char = char + 1

# Add Analyze job
analyze = Job(e_analyze)
d = File("f." + chr(char))
analyze.add_args("-a analyze", "-T10", "-i", c1, c2, "-o", d)
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
