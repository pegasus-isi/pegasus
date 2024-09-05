#!/usr/bin/env python3

import os
import sys

from Pegasus.api import *

if len(sys.argv) != 2:
    print("Usage: %s PEGASUS_BIN" % (sys.argv[0]))
    sys.exit(1)


# Create a DAX
diamond = Workflow("diamond")

rc = ReplicaCatalog()
tc = TransformationCatalog()
diamond.add_replica_catalog(rc)
diamond.add_transformation_catalog(tc)


# Add some metadata
diamond.add_metadata(name="diamond")
diamond.add_metadata(createdby="Karan Vahi")

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.add_metadata(size="1024")
a.add_metadata(raw_input="true")
rc.add_replica("local", a.lfn, "scp://bamboo@bamboo.isi.edu" + os.getcwd() + "/f.a")

# Add executables to the DAX-level replica catalog
e_preprocess = Transformation(
    namespace="diamond",
    name="preprocess",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/pegasus-keg",
)
e_preprocess.add_metadata(size="2048")
e_preprocess.add_metadata(transformation="preprocess")

e_findrange = Transformation(
    namespace="diamond",
    name="findrange",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/pegasus-keg",
)
e_findrange.add_metadata(size="2048")
e_findrange.add_metadata(transformation="findrange")

e_analyze = Transformation(
    namespace="diamond",
    name="analyze",
    version="4.0",
    os_type=OS.LINUX,
    arch=Arch.X86_64,
    is_stageable=True,
    site="local",
    pfn="file://" + sys.argv[1] + "/pegasus-keg",
)
e_analyze.add_metadata(size="2048")
e_analyze.add_metadata(transformation="analyze")

tc.add_transformations(e_preprocess, e_findrange, e_analyze)

# Add a preprocess job
preprocess = Job(e_preprocess)
preprocess.add_metadata(time="60")
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.add_args("-a preprocess", "-T10", "-i", a, "-o", b1, "-o", b2)
preprocess.add_inputs(a)
preprocess.add_outputs(b1, stage_out=True, register_replica=True)
preprocess.add_outputs(b2, stage_out=True, register_replica=True)
diamond.add_jobs(preprocess)

# Add left Findrange job
frl = Job(e_findrange)
frl.add_metadata(time="60")
c1 = File("f.c1")
frl.add_args("-a findrange", "-T10", "-i", b1, "-o", c1)
frl.add_inputs(b1)
frl.add_outputs(c1, stage_out=True, register_replica=True)
diamond.add_jobs(frl)

# Add right Findrange job
frr = Job(e_findrange)
frr.add_metadata(time="60")
c2 = File("f.c2")
frr.add_args("-a findrange", "-T10", "-i", b2, "-o", c2)
frr.add_inputs(b2)
frr.add_outputs(c2, stage_out=True, register_replica=True)
diamond.add_jobs(frr)

# Add Analyze job
analyze = Job(e_analyze)
analyze.add_metadata(time="60")
d = File("f.d")
d.add_metadata(final_output="true")
analyze.add_args("-a analyze", "-T10", "-i", c1, c2, "-o", d)
analyze.add_inputs(c1)
analyze.add_inputs(c2)
analyze.add_outputs(d, stage_out=True, register_replica=True)
diamond.add_jobs(analyze)

# Add control-flow dependencies
diamond.add_dependency(preprocess, children=[frl])
diamond.add_dependency(preprocess, children=[frr])
diamond.add_dependency(frl, children=[analyze])
diamond.add_dependency(frr, children=[analyze])

# Write the DAX to stdout
diamond.write(sys.stdout)
