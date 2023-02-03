#!/usr/bin/env python3
import os
import sys
import subprocess

if len(sys.argv) != 3:
    print("Usage: %s PEGASUS_HOME DAXFILE" % (sys.argv[0]))
    sys.exit(1)

pegasus_home = sys.argv[1]
daxfile = sys.argv[2]

pegasus_config = (
    os.path.join(pegasus_home + "/bin", "pegasus-config") + " --noeoln --python"
)
lib_dir = subprocess.Popen(
    pegasus_config, stdout=subprocess.PIPE, shell=True
).communicate()[0]

# Insert this directory in our search path
os.sys.path.insert(0, str(lib_dir))

from Pegasus.DAX3 import *


# Create a abstract dag
diamond = ADAG("diamond")


# Add a preprocess job
preprocess = Job(namespace="diamond", name="preprocess", version="4.0")
a = File("f.a")
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.addArguments("-a preprocess", "-T60", "-i", a, "-o", b1, b2)
preprocess.uses(a, link=Link.INPUT)
preprocess.uses(b1, link=Link.OUTPUT)
preprocess.uses(b2, link=Link.OUTPUT)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(namespace="diamond", name="findrange", version="4.0")
c1 = File("f.c1")
frl.addArguments("-a findrange", "-T60", "-i", b1, "-o", c1)
frl.uses(b1, link=Link.INPUT)
frl.uses(c1, link=Link.OUTPUT)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(namespace="diamond", name="findrange", version="4.0")
c2 = File("f.c2")
frr.addArguments("-a findrange", "-T60", "-i", b2, "-o", c2)
frr.uses(b2, link=Link.INPUT)
frr.uses(c2, link=Link.OUTPUT)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(namespace="diamond", name="analyze", version="4.0")
d = File("f.d")
analyze.addArguments("-a analyze", "-T60", "-i", c1, c2, "-o", d)
analyze.uses(c1, link=Link.INPUT)
analyze.uses(c2, link=Link.INPUT)
analyze.uses(d, link=Link.OUTPUT, register=True)
diamond.addJob(analyze)

# Add control-flow dependencies
diamond.addDependency(Dependency(parent=preprocess, child=frl))
diamond.addDependency(Dependency(parent=preprocess, child=frr))
diamond.addDependency(Dependency(parent=frl, child=analyze))
diamond.addDependency(Dependency(parent=frr, child=analyze))

# Write the DAX to daxfile
f = open(daxfile, "w")
print("Writing DAX to %s" % (os.path.abspath(daxfile)))
diamond.writeXML(f)
f.close()
