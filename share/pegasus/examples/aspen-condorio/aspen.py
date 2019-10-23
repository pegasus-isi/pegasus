#!/usr/bin/env python3

from Pegasus.DAX3 import *
import sys
import os

if len(sys.argv) != 2:
	print "Usage: %s PEGASUS_BIN" % (sys.argv[0])
	sys.exit(1)

# Create a abstract dag
diamond = ADAG("aspen")


# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("file://" + os.getcwd() + "/f.a", "local"))
diamond.addFile(a)
	
# Add executables to the DAX-level replica catalog
# In this case the binary is keg, which is shipped with Pegasus, so we use
# the remote PEGASUS_HOME to build the path.
e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64", installed=False)
e_preprocess.addPFN(PFN("file://" + sys.argv[1] + "/pegasus-keg", "local"))
diamond.addExecutable(e_preprocess)

# Add a preprocess job
preprocess = Job(namespace="diamond", name="preprocess", version="4.0")
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
preprocess.uses(a, link=Link.INPUT)
preprocess.uses(b1, link=Link.OUTPUT, register=True)
preprocess.uses(b2, link=Link.OUTPUT, register=True)

# add metadata to call out aspen with
preprocess.metadata( "time", "60" )
preprocess.metadata( "appmodel", "md/md.aspen" )
preprocess.metadata( "machmodel", "machine/keeneland.aspen" )
preprocess.metadata( "nAtom", "10000" )
preprocess.metadata( "socket", "nvidia_m2090" )

diamond.addJob(preprocess)

# Add control-flow dependencies

# Write the DAX to stdout
diamond.writeXML(sys.stdout)



