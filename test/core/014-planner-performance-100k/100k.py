#!/usr/bin/env python

from Pegasus.DAX3 import *
import sys
import os

# Create a abstract dag
dax = ADAG("100k")

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("file://" + os.getcwd() + "/f.a", "local"))
dax.addFile(a)
	
# Add executables to the DAX-level replica catalog
exe = Executable(name="mymodel", installed=False)
exe.addPFN(PFN("file:///usr/bin/pegasus-keg", "local"))
dax.addExecutable(exe)

for i in range(100000):
    job = Job(name="mymodel")
    job.addArguments("-T", "5", "-i", a)
    job.uses(a, link=Link.INPUT)
    dax.addJob(job)

# Write the DAX to stdout
dax.writeXML(sys.stdout)



