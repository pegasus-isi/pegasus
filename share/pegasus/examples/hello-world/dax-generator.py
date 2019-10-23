#!/usr/bin/env python3

from Pegasus.DAX3 import *
import sys
import os

# Create a abstract dag
dax = ADAG("hello_world")

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("file://" + os.getcwd() + "/f.a", "local"))
dax.addFile(a)
	
# Add executables to the DAX-level transformation catalog
e_hello = Executable(namespace="hello_world", name="hello", version="1.0", \
                     os="linux", arch="x86_64", installed=False)
e_hello.addPFN(PFN("file://" + os.getcwd() + "/hello.sh", "local"))
dax.addExecutable(e_hello)
	
e_world = Executable(namespace="hello_world", name="world", version="1.0", \
                     os="linux", arch="x86_64", installed=False)
e_world.addPFN(PFN("file://" + os.getcwd() + "/world.sh", "local"))
dax.addExecutable(e_world)
	
# Add the hello job
hello = Job(namespace="hello_world", name="hello", version="1.0")
b = File("f.b")
hello.uses(a, link=Link.INPUT)
hello.uses(b, link=Link.OUTPUT)
dax.addJob(hello)

# Add the world job (depends on the hello job)
world = Job(namespace="hello_world", name="world", version="1.0")
c = File("f.c")
world.uses(b, link=Link.INPUT)
world.uses(c, link=Link.OUTPUT)
dax.addJob(world)

# Add control-flow dependencies
dax.addDependency(Dependency(parent=hello, child=world))

# Write the DAX to stdout
dax.writeXML(sys.stdout)



