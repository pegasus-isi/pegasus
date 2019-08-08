#!/usr/bin/env python

from Pegasus.DAX3 import *
import sys
import os

# Create a abstract dag
dax = ADAG("022-data-reuse-regexrc")

# Add executables to the DAX-level replica catalog
e_hello = Executable(namespace="hello_world", name="hello", version="1.0", \
                     os="linux", osrelease="rhel", osversion="7", arch="x86_64",  installed=False)
e_hello.addPFN(PFN("file://" + os.getcwd() + "/hello.sh", "local"))
dax.addExecutable(e_hello)
	
e_world = Executable(namespace="hello_world", name="world", version="1.0", \
                     os="linux",  osrelease="rhel", osversion="7", arch="x86_64", installed=False)
e_world.addPFN(PFN("file://" + os.getcwd() + "/world.sh", "local"))
dax.addExecutable(e_world)

for i in range(3):	
	# Add the hello job
	hello = Job(namespace="hello_world", name="hello", version="1.0")
	a=File ("f.a")
	aa=File ("f.x")
	a2=File ("fax")
	a3=File ('dir/file.x')
	a4=File ('dir/file.y')
	b = File("f.b" + str(i))
	hello.addArguments (b)
	hello.uses(a, link=Link.INPUT)
	hello.uses(aa, link=Link.INPUT)
	hello.uses(a2, link=Link.INPUT)
	hello.uses(a3, link=Link.INPUT)
	hello.uses(a4, link=Link.INPUT)

	hello.uses(b, link=Link.OUTPUT, transfer=False)
	dax.addJob(hello)

	for j in range(3):
		# Add the world job (depends on the hello job)
		world = Job(namespace="hello_world", name="world", version="1.0")
		c = File("f.c" + str(i) + str(j))
		world.uses(b, link=Link.INPUT)
		world.uses(c, link=Link.OUTPUT)
		world.addArguments (c)
		dax.addJob(world)

		# Add control-flow dependencies
		dax.addDependency(Dependency(parent=hello, child=world))

# Write the DAX to stdout
dax.writeXML(sys.stdout)



