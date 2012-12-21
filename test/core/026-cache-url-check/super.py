#!/usr/bin/env python

from Pegasus.DAX3 import *
import sys
import os

# Create a abstract dag
diamond = ADAG("super-diamond")

# Add input file to the DAX-level replica catalog
a = File("one.dax")
a.addPFN(PFN("file://" + os.getcwd() + "/one.dax", "local"))
diamond.addFile(a)

	
b = File("two.dax")
b.addPFN(PFN("file://" + os.getcwd() + "/two.dax", "local"))
diamond.addFile(b)

# Add DAX job 1
one = DAX("one.dax")
one.addArguments('-vvvv')
diamond.addJob(one)

# Add DAX job 2
two = DAX("two.dax")
two.addArguments('-vvvv')
diamond.addJob(two)

# Add control-flow dependencies
diamond.addDependency(Dependency(parent=one, child=two))

# Write the DAX to stdout
diamond.writeXML(sys.stdout)



