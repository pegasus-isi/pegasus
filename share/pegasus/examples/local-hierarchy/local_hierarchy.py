#!/usr/bin/env python3

import os
import sys

from Pegasus.DAX3 import *

# Create a abstract dag
adag = ADAG('local-hierarchy')

# Add a job
pre_dax = Job(namespace="level1", name="sleep")
pre_dax.addArguments('5')
adag.addJob(pre_dax)

daxfile = File('inner.dax')

dax1 = DAX (daxfile)
#DAX jobs are called with same arguments passed, while planning the root level dax
adag.addJob(dax1)

dax2 = DAX (daxfile)
#pegasus-plan arguments for the DAX jobs can be overwritten
dax1.addArguments ('--output-site local')
adag.addJob(dax2)

# Add a job
post_dax = Job(namespace="level2", name="sleep")
post_dax.addArguments('5')
adag.addJob(post_dax)

# Add control-flow dependencies
adag.addDependency(Dependency(parent=pre_dax, child=dax1))
adag.addDependency(Dependency(parent=dax1, child=dax2))
adag.addDependency(Dependency(parent=dax2, child=post_dax))

# Write the DAX to stdout
adag.writeXML(sys.stdout)

