#!/usr/bin/env python3
import sys
import os
from Pegasus.DAX3 import *

# API Documentation: http://pegasus.isi.edu/documentation

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s DAXFILE\n" % (sys.argv[0]))
    exit(1)

daxfile = sys.argv[1]

# Create a abstract dag
workflow = ADAG("{{name}}")

# TODO Add some jobs to the workflow
#j = Job(name="myexe")
#a = File("a")
#b = File("b")
#c = File("c")
#j.addArguments("-i",a,"-o",b,"-o",c)
#j.uses(a, link=Link.INPUT)
#j.uses(b, link=Link.OUTPUT, transfer=False, register=False)
#j.uses(c, link=Link.OUTPUT, transfer=False, register=False)
#workflow.addJob(j)

# TODO Add dependencies
#workflow.depends(parent=j, child=k)

# Write the DAX to file
f = open(daxfile, "w")
workflow.writeXML(f)
f.close()

