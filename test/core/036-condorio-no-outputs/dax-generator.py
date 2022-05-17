#!/usr/bin/env python3

from Pegasus.DAX3 import *
import sys
import os
import math

base_dir = os.getcwd()

dax = ADAG("test")

# Add executables to the DAX-level replica catalog
exe = Executable(name = "test.sh", arch = "x86_64", installed = False)
exe.addPFN(PFN("file://" + base_dir + "/test.sh", "local"))
dax.addExecutable(exe)

j = Job(name = "test.sh")
dax.addJob(j)

# Write the DAX
f = open("dax.xml", "w")
dax.writeXML(f)
f.close()


