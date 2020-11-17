#!/usr/bin/env python3

import os
import sys
from Pegasus.DAX3 import *

if len(sys.argv) != 2:
    print "Usage: python daxgen.py DAXFILE"
    exit(1)

daxfile = sys.argv[1]

dax = ADAG("pegasus_mpi_keg_test")

sample_file1 = File("file1")
sample_file2 = File("file2")

kegjob = Job("pegasus-mpi-keg")

kegjob.addArguments("-o file1=100K file2=1M")

kegjob.uses(sample_file1, link=Link.OUTPUT, transfer=True)
kegjob.uses(sample_file2, link=Link.OUTPUT, transfer=True)

kegjob.profile("globus", "jobtype", "mpi")
kegjob.profile("globus", "maxwalltime", "1")
kegjob.profile("globus", "count", "8")

dax.addJob(kegjob)

# Write the DAX file
dax.writeXMLFile(daxfile)
