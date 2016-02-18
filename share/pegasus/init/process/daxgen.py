#!/usr/bin/env python

import os
import sys
from Pegasus.DAX3 import *

# The name of the DAX file is the first argument
if len(sys.argv) != 2:
        sys.stderr.write("Usage: %s DAXFILE\n" % (sys.argv[0]))
        sys.exit(1)
daxfile = sys.argv[1]


dax =ADAG("process")

listing = File("listing.txt")

job = Job("ls")
job.addArguments("-l","/")
job.setStdout(listing)
job.uses(listing, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(job)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()

