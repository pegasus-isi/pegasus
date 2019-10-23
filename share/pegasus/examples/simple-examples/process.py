#!/usr/bin/env python3

import sys
from Pegasus.DAX3 import *

dax =ADAG("process")

listing = File("listing.txt")

job = Job("ls")
job.addArguments("-l","/")
job.setStdout(listing)
job.uses(listing, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(job)

dax.writeXML(sys.stdout)

