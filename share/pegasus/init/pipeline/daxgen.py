#!/usr/bin/env python3

import os
import pwd
import sys
import time

from Pegasus.DAX3 import *

# The name of the DAX file is the first argument
if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s DAXFILE\n" % (sys.argv[0]))
    sys.exit(1)
daxfile = sys.argv[1]

USER = pwd.getpwuid(os.getuid())[0]

dax = ADAG("pipeline")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

webpage = File("pegasus.html")

curl = Job("curl")
curl.addArguments("-o", webpage, "http://pegasus.isi.edu")
curl.uses(webpage, link=Link.OUTPUT)
dax.addJob(curl)

count = File("count.txt")

wc = Job("wc")
wc.addArguments("-l", webpage)
wc.setStdout(count)
wc.uses(webpage, link=Link.INPUT)
wc.uses(count, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(wc)

dax.depends(wc, curl)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()
