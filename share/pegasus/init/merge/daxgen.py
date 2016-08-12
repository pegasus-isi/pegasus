#!/usr/bin/env python

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

dax = ADAG("merge")

# Add some workflow-level metadata
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

dirs = ["/bin","/usr/bin","/usr/local/bin"]
jobs = []
files = []

for i,d in enumerate(dirs):
    ls = Job("ls")
    jobs.append(ls)
    ls.addArguments("-l",d)
    f = File("bin_%d.txt" % i)
    files.append(f)
    ls.setStdout(f)
    ls.uses(f, link=Link.OUTPUT)
    dax.addJob(ls)

cat = Job("cat")
cat.addArguments(*files)
for f in files:
    cat.uses(f, link=Link.INPUT)
output = File("binaries.txt")
cat.setStdout(output)
cat.uses(output, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(cat)

for j in jobs:
    dax.depends(cat, j)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()


