#!/usr/bin/env python3

import sys
from Pegasus.DAX3 import *

dax = ADAG("merge")

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

dax.writeXML(sys.stdout)

