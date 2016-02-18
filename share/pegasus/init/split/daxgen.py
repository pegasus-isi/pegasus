#!/usr/bin/env python

import os
import sys
from Pegasus.DAX3 import *

# The name of the DAX file is the first argument
if len(sys.argv) != 2:
        sys.stderr.write("Usage: %s DAXFILE\n" % (sys.argv[0]))
        sys.exit(1)
daxfile = sys.argv[1]

# Create a abstract dag
dax = ADAG("split")

webpage = File("pegasus.html")

# optional curl job to grab the input from Pegasus website
#curl = Job("curl")
#curl.addArguments("-o",webpage,"http://pegasus.isi.edu")
#curl.uses(webpage, link=Link.OUTPUT)
#dax.addJob(curl)

# the split job that splits the webpage into smaller chunks
split = Job("split")
split.addArguments("-l","100","-a","1",webpage,"part.")
split.uses(webpage, link=Link.INPUT)
dax.addJob(split)

#dax.depends(split, curl)

# we do a parmeter sweep on the first 4 chunks created
for c in "abcd":
    part = File("part.%s" % c)
    split.uses(part, link=Link.OUTPUT, transfer=False, register=False)

    count = File("count.txt.%s" % c)

    wc = Job("wc")
    wc.addArguments("-l",part)
    wc.setStdout(count)
    wc.uses(part, link=Link.INPUT)
    wc.uses(count, link=Link.OUTPUT, transfer=True, register=False)
    dax.addJob(wc)
    
    #adding dependency
    dax.depends(wc, split)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()
print "Generated dax %s" %daxfile
