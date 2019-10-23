#!/usr/bin/env python3

import sys
from Pegasus.DAX3 import *

dax =ADAG("pipeline")

webpage = File("pegasus.html")

curl = Job("curl")
curl.addArguments("-o",webpage,"http://pegasus.isi.edu")
curl.uses(webpage, link=Link.OUTPUT)
dax.addJob(curl)

count = File("count.txt")

wc = Job("wc")
wc.addArguments("-l",webpage)
wc.setStdout(count)
wc.uses(webpage, link=Link.INPUT)
wc.uses(count, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(wc)

dax.depends(wc, curl)

dax.writeXML(sys.stdout)

