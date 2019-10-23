#!/usr/bin/env python3

import sys
import os
from Pegasus.DAX3 import *

if len(sys.argv) != 2:
        print "Usage: %s PEGASUS_BIN" % (sys.argv[0])
        sys.exit(1)


# Create a DAX
diamond = ADAG("diamond")
 
# Add some metadata
diamond.metadata("name", "diamond")
diamond.metadata("createdby", "Karan Vahi")
 
# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("file://" + os.getcwd() + "/f.a", "local"))
a.metadata("size", "1024")
a.metadata("raw_input", "true")
diamond.addFile(a)
 
# Add executables to the DAX-level replica catalog
keg = PFN("file://" + sys.argv[1] + "/pegasus-keg", "local")
e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64", installed=False)
e_preprocess.metadata("size", "2048")
e_preprocess.metadata("transformation", "preprocess")
e_preprocess.addPFN(keg)
diamond.addExecutable(e_preprocess)
 
e_findrange = Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64", installed=False)
e_findrange.metadata("size", "2048")
e_findrange.metadata("transformation", "findrange")
e_findrange.addPFN(keg)
diamond.addExecutable(e_findrange)
 
e_analyze = Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64", installed=False)
e_analyze.metadata("size", "2048")
e_analyze.metadata("transformation", "analyze")
e_analyze.addPFN(keg)
diamond.addExecutable(e_analyze)
 
# Add a preprocess job
preprocess = Job(e_preprocess)
preprocess.metadata("time", "60")
b1 = File("f.b1")
b2 = File("f.b2")
preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1, "-o",b2)
preprocess.uses(a, link=Link.INPUT)
preprocess.uses(b1, link=Link.OUTPUT, transfer=True, register=True)
preprocess.uses(b2, link=Link.OUTPUT, transfer=True, register=True)
diamond.addJob(preprocess)
 
# Add left Findrange job
frl = Job(e_findrange)
frl.metadata("time", "60")
c1 = File("f.c1")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.uses(b1, link=Link.INPUT)
frl.uses(c1, link=Link.OUTPUT, transfer=True, register=True)
diamond.addJob(frl)
 
# Add right Findrange job
frr = Job(e_findrange)
frr.metadata("time", "60")
c2 = File("f.c2")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.uses(b2, link=Link.INPUT)
frr.uses(c2, link=Link.OUTPUT, transfer=True, register=True)
diamond.addJob(frr)
 
# Add Analyze job
analyze = Job(e_analyze)
analyze.metadata("time", "60")
d = File("f.d")
analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
analyze.uses(c1, link=Link.INPUT)
analyze.uses(c2, link=Link.INPUT)
analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
diamond.addJob(analyze)
 
# Add dependencies
diamond.depends(parent=preprocess, child=frl)
diamond.depends(parent=preprocess, child=frr)
diamond.depends(parent=frl, child=analyze)
diamond.depends(parent=frr, child=analyze)
 
# Write the DAX to stdout
import sys
diamond.writeXML(sys.stdout)
 
# Write the DAX to a file
f = open("blackdiamond.dax","w")
diamond.writeXML(f)
f.close()
