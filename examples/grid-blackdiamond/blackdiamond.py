#!/usr/bin/env python

from Pegasus.DAX2 import *
import sys

# Create a DAX
diamond = DAX("diamond")

# Create some logical file names
a = Filename("f.a",link=LFN.INPUT,transfer=True)
b1 = Filename("f.b1",link=LFN.OUTPUT,transfer=True)
b2 = Filename("f.b2",link=LFN.OUTPUT,transfer=True)
c1 = Filename("f.c1",link=LFN.OUTPUT,transfer=True)
c2 = Filename("f.c2",link=LFN.OUTPUT,transfer=True)
d = Filename("f.d",link=LFN.OUTPUT,transfer=True,register=True)

# Add the filenames to the DAX (this is not strictly required by the DAX schema)
diamond.addFilename(a)
diamond.addFilename(d)

# Add a preprocess job
preprocess = Job(namespace="diamond",name="preprocess",version="2.0")
preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
preprocess.addUses(a,link=LFN.INPUT)
preprocess.addUses(b1,link=LFN.OUTPUT)
preprocess.addUses(b2,link=LFN.OUTPUT)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(namespace="diamond",name="findrange",version="2.0")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.addUses(b1,link=LFN.INPUT)
frl.addUses(c1,link=LFN.OUTPUT)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(namespace="diamond",name="findrange",version="2.0")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.addUses(b2,link=LFN.INPUT)
frr.addUses(c2,link=LFN.OUTPUT)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(namespace="diamond",name="analyze",version="2.0")
analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
analyze.addUses(c1,link=LFN.INPUT)
analyze.addUses(c2,link=LFN.INPUT)
analyze.addUses(d,link=LFN.OUTPUT)
diamond.addJob(analyze)

# Add control-flow dependencies
diamond.addDependency(parent=preprocess, child=frl)
diamond.addDependency(parent=preprocess, child=frr)
diamond.addDependency(parent=frl, child=analyze)
diamond.addDependency(parent=frr, child=analyze)

# Write the DAX to stdout
diamond.writeXML(sys.stdout)


