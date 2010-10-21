#!/usr/bin/env python

from Pegasus.DAX3 import *
import sys

# Create a DAX
diamond = ADAG("diamond")

# Create some logical file names
a = File("f.a")
b1 = File("f.b1")
b2 = File("f.b2")
c1 = File("f.c1")
c2 = File("f.c2")
d = File("f.d")

# Add a preprocess job
preprocess = Job(namespace="diamond",name="preprocess",version="2.0")
preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
preprocess.addUses(a,link=Link.INPUT,transfer=True)
preprocess.addUses(b1,link=Link.OUTPUT,transfer=True)
preprocess.addUses(b2,link=Link.OUTPUT,transfer=True)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(namespace="diamond",name="findrange",version="2.0")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.addUses(b1,link=Link.INPUT,transfer=True)
frl.addUses(c1,link=Link.OUTPUT,transfer=True)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(namespace="diamond",name="findrange",version="2.0")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.addUses(b2,link=Link.INPUT,transfer=True)
frr.addUses(c2,link=Link.OUTPUT,transfer=True)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(namespace="diamond",name="analyze",version="2.0")
analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
analyze.addUses(c1,link=Link.INPUT,transfer=True)
analyze.addUses(c2,link=Link.INPUT,transfer=True)
analyze.addUses(d,link=Link.OUTPUT,transfer=True,register=True)
diamond.addJob(analyze)

# Add control-flow dependencies
diamond.addDependency(parent=preprocess, child=frl)
diamond.addDependency(parent=preprocess, child=frr)
diamond.addDependency(parent=frl, child=analyze)
diamond.addDependency(parent=frr, child=analyze)

# Write the DAX to stdout
diamond.writeXML(sys.stdout)


