#!/usr/bin/env python


"""
Pegasus Dax Generator example to show spliting of 
large files without knowing the number of output files.
The technique used here is to introduce a fake dependent 
node that determines the number of out files created and generates a sub dax
at runtime. This dax is then evaluated and executed on the remote host 
just in time before execution

Usage: split-hierarchical daxfile
"""

from Pegasus.DAX3 import *
import sys
import os
import re

if len(sys.argv) != 4:
	print "Usage: %s PEGASUS_HOME inputlistfile outputfile" % (sys.argv[0])
	sys.exit(1)


if not os.path.isfile(sys.argv[2]):
	print "please provide the input list file"
	print "Usage: %s PEGASUS_HOME inputlistfile outputfile" % (sys.argv[0])
	sys.exit(1)	
	
infile = open(sys.argv[2])
files=[]
fileregex=re.compile("(.+)\s+(.+)")

# Create a abstract dag
diamond = ADAG("sub-workflow")

	# Add executables to the DAX-level replica catalog
	# In this case the binary is keg, which is shipped with Pegasus, so we use
	# the remote PEGASUS_HOME to build the path.
e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64", installed=True)
e_preprocess.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "TestCluster"))
diamond.addExecutable(e_preprocess)
	
e_findrange = Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64", installed=True)
e_findrange.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "TestCluster"))
diamond.addExecutable(e_findrange)
	
e_analyze = Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64", installed=True)
e_analyze.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "TestCluster"))
diamond.addExecutable(e_analyze)

for l in infile:
	line = l.strip()
	if not line:
		continue
	ifile=fileregex.split(line)
	# Add input file to the DAX-level replica catalog
	a = File(ifile[1])
	a.addPFN(PFN(ifile[2],"TestCluster"))
	diamond.addFile(a)
		

	# Add a preprocess job
	preprocess = Job(namespace="diamond", name="preprocess", version="4.0")
	b1 = File(ifile[1]+".b1")
	b2 = File(ifile[1]+".b2")
	preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
	preprocess.uses(a, link=Link.INPUT)
	preprocess.uses(b1, link=Link.OUTPUT)
	preprocess.uses(b2, link=Link.OUTPUT)
	diamond.addJob(preprocess)
	
	# Add left Findrange job
	frl = Job(namespace="diamond", name="findrange", version="4.0")
	c1 = File(ifile[1]+".c1")
	frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
	frl.uses(b1, link=Link.INPUT)
	frl.uses(c1, link=Link.OUTPUT)
	diamond.addJob(frl)
	
	# Add right Findrange job
	frr = Job(namespace="diamond", name="findrange", version="4.0")
	c2 = File(ifile[1]+".c2")
	frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
	frr.uses(b2, link=Link.INPUT)
	frr.uses(c2, link=Link.OUTPUT)
	diamond.addJob(frr)
	
	# Add Analyze job
	analyze = Job(namespace="diamond", name="analyze", version="4.0")
	d = File(ifile[1]+".d")
	analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
	analyze.uses(c1, link=Link.INPUT)
	analyze.uses(c2, link=Link.INPUT)
	analyze.uses(d, link=Link.OUTPUT, register=True)
	diamond.addJob(analyze)
	
	# Add control-flow dependencies
	diamond.addDependency(Dependency(parent=preprocess, child=frl))
	diamond.addDependency(Dependency(parent=preprocess, child=frr))
	diamond.addDependency(Dependency(parent=frl, child=analyze))
	diamond.addDependency(Dependency(parent=frr, child=analyze))

# Write the DAX to stdout

outfile=open(sys.argv[3],"w")
diamond.writeXML(outfile)
infile.close()
outfile.close()



