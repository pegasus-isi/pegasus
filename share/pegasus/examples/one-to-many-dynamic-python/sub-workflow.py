#!/usr/bin/env python3


"""
SubDax generator that takes a list of input files 
and generates a dax file containing one job per input file 
that is included as a sub dax job in the root workflow.

DO NOT RUN DIRECTLY

Usage: sub-workflow.py PEGASUS_HOME inputlistfile outputdaxfile
"""

from Pegasus.DAX3 import *
import sys
import os
import re

if len(sys.argv) != 4:
	print "Usage: %s PEGASUS_HOME inputlistfile outputdaxfile" % (sys.argv[0])
	sys.exit(1)


if not os.path.isfile(sys.argv[2]):
	print "please provide the input list file"
	print "Usage: %s PEGASUS_HOME inputlistfile outputdaxfile" % (sys.argv[0])
	sys.exit(1)	
	
infile = open(sys.argv[2])
files=[]
fileregex=re.compile("(.+)\s+(.+)")

# Create a abstract dag
diamond = ADAG("sub-workflow")

	# Add executables to the DAX-level replica catalog
	# In this case the binary is keg, which is shipped with Pegasus, so we use
	# the remote PEGASUS_HOME to build the path.
	
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
			
	# Add Analyze job
	analyze = Job(namespace="diamond", name="analyze", version="4.0")
	d = File(ifile[1]+".out")
	analyze.addArguments("-a analyze","-T60","-i",a,"-o",d)
	analyze.uses(a, link=Link.INPUT)
	analyze.uses(d, link=Link.OUTPUT, register=True)
	diamond.addJob(analyze)
	


# Write the DAX to stdout

outfile=open(sys.argv[3],"w")
diamond.writeXML(outfile)
infile.close()
outfile.close()



