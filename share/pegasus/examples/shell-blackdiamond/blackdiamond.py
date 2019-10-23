#!/usr/bin/env python3

from Pegasus.DAX3 import *
import os
import sys

# Create a DAX
diamond = ADAG("diamond")

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN("file://" + os.getcwd() + "/f.a", "local"))
diamond.addFile(a)

# Add executables to the DAX-level replica catalog
# In this case the binary is keg, which is shipped with Pegasus
e_preprocess = Executable(namespace="diamond", name="preprocess", version="2.0", os="linux", arch="x86_64")
e_preprocess.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "local"))
diamond.addExecutable(e_preprocess)

e_findrange = Executable(namespace="diamond", name="findrange", version="2.0", os="linux", arch="x86_64")
e_findrange.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "local"))
diamond.addExecutable(e_findrange)

e_analyze = Executable(namespace="diamond", name="analyze", version="2.0", os="linux", arch="x86_64")
e_analyze.addPFN(PFN("file://" + sys.argv[1] + "/bin/pegasus-keg", "local"))
diamond.addExecutable(e_analyze)

# Add transformations to the DAX-level transformation catalog
#t_preprocess = Transformation(e_preprocess)
#diamond.addTransformation(t_preprocess)

#t_findrange = Transformation(e_findrange)
#diamond.addTransformation(t_findrange)

#t_analyze = Transformation(e_analyze)
#diamond.addTransformation(t_analyze)

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
preprocess.uses(a,link=Link.INPUT)
preprocess.uses(b1,link=Link.OUTPUT, register=False)
preprocess.uses(b2,link=Link.OUTPUT, register=False)
diamond.addJob(preprocess)

# Add left Findrange job
frl = Job(namespace="diamond",name="findrange",version="2.0")
frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
frl.uses(b1,link=Link.INPUT)
frl.uses(c1,link=Link.OUTPUT, register=False)
diamond.addJob(frl)

# Add right Findrange job
frr = Job(namespace="diamond",name="findrange",version="2.0")
frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
frr.uses(b2,link=Link.INPUT)
frr.uses(c2,link=Link.OUTPUT, register=False)
diamond.addJob(frr)

# Add Analyze job
analyze = Job(namespace="diamond",name="analyze",version="2.0")
analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
analyze.uses(c1,link=Link.INPUT)
analyze.uses(c2,link=Link.INPUT)
analyze.uses(d,link=Link.OUTPUT, register=False)
diamond.addJob(analyze)

# Add control-flow dependencies
diamond.addDependency(Dependency(parent=preprocess, child=frl))
diamond.addDependency(Dependency(parent=preprocess, child=frr))
diamond.addDependency(Dependency(parent=frl, child=analyze))
diamond.addDependency(Dependency(parent=frr, child=analyze))

# Write the DAX to stdout
diamond.writeXML(sys.stdout)


