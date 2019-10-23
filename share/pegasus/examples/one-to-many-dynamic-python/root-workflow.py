#!/usr/bin/env python3

"""
Pegasus Dax Generator example to show spliting of 
large files without knowing the number of output files.
The technique used here is to introduce a fake dependent 
node that determines the number of out files created and generates a sub dax
at runtime. This dax is then evaluated and executed on the remote host 
just in time before execution

Usage: root-workflow.py  PEGASUS_HOME  > daxfile
"""


from Pegasus.DAX3 import *
import sys
import os

if len(sys.argv) != 2:
    print "Usage: %s PEGASUS_HOME" % (sys.argv[0])
    sys.exit(1)

# Create a abstract dag
root_dax = ADAG("root-workflow")

# Add input file to the DAX-level replica catalog
a = File("input")
a.addPFN(PFN("file://" + os.getcwd() + "/input", "local"))
root_dax.addFile(a)

c = File("sub-workflow.dax")
c.addPFN(PFN("file://"+os.getcwd()+"/sub-workflow.dax","local"))
root_dax.addFile(c)

# Add executables to the DAX-level replica catalog
# In this case the binary is linux split for splitting files, sub-workflow.py to generate the diamond sub dax.

e_split = Executable(namespace="linux", name="split",  os="linux", arch="x86_64", installed=True)
e_split.addPFN(PFN("file://"+os.getenv('CLUSTER_SOFTWARE_LOCATION')+"/split", "TestCluster"))
root_dax.addExecutable(e_split)
    
e_generate_sub = Executable(namespace="workflow", name="generate",  os="linux", arch="x86_64", installed=True)
e_generate_sub.addPFN(PFN("file://" + os.getcwd() + "/sub-workflow.py", "local"))
root_dax.addExecutable(e_generate_sub)

# Add a remote or local split job
split = Job(namespace="linux", name="split")
b = File("input_list")
split.addArguments("-l 1",a,"input.")
split.uses(a, link=Link.INPUT)
split.setStdout(b)
split.uses(b, link=Link.OUTPUT)
root_dax.addJob(split)

# Add a job to analyze the output of split and generate a sub dax with correct number of parallelism based on output of previous job
generate = Job(namespace="workflow", name="generate")
generate.addArguments(sys.argv[1],b,os.getcwd()+"/sub-workflow.dax")
#generate.setStdout(c)
generate.uses(b, link=Link.INPUT)
generate.addProfile(Profile(namespace="env",key="PYTHONPATH",value=os.environ['PYTHONPATH']))
#generate.uses(c, link=Link.OUTPUT)
root_dax.addJob(generate)

# Add a subdax job of type DAX that takes the runtime generated sub dax file in the previous step and runs the computation.
sub_dax = DAX(c)
sub_dax.addArguments("--output-site local","--basename sub-workflow")
root_dax.addJob(sub_dax)

# Add control-flow dependencies
root_dax.addDependency(Dependency(parent=split, child=generate))
root_dax.addDependency(Dependency(parent=generate, child=sub_dax))

# Write the DAX to stdout
root_dax.writeXML(sys.stdout)
