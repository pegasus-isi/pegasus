import os
import sys
from Pegasus.DAX3 import *

if len(sys.argv) != 2:
    print "Usage: python daxgen.py DAXFILE"
    exit(1)

daxfile = sys.argv[1]

def create_job(cluster):
    j = Job(name="sleep")
    j.addArguments("1")
    j.profile(namespace="pegasus", key="label", value="dag_%d" % cluster)
    return j

dax = ADAG("pmc-only")

sleep = Executable(name="sleep", os="linux", arch="x86", installed=True)
sleep.PFN("file:///bin/sleep", "local")
dax.addExecutable(sleep)

# Create 48 jobs in 4 clusters of 12
for i in range(4):
    parent = create_job(i)
    dax.addJob(parent)
    
    child = create_job(i)
    dax.addJob(child)
    
    for j in range(10):    
        j = create_job(i)
        dax.addJob(j)
        
        dax.depends(parent=parent, child=j)
        dax.depends(parent=j, child=child)

f = open(daxfile, "w")
dax.writeXML(f)
f.close()
