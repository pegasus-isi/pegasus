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
    j.profile(namespace="pegasus", key="pmc_request_memory", value="1")
    j.profile(namespace="pegasus", key="pmc_request_cpus", value="2")
    j.profile(namespace="pegasus", key="pmc_priority", value="1")
    j.profile(namespace="pegasus", key="pmc_task_arguments", value="--tries 3")
    return j

dax = ADAG("mpi-cluster")

sleep = Executable(name="sleep", os="linux", arch="x86_64", osrelease="rhel", osversion="7", installed=True)
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
