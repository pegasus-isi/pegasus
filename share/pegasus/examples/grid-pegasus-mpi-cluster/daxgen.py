#!/usr/bin/env python3

import os
import sys
from Pegasus.DAX3 import *

if len(sys.argv) != 2:
    print "Usage: python daxgen.py DAXFILE"
    exit(1)

daxfile = sys.argv[1]

def create_job(cluster):
    # The test job just sleeps for 1 second
    j = Job(name="sleep")
    j.addArguments("1")
    
    # This is how you do label-based clustering
    j.profile(namespace="pegasus", key="label", value="dag_%d" % cluster)
    
    # This is how you tell PMC to reserve memory and CPUs for the task
    j.profile(namespace="pegasus", key="pmc_request_memory", value="1")
    j.profile(namespace="pegasus", key="pmc_request_cpus", value="2")
    
    # This is how you tell PMC to prioritize a task
    j.profile(namespace="pegasus", key="pmc_priority", value="1")
    
    # This is how you tell PMC to retry a task 3 times
    j.profile(namespace="pegasus", key="pmc_task_arguments", value="--tries 3")
    
    return j

dax = ADAG("pmc_sleep")

# Create 48 jobs in 4 clusters of 12
# Each cluster is a fork-join structure with 1 fork job,
# 10 compute jobs, and one join job
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
