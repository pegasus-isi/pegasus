#!/usr/bin/env python

from Pegasus.DAX3 import *
import sys
import os


# Create a abstract dag
mpi_hw_wf = ADAG("mpi-hello-world")

# Add input file to the DAX-level replica catalog
fin = File("fin")
fin.addPFN(PFN("file://" + os.getcwd() + "./input/f.in", "bluewaters"))
mpi_hw_wf.addFile(fin)
        

# Add the mpi hello world job
mpi_hw_job = Job(namespace="pegasus", name="mpihw" )
fout = File("f.out")
mpi_hw_job.addArguments("-o ", fout )
mpi_hw_job.uses(fin, link=Link.INPUT)
mpi_hw_job.uses(fout, link=Link.OUTPUT)

# tell pegasus it is an MPI job
mpi_hw_job.addProfile( Profile( "globus", "jobtype", "mpi"))

# add profiles indicating PBS specific parameters for BLUEWATERS

# pegasus.cores 
mpi_hw_job.addProfile( Profile("pegasus", "cores", "32" ))
# pegasus.nodes 
mpi_hw_job.addProfile( Profile("pegasus", "nodes", "2" ))    
# pegasus.ppn 
mpi_hw_job.addProfile( Profile("pegasus", "ppn", "16" ))    

# pegasus.runtime is walltime in seconds. 
mpi_hw_job.addProfile( Profile("pegasus", "runtime", "300"))
mpi_hw_wf.addJob(mpi_hw_job)

# Write the DAX to stdout
mpi_hw_wf.writeXML(sys.stdout)

