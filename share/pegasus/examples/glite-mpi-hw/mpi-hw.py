#!/usr/bin/env python3

from Pegasus.DAX3 import *
import sys
import os


# Create a abstract dag
mpi_hw_wf = ADAG("mpi-hello-world")

# Add input file to the DAX-level replica catalog
fin = File("fin")
fin.addPFN(PFN("file://" + os.getcwd() + "/f.in", "hpcc"))
mpi_hw_wf.addFile(fin)
        
# Add executables to the DAX-level transformation catalog
# For submitting MPI jobs directly through condor without GRAM
# we need to refer to wrapper that calls mpiexec with 
# the mpi executable
e_mpi_hw = Executable(namespace="pegasus", name="mpihw", os="linux", arch="x86_64", installed=True)
e_mpi_hw.addPFN(PFN("file://" + os.getcwd() + "/mpi-hello-world-wrapper", "hpcc"))
mpi_hw_wf.addExecutable(e_mpi_hw)


# Add the mpi hello world job
mpi_hw_job = Job(namespace="pegasus", name="mpihw" )
fout = File("f.out")
mpi_hw_job.addArguments("-o ", fout )
mpi_hw_job.uses(fin, link=Link.INPUT)
mpi_hw_job.uses(fout, link=Link.OUTPUT)

# tell pegasus it is an MPI job
mpi_hw_job.addProfile( Profile( "globus", "jobtype", "mpi"))

# add profiles indicating PBS specific parameters for HPCC
# pegasus.nodes corresponds to PBS -l nodes
mpi_hw_job.addProfile( Profile("pegasus", "nodes", "1" ))

# the globus key xcount is PROCS or PPN
# pegasus.ppn corresponds to PBS-l ppn
mpi_hw_job.addProfile( Profile("pegasus", "ppn", "16" ))    

# pegasus.runtime corresponds to PBS -l walltime  
# pegasus.runtime specified in seconds
mpi_hw_job.addProfile( Profile("pegasus", "runtime", "120"))
mpi_hw_wf.addJob(mpi_hw_job)

# Write the DAX to stdout
mpi_hw_wf.writeXML(sys.stdout)

