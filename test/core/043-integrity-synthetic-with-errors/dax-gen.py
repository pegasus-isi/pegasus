#!/usr/bin/python

from Pegasus.DAX3 import *
import sys
import os

# globals
wf = ADAG("synthetic", auto=True)

# keg is an input
keg = File(name="keg")
keg.addPFN(PFN("file://" + os.getcwd() + "/keg", "local"))
keg.metadata("checksum.type", "sha256")
keg.metadata("checksum.value", "d21f06ba2dfb7fe80ff13d5e0d3d4ef3eaa41b1a4941b8947859bce3164eb223")
wf.addFile(keg)


def add_file_checksum(f, path):
    # TODO
    f.metadata("checksum.type", "sha256")
    f.metadata("checksum.value", "630408689fceb0d34f68ab5b47a8be5ca554d4e257055c4829479f5de886970a")


def add_level(level_id, num_jobs, input_files, final_outputs):

    inputs_per_job = len(input_files) / num_jobs
    output_files = []

    print("Level " + str(level_id) + ": " + str(inputs_per_job) + " inputs per job")

    exe = Executable(name="level" + str(level_id), installed=False)
    exe.addPFN(PFN("file://" + os.getcwd() + "/job.sh", "local"))
    wf.addExecutable(exe)

    for i in range(num_jobs):
        job = Job(name="level" + str(level_id))
   
        # executable
        job.uses(keg, link=Link.INPUT)

        # inputs
        for j in range(inputs_per_job):
            job.uses(input_files.pop(0), link=Link.INPUT)

        f = File("%d-%d-1.data" %(level_id, i))
        job.uses(f, link=Link.OUTPUT, register=final_outputs)
        job.addArguments(f)
        output_files.append(f)

        f = File("%d-%d-2.data" %(level_id, i))
        job.uses(f, link=Link.OUTPUT, register=final_outputs)
        job.addArguments(f)
        output_files.append(f)

        wf.addJob(job)

    return output_files
    

l1_outputs = add_level(1, 50, [], False)
l2_outputs = add_level(2, 10, l1_outputs, False)
l2_outputs = add_level(3, 20, l2_outputs, True)

f = open("dax.xml", "w")
wf.writeXML(f)
f.close()

