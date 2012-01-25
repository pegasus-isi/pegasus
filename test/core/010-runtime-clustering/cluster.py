#!/usr/bin/env python

from Pegasus.DAX3 import ADAG, Job, Executable, PFN, Profile
import sys
import os

if len(sys.argv) != 2:
    print "Usage: %s PEGASUS_HOME" % (sys.argv[0])
    sys.exit(1)

# Create an abstract dag
cluster = ADAG ("horizontal-clustering-test")

for i in range (1, 3):
    sleep = Executable (namespace = "cluster", name = "level" + str (i), version = "1.0", os = "linux", arch = "x86")
    sleep.addPFN (PFN ("file:///bin/sleep", "condorpool"))
    sleep.addProfile (Profile (namespace = "pegasus", key = "clusters.size", value = "3"))
    sleep.addProfile (Profile (namespace = "pegasus", key = "clusters.maxruntime", value = "7"))
    cluster.addExecutable(sleep)

for i in range (4):
    job = Job (namespace = "cluster", name = "level1", version = "1.0")
    job.addArguments(str (i + 1))
    job.addProfile (Profile (namespace = "pegasus", key = "job.runtime", value = str (i + 1)))
    cluster.addJob (job)

    for j in range (4):
        child = Job (namespace = "cluster", name = "level2", version = "1.0")
        child.addArguments(str ((j + 1) * 2))
        child.addProfile (Profile (namespace = "pegasus", key = "job.runtime", value = str ((j + 1) * 2)))
        cluster.addJob (child)

        cluster.depends (parent = job, child = child)

# Write the DAX to standard out
cluster.writeXML (sys.stdout)
