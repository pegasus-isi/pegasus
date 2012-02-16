#!/usr/bin/env python

from Pegasus.DAX3 import ADAG, File, Link, Job, Executable, PFN, Profile
import sys
import os
import ConfigParser

if len(sys.argv) != 3:
    print "Usage: %s PEGASUS_HOME" % (sys.argv[0])
    sys.exit(1)

config = ConfigParser.ConfigParser({'input_file':'', 'workflow_name':'horizontal-clustering-test', 'executable_installed':"False", 'clusters_size':"3", 'clusters_maxruntime':"7"})
config.read(sys.argv[2] + '/test.config')

# Create an abstract dag
cluster = ADAG (config.get('all', 'workflow_name'))

input_file = config.get('all', 'input_file')
if (input_file == ''):
        input_file = os.getcwd ()
else:
        input_file += '/' + os.getenv ('USER') + '/inputs'

# Add input file to the DAX-level replica catalog
a = File("f.a")
a.addPFN(PFN(config.get('all', 'file_url') + input_file + "/f.a", config.get('all', 'file_site')))
cluster.addFile(a)

for i in range (1, 3):
    sleep = Executable (namespace = "cluster", name = "level" + str (i), version = "1.0", os = "linux", arch = "x86", installed=config.getboolean('all', 'executable_installed'))
    sleep.addPFN (PFN (config.get('all', 'executable_url') + sys.argv[1] + "/bin/pegasus-keg", config.get('all', 'executable_site')))
    sleep.addProfile (Profile (namespace = "pegasus", key = "clusters.size", value = config.get('all', 'clusters_size')))
    sleep.addProfile (Profile (namespace = "pegasus", key = "clusters.maxruntime", value = config.get('all', 'clusters_maxruntime')))
    cluster.addExecutable(sleep)

for i in range (4):
    job = Job (namespace = "cluster", name = "level1", version = "1.0")
    job.addArguments('-a level1 -T ' + str (i + 1))
    job.addArguments('-i', a)
    job.addProfile (Profile (namespace = "pegasus", key = "job.runtime", value = str (i + 1)))
    job.uses(a, link=Link.INPUT)
    cluster.addJob (job)

    for j in range (4):
        child = Job (namespace = "cluster", name = "level2", version = "1.0")
	child.addArguments('-a level2 -T ' + str ((j + 1) * 2))
        child.addProfile (Profile (namespace = "pegasus", key = "job.runtime", value = str ((j + 1) * 2)))
        cluster.addJob (child)

        cluster.depends (parent = job, child = child)

# Write the DAX to standard out
cluster.writeXML (sys.stdout)
