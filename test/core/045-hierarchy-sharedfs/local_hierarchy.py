#!/usr/bin/env python

import os
import sys
import subprocess

if len(sys.argv) != 2:
        print "Usage: %s CLUSTER_PEGASUS_HOME" % (sys.argv[0])
        sys.exit(1)

cluster_pegasus_home=sys.argv[1]


# to setup python lib dir for importing Pegasus PYTHON DAX API
#pegasus_config = os.path.join("pegasus-config") + " --noeoln --python"
#lib_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]
#Insert this directory in our search path
#os.sys.path.insert(0, lib_dir)

from Pegasus.DAX3 import *


# Create a abstract dag
adag = ADAG('local-hierarchy')

daxfile = File('blackdiamond.dax')
dax1 = DAX (daxfile)
#DAX jobs are called with same arguments passed, while planning the root level dax
dax1.addArguments('--output-site local')
dax1.addArguments( '-vvv')
adag.addJob(dax1)


# this dax job uses a pre-existing dax file
# that has to be present in the replica catalog
daxfile2 = File('blackdiamond.dax')
dax2 = DAX (daxfile2)
dax2.addArguments('--output-site local')
dax2.addArguments( '-vvv')
adag.addJob(dax2)


# Add control-flow dependencies
#adag.addDependency(Dependency(parent=dax1, child=dax2))


# Write the DAX to stdout
adag.writeXML(sys.stdout)

