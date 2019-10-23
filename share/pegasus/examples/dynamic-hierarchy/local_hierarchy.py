#!/usr/bin/env python3

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


#pre_dax = Job(namespace="level1", name="sleep")
#pre_dax.addArguments('5')
#adag.addJob(pre_dax)

# Add a job to generate the DAX
daxfile = File('inner.dax')
gen_dax = Job(namespace="blackdiamond", name="generate")
gen_dax.addArguments( cluster_pegasus_home)
gen_dax.addArguments( "inner.dax")
gen_dax.uses( daxfile, link=Link.OUTPUT, transfer=True)
adag.addJob(gen_dax)

dax1 = DAX (daxfile)
#DAX jobs are called with same arguments passed, while planning the root level dax
dax1.addArguments('--conf dax1.properties')
dax1.addArguments('--output-site local')
dax1.addArguments( '-vvv')
# the dax job needs a basename option as DAX doesnt exist when outer level workflow is planned
dax1.addArguments( '--basename inner' )
dax1.uses( File("dax1.properties"), link=Link.INPUT)
dax1.uses( File("dax1.rc"), link=Link.INPUT)
dax1.uses( File("dax1.sites.xml"), link=Link.INPUT)
dax1.uses( File("dax1.tc.text"), link=Link.INPUT)
adag.addJob(dax1)


# this dax job uses a pre-existing dax file
# that has to be present in the replica catalog
daxfile2 = File('inner2.dax')
dax2 = DAX (daxfile2)
#pegasus-plan arguments for the DAX jobs can be overwritten
dax2.addArguments('--output-site local')
dax2.addArguments( '-vvv')
adag.addJob(dax2)

# Add a job
post_dax = Job(namespace="level2", name="sleep")
post_dax.addArguments('5')
adag.addJob(post_dax)

# Add control-flow dependencies
adag.addDependency(Dependency(parent=gen_dax, child=dax1))
adag.addDependency(Dependency(parent=dax1, child=dax2))
adag.addDependency(Dependency(parent=dax2, child=post_dax))

# Write the DAX to stdout
adag.writeXML(sys.stdout)

