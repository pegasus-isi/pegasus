#!/usr/bin/env python3

import os
import sys

from Pegasus.api import *

# Create a abstract dag
diamond = Workflow("super-diamond")

rc = ReplicaCatalog()
tc = TransformationCatalog()
diamond.add_replica_catalog(rc)
diamond.add_transformation_catalog(tc)

# Add input file to the DAX-level replica catalog
a = File("one.dax")
rc.add_replica("local", a.lfn, "file://" + os.getcwd() + "/one.dax")

b = File("two.dax")
rc.add_replica("local", b.lfn, "file://" + os.getcwd() + "/two.dax")

# Add DAX job 1
one = SubWorkflow("one.dax")
one.add_planner_args(staging_sites={"condor_pool": "local"}, verbose=4)
diamond.add_jobs(one)

# Add DAX job 2
two = SubWorkflow("two.dax")
two.add_planner_args(staging_sites={"condor_pool": "local"}, verbose=4)
diamond.add_jobs(two)

# Add control-flow dependencies
diamond.add_dependency(one, children=[two])

# Write the DAX to stdout
diamond.write(sys.stdout)
