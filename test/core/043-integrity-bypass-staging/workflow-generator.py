#!/usr/bin/env python3

import sys
import os
from pathlib import Path
from Pegasus.api import *

TOP_DIR = Path(Path.cwd())

wf = Workflow('bypass-staging')

# the workflow will be planned by not executed - src does not need to exist
infile = File('input.txt')
rc = (ReplicaCatalog()
       .add_replica('local',
                    infile, 
                    'file://' + str(TOP_DIR / infile.lfn),
                    checksum = {'sha256':'66a42b4be204c824a7533d2c677ff7cc5c44526300ecd6b450602e06128063f9'}
                ))

tc = (TransformationCatalog()
       .add_transformations(Transformation('ls', site='condorpool', pfn='/bin/ls', is_stageable=False)))

job = (Job('ls')
        .add_args('-l')
        .add_inputs(infile))
wf.add_jobs(job)

wf.add_transformation_catalog(tc)
wf.add_replica_catalog(rc)
wf.write('workflow.yml')

