#!/usr/bin/env python3
import logging
import sys

from Pegasus.api import *

# launch-bamboo-test needs these logs to be sent to stdout
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

# --- Transformations ----------------------------------------------------------
echo = Transformation(
        "echo", 
        pfn="/bin/echo", 
        is_stageable=False, 
        site="condorpool"
    )
  
tc = TransformationCatalog()\
        .add_transformations(echo)

# --- Workflow -----------------------------------------------------------------
Workflow("hello-world", infer_dependencies=True)\
    .add_jobs(
        Job(echo)
            .add_args("Hello World")
    ).add_transformation_catalog(tc)\
    .plan(submit=True)

