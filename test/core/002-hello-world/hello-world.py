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
        site="condorpool",
        is_stageable=False
    )
  
tc = TransformationCatalog()\
        .add_transformations(echo)

# --- Workflow -----------------------------------------------------------------
try:
    Workflow("hello-world")\
        .add_jobs(
            Job(echo)
                .add_args("Hello World")
                .set_stdout("hello.out")
        ).add_transformation_catalog(tc)\
        .plan(submit=True)
except Exception as e:
    print(e)
    print(e.args[1].stdout)
    print(e.args[1].stderr)
