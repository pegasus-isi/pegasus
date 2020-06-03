#!/usr/bin/env python3
import logging
import sys

from Pegasus.api import *

# --- Transformations ----------------------------------------------------------
echo = Transformation(
        "echo", 
        pfn="/bin/echo",
        site="condorpool",
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
except PegasusClientError as e:
    print(e.output)