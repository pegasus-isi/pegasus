#!/usr/bin/env python3

from Pegasus.api import *

# --- Transformations ----------------------------------------------------------
echo = Transformation("echo", pfn="/bin/echo", is_stageable=True)
  
tc = TransformationCatalog()\
        .add_transformations(echo)

# --- Workflow -----------------------------------------------------------------
Workflow("hello-world", infer_dependencies=True)\
    .add_jobs(
        Job(echo)
            .add_args("Hello World")
    ).add_transformation_catalog(tc)\
    .plan(submit=True)

