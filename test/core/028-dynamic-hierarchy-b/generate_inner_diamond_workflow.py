#!/usr/bin/env python3
from Pegasus.api import *

# --- Workflow -----------------------------------------------------------------
fa = File("f.a")
fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

wf = Workflow("diamond")

preprocess_job = Job("preprocess")\
                    .add_args("-a", "preprocess", "-T", "10", "-i", fa, "-o", fb1, fb2)\
                    .add_inputs(fa)\
                    .add_outputs(fb1, fb2)

findrange_1_job = Job("findrange")\
                    .add_args("-a", "findrange", "-T", "5", "-i", fb1, "-o", fc1)\
                    .add_inputs(fb1)\
                    .add_outputs(fc1)

findrange_2_job = Job("findrange")\
                    .add_args("-a", "findrange", "-T", "5", "-i", fb2, "-o", fc2)\
                    .add_inputs(fb2)\
                    .add_outputs(fc2)

analyze_job = Job("analyze")\
                .add_args("-a", "analyze", "-T", "10", "-i", fc1, fc2, "-o", fd)\
                .add_inputs(fc1, fc2)\
                .add_outputs(fd)

wf.add_jobs(
    preprocess_job,
    findrange_1_job,
    findrange_2_job,
    analyze_job
)

wf.write(file="inner_diamond_workflow.yml")
