#!/usr/bin/env python3

from Pegasus.api import *

# --- Workflow -----------------------------------------------------------------
wf = Workflow("sleep-wf")

sleep_1 = Job("sleep").add_args(2)
sleep_2 = Job("sleep").add_args(2)

wf.add_jobs(sleep_1, sleep_2)
wf.add_dependency(job=sleep_1, children=[sleep_2])
wf.write(file="inner_sleep_workflow.yml")
