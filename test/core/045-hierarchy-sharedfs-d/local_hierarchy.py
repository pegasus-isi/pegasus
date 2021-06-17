#!/usr/bin/env python3
import subprocess
import sys
import logging

from datetime import datetime
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Work Directory Setup -----------------------------------------------------
RUN_ID = "local-hierarchy-sharedfs-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Properties ---------------------------------------------------------------
props = Properties()
#props["pegasus.catalog.replica.file"] = "replicas.yml"
props.write()

# --- Replicas ---------------------------------------------------------------
with open("input.txt", "w") as f:
    f.write("test input file\n")

rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="input.txt", pfn=Path(__file__).parent.resolve() / "input.txt")
rc.add_replica(site="local", lfn="subwf1.yml", pfn=Path(__file__).parent.resolve() / "subwf1.yml")
rc.add_replica(site="local", lfn="subwf2.yml", pfn=Path(__file__).parent.resolve() / "subwf2.yml")
#rc.write()


# --- Transformations ---------------------------------------------------------------
try:
    pegasus_config = subprocess.run(
        ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
except FileNotFoundError as e:
    print("Unable to find pegasus-config")

assert pegasus_config.returncode == 0

PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()

tc = TransformationCatalog()
keg = Transformation(
            "keg",
            site="local",
            pfn=PEGASUS_BIN_DIR + "/pegasus-keg",
            is_stageable=True
        )

ls = Transformation(
            "ls",
            site="condorpool",
            pfn="/bin/ls",
            is_stageable=False
        )

cat = Transformation(
            "cat",
            site="condorpool",
            pfn="/bin/cat",
            is_stageable=False
        )

tc.add_transformations(keg, ls, cat)
tc.write()

# --- SubWorkflow1 ---------------------------------------------------------------
input_file = File("input.txt")
k1_out = File("k1.txt")
wf1 = Workflow("subworkflow-1")
k1 = Job(keg)\
        .add_args("-i", input_file, "-o", k1_out, "-T", 5)\
        .add_inputs(input_file)\
        .add_outputs(k1_out)

ls1 = Job(ls)\
        .add_args("-alh")

wf1.add_jobs(k1, ls1)
wf1.write("subwf1.yml")

# --- SubWorkflow2 ---------------------------------------------------------------
k2_out = File("k2.txt")
wf2 = Workflow("subworkflow-2")
k2 = Job(keg)\
        .add_args("-i", k1_out, "-o", k2_out, "-T", 5)\
        .add_inputs(k1_out)\
        .add_outputs(k2_out)

wf2.add_jobs(k2)
wf2.write("subwf2.yml")

# Root
root_wf = Workflow("root")

# we write out the replica catalog into the workflow to make sure it gets inherited
# by the sub workflow, or specify the location to it in the propoerties file
root_wf.add_replica_catalog(rc)

j1 = SubWorkflow("subwf1.yml", _id="subwf1")\
        .add_planner_args(force=True, verbose=3)\
        .add_inputs(input_file)\
        .add_outputs(k1_out)

j2 = SubWorkflow("subwf2.yml", _id="subwf2")\
        .add_planner_args(force=True, verbose=3)\
        .add_inputs(k1_out)\
        .add_outputs(k2_out)

root_wf.add_jobs(j1, j2)


try:
    root_wf.plan(submit=True)
except PegasusClientError as e:
    print(e.output)
