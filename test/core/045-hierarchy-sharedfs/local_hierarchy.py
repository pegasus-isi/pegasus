#!/usr/bin/env python3
import subprocess
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

props["pegasus.dir.storage.deep"] = "false"
props["pegasus.data.configuration"] = "sharedfs"

props.write()

# --- Sites --------------------------------------------------------------------
sites = """
pegasus: "5.0"
sites:
 -
  name: "CCG"
  arch: "x86_64"
  os.type: "linux"
  os.release: "rhel"
  os.version: "7"
  directories:
   -
    type: "sharedScratch"
    path: "/lizard/scratch-90-days/CCG/scratch/{run_id}"
    fileServers:
     -
      operation: "all"
      url: "gsiftp://obelix.isi.edu/lizard/scratch-90-days/CCG/scratch/{run_id}"
   -
    type: "localStorage"
    path: "/lizard/scratch-90-days/CCG/outputs"
    fileServers:
     -
      operation: "all"
      url: "gsiftp://obelix.isi.edu/lizard/scratch-90-days/CCG/outputs/{run_id}"
  grids:
   -
    type: "gt5"
    contact: "obelix.isi.edu/jobmanager-condor"
    scheduler: "condor"
    jobtype: "compute"
   -
    type: "gt5"
    contact: "obelix.isi.edu/jobmanager-fork"
    scheduler: "fork"
    jobtype: "auxillary"
  profiles:
    env:
      PEGASUS_HOME: "{condor_pool_pegasus_home}"
 -
  name: "local"
  arch: "x86_64"
  os.type: "linux"
  os.release: "rhel"
  os.version: "7"
  directories:
   -
    type: "sharedScratch"
    path: "{work_dir}/local-site/scratch"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/local-site/scratch"
   -
    type: "localStorage"
    path: "{work_dir}/outputs/local-site"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/outputs/local-site"
""".format(
        run_id=RUN_ID, 
        work_dir=str(WORK_DIR),
        condor_pool_pegasus_home="/usr"
    )

with open("sites.yml", "w") as f:
        f.write(sites)

# --- Transformations ----------------------------------------------------------

try:
    pegasus_config = subprocess.run(
        ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
except FileNotFoundError as e:
    print("Unable to find pegasus-config")

assert pegasus_config.returncode == 0

PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()

transformations = """
pegasus: "5.0"
transformations:
 -
  namespace: "diamond"
  name: "analyze"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "diamond"
  name: "findrange"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "diamond"
  name: "preprocess"
  version: "4.0"
  sites:
   -
    name: "local"
    type: "stageable"
    pfn: "{pegasus_bin_dir}/pegasus-keg"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "level1"
  name: "sleep"
  sites:
   -
    name: "CCG"
    type: "installed"
    pfn: "/bin/sleep"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
 -
  namespace: "level2"
  name: "sleep"
  sites:
   -
    name: "CCG"
    type: "installed"
    pfn: "/bin/sleep"
    arch: "x86_64"
    os.type: "linux"
    os.release: "rhel"
    os.version: "7"
""".format(pegasus_bin_dir=PEGASUS_BIN_DIR)

with open("transformations.yml", "w") as f:
        f.write(transformations)

# --- Input Directory Setup ----------------------------------------------------
try:
    Path.mkdir(Path("input"))
except FileExistsError:
    pass

# --- Blackdiamond Subworkflow -------------------------------------------------
with open("input/f.a", "w") as f:
    f.write("Sample input file\n")

fa = File("f.a")
fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

wf = Workflow("blackdiamond", infer_dependencies=True)\
    .add_jobs(
        Job("preprocess", namespace="diamond", version="4.0")
        .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)
        .add_inputs(fa)
        .add_outputs(fb1, fb2, register_replica=True),

        Job("findrange", namespace="diamond", version="4.0")
        .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)
        .add_inputs(fb1)
        .add_outputs(fc1, register_replica=True),

        Job("findrange", namespace="diamond", version="4.0")
        .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)
        .add_inputs(fb2)
        .add_outputs(fc2, register_replica=True),

        Job("analyze", namespace="diamond", version="4.0")
        .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)
        .add_inputs(fc1, fc2)
        .add_outputs(fd, register_replica=True),

    )\
    .write(str(TOP_DIR / "input/blackdiamond.yml"))

# --- Sleep Subworkflow --------------------------------------
j1 = Job("sleep", _id="sleep1", namespace="level1").add_args(2)
j2 = Job("sleep", _id="sleep2", namespace="level2").add_args(2)
wf = Workflow("sleep-wf")\
        .add_jobs(j1, j2)\
        .add_dependency(j1, children=[j2])\
        .write(str(TOP_DIR / "input/sleep.yml"))

# --- Top Level Workflow -------------------------------------------------------
wf = Workflow("local-hierarchy")

blackdiamond_wf = SubWorkflow("blackdiamond.yml", False)\
                .add_args("--input-dir", "input", "--output-site", "local", "-vvv")

sleep_wf = SubWorkflow("sleep.yml", False)\
                .add_args("--output-site", "local", "-vvv")

wf.add_jobs(blackdiamond_wf, sleep_wf)
wf.add_dependency(blackdiamond_wf, children=[sleep_wf])

try:
    wf.plan(
        verbose=5,
        site=["CCG"],
        dir=str(WORK_DIR),
        relative_dir=RUN_ID,
        input_dir="input",
        submit=True
    )
except Exception as e:
    print(e)
    print(e.args[1].stdout)
    print(e.args[1].stderr)
