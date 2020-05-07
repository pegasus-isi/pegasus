#!/usr/bin/env python3
import logging
import sys
import subprocess

from pathlib import Path
from datetime import datetime

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "023-sc4-ssh-http-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Configuration ------------------------------------------------------------
print("Generating pegasus.properties at: {}".format(TOP_DIR / "pegasus.properties"))

props = Properties()

props["pegasus.dir.useTimestamp"] = "true"
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.data.configuration"] = "nonsharedfs"

props.write()

# --- Sites --------------------------------------------------------------------
print("Generating site catalog at: sites.yml")

LOCAL = "local"
CONDOR_POOL = "condorpool"
STAGING_SITE = "staging_site"

try:
    pegasus_config = subprocess.run(
        ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
except FileNotFoundError as e:
    print("Unable to find pegasus-config")

assert pegasus_config.returncode == 0

PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()

sites = """
pegasus: "5.0"
sites:
 -
  name: "condor_pool"
  arch: "x86_64"
  os.type: "linux"
  profiles:
    condor:
      universe: "vanilla"
    pegasus:
      style: "condor"
 -
  name: "staging_site"
  arch: "x86_64"
  os.type: "linux"
  directories:
   -
    type: "sharedScratch"
    path: "/data/scratch/http"
    fileServers:
     -
      operation: "get"
      url: "http://obelix.isi.edu/scratch"
     -
      operation: "put"
      url: "scp://ptesting@obelix.isi.edu/data/scratch/http"
 -
  name: "local"
  arch: "x86_64"
  os.type: "linux"
  os.release: "rhel"
  os.version: "7"
  directories:
   -
    type: "sharedScratch"
    path: "{work_dir}/scratch"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/scratch"
   -
    type: "localStorage"
    path: "{work_dir}/outputs"
    fileServers:
     -
      operation: "all"
      url: "file://{work_dir}/outputs"
  profiles:
    env:
      PEGASUS_BIN_DIR: "{pegasus_bin_dir}"
    pegasus:
      ssh_private_key: "/localhome/bamboo/.ssh/workflow_id_rsa"
""".format(
    work_dir=str(WORK_DIR), pegasus_bin_dir=PEGASUS_BIN_DIR
)

with (TOP_DIR / "sites.yml").open(mode="w") as f:
    f.write(sites)

# --- Transformations ----------------------------------------------------------
rosetta_exe = Transformation(
    "rosetta.exe",
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    site="local",
    pfn="file://" + str(TOP_DIR / "rosetta.exe"),
    is_stageable=True,
).add_pegasus_profile(clusters_size=3)

tc = TransformationCatalog().add_transformations(rosetta_exe)

# --- Replicas & Workflow ------------------------------------------------------
rc = ReplicaCatalog()

# add all files in minirosetta_database
inputs = list()


def get_files(d: Path) -> None:
    for p in d.iterdir():
        if p.is_file():
            f = File(str(p))
            inputs.append(f)
            rc.add_replica(LOCAL, str(p), str(p.resolve()))
        else:
            get_files(p)


get_files(Path("minirosetta_database"))

f1 = File("design.resfile")
inputs.append(f1)
rc.add_replica(LOCAL, f1, str(Path("design.resfile").resolve()))

f2 = File("repack.resfile")
inputs.append(f2)
rc.add_replica(LOCAL, f2, str(Path("repack.resfile").resolve()))

wf = Workflow("rosetta", infer_dependencies=True)

pdb_files = list(Path("pdbs").iterdir())
for i in range(10):
    current_file = pdb_files[i]

    if current_file.is_file():
        job = (
            Job(rosetta_exe, _id=current_file.name.replace(".pdb", ""))
            .add_inputs(File(current_file.name), *inputs)
            .add_outputs(File(current_file.name + ".score.sc"), register_replica=True)
            .add_args(
                "-in:file:s",
                current_file.name,
                "-out:prefix " + current_file.name + ".",
                "-database ./minirosetta_database",
                "-linmem_ig 10",
                "-nstruct 1",
                "-pert_num 2",
                "-inner_num 1",
                "-jd2::ntrials 1",
            )
        )

        rc.add_replica("local", current_file.name, str(current_file.resolve()))

        wf.add_jobs(job)

wf.add_transformation_catalog(tc)
wf.add_replica_catalog(rc)

try:
    wf.plan(
        dir=str(WORK_DIR),
        verbose=5,
        sites=[CONDOR_POOL],
        staging_sites={CONDOR_POOL: STAGING_SITE},
        submit=True,
    )
except Exception as e:
    print(e)
    print(e.args[1].stdout)
    print(e.args[1].stderr)
