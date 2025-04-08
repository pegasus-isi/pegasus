#!/usr/bin/env python3

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from Pegasus.api import *
from Pegasus.tools import worker_utils as utils

logging.basicConfig(level=logging.DEBUG)

PEGASUS_LOCATION = utils.backticks("which pegasus-keg").strip()
PEGASUS_VERSION = utils.backticks("pegasus-version").strip()

# figure out test name from arguments
if len(sys.argv) != 2:
    logging.error("ERROR: worflow generator requires testname to invoke as an argument")
    sys.exit(1)

TEST_NAME = sys.argv[1]

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-5.0-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work" / PEGASUS_VERSION / TEST_NAME

try:
    Path.mkdir(WORK_DIR, parents=True)
except FileExistsError:
    pass


# --- Configuration ------------------------------------------------------------
PEGASUS_CONF = f"{TEST_NAME}/pegasusrc"
print(PEGASUS_CONF)

# pick the test config file
config = json.load(open(f"{TEST_NAME}/test.config"))

# --- Sites --------------------------------------------------------------------
LOCAL = "local"
COMPUTE = "condorpool"
STAGING = config["STAGING"] if "STAGING" in config else "cartman-data"
SHARED  = config["SHARED"] if "SHARED" in config else False
if not STAGING:
    # empty value in test.config
    STAGING = COMPUTE

logging.debug(f"Staging site for the test is {STAGING}")

shared_scratch_dir = str(WORK_DIR / "shared-scratch")
staging_scratch_dir = str(WORK_DIR / "staging-site" / "scratch")
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)
condorpool_scratch_dir = "/webdav/scitech/shared/scratch-90-days/{}/{}/scratch".format(
    PEGASUS_VERSION, TEST_NAME
)
condorpoool_shared_dir = "/scitech/shared/scratch-90-days/{}/{}/shared".format(
    PEGASUS_VERSION, TEST_NAME
)
logging.info("Generating site catalog at: {}".format(TOP_DIR / "sites.yml"))

SiteCatalog().add_sites(
    Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
    .add_directories(
        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        ),
        Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
            FileServer("file://" + local_storage_dir, Operation.ALL)
        ),
    )
    .add_pegasus_profile(clusters_num=1)
    .add_env("SSH_PRIVATE_KEY", "/scitech/shared/home/bamboo/.ssh/workflow_id_rsa"),
    Site(COMPUTE, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_directories(
        Directory(
            Directory.SHARED_SCRATCH, str(condorpool_scratch_dir)
        ).add_file_servers(
            FileServer(
                "webdavs://workflow.isi.edu/" + str(condorpool_scratch_dir),
                Operation.ALL,
            )
        )
    )
    .add_pegasus_profile(style="condor")
    .add_pegasus_profile(clusters_num=1)
    .add_condor_profile(universe="container"),
    Site("cartman-data", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
        Directory(Directory.SHARED_SCRATCH, staging_scratch_dir).add_file_servers(
            FileServer(
                "scp://bamboo@bamboo.isi.edu/" + staging_scratch_dir, Operation.ALL
            )
        )
    ),
).write()

# --- Replicas -----------------------------------------------------------------

logging.info("Generating replica catalog at: {}".format(TOP_DIR / "replicas.yml"))

# create initial input file
INPUT_DIR = Path(condorpoool_shared_dir) if SHARED else TOP_DIR
    
with open("f.a", "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.a").add_metadata({"㐦": "㒦"})
ReplicaCatalog().add_replica(COMPUTE if SHARED else LOCAL, fa, INPUT_DIR / fa.lfn).write()

# --- Transformations ----------------------------------------------------------

logging.info(
    "Generating transformation catalog at: {}".format(TOP_DIR / "transformations.yml")
)

base_container = Container(
    "osgvo-el7",
    Container.SINGULARITY,
    image_site="local",
    image="scp://bamboo@bamboo.isi.edu/ceph/kubernetes/pv/data/data-html/osg/images/opensciencegrid__osgvo-el7__latest.sif",
    bypass_staging=False,
)

preprocess = Transformation("preprocess", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
        container=base_container,
    )
)

findrage = Transformation("findrange", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
        container=base_container,
    )
)

analyze = Transformation("analyze", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
        container=base_container,
    )
)

TransformationCatalog().add_containers(base_container).add_transformations(
    preprocess, findrage, analyze
).write("transformations.yml")

# --- Workflow -----------------------------------------------------------------
logging.info("Generating workflow")

fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

try:
    Workflow("blackƊiamond").add_jobs(
        Job(preprocess)
        .add_args("-a", "preprocess", "-T10", "-i", fa, "-o", fb1, fb2)
        .add_inputs(fa)
        .add_outputs(fb1, fb2, register_replica=True),
        Job(findrage)
        .add_args("-a", "findrange", "-T10", "-i", fb1, "-o", fc1)
        .add_inputs(fb1)
        .add_outputs(fc1, register_replica=True),
        Job(findrage)
        .add_args("-a", "findrange", "-T10", "-i", fb2, "-o", fc2)
        .add_inputs(fb2)
        .add_outputs(fc2, register_replica=True),
        Job(analyze)
        .add_args("-a", "analyze", "-T10", "-i", fc1, fc2, "-o", fd)
        .add_inputs(fc1, fc2)
        .add_outputs(fd, register_replica=True),
    ).plan(
        conf=PEGASUS_CONF,
        dir=f"{WORK_DIR}/dags",
        verbose=3,
        sites=[COMPUTE],
        staging_sites={COMPUTE: STAGING},
        output_sites=[LOCAL],
        cluster=["horizontal"],
        force=True,
    )
except PegasusClientError as e:
    logging.error(e.output)
