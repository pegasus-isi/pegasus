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
SUBMIT_DIR = TOP_DIR / TEST_NAME / "submit"

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
STAGING = config["STAGING"] if "STAGING" in config else "workflow-webdav"
SHARED = config["SHARED"] if "SHARED" in config else False
SYMLINK = config["SYMLINK"] if "SYMLINK" in config else False
if not STAGING:
    # empty value in test.config
    STAGING = COMPUTE

logging.debug(f"Staging site for the test is {STAGING}")

shared_scratch_dir = str(WORK_DIR / "shared-scratch")
staging_scratch_dir = str(WORK_DIR / "staging-site" / "scratch")
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)
condorpool_scratch_dir = "/scitech/shared/scratch-90-days/{}/{}/scratch".format(
    PEGASUS_VERSION, TEST_NAME
)
condorpool_shared_dir = "/scitech/shared/scratch-90-days/{}/{}/shared".format(
    PEGASUS_VERSION, TEST_NAME
)

cmd_properties = {}
site_catalog_file = TOP_DIR / TEST_NAME / "sites.yml"
logging.info("Generating site catalog at: {}".format(site_catalog_file))
cmd_properties["pegasus.catalog.site.file"] = site_catalog_file

compute_site = Site(COMPUTE, arch=Arch.X86_64, os_type=OS.LINUX).add_pegasus_profile(
    style="condor").add_pegasus_profile(clusters_num=1)
if SHARED:
    compute_site.add_directories(
        Directory(
            Directory.SHARED_SCRATCH, str(condorpool_scratch_dir), shared_file_system=SHARED
        ).add_file_servers(
            FileServer(
                "scp://bamboo@bamboo.isi.edu/" + condorpool_scratch_dir, Operation.ALL
            )
        )
    )

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
    compute_site,
    Site("workflow-webdav", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
        Directory(Directory.SHARED_SCRATCH, staging_scratch_dir).add_file_servers(
            FileServer(
                "webdavs://workflow.isi.edu/webdav" + str(condorpool_scratch_dir),
                Operation.ALL,
            )
        )
    ),
).write(str(site_catalog_file))

# --- Replicas -----------------------------------------------------------------
replica_catalog_file = TOP_DIR / TEST_NAME / "replicas.yml"
logging.info("Generating replica catalog at: {}".format(replica_catalog_file))
cmd_properties["pegasus.catalog.replica.file"] = replica_catalog_file

# create initial input file
INPUT_DIR = Path(condorpool_shared_dir) if SHARED else TOP_DIR / TEST_NAME
os.makedirs(INPUT_DIR, exist_ok=True)
with open("{}/f.a".format(INPUT_DIR), "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.a").add_metadata({"㐦": "㒦"})
ReplicaCatalog().add_replica(COMPUTE if SHARED else LOCAL, fa, INPUT_DIR / fa.lfn).write(str(replica_catalog_file))

# --- Transformations ----------------------------------------------------------

transformation_catalog_file = TOP_DIR / TEST_NAME / "transformations.yml"
logging.info("Generating transformation catalog at: {}".format(transformation_catalog_file))
cmd_properties["pegasus.catalog.transformation.file"] = transformation_catalog_file

container_mounts = {}
if SYMLINK:
    # mount the shared dir where the raw input is
    container_mounts["mounts"] = ["{}:/existing/data:ro".format(condorpool_shared_dir)]
base_container = Container(
    "centos-osgvo-el8",
    Container.DOCKER,
    image_site="local",
    image="docker:///hub.opensciencegrid.org/opensciencegrid/osgvo-el8:latest",
    bypass_staging=False,
    **container_mounts
)
base_container.add_env("APP_HOME", "/tmp/myscratch")
base_container.add_env("JAVA_HOME", "/bin/java.1.8")


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
).write(str(transformation_catalog_file))

# --- Workflow -----------------------------------------------------------------
logging.info("Generating workflow")

fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

try:
    Workflow("blackdiamond").add_jobs(
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
        dir=f"{SUBMIT_DIR}",
        verbose=3,
        sites=[COMPUTE],
        staging_sites={COMPUTE: STAGING},
        output_sites=[LOCAL],
        cluster=["horizontal"],
        force=True,
        **cmd_properties
    )
except PegasusClientError as e:
    logging.error(e.output)
