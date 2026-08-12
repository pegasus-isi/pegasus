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
    logging.error("ERROR: workflow generator requires testname to invoke as an argument")
    sys.exit(1)

TEST_NAME = sys.argv[1]

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-5.0-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
BASE_TEST = TOP_DIR.name
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
COMPUTE = "slurm"
STAGING = config["STAGING"] if "STAGING" in config else COMPUTE
SHARED = config["SHARED"] if "SHARED" in config else False
CONTAINERS = config["CONTAINERS"] if "CONTAINERS" in config else False

if not STAGING:
    STAGING = COMPUTE

logging.debug(f"Staging site for the test is {STAGING}")

shared_scratch_dir = str(WORK_DIR / "shared-scratch")
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)
# scratch directory on the shared filesystem visible to both submit host and compute nodes
slurm_scratch_dir = "/scitech/shared/scratch-90-days/{}/{}/{}/scratch".format(
    PEGASUS_VERSION, BASE_TEST, TEST_NAME
)
# separate shared directory used to pre-stage the workflow input file in sharedfs mode
slurm_shared_dir = "/scitech/shared/scratch-90-days/{}/{}/{}/shared".format(
    PEGASUS_VERSION, BASE_TEST, TEST_NAME
)

cmd_properties = {}
site_catalog_file = TOP_DIR / TEST_NAME / "sites.yml"
logging.info("Generating site catalog at: {}".format(site_catalog_file))
cmd_properties["pegasus.catalog.site.file"] = site_catalog_file

# local (submit host) site
local_site = (
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
    .add_env("SSH_PRIVATE_KEY", "/scitech/shared/home/bamboo/.ssh/workflow_id_rsa")
)

# compute site: local Slurm cluster accessed via HTCondor's BLAHP glite interface
compute_site = (
    Site(COMPUTE, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_pegasus_profile(style="glite")
    .add_pegasus_profile(clusters_num=1)
    .add_pegasus_profile(queue="debug", runtime=70)
    .add_profiles(Namespace.CONDOR, key="grid_resource", value="batch slurm")
)

if SHARED:
    # sharedfs mode: scratch dir is on a filesystem directly accessible from
    # both the submit host and compute nodes at the same path
    compute_site.add_directories(
        Directory(
            Directory.SHARED_SCRATCH,
            slurm_scratch_dir,
            shared_file_system=True,
        ).add_file_servers(
            FileServer(
                "scp://bamboo@bamboo.isi.edu/" + slurm_scratch_dir, Operation.ALL
            )
        )
    )

sc = SiteCatalog().add_sites(local_site, compute_site)

if STAGING == "workflow-webdav":
    # nonsharedfs mode: a WebDAV server acts as the staging site; Pegasus
    # transfers files to/from it over HTTPS and the compute job pulls them in
    staging_scratch_dir = str(WORK_DIR / "staging-site" / "scratch")
    sc.add_sites(
        Site("workflow-webdav", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
            Directory(Directory.SHARED_SCRATCH, staging_scratch_dir).add_file_servers(
                FileServer(
                    "webdavs://workflow.isi.edu/webdav" + slurm_scratch_dir,
                    Operation.ALL,
                )
            )
        )
    )

sc.write(str(site_catalog_file))

# --- Replicas -----------------------------------------------------------------
replica_catalog_file = TOP_DIR / TEST_NAME / "replicas.yml"
logging.info("Generating replica catalog at: {}".format(replica_catalog_file))
cmd_properties["pegasus.catalog.replica.file"] = replica_catalog_file

# place the input file on the shared filesystem when in sharedfs mode so the
# compute nodes can read it directly; otherwise keep it local
INPUT_DIR = Path(slurm_shared_dir) if SHARED else TOP_DIR / TEST_NAME
os.makedirs(INPUT_DIR, exist_ok=True)
with open("{}/f.a".format(INPUT_DIR), "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.a").add_metadata({"㐦": "㒦"})
ReplicaCatalog().add_replica(
    COMPUTE if SHARED else LOCAL, fa, INPUT_DIR / fa.lfn
).write(str(replica_catalog_file))

# --- Transformations ----------------------------------------------------------
transformation_catalog_file = TOP_DIR / TEST_NAME / "transformations.yml"
logging.info(
    "Generating transformation catalog at: {}".format(transformation_catalog_file)
)
cmd_properties["pegasus.catalog.transformation.file"] = transformation_catalog_file

docker_container = None
singularity_container = None
if CONTAINERS:
    docker_container = Container(
        "osgvo-el8-docker",
        Container.DOCKER,
        image_site="local",
        image="docker:///hub.opensciencegrid.org/opensciencegrid/osgvo-el8:latest",
        bypass_staging=False,
    )
    docker_container.add_env("APP_HOME", "/tmp/myscratch")

    singularity_container = Container(
        "ospool-rocky-9-singularity",
        Container.SINGULARITY,
        image_site="local",
        image="scp://bamboo@bamboo.isi.edu/scitech/shared/projects/Pegasus/test-containers/ospool-rocky-9.sif",
        bypass_staging=False,
    )
    singularity_container.add_env("APP_HOME", "/tmp/myscratch")

preprocess = Transformation("preprocess", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL, PEGASUS_LOCATION, is_stageable=True, arch=Arch.X86_64, os_type=OS.LINUX,
        container=docker_container,
    )
)

findrage = Transformation("findrange", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL, PEGASUS_LOCATION, is_stageable=True, arch=Arch.X86_64, os_type=OS.LINUX,
        container=singularity_container,
    )
)

analyze = Transformation("analyze", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        LOCAL, PEGASUS_LOCATION, is_stageable=True, arch=Arch.X86_64, os_type=OS.LINUX
    )
)

tc = TransformationCatalog()
if CONTAINERS:
    tc.add_containers(docker_container, singularity_container)
tc.add_transformations(preprocess, findrage, analyze).write(
    str(transformation_catalog_file)
)

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
        **cmd_properties,
    )
except PegasusClientError as e:
    logging.error(e.output)
