import logging
import sys
import os

from pathlib import Path
from datetime import datetime

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

PEGASUS_LOCATION = "/usr/bin/pegasus-keg"

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-5.0-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Output Dir Setup for condorpool Site -------------------------------------
condorpool_local_storage_dir = Path("/scitech/shared/scratch-90-days/bamboo/outputs") / RUN_ID
try:
    Path.mkdir(condorpool_local_storage_dir, parents=True)
except FileExistsError:
    pass

# --- Configuration ------------------------------------------------------------

print("Generating pegasus.conf at: {}".format(TOP_DIR / "pegasus.properties"))

conf = Properties()

conf["pegasus.catalog.site"] = "YAML"
conf["pegasus.catalog.site.file"] = "sites.yml"
conf["pegasus.catalog.transformation"] = "YAML"
conf["pegasus.catalog.transformation.file"] = "transformations.yml"
conf["pegasus.catalog.replica"] = "YAML"
conf["pegasus.catalog.replica.file"] = "replicas.yml"
conf["pegasus.data.configuration"] = "condorio"
#conf["pegasus.integrity.checking"] = "none"
conf.write()

# --- Sites --------------------------------------------------------------------
LOCAL = "local"
CONDOR_POOL = "condor-pool"

shared_scratch_dir = str(WORK_DIR / "shared-scratch")
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)

print("Generating site catalog at: {}".format(TOP_DIR / "sites.yml"))

SiteCatalog().add_sites(
    Site(
        LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7"
    ).add_directories(
        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        ),
        Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
            FileServer("file://" + local_storage_dir, Operation.ALL)
        ),
    ),
    Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_directories(
        Directory(Directory.LOCAL_STORAGE, str(condorpool_local_storage_dir))
        .add_file_servers(FileServer("file://" + str(condorpool_local_storage_dir), Operation.ALL))
    )
    .add_pegasus_profile(style="condor")
    .add_pegasus_profile(auxillary_local="true")
    .add_condor_profile(universe="vanilla"),
).write()

# --- Replicas -----------------------------------------------------------------

print("Generating replica catalog at: {}".format(TOP_DIR / "replicas.yml"))

# create initial input file
os.makedirs(os.path.dirname("./deep/"), exist_ok=True)
with open("deep/f.a", "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("deep/f.a").add_metadata({"㐦": "㒦"})
ReplicaCatalog().add_replica(LOCAL, fa, TOP_DIR / fa.lfn).write()

# --- Transformations ----------------------------------------------------------

print(
    "Generating transformation catalog at: {}".format(TOP_DIR / "transformations.yml")
)

preprocess = Transformation("preprocess", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

findrage = Transformation("findrange", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

analyze = Transformation("analyze", namespace="pegasus", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=True,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

TransformationCatalog().add_transformations(preprocess, findrage, analyze)\
        .write("transformations.yml")

# --- Workflow -----------------------------------------------------------------
print("Generating workflow")

fb1 = File("deep/f.b1")
fb2 = File("deep/f.b2")
fc1 = File("deep/f.c1")
fc2 = File("deep/f.c2")
fd = File("f.d")

try:
    Workflow("blackƊiamond").add_jobs(
        Job(preprocess)
        .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)
        .add_inputs(fa)
        .add_outputs(fb1, fb2, register_replica=True),
        Job(findrage)
        .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)
        .add_inputs(fb1)
        .add_outputs(fc1, register_replica=True),
        Job(findrage)
        .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)
        .add_inputs(fb2)
        .add_outputs(fc2, register_replica=True),
        Job(analyze)
        .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)
        .add_inputs(fc1, fc2)
        .add_outputs(fd, register_replica=True),
    ).plan(
        dir=str(WORK_DIR),
        verbose=3,
        relative_dir=RUN_ID,
        sites=[CONDOR_POOL],
        output_sites=[LOCAL, CONDOR_POOL],
        force=True,
        submit=True,
    )
except PegasusClientError as e:
    print(e.output)
