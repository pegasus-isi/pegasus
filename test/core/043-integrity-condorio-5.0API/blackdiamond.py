import hashlib
import logging
import sys

from pathlib import Path
from datetime import datetime

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

PEGASUS_LOCATION = "/usr/bin/pegasus-keg"

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-integrity-checking-condorio-5.0api-" + datetime.now().strftime(
    "%s"
)
TOP_DIR = Path(Path.cwd())
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Configuration ------------------------------------------------------------

print("Generating pegasus.conf at: {}".format(TOP_DIR / "pegasus.properties"))

conf = Properties()

conf["pegasus.data.configuration"] = "condorio"
conf["pegasus.integrity.checking"] = "full"

conf.write()

# --- Sites --------------------------------------------------------------------
LOCAL = "local"
CONDOR_POOL = "condorpool"

shared_scratch_dir = str(WORK_DIR / "LOCAL/shared-scratch")
shared_storage_dir = str(WORK_DIR / "LOCAL/shared-storage")

print("Generating site catalog")

sc = SiteCatalog().add_sites(
    Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_directories(
        Directory(Directory.SHAREDSCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        ),
        Directory(Directory.SHAREDSTORAGE, shared_storage_dir).add_file_servers(
            FileServer("file://" + shared_storage_dir, Operation.ALL)
        ),
    )
    .add_pegasus_profile(clusters_num=1),
    Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_pegasus_profile(style="condor")
    .add_condor_profile(universe="vanilla"),
)

# --- Replicas -----------------------------------------------------------------

print("Generating replica catalog")

# create initial input file and compute its hash for integrity checking
with open("f.a", "wb+") as f:
    f.write(b"This is sample input to KEG\n")
    f.seek(0)
    readable_hash = hashlib.sha256(f.read()).hexdigest()

fa = File("f.a")
rc = ReplicaCatalog().add_replica(
    LOCAL,
    fa,
    "file://" + str(TOP_DIR / fa.lfn),
    checksum_type="sha256",
    checksum_value=readable_hash,
)

# --- Transformations ----------------------------------------------------------

print("Generating transformation catalog")

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

tc = TransformationCatalog().add_transformations(preprocess, findrage, analyze)

# --- Workflow -----------------------------------------------------------------
print("Generating workflow")

fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

try:
    Workflow("blackdiamond", infer_dependencies=True).add_jobs(
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
    ).add_site_catalog(sc).add_replica_catalog(rc).add_transformation_catalog(tc).plan(
        dir=str(WORK_DIR),
        verbose=3,
        relative_dir=RUN_ID,
        sites=[CONDOR_POOL],
        output_site=LOCAL,
        force=True,
        submit=True,
    )
except Exception as e:
    print(e.args)
