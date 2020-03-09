from pathlib import Path
from datetime import date

from Pegasus.api import *

PEGASUS_LOCATION = "file:///usr/bin/pegasus-keg"

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-5.0api-" + date.today().strftime("%s")
TOP_DIR = Path(Path.cwd())
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Configuration ------------------------------------------------------------
CONF_FILENAME = "pegasus.conf"

print("Generating pegasus.conf at: {}".format(TOP_DIR / CONF_FILENAME))

conf = Properties()
conf["pegasus.catalog.site"] = "YAML"
conf["pegasus.catalog.site.file"] = "SiteCatalog.yml"
conf["pegasus.catalog.transformation"] = "YAML"
conf["pegasus.catalog.transformation.file"] = "TransformationCatalog.yml"
conf["pegasus.catalog.replica"] = "YAML"
conf["pegasus.catalog.replica.file"] = "ReplicaCatalog.yml"
conf["pegasus.data.configuration"] = "condorio"

with open(CONF_FILENAME, "w") as f:
    conf.write(f)

# --- Sites --------------------------------------------------------------------
LOCAL = "locäl"
CONDOR_POOL = "⿔condor-pool⼤"

shared_scratch_dir = str(WORK_DIR / RUN_ID)
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)
SC_FILENAME = "SiteCatalog.yml"

print("Generating site catalog at: {}".format(TOP_DIR / SC_FILENAME))

SiteCatalog().add_sites(
    Site(
        LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7"
    ).add_directories(
        Directory(Directory.SHAREDSCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        ),
        Directory(Directory.LOCALSTORAGE, local_storage_dir).add_file_servers(
            FileServer("file://" + local_storage_dir, Operation.ALL)
        ),
    ),
    Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
    .add_pegasus_profile(style="condor")
    .add_condor_profile(universe="vanilla"),
).write(SC_FILENAME)

# --- Replicas -----------------------------------------------------------------
RC_FILENAME = "ReplicaCatalog.yml"

print("Generating replica catalog at: {}".format(TOP_DIR / RC_FILENAME))

# create initial input file
with open("f.å", "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.å").add_metadata({"㐦": "㒦"})
ReplicaCatalog().add_replica(fa, "file://" + str(TOP_DIR / fa.lfn), LOCAL).write(
    RC_FILENAME
)

# --- Transformations ----------------------------------------------------------
TC_FILENAME = "TransformationCatalog.yml"

print("Generating transformation catalog at: {}".format(TOP_DIR / TC_FILENAME))

preprocess = Transformation("pЯёprocess", namespace="pέgasuζ", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=False,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

findrage = Transformation("findrange", namespace="pέgasuζ", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=False,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

analyze = Transformation("analyze", namespace="pέgasuζ", version="4.0").add_sites(
    TransformationSite(
        CONDOR_POOL,
        PEGASUS_LOCATION,
        is_stageable=False,
        arch=Arch.X86_64,
        os_type=OS.LINUX,
    )
)

TransformationCatalog().add_transformations(preprocess, findrage, analyze).write(
    TC_FILENAME
)

# --- Workflow -----------------------------------------------------------------
print("Generating workflow")

fb1 = File("f.ƀ1")
fb2 = File("f.β2")
fc1 = File("f.Ҫ1")
fc2 = File("f.Ͻ2")
fd = File("f.Ɗ")

Workflow("blÅckƊiamond㒀㑖", infer_dependencies=True).add_jobs(
    Job(preprocess)
    .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)
    .add_inputs(fa)
    .add_outputs(fb1, fb2),
    Job(findrage)
    .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)
    .add_inputs(fb1)
    .add_outputs(fc1),
    Job(findrage)
    .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)
    .add_inputs(fb2)
    .add_outputs(fc2),
    Job(analyze)
    .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)
    .add_inputs(fc1, fc2)
    .add_outputs(fd),
).plan(
    dir=str(WORK_DIR),
    relative_dir=RUN_ID,
    conf=CONF_FILENAME,
    sites=CONDOR_POOL,
    output_site=LOCAL,
    force=True,
    submit=True,
)
