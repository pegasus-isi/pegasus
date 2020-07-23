import logging

from pathlib import Path
from datetime import datetime

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

PEGASUS_LOCATION = "/usr/bin/pegasus-keg"

# --- Work Dir Setup -----------------------------------------------------------
RUN_ID = "black-diamond-metadata-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Configuration ------------------------------------------------------------

print("Generating pegasus.conf at: {}".format(TOP_DIR / "pegasus.properties"))

conf = Properties()
conf["pegasus.catalog.site.file"] = "./conf/sites.yml"
conf["pegasus.catalog.site"] = "YAML"
conf.write()

# --- Replicas -----------------------------------------------------------------

print("Generating replica catalog at: {}".format(TOP_DIR / "replicas.yml"))

# create initial input file
with open("f.a", "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.a", size=1024).add_metadata({"raw_input": "true"})
ReplicaCatalog().add_replica("local", fa, TOP_DIR / fa.lfn).write()

# --- Transformations ----------------------------------------------------------

print(
    "Generating transformation catalog at: {}".format(TOP_DIR / "transformations.yml")
)

preprocess = Transformation(
                "preprocess", 
                namespace="pegasus", 
                version="4.0",
                site="condorpool",
                pfn=PEGASUS_LOCATION,
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            ).add_metadata(size=2048, transformation="preprocess")

findrange = Transformation(
                "findrange", 
                namespace="pegasus", 
                version="4.0",
                site="condorpool",
                pfn=PEGASUS_LOCATION,
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            ).add_metadata(size=2048, transformation="findrange")

analyze = Transformation(
                "analyze", 
                namespace="pegasus", 
                version="4.0",
                site="condorpool",
                pfn=PEGASUS_LOCATION,
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            ).add_metadata(size=2048, transformation="analyze")

TransformationCatalog().add_transformations(preprocess, findrange, analyze).write()

# --- Workflow -----------------------------------------------------------------
print("Generating workflow")

wf = Workflow("diamond")
wf.add_metadata(label="keg-diamond", group="test")

fb1 = File("f.ƀ1")
fb2 = File("f.β2")
fc1 = File("f.Ҫ1")
fc2 = File("f.Ͻ2")
fd = File("f.Ɗ").add_metadata(final_output=True)

preprocess_job = Job(preprocess)\
                    .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)\
                    .add_inputs(fa)\
                    .add_outputs(fb1, fb2, register_replica=True)\
                    .add_metadata(time=60)

findrange_1_job = Job(findrange)\
                    .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)\
                    .add_inputs(fb1)\
                    .add_outputs(fc1, register_replica=True)\
                    .add_metadata(time=60)

findrange_2_job = Job(findrange)\
                    .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)\
                    .add_inputs(fb2)\
                    .add_outputs(fc2, register_replica=True)\
                    .add_metadata(time=60)

analyze_job = Job(analyze)\
                .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)\
                .add_inputs(fc1, fc2)\
                .add_outputs(fd, register_replica=True)\
                .add_metadata(time=60)

wf.add_jobs(preprocess_job, findrange_1_job, findrange_2_job, analyze_job)

try:
    wf.plan(
        dir=WORK_DIR,
        verbose=3,
        relative_dir=RUN_ID,
        sites=["condorpool"],
        force=True,
        submit=True,
    )
except PegasusClientError as e:
    print(e.output)
