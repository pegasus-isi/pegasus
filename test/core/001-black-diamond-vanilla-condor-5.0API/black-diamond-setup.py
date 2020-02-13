
import argparse
from pathlib import Path

from Pegasus.api import *

parser = argparse.ArgumentParser("Generates SC, TC, RC, and WF")
parser.add_argument("top_dir")
parser.add_argument("work_dir")
parser.add_argument("run_id")

args = parser.parse_args()

PEGASUS_LOCATION = "file:///usr/bin/pegasus-keg"

RUN_ID = args.run_id
TOP_DIR = Path(args.top_dir)
WORK_DIR = Path(args.work_dir)

# --- Sites --------------------------------------------------------------------
LOCAL = "locäl"
CONDOR_POOL = "⿔condor-pool⼤"

shared_scratch_dir = str(WORK_DIR / RUN_ID)
local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)
SC_FILENAME = "SiteCatalog.yml"

print("Generating site catalog at: {}".format(TOP_DIR / SC_FILENAME))

SiteCatalog()\
    .add_site(
        Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
            .add_directory(
                Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                    .add_file_server(FileServer("file://" + shared_scratch_dir, Operation.ALL))
            ).add_directory(
                Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                    .add_file_server(FileServer("file://" + local_storage_dir, Operation.ALL))
            )
    ).add_site(
        Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
            .add_pegasus(style="condor")
            .add_condor(universe="vanilla")
    ).write(SC_FILENAME)

# --- Replicas -----------------------------------------------------------------
RC_FILENAME = "ReplicaCatalog.yml"

print("Generating replica catalog at: {}".format(TOP_DIR / RC_FILENAME))

# create initial input file 
with open("f.å", "w") as f:
    f.write("This is sample input to KEG\n")

fa = File("f.å").add_metadata({"㐦": "㒦"})
ReplicaCatalog()\
    .add_replica(fa, "file://" + str(TOP_DIR / fa.lfn), LOCAL)\
    .write(RC_FILENAME)

# --- Transformations ----------------------------------------------------------
TC_FILENAME = "TransformationCatalog.yml"

print("Generating transformation catalog at: {}".format(TOP_DIR / TC_FILENAME))

preprocess = Transformation("pЯёprocess", namespace="pέgasuζ", version="4.0")\
                .add_site(
                    TransformationSite(
                        CONDOR_POOL, 
                        PEGASUS_LOCATION, 
                        is_stageable=False, 
                        arch=Arch.X86_64, 
                        os_type=OS.LINUX)
                )

findrage = Transformation("findrange", namespace="pέgasuζ", version="4.0")\
                .add_site(
                    TransformationSite(
                        CONDOR_POOL, 
                        PEGASUS_LOCATION, 
                        is_stageable=False, 
                        arch=Arch.X86_64, 
                        os_type=OS.LINUX)
                )

analyze = Transformation("analyze", namespace="pέgasuζ", version="4.0")\
                .add_site(
                    TransformationSite(
                        CONDOR_POOL, 
                        PEGASUS_LOCATION, 
                        is_stageable=False, 
                        arch=Arch.X86_64, 
                        os_type=OS.LINUX)
                )

TransformationCatalog()\
    .add_transformations(preprocess, findrage, analyze)\
    .write(TC_FILENAME)

# --- Workflow -----------------------------------------------------------------
WF_FILENAME = "Workflow.yml"

print("Generating workflow at: {}".format(TOP_DIR / WF_FILENAME))

fb1 = File("f.ƀ1")
fb2 = File("f.β2")
fc1 = File("f.Ҫ1")
fc2 = File("f.Ͻ2")
fd = File("f.Ɗ")

Workflow("blÅckƊiamond㒀㑖", infer_dependencies=True)\
    .add_jobs(
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
            .add_outputs(fd)
    ).write(WF_FILENAME)
