#!/usr/bin/env python3
import logging

from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Dir Setup -----------------------------------------------------------
TOP_DIR = Path.cwd().resolve()
Path.mkdir(TOP_DIR / "outputs", exist_ok=True)
Path.mkdir(TOP_DIR / "staging-site", exist_ok=True)

# --- Configuration ------------------------------------------------------------
props = Properties()
props["pegasus.dir.useTimestamp"] = "true"
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "nonsharedfs"

#PM-1192 always print out the job env
props["pegasus.gridstart.arguments"] = "-f"

props.write()

# --- Sites --------------------------------------------------------------------
sc = SiteCatalog()

# local
local_site = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
local_site.add_directories(
    Directory(directory_type=Directory.SHARED_STORAGE, path=TOP_DIR / "outputs")
        .add_file_servers(FileServer(url="file://" + str(TOP_DIR / "outputs"), operation_type=Operation.ALL)),
    Directory(directory_type=Directory.SHARED_SCRATCH, path=TOP_DIR / "work")
        .add_file_servers(FileServer(url="file://" + str(TOP_DIR / "work"), operation_type=Operation.ALL))
)
local_site.add_pegasus_profile(pegasus_lite_env_source=TOP_DIR / "job-setup.sh")

# condorpool
condorpool_site = Site(name="condorpool", os_type=OS.LINUX)
condorpool_site.add_condor_profile(universe="vanilla")
condorpool_site.add_pegasus_profile(style="condor")

# cartman-data
cartman_data_site = Site(name="cartman-data", arch=Arch.X86_64, os_type=OS.LINUX)
cartman_data_site.add_directories(
    Directory(directory_type=Directory.SHARED_SCRATCH, path=TOP_DIR / "staging-site/scratch")
        .add_file_servers(FileServer(url="webdavs://workflow.isi.edu/webdav/scratch-90-days/{}".format(TOP_DIR / "staging-site/scratch"), operation_type=Operation.ALL))
)

sc.add_sites(local_site, condorpool_site, cartman_data_site)
sc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f:
    f.write("This is sample input to KEG\n")

rc = ReplicaCatalog()
rc.add_replica("local", "f.a", TOP_DIR / "f.a")
rc.write()

# --- Transformations ----------------------------------------------------------
preprocess = Transformation(
                name="preprocess",
                namespace="diamond",
                version="4.0",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            )

findrange = Transformation(
                name="findrange",
                namespace="diamond",
                version="4.0",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            )

analyze = Transformation(
                name="analyze",
                namespace="diamond",
                version="4.0",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            )

tc = TransformationCatalog()
tc.add_transformations(preprocess, findrange, analyze)
tc.write()

# --- Workflow -----------------------------------------------------------------

fa = File("f.a")
fb1 = File("f.b1")
fb2 = File("f.b2")
fc1 = File("f.c1")
fc2 = File("f.c2")
fd = File("f.d")

wf = Workflow("diamond")

preprocess_job = Job(preprocess)\
                    .add_args("-a", "preprocess", "-T", "10", "-i", fa, "-o", fb1, fb2)\
                    .add_inputs(fa)\
                    .add_outputs(fb1, fb2)

findrange_1_job = Job(findrange)\
                    .add_args("-a", "findrange", "-T", "5", "-i", fb1, "-o", fc1)\
                    .add_inputs(fb1)\
                    .add_outputs(fc1)

findrange_2_job = Job(findrange)\
                    .add_args("-a", "findrange", "-T", "5", "-i", fb2, "-o", fc2)\
                    .add_inputs(fb2)\
                    .add_outputs(fc2)

analyze_job = Job(analyze)\
                .add_args("-a", "analyze", "-T", "10", "-i", fc1, fc2, "-o", fd)\
                .add_inputs(fc1, fc2)\
                .add_outputs(fd)

wf.add_jobs(
    preprocess_job,
    findrange_1_job,
    findrange_2_job,
    analyze_job
)

wf.plan(
    dir="work",
    sites=["condorpool"],
    output_sites=["local"],
    staging_sites={"condorpool":"cartman-data"},
    cleanup="leaf",
    random_dir=True,
    force=True
)

with open("submit_dir", "w") as f:
    f.write(str(wf.braindump.submit_dir))
