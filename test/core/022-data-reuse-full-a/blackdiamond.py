#!/usr/bin/env python3
import logging
import shutil

from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Dir Setup -----------------------------------------------------------
TOP_DIR = Path.cwd().resolve()
Path.mkdir(TOP_DIR / "outputs", exist_ok=True)

# --- Configuration ------------------------------------------------------------
props = Properties()
props["pegasus.dir.useTimestamp"] = "true"
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "condorio"
props["pegasus.transfer.worker.package"] = "true"
props.write()

# --- Sites --------------------------------------------------------------------
sc = SiteCatalog()

# isi-condorc site
isi_condor_c_site = Site(name="isi-condorc", arch=Arch.X86, os_type=OS.LINUX)
isi_condor_c_site.add_grids(
    Grid(
        grid_type=Grid.CONDOR,
        contact="ccg-testing1.isi.edu",
        scheduler_type=Scheduler.CONDOR,
        job_type=SupportedJobs.AUXILLARY,
    ),
    Grid(
        grid_type=Grid.CONDOR,
        contact="ccg-testing1.isi.edu",
        scheduler_type=Scheduler.CONDOR,
        job_type=SupportedJobs.COMPUTE,
    ),
)
isi_condor_c_site.add_condor_profile(universe="vanilla")
isi_condor_c_site.add_pegasus_profile(clusters_num=1, style="condorc")

# condorpool site
condorpool_site = Site(name="condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
condorpool_site.add_condor_profile(universe="vanilla")
condorpool_site.add_pegasus_profile(style="condor")

# local site
local_site = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX)
local_site.add_directories(
    Directory(
        directory_type=Directory.SHARED_STORAGE, path=TOP_DIR / "outputs"
    ).add_file_servers(
        FileServer(
            url="file://" + str(TOP_DIR / "outputs"), operation_type=Operation.ALL
        )
    ),
    Directory(
        directory_type=Directory.SHARED_SCRATCH, path=TOP_DIR / "work"
    ).add_file_servers(
        FileServer(url="file://" + str(TOP_DIR / "work"), operation_type=Operation.ALL)
    ),
)


sc.add_sites(isi_condor_c_site, condorpool_site, local_site)
sc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f_a, open("f.d", "w") as f_d:
    # generate the input file
    f_a.write("This is sample input to KEG\n")

    # generate the final output file
    f_d.write("This is preexisitng output file for the workflow")

rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="f.a", pfn=TOP_DIR / "f.a")
rc.add_replica(site="local", lfn="f.d", pfn=TOP_DIR / "f.d")
rc.write()

# --- Transformations ----------------------------------------------------------
# Using the binary is 32 bit version of pegasus-keg
# that is stored at "/scitech/shared/scratch-90-days/bamboo/inputs/pegasus-keg-x86"
keg_path = "/scitech/shared/scratch-90-days/bamboo/inputs/pegasus-keg-x86"
preprocess = Transformation(
    name="preprocess",
    namespace="diamond",
    version="4.0",
    site="condorpool",
    pfn=shutil.which("pegasus-keg"),
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
)

findrange = Transformation(
    name="findrange",
    namespace="diamond",
    version="4.0",
    site="local",
    pfn=keg_path,
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
)

analyze = Transformation(
    name="analyze",
    namespace="diamond",
    version="4.0",
    site="condorpool",
    pfn=shutil.which("pegasus-keg"),
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
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

preprocess_job = (
    Job(preprocess)
    .add_args("-a", "preprocess", "-T", "10", "-i", fa, "-o", fb1, fb2)
    .add_inputs(fa)
    .add_outputs(fb1, fb2, stage_out=False)
    .add_pegasus_profile(grid_start="none")
    .add_condor_profile(universe="vanilla")
)

findrange_1_job = (
    Job(findrange)
    .add_args("-a", "findrange", "-T", "5", "-i", fb1, "-o", fc1)
    .add_inputs(fb1)
    .add_outputs(fc1, stage_out=False)
    .add_pegasus_profile(grid_start="pegasuslite")
    .add_condor_profile(universe="vanilla")
)

findrange_2_job = (
    Job(findrange)
    .add_args("-a", "findrange", "-T", "5", "-i", fb2, "-o", fc2)
    .add_inputs(fb2)
    .add_outputs(fc2, stage_out=False)
    .add_pegasus_profile(grid_start="pegasuslite")
    .add_condor_profile(universe="vanilla")
)

analyze_job = (
    Job(analyze)
    .add_args("-a", "analyze", "-T", "10", "-i", fc1, fc2, "-o", fd)
    .add_inputs(fc1, fc2)
    .add_outputs(fd, stage_out=True, register_replica=True)
    .add_pegasus_profile(grid_start="none")
    .add_condor_profile(universe="vanilla")
)

wf.add_jobs(preprocess_job, findrange_1_job, findrange_2_job, analyze_job)

wf.plan(
    dir="work",
    sites=["condorpool"],
    output_sites=["local"],
    cluster=["horizontal"],
)

# save submit dir so we can analyze it for certain jobs
with open("submit_dir", "w") as f:
    f.write(str(wf.braindump.submit_dir))
