#!/usr/bin/env python3
import logging
import shutil

from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Configuration ------------------------------------------------------------
props = Properties()
props["pegasus.dir.useTimestamp"] = "true"
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "sharedfs"
props["pegasus.transfer.worker.package"] = "true"

# there are compute jobs that are not launched by kickstart, hence *disable* integrity checking
props["pegasus.integrity.checking"] = "none"

props.write()

# --- Sites --------------------------------------------------------------------
sc = SiteCatalog()

# CCG site
ccg_site = Site(
    name="CCG", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7"
)
ccg_site.add_directories(
    Directory(
        directory_type=Directory.SHARED_STORAGE,
        path="/scitech/shared/scratch-90-days/bamboo/outputs",
    ).add_file_servers(
        FileServer(
            url="file:///scitech/shared/scratch-90-days/bamboo/outputs",
            operation_type=Operation.ALL,
        )
    ),
    Directory(
        directory_type=Directory.SHARED_SCRATCH, path="/scitech/shared/scratch-90-days"
    ).add_file_servers(
        FileServer(
            url="file:///scitech/shared/scratch-90-days", operation_type=Operation.ALL
        )
    ),
)
ccg_site.add_profiles(
    Namespace.CONDOR, getenv="True", requirements='"(TARGET.FileSystemDomain =!= "")"'
)
ccg_site.add_profiles(Namespace.PEGASUS, style="condor")

# local site
local_site = Site(
    name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7"
)
local_site.add_directories(
    Directory(
        directory_type=Directory.SHARED_STORAGE,
        path="/scitech/shared/scratch-90-days/bamboo/outputs",
    ).add_file_servers(
        FileServer(
            url="file:///scitech/shared/scratch-90-days/bamboo/outputs",
            operation_type=Operation.ALL,
        )
    ),
    Directory(
        directory_type=Directory.SHARED_SCRATCH, path="/scitech/shared/scratch-90-days"
    ).add_file_servers(
        FileServer(
            url="file:///scitech/shared/scratch-90-days", operation_type=Operation.ALL
        )
    ),
)
local_site.add_grids(
    Grid(
        grid_type=Grid.CONDOR,
        contact="ccg-testing1.isi.edu",
        scheduler_type=Scheduler.CONDOR,
        job_type=SupportedJobs.COMPUTE,
    ),
    Grid(
        grid_type=Grid.CONDOR,
        contact="ccg-testing1.isi.edu",
        scheduler_type=Scheduler.CONDOR,
        job_type=SupportedJobs.AUXILLARY,
    ),
)
local_site.add_profiles(
    Namespace.CONDOR, getenv="True", requirements='"(TARGET.FileSystemDomain =!= "")"'
)
local_site.add_profiles(Namespace.PEGASUS, style="condor")

sc.add_sites(ccg_site, local_site)
sc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f:
    f.write("This is sample input to KEG\n")

rc = ReplicaCatalog()
rc.add_replica("local", "f.a", Path(".").resolve() / "f.a")
rc.write()

# --- Transformations ----------------------------------------------------------
preprocess = Transformation(
    name="preprocess",
    namespace="diamond",
    version="4.0",
    site="local",
    pfn=shutil.which("pegasus-keg"),
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    os_release="rhel",
    os_version="7",
)

findrange = Transformation(
    name="findrange",
    namespace="diamond",
    version="4.0",
    site="local",
    pfn=shutil.which("pegasus-keg"),
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    os_release="rhel",
    os_version="7",
).add_pegasus_profile(style="condorc")

analyze = Transformation(
    name="analyze",
    namespace="diamond",
    version="4.0",
    site="local",
    pfn=shutil.which("pegasus-keg"),
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    os_release="rhel",
    os_version="7",
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
    .add_outputs(fb1, fb2)
    .add_condor_profile(universe="vanilla")
    .add_pegasus_profile(grid_start="none")
)

findrange_1_job = (
    Job(findrange)
    .add_args("-a", "findrange", "-T", "5", "-i", fb1, "-o", fc1)
    .add_inputs(fb1)
    .add_outputs(fc1)
    .add_condor_profile(universe="vanilla")
    .add_pegasus_profile(grid_start="pegasuslite")
)

findrange_2_job = (
    Job(findrange)
    .add_args("-a", "findrange", "-T", "5", "-i", fb2, "-o", fc2)
    .add_inputs(fb2)
    .add_outputs(fc2)
    .add_condor_profile(universe="vanilla")
    .add_pegasus_profile(grid_start="pegasuslite")
)

analyze_job = (
    Job(analyze)
    .add_args("-a", "analyze", "-T", "10", "-i", fc1, fc2, "-o", fd)
    .add_inputs(fc1, fc2)
    .add_outputs(fd)
    .add_condor_profile(universe="vanilla")
    .add_pegasus_profile(grid_start="none")
)

wf.add_jobs(preprocess_job, findrange_1_job, findrange_2_job, analyze_job)

try:
    wf.plan(
        dir="work",
        verbose=3,
        sites=["local"],
        output_sites=["local"],
        cluster=["horizontal"],
        cleanup="leaf",
        force=True,
        submit=True,
    )
except PegasusClientError as e:
    print(e)
