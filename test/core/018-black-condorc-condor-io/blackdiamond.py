#!/usr/bin/env python3
import logging

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
isi_condor_c_site = Site(name="isi-condorc", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
isi_condor_c_site.add_grids(
    Grid(grid_type=Grid.CONDOR, contact="ccg-testing1.isi.edu", scheduler_type=Scheduler.CONDOR, job_type=SupportedJobs.AUXILLARY),
    Grid(grid_type=Grid.CONDOR, contact="ccg-testing1.isi.edu", scheduler_type=Scheduler.CONDOR, job_type=SupportedJobs.COMPUTE),
)
isi_condor_c_site.add_condor_profile(universe="vanilla")
isi_condor_c_site.add_pegasus_profile(clusters_num=1, style="condorc")
sc.add_sites(isi_condor_c_site)

# condorpool
condorpool_site = Site(name="condorpool", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
condorpool_site.add_condor_profile(universe="vanilla")
condorpool_site.add_pegasus_profile(style="condor")
sc.add_sites(condorpool_site)

# local
local_site = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
local_site.add_directories(
    Directory(directory_type=Directory.SHARED_STORAGE, path=TOP_DIR / "outputs")
        .add_file_servers(FileServer(url="file://" + str(TOP_DIR / "outputs"), operation_type=Operation.ALL)),
    Directory(directory_type=Directory.SHARED_SCRATCH, path=TOP_DIR / "work")
        .add_file_servers(FileServer(url="file://" + str(TOP_DIR / "work"), operation_type=Operation.ALL))
)
sc.add_sites(local_site)

sc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f:
    f.write("This is sample input to KEG\n")

rc = ReplicaCatalog()
rc.add_replica("local", "f.a", TOP_DIR / "f.a")
rc.write()

# --- Transformations ----------------------------------------------------------
# Using the binary is 32 bit version of pegasus-keg
# that is stored at file:///nfs/ccg3/software/bamboo/x86/pegasus-keg
keg_path = "/nfs/ccg3/software/bamboo/x86/pegasus-keg"
preprocess = Transformation(
                name="preprocess", 
                namespace="diamond", 
                version="4.0",
                site="local",
                pfn=keg_path,
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
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
                os_release="rhel",
                os_version="7"
            )

analyze = Transformation(
                name="analyze", 
                namespace="diamond", 
                version="4.0",
                site="local",
                pfn=keg_path,
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
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

try:
    wf.plan(
        dir="work/submit",
        verbose=3,
        sites=["isi-condorc"],
        output_sites=["local"],
        cluster=["horizontal"],
        cleanup="leaf",
        force=True,
        submit=True,
    )
except PegasusClientError as e:
    print(e)