#!/usr/bin/env python3
import logging
import sys
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Dir Setup ----------------------------------------------------------------
TOP_DIR = Path.cwd().resolve()
Path.mkdir(TOP_DIR / "outputs", exist_ok=True)

# --- Configuration ------------------------------------------------------------
props = Properties()
props["pegasus.dir.useTimestamp"] = "true"
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "nonsharedfs"
props.write()

# --- Sites --------------------------------------------------------------------
sc = SiteCatalog()

# local site
local_site = Site(
    name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7"
)
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

sc.add_sites(local_site)
sc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f_a, open("f.b0", "w") as f_b0, open("f.c12", "w") as f_c12:
    f_a.write("")
    f_b0.write("Hello!")
    f_c12.write("Hello world!")

rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="f.a", pfn=TOP_DIR / "f.a")
rc.add_regex_replica(site="local", pattern=r"f\\.([x])", pfn=TOP_DIR / "f.a")
rc.add_regex_replica(site="local", pattern="fa([x])", pfn=TOP_DIR / "f.a")
rc.add_regex_replica(site="local", pattern="dir/file.x", pfn=TOP_DIR / "f.a")
rc.add_regex_replica(site="local", pattern="dir/file.y", pfn=TOP_DIR / "f.a")

rc.add_replica(site="local", lfn="f.b0", pfn=TOP_DIR / "f.b0")
rc.add_replica(site="local", lfn="f.c12", pfn=TOP_DIR / "f.c12")
rc.add_regex_replica(site="local", pattern=r"f\.c2([0-9])", pfn=TOP_DIR / "f.c12")

rc.write()

# --- Transformations ----------------------------------------------------------
hello_tr = Transformation(
    name="hello",
    namespace="hello_world",
    version="1.0",
    site="local",
    pfn=TOP_DIR / "hello.sh",
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    os_release="rhel",
    os_version="7",
)

world_tr = Transformation(
    name="world",
    namespace="hello_world",
    version="1.0",
    site="local",
    pfn=TOP_DIR / "world.sh",
    is_stageable=True,
    arch=Arch.X86_64,
    os_type=OS.LINUX,
    os_release="rhel",
    os_version="7",
)

tc = TransformationCatalog()
tc.add_transformations(hello_tr, world_tr)
tc.write()

# --- Workflow -----------------------------------------------------------------
wf = Workflow("022-data-reuse-regexrc")

for i in range(3):
    # add hello jobs
    a1 = File("f.a")
    a2 = File("f.x")
    a2 = File("fax")
    a3 = File("dir/file.x")
    a4 = File("dir/file.y")
    b = File(f"f.b{i}")

    hello_job = (
        Job(hello_tr, _id=f"ID_hello_{i}")
        .add_args(b)
        .add_inputs(a1, a2, a3, a4)
        .add_outputs(b, stage_out=False)
    )

    wf.add_jobs(hello_job)

    for j in range(3):
        # add world jobs (which depends on hello job)
        c = File(f"f.c{i}{j}")
        world_job = (
            Job(world_tr, _id=f"ID_world_{i}_{j}")
            .add_args(c)
            .add_inputs(b)
            .add_outputs(c)
        )

        wf.add_jobs(world_job)

wf.plan(sites=["local"], dir="work/submit", output_sites=["local"], cleanup="leaf")

# --- Test ---------------------------------------------------------------------
# Test that the appropriate jobs have been removed during the planning process.
submit_dir = wf.braindump.submit_dir

# submit files that should not be present
should_have_been_removed = {
    "hello_ID_hello_0.sub",
    "hello_ID_hello_2.sub",
    "world_ID_world_1_2.sub",
    "world_ID_world_2_0.sub" "world_ID_world_2_1.sub" "world_ID_world_2_2.sub",
}

found_submit_files = set()

for f in (submit_dir / "00/00").iterdir():
    if f.name.endswith(".sub"):
        found_submit_files.add(f.name)

if found_submit_files.isdisjoint(should_have_been_removed):
    print(
        "Test SUCCESS. Submit files for jobs for which data already exists were removed"
    )
else:
    print(
        "Test FAILURE. Submit files for jobs for which data already exists were NOT removed"
    )
    sys.exit(1)

# --- Run Workflow ------------------------------------------------------------
wf.run()
