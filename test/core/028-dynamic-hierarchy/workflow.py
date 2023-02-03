#!/usr/bin/env python3
import logging

from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

TOP_DIR = Path(__file__).resolve().parent

# --- Properties ---------------------------------------------------------------
# properties that will be used by both the outer workflow  and inner diamond workflow
props = Properties()
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "condorio"
props["pegasus.mode"] = "development"
props.write()

# properties that will be used by inner diamond workflow
props["pegasus.catalog.transformation.file"] = "inner_diamond_workflow_tc.yml"
props["pegasus.catalog.replica.file"] = "inner_diamond_workflow_rc.yml"
props.write("inner_diamond_workflow.pegasus.properties")

# --- Sites --------------------------------------------------------------------
sc = SiteCatalog()

# local site
local_site = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
local_site.add_directories(
    Directory(Directory.SHARED_SCRATCH, TOP_DIR / "work/local-site/scratch")
        .add_file_servers(FileServer("file://{}".format(TOP_DIR / "work/local-site/scratch"), Operation.ALL)),
    Directory(Directory.LOCAL_STORAGE, TOP_DIR / "outputs/local-site")
        .add_file_servers(FileServer("file://{}".format(TOP_DIR / "outputs/local-site"), Operation.ALL))
)

# CCG site
ccg_site = Site(name="CCG", arch=Arch.X86_64, os_type=OS.LINUX)
ccg_site.add_grids(
    Grid(grid_type=Grid.GT5, contact="obelix.isi.edu/jobmanager-fork", scheduler_type=Scheduler.FORK, job_type=SupportedJobs.AUXILLARY),
    Grid(grid_type=Grid.GT5, contact="obelix.isi.edu/jobmanager-condor", scheduler_type=Scheduler.CONDOR, job_type=SupportedJobs.COMPUTE),
)
ccg_site.add_directories(
    Directory(Directory.SHARED_SCRATCH, "/scitech/shared/scratch-90-days/CCG/scratch")
        .add_file_servers(FileServer("gsiftp://obelix.isi.edu/scitech/shared/scratch-90-days/CCG/scratch", Operation.ALL)),
    Directory(Directory.LOCAL_STORAGE, "/scitech/shared/scratch-90-days/CCG/outputs")
        .add_file_servers(FileServer("gsiftp://obelix.isi.edu/scitech/shared/scratch-90-days/CCG/outputs", Operation.ALL))
)
ccg_site.add_env(PEGASUS_HOME="/usr/bin")

sc.add_sites(local_site, ccg_site)
sc.write()

# --- Transformations ----------------------------------------------------------
# create transformation catalog for the outer level workflow
sleep = Transformation(
                name="sleep",
                site="local",
                pfn="/bin/sleep",
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

generate_diamond_wf = Transformation(
                        name="generate_inner_diamond_workflow.py",
                        site="local",
                        pfn=TOP_DIR / "generate_inner_diamond_workflow.py",
                        is_stageable=True,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        os_release="rhel",
                        os_version="7"
                    )

tc = TransformationCatalog()
tc.add_transformations(sleep, generate_diamond_wf)
tc.write()

# create transformation catalog for the inner diamond workflow
preprocess = Transformation(
                name="preprocess",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

findrange = Transformation(
                name="findrange",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

analyze = Transformation(
                name="analyze",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

inner_diamond_workflow_tc = TransformationCatalog()
inner_diamond_workflow_tc.add_transformations(preprocess, findrange, analyze)
inner_diamond_workflow_tc.write("inner_diamond_workflow_tc.yml")

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f:
    f.write("Sample input file for the first inner dax job.")

# replica catalog for the inner diamond workflow
inner_diamond_workflow_rc = ReplicaCatalog()
inner_diamond_workflow_rc.add_replica(site="local", lfn="f.a", pfn=TOP_DIR / "f.a")
inner_diamond_workflow_rc.write("inner_diamond_workflow_rc.yml")

# replica catalog for the outer workflow
rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="inner_diamond_workflow.pegasus.properties", pfn=TOP_DIR / "inner_diamond_workflow.pegasus.properties")
rc.add_replica(site="local", lfn="inner_diamond_workflow_rc.yml", pfn =TOP_DIR / "inner_diamond_workflow_rc.yml")
rc.add_replica(site="local", lfn="inner_diamond_workflow_tc.yml", pfn=TOP_DIR / "inner_diamond_workflow_tc.yml")
rc.add_replica(site="local", lfn="sites.yml", pfn=TOP_DIR / "sites.yml")
rc.add_replica(site="local", lfn="inner_sleep_workflow.yml", pfn=TOP_DIR / "inner_sleep_workflow.yml")
rc.write()

# --- Workflow -----------------------------------------------------------------
wf = Workflow("hierarchical-workflow")

# job to generate the diamond workflow
diamond_wf_file = File("inner_diamond_workflow.yml")
generate_diamond_wf_job = Job(generate_diamond_wf, _id="diamond_workflow_gen")\
                            .add_outputs(diamond_wf_file)

# job to plan and run the diamond workflow
diamond_wf_job = SubWorkflow(file=diamond_wf_file, is_planned=False, _id="diamond_subworkflow")\
                    .add_args(
                        "--conf",
                        "inner_diamond_workflow.pegasus.properties",
                        "--output-sites",
                        "local",
                        "-vvv",
                        "--basename",
                        "inner"
                    )\
                    .add_inputs(
                        File("inner_diamond_workflow.pegasus.properties"),
                        File("inner_diamond_workflow_rc.yml"),
                        File("inner_diamond_workflow_tc.yml"),
                        File("sites.yml"),
                    )

sleep_wf_file = File("inner_sleep_workflow.yml")
sleep_wf_job = SubWorkflow(file=sleep_wf_file, is_planned=False, _id="sleep_subworkflow")\
                .add_args(
                    "--output-sites",
                    "local",
                    "-vvv"
                )

sleep_job = Job(sleep, _id="sleep_job").add_args(5)

wf.add_jobs(generate_diamond_wf_job, diamond_wf_job, sleep_wf_job, sleep_job)
wf.add_dependency(generate_diamond_wf_job, children=[diamond_wf_job])
wf.add_dependency(diamond_wf_job, children=[sleep_wf_job])
wf.add_dependency(sleep_wf_job, children=[sleep_job])

wf.plan(
    sites=["local", "CCG"],
    output_sites=["local"],
    dir="work",
    verbose=3,
    submit=True
)

