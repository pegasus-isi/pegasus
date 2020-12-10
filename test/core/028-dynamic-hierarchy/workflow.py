#!/usr/bin/env python3
import logging

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

TOP_DIR = Path(__file__).resolve().parent

# --- Properties ---------------------------------------------------------------
# properties that will be used by both the outer workflow and inner diamond workflow
props = Properties()
props["pegasus.dir.storage.deep"] = "false"
props["pegasus.condor.logs.symlink"] = "false"
props["pegasus.data.configuration"] = "condorio"
props.write()

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
    Grid(grid_type=Grid.GT5, contact="obelix.isi.edu/jobmanager-fork", scheduler=Scheduler.FORK, job_type=SupportedJobs.AUXILLARY),
    Grid(grid_type=Grid.GT5, contact="obelix.isi.edu/jobmanager-condor", scheduler=Scheduler.CONDOR, job_type=SupportedJobs.COMPUTE),
)
ccg_site.add_directories(
    Directory(Directory.SHARED_SCRATCH, "/lizard/scratch-90-days/CCG/scratch")
        .add_file_servers(FileServer("gsiftp://obelix.isi.edu/lizard/scratch-90-days/CCG/scratch", Operation.ALL)),
    Directory(Directory.LOCAL_STORAGE, "/lizard/scratch-90-days/CCG/outputs")
        .add_file_servers(FileServer("gsiftp://obelix.isi.edu/lizard/scratch-90-days/CCG/outputs", Operation.ALL))
)
ccg_site.add_env(PEGASUS_HOME="/usr")

sc.add_sites(local_site, ccg_site)
sc.write()

# --- Transformations ----------------------------------------------------------
# create transformation catalog for the outer level workflow
sleep_ = Transformation(
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
                        namespace="blackdiamond",
                        site="local",
                        pfn=TOP_DIR / "generate_inner_diamond_workflow.py",
                        is_stageable=True,
                    )

tc = TransformationCatalog()
tc.add_transformations(sleep_lvl1, sleep_lvl2, generate_diamond_wf)
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
inner_diamond_workflow_tc.write()

# --- Replicas -----------------------------------------------------------------
with open("f.a", "w") as f:
    f.write("Sample input file for the first inner dax job.")

# replica catalog for the inner diamond workflow
inner_diamond_workflow_rc = ReplicaCatalog()
inner_diamond_workflow_rc.add_replica(site="local", lfn="f.a", pfn=TOP_DIR / "f.a")
inner_diamond_workflow_rc.write("inner_diamond_workflow_rc.yml")

# replica catalog for the outer workflow
rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="pegasus.properties", pfn=TOP_DIR / "pegasus.properties")
rc.add_replica(site="local", lfn="inner_diamond_workflow_rc.yml", pfn = TOP_DIR / "inner_diamond_workflow_rc.yml")
rc.add_replica(site="local", lfn="sites.yml", pfn=TOP_DIR / "sites.yml")
rc.add_replica(site="local", lfn="inner_sleep_workflow.yml", pfn=TOP_DIR / "inner_sleep_workflow.yml")
rc.write()

# --- Workflow -----------------------------------------------------------------
wf = Workflow("hierarchical-workflow")

# job to generate the diamond workflow
diamond_wf_file = File("inner_diamond_workflow.yml")
generate_diamond_wf_job = Job(generate_diamond_wf)\
                            .add_outputs(diamond_wf_file)

# job to plan and run the diamond workflow
diamond_wf_job = SubWorkflow(file=diamond_wf_file, is_planned=False)\
                    .add_args(
                        "--conf",
                        "pegasus.properties",
                        "--output-sites",
                        "local",
                        "-vvv",
                        "--basename",
                        "inner"
                    )\
                    .add_inputs(
                        File("pegasus.properties"),
                        File("inner_diamond_workflow_rc.yml"),
                        File("sites.yml"),
                    )

sleep_wf_file = File("inner_sleep_workflow.yml")
sleep_wf_job = SubWorkflow(file=sleep_wf_file)\
                .add_args(
                    "--output-sites",
                    "local",
                    "-vvv"
                )

sleep_job = Job(sleep).add_args(5)

wf.add_jobs(generate_diamond_wf, diamond_wf_job, sleep_wf_job, sleep)
wf.add_dependency(generate_diamond_wf, children=[diamond_wf_job])
wf.add_dependency(diamond_wf_job, children=[sleep_wf_job])
wf.add_dependency(sleep_wf_job, children=[sleep])

wf.plan(
    sites=["local", "CCG"],
    output_sites=["local"],
    dir="work",
    submit=True
)

