import os
import subprocess
import time
import urllib.request
from argparse import ArgumentParser
from enum import Enum, unique

import yaml
from git import Repo

from Pegasus.api import *
from Pegasus.api.site_catalog import SupportedJobs

# import Sites
# from Pegasus.api import Properties


@unique
class SitesAvailable(Enum):
    LOCAL = 1
    USC_HPCC = 2
    OSG_ISI = 3
    XSEDE_BOSCO = 4
    BLUEWATERS_GLITE = 5
    WRANGLER_GLITE = 6
    SUMMIT_GLITE = 7
    SUMMIT_KUBERNETES = 8


SitesRequireProject = [
    SitesAvailable.BLUEWATERS_GLITE,
    SitesAvailable.WRANGLER_GLITE,
    SitesAvailable.SUMMIT_GLITE,
    SitesAvailable.SUMMIT_KUBERNETES,
]


class MySite:
    def __init__(
        self,
        scratch_parent_dir,
        storage_parent_dir,
        target_site: SitesAvailable,
        project="",
    ):
        self.shared_scratch_parent_dir = scratch_parent_dir
        self.local_storage_parent_dir = storage_parent_dir

        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(
                Directory.SHARED_SCRATCH,
                os.path.join(self.shared_scratch_parent_dir, "scratch"),
            ).add_file_servers(
                FileServer(
                    "file://" + os.path.join(self.shared_scratch_parent_dir, "scratch"),
                    Operation.ALL,
                )
            ),
            Directory(
                Directory.LOCAL_STORAGE,
                os.path.join(self.local_storage_parent_dir, "storage"),
            ).add_file_servers(
                FileServer(
                    "file://" + os.path.join(self.local_storage_parent_dir, "storage"),
                    Operation.ALL,
                )
            ),
        )

        self.sc.add_sites(local)

        if target_site is SitesAvailable.LOCAL:
            self.exec_site_name = "condorpool"
            self.condorpool()
        elif target_site is SitesAvailable.USC_HPCC:
            self.exec_site_name = "usc-hpcc"
            self.usc_hpcc()
        elif target_site is SitesAvailable.OSG_ISI:
            self.exec_site_name = "osg-isi"
            self.osg_isi()
        elif target_site is SitesAvailable.XSEDE_BOSCO:
            self.exec_site_name = "xsede"
            self.condorpool()
        elif target_site is SitesAvailable.BLUEWATERS_GLITE:
            self.exec_site_name = "bluewaters"
            self.bluewaters_glite(project)
        elif target_site is SitesAvailable.WRANGLER_GLITE:
            self.exec_site_name = "wrangler"
            self.wrangler_glite(project)
        elif target_site is SitesAvailable.SUMMIT_GLITE:
            self.exec_site_name = "summit"
            self.summit_glite(project)
        elif target_site is SitesAvailable.SUMMIT_KUBERNETES:
            self.exec_site_name = "summit"
            self.summit_kubernetes(project)

    def write(self):
        self.sc.write()

    def condorpool(self):
        condorpool = (
            Site(self.exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_pegasus_profile(data_configuration="condorio")
            .add_condor_profile(
                periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)"
            )
            .add_pegasus_profile(clusters_num=2)
        )

        self.sc.add_sites(condorpool)

    def usc_hpcc(self):
        usc = (
            Site(self.exec_site_name)
            .add_directories(
                Directory(
                    Directory.SHARED_SCRATCH,
                    os.path.join(
                        self.shared_scratch_parent_dir, self.exec_site_name, "scratch"
                    ),
                ).add_file_servers(
                    FileServer(
                        "file://"
                        + os.path.join(
                            self.shared_scratch_parent_dir,
                            self.exec_site_name,
                            "scratch",
                        ),
                        Operation.ALL,
                    )
                )
            )
            .add_pegasus_profile(style="glite")
            .add_condor_profile(grid_resource="batch slurm")
            .add_pegasus_profile(queue="quick")
            .add_pegasus_profile(data_configuration="sharedfs")
            .add_pegasus_profile(auxillary_local=True)
            .add_pegasus_profile(clusters_num=2)
            .add_pegasus_profile(job_aggregator="mpiexec")
            .add_env(
                key="PEGASUS_HOME",
                value="/home/rcf-proj/gmj/pegasus/SOFTWARE/pegasus/default",
            )
        )

        self.sc.add_sites(usc)

    def osg_isi(self):
        osg = (
            Site(self.exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_pegasus_profile(data_configuration="condorio")
            .add_condor_profile(
                periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)"
            )
            .add_condor_profile(
                requirements='OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True'
            )
            .add_profiles(Namespace.CONDOR, key="+ProjectName", value="PegasusTraining")
            .add_pegasus_profile(clusters_num=2)
        )

        self.sc.add_sites(osg)

    def bluewaters_glite(self, project):
        bluewaters = (
            Site(self.exec_site_name)
            .add_directories(
                Directory(
                    Directory.SHARED_SCRATCH,
                    os.path.join(
                        self.shared_scratch_parent_dir, self.exec_site_name, "scratch"
                    ),
                ).add_file_servers(
                    FileServer(
                        "file://"
                        + os.path.join(
                            self.shared_scratch_parent_dir,
                            self.exec_site_name,
                            "scratch",
                        ),
                        Operation.ALL,
                    )
                )
            )
            .add_pegasus_profile(style="glite")
            .add_pegasus_profile(queue="normal")
            .add_condor_profile(grid_resource="batch pbs")
            .add_pegasus_profile(data_configuration="sharedfs")
            .add_pegasus_profile(auxillary_local="true")
            .add_pegasus_profile(cores=1)
            .add_pegasus_profile(ppn=1)
            .add_pegasus_profile(project=project)
            .add_pegasus_profile(clusters_num=2)
            .add_env(
                key="PEGASUS_HOME",
                value="/mnt/a/u/training/instr006/SOFTWARE/install/pegasus/default",
            )
        )

        self.sc.add_sites(bluewaters)

    def wrangler_glite(self, project):
        wrangler = (
            Site(self.exec_site_name)
            .add_directories(
                Directory(
                    Directory.SHARED_SCRATCH,
                    os.path.join(
                        self.shared_scratch_parent_dir, self.exec_site_name, "scratch"
                    ),
                ).add_file_servers(
                    FileServer(
                        "file://"
                        + os.path.join(
                            self.shared_scratch_parent_dir,
                            self.exec_site_name,
                            "scratch",
                        ),
                        Operation.ALL,
                    )
                )
            )
            .add_pegasus_profile(style="glite")
            .add_pegasus_profile(queue="normal")
            .add_condor_profile(grid_resource="batch slurm")
            .add_pegasus_profile(data_configuration="sharedfs")
            .add_pegasus_profile(auxillary_local="true")
            .add_pegasus_profile(cores=1)
            .add_pegasus_profile(nodes=1)
            .add_pegasus_profile(project=project)
            .add_pegasus_profile(job_aggregator="mpiexec")
            .add_pegasus_profile(runtime=14400)
            .add_pegasus_profile(clusters_num=2)
            .add_env(
                key="PEGASUS_HOME",
                value="/home/00340/vahik/SOFTWARE/install/pegasus/default",
            )
        )

        self.sc.add_sites(wrangler)

    def summit_glite(self, project):
        summit = (
            Site(self.exec_site_name)
            .add_directories(
                Directory(
                    Directory.SHARED_SCRATCH,
                    os.path.join(
                        self.shared_scratch_parent_dir, self.exec_site_name, "scratch"
                    ),
                ).add_file_servers(
                    FileServer(
                        "file://"
                        + os.path.join(
                            self.shared_scratch_parent_dir,
                            self.exec_site_name,
                            "scratch",
                        ),
                        Operation.ALL,
                    )
                )
            )
            .add_pegasus_profile(style="glite")
            .add_pegasus_profile(queue="batch")
            .add_condor_profile(grid_resource="batch lsf")
            .add_pegasus_profile(data_configuration="sharedfs")
            .add_pegasus_profile(auxillary_local="true")
            .add_pegasus_profile(nodes=1)
            .add_pegasus_profile(project=project)
            .add_pegasus_profile(job_aggregator="mpiexec")
            .add_pegasus_profile(runtime=1800)
            .add_pegasus_profile(clusters_num=2)
            .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        )

        self.sc.add_sites(summit)

    def summit_kubernetes(self, project):
        summit = (
            Site(self.exec_site_name)
            .add_grids(
                Grid(
                    grid_type=Grid.BATCH,
                    scheduler_type=Scheduler.LSF,
                    contact="${USER}@dtn.ccs.ornl.gov",
                    job_type=SupportedJobs.COMPUTE,
                ),
                Grid(
                    grid_type=Grid.BATCH,
                    scheduler_type=Scheduler.LSF,
                    contact="${USER}@dtn.ccs.ornl.gov",
                    job_type=SupportedJobs.AUXILLARY,
                ),
            )
            .add_directories(
                Directory(
                    Directory.SHARED_SCRATCH,
                    os.path.join(
                        self.shared_scratch_parent_dir, self.exec_site_name, "scratch"
                    ),
                ).add_file_servers(
                    FileServer(
                        "file://"
                        + os.path.join(
                            self.shared_scratch_parent_dir,
                            self.exec_site_name,
                            "scratch",
                        ),
                        Operation.ALL,
                    )
                )
            )
            .add_pegasus_profile(style="ssh")
            .add_pegasus_profile(queue="batch")
            .add_pegasus_profile(auxillary_local="true")
            .add_pegasus_profile(change_dir="true")
            .add_pegasus_profile(nodes=1)
            .add_pegasus_profile(project=project)
            .add_pegasus_profile(job_aggregator="mpiexec")
            .add_pegasus_profile(runtime=1800)
            .add_pegasus_profile(clusters_num=2)
            .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        )

        self.sc.add_sites(summit)


def console_select_workflow(workflows_available):
    pass

    print_workflows(workflows_available)

    try:
        option = int(input("Select a training workflow: "))
        workflow = workflows_available[option]
    except:
        print("This is not a valid option...")
        exit()

    return workflow


def console_select_site():
    site = None
    project_name = None

    sites_available = {
        site.value: {"name": site.name, "member": site} for site in SitesAvailable
    }
    print_sites(sites_available)

    try:
        option = int(input("Select an execution site: "))
        site = sites_available[option]["member"]
    except:
        print("This is not a valid option...")
        exit()

    if site in SitesRequireProject:
        project_name = input("What's your project name: ")

    return (site, project_name)


def print_sites(sites_available):
    for k in sites_available:
        site = sites_available[k]
        print(f"{k}) {site['name']}")

    return


def print_workflows(workflows_available):
    for k in workflows_available:
        workflow = workflows_available[k]
        print(f"{k}) {workflow['organization']}/{workflow['repo_name']}")

    return


def clone_workflow(wf_dir, workflow):
    print("Fetching workflow...")
    Repo.clone_from(
        f"https://github.com/{workflow['organization']}/{workflow['repo_name']}.git",
        os.path.join(os.getcwd(), wf_dir),
    )
    return


def read_pegasushub_config(wf_dir):
    config = {"generator": "workflow_generator.py"}
    data = None
    # data = yaml.load(open(os.path.join(os.getcwd(), wf_dir, ".pegasushub.yml")), Loader=yaml.FullLoader)
    if not data is None:
        if "generator" in data:
            config["generator"] = data["generator"]

    return config


def create_pegasus_properties():
    props = Properties()
    props["pegasus.transfer.arguments"] = "-m 1"

    props.write()
    return


def create_workflow(wf_dir, workflow, site, project_name):
    print("Generating workflow...")
    pegasushub_config = read_pegasushub_config(wf_dir)

    os.chdir(wf_dir)

    if project_name is None:
        exec_sites = MySite(os.getcwd(), os.getcwd(), site)
    else:
        exec_sites = MySite(os.getcwd(), os.getcwd(), site, project=project_name)

    subprocess.run(
        [
            "python3",
            pegasushub_config["generator"],
            "-s",
            "-e",
            exec_sites.exec_site_name,
        ]
    )
    exec_sites.write()
    create_pegasus_properties()
    return


def read_workflows(wf_gallery, site):
    data = yaml.load(open(wf_gallery), Loader=yaml.FullLoader)
    workflows_available = [
        x
        for x in data
        if "training" in x
        and x["training"] == True
        and site.name in x["execution_sites"]
    ]
    workflows_available_tmp = sorted(
        workflows_available, key=lambda x: (x["organization"], x["repo_name"])
    )
    workflows_available = {}
    for i in range(len(workflows_available_tmp)):
        workflows_available[i + 1] = workflows_available_tmp[i]

    return workflows_available


def update_workflow_list(wf_gallery):
    url = "https://raw.githubusercontent.com/pegasushub/pegasushub.github.io/master/_data/workflows.yml"
    if not os.path.isfile(wf_gallery):
        os.makedirs(wf_gallery[: wf_gallery.rfind("/")], exist_ok=True)
        urllib.request.urlretrieve(url, wf_gallery)
    elif int(os.path.getmtime(wf_gallery)) < time.time() - 24 * 60 * 60:
        urllib.request.urlretrieve(url, wf_gallery)


def main():
    parser = ArgumentParser()

    parser.add_argument("-d", "--dir", type=str, help="Directory Name", required=True)
    parser.add_argument(
        "-w",
        "--workflows",
        default="~/.pegasus/pegasushub/workflows.yml",
        type=str,
        help="Workflow Gallery (Default: ~/.pegasus/workflows.yml)",
        required=False,
    )
    args = parser.parse_args()

    if args.workflows.startswith("~"):
        args.workflows = os.path.expanduser(args.workflows)
        if args.workflows == os.path.expanduser("~/.pegasus/pegasushub/workflows.yml"):
            update_workflow_list(args.workflows)

    (site, project_name) = console_select_site()
    workflows_available = read_workflows(args.workflows, site)

    workflow = console_select_workflow(workflows_available)

    clone_workflow(args.dir, workflow)

    create_workflow(args.dir, workflow, site, project_name)

    return


if __name__ == "__main__":
    main()
