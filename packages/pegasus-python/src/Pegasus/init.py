import os
import subprocess
import sys
import time
import yaml
import urllib.request
from git import Repo
from argparse import ArgumentParser
from Pegasus.api import *

#### Default time to update dynamic config files in seconds ####
update_config_timeout = 24 * 60 * 60

#### Get PEGASUS_HOME from env ####
PEGASUS_HOME = os.getenv("PEGASUS_HOME")

#### Default data path ####
pegasushub_data_path = os.path.expanduser("~/.pegasus/pegasushub")

#### Pegasus major_minor_version ####
pegasus_major_minor_version = (
    subprocess.check_output(
        [os.path.join(PEGASUS_HOME, "bin", "pegasus-version"), "-m"]
    )
    .strip()
    .decode("utf-8")
)

#### Url to Sites.py on pegasushub and Sites.py location ####
pegasushub_site_catalogs_url = f"https://raw.githubusercontent.com/pegasushub/pegasus-site-catalogs/{pegasus_major_minor_version}/Sites.py"
wf_sites = os.path.join(pegasushub_data_path, f"{pegasus_major_minor_version}/Sites.py")


def update_site_catalogs(wf_sites):
    if not os.path.isfile(wf_sites):
        os.makedirs(wf_sites[: wf_sites.rfind("/")], exist_ok=True)
        urllib.request.urlretrieve(pegasushub_site_catalogs_url, wf_sites)
    elif int(os.path.getmtime(wf_sites)) < time.time() - update_config_timeout:
        urllib.request.urlretrieve(pegasushub_site_catalogs_url, wf_sites)

#### Url to workflows on pegasushub ####
pegasushub_workflows_url = "https://raw.githubusercontent.com/pegasushub/pegasushub.github.io/master/_data/workflows.yml"


def update_workflow_list(wf_gallery):
    if not os.path.isfile(wf_gallery):
        os.makedirs(wf_gallery[: wf_gallery.rfind("/")], exist_ok=True)
        urllib.request.urlretrieve(pegasushub_workflows_url, wf_gallery)
    elif int(os.path.getmtime(wf_gallery)) < time.time() - update_config_timeout:
        urllib.request.urlretrieve(pegasushub_workflows_url, wf_gallery)


#### Update Site Catalogs and load Sites ####
update_site_catalogs(wf_sites)
sys.path.insert(0, os.path.join(pegasushub_data_path, pegasus_major_minor_version))
import Sites


def console_select_workflow(workflows_available):
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
    queue_name = None

    sites_available = {
        site.value: {"name": site.name, "member": site} for site in Sites.SitesAvailable
    }
    print_sites(sites_available)

    try:
        option = int(input("Select an execution site: "))
        site = sites_available[option]["member"]
    except:
        print("This is not a valid option...")
        exit()

    if site in Sites.SitesRequireQueue:
        queue_name = input("What's the targeted queue name: ")

    if site in Sites.SitesRequireProject:
        project_name = input("What's your project name: ")

    return (site, project_name, queue_name)


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
    data = yaml.load(
        open(os.path.join(os.getcwd(), wf_dir, ".pegasushub.yml")),
        Loader=yaml.FullLoader,
    )
    if not data is None:
        if "generator" in data:
            config["generator"] = data["generator"]

    return config


def create_pegasus_properties():
    props = Properties()
    props["pegasus.transfer.arguments"] = "-m 1"

    props.write()
    return


def create_workflow(wf_dir, workflow, site, project_name, queue_name):
    print("Generating workflow...")
    pegasushub_config = read_pegasushub_config(wf_dir)

    os.chdir(wf_dir)

    if project_name is None:
        if queue_name is None:
            exec_sites = Sites.MySite(os.getcwd(), os.getcwd(), site)
        else:
            exec_sites = Sites.MySite(os.getcwd(), os.getcwd(), site, queue_name=queue_name)
    else:
        if queue_name is None:
            exec_sites = Sites.MySite(os.getcwd(), os.getcwd(), site, project_name=project_name)
        else:
            exec_sites = Sites.MySite(os.getcwd(), os.getcwd(), site, project_name=project_name, queue_name=queue_name)


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

    (site, project_name, queue_name) = console_select_site()
    workflows_available = read_workflows(args.workflows, site)

    workflow = console_select_workflow(workflows_available)

    clone_workflow(args.dir, workflow)

    create_workflow(args.dir, workflow, site, project_name, queue_name)

    return


if __name__ == "__main__":
    main()
