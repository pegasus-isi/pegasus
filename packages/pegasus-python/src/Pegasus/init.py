import os
import subprocess
import sys
import time
import urllib.request

import click
from git import Repo
from git.exc import GitCommandError

from Pegasus import yaml
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
pegasushub_site_catalogs_url = "https://raw.githubusercontent.com/pegasushub/pegasus-site-catalogs/{}/Sites.py".format(
    pegasus_major_minor_version
)
wf_sites = os.path.join(
    pegasushub_data_path, "{}/Sites.py".format(pegasus_major_minor_version)
)


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
import Sites  # isort:skip


def console_select_workflow(workflows_available):
    print_workflows(workflows_available)

    workflow = click.prompt(
        "Select an example workflow",
        default=1,
        type=click.IntRange(1, len(workflows_available)),
        show_default=True,
    )
    workflow = workflows_available[workflow]

    return workflow


def console_select_site():
    site = None
    project_name = ""
    queue_name = ""
    pegasus_home = ""

    #### Select Site ####
    sites_available = {
        site.value: {"name": site.name, "member": site} for site in Sites.SitesAvailable
    }
    print_sites(sites_available)

    site = click.prompt(
        "Select an execution environment",
        default=1,
        type=click.IntRange(1, len(sites_available)),
        show_default=True,
    )
    site = sites_available[site]["member"]

    #### Insert queue name ####
    if site in Sites.SitesRequireQueue:
        queue_name = click.prompt("What's the execution environment's queue")

    #### Insert project name ####
    if site in Sites.SitesRequireProject:
        project_name = click.prompt("What's your project's name")
    elif site in Sites.SitesMayRequireProject:
        project_name = click.prompt(
            "What's your project's name", default="", show_default=True
        )

    #### Insert pegasus home location ####
    if site in Sites.SitesRequirePegasusHome:
        pegasus_home = click.prompt(
            "What's the location of the PEGASUS_HOME dir on the compute nodes",
            default=PEGASUS_HOME,
            show_default=True,
        )

    return (site, project_name, queue_name, pegasus_home)


def print_sites(sites_available):
    click.echo(
        """###########################################################
###########   Available Execution Environments   ##########
###########################################################"""
    )

    for k in sites_available:
        click.echo(
            "{}) {}".format(
                k, Sites.SitesAvailableDescription[sites_available[k]["member"]]
            )
        )

    click.echo()
    return


def print_workflows(workflows_available):
    click.echo(
        """###########################################################
###########     Available Workflow Examples      ##########
###########################################################"""
    )

    for k in workflows_available:
        workflow = workflows_available[k]
        click.echo(
            "{}) {}/{}".format(k, workflow["organization"], workflow["repo_name"])
        )

    click.echo()
    return


def clone_workflow(wf_dir, workflow):
    workflow_source = "https://null:null@github.com/{}/{}.git".format(
        workflow["organization"], workflow["repo_name"]
    )

    click.echo(
        "Fetching workflow from https://github.com/{}/{}.git".format(
            workflow["organization"], workflow["repo_name"]
        )
    )

    try:
        repo = Repo.clone_from(
            workflow_source, os.path.join(os.getcwd(), wf_dir, workflow["repo_name"]),
        )
    except GitCommandError:
        click.echo("This repository doesn't exist in this location or it's private.")
        click.echo("Exiting...")
        exit()

    return


def read_pegasushub_config(wf_dir, workflow):
    config = None

    config = yaml.load(
        open(
            os.path.join(os.getcwd(), wf_dir, workflow["repo_name"], ".pegasushub.yml")
        )
    )

    if config:
        if "scripts" in config:
            if (not "generator" in config["scripts"]) or (
                config["scripts"]["generator"] == ""
            ):
                config["scripts"]["generator"] = "workflow_generator.py"
        else:
            config["scripts"] = {"generator": "workflow_generator.py"}
    else:
        config = {"scripts": {"generator": "workflow_generator.py"}}

    return config


def create_pegasus_properties(commands):
    commands.append("#### Generating Pegasus Properties ####")

    props = Properties()
    props["pegasus.transfer.arguments"] = "-m 1"
    commands.append(
        'echo "{} = {}" > pegasus.properties'.format(
            "pegasus.transfer.arguments", "-m 1"
        )
    )

    props.write()

    return commands


def create_plan_script(exec_site_name, workflow_file):
    plan_script = """#!/bin/sh

pegasus-plan --conf pegasus.properties \\
    --dir submit \\
    --sites {} \\
    --output-sites local \\
    --cleanup leaf \\
    --force \\
    {}""".format(
        exec_site_name, workflow_file
    )

    with open("plan.sh", "w+") as g:
        g.write(plan_script)
        g.write("\n")

    os.chmod("plan.sh", 0o775)

    return


def create_generate_script(commands):
    generate_script = "#!/bin/sh\n\nexport PYTHONPATH={}\n\n{}".format(
        os.getenv("PYTHONPATH"), "\n".join(commands)
    )

    with open("generate.sh", "w+") as g:
        g.write(generate_script)
        g.write("\n")

    os.chmod("generate.sh", 0o775)

    return


def create_workflow(wf_dir, workflow, site, project_name, queue_name, pegasus_home):
    commands = []
    pegasushub_config = read_pegasushub_config(wf_dir, workflow)

    click.echo(
        "Generating workflow based on {}/{}".format(
            workflow["organization"], workflow["repo_name"]
        )
    )
    if queue_name:
        click.echo('This workflow will target queue "{}"'.format(queue_name))

    if project_name:
        click.echo('The project allocation used is "{}"'.format(project_name))

    if pegasus_home:
        click.echo('The PEGASUS_HOME location is "{}"'.format(pegasus_home))

    old_dir = os.getcwd()
    os.chdir(wf_dir)

    exec_site = Sites.MySite(
        os.getcwd(),
        os.getcwd(),
        site,
        project_name=project_name,
        queue_name=queue_name,
        pegasus_home=pegasus_home,
    )

    pre_scripts = [x for x in pegasushub_config["scripts"] if x.startswith("pre-")]

    scripts = [
        x
        for x in pegasushub_config["scripts"]
        if not (x.startswith(("pre-", "post-")) or x == "generator")
    ]

    post_scripts = [x for x in pegasushub_config["scripts"] if x.startswith("post-")]

    if pre_scripts:
        commands.append("#### Executing Workflow Pre Scripts ####")

    for pre_script in pre_scripts:
        exec_script = pegasushub_config["scripts"][pre_script]
        if not exec_script.startswith("/"):
            exec_script = os.path.join("./", workflow["repo_name"], exec_script)
        subprocess.run(exec_script, shell=True)
        commands.append(exec_script)

    if scripts:
        commands.append("#### Executing Workflow Scripts ####")
    for script in scripts:
        exec_script = pegasushub_config["scripts"][script]
        if not exec_script.startswith("/"):
            exec_script = os.path.join("./", workflow["repo_name"], exec_script)
        subprocess.run(exec_script, shell=True)
        commands.append(exec_script)

    commands.append("#### Executing Workflow Generator ####")
    exec_script = pegasushub_config["scripts"]["generator"]
    if not exec_script.startswith("/"):
        exec_script = os.path.join("./", workflow["repo_name"], exec_script)
    generate_workflow_cmd = " ".join(
        [exec_script, "-s", "-e", exec_site.exec_site_name, "-o", "workflow.yml"]
    )
    subprocess.run(generate_workflow_cmd, shell=True)
    commands.append(generate_workflow_cmd)

    if post_scripts:
        commands.append("#### Executing Workflow Post Scripts ####")
    for post_script in post_scripts:
        exec_script = pegasushub_config["scripts"][post_script]
        if not exec_script.startswith("/"):
            exec_script = os.path.join("./", workflow["repo_name"], exec_script)
        subprocess.run(exec_script, shell=True)
        commands.append(exec_script)

    click.echo("Creating properties file...")
    commands = create_pegasus_properties(commands)

    click.echo("Creating site catalog for {}...".format(site))
    exec_site.write()

    commands.append("#### Generating Sites Catalog ####")
    generate_sites_cmd = """python3 {} \\
    --execution-site {} \\
    --project-name \"{}\" \\
    --queue-name \"{}\" \\
    --pegasus-home \"{}\" \\
    --scratch-parent-dir {} \\
    --storage-parent-dir {}""".format(
        wf_sites,
        site.name,
        str(project_name),
        queue_name,
        pegasus_home,
        os.getcwd(),
        os.getcwd(),
    )
    commands.append(generate_sites_cmd)

    create_plan_script(exec_site.exec_site_name, "workflow.yml")
    create_generate_script(commands)

    os.chdir(old_dir)

    return


def read_workflows(wf_gallery, site):
    data = yaml.load(open(wf_gallery))
    workflows_available = [
        x
        for x in data
        if "training" in x
        and x["training"] is True
        and site.name in x["execution_sites"]
    ]

    workflows_available_tmp = sorted(
        workflows_available, key=lambda x: (x["organization"], x["repo_name"])
    )
    workflows_available = {}
    for i in range(len(workflows_available_tmp)):
        workflows_available[i + 1] = workflows_available_tmp[i]

    return workflows_available


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-w",
    "--workflow-gallery",
    default=os.path.expanduser("~/.pegasus/pegasushub/workflows.yml"),
    type=click.Path(),
    show_default=True,
    help="Workflow Gallery File.",
)
@click.argument("directory", type=click.Path(exists=False))
def main(directory, workflow_gallery):
    """Welcome to Pegasus Init.

    This tool is designed to be an interactive cli tool that generates
    example workflows, ready to be executed on common execution environments.
    The example workflows provided are a subset of the workflows availabe at
    PegasusHub ( https://pegasushub.github.io ), which are marked as examples.

    Some of the example workflows might overwrite the configuration
    Pegasus Init generates. Be cautious when executing commands that
    may alter the workflow and catalogs generated by Pegasus Init.
    """

    if os.path.isfile(directory) or os.path.isdir(directory):
        click.echo("The given directory name already exists")
        click.echo("Exiting...")
        exit()

    if workflow_gallery == os.path.expanduser("~/.pegasus/pegasushub/workflows.yml"):
        update_workflow_list(workflow_gallery)

    (site, project_name, queue_name, pegasus_home) = console_select_site()
    workflows_available = read_workflows(workflow_gallery, site)

    if not workflows_available:
        click.echo("There are no example workflows supported for this site.")
        click.echo("Exiting...")
        exit()

    workflow = console_select_workflow(workflows_available)

    clone_workflow(directory, workflow)

    create_workflow(directory, workflow, site, project_name, queue_name, pegasus_home)

    return


if __name__ == "__main__":
    main()
