#!/usr/bin/env python3

import os
import re
import shutil
import subprocess
from pathlib import Path

import click

from Pegasus.tools.utils import slurp_braindb


@click.command()
@click.pass_context
@click.option(
    # TODO: Remove `--dagid` as it is replaced with `--dag-id`
    "--dag-id",
    "--dagid",
    "-d",
    metavar="<dag-id>",
    help="The id of the dag to be removed.",
)
@click.argument(
    "submit-dir",
    required=False,
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
)
def pegasus_remove(ctx, dag_id=None, verbose=False, submit_dir=None):
    """pegasus-remove helps you remove an entire workflow."""
    if not submit_dir and not dag_id:
        print("You must provide either a dag_id or dagdirectory to remove a workflow.")
        ctx.exit(1)

    if submit_dir:
        cwd = os.getcwd()

        submit_dir = str(Path(submit_dir).resolve())
        try:
            os.chdir(submit_dir)
        except PermissionError:
            click.secho(
                click.style("Error: ", fg="red", bold=True)
                + "Cannot change to directory %s" % submit_dir
            )
            ctx.exit(1)

        config = slurp_braindb(submit_dir)
        if not config:
            click.secho(
                click.style("Error: ", fg="red", bold=True)
                + "%s is not a valid submit-dir" % submit_dir
            )
            ctx.exit(1)

        dag_log_file = config["dag"] + ".dagman.out"
        pattern = re.compile(r"\.([0-9\.]+) \(CONDOR_DAGMAN\) STARTING UP")

        with open(dag_log_file) as fp:
            for line in fp.readlines():
                match = pattern.search(line)
                if match:
                    dag_id = match.group(1)
            else:
                if not dag_id:
                    click.secho(
                        click.style("Error: ", fg="red", bold=True)
                        + "You must provide either a dag-id or dag-directory to remove a workflow."
                    )
                    ctx.exit(1)

        os.chdir(cwd)

    if dag_id:
        condor_rm = shutil.which("condor_rm")
        cmd = (condor_rm, dag_id)

        rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if rv.returncode == 0:
            click.echo(rv.stdout.decode().strip())
            click.secho("✨ Success", fg="green")
        else:
            click.echo(rv.stderr.decode().strip())
            click.secho("Error ", fg="red", bold=True)
            ctx.exit(42)


if __name__ == "__main__":
    pegasus_remove()
