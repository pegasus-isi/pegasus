import os
import json
from pathlib import Path
import logging
import optparse
import shutil
import time
import click
import sys

from Pegasus.client.status import Status
from Pegasus.tools.utils import configureLogging, slurp_braindb

configureLogging(level=logging.DEBUG)


@click.command(options_metavar='<options>')
@click.pass_context
@click.option(
    "--long",
    "-l",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show all DAG states, including sub-DAGs, default only totals",
)
@click.option(
    "--json",
    "-j",
    is_flag=True,
    default=False,
    show_default=True,
    help="Returns the workflow status as a JSON serializable object",
)
@click.option(
    "--watch",
    "-w",
    type=int,
    metavar='<secs>',
    help="Repeatedly print status of workflow every <secs> seconds",
    nargs=1
)
@click.option(
    "--attrs",
    "-a",
    type=str,
    metavar='<attribute>',
    help="The job attribute column to be shown in condor q",
    multiple=True
)
@click.option(
    "--dirs",
    "-D",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show dagnames with path realtive to submit directory",
)
@click.argument(
    "submit-dir",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    nargs=1
)
def get_wf_status(ctx, long, json, watch, attrs, dirs, submit_dir):
    """pegasus-status helps to retrieve status of a given workflow."""
    
    if not submit_dir :
        print("You must provide a submit-directory to get workflow status.")
        ctx.exit(1)
    
    else:
        cwd = os.getcwd()

        submit_dir = str(Path(submit_dir).resolve())
        try:
            os.chdir(submit_dir)
        except PermissionError:
            click.secho(
                "{}{} directory is not readable".format(
                click.style("Error: ",fg="red",bold=True),submit_dir)
            )
            ctx.exit(1)
            
    braindb = slurp_braindb(submit_dir)
    if not braindb:
        click.secho(
            "{}{} is not a valid workflow submit-directory".format(
            click.style("Error: ",fg="red",bold=True),submit_dir)
        )
        ctx.exit(1)
    
    if json:
        progress = Status()
        print(json.dumps(progress.fetch_status(submit_dir=submit_dir,json=json, dirs=dirs),indent=2))
        exit(0)
    elif watch:
        continue_running = True
        while(continue_running):
            try:
                os.system('clear')
                screen_size = shutil.get_terminal_size().columns
                click.echo("{}".format('Press Ctrl+C to exit'))
                progress = Status()
                progress.fetch_status(submit_dir=submit_dir,long=long,dirs=dirs)
                if progress.kill_signal:
                    break
                time.sleep(watch)
            except KeyboardInterrupt:
                break
    else:
        progress = Status()
        progress.fetch_status(submit_dir=submit_dir,long=long,dirs=dirs)


if __name__ == "__main__":
    get_wf_status()
