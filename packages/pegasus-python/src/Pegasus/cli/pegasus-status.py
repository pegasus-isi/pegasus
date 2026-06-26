import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

import click

from Pegasus.client.status import Status
from Pegasus.tools.utils import configureLogging, slurp_braindb

configureLogging(level=logging.DEBUG)


@click.command(options_metavar="<options>")
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
    "--jsonrv",
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
    metavar="<secs>",
    help="Repeatedly print status of workflow every <secs> seconds",
    nargs=1,
)
@click.option(
    "--dirs",
    "-D",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show dagnames with path realtive to submit directory",
)
@click.option(
    "--legend",
    "-L",
    is_flag=True,
    default=False,
    show_default=True,
    help="Legend explaining the columns in the output",
)
@click.option(
    "--noqueue",
    "-Q",
    is_flag=True,
    default=False,
    show_default=True,
    help="Turns off the output from parsing Condor Q",
)
@click.option(
    "--debug",
    "-d",
    is_flag=True,
    default=False,
    show_default=True,
    help="Internal debugging tool",
)
@click.argument(
    "submit-dir",
    required=False,
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    nargs=1,
)
def get_wf_status(ctx, long, jsonrv, watch, dirs, legend, noqueue, debug, submit_dir):
    """pegasus-status helps to retrieve status of a given workflow."""

    if not submit_dir:
        cwd = os.getcwd()
        if slurp_braindb(cwd):
            submit_dir = cwd
        else:
            submit_dir = None

    else:
        submit_dir = str(Path(submit_dir).resolve())
        try:
            os.chdir(submit_dir)
        except PermissionError:
            click.secho(
                "{}{} directory is not readable".format(
                    click.style("Error: ", fg="red", bold=True), submit_dir
                )
            )
            ctx.exit(1)

        braindb = slurp_braindb(submit_dir)
        if not braindb:
            click.secho(
                "{}{} is not a valid workflow submit-directory".format(
                    click.style("Error: ", fg="red", bold=True), submit_dir
                )
            )
            ctx.exit(1)

    if jsonrv:
        progress = Status()
        print(
            json.dumps(
                progress.fetch_status(
                    submit_dir=submit_dir,
                    json=jsonrv,
                    dirs=dirs,
                    legend=legend,
                    noqueue=noqueue,
                    debug=debug,
                ),
                indent=2,
            )
        )

    elif watch:
        continue_running = True
        while continue_running:
            try:
                os.system("clear")
                size = shutil.get_terminal_size().columns // 3
                ctrlc = "{0:<{1}}".format("Press Ctrl+C to exit", size)
                pid = "{0:^{1}}".format("(pid=" + str(os.getpid()) + ")", size)
                clock = "{0:>{1}}".format(
                    datetime.now().strftime("%a %b-%d-%Y %H:%M:%S"), size
                )
                click.echo(f"{ctrlc}{pid}{clock}")
                progress = Status()
                progress.fetch_status(
                    submit_dir=submit_dir,
                    long=long,
                    dirs=dirs,
                    legend=legend,
                    noqueue=noqueue,
                )
                if progress.kill_signal:
                    break
                time.sleep(watch)
            except KeyboardInterrupt:
                break

    else:
        progress = Status()
        progress.fetch_status(
            submit_dir=submit_dir,
            long=long,
            dirs=dirs,
            legend=legend,
            noqueue=noqueue,
            debug=debug,
        )


if __name__ == "__main__":
    get_wf_status()
