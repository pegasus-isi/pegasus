#!/usr/bin/env python3

import logging
import os
import shutil
import subprocess
from json import dumps
from pathlib import Path

import click

from Pegasus.tools.utils import slurp_braindb

log = logging.getLogger("pegasus-run")


def check_dag(dag_file):
    """Check if DAG file exists."""
    dag_file = Path(dag_file)

    if not dag_file.is_file():
        raise FileNotFoundError("%s not found" % dag_file)
    elif not os.access(str(dag_file), os.R_OK):
        raise PermissionError("%s is not readable" % dag_file)

    log.debug("# found %s", dag_file)


def check_dag_sub_file(dag_sub_file):
    """Check if DAGman submit file exists."""
    dag_sub_file = Path(dag_sub_file)

    if not dag_sub_file.is_file():
        raise FileNotFoundError("%s not found" % dag_sub_file)
    elif not os.access(str(dag_sub_file), os.R_OK):
        raise PermissionError("%s is not readable" % dag_sub_file)

    log.debug("# dagman condor submit file is %s", dag_sub_file)


def get_condor_submit():
    """Locate `condor_submit` and check if it is executable."""
    condor_submit = shutil.which("condor_submit")

    if condor_submit is None:
        raise FileNotFoundError("condor_submit not found")
    elif not os.access(condor_submit, os.X_OK):
        raise PermissionError("%s is not executable" % condor_submit)

    log.debug("# found %s", condor_submit)
    return condor_submit


def get_grid_proxy_info(globus_location):
    """Locate `condor_submit` and check if it is executable."""
    grid_proxy_info = Path(globus_location) / "bin" / "grid-proxy-info"

    if not grid_proxy_info.exists():
        raise FileNotFoundError("%s not found" % grid_proxy_info)
    elif not os.access(str(grid_proxy_info), os.X_OK):
        raise PermissionError("%s is not executable" % grid_proxy_info)

    log.debug("# found %s", grid_proxy_info)
    return str(grid_proxy_info)


def salvage_log_file(condor_log):
    """Salvage jobs log files."""
    condor_log = Path(condor_log)

    if condor_log.exists():
        cl = str(condor_log)
        for i in range(1000):
            _cl = cl + ".%03d" % i
            if not Path(_cl).exists():
                log.debug("# log $result exists, rescuing from DAGMan.")
                try:
                    shutil.copyfile(cl, _cl)
                    click.secho("Rescued {} as {}".format(cl, _cl), err=True)
                except OSError as e:
                    raise ValueError(str(e))
                break


def grid_check():
    """Perform grid checks if enabled."""
    # sanity check: Is there a GLOBUS_LOCATION?
    globus_location = os.environ.get("GLOBUS_LOCATION", None)
    if globus_location is None:
        raise ValueError(
            "Your environment setup misses GLOBUS_LOCATION.\n"
            + "Please check carefully that you have sourced the correct setup files!",
        )

    log.debug("# GLOBUS_LOCATION=%s" % globus_location)

    # sanity check: Is GLOBUS_LOCCATION part of LD_LIBRARY_PATH?
    llp = os.environ.get("LD_LIBRARY_PATH", "")
    if not llp:
        llp = str(Path(globus_location) / "lib")
        os.putenv("LD_LIBRARY_PATH", llp)
        log.info("Setting LD_LIBRARY_PATH=%s", llp)

    # Find grid-proxy-info (should we use openssl instead? )
    gpi = get_grid_proxy_info(globus_location)

    # common user error
    # sanity check: Sufficient time left on grid proxy certificate
    cmd = (gpi, "-timeleft")

    rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    ec = rv.returncode

    if ec & 127:
        raise ValueError("%s died on signal %d" % (gpi, ec & 127))
    elif ec != 0:
        raise ValueError("%s exited with status %d" % (gpi, ec))

    time_left = int(rv.stdout.decode().strip())
    if time_left == -1:
        raise ValueError("Grid proxy not initialized, Please generate a new proxy")
    elif time_left == 0:
        raise ValueError("Grid proxy expired, please refresh")
    elif time_left < 7200:
        raise ValueError(
            "Too little time left (%d s) on grid proxy. Please refresh your proxy"
            % time_left
        )

    log.debug("# grid proxy has %d s left" % time_left)


def exec_dag(dag_sub_file, condor_log):
    """Execute `condor_submit` on dagman.condor.sub."""
    # PM-797 move away from using pegasus-submit-dag
    condor_submit = get_condor_submit()

    check_dag_sub_file(dag_sub_file)

    salvage_log_file(condor_log)
    click.secho("Submitting to condor %s" % dag_sub_file, err=True)
    cmd = (condor_submit, dag_sub_file)

    rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    rv.check_returncode()


def exec_script(script):
    """Locate `condor_submit` and check if it is executable."""
    script = Path(script)

    if not script.is_file():
        raise FileNotFoundError("%s not found" % script)
    elif not os.access(str(script), os.X_OK):
        raise PermissionError("%s is not executable" % script)

    log.debug("# found %s" % script)

    cmd = ("/bin/bash", str(script))
    rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    rv.check_returncode()


@click.command()
@click.pass_context
@click.option(
    "--grid/--nogrid",
    default=False,
    show_default=True,
    help="Enable/Disable checking for grids.",
)
@click.option(
    "-j",
    "--json",
    default=False,
    show_default=True,
    is_flag=True,
    help="Output in JSON format.",
)
@click.option(
    "-v", "--verbose", default=0, count=True, help="Raises debug level by 1.",
)
@click.argument(
    "submit-dir",
    required=False,
    default=".",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
)
def pegasus_run(ctx, grid=False, json=False, verbose=0, submit_dir=None):
    """."""
    logging.basicConfig(level=logging.ERROR - (min(verbose, 3) * 10))

    os.umask(0o022)
    cwd = os.getcwd()
    config = slurp_braindb(submit_dir)
    submit_dir = str(Path(submit_dir).resolve())

    if not config:
        click.secho(
            click.style("Error: ", fg="red", bold=True)
            + "%s is not a valid submit-dir" % submit_dir
        )
        ctx.exit(1)

    try:
        os.chdir(submit_dir)
    except PermissionError:
        click.secho(
            click.style("Error: ", fg="red", bold=True)
            + "Cannot change to directory %s" % submit_dir
        )
        ctx.exit(1)

    if grid:
        try:
            grid_check()
        except (FileNotFoundError, PermissionError, ValueError) as e:
            click.secho(click.style("Error: ", fg="red", bold=True) + str(e))
            ctx.exit(1)

    if config["dag"]:
        try:
            # sanity check: Is the DAG file there?
            check_dag(config["dag"])

            # PM-870 we have already changed to the directory, don't prepend $run again
            dag_sub_file = config["dag"] + ".condor.sub"

            # PM-702: clean up .halt files from pegasus-halt
            halt_released = False
            if Path(config["dag"] + ".halt").exists():
                click.echo("Found a previously halted workflow. Releasing it now.")
                os.system("find . -name '*.dag.halt' -exec rm {} \\;")
                halt_released = True

            # After the switch from condor_submit_dag, we lost the check to see if
            # a workflow is already running. This replaces those checks.
            if Path("monitord.pid").exists():
                if halt_released:
                    ctx.exit(0)
                else:
                    click.secho(
                        click.style("Error: ", fg="red", bold=True)
                        + "It looks like the workflow is already running! If you are sure\n"
                        + "       that is not the case, please remove the monitord.pid file and try\n"
                        + "       again.",
                        err=True,
                    )
                    ctx.exit(1)

            # PM-797 do condor_submit on dagman.condor.sub file if it exists
            exec_dag(dag_sub_file, config["condor_log"])
            log.debug("# dagman is running")
            if json:
                click.echo(dumps(config))
            else:
                click.secho(
                    """
Your workflow has been started and is running in the base directory:

%(submit_dir)s

*** To monitor the workflow you can run ***

pegasus-status -l %(submit_dir)s

*** To remove your workflow run ***

pegasus-remove %(submit_dir)s"""
                    % {"submit_dir": submit_dir}
                )
        except (FileNotFoundError, PermissionError, ValueError) as e:
            click.secho(click.style("Error: ", fg="red", bold=True) + str(e))
            ctx.exit(1)
        except subprocess.CalledProcessError as e:
            rc = e.returncode
            if rc != 0:
                click.secho(
                    click.style("Error: ", fg="red", bold=True)
                    + "Running %s failed with %d" % (e.cmd, rc)
                )
                ctx.exit(rc)

    elif config["type"] == "shell":
        try:
            exec_script(config["script"])
            click.secho("✨ Success", fg="green")
        except (FileNotFoundError, PermissionError) as e:
            click.secho(click.style("Error: ", fg="red", bold=True) + str(e))
            ctx.exit(1)
        except subprocess.CalledProcessError as e:
            rc = e.returncode
            if rc != 0:
                click.secho(
                    click.style("Error: ", fg="red", bold=True)
                    + "Running %s failed with %d" % (config["script"], rc)
                )
                ctx.exit(rc)

    os.chdir(cwd)


if __name__ == "__main__":
    pegasus_run()
