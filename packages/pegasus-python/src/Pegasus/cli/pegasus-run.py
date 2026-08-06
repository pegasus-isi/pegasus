#!/usr/bin/env python3

import logging
import os
import shutil
import subprocess
import sys
from json import dumps
from pathlib import Path

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

import click

from Pegasus.tools.utils import slurp_braindb

log = logging.getLogger("pegasus-run")


def check_dag(dag_file):
    """Check if DAG file exists."""
    dag_file = Path(dag_file)

    if not dag_file.is_file():
        raise FileNotFoundError(f"{dag_file} not found")
    if not os.access(str(dag_file), os.R_OK):
        raise PermissionError(f"{dag_file} is not readable")

    log.debug("# found %s", dag_file)


def check_dag_sub_file(dag_sub_file):
    """Check if DAGman submit file exists."""
    dag_sub_file = Path(dag_sub_file)

    if not dag_sub_file.is_file():
        raise FileNotFoundError(f"{dag_sub_file} not found")
    if not os.access(str(dag_sub_file), os.R_OK):
        raise PermissionError(f"{dag_sub_file} is not readable")

    log.debug("# dagman condor submit file is %s", dag_sub_file)


def get_condor_submit():
    """Locate `condor_submit` and check if it is executable."""
    condor_submit = shutil.which("condor_submit")

    if condor_submit is None:
        raise FileNotFoundError("condor_submit not found")
    if not os.access(condor_submit, os.X_OK):
        raise PermissionError(f"{condor_submit} is not executable")

    log.debug("# found %s", condor_submit)
    return condor_submit


def get_grid_proxy_info(globus_location):
    """Locate `condor_submit` and check if it is executable."""
    grid_proxy_info = Path(globus_location) / "bin" / "grid-proxy-info"

    if not grid_proxy_info.exists():
        raise FileNotFoundError(f"{grid_proxy_info} not found")
    if not os.access(str(grid_proxy_info), os.X_OK):
        raise PermissionError(f"{grid_proxy_info} is not executable")

    log.debug("# found %s", grid_proxy_info)
    return str(grid_proxy_info)


def salvage_log_file(condor_log):
    """Salvage jobs log files."""
    condor_log = Path(condor_log)

    if condor_log.exists():
        cl = str(condor_log)
        for i in range(1000):
            _cl = cl + f".{i:03d}"
            if not Path(_cl).exists():
                log.debug("# log $result exists, rescuing from DAGMan.")
                try:
                    shutil.copyfile(cl, _cl)
                    click.secho(f"Rescued {cl} as {_cl}", err=True)
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
            "Please check carefully that you have sourced the correct setup files!",
        )

    log.debug(f"# GLOBUS_LOCATION={globus_location}")

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
        raise ValueError(f"{gpi} died on signal {ec & 127:d}")
    if ec != 0:
        raise ValueError(f"{gpi} exited with status {ec:d}")

    time_left = int(rv.stdout.decode().strip())
    if time_left == -1:
        raise ValueError("Grid proxy not initialized, Please generate a new proxy")
    if time_left == 0:
        raise ValueError("Grid proxy expired, please refresh")
    if time_left < 7200:
        raise ValueError(
            f"Too little time left ({time_left:d} s) on grid proxy. Please refresh your proxy"
        )

    log.debug(f"# grid proxy has {time_left:d} s left")


def exec_dag(dag_sub_file, condor_log):
    """Execute `condor_submit` on dagman.condor.sub."""
    # PM-797 move away from using pegasus-submit-dag
    condor_submit = get_condor_submit()

    check_dag_sub_file(dag_sub_file)

    salvage_log_file(condor_log)
    click.secho(f"Submitting to condor {dag_sub_file}", err=True)
    cmd = (condor_submit, "-terse", dag_sub_file)

    rv = subprocess.run(cmd, capture_output=True)
    rv.check_returncode()
    return float(rv.stdout.decode().split("-")[0].strip())


def exec_script(script):
    """Locate `condor_submit` and check if it is executable."""
    script = Path(script)

    if not script.is_file():
        raise FileNotFoundError(f"{script} not found")
    if not os.access(str(script), os.X_OK):
        raise PermissionError(f"{script} is not executable")

    log.debug(f"# found {script}")

    cmd = ("/bin/bash", str(script))
    rv = subprocess.run(cmd)
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
    "-v",
    "--verbose",
    default=0,
    count=True,
    help="Raises debug level by 1.",
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
            + f"{submit_dir} is not a valid submit-dir"
        )
        ctx.exit(1)

    try:
        os.chdir(submit_dir)
    except PermissionError:
        click.secho(
            click.style("Error: ", fg="red", bold=True)
            + f"Cannot change to directory {submit_dir}"
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
                for halt_file in Path(".").rglob("*.dag.halt"):
                    try:
                        halt_file.unlink()
                    except OSError:
                        pass
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
            job_id = exec_dag(dag_sub_file, config["condor_log"])
            log.debug("# dagman is running")
            if json:
                click.echo(dumps(config))
            else:
                click.secho(
                    f"""Submitting job(s).
1 job(s) submitted to cluster {job_id}.""",
                    err=True,
                )
                click.secho(
                    f"""
Your workflow has been started and is running in the base directory:

{submit_dir}

*** To monitor the workflow you can run ***

pegasus-status -l {submit_dir}

*** To remove your workflow run ***

pegasus-remove {submit_dir}"""
                )
        except (FileNotFoundError, PermissionError, ValueError) as e:
            click.secho(click.style("Error: ", fg="red", bold=True) + str(e))
            ctx.exit(1)
        except subprocess.CalledProcessError as e:
            rc = e.returncode
            if rc != 0:
                click.secho(
                    click.style("Error: ", fg="red", bold=True)
                    + f"Running {e.cmd} failed with {rc:d}"
                )
                click.secho(
                    click.style("Message: ", fg="red", bold=True)
                    + str(e.output or e.stderr, encoding="utf-8").strip()
                )
                ctx.exit(rc)

    elif config["type"] == "shell":
        try:
            if json:
                click.secho(
                    click.style("Error: ", fg="red", bold=True)
                    + "--json option is not supported for shell code"
                )
                ctx.exit(1)

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
                    + f"Running {config['script']} failed with {rc:d}"
                )
                ctx.exit(rc)

    os.chdir(cwd)


if __name__ == "__main__":
    pegasus_run()
