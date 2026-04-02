"""pegasus halt — halt a workflow gracefully by placing .halt files."""

import os
import sys
from pathlib import Path

import click


@click.command("halt")
@click.argument(
    "run-dir",
    required=False,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
def halt(run_dir=None):
    """Halt a workflow gracefully by placing .halt files.

    The workflow will exit once all current jobs have finished.
    It can be restarted with ``pegasus run``.
    """
    # If no run dir given, check if current dir is a run dir
    if run_dir is None:
        if Path("braindump.yml").exists():
            run_dir = os.getcwd()
        else:
            click.echo("Please specify a run dir", err=True)
            click.echo("Usage: pegasus halt [rundir]", err=True)
            sys.exit(1)

    run_path = Path(run_dir)

    if not run_path.exists():
        click.echo(f"{run_dir} does not exist!", err=True)
        sys.exit(1)

    if not (run_path / "braindump.yml").exists():
        click.echo(
            f"{run_dir} does not contain a braindump file, "
            "and hence is probably not a run directory.",
            err=True,
        )
        sys.exit(1)

    # Find all .dag files and create .halt files
    dag_count = 0
    for dag_file in run_path.rglob("*.dag"):
        if dag_file.is_file():
            halt_file = dag_file.parent / (dag_file.name + ".halt")
            halt_file.touch()
            dag_count += 1

    click.echo(
        "Workflow has been given the halt signal, and will gracefully exit once\n"
        "all current jobs have finished. The workflow can be restarted from\n"
        "where it was left off with the pegasus run command."
    )
