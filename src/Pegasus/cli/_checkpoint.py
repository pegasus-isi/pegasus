"""pegasus checkpoint — Manage workflow checkpoints."""

import os
import runpy
import sys

import click


@click.command("checkpoint", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def checkpoint(args):
    """Manage workflow checkpoints."""
    sys.argv = ["pegasus-checkpoint"] + list(args)
    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "pegasus-checkpoint.py"),
        run_name="__main__",
    )
