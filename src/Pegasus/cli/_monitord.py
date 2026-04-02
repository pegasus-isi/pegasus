"""pegasus monitord — Logging daemon to update jobstate.log from DAGMan logs."""

import os
import runpy
import sys

import click


@click.command("monitord", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def monitord(args):
    """Logging daemon to update jobstate.log from DAGMan logs."""
    sys.argv = ["pegasus-monitord"] + list(args)
    # Run the original script as __main__
    script_dir = os.path.dirname(__file__)
    runpy.run_path(os.path.join(script_dir, "pegasus-monitord.py"), run_name="__main__")
