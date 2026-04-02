"""pegasus dagman — DAGMan invocation wrapper."""

import os
import runpy
import sys

import click


@click.command("dagman", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def dagman(args):
    """DAGMan invocation wrapper."""
    sys.argv = ["pegasus-dagman"] + list(args)
    # Run the original script as __main__
    script_dir = os.path.dirname(__file__)
    runpy.run_path(os.path.join(script_dir, "pegasus-dagman.py"), run_name="__main__")
