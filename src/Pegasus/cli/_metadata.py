"""pegasus metadata — Manage workflow metadata."""

import os
import runpy
import sys

import click


@click.command("metadata", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def metadata(args):
    """Manage workflow metadata."""
    sys.argv = ["pegasus-metadata"] + list(args)
    # Run the original script as __main__
    script_dir = os.path.dirname(__file__)
    runpy.run_path(os.path.join(script_dir, "pegasus-metadata.py"), run_name="__main__")
