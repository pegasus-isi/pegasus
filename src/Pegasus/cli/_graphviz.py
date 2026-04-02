"""pegasus graphviz — Generate workflow visualizations."""

import os
import runpy
import sys

import click


@click.command("graphviz", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def graphviz(args):
    """Generate workflow visualizations."""
    sys.argv = ["pegasus-graphviz"] + list(args)
    # Run the original script as __main__
    script_dir = os.path.dirname(__file__)
    runpy.run_path(os.path.join(script_dir, "pegasus-graphviz.py"), run_name="__main__")
