"""pegasus globus-online — Interact with Globus Online transfer service."""

import os
import runpy
import sys

import click


@click.command("globus-online", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def globus_online(args):
    """Interact with Globus Online transfer service."""
    sys.argv = ["pegasus-globus-online"] + list(args)
    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "pegasus-globus-online.py"),
        run_name="__main__",
    )
