"""pegasus integrity — Verify file integrity."""

import os
import runpy
import sys

import click


@click.command("integrity", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def integrity(args):
    """Verify file integrity."""
    sys.argv = ["pegasus-integrity"] + list(args)
    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "pegasus-integrity.py"),
        run_name="__main__",
    )
