"""pegasus init — Initialize a new Pegasus project."""

import sys

import click


@click.command("init", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def init(args):
    """Initialize a new Pegasus project."""
    sys.argv = ["pegasus-init"] + list(args)
    from Pegasus.init import main
    main()
