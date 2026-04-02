"""pegasus submitdir — Manage workflow submission directories."""

import sys

import click


@click.command("submitdir", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def submitdir(args):
    """Manage workflow submission directories."""
    sys.argv = ["pegasus-submitdir"] + list(args)
    from Pegasus.submitdir import main
    main()
