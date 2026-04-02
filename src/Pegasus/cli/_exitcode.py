"""pegasus exitcode — Parse and analyze job exit codes."""

import sys

import click


@click.command("exitcode", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def exitcode(args):
    """Parse and analyze job exit codes."""
    from Pegasus import exitcode as _exitcode
    _exitcode.main(list(args))
