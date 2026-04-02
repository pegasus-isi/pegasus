"""pegasus transfer — Multi-protocol file transfer engine."""

import sys

import click


@click.command("transfer", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def transfer(args):
    """Multi-protocol file transfer engine."""
    sys.argv = ["pegasus-transfer"] + list(args)
    from Pegasus import transfer as _transfer

    _transfer.main()
