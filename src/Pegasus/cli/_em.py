"""pegasus em — Manage ensemble workflows."""

import sys

import click


@click.command("em", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def em(args):
    """Manage ensemble workflows."""
    sys.argv = ["pegasus-em"] + list(args)
    from Pegasus.service.ensembles.commands import main
    main()
