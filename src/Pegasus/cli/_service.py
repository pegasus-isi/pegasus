"""pegasus service — Start the Pegasus web service."""

import sys

import click


@click.command("service", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def service(args):
    """Start the Pegasus web service."""
    sys.argv = ["pegasus-service"] + list(args)
    from Pegasus.service.server import main
    main()
