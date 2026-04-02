"""pegasus s3 — S3 operations (ls, get, put, cp, mkdir, rm)."""

import sys

import click


@click.command("s3", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def s3(args):
    """S3 operations (ls, get, put, cp, mkdir, rm)."""
    sys.argv = ["pegasus-s3"] + list(args)
    from Pegasus import s3 as _s3

    _s3.main()
