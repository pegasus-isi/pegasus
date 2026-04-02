"""pegasus db-admin — Administer Pegasus workflow database."""

import sys

import click


@click.command("db-admin", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def db_admin(args):
    """Administer Pegasus workflow database."""
    sys.argv = ["pegasus-db-admin"] + list(args)
    from Pegasus.db.admin import main
    main()
