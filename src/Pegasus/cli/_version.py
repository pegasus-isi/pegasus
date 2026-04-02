"""pegasus version — Print Pegasus version information."""

import click

from Pegasus.cli._java import get_system_properties, run_java_tool


@click.command("version", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def version(args):
    """Print Pegasus version information."""
    run_java_tool(
        main_class="edu.isi.pegasus.planner.client.VersionNumber",
        args=args,
        system_properties=get_system_properties(),
    )
