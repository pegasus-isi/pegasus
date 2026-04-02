"""pegasus plan — Generate a concrete workflow from an abstract workflow."""

import click

from Pegasus.cli._java import get_system_properties, run_java_tool


@click.command("plan", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def plan(args):
    """Generate a concrete workflow from an abstract workflow."""
    run_java_tool(
        main_class="edu.isi.pegasus.planner.client.CPlanner",
        args=args,
        system_properties=get_system_properties(),
    )
