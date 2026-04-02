"""pegasus aws-batch — Command-line client for AWS Batch."""

import click

from Pegasus.cli._java import get_system_properties, run_java_tool


@click.command("aws-batch", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
    allow_interspersed_args=False,
    help_option_names=[],
))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def aws_batch(args):
    """Command-line client for AWS Batch."""
    run_java_tool(
        main_class="edu.isi.pegasus.aws.batch.client.PegasusAWSBatch",
        args=args,
        system_properties=get_system_properties(),
    )
