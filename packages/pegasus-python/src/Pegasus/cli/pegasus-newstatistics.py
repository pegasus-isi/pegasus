"""
Pegasus utility for reporting successful and failed jobs

Usage: pegasus-newstatistics [options] [[submitdir ..] | [workflow_uuid ..]]

"""

import logging
import os
import sys

import click

from Pegasus import statistics

root_logger = logging.getLogger()
prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")  # Name of this program
st_lvls = {
    "all",
    "summary",
    "wf_stats",
    "jb_stats",
    "tf_stats",
    "ti_stats",
    "int_stats",
}


class HelpCmd(click.Command):
    def format_usage(self, ctx, formatter):
        click.echo(
            f"Usage: pegasus-newstatistics [options] [[submitdir ..] | [workflow_uuid ..]]"
        )


@click.command(cls=HelpCmd, options_metavar="<options>")
@click.pass_context
@click.option(
    "-o",
    "--output-dir",
    "output_dir",
    required=False,
    metavar="OUTPUT_DIR",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    help="Provides an output directory for all monitord log files",
)
@click.option(
    "--verbose", "-v", "verbose", count=True, help="Increase verbosity, repeatable",
)
@click.option(
    "--quiet", "-q", "quiet", count=True, help="Decrease verbosity, repeatable",
)
@click.option(
    "-f",
    "--file",
    "filetype",
    required=False,
    default="txt",
    metavar="FILE_TYPE",
    type=click.Choice(["txt", "csv"], case_sensitive=False),
    help="Select output file type. Valid values are 'text' and 'csv'. Default is 'txt'.",
)
@click.option(
    "-c",
    "--conf",
    "config_properties",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, exists=True),
    required=False,
    metavar="CONFIG_PROPERTIES_FILE",
    help="Specifies the properties file to use. This overrides all other property files.",
)
@click.option(
    "-s",
    "--statistics-level",
    "stats_level",
    default="summary",
    help="Comma separated list. Valid levels are: all,summary,wf_stats,jb_stats,tf_stats,ti_stats,int_stats; Default is 'summary'.",
)
@click.option(
    "-t",
    "--time-filter",
    "time_filter",
    default="day",
    type=click.Choice(["day", "hour"], case_sensitive=False),
    help="Valid levels are: day,hour; Default is 'day'.",
)
@click.option(
    "-i",
    "--ignore-db-inconsistency",
    "ignore_db_inconsistency",
    is_flag=True,
    help="turn off the check for db consistency",
)
@click.option(
    "-m",
    "--multiple-wf",
    "multiple_wf",
    is_flag=True,
    help="Calculate statistics for multiple workflows",
)
@click.option(
    "-p",
    "--ispmc",
    "is_pmc",
    is_flag=True,
    help="Calculate statistics for workflows which use PMC",
)
@click.option(
    "-u",
    "--isuuid",
    "is_uuid",
    is_flag=True,
    help="Set if the positional arguments are wf uuids",
)
@click.argument(
    "arguments",
    required=False,
    nargs=-1,
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
)
def pegasus_statistics(
    ctx,
    output_dir,
    verbose,
    quiet,
    filetype,
    config_properties,
    stats_level,
    time_filter,
    ignore_db_inconsistency,
    multiple_wf,
    is_pmc,
    is_uuid,
    arguments,
):

    options = statistics.Options(
        output_dir=output_dir,
        verbose=verbose,
        quiet=quiet,
        file_type=filetype,
        config_properties=config_properties,
        statistics_level=stats_level,
        time_filter=time_filter,
        ignore_db_inconsistency=ignore_db_inconsistency,
        multiple_wf=multiple_wf,
        uses_PMC=is_pmc,
        is_uuid=is_uuid,
    )

    log_level = options.verbose - options.quiet
    if log_level < 0:
        root_logger.setLevel(logging.ERROR)
    elif log_level == 0:
        root_logger.setLevel(logging.WARNING)
    elif log_level == 1:
        root_logger.setLevel(logging.INFO)
    elif log_level > 1:
        root_logger.setLevel(logging.DEBUG)

    sl = set(stats_level.lower().split(","))

    if sl - st_lvls:
        click.secho(
            "{}Invalid value(s) for statistics_level! Ignoring: {}\n".format(
                click.style("Warning: ", bold=True, fg="yellow"),
                click.style(",".join(sl - st_lvls), bold=True),
            )
        )

    root_logger.info("Statistics level is %s" % sl)

    if "summary" in sl or "all" in sl:
        options.calc_wf_summary = True
    if "wf_stats" in sl or "all" in sl:
        options.calc_wf_stats = True
    if "jb_stats" in sl or "all" in sl:
        if multiple_wf and "all" not in sl:
            click.secho(
                "{}{}".format(
                    click.style("Error: ", fg="red", bold=True),
                    "Job breakdown statistics cannot be computed over multiple workflows!",
                )
            )
            ctx.exit(1)
        options.calc_jb_stats = True
    if "tf_stats" in sl or "all" in sl:
        options.calc_tf_stats = True
    if "ti_stats" in sl or "all" in sl:
        options.calc_ti_stats = True
    if "int_stats" in sl or "all" in sl:
        options.calc_int_stats = True

    # print(options.json)

    statistics.get_stats(options, list(arguments))


if __name__ == "__main__":
    pegasus_statistics()
