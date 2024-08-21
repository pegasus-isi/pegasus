"""
Pegasus utility for reporting successful and failed jobs

Usage: pegasus-newstatistics [options] [[submitdir ..] | [workflow_uuid ..]]

"""

import logging
import typing as t

import click

from Pegasus.statistics import PegasusStatistics


def _arg_split(ctx, param, value):
    if not value.strip():
        value = param.default

    input_levels = {v.strip().lower() for v in value.split(",")}
    valid_levels = {
        "all",
        "summary",
        "wf_stats",
        "jb_stats",
        "tf_stats",
        "ti_stats",
        "int_stats",
    }
    invalid_levels = input_levels - valid_levels

    if invalid_levels:
        click.secho(
            f"Invalid value(s) for statistics_level, ignoring <{','.join(invalid_levels)}>",
            err=True,
        )

    return input_levels - invalid_levels


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-o",
    "--output",
    "output_dir",
    type=click.Path(file_okay=False, dir_okay=True),
    help="Writes the output to given directory.",
)
@click.option(
    "-f",
    "--file",
    "filetype",
    default=PegasusStatistics.file_type_txt,
    type=click.Choice(
        (PegasusStatistics.file_type_txt, PegasusStatistics.file_type_csv),
        case_sensitive=False,
    ),
    help="Select output file type. Valid values are 'text' and 'csv'. Default is '%default'.",
)
@click.option(
    "-c",
    "--conf",
    "config_properties",
    default=None,
    type=click.Path(file_okay=True, dir_okay=False, readable=True),
    help="Specifies the properties file to use. This option overrides all other property files.",
)
@click.option(
    "-s",
    "--statistics-level",
    default="summary",
    callback=_arg_split,
    help="Comma separated list. Valid levels are: all,summary,wf_stats,jb_stats,tf_stats,ti_stats,int_stats; Default is '%default'.",
)
@click.option(
    "-t",
    "--time-filter",
    default="day",
    type=click.Choice(("day", "hour"), case_sensitive=False),
    help="Valid levels are: day,hour; Default is '%default'.",
)
@click.option(
    "-i",
    "--ignore-db-inconsistency",
    "ignore_db_inconsistency",
    is_flag=True,
    help="turn off the check for db consistency",
)
@click.option(
    "-v",
    "--verbose",
    "verbose",
    default=0,
    count=True,
    help="Increase verbosity, repeatable",
)
@click.option(
    "-q",
    "--quiet",
    "quiet",
    default=0,
    count=True,
    help="Decrease verbosity, repeatable",
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
@click.argument("submit-dirs", required=False, nargs=-1)
@click.pass_context
def pegasus_statistics(
    ctx,
    output_dir: str = PegasusStatistics.default_output_dir,
    filetype: str = PegasusStatistics.file_type_txt,
    config_properties=None,
    statistics_level: set = {"summary"},
    time_filter: str = "day",
    ignore_db_inconsistency: bool = False,
    verbose: int = 0,
    quiet: int = 0,
    multiple_wf: bool = False,
    is_pmc: bool = False,
    is_uuid: bool = False,
    submit_dirs: t.Sequence[str] = [],
):
    """A tool to generate statistics about the workflow run."""
    s = PegasusStatistics(
        output_dir,
        filetype,
        config_properties,
        statistics_level,
        time_filter,
        ignore_db_inconsistency,
        multiple_wf,
        is_pmc,
        is_uuid,
        submit_dirs,
    )
    try:
        s(ctx, verbose, quiet)
    except Exception as e:
        click.echo(f"Error: {e}")
        logging.debug(e, exc_info=True)


if __name__ == "__main__":
    pegasus_statistics()
