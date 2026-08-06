#!/usr/bin/env python3

"""
Pegasus utility for reporting successful and failed jobs

Usage: pegasus-analyzer [options]
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

import click

from Pegasus import analyzer_report
from Pegasus.client import agent, analyzer

root_logger = logging.getLogger()


class HelpCmd(click.Command):
    def format_usage(self, ctx, formatter):
        click.echo("Usage: pegasus-analyzer [options] workflow_directory")


@click.command(cls=HelpCmd, options_metavar="<options>")
@click.pass_context
@click.option(
    "--verbose",
    "-v",
    "vb",
    count=True,
    help="Increase verbosity, repeatable",
)
@click.option(
    "-i",
    "-d",
    "--input-dir",
    "input_dir",
    required=False,
    metavar="INPUT_DIR",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    help="Input directory where the jobstate.log file is located, default is the current directory",
)
@click.option(
    "--dag",
    "dag_filename",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, exists=True),
    required=False,
    metavar="DAG_FILENAME",
    help="Full path to the dag file to use -- this option overrides the -d option",
)
@click.option(
    "-f",
    "--files",
    "use_files",
    is_flag=True,
    default=False,
    help="Disables the database mode and forces the use of workflow directory files",
)
@click.option(
    "-m",
    "-t",
    "--monitord",
    "run_monitord",
    is_flag=True,
    help="Run pegasus-monitord before analyzing the output",
)
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
    "--top-dir",
    "top_dir",
    required=False,
    metavar="TOP_DIR",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    help="Provides the location of the top-level workflow directory, needed to analyze sub-workflows",
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
    "-q",
    "--quiet",
    "quiet_mode",
    is_flag=True,
    help="Output out/err filenames instead of their contents",
)
@click.option(
    "-r",
    "--recurse",
    "recurse_mode",
    is_flag=True,
    help="Automatically recurse into sub workflows in case of failure",
)
@click.option(
    "-I",
    "--indent",
    "indent_length",
    type=int,
    metavar="INDENT_LENGTH",
    default=0,
    help="Dictates the number of white spaces to use when indenting the output",
)
@click.option(
    "-p",
    "--print",
    "print_options",
    type=str,
    metavar="PRINT_OPTIONS",
    help="Specifies print options from pre, invocation or all",
)
@click.option(
    "-s",
    "--strict",
    "strict_mode",
    is_flag=True,
    help="gets a job's out and err files from the submit file",
)
@click.option(
    "-S",
    "--summary",
    "summary_mode",
    is_flag=True,
    help="Just print the summary and exit",
)
@click.option(
    "-T",
    "--traverse-all",
    "traverse_all",
    is_flag=True,
    help="Traverse through all sub workflows for this workflow in the database",
)
@click.option(
    "--debug-job",
    "debug_job",
    type=str,
    metavar="DEBUG_JOB",
    help="Specifies a job to debug (can be either the job base name or the submit file name) -- this option enables debugging a single pegasus lite job",
)
@click.option(
    "--local-executable",
    "debug_job_local_executable",
    type=str,
    metavar="DEBUG_JOB_LOCAL_EXECUTABLE",
    help="The path to the local user application that pegasus-lite job refers to.",
)
@click.option(
    "--debug-dir",
    "debug_dir",
    required=False,
    metavar="DEBUG_DIR",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
    help="Specifies the directory to use as debug directory (default is to create a random directory in /tmp)",
)
@click.option(
    "--type",
    "workflow_type",
    type=str,
    metavar="WORKFLOW_TYPE",
    help="Specifies what type of workflow we are debugging (available types: condor)",
)
@click.option(
    "-j",
    "--json",
    "json_mode",
    is_flag=True,
    help="Returns job info in a structured format",
)
@click.argument(
    "submit-dir",
    required=False,
    default=".",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
)
@click.option(
    "--ai",
    "ai",
    type=bool,
    default=True,
    show_default=True,
    help="Pegasus AI-based analysis of the workflow",
)
@click.option(
    "--plain/--no-plain",
    "plain",
    default=False,
    help="Emit raw Markdown even on a terminal (no Rich rendering)",
)
def pegasus_analyzer(
    ctx: click.Context,
    vb: int,
    input_dir: str | None,
    dag_filename: str | None,
    use_files: bool,
    run_monitord: bool,
    output_dir: str | None,
    top_dir: str | None,
    config_properties: str | None,
    quiet_mode: bool,
    recurse_mode: bool,
    indent_length: int,
    print_options: str | None,
    strict_mode: bool,
    summary_mode: bool,
    traverse_all: bool,
    debug_job: str | None,
    debug_job_local_executable: str | None,
    debug_dir: str | None,
    workflow_type: str | None,
    submit_dir: str | None,
    json_mode: bool,
    ai: bool,
    plain: bool,
) -> None:

    if vb == 0:
        lvl = logging.WARNING
    elif vb == 1:
        lvl = logging.INFO
    else:
        lvl = logging.DEBUG
    root_logger.setLevel(lvl)

    # Initializes user options data class used to run analyzer
    options = analyzer.Options(
        debug_dir=debug_dir,
        debug_job_local_executable=debug_job_local_executable,
        workflow_type=workflow_type,
        run_monitord=run_monitord,
        strict_mode=strict_mode,
        summary_mode=summary_mode,
        quiet_mode=quiet_mode,
        recurse_mode=recurse_mode,
        traverse_all=traverse_all,
        json_mode=json_mode,
        indent_length=indent_length,
        use_files=use_files,
    )

    if print_options is not None:
        my_options = print_options.split(",")
        if "pre" in my_options or "all" in my_options:
            options.print_pre_script = True
        if "invocation" in my_options or "all" in my_options:
            options.print_invocation = True

    if top_dir is not None:
        options.top_dir = os.path.abspath(top_dir)

    if debug_job is not None:
        options.debug_job = debug_job
        # Enables the debugging mode
        options.debug_mode = True

    if dag_filename:
        options.input_dir = os.path.abspath(os.path.split(dag_filename)[0])
        # Assume current directory if input dir is empty
        if input_dir is None:
            options.input_dir = os.getcwd()
    else:
        # Select directory where jobstate.log is located
        if input_dir:
            options.input_dir = os.path.abspath(input_dir)
        elif submit_dir:
            options.input_dir = submit_dir
        else:
            options.input_dir = os.getcwd()

    if options.debug_mode == 1:
        # Enter debug mode if job name given
        try:
            debug = analyzer.DebugWF(options)
            debug.debug_workflow()
            ctx.exit(0)
        except Exception:
            ctx.exit(1)

    # sanity check
    if recurse_mode and traverse_all:
        analyzer.logger.error(
            "Options --recurse and --traverse-all are mutually exclusive. Please specify only one of these options"
        )
        ctx.exit(1)

    # Run the analyzer
    try:
        # Run via the files option (using jobstate.log, dag file etc)
        if use_files:
            analyze = analyzer.AnalyzeFiles(options)
            output = analyze.analyze_files()
        # Run via querying the stampede database
        else:
            analyze = analyzer.AnalyzeDB(options)
            output = analyze.analyze_db(config_properties)
    except analyzer.AnalyzerError as err:
        analyzer.logger.error(err)
        ctx.exit(1)
    except Exception:
        analyzer.logger.error(traceback.format_exc())
        ctx.exit(1)

    # generate the console output - we might need it later for the ai
    console = analyzer_report.make_console()
    if json_mode:
        console_output = json.dumps(output.as_dict(), indent=2)
        print(console_output)
    else:
        # Single Markdown source of truth: rendered on a terminal, raw when
        # redirected/piped, and sent verbatim to the AI agent.
        console_output = analyzer_report.build_report_markdown(options, output)
        analyzer_report.emit(
            console, console_output, plain=plain, indent_length=options.indent_length
        )
    print()

    if ai:
        # Show the section header up front -- the agent call can take a while,
        # so the response is emitted separately once it returns.
        analyzer_report.emit(
            console,
            "## Pegasus AI Analysis\n",
            plain=plain,
            indent_length=options.indent_length,
        )
        # The response is a separate render, so add the blank line that Rich would
        # otherwise place after a heading followed by content in the same document.
        console.print()
        agent_output = None
        try:
            agent_output = agent.AgentClient().analyze(
                output.root_wf_uuid, console_output
            )
        except Exception as e:
            analyzer.logger.error(
                f"Error occurred while calling Pegasus Agent for AI analysis: {e}"
            )
        if agent_output is not None:
            analyzer_report.emit(
                console,
                agent_output + "\n",
                plain=plain,
                indent_length=options.indent_length,
            )
        print()

    sys.exit(0)


if __name__ == "__main__":
    pegasus_analyzer()
