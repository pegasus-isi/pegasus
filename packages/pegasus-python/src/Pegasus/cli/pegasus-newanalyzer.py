#!/usr/bin/env python3

"""
Pegasus utility for reporting successful and failed jobs

Usage: pegasus-analyzer [options]

"""

##
#  Copyright 2007-2012 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2023 $


import json
import logging
import os
import subprocess
import sys
import traceback

import click

from Pegasus import analyzer

root_logger = logging.getLogger()
prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")  # Name of this program
indent = ""  # the corresponding indent string


class HelpCmd(click.Command):
    def format_usage(self, ctx, formatter):
        click.echo(f"Usage: pegasus-analyzer [options] workflow_directory")


@click.command(cls=HelpCmd, options_metavar="<options>")
@click.pass_context
@click.option(
    "--verbose", "-v", "vb", count=True, help="Increase verbosity, repeatable",
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
    type=click.Path(file_okay=False, dir_okay=True, readable=True, exists=True),
)
def pegasus_analyzer(
    ctx,
    vb,
    input_dir,
    dag_filename,
    use_files,
    run_monitord,
    output_dir,
    top_dir,
    config_properties,
    quiet_mode,
    recurse_mode,
    indent_length,
    print_options,
    strict_mode,
    summary_mode,
    traverse_all,
    debug_job,
    debug_job_local_executable,
    debug_dir,
    workflow_type,
    submit_dir,
    json_mode,
):

    if vb == 0:
        lvl = logging.WARN
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

    for num in range(0, options.indent_length):
        indent += "\t"

    if dag_filename:
        options.input_dir = os.path.abspath(os.path.split(dag_filename)[0])
        # Assume current directory if input dir is empty
        if input_dir == None:
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
        except:
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
        # Run via quering the stampede database
        else:
            analyze = analyzer.AnalyzeDB(options)
            output = analyze.analyze_db(config_properties)
    except analyzer.AnalyzerError as err:
        analyzer.logger.error(err)
        ctx.exit(1)
    except Exception:
        analyzer.logger.error(traceback.format_exc())
        ctx.exit(1)

    if json_mode:
        print(json.dumps(output.as_dict(), indent=2))
    else:
        print_analyzer_output(ctx, options, output)


def print_analyzer_output(ctx, options, output):

    for wf in output.workflows:
        print_output(ctx, options, output.workflows[wf])

    if len(output.get_failed_workflows()) > 0:
        click.secho(
            "{}".format(
                click.style("One or more workflows failed!", fg="red", bold=True)
            )
        )
    else:
        print("Done".center(80, "*"))
        print()
        print("%s: end of status report" % (analyzer.prog_base))
        print()


def print_output(ctx, options, wf):
    """
    This function prints the summary for the analyzer report,
    which is the same for the long and short output versions
    """
    options.input_dir
    counts = wf.jobs

    # PM-1762 add a helpful message in case failed jobs are zero and workflow failed
    if wf.wf_status == "failure" and counts.failed == 0 and not options.use_files:
        print_console(
            "\nIt seems your workflow failed with zero failed jobs. Please check the dagman.out and the monitord.log file in %s"
            % (options.input_dir or options.top_dir)
        )

    # Let's print the results
    print_console()
    summary = "Summary".center(80, "*")
    print_console(summary)
    print_console()
    print_console(" Submit Directory   : %s" % (wf.submit_dir))
    print_console(" Workflow Status    : %s" % (wf.wf_status))

    print_console(
        " Total jobs         : % 6d (%3.2f%%)"
        % (counts.total, 100 * (1.0 * counts.total / (counts.total or 1)))
    )
    print_console(
        " # jobs succeeded   : % 6d (%3.2f%%)"
        % (counts.success, 100 * (1.0 * counts.success / (counts.total or 1)))
    )
    print_console(
        " # jobs failed      : % 6d (%3.2f%%)"
        % (counts.failed, 100 * (1.0 * counts.failed / (counts.total or 1)))
    )
    print_console(
        " # jobs held        : % 6d (%3.2f%%)"
        % (counts.held, 100 * (1.0 * counts.held / (counts.total or 1)))
    )
    print_console(
        " # jobs unsubmitted : % 6d (%3.2f%%)"
        % (counts.unsubmitted, 100 * (1.0 * counts.unsubmitted / (counts.total or 1)))
    )
    if options.use_files:
        if "unknown_jobs_details" in wf.jobs.job_details:
            unknown = len(wf.jobs.job_details["unknown_jobs_details"])
        else:
            unknown = 0
        print_console(
            " # jobs unknown     : % 6d (%3.2f%%)"
            % (unknown, 100 * (1.0 * unknown / (counts.total or 1)))
        )
    print()

    if not options.summary_mode:
        if counts.held > 0:
            print_held_jobs(options, wf.jobs.job_details["held_jobs_details"])

        if "failing_jobs_details" in wf.jobs.job_details:
            print_failing_jobs(options, wf.jobs.job_details["failing_jobs_details"])

        if counts.failed > 0:
            print_failed_jobs(options, wf.jobs.job_details["failed_jobs_details"])

        if "unknown_jobs_details" in wf.jobs.job_details:
            print_unknown_jobs(options, wf.jobs.job_details["unknown_jobs_details"])

    if counts.failed > 0:
        click.secho(
            "{} wf_uuid:{} submit dir:{}".format(
                click.style("Workflow failed :", fg="red", bold=True),
                wf.wf_uuid,
                wf.submit_dir,
            )
        )


def print_held_jobs(options, held_jobs):
    print_console("Held jobs' details".center(80, "*"))
    print_console()

    for job in held_jobs:
        held_job = held_jobs[job]
        # each tuple is max_ji_id, jobid, jobname, reason
        # first two are database id's for debugging
        print_console(job.center(80, "="))
        print_console()
        print_console("submit file            : %s" % (held_job["submit_file"]))
        print_console(
            "last_job_instance_id   : %s" % (held_job["last_job_instance_id"])
        )
        print_console("reason                 : %s" % (held_job["reason"]))
        print_console()


def print_failing_jobs(options, failing_jobs):
    print_console("failing jobs' details".center(80, "*"))
    for my_job in failing_jobs:
        print_job_instance(options, failing[my_job])


def print_failed_jobs(options, failed_jobs):
    print_console("Failed jobs' details".center(80, "*"))

    for my_job in failed_jobs:
        sub_wf_cmd = print_job_instance(options, failed_jobs[my_job])

        # recurse for sub workflow
        if sub_wf_cmd is not None and options.recurse_mode:
            print_console(("Failed Sub Workflow").center(80, "="))
            subprocess.Popen(sub_wf_cmd, shell=True).communicate()[0]
            print_console(("").center(80, "="))


def print_unknown_jobs(options, unknown_jobs):
    print_console("Unknown jobs' details".center(80, "*"))

    for my_job in unknown_jobs:
        print_job_instance(options, unknown_jobs[my_job])


def print_console(stmt=""):
    """
    A utilty function to print to console with the correct indentation
    """
    print(indent + stmt)


def print_job_instance(options, job_instance_info):
    """
    The function prints information related to one job instance retried from the
    stampede database
    """

    sub_wf_cmd = None
    print_console()
    print_console(job_instance_info.job_name.center(80, "="))
    print_console()
    print_console(" last state: %s" % (job_instance_info.state or "-"))
    print_console("       site: %s" % (job_instance_info.site or "-"))
    print_console("submit file: %s" % (job_instance_info.submit_file or "-"))
    print_console("output file: %s" % (job_instance_info.stdout_file or "-"))
    print_console(" error file: %s" % (job_instance_info.stderr_file or "-"))
    if options.print_invocation:
        print_console()
        print_console(
            "To re-run this job, use: %s %s"
            % ((job_instance_info.executable or "-"), (job_instance_info.argv or "-"))
        )
        print_console()
    if options.print_pre_script and len(job_instance_info.pre_executable or "") > 0:
        print_console()
        print_console("SCRIPT PRE:")
        print_console(
            "%s %s"
            % (
                (job_instance_info.pre_executable or ""),
                (job_instance_info.pre_argv or ""),
            )
        )
        print_console()

    if job_instance_info.subwf_dir != "-":
        # This job has a sub workflow
        user_cmd = " %s" % (prog_base)

        # get any options that need to be invoked for the sub workflow
        # extraOptions = addon(options)
        extraOptions = ""

        if options.use_files:
            if self.options.output_dir is not None:
                user_cmd = user_cmd + " --output-dir %s" % (self.options.output_dir)

            # get any options that need to be invoked for the sub workflow

            sub_wf_cmd = "{} {} -d {}".format(
                user_cmd, extraOptions, os.path.split(job_instance_info.subwf_dir)[0],
            )

        else:
            my_wfdir = os.path.normpath(job_instance_info.subwf_dir)
            if my_wfdir.find(job_instance_info.submit_dir) >= 0:
                # Path to dagman_out file includes original submit_dir, let's try to change it...
                my_wfdir = os.path.normpath(
                    my_wfdir.replace((job_instance_info.submit_dir + os.sep), "", 1)
                )
                my_wfdir = os.path.join(options.input_dir, my_wfdir)

            sub_wf_cmd = "{} {} -d {} --top-dir {}".format(
                user_cmd,
                extraOptions,
                my_wfdir,
                (options.top_dir or options.input_dir),
            )
        if not options.recurse_mode:
            # we print only if recurse mode is disabled
            print_console(" This job contains sub workflows!")
            print_console(" Please run the command below for more information:")
            print_console(sub_wf_cmd)
        print()
    print()

    # print all the tasks associated with the job
    print_tasks_info(options, job_instance_info)
    return sub_wf_cmd


def print_tasks_info(options, job_instance_info):
    """
    The function prints information related tasks relevant to one job instance
    """

    # Unquote stdout and stderr
    ji_stdout_text = job_instance_info.stdout_text
    ji_stderr_text = job_instance_info.stderr_text
    job_tasks = job_instance_info.tasks

    some_tasks_failed = False
    for task in job_tasks:
        my_task = job_tasks[task]
        some_tasks_failed = some_tasks_failed or (my_task.exitcode != 0)

    # PM-798 track whether we need to actually print the condor job stderr or not
    # we only print if there is no information in the kickstart record
    my_print_job_stderr = True

    # Now, print task information
    for task in job_tasks:
        my_task = job_tasks[task]

        if my_task.exitcode == 0 and some_tasks_failed:
            # Skip tasks that succeeded only if we know some tasks did fail
            continue

        # Print task summary
        print_console(
            ("Task #" + str(my_task.task_submit_seq) + " - Summary").center(80, "-")
        )
        print_console()
        print_console("site        : %s" % (job_instance_info.site))
        print_console("hostname    : %s" % (job_instance_info.hostname))
        print_console("executable  : %s" % (my_task.executable))
        print_console("arguments   : %s" % (my_task.arguments))
        print_console("exitcode    : %s" % (my_task.exitcode))
        print_console("working dir : %s" % (job_instance_info.work_dir))
        print_console()

        if not options.quiet_mode:
            stdout_output = (
                "Task #"
                + str(my_task.task_submit_seq)
                + " - "
                + str(my_task.transformation)
                + " - "
                + str(my_task.abs_task_id)
                + " - stdout"
            ).center(80, "-")

            if options.use_files and ji_stdout_text != "-":
                if ji_stdout_text:
                    my_print_job_stderr = False
                    print_console(stdout_output)
                    print_console(ji_stdout_text)
            else:
                # Now, print task stdout and stderr, if anything is there
                my_stdout_str = "#@ %d stdout" % (my_task.task_submit_seq)
                my_stderr_str = "#@ %d stderr" % (my_task.task_submit_seq)

                # Start with stdout
                my_stdout_start = ji_stdout_text.find(my_stdout_str)
                if my_stdout_start >= 0:
                    my_stdout_start = my_stdout_start + len(my_stdout_str) + 1
                    my_stdout_end = ji_stdout_text.find("#@", my_stdout_start)
                    if my_stdout_end < 0:
                        # Next comment not found, possibly the last entry
                        my_stdout_end = len(ji_stdout_text)
                    else:
                        my_stdout_end = my_stdout_end - 1

                    if my_stdout_end - my_stdout_start > 0:
                        # Something to display
                        my_print_job_stderr = False
                        print_console(stdout_output)
                        print_console()
                        print_console(ji_stdout_text[my_stdout_start:my_stdout_end])
                        print_console()

                # Now print stderr (from the kickstart output file)
                my_stderr_start = ji_stdout_text.find(my_stderr_str)
                if my_stderr_start >= 0:
                    my_stderr_start = my_stderr_start + len(my_stderr_str) + 1
                    my_stderr_end = ji_stdout_text.find("#@", my_stderr_start)
                    if my_stderr_end < 0:
                        # Next comment not found, possibly the last entry
                        my_stderr_end = len(ji_stdout_text)
                    else:
                        my_stderr_end = my_stderr_end - 1

                    if my_stderr_end - my_stderr_start > 0:
                        # Something to display
                        my_print_job_stderr = False
                        print_console(
                            (
                                "Task #"
                                + str(my_task.task_submit_seq)
                                + " - "
                                + str(my_task.transformation)
                                + " - "
                                + str(my_task.abs_task_id)
                                + " - Kickstart stderr"
                            ).center(80, "-")
                        )
                        print_console()
                        print_console(ji_stdout_text[my_stderr_start:my_stderr_end])
                        print_console()

                # PM-808 print jobinstance stdout for prescript failures only.
                if my_task.task_submit_seq == -1 and ji_stdout_text is not None:
                    print_console(
                        (
                            "Task #"
                            + str(my_task.task_submit_seq)
                            + " - "
                            + str(my_task.transformation)
                            + " - "
                            + str(my_task.abs_task_id)
                            + " - stdout"
                        ).center(80, "-")
                    )
                    print_console()
                    print_console(ji_stdout_text)
                    print_console()

    # Now print the stderr output from the .err file
    if ji_stderr_text and ji_stderr_text.strip("\n\t \r") != "" and my_print_job_stderr:
        # Something to display
        # if task exitcode is 0, it should indicate PegasusLite case compute job succeeded but stageout failed
        print_console(
            ("Job stderr - %s" % (job_instance_info.job_name or "-")).center(80, "-")
        )
        print_console()
        print_console(ji_stderr_text)
        print_console()


if __name__ == "__main__":
    pegasus_analyzer()
