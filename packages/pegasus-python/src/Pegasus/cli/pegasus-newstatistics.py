"""
Pegasus utility for reporting successful and failed jobs

Usage: pegasus-newstatistics [options] [[submitdir ..] | [workflow_uuid ..]]

"""

import json
import logging
import os
import subprocess
import sys
import traceback

import click

from Pegasus import statistics

root_logger = logging.getLogger()
prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")  # Name of this program
indent = ""  # the corresponding indent string


class HelpCmd(click.Command):
    def format_usage(self, ctx, formatter):
        click.echo(f"Usage: pegasus-newstatistics [options] [[submitdir ..] | [workflow_uuid ..]]")

@click.command(cls=HelpCmd, options_metavar="<options>")
@click.pass_context
@click.option(
    "--verbose", "-v", "vb", count=True, help="Increase verbosity, repeatable",
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
    "-m",
    "--multiple-wf",
    action="store_true",
    dest="multiple_wf",
    default=False,
    help="Calculate statistics for multiple workflows",
)
@click.option(
    "-u",
    "--isuuid",
    action="store_true",
    dest="is_uuid",
    default=False,
    help="Set if the positional arguments are wf uuids",
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
def pegasus_statistics(
    ctx,
    vb,
    filetype,
    output_dir,
    config_properties,
    ignore_db_inconsistency,
    multiple_wf,
    is_pmc,
    is_uuid,
    quiet_mode,
    json_mode,
):

    # Parse command line options
    (options, args) = parser.parse_args()

     statistics_level = {
        t.lower().strip() for t in  statistics_level.split(",")
    }

    if not  statistics_level:
         statistics_level = {"summary"}

    sl =  statistics_level - {
        "all",
        "summary",
        "wf_stats",
        "jb_stats",
        "tf_stats",
        "ti_stats",
        "int_stats",
    }
    if sl:
        sys.stderr.write(
            "Invalid value(s) for statistics_level, ignoring %s\n" % ",".join(sl)
        )

    # Multiple workflow is set to true if there are multiple positional arguments.
    multiple_wf =  multiple_wf

    if len(args) < 1:
        # * means all workflows in the database, and . means current directory
        submit_dir = "."
        if multiple_wf:
            submit_dir = "*"
    elif len(args) > 1:
         multiple_wf = True
        multiple_wf = True
        submit_dir = args
    else:
         multiple_wf = False
        multiple_wf = False
        submit_dir = args[0]

    log_level =  verbose -  quiet
    if log_level < 0:
        root_logger.setLevel(logging.ERROR)
    elif log_level == 0:
        root_logger.setLevel(logging.WARNING)
    elif log_level == 1:
        root_logger.setLevel(logging.INFO)
    elif log_level > 1:
        root_logger.setLevel(logging.DEBUG)

    def check_dump(dir):
        if not os.path.isfile(
            os.path.join(dir, "braindump.yml")
        ) and not os.path.isfile(os.path.join(dir, "braindump.txt")):
            sys.stderr.write("Not a workflow submit directory: %s\n" % submit_dir)
            ctx.exit(1)

    if multiple_wf:
        # Check for braindump file's existence if workflows are not specified as UUIDs and
        # statistics need to be calculated only on a sub set of workflows
        if not  is_uuid and submit_dir != "*":
            for dir in submit_dir:
                check_dump(dir)
    else:
        if not  is_uuid:
            check_dump(submit_dir)

    if  ignore_db_inconsistency:
        logger.warning("Ignoring db inconsistency")
        logger.warning(
            "The tool is meant to be run after the completion of workflow run."
        )
    else:

        def loading_complete(dir):
            if not utils.loading_completed(dir):
                if utils.monitoring_running(dir):
                    sys.stderr.write(
                        "pegasus-monitord still running. Please wait for it to complete.\n"
                    )
                else:
                    sys.stderr.write("Please run pegasus monitord in replay mode.\n")
                ctx.exit(1)

        if multiple_wf:
            if submit_dir == "*":
                logger.warning(
                    "Statistics have to be calculated on all workflows. Tool cannot check to see if all of them have finished. Ensure that all workflows have finished"
                )

            if not  is_uuid and submit_dir != "*":
                for dir in submit_dir:
                    loading_complete(dir)
        else:
            if not  is_uuid:
                loading_complete(submit_dir)

    # Figure out what statistics we need to calculate
    sl =  statistics_level
    logger.info("Statistics level is %s" % sl)
    if "all" in sl:
        calc_wf_stats = True
        calc_wf_summary = True
        calc_tf_stats = True
        calc_int_stats = True
        calc_ti_stats = True
        if not multiple_wf:
            calc_jb_stats = True

    if "summary" in sl:
        calc_wf_summary = True

    if "wf_stats" in sl:
        calc_wf_stats = True

    if "jb_stats" in sl:
        if multiple_wf:
            logger.fatal(
                "Job breakdown statistics cannot be computed over multiple workflows"
            )
            ctx.exit(1)
        calc_jb_stats = True

    if "tf_stats" in sl:
        calc_tf_stats = True

    if "ti_stats" in sl:
        calc_ti_stats = True

    if "int_stats" in sl:
        calc_int_stats = True

    global file_type
    file_type =  filetype
    logger.info("File type is %s" % file_type)

    global time_filter
    time_filter =  time_filter
    logger.info("Time filter is %s" % time_filter)

    # Change the legend to show the time filter format
    tf_format = str(stats_utils.get_date_print_format(time_filter))

    time_stats_col_name_text[0] += tf_format
    time_stats_col_name_csv[1] += tf_format
    time_host_stats_col_name_text[0] += tf_format
    time_host_stats_col_name_csv[1] += tf_format

    if  output_dir:
        delete_if_exists = False
        output_dir =  output_dir
    else:
        delete_if_exists = True
        if multiple_wf or  is_uuid:
            sys.stderr.write(
                "Output directory option is required when calculating statistics over multiple workflows.\n"
            )
            ctx.exit(1)
        else:
            output_dir = os.path.join(submit_dir, DEFAULT_OUTPUT_DIR)

    logger.info("Output directory is %s" % output_dir)
    utils.create_directory(output_dir, delete_if_exists=delete_if_exists)

    global uses_PMC

    def use_pmc(dir):
        braindb = utils.slurp_braindb(dir)
        return braindb["uses_pmc"] is True

    if  is_pmc:
        logger.info("Calculating statistics with use of PMC clustering")
        uses_PMC = True
    else:
        if  is_uuid:
            # User provided workflow UUID
            logger.info("Workflows are specified as UUIDs and ispmc option is not set.")
            uses_PMC = False
        else:
            # User provided workflow submit directories
            if multiple_wf:
                if submit_dir == "*":
                    logger.info(
                        "Calculating statistics over all workflows, and ispmc option is not set."
                    )
                else:
                    # int(True) -> 1
                    tmp = sum(int(use_pmc(dir)) for dir in submit_dir)

                    # All workflow are either PMC or non PMC workflows?
                    if tmp == len(submit_dir) or tmp == 0:
                        uses_PMC = use_pmc(submit_dir[0])
                    else:
                        uses_PMC = False
                        logger.warn(
                            "Input workflows use both PMC & regular clustering! Calculating statistics with regular clustering"
                        )

            else:
                uses_PMC = use_pmc(submit_dir)

    # Check db_url, and get wf_uuid's
    if multiple_wf:
        if  is_uuid or submit_dir == "*":
            # URL picked from config_properties file.
            output_db_url = connection.url_by_properties(
                 config_properties, connection.DBType.WORKFLOW
            )
            wf_uuid = submit_dir

            if not output_db_url:
                logger.error(
                    'Unable to determine database URL. Kindly specify a value for "pegasus.monitord.output" property'
                )
                ctx.exit(1)
        else:
            db_url_set = set()
            wf_uuid = []

            for dir in submit_dir:
                db_url = connection.url_by_submitdir(
                    dir, connection.DBType.WORKFLOW,  config_properties
                )
                uuid = connection.get_wf_uuid(dir)
                db_url_set.add(db_url)
                wf_uuid.append(uuid)

            if len(db_url_set) != 1:
                logger.error(
                    "Workflows are distributed across multiple databases, which is not supported"
                )
                ctx.exit(1)

            output_db_url = db_url_set.pop()

    else:
        if  is_uuid:
            output_db_url = connection.url_by_properties(
                 config_properties, connection.DBType.WORKFLOW
            )
            wf_uuid = submit_dir

            if not output_db_url:
                logger.error(
                    'Unable to determine database URL. Kindly specify a value for "pegasus.monitord.output" property'
                )
                ctx.exit(1)
        else:
            output_db_url = connection.url_by_submitdir(
                submit_dir, connection.DBType.WORKFLOW,  config_properties
            )
            wf_uuid = connection.get_wf_uuid(submit_dir)

    logger.info("DB URL is: %s" % output_db_url)
    logger.info("workflow UUID is: %s" % wf_uuid)

    if output_db_url is not None:
        errors = statistics.print_workflow_details(
            output_db_url, wf_uuid, output_dir, multiple_wf=multiple_wf
        )

        if errors:
            logger.error("Failed to generate %d type(s) of statistics" % errors)
            ctx.exit(1)


######## Fucntions for rendering the reveived output on screen #############

def formatted_wf_summary_legends_part1():
    return """
#
# Pegasus Workflow Management System - http://pegasus.isi.edu
#
# Workflow summary:
#   Summary of the workflow execution. It shows total
#   tasks/jobs/sub workflows run, how many succeeded/failed etc.
#   In case of hierarchical workflow the calculation shows the
#   statistics across all the sub workflows.It shows the following
#   statistics about tasks, jobs and sub workflows.
#     * Succeeded - total count of succeeded tasks/jobs/sub workflows.
#     * Failed - total count of failed tasks/jobs/sub workflows.
#     * Incomplete - total count of tasks/jobs/sub workflows that are
#       not in succeeded or failed state. This includes all the jobs
#       that are not submitted, submitted but not completed etc. This
#       is calculated as  difference between 'total' count and sum of
#       'succeeded' and 'failed' count.
#     * Total - total count of tasks/jobs/sub workflows.
#     * Retries - total retry count of tasks/jobs/sub workflows.
#     * Total+Retries - total count of tasks/jobs/sub workflows executed
#       during workflow run. This is the cumulative of retries,
#       succeeded and failed count."""


def formatted_wf_summary_legends_part2():
    return """
# Workflow wall time:
#   The wall time from the start of the workflow execution to the end as
#   reported by the DAGMAN.In case of rescue dag the value is the
#   cumulative of all retries.
# Cumulative job wall time:
#   The sum of the wall time of all jobs as reported by kickstart.
#   In case of job retries the value is the cumulative of all retries.
#   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
#   the wall time value includes jobs from the sub workflows as well.
# Cumulative job wall time as seen from submit side:
#   The sum of the wall time of all jobs as reported by DAGMan.
#   This is similar to the regular cumulative job wall time, but includes
#   job management overhead and delays. In case of job retries the value
#   is the cumulative of all retries. For workflows having sub workflow
#   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
#   from the sub workflows as well.
# Cumulative job badput wall time:
#   The sum of the wall time of all failed jobs as reported by kickstart.
#   In case of job retries the value is the cumulative of all retries.
#   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
#   the wall time value includes jobs from the sub workflows as well.
# Cumulative job badput wall time as seen from submit side:
#   The sum of the wall time of all failed jobs as reported by DAGMan.
#   This is similar to the regular cumulative job badput wall time, but includes
#   job management overhead and delays. In case of job retries the value
#   is the cumulative of all retries. For workflows having sub workflow
#   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
#   from the sub workflows as well."""


def formatted_wf_summary_legends_txt():
    return formatted_wf_summary_legends_part1() + formatted_wf_summary_legends_part2()


def formatted_wf_summary_legends_csv1():
    return formatted_wf_summary_legends_part1()


def formatted_wf_summary_legends_csv2():
    return formatted_wf_summary_legends_part2()


def formatted_wf_status_legends():
    return """
# Workflow summary
#   Summary of the workflow execution. It shows total
#   tasks/jobs/sub workflows run, how many succeeded/failed etc.
#   In case of hierarchical workflow the calculation shows the
#   statistics of each individual sub workflow.The file also
#   contains a 'Total' table at the bottom which is the cummulative
#   of all the individual statistics details.t shows the following
#   statistics about tasks, jobs and sub workflows.
#
#     * WF Retries - number of times a workflow was retried.
#     * Succeeded - total count of succeeded tasks/jobs/sub workflows.
#     * Failed - total count of failed tasks/jobs/sub workflows.
#     * Incomplete - total count of tasks/jobs/sub workflows that are
#       not in succeeded or failed state. This includes all the jobs
#       that are not submitted, submitted but not completed etc. This
#       is calculated as  difference between 'total' count and sum of
#       'succeeded' and 'failed' count.
#     * Total - total count of tasks/jobs/sub workflows.
#     * Retries - total retry count of tasks/jobs/sub workflows.
#     * Total+Retries - total count of tasks/jobs/sub workflows executed
#       during workflow run. This is the cumulative of retries,
#       succeeded and failed count.
#
"""


def formatted_job_stats_legends():
    return """
# Job            - name of the job
# Try            - number representing the job instance run count
# Site           - site where the job ran
# Kickstart      - actual duration of the job instance in seconds on the
#                  remote compute node
# Mult           - multiplier factor specified by the user
# Kickstart-Mult - Kickstart time multiplied by the multiplier factor
# CPU-Time       - remote cpu time computed as the stime + utime
# Post           - postscript time as reported by DAGMan
# CondorQTime    - time between submission by DAGMan and the remote Grid
#                  submission. It is an estimate of the time spent in the
#                  condor q on the submit node
# Resource       - time between the remote Grid submission and start of
#                  remote execution. It is an estimate of the time job
#                  spent in the remote queue
# Runtime        - time spent on the resource as seen by Condor DAGMan.
#                  Is always >= Kickstart
# Seqexec        - time taken for the completion of a clustered job
# Seqexec-Delay  - time difference between the time for the completion
#                  of a clustered job and sum of all the individual
#                  tasks Kickstart time
# Exitcode       - exitcode for this job
# Hostname       - name of the host where the job ran, as reported by
#                  Kickstart"""


def formatted_transformation_stats_legends():
    return """
# Transformation   - name of the transformation.
# Type             - successful or failed
# Count            - the number of times the invocations corresponding to
#                    the transformation was executed.
# Min(sec)         - the minimum invocation runtime value corresponding
#                    to the transformation.
# Max(sec)         - the maximum invocation runtime value corresponding
#                    to the transformation.
# Mean(sec)        - the mean of the invocation runtime corresponding
#                    to the transformation.
# Total(sec)       - the cumulative of invocation runtime corresponding
#                    to the transformation.
# Min (mem)        - the minimum of the max. resident set size (RSS) value corresponding
#                    to the transformation. In MB.
# Max (mem)        - the maximum of the max. resident set size (RSS) value corresponding
#                    to the transformation. In MB.
# Mean (mem)       - the mean of the max. resident set size (RSS) value corresponding
#                    to the transformation. In MB.
# Min (avg. cpu)   - the minimum of the average cpu utilization value corresponding
#                    to the transformation.
# Max (avg. cpu)   - the maximum of the average cpu utilization value corresponding
#                    to the transformation.
# Mean (avg. cpu)  - the mean of the average cpu utilization value corresponding
#                    to the transformation."""


def formatted_integrity_stats_legends():
    return """
# Integrity Statistics Breakdown
# Type           - the type of integrity metric.
                   check - means checksum was compared for a file,
                   compute - means a checksum was generated for a file
# File Type      - the type of file: input or output from a job perspective
# Count          - the number of times done
# Total Duration - sum of duration in seconds for the 'count' number
                   of records matching the particular type,
                   file-type combo
"""


def formatted_time_stats_legends_text():
    return """
# Job instance statistics per FILTER:
#   the number of job instances run, total runtime sorted by FILTER
# Invocation statistics per FILTER:
#   the number of invocations , total runtime sorted by FILTER
# Job instance statistics by host per FILTER:
#   the number of job instance run, total runtime on each host sorted by FILTER
# Invocation by host per FILTER:
#   the number of invocations, total runtime on each host sorted by FILTER
""".replace(
        "FILTER", str(time_filter)
    )


def formatted_time_stats_legends_csv():
    return """
# Job instance statistics per FILTER:
#   the number of job instances run, total runtime sorted by FILTER
# Invocation statistics per FILTER:
#   the number of invocations , total runtime sorted by FILTER
""".replace(
        "FILTER", str(time_filter)
    )


def formatted_time_host_stats_legends_csv():
    return """
# Job instance statistics by host per FILTER:
#   the number of job instance run, total runtime on each host sorted by FILTER
# Invocation by host per FILTER:
#   the number of invocations, total runtime on each host sorted by FILTER
""".replace(
        "FILTER", str(time_filter)
    )


if __name__ == "__main__":
    pegasus_statistics()
