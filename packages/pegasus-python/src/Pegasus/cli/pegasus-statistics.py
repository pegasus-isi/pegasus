#!/usr/bin/env python3

import logging
import optparse
import os
import re
import sys
import traceback

from Pegasus.db import connection
from Pegasus.db.workflow.stampede_statistics import StampedeStatistics
from Pegasus.db.workflow.stampede_wf_statistics import StampedeWorkflowStatistics
from Pegasus.plots_stats import utils as stats_utils
from Pegasus.tools import utils

root_logger = logging.getLogger()
logger = logging.getLogger("pegasus-statistics")


utils.configureLogging(level=logging.WARNING)

# Regular expressions
re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")

workflow_summary_file_name = "summary"
workflow_summary_time_file_name = "summary-time"
workflow_statistics_file_name = "workflow"
job_statistics_file_name = "jobs"
logical_transformation_statistics_file_name = "breakdown"
logical_integrity_statistics_file_name = "integrity"
time_statistics_file_name = "time"
time_statistics_per_host_file_name = "time-per-host"
text_file_extension = ".txt"
csv_file_extension = ".csv"
calc_wf_stats = False
calc_wf_summary = False
calc_jb_stats = False
calc_tf_stats = False
calc_ti_stats = False
calc_int_stats = False
time_filter = None
NEW_LINE_STR = "\n"
DEFAULT_OUTPUT_DIR = "statistics"
FILE_TYPE_TXT = "text"
FILE_TYPE_CSV = "csv"
uses_PMC = False

# Transformations file column names
transformation_stats_col_name_text = [
    "Transformation",
    "Type",
    "Count",
    "Min (runtime)",
    "Max (runtime)",
    "Mean (runtime)",
    "Total (runtime)",
    "Min (mem)",
    "Max (mem)",
    "Mean (mem)",
    "Min (avg. cpu)",
    "Max (avg. cpu)",
    "Mean (avg. cpu)",
]
transformation_stats_col_name_csv = [
    "Workflow_Id",
    "Dax_Label",
    "Transformation",
    "Type",
    "Count",
    "Min (runtime)",
    "Max (runtime)",
    "Mean (runtime)",
    "Total (runtime)",
    "Min (mem)",
    "Max (mem)",
    "Mean (mem)",
    "Min (avg. cpu)",
    "Max (avg. cpu)",
    "Mean (avg. cpu)",
]

transformation_stats_col_size = [
    15,
    5,
    6,
    14,
    14,
    21,
    22,
    10,
    10,
    11,
    15,
    15,
    16,
]

# Integrity file column names
integrity_stats_col_name_text = ["Type", "File Type", "Count", "Total Duration"]
integrity_stats_col_name_csv = [
    "Workflow_Id",
    "Dax_Label",
    "Type",
    "File Type",
    "Count",
    "Total Duration",
]
integrity_stats_col_size = [10, 15, 10, 25]

# Jobs file column names
job_stats_col_name_text = [
    "Job",
    "Try",
    "Site",
    "Kickstart",
    "Mult",
    "Kickstart-Mult",
    "CPU-Time",
    "Post",
    "CondorQTime",
    "Resource",
    "Runtime",
    "Seqexec",
    "Seqexec-Delay",
    "Exitcode",
    "Hostname",
]
job_stats_col_name_csv = [
    "Workflow_Id",
    "Dax_Label",
    "Job",
    "Try",
    "Site",
    "Kickstart",
    "Mult",
    "Kickstart-Mult",
    "CPU-Time",
    "Post",
    "CondorQTime",
    "Resource",
    "Runtime",
    "Seqexec",
    "Seqexec-Delay",
    "Exitcode",
    "Hostname",
]
job_stats_col_size = [35, 4, 12, 12, 6, 16, 12, 6, 12, 12, 12, 12, 15, 10, 30]

# Summary file column names
workflow_summary_col_name_csv = [
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
]
workflow_summary_col_name_text = [
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
]
workflow_summary_col_size = [15, 10, 8, 12, 10, 10, 13]
workflow_time_summary_col_name_csv = ["stat_type", "time_seconds"]

# Workflow file column names
workflow_status_col_name_text = [
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
    "WF Retries",
]
workflow_status_col_name_csv = [
    "Workflow_Id",
    "Dax_Label",
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
    "WF_Retries",
]
workflow_status_col_size = [15, 11, 10, 12, 10, 10, 15, 10]

# Time file column names
time_stats_col_name_csv = ["stat_type", "date", "count", "runtime (sec)"]
time_stats_col_name_text = ["Date", "Count", "Runtime (sec)"]
time_stats_col_size = [30, 20, 20]
time_host_stats_col_name_csv = ["stat_type", "date", "host", "count", "runtime (sec)"]
time_host_stats_col_name_text = ["Date", "Host", "Count", "Runtime (sec)"]
time_host_stats_col_size = [23, 25, 10, 20]


class JobStatistics:
    def __init__(self):
        self.name = None
        self.site = None
        self.kickstart = None
        self.multiplier_factor = None
        self.kickstart_mult = None
        self.remote_cpu_time = None
        self.post = None
        self.condor_delay = None
        self.resource = None
        self.runtime = None
        self.condorQlen = None
        self.seqexec = None
        self.seqexec_delay = None
        self.retry_count = 0
        self.exitcode = None
        self.hostname = None

    def getFormattedJobStatistics(self):
        return [
            self.name,
            str(self.retry_count),
            self.site or "-",
            fstr(self.kickstart),
            str(self.multiplier_factor),
            fstr(self.kickstart_mult),
            fstr(self.remote_cpu_time),
            fstr(self.post),
            fstr(self.condor_delay),
            fstr(self.resource),
            fstr(self.runtime),
            fstr(self.seqexec),
            fstr(self.seqexec_delay),
            str(self.exitcode),
            self.hostname,
        ]


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


def remove_file(path):
    try:
        os.remove(path)
    except OSError as e:
        if e.errno != 2:
            raise


def write_to_file(file_path, mode, content):
    """
    Utility method for writing content to a given file
    @param file_path :  file path
    @param mode :   file writing mode 'a' append , 'w' write
    @param content :  content to write to file
    """
    try:
        fh = open(file_path, mode)
        fh.write(content)
    except OSError:
        logger.error("Unable to write to file " + file_path)
        sys.exit(1)
    else:
        fh.close()


def format_seconds(duration):
    """
    Utility for converting time to a readable format
    @param duration :  time in seconds and miliseconds
    @return time in format day,hour, min,sec
    """
    return stats_utils.format_seconds(duration)


def istr(value):
    """
    Utility for returning a str representation of the given value.
    Return '-' if value is None
    @parem value : the given value that need to be converted to string
    """
    if value is None:
        return "-"
    return str(value)


def fstr(value, to=3):
    """
    Utility method for rounding the float value to rounded string
    @param value :  value to round
    @param to    :  how many decimal points to round to
    """
    if value is None:
        return "-"
    return stats_utils.round_decimal_to_str(value, to)


def pstr(value, to=2):
    """
    Utility method for rounding the float value to rounded string
    @param value :  value to round
    @param to    :  how many decimal points to round to
    """
    if value is None:
        return "-"
    return stats_utils.round_decimal_to_str(value, to) + "%"


def print_row(row, sizes, fmt):
    """
    Utility method for generating formatted row based on the column format given
    row   : list of column values
    sizes : list of column widths for text format
    fmt   : format of the row ('text' or 'csv')
    """
    if fmt in ["text", "txt"]:
        return "".join(value.ljust(sizes[i]) for i, value in enumerate(row))
    elif fmt == "csv":
        return ",".join(row)
    else:
        print("Output format %s not recognized!" % fmt)
        sys.exit(1)


def print_workflow_details(output_db_url, wf_uuid, output_dir, multiple_wf=False):
    """
    Prints the workflow statistics information of all workflows
    @param output_db_url : URL of stampede DB
    @param wf_uuid       : uuid of the top level workflow
    @param output_dir    : directory to write output files
    """
    errors = 0
    try:
        if multiple_wf:
            expanded_workflow_stats = StampedeWorkflowStatistics(output_db_url)
        else:
            expanded_workflow_stats = StampedeStatistics(output_db_url)

        wf_found = expanded_workflow_stats.initialize(wf_uuid)

        if wf_found is False:
            print(
                "Workflow {!r} not found in database {!r}".format(
                    wf_uuid, output_db_url
                )
            )
            sys.exit(1)
    except Exception:
        logger.error("Failed to load the database." + output_db_url)
        logger.warning(traceback.format_exc())
        sys.exit(1)

    # print workflow statistics
    if multiple_wf:
        wf_uuid_list = []
        desc_wf_uuid_list = expanded_workflow_stats.get_workflow_ids()
    else:
        wf_uuid_list = [wf_uuid]
        desc_wf_uuid_list = expanded_workflow_stats.get_descendant_workflow_ids()

    for wf_det in desc_wf_uuid_list:
        wf_uuid_list.append(wf_det.wf_uuid)

    global calc_wf_stats
    if calc_wf_stats:
        if file_type == FILE_TYPE_TXT:
            wf_stats_file_txt = os.path.join(
                output_dir, workflow_statistics_file_name + text_file_extension
            )
            write_to_file(wf_stats_file_txt, "w", formatted_wf_status_legends())
            header = print_row(
                workflow_status_col_name_text, workflow_status_col_size, "text"
            )
            write_to_file(wf_stats_file_txt, "a", header)

        if file_type == FILE_TYPE_CSV:
            wf_stats_file_csv = os.path.join(
                output_dir, workflow_statistics_file_name + csv_file_extension
            )
            write_to_file(wf_stats_file_csv, "w", formatted_wf_status_legends())
            header = print_row(
                workflow_status_col_name_csv, workflow_status_col_size, "csv"
            )
            write_to_file(wf_stats_file_csv, "a", header)

    global calc_jb_stats
    if calc_jb_stats:
        jobs_stats_file_txt = os.path.join(
            output_dir, job_statistics_file_name + text_file_extension
        )
        if file_type == FILE_TYPE_TXT:
            write_to_file(jobs_stats_file_txt, "w", formatted_job_stats_legends())

        jobs_stats_file_csv = os.path.join(
            output_dir, job_statistics_file_name + csv_file_extension
        )
        if file_type == FILE_TYPE_CSV:
            write_to_file(jobs_stats_file_csv, "w", formatted_job_stats_legends())

    global calc_tf_stats
    if calc_tf_stats:
        if file_type == FILE_TYPE_TXT:
            transformation_stats_file_txt = os.path.join(
                output_dir,
                logical_transformation_statistics_file_name + text_file_extension,
            )
            write_to_file(
                transformation_stats_file_txt,
                "w",
                formatted_transformation_stats_legends(),
            )

        if file_type == FILE_TYPE_CSV:
            transformation_stats_file_csv = os.path.join(
                output_dir,
                logical_transformation_statistics_file_name + csv_file_extension,
            )
            write_to_file(
                transformation_stats_file_csv,
                "w",
                formatted_transformation_stats_legends(),
            )

    global calc_int_stats
    if calc_int_stats:
        if file_type == FILE_TYPE_TXT:
            integrity_stats_file_txt = os.path.join(
                output_dir, logical_integrity_statistics_file_name + text_file_extension
            )
            write_to_file(
                integrity_stats_file_txt, "w", formatted_integrity_stats_legends()
            )

        if file_type == FILE_TYPE_CSV:
            integrity_stats_file_csv = os.path.join(
                output_dir, logical_integrity_statistics_file_name + csv_file_extension
            )
            write_to_file(
                integrity_stats_file_csv, "w", formatted_integrity_stats_legends()
            )

    global calc_ti_stats
    if calc_ti_stats:
        try:
            time_stats_file_txt = os.path.join(
                output_dir, time_statistics_file_name + text_file_extension
            )
            if file_type == FILE_TYPE_TXT:
                content = print_statistics_by_time_and_host(
                    expanded_workflow_stats, "text", combined=True, per_host=True
                )
                write_to_file(
                    time_stats_file_txt, "w", formatted_time_stats_legends_text()
                )
                write_to_file(time_stats_file_txt, "a", content)

            time_stats_file_csv = os.path.join(
                output_dir, time_statistics_file_name + csv_file_extension
            )
            if file_type == FILE_TYPE_CSV:
                content = print_statistics_by_time_and_host(
                    expanded_workflow_stats, "csv", combined=True, per_host=False
                )
                write_to_file(
                    time_stats_file_csv, "w", formatted_time_stats_legends_csv()
                )
                write_to_file(time_stats_file_csv, "a", content)

                time_stats_file2_csv = os.path.join(
                    output_dir, time_statistics_per_host_file_name + csv_file_extension
                )
                content = print_statistics_by_time_and_host(
                    expanded_workflow_stats, "csv", combined=False, per_host=True
                )
                write_to_file(
                    time_stats_file2_csv, "w", formatted_time_host_stats_legends_csv()
                )
                write_to_file(time_stats_file2_csv, "a", content)
        except Exception:
            logger.warn("time statistics generation failed")
            logger.debug("time statistics generation failed", exc_info=1)

            calc_ti_stats = False
            if file_type == FILE_TYPE_TXT:
                remove_file(time_stats_file_txt)
            if file_type == FILE_TYPE_CSV:
                remove_file(time_stats_file_csv)
                remove_file(time_stats_file2_csv)

            errors += 1

    if calc_jb_stats or calc_tf_stats or calc_wf_stats or calc_int_stats:
        for sub_wf_uuid in wf_uuid_list:
            try:
                individual_workflow_stats = StampedeStatistics(output_db_url, False)
                wf_found = individual_workflow_stats.initialize(sub_wf_uuid)

                if wf_found is False:
                    print(
                        "Workflow %r not found in database %r"
                        % (sub_wf_uuid, output_db_url)
                    )
                    sys.exit(1)
            except Exception:
                logger.error("Failed to load the database." + output_db_url)
                logger.warning(traceback.format_exc())
                sys.exit(1)

            wf_det = individual_workflow_stats.get_workflow_details()[0]

            workflow_id = str(sub_wf_uuid)
            dax_label = str(wf_det.dax_label)
            logger.info(
                "Generating statistics information about the workflow "
                + workflow_id
                + " ... "
            )

            if calc_jb_stats:
                try:
                    logger.debug(
                        "Generating job instance statistics information for workflow "
                        + workflow_id
                        + " ... "
                    )
                    individual_workflow_stats.set_job_filter("all")

                    if file_type == FILE_TYPE_TXT:
                        content = print_individual_wf_job_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(jobs_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = print_individual_wf_job_stats(
                            individual_workflow_stats, workflow_id, dax_label, "csv"
                        )
                        write_to_file(jobs_stats_file_csv, "a", content)
                except Exception:
                    logger.warn("job instance statistics generation failed")
                    logger.debug(
                        "job instance statistics generation failed", exc_info=1
                    )

                    calc_jb_stats = False
                    if file_type == FILE_TYPE_TXT:
                        remove_file(jobs_stats_file_txt)
                    if file_type == FILE_TYPE_CSV:
                        remove_file(jobs_stats_file_csv)

                    errors += 1

            if calc_tf_stats:
                try:
                    logger.debug(
                        "Generating invocation statistics information for workflow "
                        + workflow_id
                        + " ... "
                    )
                    individual_workflow_stats.set_job_filter("all")
                    if file_type == FILE_TYPE_TXT:
                        content = print_wf_transformation_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(transformation_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = print_wf_transformation_stats(
                            individual_workflow_stats, workflow_id, dax_label, "csv"
                        )
                        write_to_file(transformation_stats_file_csv, "a", content)
                except Exception:
                    logger.warn("transformation statistics generation failed")
                    logger.debug(
                        "transformation statistics generation failed", exc_info=1
                    )

                    calc_tf_stats = False
                    if file_type == FILE_TYPE_TXT:
                        remove_file(transformation_stats_file_txt)
                    if file_type == FILE_TYPE_CSV:
                        remove_file(transformation_stats_file_csv)

                    errors += 1

            if calc_int_stats:
                try:
                    logger.debug(
                        "Generating integrity statistics information for workflow "
                        + workflow_id
                        + " ... "
                    )
                    individual_workflow_stats.set_job_filter("all")
                    if file_type == FILE_TYPE_TXT:
                        content = print_wf_integrity_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(integrity_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = print_wf_integrity_stats(
                            individual_workflow_stats, workflow_id, dax_label, "csv"
                        )
                        write_to_file(integrity_stats_file_csv, "a", content)
                except Exception:
                    logger.error(traceback.format_exc())
                    logger.warn("integrity statistics generation failed")
                    logger.debug("integrity statistics generation failed", exc_info=1)

                    calc_int_stats = False
                    if file_type == FILE_TYPE_TXT:
                        remove_file(integrity_stats_file_txt)
                    if file_type == FILE_TYPE_CSV:
                        remove_file(integrity_stats_file_csv)

                    errors += 1

            if calc_wf_stats:
                try:
                    logger.debug(
                        "Generating workflow statistics information for workflow "
                        + workflow_id
                        + " ... "
                    )
                    individual_workflow_stats.set_job_filter("all")
                    if file_type == FILE_TYPE_TXT:
                        content = print_individual_workflow_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(wf_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = print_individual_workflow_stats(
                            individual_workflow_stats, workflow_id, dax_label, "csv"
                        )
                        write_to_file(wf_stats_file_csv, "a", content)
                except Exception:
                    logger.warn("workflow statistics generation failed")
                    logger.debug("workflow statistics generation failed", exc_info=1)

                    calc_wf_stats = False
                    if file_type == FILE_TYPE_TXT:
                        remove_file(wf_stats_file_txt)
                    if file_type == FILE_TYPE_CSV:
                        remove_file(wf_stats_file_csv)

                    errors += 1

            individual_workflow_stats.close()

    #
    # Output printed to console
    #

    stats_output = ""

    global calc_wf_summary
    if calc_wf_summary:
        logger.info("Generating workflow summary ... ")
        try:
            if file_type == FILE_TYPE_TXT:
                summary_output = formatted_wf_summary_legends_txt()
                summary_output += NEW_LINE_STR
                summary_output += print_workflow_summary(
                    expanded_workflow_stats,
                    "text",
                    wf_summary=True,
                    time_summary=True,
                    multiple_wf=multiple_wf,
                )
                wf_summary_file_txt = os.path.join(
                    output_dir, workflow_summary_file_name + text_file_extension
                )
                write_to_file(wf_summary_file_txt, "w", summary_output)

                stats_output += summary_output + "\n"
                stats_output += "{:<30}: {}\n".format("Summary", wf_summary_file_txt)

            if file_type == FILE_TYPE_CSV:
                # Generate the first csv summary file
                summary_output = formatted_wf_summary_legends_csv1()
                summary_output += NEW_LINE_STR
                summary_output += print_workflow_summary(
                    expanded_workflow_stats,
                    "csv",
                    wf_summary=True,
                    time_summary=False,
                    multiple_wf=multiple_wf,
                )
                wf_summary_file_csv = os.path.join(
                    output_dir, workflow_summary_file_name + csv_file_extension
                )
                write_to_file(wf_summary_file_csv, "w", summary_output)

                stats_output += "{:<30}: {}\n".format("Summary:", wf_summary_file_csv)

                # Generate the second csv summary file
                summary_output = formatted_wf_summary_legends_csv2()
                summary_output += NEW_LINE_STR
                summary_output += print_workflow_summary(
                    expanded_workflow_stats,
                    "csv",
                    wf_summary=False,
                    time_summary=True,
                    multiple_wf=multiple_wf,
                )
                wf_summary_file2_csv = os.path.join(
                    output_dir, workflow_summary_time_file_name + csv_file_extension
                )
                write_to_file(wf_summary_file2_csv, "w", summary_output)

                stats_output += "{:<30}: {}\n".format(
                    "Summary Time:", wf_summary_file2_csv
                )
        except Exception:
            logger.warn("summary statistics generation failed")
            logger.debug("summary statistics generation failed", exc_info=1)
            stats_output = ""

            calc_wf_summary = False
            if file_type == FILE_TYPE_TXT:
                remove_file(wf_summary_file_txt)
            if file_type == FILE_TYPE_CSV:
                remove_file(workflow_summary_file_name)
                remove_file(wf_summary_file2_csv)

            errors += 1

    if calc_wf_stats:
        pre_stats_output = stats_output
        try:
            stats_output += "%-30s: " % "Workflow execution statistics"

            if file_type == FILE_TYPE_TXT:
                content = print_individual_workflow_stats(
                    expanded_workflow_stats, "All Workflows", "", "text"
                )
                write_to_file(wf_stats_file_txt, "a", content)
                stats_output += wf_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = print_individual_workflow_stats(
                    expanded_workflow_stats, "ALL", "", "csv"
                )
                write_to_file(wf_stats_file_csv, "a", content)
                stats_output += wf_stats_file_csv + "\n"
        except Exception:
            logger.warn("workflow statistics generation failed")
            logger.debug("workflow statistics generation failed", exc_info=1)
            stats_output = pre_stats_output

            calc_wf_stats = False
            if file_type == FILE_TYPE_TXT:
                remove_file(wf_stats_file_txt)
            if file_type == FILE_TYPE_CSV:
                remove_file(wf_stats_file_csv)

            errors += 1

    if calc_jb_stats:
        stats_output += "%-30s: " % "Job instance statistics"
        if file_type == FILE_TYPE_TXT:
            stats_output += jobs_stats_file_txt + "\n"

        if file_type == FILE_TYPE_CSV:
            stats_output += jobs_stats_file_csv + "\n"

    if calc_tf_stats:
        expanded_workflow_stats.set_job_filter("all")
        pre_stats_output = stats_output

        try:
            stats_output += "%-30s: " % "Transformation statistics"

            if file_type == FILE_TYPE_TXT:
                content = print_wf_transformation_stats(
                    expanded_workflow_stats, "All", "", "text"
                )
                write_to_file(transformation_stats_file_txt, "a", content)
                stats_output += transformation_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = print_wf_transformation_stats(
                    expanded_workflow_stats, "ALL", "", "csv"
                )
                write_to_file(transformation_stats_file_csv, "a", content)
                stats_output += transformation_stats_file_csv + "\n"
        except Exception:
            logger.warn("transformation statistics generation failed")
            logger.debug("transformation statistics generation failed", exc_info=1)
            stats_output = pre_stats_output

            calc_tf_stats = False
            if file_type == FILE_TYPE_TXT:
                remove_file(transformation_stats_file_txt)
            if file_type == FILE_TYPE_CSV:
                remove_file(transformation_stats_file_csv)
            errors += 1

    if calc_int_stats:
        expanded_workflow_stats.set_job_filter("all")
        pre_stats_output = stats_output

        try:
            stats_output += "%-30s: " % "Integrity statistics"

            if file_type == FILE_TYPE_TXT:
                content = print_wf_integrity_stats(
                    expanded_workflow_stats, "All", "", "text"
                )
                write_to_file(integrity_stats_file_txt, "a", content)
                stats_output += integrity_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = print_wf_integrity_stats(
                    expanded_workflow_stats, "ALL", "", "csv"
                )
                write_to_file(integrity_stats_file_csv, "a", content)
                stats_output += integrity_stats_file_csv + "\n"
        except Exception:
            logger.warn("integrity statistics generation failed")
            logger.debug("integrity statistics generation failed", exc_info=1)
            stats_output = pre_stats_output

            calc_int_stats = False
            if file_type == FILE_TYPE_TXT:
                remove_file(integrity_stats_file_txt)
            if file_type == FILE_TYPE_CSV:
                remove_file(integrity_stats_file_csv)

            errors += 1

    if calc_ti_stats:
        stats_output += "%-30s: " % "Time statistics"

        if file_type == FILE_TYPE_TXT:
            stats_output += time_stats_file_txt + "\n"

        if file_type == FILE_TYPE_CSV:
            stats_output += time_stats_file_csv + "\n"

    expanded_workflow_stats.close()

    print(stats_output)
    return errors


def print_workflow_summary(
    workflow_stats, output_format, wf_summary=True, time_summary=True, multiple_wf=False
):
    """
    Prints the workflow statistics summary of an top level workflow
    @param workflow_stats :  workflow statistics object reference
    """

    summary_str = ""

    if wf_summary is True:
        # status
        workflow_stats.set_job_filter("nonsub")

        # Tasks
        total_tasks = workflow_stats.get_total_tasks_status()
        total_succeeded_tasks = workflow_stats.get_total_succeeded_tasks_status(
            uses_PMC
        )
        total_failed_tasks = workflow_stats.get_total_failed_tasks_status()
        total_unsubmitted_tasks = total_tasks - (
            total_succeeded_tasks + total_failed_tasks
        )
        total_task_retries = workflow_stats.get_total_tasks_retries()
        total_invocations = (
            total_succeeded_tasks + total_failed_tasks + total_task_retries
        )

        # Jobs
        total_jobs = workflow_stats.get_total_jobs_status()
        total_succeeded_failed_jobs = (
            workflow_stats.get_total_succeeded_failed_jobs_status()
        )
        total_failed_jobs_integrity = workflow_stats.get_total_succeeded_failed_jobs_status(
            classify_error=True, tag="int.error"
        )
        total_succeeded_jobs = total_succeeded_failed_jobs.succeeded
        total_failed_jobs = total_succeeded_failed_jobs.failed
        total_unsubmitted_jobs = total_jobs - (total_succeeded_jobs + total_failed_jobs)
        total_job_retries = workflow_stats.get_total_jobs_retries()
        total_job_instance_retries = (
            total_succeeded_jobs + total_failed_jobs + total_job_retries
        )

        # Sub workflows
        workflow_stats.set_job_filter("subwf")
        total_sub_wfs = workflow_stats.get_total_jobs_status()
        total_succeeded_failed_sub_wfs = (
            workflow_stats.get_total_succeeded_failed_jobs_status()
        )
        total_succeeded_sub_wfs = total_succeeded_failed_sub_wfs.succeeded
        total_failed_sub_wfs = total_succeeded_failed_sub_wfs.failed
        # for non hierarichal workflows the combined query can return none
        if total_succeeded_sub_wfs is None:
            total_succeeded_sub_wfs = 0

        if total_failed_sub_wfs is None:
            total_failed_sub_wfs = 0

        total_unsubmitted_sub_wfs = total_sub_wfs - (
            total_succeeded_sub_wfs + total_failed_sub_wfs
        )
        total_sub_wfs_retries = workflow_stats.get_total_jobs_retries()
        total_sub_wfs_tries = (
            total_succeeded_sub_wfs + total_failed_sub_wfs + total_sub_wfs_retries
        )

        # Format the output
        if output_format == "text":
            summary_str += "".center(sum(workflow_summary_col_size), "-") + "\n"
            summary_str += (
                print_row(
                    workflow_summary_col_name_text,
                    workflow_summary_col_size,
                    output_format,
                )
                + "\n"
            )
        else:
            summary_str += (
                print_row(
                    workflow_summary_col_name_csv,
                    workflow_summary_col_size,
                    output_format,
                )
                + "\n"
            )

        content = [
            "Tasks",
            istr(total_succeeded_tasks),
            istr(total_failed_tasks),
            istr(total_unsubmitted_tasks),
            istr(total_tasks),
            istr(total_task_retries),
            istr(total_invocations),
        ]
        summary_str += (
            print_row(content, workflow_summary_col_size, output_format) + "\n"
        )

        content = [
            "Jobs",
            istr(total_succeeded_jobs),
            istr(total_failed_jobs),
            istr(total_unsubmitted_jobs),
            istr(total_jobs),
            str(total_job_retries),
            istr(total_job_instance_retries),
        ]
        summary_str += (
            print_row(content, workflow_summary_col_size, output_format) + "\n"
        )

        content = [
            "Sub-Workflows",
            istr(total_succeeded_sub_wfs),
            istr(total_failed_sub_wfs),
            istr(total_unsubmitted_sub_wfs),
            istr(total_sub_wfs),
            str(total_sub_wfs_retries),
            istr(total_sub_wfs_tries),
        ]
        summary_str += (
            print_row(content, workflow_summary_col_size, output_format) + "\n"
        )

        if output_format == "text":
            summary_str += "".center(sum(workflow_summary_col_size), "-") + "\n\n"

    if time_summary is True:
        states = workflow_stats.get_workflow_states()
        wwt = stats_utils.get_workflow_wall_time(states)
        wcjwt, wcgpt, wcbpt = workflow_stats.get_workflow_cum_job_wall_time()
        ssjwt, ssgpt, ssbpt = workflow_stats.get_submit_side_job_wall_time()
        if output_format == "text":

            def myfmt(val):
                if val is None:
                    return "-"
                else:
                    return format_seconds(val)

            if not multiple_wf:
                summary_str += "{:<57}: {}\n".format("Workflow wall time", myfmt(wwt))
            summary_str += "{:<57}: {}\n".format(
                "Cumulative job wall time", myfmt(wcjwt)
            )
            summary_str += "{:<57}: {}\n".format(
                "Cumulative job wall time as seen from submit side", myfmt(ssjwt),
            )
            summary_str += "{:<57}: {}\n".format(
                "Cumulative job badput wall time", myfmt(wcbpt),
            )
            summary_str += "{:<57}: {}\n".format(
                "Cumulative job badput wall time as seen from submit side",
                myfmt(ssbpt),
            )
        else:

            def myfmt(val):
                if val is None:
                    return ""
                else:
                    return str(val)

            summary_str += (
                print_row(workflow_time_summary_col_name_csv, None, output_format)
                + "\n"
            )
            summary_str += "workflow_wall_time,%s\n" % myfmt(wwt)
            summary_str += "workflow_cumulative_job_wall_time,%s\n" % myfmt(wcjwt)
            summary_str += "cumulative_job_walltime_from_submit_side,%s\n" % myfmt(
                ssjwt
            )
            summary_str += "workflow_cumulative_badput_time,%s\n" % myfmt(wcbpt)
            summary_str += (
                "cumulative_job_badput_walltime_from_submit_side,%s\n" % myfmt(ssbpt)
            )

        # PM-1260 integrity metrics summary
        int_metrics_summary = workflow_stats.get_summary_integrity_metrics()
        if int_metrics_summary and output_format == "text":

            def myfmt(val):
                if val is None:
                    return "-"
                else:
                    return format_seconds(val)

            summary_str += """
# Integrity Metrics
# Number of files for which checksums were compared/computed along with total time spent doing it. \n"""
            for result in int_metrics_summary:
                type = result.type
                if result.type == "check":
                    type = "compared"
                if result.type == "compute":
                    type = "generated"
                summary_str += "{} files checksums {} with total duration of {}\n".format(
                    result.count, type, myfmt(result.duration),
                )

            summary_str += """
# Integrity Errors
# Total:
#       Total number of integrity errors encountered across all job executions(including retries) of a workflow.
# Failures:
#       Number of failed jobs where the last job instance had integrity errors.
"""
            int_error_summary = workflow_stats.get_tag_metrics("int.error")
            for result in int_error_summary:
                type = result.name
                if result.name == "int.error":
                    type = "integrity"
                summary_str += (
                    "Total:    A total of %s %s errors encountered in the workflow\n"
                    % (result.count, type)
                )

            # PM-1295 jobs where last_job_instance failed and there were integrity errors in those last job instances
            summary_str += "Failures: %s job failures had integrity errors\n" % (
                total_failed_jobs_integrity.failed or 0
            )

    return summary_str


def print_individual_workflow_stats(
    workflow_stats, workflow_id, dax_label, output_format
):
    """
    Prints the workflow statistics of workflow
    @param workflow_stats :  workflow statistics object reference
    @param workflow_id  : workflow_id (title of the workflow table)
    """
    content_str = "\n"
    # individual workflow status

    # Add dax_label to workflow_id if writing text file
    if output_format == "text" and dax_label != "":
        workflow_id = workflow_id + " (" + dax_label + ")"

    # workflow status
    workflow_stats.set_job_filter("all")
    total_wf_retries = workflow_stats.get_workflow_retries()
    # only used for the text output...
    content = [workflow_id, istr(total_wf_retries)]
    retry_col_size = workflow_status_col_size[len(workflow_status_col_size) - 1]
    wf_status_str = print_row(
        content,
        [sum(workflow_status_col_size) - retry_col_size, retry_col_size],
        output_format,
    )

    # tasks
    workflow_stats.set_job_filter("nonsub")
    total_tasks = workflow_stats.get_total_tasks_status()
    total_succeeded_tasks = workflow_stats.get_total_succeeded_tasks_status(uses_PMC)
    total_failed_tasks = workflow_stats.get_total_failed_tasks_status()
    total_unsubmitted_tasks = total_tasks - (total_succeeded_tasks + total_failed_tasks)
    total_task_retries = workflow_stats.get_total_tasks_retries()
    total_task_invocations = (
        total_succeeded_tasks + total_failed_tasks + total_task_retries
    )
    if output_format == "text":
        content = [
            "Tasks",
            istr(total_succeeded_tasks),
            istr(total_failed_tasks),
            istr(total_unsubmitted_tasks),
            istr(total_tasks),
            istr(total_task_retries),
            istr(total_task_invocations),
            "",
        ]
    else:
        content = [
            workflow_id,
            dax_label,
            "Tasks",
            istr(total_succeeded_tasks),
            istr(total_failed_tasks),
            istr(total_unsubmitted_tasks),
            istr(total_tasks),
            istr(total_task_retries),
            istr(total_task_invocations),
            istr(total_wf_retries),
        ]

    tasks_status_str = print_row(content, workflow_status_col_size, output_format)

    # job status
    workflow_stats.set_job_filter("nonsub")
    total_jobs = workflow_stats.get_total_jobs_status()

    tmp = workflow_stats.get_total_succeeded_failed_jobs_status()
    total_succeeded_jobs = tmp.succeeded
    total_failed_jobs = tmp.failed

    total_unsubmitted_jobs = total_jobs - (total_succeeded_jobs + total_failed_jobs)
    total_job_retries = workflow_stats.get_total_jobs_retries()
    total_job_invocations = total_succeeded_jobs + total_failed_jobs + total_job_retries
    if output_format == "text":
        content = [
            "Jobs",
            istr(total_succeeded_jobs),
            istr(total_failed_jobs),
            istr(total_unsubmitted_jobs),
            istr(total_jobs),
            istr(total_job_retries),
            istr(total_job_invocations),
            "",
        ]
    else:
        content = [
            workflow_id,
            dax_label,
            "Jobs",
            istr(total_succeeded_jobs),
            istr(total_failed_jobs),
            istr(total_unsubmitted_jobs),
            istr(total_jobs),
            istr(total_job_retries),
            istr(total_job_invocations),
            istr(total_wf_retries),
        ]

    jobs_status_str = print_row(content, workflow_status_col_size, output_format)

    # sub workflow
    workflow_stats.set_job_filter("subwf")
    total_sub_wfs = workflow_stats.get_total_jobs_status()

    tmp = workflow_stats.get_total_succeeded_failed_jobs_status()

    total_succeeded_sub_wfs = 0
    if tmp.succeeded:
        total_succeeded_sub_wfs = tmp.succeeded

    total_failed_sub_wfs = 0
    if tmp.failed:
        total_failed_sub_wfs = tmp.failed

    total_unsubmitted_sub_wfs = total_sub_wfs - (
        total_succeeded_sub_wfs + total_failed_sub_wfs
    )
    total_sub_wfs_retries = workflow_stats.get_total_jobs_retries()
    total_sub_wfs_invocations = (
        total_succeeded_sub_wfs + total_failed_sub_wfs + total_sub_wfs_retries
    )
    if output_format == "text":
        content = [
            "Sub Workflows",
            istr(total_succeeded_sub_wfs),
            istr(total_failed_sub_wfs),
            istr(total_unsubmitted_sub_wfs),
            istr(total_sub_wfs),
            istr(total_sub_wfs_retries),
            istr(total_sub_wfs_invocations),
            "",
        ]
    else:
        content = [
            workflow_id,
            dax_label,
            "Sub_Workflows",
            istr(total_succeeded_sub_wfs),
            istr(total_failed_sub_wfs),
            istr(total_unsubmitted_sub_wfs),
            istr(total_sub_wfs),
            istr(total_sub_wfs_retries),
            istr(total_sub_wfs_invocations),
            istr(total_wf_retries),
        ]

    sub_wf_status_str = print_row(content, workflow_status_col_size, output_format)

    if output_format == "text":
        # Only print these in the text format output
        content_str += "".center(sum(workflow_status_col_size), "-") + "\n"
        content_str += wf_status_str + "\n"

    content_str += tasks_status_str + "\n"
    content_str += jobs_status_str + "\n"
    content_str += sub_wf_status_str + "\n"

    return content_str


def print_individual_wf_job_stats(
    workflow_stats, workflow_id, dax_label, output_format
):
    """
    Prints the job statistics of workflow
    @param workflow_stats :  workflow statistics object reference
    @param workflow_id : workflow_id (title for the table)
    """
    job_stats_list = []
    job_retry_count_dict = {}

    # Add dax_label to workflow_id if writing text file
    if output_format == "text":
        workflow_id = workflow_id + " (" + dax_label + ")"

    if output_format == "text":
        job_status_str = "\n# " + workflow_id + "\n"
    else:
        job_status_str = "\n"

    if output_format == "text":
        max_length = [max(0, len(i)) for i in job_stats_col_name_text]

    wf_job_stats_list = workflow_stats.get_job_statistics()

    # Go through each job in the workflow
    for job in wf_job_stats_list:
        job_stats = JobStatistics()
        job_stats.name = job.job_name
        job_stats.site = job.site
        job_stats.kickstart = job.kickstart
        job_stats.multiplier_factor = job.multiplier_factor
        job_stats.kickstart_mult = job.kickstart_multi
        job_stats.remote_cpu_time = job.remote_cpu_time
        job_stats.post = job.post_time
        job_stats.runtime = job.runtime
        job_stats.condor_delay = job.condor_q_time
        job_stats.resource = job.resource_delay
        job_stats.seqexec = job.seqexec
        job_stats.exitcode = utils.raw_to_regular(job.exit_code)
        job_stats.hostname = job.host_name
        if job_stats.seqexec is not None and job_stats.kickstart is not None:
            job_stats.seqexec_delay = float(job_stats.seqexec) - float(
                job_stats.kickstart
            )
        if job.job_name in job_retry_count_dict:
            job_retry_count_dict[job.job_name] += 1
        else:
            job_retry_count_dict[job.job_name] = 1
        job_stats.retry_count = job_retry_count_dict[job.job_name]

        if output_format == "text":
            max_length[0] = max(max_length[0], len(job_stats.name))
            max_length[1] = max(max_length[1], len(str(job_stats.retry_count)))
            max_length[2] = max(max_length[2], len(job_stats.site or " "))
            max_length[3] = max(max_length[3], len(str(job_stats.kickstart)))
            max_length[4] = max(max_length[4], len(str(job_stats.multiplier_factor)))
            max_length[5] = max(max_length[5], len(str(job_stats.kickstart_mult)))
            max_length[6] = max(max_length[6], len(str(job_stats.remote_cpu_time)))
            max_length[7] = max(max_length[7], len(str(job_stats.post)))
            max_length[8] = max(max_length[8], len(str(job_stats.condor_delay)))
            max_length[9] = max(max_length[9], len(str(job_stats.resource)))
            max_length[10] = max(max_length[10], len(str(job_stats.runtime)))
            max_length[11] = max(max_length[11], len(str(job_stats.seqexec)))
            max_length[12] = max(max_length[12], len(str(job_stats.seqexec_delay)))
            max_length[13] = max(max_length[13], len(str(job_stats.exitcode)))
            max_length[14] = max(
                max_length[14],
                len(job_stats.hostname if job_stats.hostname else "None"),
            )

        job_stats_list.append(job_stats)

    # Print header
    if output_format == "text":
        max_length = [i + 1 for i in max_length]
        job_status_str += print_row(job_stats_col_name_text, max_length, output_format)
    else:
        job_status_str += print_row(
            job_stats_col_name_csv, job_stats_col_size, output_format
        )

    job_status_str += "\n"

    # printing
    # find the pretty print length
    for job_stat in job_stats_list:
        job_det = job_stat.getFormattedJobStatistics()
        if output_format == "text":
            index = 0
            for content in job_det:
                job_status_str += str(content).ljust(max_length[index])
                index = index + 1
        else:
            job_status_str += workflow_id
            job_status_str += ","
            job_status_str += dax_label
            for content in job_det:
                job_status_str += "," + str(content)

        job_status_str += NEW_LINE_STR

    return job_status_str


def print_wf_transformation_stats(stats, workflow_id, dax_label, fmt):
    """
    Prints the transformation statistics of workflow
    stats       : workflow statistics object reference
    workflow_id : UUID of workflow
    dax_label   : Name of workflow
    format      : Format of report ('text' or 'csv')
    """
    if fmt not in ["text", "csv"]:
        print("Output format %s not recognized!" % fmt)
        sys.exit(1)

    report = ["\n"]

    if fmt == "text":
        # In text file, we need a line with the workflow id first
        report.append("# {} ({})".format(workflow_id, dax_label or "All"))

    col_names = transformation_stats_col_name_text
    if fmt == "csv":
        col_names = transformation_stats_col_name_csv

    transformation_statistics = stats.get_transformation_statistics()

    if fmt == "text":
        max_length = [max(0, len(col_names[i])) for i in range(13)]
        columns = ["" for i in range(13)]

        for t in transformation_statistics:
            max_length[0] = max(max_length[0], len(t.transformation))
            max_length[1] = max(max_length[1], len(str(t.type)))
            max_length[2] = max(max_length[2], len(str(t.count)))
            max_length[3] = max(max_length[3], len(str(t.min)))
            max_length[4] = max(max_length[4], len(str(t.max)))
            max_length[5] = max(max_length[5], len(str(t.avg)))
            max_length[6] = max(max_length[6], len(str(t.sum)))
            # maxrss
            max_length[7] = max(max_length[7], len(str(t.min_maxrss)))
            max_length[8] = max(max_length[8], len(str(t.max_maxrss)))
            max_length[9] = max(max_length[9], len(str(t.avg_maxrss)))
            # avg_cpu
            max_length[10] = max(max_length[10], len(str(t.min_avg_cpu)))
            max_length[11] = max(max_length[11], len(str(t.max_avg_cpu)))
            max_length[12] = max(max_length[12], len(str(t.avg_avg_cpu)))

        max_length = [i + 1 for i in max_length]

    header_printed = False

    for t in transformation_statistics:
        content = [
            t.transformation,
            t.type,
            str(t.count),
            fstr(t.min),
            fstr(t.max),
            fstr(t.avg),
            fstr(t.sum),
            fstr(t.min_maxrss / 1024) if t.min_maxrss else "-",
            fstr(t.max_maxrss / 1024) if t.max_maxrss else "-",
            fstr(t.avg_maxrss / 1024) if t.avg_maxrss else "-",
            pstr(t.min_avg_cpu * 100) if t.min_avg_cpu else "-",
            pstr(t.max_avg_cpu * 100) if t.max_avg_cpu else "-",
            pstr(t.avg_avg_cpu * 100) if t.avg_avg_cpu else "-",
        ]

        if fmt == "text":
            for i in range(0, 13):
                columns[i] = col_names[i].ljust(max_length[i])
                content[i] = content[i].ljust(max_length[i])

        if fmt == "csv":
            columns = transformation_stats_col_name_csv
            content = [workflow_id, dax_label] + content

        if not header_printed:
            header_printed = True
            report.append(print_row(columns, transformation_stats_col_size, fmt))

        report.append(print_row(content, transformation_stats_col_size, fmt))

    return NEW_LINE_STR.join(report) + NEW_LINE_STR


def print_wf_integrity_stats(stats, workflow_id, dax_label, fmt):
    """
    Prints the integrity statistics of workflow
    stats       : workflow statistics object reference
    workflow_id : UUID of workflow
    dax_label   : Name of workflow
    format      : Format of report ('text' or 'csv')
    """
    if fmt not in ["text", "csv"]:
        print("Output format %s not recognized!" % fmt)
        sys.exit(1)

    report = ["\n"]

    if fmt == "text":
        # In text file, we need a line with the workflow id first
        report.append("# {} ({})".format(workflow_id, dax_label or "All"))

    col_names = integrity_stats_col_name_text
    if fmt == "csv":
        col_names = integrity_stats_col_name_csv

    integrity_statistics = stats.get_integrity_metrics()

    if fmt == "text":
        max_length = [max(0, len(col_names[i])) for i in range(4)]
        columns = ["" for i in range(4)]
        # figure out max lengths?
        for i in integrity_statistics:
            max_length[0] = max(max_length[0], len(i.type))
            max_length[1] = max(max_length[0], len(i.file_type))
            max_length[2] = max(max_length[1], len(str(i.count)))
            max_length[3] = max(max_length[2], len(str(i.duration)))

        max_length = [i + 1 for i in max_length]

    header_printed = False

    for i in integrity_statistics:
        content = [i.type, i.file_type, str(i.count), str(i.duration)]

        if fmt == "text":
            for i in range(0, 4):
                columns[i] = col_names[i].ljust(max_length[i])
                content[i] = content[i].ljust(max_length[i])

        if fmt == "csv":
            columns = integrity_stats_col_name_csv
            content = [workflow_id, dax_label] + content

        if not header_printed:
            header_printed = True
            report.append(print_row(columns, integrity_stats_col_size, fmt))

        report.append(print_row(content, integrity_stats_col_size, fmt))

    return NEW_LINE_STR.join(report) + NEW_LINE_STR


def print_statistics_by_time_and_host(stats, fmt, combined=True, per_host=True):
    """
    Prints the job instance and invocation statistics sorted by time
    @param stats     : workflow statistics object reference
    @param fmt       : indicates how to format the output: "text" or "csv"
    @param combined  : print combined output (all hosts consolidated)
    @param per_host  : print per-host totals
    """
    report = []
    stats.set_job_filter("nonsub")
    stats.set_time_filter("hour")
    stats.set_transformation_filter(exclude=["condor::dagman"])

    if combined is True:
        col_names = time_stats_col_name_text
        if fmt == "csv":
            col_names = time_stats_col_name_csv

        report.append("\n# Job instances statistics per " + time_filter)
        report.append(print_row(col_names, time_stats_col_size, fmt))
        stats_by_time = stats.get_jobs_run_by_time()
        formatted = stats_utils.convert_stats_to_base_time(stats_by_time, time_filter)
        for s in formatted:
            content = [s["date_format"], str(s["count"]), fstr(s["runtime"])]
            if fmt == "csv":
                content.insert(0, "jobs/" + time_filter)
            report.append(print_row(content, time_stats_col_size, fmt))

        report.append("\n# Invocation statistics run per " + time_filter)
        report.append(print_row(col_names, time_stats_col_size, fmt))
        stats_by_time = stats.get_invocation_by_time()
        formatted = stats_utils.convert_stats_to_base_time(stats_by_time, time_filter)
        for s in formatted:
            content = [s["date_format"], str(s["count"]), fstr(s["runtime"])]
            if fmt == "csv":
                content.insert(0, "invocations/" + time_filter)
            report.append(print_row(content, time_stats_col_size, fmt))

    if per_host is True:
        col_names = time_host_stats_col_name_text
        if fmt == "csv":
            col_names = time_host_stats_col_name_csv

        report.append("\n# Job instances statistics on host per " + time_filter)
        report.append(print_row(col_names, time_host_stats_col_size, fmt))
        stats_by_time = stats.get_jobs_run_by_time_per_host()
        formatted_stats_list = stats_utils.convert_stats_to_base_time(
            stats_by_time, time_filter, True
        )
        for s in formatted_stats_list:
            content = [
                s["date_format"],
                str(s["host"]),
                str(s["count"]),
                fstr(s["runtime"]),
            ]
            if fmt == "csv":
                content.insert(0, "jobs/host/" + time_filter)
            report.append(print_row(content, time_host_stats_col_size, fmt))

        report.append("\n# Invocation statistics on host per " + time_filter)
        report.append(print_row(col_names, time_host_stats_col_size, fmt))
        stats_by_time = stats.get_invocation_by_time_per_host()
        formatted_stats_list = stats_utils.convert_stats_to_base_time(
            stats_by_time, time_filter, True
        )
        for s in formatted_stats_list:
            content = [
                s["date_format"],
                str(s["host"]),
                str(s["count"]),
                fstr(s["runtime"]),
            ]
            if fmt == "csv":
                content.insert(0, "invocations/host/" + time_filter)
            report.append(print_row(content, time_host_stats_col_size, fmt))

    return "\n".join(report)


def main():
    # Configure command line option parser
    prog_usage = (
        "%s [options] [[SUBMIT_DIRECTORY ..] | [WORKFLOW_UUID ..]]" % sys.argv[0]
    )

    parser = optparse.OptionParser(usage=prog_usage)
    parser.add_option(
        "-o",
        "--output",
        action="store",
        dest="output_dir",
        help="Writes the output to given directory.",
    )
    parser.add_option(
        "-f",
        "--file",
        action="store",
        dest="filetype",
        choices=[FILE_TYPE_TXT, FILE_TYPE_CSV],
        default=FILE_TYPE_TXT,
        help="Select output file type. Valid values are 'text' and 'csv'. Default is '%default'.",
    )
    parser.add_option(
        "-c",
        "--conf",
        action="store",
        type="string",
        dest="config_properties",
        default=None,
        help="Specifies the properties file to use. This option overrides all other property files.",
    )
    parser.add_option(
        "-s",
        "--statistics-level",
        action="store",
        dest="statistics_level",
        # choices=['all', 'summary', 'wf_stats', 'jb_stats', 'tf_stats', 'ti_stats','int_stats'],
        default="summary",
        help="Comma separated list. Valid levels are: all,summary,wf_stats,jb_stats,tf_stats,ti_stats,int_stats; Default is '%default'.",
    )
    parser.add_option(
        "-t",
        "--time-filter",
        action="store",
        dest="time_filter",
        choices=["day", "hour"],
        default="day",
        help="Valid levels are: day,hour; Default is '%default'.",
    )
    parser.add_option(
        "-i",
        "--ignore-db-inconsistency",
        action="store_true",
        default=False,
        dest="ignore_db_inconsistency",
        help="turn off the check for db consistency",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="count",
        default=0,
        dest="verbose",
        help="Increase verbosity, repeatable",
    )
    parser.add_option(
        "-q",
        "--quiet",
        action="count",
        default=0,
        dest="quiet",
        help="Decrease verbosity, repeatable",
    )

    parser.add_option(
        "-m",
        "--multiple-wf",
        action="store_true",
        dest="multiple_wf",
        default=False,
        help="Calculate statistics for multiple workflows",
    )
    parser.add_option(
        "-p",
        "--ispmc",
        action="store_true",
        dest="is_pmc",
        default=False,
        help="Calculate statistics for workflows which use PMC",
    )
    parser.add_option(
        "-u",
        "--isuuid",
        action="store_true",
        dest="is_uuid",
        default=False,
        help="Set if the positional arguments are wf uuids",
    )

    # Parse command line options
    (options, args) = parser.parse_args()

    options.statistics_level = {
        t.lower().strip() for t in options.statistics_level.split(",")
    }

    if not options.statistics_level:
        options.statistics_level = {"summary"}

    sl = options.statistics_level - {
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
    multiple_wf = options.multiple_wf

    if len(args) < 1:
        # * means all workflows in the database, and . means current directory
        submit_dir = "."
        if multiple_wf:
            submit_dir = "*"
    elif len(args) > 1:
        options.multiple_wf = True
        multiple_wf = True
        submit_dir = args
    else:
        options.multiple_wf = False
        multiple_wf = False
        submit_dir = args[0]

    log_level = options.verbose - options.quiet
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
            sys.exit(1)

    if multiple_wf:
        # Check for braindump file's existence if workflows are not specified as UUIDs and
        # statistics need to be calculated only on a sub set of workflows
        if not options.is_uuid and submit_dir != "*":
            for dir in submit_dir:
                check_dump(dir)
    else:
        if not options.is_uuid:
            check_dump(submit_dir)

    if options.ignore_db_inconsistency:
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
                sys.exit(1)

        if multiple_wf:
            if submit_dir == "*":
                logger.warning(
                    "Statistics have to be calculated on all workflows. Tool cannot check to see if all of them have finished. Ensure that all workflows have finished"
                )

            if not options.is_uuid and submit_dir != "*":
                for dir in submit_dir:
                    loading_complete(dir)
        else:
            if not options.is_uuid:
                loading_complete(submit_dir)

    # Figure out what statistics we need to calculate
    global calc_wf_stats
    global calc_wf_summary
    global calc_jb_stats
    global calc_tf_stats
    global calc_ti_stats
    global calc_int_stats
    sl = options.statistics_level
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
            sys.exit(1)
        calc_jb_stats = True

    if "tf_stats" in sl:
        calc_tf_stats = True

    if "int_stats" in sl:
        calc_int_stats = True

    global file_type
    file_type = options.filetype
    logger.info("File type is %s" % file_type)

    global time_filter
    time_filter = options.time_filter
    logger.info("Time filter is %s" % time_filter)

    # Change the legend to show the time filter format
    tf_format = str(stats_utils.get_date_print_format(time_filter))

    time_stats_col_name_text[0] += tf_format
    time_stats_col_name_csv[1] += tf_format
    time_host_stats_col_name_text[0] += tf_format
    time_host_stats_col_name_csv[1] += tf_format

    if options.output_dir:
        delete_if_exists = False
        output_dir = options.output_dir
    else:
        delete_if_exists = True
        if multiple_wf or options.is_uuid:
            sys.stderr.write(
                "Output directory option is required when calculating statistics over multiple workflows.\n"
            )
            sys.exit(1)
        else:
            output_dir = os.path.join(submit_dir, DEFAULT_OUTPUT_DIR)

    logger.info("Output directory is %s" % output_dir)
    utils.create_directory(output_dir, delete_if_exists=delete_if_exists)

    global uses_PMC

    def use_pmc(dir):
        braindb = utils.slurp_braindb(dir)
        return braindb["uses_pmc"] is True

    if options.is_pmc:
        logger.info("Calculating statistics with use of PMC clustering")
        uses_PMC = True
    else:
        if options.is_uuid:
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
                    tmp = sum([int(use_pmc(dir)) for dir in submit_dir])

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
        if options.is_uuid or submit_dir == "*":
            # URL picked from config_properties file.
            output_db_url = connection.url_by_properties(
                options.config_properties, connection.DBType.WORKFLOW
            )
            wf_uuid = submit_dir

            if not output_db_url:
                logger.error(
                    'Unable to determine database URL. Kindly specify a value for "pegasus.monitord.output" property'
                )
                sys.exit(1)
        else:
            db_url_set = set()
            wf_uuid = []

            for dir in submit_dir:
                db_url = connection.url_by_submitdir(
                    dir, connection.DBType.WORKFLOW, options.config_properties
                )
                uuid = connection.get_wf_uuid(dir)
                db_url_set.add(db_url)
                wf_uuid.append(uuid)

            if len(db_url_set) != 1:
                logger.error(
                    "Workflows are distributed across multiple databases, which is not supported"
                )
                sys.exit(1)

            output_db_url = db_url_set.pop()

    else:
        if options.is_uuid:
            output_db_url = connection.url_by_properties(
                options.config_properties, connection.DBType.WORKFLOW
            )
            wf_uuid = submit_dir

            if not output_db_url:
                logger.error(
                    'Unable to determine database URL. Kindly specify a value for "pegasus.monitord.output" property'
                )
                sys.exit(1)
        else:
            output_db_url = connection.url_by_submitdir(
                submit_dir, connection.DBType.WORKFLOW, options.config_properties
            )
            wf_uuid = connection.get_wf_uuid(submit_dir)

    logger.info("DB URL is: %s" % output_db_url)
    logger.info("workflow UUID is: %s" % wf_uuid)

    if output_db_url is not None:
        errors = print_workflow_details(
            output_db_url, wf_uuid, output_dir, multiple_wf=multiple_wf
        )

        if errors:
            logger.error("Failed to generate %d type(s) of statistics" % errors)
            sys.exit(1)


if __name__ == "__main__":
    main()
