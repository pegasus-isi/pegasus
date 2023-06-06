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
logger = logging.getLogger("pegasus-newstatistics")


utils.configureLogging(level=logging.WARNING)

# Regular expressions
re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")

# --- exceptions ----------------------------------------------------------------------
class StatisticsError(Exception):
    pass


# ----- Data classes ------------------------------------------------------------------
@dataclass
class Options:    
    calc_wf_stats: bool = False
    calc_wf_summary: bool = False
    calc_jb_stats: bool = False
    calc_tf_stats: bool = False
    calc_ti_stats: bool = False
    calc_int_stats: bool = False
    time_filter: str = None
    uses_PMC: bool = False

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


#variables for names
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
NEW_LINE_STR = "\n"
DEFAULT_OUTPUT_DIR = "statistics"
FILE_TYPE_TXT = "text"
FILE_TYPE_CSV = "csv"
    
    
    

# Transformations file column 
transformation_stats_col_name = [
    "Workflow_Id",
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

# Integrity file column
integrity_stats_col_name = ["Workflow_Id", "Type", "File Type", "Count", "Total Duration"]
integrity_stats_col_size = [10, 15, 10, 25]

# Jobs file column
job_stats_col_name = [
    "Workflow_Id",
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

# Summary file column 
workflow_summary_col_name = [
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
]
workflow_summary_col_size = [15, 10, 8, 12, 10, 10, 13]

# Workflow file column names
workflow_status_col_name = [
    "Workflow_Id",
    "Type",
    "Succeeded",
    "Failed",
    "Incomplete",
    "Total",
    "Retries",
    "Total+Retries",
    "WF Retries",
]
workflow_status_col_size = [15, 11, 10, 12, 10, 10, 15, 10]

# Time file column
time_stats_col_name = ["Date", "Count", "stat_type", "Runtime (sec)"]
time_stats_col_size = [30, 20, 20]
time_host_stats_col_name = ["Date", "Host","stat_type", "Count", "Runtime (sec)"]
time_host_stats_col_size = [23, 30, 10, 20]


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


def get_row(row, sizes, fmt):
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


def get_workflow_details(output_db_url, wf_uuid, output_dir, multiple_wf=False):
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
            header = get_row(
                workflow_status_col_name_text, workflow_status_col_size, "text"
            )
            write_to_file(wf_stats_file_txt, "a", header)

        if file_type == FILE_TYPE_CSV:
            wf_stats_file_csv = os.path.join(
                output_dir, workflow_statistics_file_name + csv_file_extension
            )
            write_to_file(wf_stats_file_csv, "w", formatted_wf_status_legends())
            header = get_row(
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
                content = get_statistics_by_time_and_host(
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
                content = get_statistics_by_time_and_host(
                    expanded_workflow_stats, "csv", combined=True, per_host=False
                )
                write_to_file(
                    time_stats_file_csv, "w", formatted_time_stats_legends_csv()
                )
                write_to_file(time_stats_file_csv, "a", content)

                time_stats_file2_csv = os.path.join(
                    output_dir, time_statistics_per_host_file_name + csv_file_extension
                )
                content = get_statistics_by_time_and_host(
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
                        content = get_individual_wf_job_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(jobs_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = get_individual_wf_job_stats(
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
                        content = get_wf_transformation_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(transformation_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = get_wf_transformation_stats(
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
                        content = get_wf_integrity_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(integrity_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = get_wf_integrity_stats(
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
                        content = get_individual_workflow_stats(
                            individual_workflow_stats, workflow_id, dax_label, "text"
                        )
                        write_to_file(wf_stats_file_txt, "a", content)

                    if file_type == FILE_TYPE_CSV:
                        content = get_individual_workflow_stats(
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
                summary_output += get_workflow_summary(
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
                summary_output += get_workflow_summary(
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
                summary_output += get_workflow_summary(
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
                content = get_individual_workflow_stats(
                    expanded_workflow_stats, "All Workflows", "", "text"
                )
                write_to_file(wf_stats_file_txt, "a", content)
                stats_output += wf_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = get_individual_workflow_stats(
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
                content = get_wf_transformation_stats(
                    expanded_workflow_stats, "All", "", "text"
                )
                write_to_file(transformation_stats_file_txt, "a", content)
                stats_output += transformation_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = get_wf_transformation_stats(
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
                content = get_wf_integrity_stats(
                    expanded_workflow_stats, "All", "", "text"
                )
                write_to_file(integrity_stats_file_txt, "a", content)
                stats_output += integrity_stats_file_txt + "\n"

            if file_type == FILE_TYPE_CSV:
                content = get_wf_integrity_stats(
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


def get_workflow_summary(
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
                get_row(
                    workflow_summary_col_name_text,
                    workflow_summary_col_size,
                    output_format,
                )
                + "\n"
            )
        else:
            summary_str += (
                get_row(
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
            get_row(content, workflow_summary_col_size, output_format) + "\n"
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
            get_row(content, workflow_summary_col_size, output_format) + "\n"
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
            get_row(content, workflow_summary_col_size, output_format) + "\n"
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
                get_row(workflow_time_summary_col_name_csv, None, output_format)
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


def get_individual_workflow_stats(
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
    wf_status_str = get_row(
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

    tasks_status_str = get_row(content, workflow_status_col_size, output_format)

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

    jobs_status_str = get_row(content, workflow_status_col_size, output_format)

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

    sub_wf_status_str = get_row(content, workflow_status_col_size, output_format)

    if output_format == "text":
        # Only print these in the text format output
        content_str += "".center(sum(workflow_status_col_size), "-") + "\n"
        content_str += wf_status_str + "\n"

    content_str += tasks_status_str + "\n"
    content_str += jobs_status_str + "\n"
    content_str += sub_wf_status_str + "\n"

    return content_str


def get_individual_wf_job_stats(
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
        job_status_str += get_row(job_stats_col_name_text, max_length, output_format)
    else:
        job_status_str += get_row(
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


def get_wf_transformation_stats(stats, workflow_id, dax_label, fmt):
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
            report.append(get_row(columns, transformation_stats_col_size, fmt))

        report.append(get_row(content, transformation_stats_col_size, fmt))

    return NEW_LINE_STR.join(report) + NEW_LINE_STR


def get_wf_integrity_stats(stats, workflow_id, dax_label, fmt):
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
            report.append(get_row(columns, integrity_stats_col_size, fmt))

        report.append(get_row(content, integrity_stats_col_size, fmt))

    return NEW_LINE_STR.join(report) + NEW_LINE_STR


def get_statistics_by_time_and_host(stats, fmt, combined=True, per_host=True):
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
        report.append(get_row(col_names, time_stats_col_size, fmt))
        stats_by_time = stats.get_jobs_run_by_time()
        formatted = stats_utils.convert_stats_to_base_time(stats_by_time, time_filter)
        for s in formatted:
            content = [s["date_format"], str(s["count"]), fstr(s["runtime"])]
            if fmt == "csv":
                content.insert(0, "jobs/" + time_filter)
            report.append(get_row(content, time_stats_col_size, fmt))

        report.append("\n# Invocation statistics run per " + time_filter)
        report.append(get_row(col_names, time_stats_col_size, fmt))
        stats_by_time = stats.get_invocation_by_time()
        formatted = stats_utils.convert_stats_to_base_time(stats_by_time, time_filter)
        for s in formatted:
            content = [s["date_format"], str(s["count"]), fstr(s["runtime"])]
            if fmt == "csv":
                content.insert(0, "invocations/" + time_filter)
            report.append(get_row(content, time_stats_col_size, fmt))

    if per_host is True:
        col_names = time_host_stats_col_name_text
        if fmt == "csv":
            col_names = time_host_stats_col_name_csv

        report.append("\n# Job instances statistics on host per " + time_filter)
        report.append(get_row(col_names, time_host_stats_col_size, fmt))
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
            report.append(get_row(content, time_host_stats_col_size, fmt))

        report.append("\n# Invocation statistics on host per " + time_filter)
        report.append(get_row(col_names, time_host_stats_col_size, fmt))
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
            report.append(get_row(content, time_host_stats_col_size, fmt))

    return "\n".join(report)
