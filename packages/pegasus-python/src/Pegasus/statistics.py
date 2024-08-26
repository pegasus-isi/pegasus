#!/usr/bin/env python3

import atexit
import logging
import os
import typing as t
from collections import namedtuple
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

import click

from Pegasus import braindump
from Pegasus.db import connection
from Pegasus.db.connection import ConnectionError
from Pegasus.db.workflow.stampede_statistics import StampedeStatistics
from Pegasus.db.workflow.stampede_wf_statistics import StampedeWorkflowStatistics
from Pegasus.plots_stats import utils as stats_utils
from Pegasus.tools import utils

log = logging.getLogger("pegasus-statistics")

format_seconds = stats_utils.format_seconds

fstr = stats_utils.round_decimal_to_str


def remove_file(path: t.Union[str, os.PathLike]):
    """
    Remove a file.

    Remove the file identified by the `path` from the file system.

    :param path: The path of the file to be removed.
    :type path: t.Optional[str, os.PathLike]
    """
    try:
        Path(path).unlink()
    except FileNotFoundError:
        log = logging.getLogger("pegasus-statistics")
        log.error("Can't remove file {path} because it was not found")


def istr(value: t.Any) -> str:
    """
    Return a str version of a number.

    :param value: A value representing a number.
    :type value: t.Any
    :return: A `str` representing the number.
    :rtype: str
    """
    return "-" if value is None else str(value)


def pstr(value: t.Any, to: int = 2) -> str:
    """
    Return a percentage str of a number.

    :param value: A value representing a number.
    :type value: t.Any
    :param to: The number of digits to round to, defaults to 2.
    :type to: int, optional
    :return: A `str` representing the percentage.
    :rtype: str
    """
    return "-" if value is None else stats_utils.round_decimal_to_str(value, to) + "%"


@dataclass
class JobStatistics:
    """A data class to hols Job statistics data."""

    #: The name of the job.
    name: str = None
    #: The site where the job ran.
    site: str = None
    #: The actual duration of the job instance in seconds on the remote compute node.
    kickstart: float = None
    #: The multiplier factor specified by the user.
    multiplier_factor: int = None
    #: Kickstart time multiplied by the multiplier factor.
    kickstart_mult: float = None
    #: The remote cpu time computed as the stime + utime.
    remote_cpu_time: float = None
    #: The postscript time as reported by DAGMan.
    post: float = None
    #: The time between submission by DAGMan and the remote Grid submission. It is an estimate of the time spent in the condor q on the submit node.
    condor_delay: float = None
    #: The time between the remote Grid submission and start of remote execution. It is an estimate of the time job spent in the remote queue.
    resource: t.Optional[float] = None
    #: The time spent on the resource as seen by Condor DAGMan. Is always >= Kickstart.
    runtime: float = None
    #: The stime taken for the completion of a clustered job.
    seqexec: t.Optional[float] = None
    #: The time difference between the time for the completion of a clustered job and sum of all the individual tasks Kickstart time.
    seqexec_delay: t.Optional[float] = None
    #: The job retry count.
    retry_count: int = 0
    #: The exitcode for this job.
    exitcode: int = None
    #: The name of the host where the job ran, as reported by Kickstart.
    hostname: str = None

    def get_formatted_statistics(self) -> t.List[str]:
        """Get formatted job statistics data."""
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
            self.hostname or "None",
        ]


@dataclass
class TransformationStatistics:
    """A data class to hols Transformation statistics data."""

    #: The transformation name.
    transformation: str = None
    #: The transformation type, i.e., successful or failed
    type: str = None
    #: The number of times the invocations corresponding to the transformation was executed.
    count: int = None
    #: The minimum invocation runtime value corresponding to the transformation.
    min: float = None
    #: The maximum invocation runtime value corresponding to the transformation.
    max: float = None
    #: The mean of the invocation runtime corresponding to the transformation.
    avg: float = None
    #: The cumulative of invocation runtime corresponding to the transformation.
    sum: float = None
    #: The minimum of the max. resident set size (RSS) value corresponding to the transformation. In MB.
    min_maxrss: float = None
    #: The maximum of the max. resident set size (RSS) value corresponding to the transformation. In MB.
    max_maxrss: float = None
    #: The mean of the max. resident set size (RSS) value corresponding to the transformation. In MB.
    avg_maxrss: float = None
    #: The minimum of the average cpu utilization value corresponding to the transformation.
    min_avg_cpu: float = None
    #: The maximum of the average cpu utilization value corresponding to the transformation.
    max_avg_cpu: float = None
    #: The mean of the average cpu utilization value corresponding to the transformation.
    avg_avg_cpu: float = None

    def get_formatted_statistics(self) -> t.List[str]:
        """Get formatted transformations statistics data."""
        return [
            self.transformation,
            self.type,
            str(self.count),
            fstr(self.min),
            fstr(self.max),
            fstr(self.avg),
            fstr(self.sum),
            fstr(self.min_maxrss / 1024) if self.min_maxrss else "-",
            fstr(self.max_maxrss / 1024) if self.max_maxrss else "-",
            fstr(self.avg_maxrss / 1024) if self.avg_maxrss else "-",
            pstr(self.min_avg_cpu * 100) if self.min_avg_cpu else "-",
            pstr(self.max_avg_cpu * 100) if self.max_avg_cpu else "-",
            pstr(self.avg_avg_cpu * 100) if self.avg_avg_cpu else "-",
        ]


class PegasusStatisticsError(Exception):
    """Pegasus Statistics Error"""


class PegasusStatistics:
    #: The name of the default output directory.
    default_output_dir: str = "statistics"

    #: Text file type.
    file_type_txt: str = "text"
    #: Text file extension.
    file_extn_text: str = "txt"

    #: Comma separated file type.
    file_type_csv: str = "csv"
    #: Comma separated file extension.
    file_extn_csv: str = "csv"

    # Summary

    #: The file name for workflow summary.
    workflow_summary_file_name: str = "summary"
    #: Legend for workflow summary stats.
    workflow_summary_legends: str = """#
# Pegasus Workflow Management System - https://pegasus.isi.edu
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
#       succeeded and failed count.
"""
    #: The column names for workflow summary file of type text.
    workflow_summary_col_name_text: t.Tuple[str, ...] = (
        "Type",
        "Succeeded",
        "Failed",
        "Incomplete",
        "Total",
        "Retries",
        "Total+Retries",
    )
    #: Column widths for workflow summary file.
    workflow_summary_col_size = (15, 10, 8, 12, 10, 10, 13)
    #: The column names for workflow summary file of type csv.
    workflow_summary_col_name_csv: t.Tuple[str, ...] = (
        "Type",
        "Succeeded",
        "Failed",
        "Incomplete",
        "Total",
        "Retries",
        "Total+Retries",
    )

    #: The file name for workflow time summary.
    workflow_summary_time_file_name: str = "summary-time"
    #: Legend for workflow summary time stats.
    workflow_summary_time_legends: str = """# Workflow wall time:
#   The wall time from the start of the workflow execution to the end as
#   reported by the DAGMAN. In case of rescue dag the value is the
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
#   from the sub workflows as well.
"""
    #: The column names for workflow summary file of type csv.
    workflow_summary_time_col_name_csv: t.Tuple[str, ...] = (
        "stat_type",
        "time_seconds",
    )

    # Workflow Summary

    #: The file name for workflow statistics.
    workflow_stats_file_name: str = "workflow"
    #: Legend for workflow stats
    workflow_stats_legends: str = """# Workflow summary
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
    #: The column names for workflow statistics file of type text.
    workflow_stats_col_name_text: t.Tuple[str, ...] = (
        "Type",
        "Succeeded",
        "Failed",
        "Incomplete",
        "Total",
        "Retries",
        "Total+Retries",
        "WF Retries",
    )
    #: Column widths for workflow statistics file.
    workflow_stats_col_size: t.Tuple[int, ...] = (15, 11, 10, 12, 10, 10, 15, 10)
    #: The column names for workflow statistics file of type csv.
    workflow_stats_col_name_csv: t.Tuple[str, ...] = (
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
    )

    # Jobs Breakdown

    #: The file name for job statistics.
    job_stats_file_name: str = "jobs"
    #: Legend for job stats.
    job_stats_legends: str = """# Job            - name of the job
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
#                  Kickstart
"""
    #: The column names for job statistics file of type text.
    job_stats_col_name_text: t.Tuple[str, ...] = (
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
    )
    #: Column widths for job statistics file.
    job_stats_col_size: t.Tuple[int, ...] = (
        35,
        4,
        12,
        12,
        6,
        16,
        12,
        6,
        12,
        12,
        12,
        12,
        15,
        10,
        30,
    )
    #: The column names for job statistics file of type csv.
    job_stats_col_name_csv: t.Tuple[str, ...] = (
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
    )

    # Transformation

    #: The file name for transformation statistics.
    transformation_stats_file_name: str = "breakdown"
    #: Legend for transformation statistics.
    transformation_stats_legends: str = """# Transformation   - name of the transformation.
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
#                    to the transformation.
"""
    #: The column names for transformation statistics file of type text.
    transformation_stats_col_name_text: t.Tuple[str, ...] = (
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
    )
    #: Column widths for transformation statistics file.
    transformation_stats_col_size: t.Tuple[int, ...] = (
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
    )
    #: The column names for transformation statistics file of type csv.
    transformation_stats_col_name_csv: t.Tuple[str, ...] = (
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
    )

    # Integrity

    #: The file name for integrity statistics.
    integrity_stats_file_name: str = "integrity"
    #: Legend for integrity stats.
    integrity_stats_legends: str = """# Integrity Statistics Breakdown
# Type           - the type of integrity metric.
                check - means checksum was compared for a file,
                compute - means a checksum was generated for a file
# File Type      - the type of file: input or output from a job perspective
# Count          - the number of times done
# Total Duration - sum of duration in seconds for the 'count' number
                of records matching the particular type,
                file-type combo
"""
    #: The column names for integrity statistics file of type text.
    integrity_stats_col_name_text: t.Tuple[str, ...] = (
        "Type",
        "File Type",
        "Count",
        "Total Duration",
    )
    #: Column widths for integrity statistics file.
    integrity_stats_col_size: t.Tuple[int, ...] = (10, 15, 10, 25)
    #: The column names for integrity statistics file of type csv.
    integrity_stats_col_name_csv: t.Tuple[str, ...] = (
        "Workflow_Id",
        "Dax_Label",
        "Type",
        "File Type",
        "Count",
        "Total Duration",
    )

    # Time

    #: The file name for time statistics.
    time_stats_file_name: str = "time"
    #: Legend for time stats (txt).
    time_stats_legends_text: str = """# Job instance statistics per %(time_filter)s:
#   the number of job instances run, total runtime sorted by %(time_filter)s
# Invocation statistics per %(time_filter)s:
#   the number of invocations , total runtime sorted by %(time_filter)s
# Job instance statistics by host per %(time_filter)s:
#   the number of job instance run, total runtime on each host sorted by %(time_filter)s
# Invocation by host per %(time_filter)s:
#   the number of invocations, total runtime on each host sorted by %(time_filter)s"""
    #: Legend for time stats (csv).
    time_stats_legends_csv: str = """# Job instance statistics per %(time_filter)s:
#   the number of job instances run, total runtime sorted by %(time_filter)s
# Invocation statistics per %(time_filter)s:
#   the number of invocations , total runtime sorted by %(time_filter)s
"""
    #: The column names for time statistics file of type text.
    time_stats_col_name_text: t.List[str] = [
        "Date%(tf_format)s",
        "Count",
        "Runtime (sec)",
    ]
    #: Column widths for time statistics file.
    time_stats_col_size: t.Tuple[int, ...] = (30, 20, 20)
    #: The column names for time statistics file of type csv.
    time_stats_col_name_csv: t.List[str] = [
        "stat_type",
        "date%(tf_format)s",
        "count",
        "runtime (sec)",
    ]

    #: The file name for host statistics.
    time_host_stats_file_name: str = "time-per-host"
    #: Legend for time host stats.
    time_host_stats_legends: str = """# Job instance statistics by host per %(time_filter)s:
#   the number of job instance run, total runtime on each host sorted by %(time_filter)s
# Invocation by host per %(time_filter)s:
#   the number of invocations, total runtime on each host sorted by %(time_filter)s
"""
    #: The column names for host statistics file of type text.
    time_host_stats_col_name_text: t.List[str] = [
        "Date%(tf_format)s",
        "Host",
        "Count",
        "Runtime (sec)",
    ]
    #: Column widths for host statistics file.
    time_host_stats_col_size: t.Tuple[int, ...] = (23, 30, 10, 20)
    #: The column names for host statistics file of type csv.
    time_host_stats_col_name_csv: t.List[str] = [
        "stat_type",
        "date%(tf_format)s",
        "host",
        "count",
        "runtime (sec)",
    ]

    def __init__(
        self,
        output_dir: str = default_output_dir,
        filetype: str = file_type_txt,
        config_properties=None,
        statistics_level: set = {"summary"},
        time_filter: str = "day",
        ignore_db_inconsistency: bool = False,
        multiple_wf: bool = False,
        is_pmc: bool = False,
        is_uuid: bool = False,
        submit_dirs: t.Sequence[str] = [],
    ):
        """Initialize the Pegasus statistics object."""
        self.log = logging.getLogger("pegasus-statistics")

        self.output_dir = output_dir
        self.file_type = filetype or self.file_type_txt
        self.config_properties = config_properties
        self.statistics_level = statistics_level or {"summary"}
        self.time_filter = time_filter or "day"
        self.ignore_db_inconsistency = ignore_db_inconsistency
        self.multiple_wf = multiple_wf
        self.is_pmc = is_pmc
        self.is_uuid = is_uuid
        self.submit_dirs: t.Union[str, t.Sequence[str]] = submit_dirs or []
        self.wf_uuids: t.Union[str, t.Sequence[str]] = []

        self.wf_uuid_list: t.Sequence[t.Tuple[str, StampedeStatistics]] = []
        self.wf_stats: t.Any = None
        self.errors: int = 0

    def _initialize(self, verbose: int = 0, quiet: int = 0):
        """Initialize the Pegasus statistics object."""
        # Initialize logging
        utils.configure_logging(verbose, quiet)

        if not self.submit_dirs:
            self.submit_dirs = "*" if self.multiple_wf is True else "."
        elif len(self.submit_dirs) > 1:
            if self.multiple_wf is False:
                self.log.warning(
                    "Multiple submit-dirs are specified, but multiple-wf flag is not set."
                )
            self.multiple_wf = True
        else:
            if self.multiple_wf is True:
                self.log.warning(
                    "Single submit-dir is specified, but multiple-wf flag is set."
                )
            self.multiple_wf = False
            self.submit_dirs = self.submit_dirs[0]

        if self.is_uuid is True or self.submit_dirs == "*":
            self.wf_uuids = self.submit_dirs

        self.file_extn: str = (
            self.file_extn_text if self.file_type == "text" else self.file_extn_csv
        )

        self.tf_format = stats_utils.get_date_print_format(self.time_filter)

        self.log.info(f"File type is {self.file_type}")
        self.log.info(f"Time filter is {self.time_filter}")
        self.log.info(f"Time format is {self.tf_format}")

        # Identify what statistics need to be computed
        self._initialize_statistics_levels()

        # Register cleanup function
        atexit.register(self._cleanup)

    def _check_braindump(self, submit_dir):
        """Load and return the braindump file."""
        try:
            with (Path(submit_dir) / "braindump.yml").open("r") as f:
                braindb = braindump.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Not a workflow submit directory {submit_dir}")

        return braindb

    def _check_inputs(self):
        """Validate the command line arguments."""
        self._check_args()

        # Check if the submit directory is valid
        # i.e. a braindump file exists
        self._check_workflow_dir()

        # Check if the run conditions are suitable
        # i.e. the workflow is not running
        self._check_workflow_state()

    def _check_args(self):
        """Validate the command line arguments."""
        if self.multiple_wf and self.calc_jb_stats:
            msg = "Job breakdown statistics cannot be computed over multiple workflows"
            self.log.critical(msg)
            raise PegasusStatisticsError(msg)

        if (self.is_uuid or self.submit_dirs == "*") and not self.config_properties:
            msg = "A config file is required if either is-uuid flag is set or submit-dirs is not set or set to *"
            self.log.critical(msg)
            raise PegasusStatisticsError(msg)

        if (self.multiple_wf or self.is_uuid) and not self.output_dir:
            msg = "Output directory option is required when calculating statistics over multiple workflows."
            self.log.critical(msg)
            raise PegasusStatisticsError(msg)

    def _check_workflow_dir(self):
        """Identify the workflow directory from the braindump."""
        if self.is_uuid is True or self.submit_dirs == "*":
            return

        if self.multiple_wf:
            # Check for braindump file's existence if workflows are not specified as UUIDs and
            # statistics need to be calculated only on a sub set of workflows
            for dir in self.submit_dirs:
                braindb = self._check_braindump(dir)
                self.wf_uuids.append(braindb.wf_uuid)
        else:
            braindb = self._check_braindump(self.submit_dirs)
            self.wf_uuids = braindb.wf_uuid

    def _check_workflow_state(self):
        """Check whether the workflow is running or not."""

        if self.ignore_db_inconsistency:
            self.log.warning("Ignoring db inconsistency")
            self.log.warning(
                "The tool is meant to be run after the workflow completion."
            )
            return

        if self.is_uuid is True or self.submit_dirs == "*":
            if self.submit_dirs == "*":
                self.log.warning(
                    "Statistics have to be calculated on all workflows. Tool cannot check to see if all of them have finished. Ensure that all workflows have finished"
                )
            return

        def loading_complete(dir):
            if not utils.loading_completed(dir):
                if utils.monitoring_running(dir):
                    msg = "pegasus-monitord still running. Please wait for it to complete."
                    self.log.warning(msg)
                    click.echo(msg, err=True)
                else:
                    msg = "Please run pegasus monitord in replay mode."
                    click.echo(msg, err=True)
                raise PegasusStatisticsError(msg)

        if self.multiple_wf:
            for dir in self.submit_dirs:
                loading_complete(dir)
        else:
            loading_complete(self.submit_dirs)

    def _initialize_statistics_levels(self):
        """Initialize the types of statistics to be generated."""
        sl = self.statistics_level
        self.log.info(f"Statistics level(s) are {', '.join(sl)}")

        self.calc_all = "all" in sl
        self.calc_wf_summary = self.calc_all or "summary" in sl
        self.calc_wf_stats = self.calc_all or "wf_stats" in sl
        self.calc_jb_stats = (
            self.calc_all and not self.multiple_wf
        ) or "jb_stats" in sl
        self.calc_tf_stats = self.calc_all or "tf_stats" in sl
        self.calc_ti_stats = self.calc_all or "ti_stats" in sl
        self.calc_int_stats = self.calc_all or "int_stats" in sl

        self.calc_ind_stats = any(
            (
                self.calc_jb_stats,
                self.calc_tf_stats,
                self.calc_wf_stats,
                self.calc_int_stats,
            )
        )

    def _get_clustering_type(self):
        """Identify the clustering type of the workflow(s)."""

        def use_pmc(dir):
            braindb = self._check_braindump(dir)
            return braindb.uses_pmc is True

        if self.is_pmc:
            self.log.info("Calculating statistics with use of PMC clustering")
        elif self.is_uuid:
            # User provided workflow UUID
            self.log.info(
                "Workflows are specified as UUIDs and is_pmc option is not set."
            )
        else:
            # User provided workflow submit directories
            if self.multiple_wf:
                if self.submit_dirs == "*":
                    self.log.info(
                        "Calculating statistics over all workflows, and is_pmc option is not set."
                    )
                else:
                    # int(True) -> 1
                    tmp = sum(int(use_pmc(dir)) for dir in self.submit_dirs)

                    # All workflows are either PMC or non PMC
                    if tmp == 0 or tmp == len(self.submit_dirs):
                        self.is_pmc = tmp != 0
                    else:
                        self.log.warning(
                            "Input workflows use both PMC & regular clustering! Calculating statistics with regular clustering"
                        )
            else:
                self.is_pmc = use_pmc(self.submit_dirs)

    def _get_workflow_db_url(self):
        """Get workflow database URL."""
        if self.is_uuid or self.submit_dirs == "*":
            try:
                # URL picked from config_properties file.
                self.output_db_url = connection.url_by_properties(
                    self.config_properties, connection.DBType.WORKFLOW
                )
            except ConnectionError:
                msg = 'Unable to determine database URL. Kindly specify a value for "pegasus.monitord.output" property'
                self.log.error(msg)
                raise PegasusStatisticsError(msg)
        elif self.multiple_wf:
            try:
                db_url_set = set()

                for dir in self.submit_dirs:
                    db_url = connection.url_by_submitdir(
                        dir, connection.DBType.WORKFLOW, self.config_properties
                    )
                    db_url_set.add(db_url)

                if len(db_url_set) != 1:
                    msg = "Workflows are distributed across multiple databases, which is not supported"
                    self.log.error(msg)
                    raise PegasusStatisticsError(msg)

                self.output_db_url = db_url_set.pop()
            except ConnectionError:
                msg = "Unable to determine database URL."
                self.log.error(msg)
                raise PegasusStatisticsError()
        else:
            try:
                self.output_db_url = connection.url_by_submitdir(
                    self.submit_dirs, connection.DBType.WORKFLOW, self.config_properties
                )
            except ConnectionError:
                msg = "Unable to determine database URL."
                self.log.error(msg)
                raise PegasusStatisticsError(msg)

        self.log.info("DB URL is: %s" % self.output_db_url)
        _uuids = (
            self.wf_uuids
            if isinstance(self.wf_uuids, str)
            else ", ".join(self.wf_uuids)
        )
        self.log.info(f"Workflow UUID is: {_uuids}")

    def _initialize_output_dir(self):
        """Initialize the output directory."""
        if self.output_dir:
            delete_if_exists = False
            self.output_dir = Path(self.output_dir)
        else:
            delete_if_exists = True
            self.output_dir = Path(self.submit_dirs, self.default_output_dir)

        self.log.info("Output directory is %s" % self.output_dir)
        utils.create_directory(str(self.output_dir), delete_if_exists=delete_if_exists)

    def _compute_statistics(self):
        """Initialize database connections."""
        try:
            self.wf_stats = (
                StampedeWorkflowStatistics(self.output_db_url)
                if self.multiple_wf
                else StampedeStatistics(self.output_db_url)
            )
            _wf_found = self.wf_stats.initialize(self.wf_uuids)

            if _wf_found is False:
                msg = "Workflow {!r} not found in database {!r}".format(
                    self.wf_uuids, self.output_db_url
                )
                click.echo(msg)
                raise PegasusStatisticsError(msg)

            WorkflowDetails = namedtuple("WorkflowDetails", ["wf_uuid", "dax_label"])
            self._wf_det = WorkflowDetails("All", "")

            if self.multiple_wf:
                _wf_uuid_list = [_.wf_uuid for _ in self.wf_stats.get_workflow_ids()]
            else:
                _wf_uuid_list = [self.wf_uuids]
                _wf_uuid_list.extend(
                    [_.wf_uuid for _ in self.wf_stats.get_descendant_workflow_ids()]
                )

            if self.calc_ind_stats:
                self.wf_uuid_list = []
                for wf_uuid in _wf_uuid_list:
                    ind_wf_stats = StampedeStatistics(self.output_db_url, False)
                    _wf_found = ind_wf_stats.initialize(wf_uuid)

                    if _wf_found is False:
                        msg = f"Workflow {wf_uuid} not found in database {self.output_db_url}"
                        click.echo(msg)
                        raise PegasusStatisticsError(msg)

                    _wf_det = ind_wf_stats.get_workflow_details()[0]
                    self.wf_uuid_list.append((_wf_det, ind_wf_stats))
        except Exception:
            msg = f"Failed to load the database {self.output_db_url}"
            self.log.error(msg)
            self.log.debug(msg, exc_info=1)
            raise PegasusStatisticsError(msg)

    def _compute_task_summary_statistics(self, ind_or_wf_stats):
        """Get task summary data from the database."""
        # status
        ind_or_wf_stats.set_job_filter("nonsub")

        # Tasks
        total = ind_or_wf_stats.get_total_tasks_status()
        succeeded = ind_or_wf_stats.get_total_succeeded_tasks_status(self.is_pmc)
        failed = ind_or_wf_stats.get_total_failed_tasks_status()
        unsubmitted = total - (succeeded + failed)
        retries = ind_or_wf_stats.get_total_tasks_retries()
        tries = succeeded + failed + retries

        return (succeeded, failed, unsubmitted, total, retries, tries)

    def _compute_job_summary_statistics(self, ind_or_wf_stats, filter="nonsub"):
        """Get job summary statistics data from the database."""
        ind_or_wf_stats.set_job_filter(filter)

        # Jobs
        total = ind_or_wf_stats.get_total_jobs_status()
        _total_succeeded_failed_jobs = (
            ind_or_wf_stats.get_total_succeeded_failed_jobs_status()
        )
        succeeded = _total_succeeded_failed_jobs.succeeded or 0
        failed = _total_succeeded_failed_jobs.failed or 0
        unsubmitted = total - (succeeded + failed)
        retries = ind_or_wf_stats.get_total_jobs_retries()
        tries = succeeded + failed + retries

        return (succeeded, failed, unsubmitted, total, retries, tries)

    def _compute_sub_wf_summary_statistics(self, ind_or_wf_stats):
        """Get sub-workflow data from the database."""
        return self._compute_job_summary_statistics(ind_or_wf_stats, "subwf")

    def _compute_workflow_retries(self, ind_of_wf_stats):
        """Get workflow retries data from the database."""
        ind_of_wf_stats.set_job_filter("all")
        total_wf_retries = ind_of_wf_stats.get_workflow_retries()
        return total_wf_retries

    def _compute_time_summary_statistics(self):
        """Get time summary data from the database."""
        states = self.wf_stats.get_workflow_states()
        wwt = stats_utils.get_workflow_wall_time(states)
        wcjwt, wcgpt, wcbpt = self.wf_stats.get_workflow_cum_job_wall_time()
        ssjwt, ssgpt, ssbpt = self.wf_stats.get_submit_side_job_wall_time()

        return (wwt, wcjwt, wcgpt, wcbpt, ssjwt, ssgpt, ssbpt)

    def _compute_integrity_summary_statistics(self):
        """Get integrity summary statistics data from the database."""
        total_failed_jobs_integrity = (
            self.wf_stats.get_total_succeeded_failed_jobs_status(
                classify_error=True, tag="int.error"
            )
        )
        int_metrics_summary = self.wf_stats.get_summary_integrity_metrics()
        int_error_summary = self.wf_stats.get_tag_metrics("int.error")

        return (int_metrics_summary, int_error_summary, total_failed_jobs_integrity)

    def _compute_job_statistics(self, ind_wf_stats):
        """Get job statistics data from the database."""
        ind_wf_stats.set_job_filter("all")

        wf_job_stats_list = ind_wf_stats.get_job_statistics()

        # Go through each job in the workflow
        job_stats_list = []
        job_retry_count_dict = dict()
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

            job_stats_list.append(job_stats)

        return job_stats_list

    def _compute_transformation_statistics(self, wf_or_ind_stats):
        """Get transformation statistics data from the database."""
        wf_or_ind_stats.set_job_filter("all")

        wf_transformation_stats_list = wf_or_ind_stats.get_transformation_statistics()

        # Go through each transformation in the workflow
        transformation_stat_list = []
        for transformation_stat in wf_transformation_stats_list:
            transformation_stats = TransformationStatistics()
            transformation_stats.transformation = transformation_stat.transformation
            transformation_stats.type = transformation_stat.type
            transformation_stats.count = transformation_stat.count
            transformation_stats.min = transformation_stat.min
            transformation_stats.max = transformation_stat.max
            transformation_stats.avg = transformation_stat.avg
            transformation_stats.sum = transformation_stat.sum
            transformation_stats.min_maxrss = transformation_stat.min_maxrss
            transformation_stats.max_maxrss = transformation_stat.max_maxrss
            transformation_stats.avg_maxrss = transformation_stat.avg_maxrss
            transformation_stats.min_avg_cpu = transformation_stat.min_avg_cpu
            transformation_stats.max_avg_cpu = transformation_stat.max_avg_cpu
            transformation_stats.avg_avg_cpu = transformation_stat.avg_avg_cpu
            transformation_stat_list.append(transformation_stats)

        return transformation_stat_list

    def _compute_integrity_statistics(self, wf_or_ind_stats):
        """Get integrity statistics data from the database."""
        wf_or_ind_stats.set_job_filter("all")

        return wf_or_ind_stats.get_integrity_metrics()

    def _compute_time_statistics(self):
        """Get time statistics data from the database."""
        self.wf_stats.set_job_filter("nonsub")
        self.wf_stats.set_time_filter("hour")
        self.wf_stats.set_transformation_filter(exclude=["condor::dagman"])

        # If combined is True
        jobs_by_time = self.wf_stats.get_jobs_run_by_time()
        invocations_by_time = self.wf_stats.get_invocation_by_time()

        # If per_host is True
        jobs_per_host = self.wf_stats.get_jobs_run_by_time_per_host()
        invocations_per_host = self.wf_stats.get_invocation_by_time_per_host()

        return jobs_by_time, invocations_by_time, jobs_per_host, invocations_per_host

    def _write_statistics(self):
        """Generate Pegasus statistics."""
        extn = self.file_extn

        if self.calc_wf_summary:
            try:
                self._write_summary_statistics()
            except Exception:
                self.log.warning("summary statistics generation failed")
                self.log.debug("summary statistics generation failed", exc_info=1)

                remove_file(
                    self.output_dir / f"{self.workflow_summary_file_name}.{extn}"
                )
                if extn == "csv":
                    remove_file(
                        self.output_dir
                        / f"{self.workflow_summary_time_file_name}.{extn}"
                    )

                self.calc_wf_summary = False
                self.errors += 1

        if self.calc_ti_stats:
            try:
                self._write_time_statistics()
            except Exception:
                self.log.warning("time statistics generation failed")
                self.log.debug("time statistics generation failed", exc_info=1)

                remove_file(self.output_dir / f"{self.time_stats_file_name}.{extn}")
                if extn == "csv":
                    remove_file(
                        self.output_dir / f"{self.time_host_stats_file_name}.{extn}"
                    )

                self.calc_ti_stats = False
                self.errors += 1

        if self.calc_jb_stats:
            try:
                self._write_job_statistics()
            except Exception:
                self.log.warning("job instance statistics generation failed")
                self.log.debug("job instance statistics generation failed", exc_info=1)

                remove_file(self.output_dir / f"{self.job_stats_file_name}.{extn}")

                self.calc_jb_stats = False
                self.errors += 1

        if self.calc_tf_stats:
            try:
                self._write_transformation_statistics()
            except Exception:
                self.log.warning("transformation statistics generation failed")
                self.log.debug(
                    "transformation statistics generation failed", exc_info=1
                )

                remove_file(
                    self.output_dir / f"{self.transformation_stats_file_name}.{extn}"
                )

                self.calc_tf_stats = False
                self.errors += 1

        if self.calc_int_stats:
            try:
                self._write_integrity_statistics()
            except Exception:
                self.log.warning("integrity statistics generation failed")
                self.log.debug("integrity statistics generation failed", exc_info=1)

                remove_file(
                    self.output_dir / f"{self.integrity_stats_file_name}.{extn}"
                )

                self.calc_int_stats = False
                self.errors += 1

        if self.calc_wf_stats:
            try:
                self._write_workflow_statistics()
            except Exception:
                self.log.warning("workflow statistics generation failed")
                self.log.debug("workflow statistics generation failed", exc_info=1)

                remove_file(self.output_dir / f"{self.workflow_stats_file_name}.{extn}")

                self.calc_wf_stats = False
                self.errors += 1

    def _write_summary_statistics(self):
        """Write summary statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_summary_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_summary_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_summary_statistics_text(self):
        """Write summary statistics data to a txt file."""
        tasks = ("Tasks",) + self._compute_task_summary_statistics(self.wf_stats)
        jobs = ("Jobs",) + self._compute_job_summary_statistics(self.wf_stats)
        sub_wfs = ("Sub-Workflows",) + self._compute_sub_wf_summary_statistics(
            self.wf_stats
        )

        (
            wwt,
            wcjwt,
            _wcgpt,
            wcbpt,
            ssjwt,
            _ssgpt,
            ssbpt,
        ) = self._compute_time_summary_statistics()
        (
            int_metrics_summary,
            int_error_summary,
            total_failed_jobs_integrity,
        ) = self._compute_integrity_summary_statistics()

        with utils.write_table(
            Path(self.output_dir)
            / f"{self.workflow_summary_file_name}.{self.file_extn}",
            fields=self.workflow_summary_col_name_text,
            widths=self.workflow_summary_col_size,
        ) as writer:
            # Workflow summary
            writer.write(self.workflow_summary_legends)
            writer.write(self.workflow_summary_time_legends)
            writer.write("".center(sum(self.workflow_summary_col_size), "-") + "\n")
            writer.writeheader()
            writer.writerow(tasks)
            writer.writerow(jobs)
            writer.writerow(sub_wfs)
            writer.write("".center(sum(self.workflow_summary_col_size), "-") + "\n\n")

            # Time summary
            if not self.multiple_wf:
                writer.write(
                    "{:<57}: {}\n".format("Workflow wall time", format_seconds(wwt))
                )

            writer.write(
                "{:<57}: {}\n".format("Cumulative job wall time", format_seconds(wcjwt))
            )
            writer.write(
                "{:<57}: {}\n".format(
                    "Cumulative job wall time as seen from submit side",
                    format_seconds(ssjwt),
                )
            )
            writer.write(
                "{:<57}: {}\n".format(
                    "Cumulative job badput wall time", format_seconds(wcbpt)
                )
            )
            writer.write(
                "{:<57}: {}\n".format(
                    "Cumulative job badput wall time as seen from submit side",
                    format_seconds(ssbpt),
                )
            )

            # Integrity summary
            if not int_metrics_summary:
                return

            writer.write(
                """
# Integrity Metrics
# Number of files for which checksums were compared/computed along with total time spent doing it.\n"""
            )
            for result in int_metrics_summary:
                type = result.type
                if result.type == "check":
                    type = "compared"
                if result.type == "compute":
                    type = "generated"
                writer.write(
                    f"{result.count} files checksums {type} with total duration of {format_seconds(result.duration)}\n"
                )

            writer.write(
                """
# Integrity Errors
# Total:
#       Total number of integrity errors encountered across all job executions (including retries) of a workflow.
# Failures:
#       Number of failed jobs where the last job instance had integrity errors.
"""
            )
            for result in int_error_summary:
                type = "integrity" if result.name == "int.error" else result.name
                writer.write(
                    f"Total:    A total of {result.count} {type} errors encountered in the workflow\n"
                )

            # PM-1295 jobs where last_job_instance failed and there were integrity errors in those last job instances
            writer.write(
                "Failures: %s job failures had integrity errors\n"
                % (total_failed_jobs_integrity.failed or 0)
            )

    def _write_summary_statistics_csv(self):
        """Write summary statistics data to a csv file."""
        tasks = ("Tasks",) + self._compute_task_summary_statistics(self.wf_stats)
        jobs = ("Jobs",) + self._compute_job_summary_statistics(self.wf_stats)
        sub_wfs = ("Sub-Workflows",) + self._compute_sub_wf_summary_statistics(
            self.wf_stats
        )

        (
            wwt,
            wcjwt,
            _wcgpt,
            wcbpt,
            ssjwt,
            _ssgpt,
            ssbpt,
        ) = self._compute_time_summary_statistics()

        with utils.write_csv(
            Path(self.output_dir)
            / f"{self.workflow_summary_file_name}.{self.file_extn}",
            fields=self.workflow_summary_col_name_csv,
        ) as writer:
            writer.write(self.workflow_summary_legends + "\n")
            writer.writeheader()
            writer.writerow(tasks)
            writer.writerow(jobs)
            writer.writerow(sub_wfs)

        with utils.write_csv(
            Path(self.output_dir)
            / f"{self.workflow_summary_time_file_name}.{self.file_extn}",
            fields=self.workflow_summary_time_col_name_csv,
        ) as writer:

            def myfmt(val):
                return "" if val is None else val

            writer.write(self.workflow_summary_time_legends)
            writer.writeheader()
            writer.writerow(("workflow_wall_time", myfmt(wwt)))
            writer.writerow(("workflow_cumulative_job_wall_time", myfmt(wcjwt)))
            writer.writerow(("cumulative_job_walltime_from_submit_side", myfmt(ssjwt)))
            writer.writerow(("workflow_cumulative_badput_time", myfmt(wcbpt)))
            writer.writerow(
                ("cumulative_job_badput_walltime_from_submit_side", myfmt(ssbpt))
            )

    def _write_workflow_statistics(self):
        """Write workflow statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_workflow_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_workflow_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_workflow_statistics_text(self):
        """Write workflow statistics data to a txt file."""
        with utils.write_table(
            Path(self.output_dir) / f"{self.workflow_stats_file_name}.{self.file_extn}",
            fields=self.workflow_stats_col_name_text,
            widths=self.workflow_stats_col_size,
        ) as writer:
            writer.write(self.workflow_stats_legends)

            writer.writeheader()
            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating workflow information for workflow {wf_det.wf_uuid} ..."
                )

                wf_retries = self._compute_workflow_retries(ind_wf_stats)
                tasks = ("Tasks",) + self._compute_task_summary_statistics(ind_wf_stats)
                jobs = ("Jobs",) + self._compute_job_summary_statistics(ind_wf_stats)
                sub_wfs = ("Sub-Workflows",) + self._compute_sub_wf_summary_statistics(
                    ind_wf_stats
                )

                # Workflow status
                wf_retry_width = (
                    sum(self.workflow_stats_col_size)
                    - self.workflow_stats_col_size[-1]
                    + 1
                )

                if wf_det.wf_uuid == "All":
                    wf_label = "All Workflows"
                else:
                    wf_label = f"{wf_det.wf_uuid} ({wf_det.dax_label})"

                wf_retry_width -= len(wf_label)

                writer.write("".center(sum(self.workflow_stats_col_size), "-") + "\n")
                writer.write(
                    f"{wf_label}{str(wf_retries).rjust(wf_retry_width)}" + "\n"
                )

                writer.writerow(tasks)
                writer.writerow(jobs)
                writer.writerow(sub_wfs)
                if wf_det.wf_uuid != "All":
                    writer.write("\n")

    def _write_workflow_statistics_csv(self):
        """Write workflow statistics data to a csv file."""
        with utils.write_csv(
            Path(self.output_dir) / f"{self.workflow_stats_file_name}.{self.file_extn}",
            fields=self.workflow_stats_col_name_csv,
        ) as writer:
            writer.write(self.workflow_stats_legends)

            writer.writeheader()
            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating workflow information for workflow {wf_det.wf_uuid} ..."
                )

                wf_retries = self._compute_workflow_retries(ind_wf_stats)
                wf_uuid = wf_det.wf_uuid if wf_det.wf_uuid != "All" else "ALL"
                tasks = (
                    wf_uuid,
                    wf_det.dax_label,
                    "Tasks",
                ) + self._compute_task_summary_statistics(ind_wf_stats)
                jobs = (
                    wf_uuid,
                    wf_det.dax_label,
                    "Jobs",
                ) + self._compute_job_summary_statistics(ind_wf_stats)
                sub_wfs = (
                    wf_uuid,
                    wf_det.dax_label,
                    "Sub-Workflows",
                ) + self._compute_sub_wf_summary_statistics(ind_wf_stats)

                writer.writerow(tasks + (wf_retries,))
                writer.writerow(jobs + (wf_retries,))
                writer.writerow(sub_wfs + (wf_retries,))
                if wf_det.wf_uuid != "All":
                    writer.write("\n")

    def _write_job_statistics(self):
        """Write job statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_job_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_job_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_job_statistics_text(self):
        """Write job statistics data to a txt file."""
        with utils.write_table(
            Path(self.output_dir) / f"{self.job_stats_file_name}.{self.file_extn}",
            fields=self.job_stats_col_name_text,
            widths=self.job_stats_col_size,
        ) as writer:
            writer.write(self.job_stats_legends)

            for wf_det, ind_wf_stats in self.wf_uuid_list:
                self.log.debug(
                    f"Generating job instance statistics information for workflow {wf_det.wf_uuid} ..."
                )
                writer.write(f"# {wf_det.wf_uuid} ({wf_det.dax_label})\n")
                max_length = [_ for _ in self.job_stats_col_size]
                job_stats_list = self._compute_job_statistics(ind_wf_stats)

                for i, job_stats in enumerate(job_stats_list):
                    # Compute max length
                    job_stats_list[i] = job_stats = job_stats.get_formatted_statistics()
                    for i in range(15):
                        max_length[i] = max(max_length[i], len(job_stats[i]))

                max_length = [i + 1 for i in max_length]
                writer.widths = max_length

                writer.writeheader()
                for job_stats in job_stats_list:
                    writer.writerow(job_stats)

    def _write_job_statistics_csv(self):
        """Write job statistics data to a csv file."""
        with utils.write_csv(
            Path(self.output_dir) / f"{self.job_stats_file_name}.{self.file_extn}",
            fields=self.job_stats_col_name_csv,
        ) as writer:
            writer.write(self.job_stats_legends)

            for wf_det, ind_wf_stats in self.wf_uuid_list:
                self.log.debug(
                    f"Generating job instance statistics information for workflow {wf_det.wf_uuid} ..."
                )
                job_stats_list = self._compute_job_statistics(ind_wf_stats)

                writer.writeheader()
                for job_stats in job_stats_list:
                    job_stats = job_stats.get_formatted_statistics()
                    job_stats.insert(0, wf_det.wf_uuid)
                    job_stats.insert(1, wf_det.dax_label)
                    writer.writerow(job_stats)

    def _write_transformation_statistics(self):
        """Write transformation statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_transformation_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_transformation_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_transformation_statistics_text(self):
        """Write transformation statistics data to a txt file."""
        with utils.write_table(
            Path(self.output_dir)
            / f"{self.transformation_stats_file_name}.{self.file_extn}",
            fields=self.transformation_stats_col_name_text,
            widths=self.transformation_stats_col_size,
        ) as writer:
            writer.write(self.transformation_stats_legends)

            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating invocation statistics information for workflow {wf_det.wf_uuid} ..."
                )
                writer.write(f"\n# {wf_det.wf_uuid} ({wf_det.dax_label or 'All'})\n")
                max_length = [_ for _ in self.transformation_stats_col_size]
                transformation_stats = self._compute_transformation_statistics(
                    ind_wf_stats
                )

                for i, transformation_stat in enumerate(transformation_stats):
                    transformation_stats[i] = transformation_stat = (
                        transformation_stat.get_formatted_statistics()
                    )
                    for i in range(13):
                        max_length[i] = max(max_length[i], len(transformation_stat[i]))

                max_length = [i + 1 for i in max_length]
                writer.widths = max_length

                writer.writeheader()
                for transformation_stat in transformation_stats:
                    writer.writerow(transformation_stat)

    def _write_transformation_statistics_csv(self):
        """Write transformation statistics data to a csv file."""
        with utils.write_csv(
            Path(self.output_dir)
            / f"{self.transformation_stats_file_name}.{self.file_extn}",
            fields=self.transformation_stats_col_name_csv,
        ) as writer:
            writer.write(self.transformation_stats_legends)

            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating invocation statistics information for workflow {wf_det.wf_uuid} ..."
                )
                transformation_stats = self._compute_transformation_statistics(
                    ind_wf_stats
                )

                writer.write("\n")
                writer.writeheader()
                for transformation_stat in transformation_stats:
                    transformation_stat = transformation_stat.get_formatted_statistics()
                    transformation_stat.insert(
                        0,
                        (
                            wf_det.wf_uuid
                            if wf_det.wf_uuid != "All"
                            else wf_det.wf_uuid.upper()
                        ),
                    )
                    transformation_stat.insert(1, wf_det.dax_label)
                    writer.writerow(transformation_stat)

    def _write_integrity_statistics(self):
        """Write integrity statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_integrity_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_integrity_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_integrity_statistics_text(self):
        """Write integrity statistics data to a txt file."""
        with utils.write_table(
            Path(self.output_dir)
            / f"{self.integrity_stats_file_name}.{self.file_extn}",
            fields=self.integrity_stats_col_name_text,
            widths=self.integrity_stats_col_size,
        ) as writer:
            writer.write(self.integrity_stats_legends)

            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating integrity statistics information for workflow {wf_det.wf_uuid} ..."
                )
                writer.write(f"\n# {wf_det.wf_uuid} ({wf_det.dax_label or 'All'})\n")
                max_length = [_ for _ in self.integrity_stats_col_size]
                integrity_stats = self._compute_integrity_statistics(ind_wf_stats)

                if not integrity_stats:
                    continue

                for i, integrity_stat in enumerate(integrity_stats):
                    integrity_stats[i] = integrity_stat = [
                        integrity_stat.type,
                        integrity_stat.file_type,
                        str(integrity_stat.count),
                        str(integrity_stat.duration),
                    ]
                    for i in range(4):
                        max_length[i] = max(max_length[i], len(integrity_stat[i]))

                max_length = [i + 1 for i in max_length]
                writer.widths = max_length

                writer.writeheader()
                for integrity_stat in integrity_stats:
                    writer.writerow(integrity_stat)

    def _write_integrity_statistics_csv(self):
        """Write integrity statistics data to a csv file."""
        with utils.write_csv(
            Path(self.output_dir)
            / f"{self.integrity_stats_file_name}.{self.file_extn}",
            fields=self.integrity_stats_col_name_csv,
        ) as writer:
            writer.write(self.integrity_stats_legends)

            for wf_det, ind_wf_stats in chain(
                self.wf_uuid_list, [(self._wf_det, self.wf_stats)]
            ):
                self.log.debug(
                    f"Generating integrity statistics information for workflow {wf_det.wf_uuid} ..."
                )
                integrity_stats = self._compute_integrity_statistics(ind_wf_stats)

                if not integrity_stats:
                    continue

                for i, integrity_stat in enumerate(integrity_stats):
                    integrity_stats[i] = integrity_stat = [
                        (
                            wf_det.wf_uuid
                            if wf_det.wf_uuid != "All"
                            else wf_det.wf_uuid.upper()
                        ),
                        wf_det.dax_label,
                        integrity_stat.type,
                        integrity_stat.file_type,
                        str(integrity_stat.count),
                        str(integrity_stat.duration),
                    ]

                writer.writeheader()
                for integrity_stat in integrity_stats:
                    writer.writerow(integrity_stat)

    def _write_time_statistics(self):
        """Write time statistics data to a file."""
        if self.file_type == self.file_type_txt:
            self._write_time_statistics_text()
        elif self.file_type == self.file_type_csv:
            self._write_time_statistics_csv()
        else:
            self.log.error(f"Invalid file type {self.file_type}")

    def _write_time_statistics_text(self):
        """Write time statistics data to a txt file."""
        (
            jobs_by_time,
            invocations_by_time,
            jobs_per_host,
            invocations_per_host,
        ) = self._compute_time_statistics()

        self.time_stats_col_name_text[0] %= {"tf_format": self.tf_format}
        with utils.write_table(
            Path(self.output_dir) / f"{self.time_stats_file_name}.{self.file_extn}",
            fields=self.time_stats_col_name_text,
            widths=self.time_stats_col_size,
        ) as writer:
            writer.write(
                "\n"
                + self.time_stats_legends_text % {"time_filter": self.time_filter}
                + "\n"
            )
            # Job instances statistics per day
            writer.write(f"\n# Job instances statistics per {self.time_filter}\n")
            writer.writeheader()
            formatted = stats_utils.convert_stats_to_base_time(
                jobs_by_time, self.time_filter
            )
            for s in formatted:
                content = (s["date_format"], s["count"], fstr(s["runtime"]))
                writer.writerow(content)

            # Invocation statistics run per day
            writer.write(f"\n# Invocation statistics run per {self.time_filter}\n")
            writer.writeheader()
            formatted = stats_utils.convert_stats_to_base_time(
                invocations_by_time, self.time_filter
            )
            for s in formatted:
                content = (s["date_format"], s["count"], fstr(s["runtime"]))
                writer.writerow(content)

            self.time_host_stats_col_name_text[0] %= {"tf_format": self.tf_format}
            writer.fields = self.time_host_stats_col_name_text
            writer.widths = self.time_host_stats_col_size

            # Job instances statistics on host per day
            writer.write(
                f"\n# Job instances statistics on host per {self.time_filter}\n"
            )
            writer.writeheader()
            formatted = stats_utils.convert_stats_to_base_time(
                jobs_per_host, self.time_filter, True
            )
            for s in formatted:
                content = [
                    s["date_format"],
                    s["host"],
                    s["count"],
                    fstr(s["runtime"]),
                ]
                writer.writerow(content)

            # Invocation statistics on host per day
            writer.write(f"\n# Invocation statistics on host per {self.time_filter}\n")
            writer.writeheader()
            formatted = stats_utils.convert_stats_to_base_time(
                invocations_per_host, self.time_filter, True
            )
            for s in formatted:
                content = [
                    s["date_format"],
                    s["host"],
                    s["count"],
                    fstr(s["runtime"]),
                ]
                writer.writerow(content)

    def _write_time_statistics_csv(self):
        """Write time statistics data to a csv file."""
        (
            jobs_by_time,
            invocations_by_time,
            jobs_per_host,
            invocations_per_host,
        ) = self._compute_time_statistics()

        self.time_stats_col_name_csv[1] %= {"tf_format": self.tf_format}
        with utils.write_csv(
            Path(self.output_dir) / f"{self.time_stats_file_name}.{self.file_extn}",
            fields=self.time_stats_col_name_csv,
        ) as writer:
            writer.write(
                self.time_stats_legends_csv % {"time_filter": self.time_filter}
            )
            # Job Instance statistics
            writer.write(f"\n# Job instances statistics per {self.time_filter}\n")
            writer.writerow(self.time_stats_col_name_csv)
            formatted = stats_utils.convert_stats_to_base_time(
                jobs_by_time, self.time_filter
            )
            for s in formatted:
                content = (
                    f"jobs/{self.time_filter}",
                    s["date_format"],
                    s["count"],
                    fstr(s["runtime"]),
                )
                writer.writerow(content)

            # Invocation statistics
            writer.write(f"\n# Invocation statistics run per {self.time_filter}\n")
            writer.writerow(self.time_stats_col_name_csv)
            formatted = stats_utils.convert_stats_to_base_time(
                invocations_by_time, self.time_filter
            )
            for s in formatted:
                content = (
                    f"invocations/{self.time_filter}",
                    s["date_format"],
                    s["count"],
                    fstr(s["runtime"]),
                )
                writer.writerow(content)

        self.time_host_stats_col_name_csv[1] %= {"tf_format": self.tf_format}
        with utils.write_csv(
            Path(self.output_dir)
            / f"{self.time_host_stats_file_name}.{self.file_extn}",
            fields=self.time_host_stats_col_name_csv,
        ) as writer:
            writer.write(
                self.time_host_stats_legends % {"time_filter": self.time_filter}
            )
            # Job instances statistics on host per day
            writer.write(
                f"\n# Job instances statistics on host per {self.time_filter}\n"
            )
            writer.writerow(self.time_host_stats_col_name_csv)
            formatted = stats_utils.convert_stats_to_base_time(
                jobs_per_host, self.time_filter, True
            )
            for s in formatted:
                content = (
                    f"jobs/host/{self.time_filter}",
                    s["date_format"],
                    s["host"],
                    s["count"],
                    fstr(s["runtime"]),
                )
                writer.writerow(content)

            # Invocation statistics on host per day
            writer.write(f"\n# Invocation statistics on host per {self.time_filter}\n")
            writer.writerow(self.time_host_stats_col_name_csv)
            formatted = stats_utils.convert_stats_to_base_time(
                invocations_per_host, self.time_filter, True
            )
            for s in formatted:
                content = (
                    f"invocations/host/{self.time_filter}",
                    s["date_format"],
                    s["host"],
                    s["count"],
                    fstr(s["runtime"]),
                )
                writer.writerow(content)

    def _cleanup(self):
        """Close all open database sessions."""
        for _, ind_wf_stats in chain(self.wf_uuid_list, [(None, self.wf_stats)]):
            try:
                if ind_wf_stats:
                    ind_wf_stats.close()
            except Exception:
                self.log.warning("Error closing database")
                self.log.debug("Error closing database", exc_info=1)

    def console_output(self, ctx):
        """Print Pegasus statistics output to the console."""

        extn = self.file_extn

        if self.calc_wf_summary:
            name = Path(self.output_dir) / f"{self.workflow_summary_file_name}.{extn}"
            if self.file_type == "text":
                for line in name.open():
                    click.echo(line.rstrip())
                click.echo()

            click.echo("%-30s: %s" % ("Summary", name))

            if self.file_type == "csv":
                name = (
                    Path(self.output_dir)
                    / f"{self.workflow_summary_time_file_name}.{extn}"
                )
                click.echo("%-30s: %s" % ("Summary Time", name))

        if self.calc_wf_stats:
            name = Path(self.output_dir) / f"{self.workflow_stats_file_name}.{extn}"
            click.echo("%-30s: %s" % ("Workflow execution statistics", name))

        if self.calc_jb_stats:
            name = Path(self.output_dir) / f"{self.job_stats_file_name}.{extn}"
            click.echo("%-30s: %s" % ("Job instance statistics", name))

        if self.calc_tf_stats:
            name = (
                Path(self.output_dir) / f"{self.transformation_stats_file_name}.{extn}"
            )
            click.echo("%-30s: %s" % ("Transformation statistics", name))

        if self.calc_int_stats:
            name = Path(self.output_dir) / f"{self.integrity_stats_file_name}.{extn}"
            click.echo("%-30s: %s" % ("Integrity statistics", name))

        if self.calc_ti_stats:
            name = Path(self.output_dir) / f"{self.time_stats_file_name}.{extn}"
            click.echo("%-30s: %s" % ("Time statistics", name))
            if extn == "csv":
                click.echo("%-30s: %s" % ("Time statistics (per host)", name))

        if self.errors:
            msg = f"Failed to generate {self.errors} type(s) of statistics"
            self.log.critical(msg)
            raise PegasusStatisticsError(msg)

    def __call__(self, ctx, verbose: int = 0, quiet: int = 0):
        """Main method of Pegasus statistics."""

        # Initialize logging
        self._initialize(verbose, quiet)

        # Check the inputs
        self._check_inputs()

        # Check the type of clustering to use while computing the statistics
        self._get_clustering_type()

        # Identify the workflow database URL from the submit-dir or the workflow UUID
        self._get_workflow_db_url()

        # Create the output directory
        self._initialize_output_dir()

        # Compute statistics for each level of statistics to be calculated
        self._compute_statistics()

        # Write out the statistics based on the chosen output format
        self._write_statistics()

        # Cleanup
        self._cleanup()

        # Final Output
        self.console_output(ctx)
