#!/usr/bin/env python3

"""
:mod:`analyzer` exposes an API to retrieve information regarding successful or failed jobs

Basic Usage::

    >>> from Pegasus import analyzer
    >>> options = analyzer.Options(input_dir = 'path/to/submit/directory')
    >>> analyze = analyzer.AnalyzeDB(options)
    >>> analyze.analyze_db()
    >>> print(analyze.analyzer_output.as_dict())
"""

import logging
import os
import re
import subprocess
import sys
import tempfile
import typing
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Dict

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.workflow import stampede_statistics
from Pegasus.tools import kickstart_parser, utils

logger = logging.getLogger("pegasus-analyzer")
utils.configureLogging(level=logging.WARNING)

if sys.version_info > (3, 6):
    setattr(typing, "_ClassVar", typing.ClassVar)


# --- classes -------------------------------------------------------------------------
class WORKFLOW_STATUS(Enum):
    """Workflow Status"""

    UNKNOWN = "unknown"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


class Job:
    def __init__(self, job_name, job_state=""):
        """
        Initializes the Job class, setting job name,
        and state, if provided
        """
        self.name = job_name  # Job name
        self.state = job_state  # Job state
        self.sub_file = ""  # Submit file for this job
        self.out_file = ""  # Output file for this job
        self.err_file = ""  # Error file for this job
        self.cluster = ""  # Cluster id for this job (from Condor)
        self.process = ""  # Process id for this job (from Condor)
        self.sub_file_parsed = (
            False  # Flag to tell if we were able to parse this job's submit file
        )
        self.site = ""  # Site where the job ran
        self.executable = ""  # Job's executable
        self.arguments = ""  # Job's arguments
        self.initial_dir = ""  # Job's initial dir (from submit file)
        self.transfer_input_files = ""  # Files to transfer when debugging a job
        self.transfer_output_files = ""  # Files to transfer when debugging a job
        self.retries = None  # Keep track of how many times a job is submitted
        self.is_subdax = False  # Flag to tell if job is a SUBDAX/pegasus-plan job
        self.is_subdag = False  # Flag to tell if job is a SUBDAG job in the dag file
        self.subdag_dir = ""  # Subdag directory from a SUBDAG job in the dag file
        self.dag_path = ""  # Full path to the dag file from a SUBDAG job
        self.dagman_out = ""  # dagman.out file for this job (only for clustered jobs)
        self.pre_script = ""  # SCRIPT PRE line from the dag file
        self.condor_subs = {}  # Lists of condor substitutions rom DAG VARS line
        self.wf_submit_dir = None  # the value of the variable wf_submit_dir indicating  wf submit directory in the submit file

    def set_state(self, new_state):
        """
        This function updates a job state
        """
        self.state = new_state


# --- exceptions ----------------------------------------------------------------------
class AnalyzerError(Exception):
    pass


# --- global variables ----------------------------------------------------------------
prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")  # Name of this program
dag_path = None  # Path of the dag fi
indent = ""  # the corresponding indent string


# ----- Data classes ------------------------------------------------------------------
@dataclass
class Options:
    input_dir: str = None  # Directory given in -i command line option
    debug_job: str = None  # Path of a submit file to debug
    debug_dir: str = None  # Temp directory to use while debugging a job
    debug_job_local_executable: str = (
        None  # the local path to user executable for debugging job
    )
    workflow_type: str = None  # Type of the workflow being debugged
    run_monitord: bool = False  # Run monitord before trying to analyze the output
    output_dir: str = None  # output_dir for all files written by monitord
    top_dir: str = None  # top_dir of the main workflow, for obtaining the db location
    quiet_mode: bool = (
        False  # Prints out/err filenames instead of dumping their contents
    )
    strict_mode: bool = False  # Gets out/err filenames from submit file
    summary_mode: bool = False  # Print just the summary output
    debug_mode: bool = False  # Mode that enables debugging a single job
    json_mode: bool = (
        False  # Mode that returns all info in a structured format like JSON
    )
    recurse_mode: bool = (
        False  # Mode that automatically recurses into failed sub workflows.
    )
    traverse_all: bool = (
        False  # Mode that automatically traverses all descendant sub workflows.
    )
    indent_length: int = 0  # the number of tabs to print before printing to console
    print_invocation: bool = False  # Prints invocation command for failed jobs
    print_pre_script: bool = (
        False  # Prints the SCRIPT PRE line for failed jobs, if present
    )
    workflow_base_dir: str = (
        None  # Workflow submit_dir or dirname(jsd) from braindump file
    )
    use_files: bool = False  # mode to use AnalyzeFiles


@dataclass
class Task:
    task_submit_seq: int = None
    exitcode: int = None
    executable: str = None
    arguments: str = None
    transformation: str = None
    abs_task_id: str = None


@dataclass
class JobInstance:
    job_name: str = None
    state: str = None
    site: str = None
    hostname: str = None
    work_dir: str = None
    submit_file: str = None
    stdout_file: str = None
    stderr_file: str = None
    executable: str = None
    argv: str = None
    pre_executable: str = None
    pre_argv: str = None
    submit_dir: str = None
    subwf_dir: str = None
    stdout_text: str = None
    stderr_text: str = None
    tasks: Dict[str, Task] = field(default_factory=lambda: ({}))


@dataclass
class Jobs:
    total: int = 0
    success: int = 0
    failed: int = 0
    held: int = 0
    unsubmitted: int = 0
    job_details: Dict[str, Dict] = field(default_factory=lambda: ({}))


@dataclass
class Workflow:
    wf_uuid: str = None
    dag_file_name: str = None
    submit_hostname: str = None
    submit_dir: str = None
    user: str = None
    planner_version: str = None
    wf_name: str = None
    wf_status: str = None
    parent_wf_name: str = None
    parent_wf_uuid: str = None
    jobs: Jobs = None


# --- classes to store analyzer output ------------------------------------------------
class AnalyzerOutput:
    def __init__(self):
        """
        Initializes the Analyzer output class
        """
        self.root_wf_uuid = None  # root workflow uuid
        self.root_submit_dir = None  # root workflow submit directory
        self.workflows: Dict[Str:Workflow] = {}
        self.structure_output = {
            "root_wf_uuid": None,
            "submit_directory": None,
            "workflows": {},
        }

    def as_dict(self):
        """
        Converts all data classes and returns the analyzer output as a dictionary

        :return: A dict containing all workflow details
        :rtype: Dict[str,Dict]
        """

        for wf in self.workflows:
            self.structure_output["workflows"][wf] = asdict(self.workflows[wf])
        return self.structure_output

    def get_failed_workflows(self):
        """
        Returns a dictionary of failed workflows

        :return: Dict of returned :class:`Pegasus.analyzer.Workflow` objects
        :rtype: Dict[str,Workflow]
        """
        failed_wfs = {}
        for wf in self.workflows:
            if self.workflows[wf].wf_status == "failure":
                failed_wfs[wf] = self.workflows[wf]
        return failed_wfs

    def get_all_jobs(self):
        """
        Returns a dictionary of all jobs' details for the root workflow

        :return: Dict of returned :class:`Pegasus.analyzer.JobInstance` objects
        :rtype: Dict[str,JobInstance]
        """
        return self.workflows["root"].jobs.job_details

    def get_jobs_counts(self):
        """
        Returns a dataclass of jobs counts

        :return: A dataclass :class:`Pegasus.analyzer.Counts` object
        """
        jobs = self.workflows["root"].jobs
        counts = Counts(
            jobs.total,
            jobs.success,
            jobs.failed,
            jobs.unsubmitted,
            0,
            jobs.held,
            [],
            [],
        )
        return counts

    def get_failed_jobs(self):
        """
        Returns a dictionary of all failed jobs details

        :return: Dict of returned :class:`Pegasus.analyzer.JobInstance` objects
        :rtype: Dict[str,JobInstance]
        """
        return self.workflows["root"].jobs.job_details.get("failed_jobs_details", None)

    def get_failing_jobs(self):
        """
        Returns a dictionary of all failing jobs details

        :return: Dict of returned :class:`Pegasus.analyzer.JobInstance` objects
        :rtype: Dict[str,JobInstance]
        """
        return self.workflows["root"].jobs.job_details.get("failing_jobs_details", None)

    def get_held_jobs(self):
        """
        Returns a dictionary of all held jobs details

        :return: Dict of returned :class:`Pegasus.analyzer.JobInstance` objects
        :rtype: Dict[str,JobInstance]
        """
        return self.workflows["root"].jobs.job_details.get("held_jobs_details", None)

    def get_unknown_jobs(self):
        """
        Returns a dictionary of all unknown jobs details, for AnalyzeFiles only

        :return: Dict of returned :class:`Pegasus.analyzer.JobInstance` objects
        :rtype: Dict[str,JobInstance]
        """
        return self.workflows["root"].jobs.job_details.get("unknown_jobs_details", None)


@dataclass
class Counts:
    total: int = 0  # Number of total jobs
    success: int = 0  # Number of successful jobs
    failed: int = 0  # Number of failed jobs
    unsubmitted: int = 0  # Number of unsubmitted jobs
    unknown: int = 0  # Number of jobs in an unknown state
    held: int = 0  # number of held jobs
    failed_jobs: list = field(default_factory=[])  # List of jobs that failed
    unknown_jobs: list = field(
        default_factory=[]
    )  # List of jobs that neither succeeded nor failed


# --- Analyze classes to run analyzer --------------------------------------------------
class BaseAnalyze:
    # --- regular expressions -------------------------------------------------------------
    re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")
    re_parse_script_pre = re.compile(r"^SCRIPT PRE (\S+) (.*)")
    re_parse_condor_subs = re.compile(r'(\S+)="([^"]+)"')
    re_collapse_condor_subs = re.compile(r"\$\([^\)]*\)")
    re_ks_invocation = re.compile(r"^[/\w]*pegasus-kickstart\b[^/]*[\s]+([/\w-]*)\b.*")
    MAXLOGFILE = 1000  # For log file rotation, check files .000 to .999

    def check_for_wf_start(options, counts):
        """
        This function checks if workflow did start.
        If not then print helpful message
        :return:
        """

        if counts.unsubmitted == counts.total:
            # PM-1039 either the workflow did not start or other errors
            # check the dagman.out file
            BaseAnalyze.print_console(
                " Looks like workflow did not start".center(80, "*")
            )
            BaseAnalyze.print_console()
            if options.input_dir is not None:
                dagman_out = BaseAnalyze.backticks(
                    "ls " + options.input_dir + "/*.dag.dagman.out" + " 2>/dev/null"
                )

                if dagman_out is not None and dagman_out != "":
                    nfs_error_string = BaseAnalyze.backticks(
                        'grep -i ".*Error.*NFS$" ' + dagman_out + " 2>/dev/null"
                    )
                    if nfs_error_string is not None and nfs_error_string != "":
                        header = " Error detected in *.dag.dagman.out "
                        BaseAnalyze.print_console(header.center(80, "="))
                        BaseAnalyze.print_console(
                            " HTCondor DAGMan NFS ERROR condition detected in "
                            + dagman_out
                        )
                        BaseAnalyze.print_console(" " + nfs_error_string)
                        BaseAnalyze.print_console(
                            " HTCondor DAGMan expects submit directories to be NOT NFS mounted"
                        )
                        BaseAnalyze.print_console(
                            " Set your submit directory to a directory on the local filesystem OR "
                        )
                        BaseAnalyze.print_console(
                            "    Set HTCondor configuration CREATE_LOCKS_ON_LOCAL_DISK and ENABLE_USERLOG_LOCKING to True. Check HTCondor documentation for further details."
                        )
                        BaseAnalyze.print_console()

                # PM-1040 check for dagman.lib.err
                dagman_lib_err = BaseAnalyze.backticks(
                    "ls " + options.input_dir + "/*.dag.lib.err" + " 2>/dev/null"
                )
                if dagman_lib_err is not None and dagman_lib_err != "":
                    dagman_lib_err_contents = BaseAnalyze.backticks(
                        "cat " + dagman_lib_err + " 2>/dev/null"
                    )
                    if (
                        dagman_lib_err_contents is not None
                        and dagman_lib_err_contents != ""
                    ):
                        header = " Error detected in *.dag.lib.err "
                        BaseAnalyze.print_console(header.center(80, "="))
                        BaseAnalyze.print_console(" Contents of " + dagman_lib_err)
                        BaseAnalyze.print_console(" " + dagman_lib_err_contents)

    def print_console(stmt=""):
        """
        A utilty function to print to console with the correct indentation
        """
        print(indent + stmt)

    def backticks(cmd_line):
        """
        what would a python program be without some perl love?
        """
        o = subprocess.Popen(
            cmd_line, shell=True, stdout=subprocess.PIPE
        ).communicate()[0]
        if o:
            o = o.decode()

    def addon(options):
        """
        This function constructs a command line invocation that needs to be passed for invoking for a sub workflow.
        Only a certain subset of options are propogated to the sub workflow invocations if passed.
        """
        cmd_line_args = ""

        if options.recurse_mode:
            cmd_line_args += "--recurse "

        if options.quiet_mode:
            cmd_line_args += "--quiet "

        if options.summary_mode:
            cmd_line_args += "--summary "

        if options.use_files:
            cmd_line_args += "--files "

        new_indent = 1
        if options.indent_length is not None:
            new_indent = options.indent_length + 1

        cmd_line_args += "--indent " + str(new_indent) + " "

        return cmd_line_args

    def parse_submit_file(my_job, options):
        """
        This function opens a submit file and reads site
        and condor dagman log information
        """
        # First we check if this is a SUBDAG job from the dag file
        if my_job.is_subdag:
            # Nothing to do here
            return

        full_path = options.input_dir
        if my_job.sub_file == "":
            # Create full path for the submit file if we already don't have the sub file set up
            my_job.sub_file = os.path.join(options.input_dir, my_job.name + ".sub")
        else:
            full_path = os.path.dirname(my_job.sub_file)
        my_job.out_file = os.path.join(full_path, my_job.name + ".out")
        my_job.err_file = os.path.join(full_path, my_job.name + ".err")

        # Try to access submit file
        if os.access(my_job.sub_file, os.R_OK):
            # Open submit file
            try:
                SUB = open(my_job.sub_file)
            except Exception:
                # print "error opening submit file: %s" % (my_job.sub_file)
                raise AnalyzerError(f"error opening submit file: {my_job.sub_file}")
                return

            # submit file found
            my_job.sub_file_parsed = True

            # Check if this job includes sub workflows
            if my_job.is_subdax:
                has_sub_workflow = True
            else:
                has_sub_workflow = False

            # Parse submit file
            for line in SUB:
                # First we need to do some trimming...
                line = line.strip(" \t")  # Remove leading and trailing spaces
                if line.startswith("#"):
                    # Skip comments
                    continue
                line = line.rstrip("\n\r")  # Remove new lines, if any
                line = line.split("#")[0]  # Remove inline comments too
                line = line.strip()  # Remove any remaining spaces at both ends
                if len(line) == 0:
                    # Skip empty lines
                    continue
                prop = BaseAnalyze.re_parse_property.search(line)
                if prop:
                    # Parse successful
                    k = prop.group(1)
                    v = prop.group(2)

                    # See if it is one of the properties we are looking for...
                    if k == "+pegasus_site":
                        my_job.site = v.strip('"')
                        continue
                    if k == "arguments":
                        my_job.arguments = v.strip('"')
                    if k == "executable":
                        my_job.executable = v
                    if k == "environment" and has_sub_workflow:
                        # Ok, we need to find the CONDOR_DAGMAN_LOG entry now...
                        sub_props = v.split(";")
                        for sub_prop_line in sub_props:
                            sub_prop_line = sub_prop_line.strip()  # Remove any spaces
                            if len(sub_prop_line) == 0:
                                continue
                            sub_prop = BaseAnalyze.re_parse_property.search(
                                sub_prop_line
                            )
                            if sub_prop:
                                if sub_prop.group(1) == "_CONDOR_DAGMAN_LOG":
                                    my_job.dagman_out = sub_prop.group(2)
                                    my_job.dagman_out = os.path.normpath(
                                        my_job.dagman_out
                                    )
                                    if (
                                        my_job.dagman_out.find(
                                            options.workflow_base_dir
                                        )
                                        >= 0
                                    ):
                                        # Path to dagman_out file includes original submit_dir, let's try to change it
                                        my_job.dagman_out = os.path.normpath(
                                            my_job.dagman_out.replace(
                                                (options.workflow_base_dir + os.sep),
                                                "",
                                                1,
                                            )
                                        )
                                        # Join with current options.input_dir
                                        my_job.dagman_out = os.path.join(
                                            options.input_dir, my_job.dagman_out
                                        )

                                        # Now, figure out the correct directory, accounting for
                                        # replanning and rescue modes

                                        # Split filename into dir and base names
                                        my_dagman_dir = os.path.dirname(
                                            my_job.dagman_out
                                        )
                                        my_dagman_file = os.path.basename(
                                            my_job.dagman_out
                                        )
                                        my_retry = my_job.retries
                                        if my_retry is None:
                                            logger.warning(
                                                "sub-workflow retry counter not initialized... continuing..."
                                            )
                                            continue
                                        # Compose directory... assuming replanning mode
                                        my_retry_dir = my_dagman_dir + ".%03d" % (
                                            my_retry
                                        )
                                        # If directory doesn't exist, let's change to rescue mode
                                        if not os.path.isdir(my_retry_dir):
                                            logger.debug(
                                                "sub-workflow directory %s does not exist, shifting to rescue mode..."
                                                % (my_retry_dir)
                                            )
                                            my_retry_dir = my_dagman_dir + ".000"
                                            if not os.path.isdir(my_retry_dir):
                                                # Still not able to find it, output warning message
                                                logger.warning(
                                                    "sub-workflow directory %s does not exist!"
                                                    % (my_retry_dir)
                                                )
                                                continue

                                        # Found sub-workflow directory, let's compose the final path to the new dagman.out file...
                                        my_job.dagman_out = os.path.join(
                                            my_retry_dir, my_dagman_file
                                        )

                    # Only parse following keys if we are running in strict mode
                    if options.strict_mode:
                        # Get initial dir
                        if k == "initialdir":
                            my_job.initial_dir = v
                        # Parse error and output keys
                        if k == "output" or k == "error":
                            # Take care of basic substitutions first
                            v = v.replace("$(cluster)", my_job.cluster)
                            v = v.replace("$(process)", my_job.process)

                            # Now we do any substitutions from the DAG's VAR line (if any)
                            for my_key in my_job.condor_subs:
                                v = v.replace(
                                    "$(%s)" % (my_key), my_job.condor_subs[my_key]
                                )

                            # Now, we collapse any remaining substitutions (not found in the VAR line)
                            v = BaseAnalyze.re_collapse_condor_subs.sub("", v)

                            # Make sure we have an absolute path
                            if not os.path.isabs(v):
                                v = os.path.join(options.input_dir, v)

                            # Done! Replace out/err filenames with what we have
                            if k == "output":
                                my_job.out_file = v
                            else:
                                my_job.err_file = v
                    # Only parse following keys if we are debugging a job
                    if options.debug_mode:
                        # Get transfer input files and output files
                        if k == "transfer_input_files":
                            my_job.transfer_input_files = v

                        if k == "transfer_output_files":
                            my_job.transfer_output_files = v

                    if k == "wf_submit_dir":
                        my_job.wf_submit_dir = v

            SUB.close()
            # If initialdir was specified, we need to make both error and output files relative to that
            if len(my_job.initial_dir):
                my_job.out_file = os.path.join(my_job.initial_dir, my_job.out_file)
                my_job.err_file = os.path.join(my_job.initial_dir, my_job.err_file)
        else:
            # Was not able to access submit file
            # fail silently for now...
            # print "cannot access submit file: %s" % (my_job.sub_file)
            pass


class AnalyzeDB(BaseAnalyze):
    def __init__(self, options):
        self.analyzer_output = AnalyzerOutput()
        self.options = options
        self.counts = Counts(0, 0, 0, 0, 0, 0, [], [])

    def analyze_db(self, config_properties):
        """
        This function runs the analyzer using data from the database.
        """

        # Get the database URL
        try:
            output_db_url = connection.url_by_submitdir(
                self.options.input_dir,
                connection.DBType.WORKFLOW,
                config_properties,
                self.options.top_dir,
            )
            wf_uuid = connection.get_wf_uuid(self.options.input_dir)
            self.analyzer_output.root_wf_uuid = wf_uuid
            self.analyzer_output.root_submit_dir = self.options.input_dir
            self.analyzer_output.structure_output["root_wf_uuid"] = wf_uuid
            self.analyzer_output.structure_output[
                "submit_directory"
            ] = self.options.input_dir

        except connection.ConnectionError as e:
            raise AnalyzerError(
                "Unable to connect to database for workflow in %s",
                self.options.input_dir,
                e,
            )

        # Nothing to do if we cannot resolve the database URL
        if output_db_url is None:
            raise ValueError("Database URL is required")

        # Now, let's try to access the database
        workflow_stats = None
        try:
            workflow_stats = stampede_statistics.StampedeStatistics(
                output_db_url, False
            )
            workflow_stats.initialize(wf_uuid)
            descendant_wfs_ids = workflow_stats.get_descendants(wf_uuid)
        except DBAdminError as e:
            raise AnalyzerError("Failed to load the database - " + output_db_url, e)
        finally:
            if workflow_stats:
                # Done with the database
                workflow_stats.close()

        logger.debug(
            "Descendant Workflows database ids for %s are %s"
            % (wf_uuid, descendant_wfs_ids)
        )

        wf_id = None
        wf_details = {}  # stores all wf details indexed by database id of a wf
        for wf_detail in workflow_stats.get_workflow_details(descendant_wfs_ids):
            wf_details[wf_detail.wf_id] = wf_detail
            if wf_detail.wf_uuid == wf_uuid:
                wf_id = wf_detail.wf_id

        if wf_id is None:
            logger.error(
                "Unable to determine the database id for workflow with uuid %s"
                % wf_uuid
            )

        wf_ids = []
        if self.options.traverse_all:
            wf_ids.extend(descendant_wfs_ids)
        else:
            # we only do the workflow on which submit directory analyzer is called
            wf_ids.append(wf_id)

        for wf_id in wf_ids:
            wf_detail = wf_details[wf_id]

            if wf_detail.wf_uuid == wf_uuid:
                wf_key = "root"
            else:
                wf_key = wf_detail.wf_uuid
            self.analyzer_output.workflows[wf_key] = self.get_wf_details(
                wf_detail, wf_details
            )
            wf = self.analyzer_output.workflows[wf_key]

            logger.debug(
                "Running analyzer on workflow with database id {} in submit dir {}".format(
                    wf_id, wf_detail.submit_dir
                )
            )

            self.counts.failed = 0
            workflow_stats = None
            try:
                # for each wf initialize a new workflow stats object
                workflow_stats = stampede_statistics.StampedeStatistics(
                    output_db_url, False
                )
                workflow_stats.initialize(root_wf_id=wf_id)
                self.analyze_db_for_wf(
                    workflow_stats, wf_detail.wf_uuid, wf_detail.submit_dir, wf
                )
            except DBAdminError as e:
                raise AnalyzerError("Failed to load the database - " + output_db_url, e)
            finally:
                if workflow_stats:
                    workflow_stats.close()

        return self.analyzer_output

    def get_wf_details(self, wf_detail, wf_details):
        """
        Filters out workflow details to be used in the workflow structure

        :param wf_detail: details regarding a workflow (wf_uuid, submit_dir, host etc.)
        :param wf_details: a dict of all workflows' details
        :return: returns a :class:`Pegasus.analyzer.Workflow` object
        """

        wf = Workflow(
            wf_detail.wf_uuid,
            wf_detail.dag_file_name,
            wf_detail.submit_hostname,
            wf_detail.submit_dir,
            wf_detail.user,
            wf_detail.planner_version,
            wf_detail.dax_label,
        )
        if wf_detail.parent_wf_id:
            wf.parent_wf_uuid = wf_details[wf_detail.parent_wf_id].wf_uuid
            if "dax_label" in wf_details[wf_detail.parent_wf_id]:
                wf.parent_wf_name = wf_details[wf_detail.parent_wf_id].dax_label
            else:
                wf.parent_wf_name = "-"
        else:
            wf.parent_wf_name = "-"
            wf.parent_wf_uuid = "-"

        return wf

    def analyze_db_for_wf(self, workflow_stats, wf_uuid, submit_dir, wf):
        """
        This function runs the analyzer using data from the database.
        :param workflow_stats: the stampede statistics object initialized with the workflow, we want to analyze
        :param wf_uuid:  the uuid of the workflow we are analyzing
        :param submit_dir: the submit dir for the workflow
        :return:
        """

        self.counts.total = workflow_stats.get_total_jobs_status()
        total_success_failed = workflow_stats.get_total_succeeded_failed_jobs_status()
        self.counts.success = total_success_failed.succeeded
        self.counts.failed = total_success_failed.failed

        held_jobs = workflow_stats.get_total_held_jobs()
        self.counts.held = len(held_jobs)

        # jobs failing
        failing, filtered_count, failing_jobs = workflow_stats.get_failing_jobs()

        # PM-1762 need to retrieve workflow states also, as you can
        # have workflow with zero failed jobs, but still the workflow failed
        # for example trying to run a workflow again from the same directory
        # where an already existing workflow is running
        workflow_states = workflow_stats.get_workflow_states()
        workflow_status = WORKFLOW_STATUS.UNKNOWN
        if workflow_states:
            # workflow states are returned in ascending order. we need the last state
            workflow_status = self.get_workflow_status(workflow_states[-1])

        logger.debug(
            "Workflow state determined from workflow database is %s"
            % workflow_status.value
        )

        # PM-1039
        if self.counts.success is None:
            self.counts.success = 0
        if self.counts.failed is None:
            self.counts.failed = 0

        self.counts.unsubmitted = (
            self.counts.total - self.counts.success - self.counts.failed
        )

        wf.wf_status = workflow_status.value
        wf.jobs = Jobs(
            self.counts.total,
            self.counts.success,
            self.counts.failed,
            self.counts.held,
            self.counts.unsubmitted,
        )

        # Let's print the results
        BaseAnalyze.check_for_wf_start(self.options, self.counts)

        # PM-1126 print information about held jobs
        if self.counts.held > 0:
            wf.jobs.job_details["held_jobs_details"] = {}

            for held_job in held_jobs:
                wf.jobs.job_details["held_jobs_details"][held_job.jobname] = {
                    "submit_file": held_job.jobname + ".sub" or "-",
                    "last_job_instance_id": held_job[0] or "-",
                    "reason": held_job.reason or "-",
                }

        # PM-1890 print failing jobs before failed jobs
        if len(failing_jobs) > 0:
            wf.jobs.job_details["failing_jobs_details"] = {}
            for i in range(len(failing_jobs)):
                failing_jobs[i] = failing_jobs[i]._asdict()
                failing_job_id = failing_jobs[i]["job_instance_id"]
                job_tasks = workflow_stats.get_invocation_info(failing_job_id)
                wf.jobs.job_details["failing_jobs_details"][
                    failing_job_id
                ] = self.get_job_details(
                    workflow_stats.get_job_instance_info(failing_job_id)[0], job_tasks,
                )

        # Now, print information about jobs that failed...
        if self.counts.failed > 0:
            wf.jobs.job_details["failed_jobs_details"] = {}
            # Get list of failed jobs from database
            self.counts.failed_jobs = workflow_stats.get_failed_job_instances()

            # Now process one by one...
            for my_job in self.counts.failed_jobs:
                job_instance_info = workflow_stats.get_job_instance_info(my_job[0])[0]
                job_tasks = workflow_stats.get_invocation_info(my_job[0])

                wf.jobs.job_details["failed_jobs_details"][
                    job_instance_info.job_name
                ] = self.get_job_details(
                    workflow_stats.get_job_instance_info(
                        job_instance_info.job_instance_id
                    )[0],
                    job_tasks,
                )

    def get_job_details(self, job_instance_info, job_tasks):
        """
        Filters out workflow details to be used in the workflow structure

        :param job_instance_info: information regarding a job (job_name, state, site etc.)
        :param job_tasks: information regarding all tasks in a job (hostname, exitcode, etc.)
        :return: returns a :class:`Pegasus.analyzer.JobInstance` object
        """

        job_instance = JobInstance(
            job_instance_info.job_name,
            job_instance_info.state,
            job_instance_info.site,
            job_instance_info.hostname or "-",
            job_instance_info.work_dir,
            job_instance_info.submit_file,
            job_instance_info.stdout_file,
            job_instance_info.stderr_file,
            job_instance_info.executable,
            job_instance_info.argv or "-",
            job_instance_info.pre_executable or "-",
            job_instance_info.pre_argv or "-",
            job_instance_info.submit_dir or "-",
            job_instance_info.subwf_dir or "-",
        )

        job_stdout_text = utils.unquote(job_instance_info.stdout_text or "").decode(
            "UTF-8"
        )
        job_stderr_text = utils.unquote(job_instance_info.stderr_text or "").decode(
            "UTF-8"
        )
        job_stdout_text = job_stdout_text.strip(" \n\r\t")
        job_stderr_text = job_stderr_text.strip(" \n\r\t")
        job_instance.stdout_text = job_stdout_text
        job_instance.stderr_text = job_stderr_text

        i = 0
        for task in job_tasks:
            # PM-798 we want to detect if some tasks failed or not
            # Skip only post script tasks. Pre script invocations have task_submit_seq as -1
            if task[0] < -1:
                continue
            if not task[5]:
                i += 1
                id = "id" + str(i)
            job_instance.tasks[task[5] or id] = Task(
                task[0],
                utils.raw_to_regular(task[1]),
                task[2],
                task[3] or "-",
                task[4],
                task[5] or "-",
            )

        return job_instance

    def get_workflow_status(self, last_wf_state_record):
        """
        Determines the workflow status from the last workflow state record for the workflow from the workflow database
        :param last_wf_state_record: of form  (wf_id, state, timestamp, restart_count, status)
        :return: the workflow status as value of type enum WORKFLOW_STATUS
        """

        workflow_status = WORKFLOW_STATUS.UNKNOWN
        if last_wf_state_record is None:
            return workflow_status

        # (wf_id, state, timestamp, restart_count, status)
        # (1, 'WORKFLOW_TERMINATED', Decimal('1619144694.000000'), 2, 1)
        last_wf_state = last_wf_state_record[1]
        last_wf_status = last_wf_state_record[4]

        if last_wf_state == "WORKFLOW_STARTED":
            workflow_status = WORKFLOW_STATUS.RUNNING
        elif last_wf_state == "WORKFLOW_TERMINATED":
            workflow_status = (
                WORKFLOW_STATUS.SUCCESS
                if last_wf_status == 0
                else WORKFLOW_STATUS.FAILURE
            )
        else:
            raise ValueError("Invalid worklfow state %s" % last_wf_state)

        return workflow_status


class AnalyzeFiles(BaseAnalyze):
    def __init__(self, options):
        self.jsdl_filename = "jobstate.log"  # Default name of the log file to use
        self.jobs = {}  # List of jobs found in the jobstate.log file
        self.analyzer_output = AnalyzerOutput()
        self.options = options
        self.counts = Counts(0, 0, 0, 0, 0, 0, [], [])

    def analyze_files(self):
        """
        This function runs the analyzer using the files in the workflow
        directory as the data source.
        """
        jsdl_path = None  # Path of the jobstate.log file
        run_directory_writable = (
            False  # Flag to indicate if we can write to the run directory
        )
        dagman_out_path = None  # Path to the dagman.out file

        global dag_path

        # Get the dag file if it was not specified by the user
        if dag_path is None:
            dag_path = self.find_file(self.options.input_dir, ".dag")
            logger.info("using %s, use the --dag option to override" % (dag_path))

        # Build dagman.out path
        dagman_out_path = dag_path + ".dagman.out"

        # Check if we can write to the run directory
        run_directory_writable = os.access(self.options.input_dir, os.W_OK)

        # Invoke monitord if requested
        if self.options.run_monitord:
            if self.options.output_dir is not None:
                # If self.options.output_dir is specified, invoke monitord with that path
                self.invoke_monitord(
                    "%s.dagman.out" % (dag_path), self.options.output_dir
                )
                # jobstate.log file uses wf_uuid as prefix
                jsdl_path = os.path.join(
                    self.options.output_dir,
                    self.get_jsdl_filename(self.options.input_dir),
                )
            else:
                if run_directory_writable:
                    # Run directory is writable, write monitord output to jobstate.log file
                    jsdl_path = os.path.join(self.options.input_dir, self.jsdl_filename)
                    # Invoke monitord
                    self.invoke_monitord("%s.dagman.out" % (dag_path), None)
                else:
                    # User must provide the --output-dir option
                    raise AnalyzerError(
                        self.options.input_dir
                        + " is not writable. "
                        + "User must specify directory for new monitord logs with the --output-dir option,"
                        + " exiting..."
                    )
        else:
            if self.options.output_dir is not None:
                # jobstate.log file uses wf_uuid as prefix and is inside self.options.output_dir
                jsdl_path = os.path.join(
                    self.options.output_dir,
                    self.get_jsdl_filename(self.options.input_dir),
                )
            else:
                jsdl_path = os.path.join(self.options.input_dir, self.jsdl_filename)

        # Compare timestamps of jsdl_path with dagman_out_path
        try:
            jsdl_stat = os.stat(jsdl_path)
        except Exception:
            raise AnalyzerError("could not access " + jsdl_path + ", exiting...")

        try:
            dagman_out_stat = os.stat(dagman_out_path)
        except Exception:
            raise AnalyzerError("could not access " + dagman_out_path + ", exiting...")

        # Compare mtime for both files
        if dagman_out_stat[8] > jsdl_stat[8]:
            logger.warning(
                "jobstate.log older than the dagman.out file, workflow logs may not be up to date..."
            )

        # Try to parse workflow parameters from braindump.txt file
        wfparams = utils.slurp_braindb(self.options.input_dir)
        if "submit_dir" in wfparams:
            self.options.workflow_base_dir = os.path.normpath(wfparams["submit_dir"])
        elif "jsd" in wfparams:
            self.options.workflow_base_dir = os.path.dirname(
                os.path.normpath(wfparams["jsd"])
            )

        # First we learn about jobs by going through the dag file
        self.parse_dag_file(dag_path)

        # Read logfile
        self.parse_jobstate_log(jsdl_path)

        # Process our jobs
        self.analyze()

        self.analyzer_output.root_wf_uuid = wfparams["root_wf_uuid"]
        self.analyzer_output.root_submit_dir = wfparams["submit_dir"]
        self.analyzer_output.structure_output["root_wf_uuid"] = wfparams["root_wf_uuid"]
        self.analyzer_output.structure_output["submit_directory"] = wfparams[
            "submit_dir"
        ]
        self.analyzer_output.workflows["root"] = self.get_wf_details(wfparams)

        # PM-1039
        BaseAnalyze.check_for_wf_start(self.options, self.counts)

        return self.analyzer_output

    def get_wf_details(self, wfparams):
        """
        Gets workflow details to be used in Analyzer Output

        :param wfparams: A Dict containing workflow attributes from braindump
        :type: Dict
        :return: returns a :class:`Pegasus.analyzer.Workflow` object
        """
        wf_jobs = Jobs(
            self.counts.total,
            self.counts.success,
            self.counts.failed,
            self.counts.held,
            self.counts.unsubmitted,
        )
        key = None
        if self.counts.total == self.counts.success:
            status = "success"
        elif self.counts.failed > 0:
            status = "failure"
            key = "failed_jobs_details"
            wf_jobs.job_details[key] = {}
            jobs_list = self.counts.failed_jobs
        elif self.counts.unknown > 0:
            status = "running"
            key = "unknown_jobs_details"
            wf_jobs.job_details[key] = {}
            jobs_list = self.counts.unknown_jobs
        else:
            status = "running"

        if key:
            for job in jobs_list:
                BaseAnalyze.parse_submit_file(self.jobs[job], self.options)
                wf_jobs.job_details[key][job] = JobInstance(
                    job_name=self.jobs[job].name,
                    state=self.jobs[job].state,
                    site=self.jobs[job].site,
                    submit_file=self.jobs[job].sub_file or "-",
                    stdout_file=self.jobs[job].out_file or "-",
                    stderr_file=self.jobs[job].err_file or "-",
                    executable=self.jobs[job].executable,
                    argv=self.jobs[job].arguments,
                    pre_executable=self.jobs[job].pre_script,
                    subwf_dir=self.jobs[job].dagman_out or "-",
                )
                self.get_output_error(wf_jobs.job_details[key][job])

        return Workflow(
            wf_uuid=wfparams["wf_uuid"],
            dag_file_name=wfparams["dag"],
            submit_hostname=wfparams["submit_hostname"],
            submit_dir=wfparams["submit_dir"],
            user=wfparams["user"],
            planner_version=wfparams["planner_version"],
            wf_name=wfparams["dax_label"],
            wf_status=status,
            parent_wf_name="-",
            parent_wf_uuid="-",
            jobs=wf_jobs,
        )

    def find_file(self, input_dir, file_type):
        """
        This function finds a file with the suffix file_type
        in the input directory. We assume there is just one
        file of the requested type in the directory (otherwise
        the function will return the first file matching the type
        """
        try:
            file_list = os.listdir(input_dir)
        except Exception:
            raise AnalyzerError("cannot read directory: " + input_dir)

        for file in file_list:
            if file.endswith(file_type):
                return os.path.join(input_dir, file)

        raise AnalyzerError(f"could not find any {file_type} file in {input_dir}")

    def invoke_monitord(self, dagman_out_file, output_dir):
        """
        This function runs monitord on the given dagman_out_file.
        """
        monitord_cmd = "pegasus-monitord -r --no-events"
        if output_dir is not None:
            # Add self.options.output_dir, if given
            monitord_cmd = monitord_cmd + " --output-dir " + output_dir
        monitord_cmd = monitord_cmd + " " + dagman_out_file
        logger.info("running: %s" % (monitord_cmd))

        try:
            # status, output = commands.getstatusoutput(monitord_cmd)
            # commenting it out, couldn't find .getstatusoutput in commands module
            BaseAnalyze.backticks(monitord_cmd)
        except Exception:
            raise AnalyzerError("could not invoke monitord, exiting...")

    def get_jsdl_filename(self, input_dir):
        """
        This function parses the braindump file in the self.options.input_dir,
        retrieving the wf_uuid and assembling the filename for the
        jobstate.log file.
        """
        try:
            my_wf_params = utils.slurp_braindb(input_dir)
        except Exception:
            raise AnalyzerError("cannot read braindump.txt file... exiting...")

        if "wf_uuid" in my_wf_params:
            return my_wf_params["wf_uuid"] + "-" + self.jsdl_filename

        raise AnalyzerError("braindump.txt does not contain wf_uuid... exiting...")

    def parse_dag_file(self, dag_fn):
        """
        This function walks through the dag file, learning about
        all jobs before hand.
        """
        # Open dag file
        try:
            DAG = open(dag_fn)
        except Exception:
            raise AnalyzerError(f"could not open dag file {dag_fn}: exiting...")

        # Loop through the dag file
        for line in DAG:
            line = line.strip(" \t")
            if line.startswith("#"):
                # Skip comments
                continue
            line = line.rstrip("\n\r")  # Remove new lines, if any
            line = line.split("#")[0]  # Remove inline comments too
            line = line.strip()  # Remove any remaining spaces at both ends
            if len(line) == 0:
                # Skip empty lines
                continue
            if line.startswith("JOB"):
                # This is a job line, let's parse it
                my_job = line.split()
                if len(my_job) != 3:
                    logger.warning("confused parsing dag line: %s" % (line))
                    continue
                if not self.has_seen(my_job[1]):
                    self.add_job(my_job[1], "UNSUBMITTED")
                    # Get submit file information from dag file
                    self.jobs[my_job[1]].sub_file = os.path.join(
                        self.options.input_dir, my_job[2]
                    )
                    if my_job[1].startswith("pegasus-plan") or my_job[1].startswith(
                        "subdax_"
                    ):
                        # Mark job as subdax
                        self.jobs[my_job[1]].is_subdax = True
                else:
                    logger.warning("job appears twice in dag file: %s" % (my_job[1]))
            if line.startswith("SUBDAG EXTERNAL"):
                # This is a subdag line, parse it to get job name and directory
                my_job = line.split()
                if len(my_job) != 6:
                    logger.warning("confused parsing dag line: %s" % (line))
                    continue
                if not self.has_seen(my_job[2]):
                    self.add_job(my_job[2], "UNSUBMITTED")
                    self.jobs[my_job[2]].is_subdag = True
                    self.jobs[my_job[2]].dag_path = my_job[3]
                    self.jobs[my_job[2]].subdag_dir = my_job[5]
                else:
                    logger.warning("job appears twice in dag file: %s" % (my_job[2]))
            if line.startswith("SCRIPT PRE"):
                # This is a SCRIPT PRE line, parse it to get the script for the job
                my_script = BaseAnalyze.re_parse_script_pre.search(line)
                if my_script is None:
                    # Couldn't parse line
                    logger.warning("confused parsing dag line: %s" % (line))
                    continue
                # Get job name, and check if we have it
                my_job = my_script.group(1)
                if not self.has_seen(my_job):
                    # Cannot find this job, ignore this line
                    logger.warning(
                        "couldn't find job: %s for PRE SCRIPT line in dag file"
                        % (my_job)
                    )
                    continue
                # Good, copy PRE script line to our job structure
                self.jobs[my_job].pre_script = my_script.group(2)
            if line.startswith("VARS"):
                # This is a VARS line, parse it to get the condor substitutions
                if len(line.split()) > 2:
                    # Line looks promising...
                    my_job = line.split()[1]
                    if not self.has_seen(my_job):
                        # Cannot find this job, ignore this line
                        logger.warning(
                            "couldn't find job: %s for VARS line in dag file" % (my_job)
                        )
                        continue
                    # Good, parse the condor substitutions, and create substitution dictionary
                    for my_key, my_val in BaseAnalyze.re_parse_condor_subs.findall(
                        line
                    ):
                        self.jobs[my_job].condor_subs[my_key] = my_val

    def parse_jobstate_log(self, jobstate_fn):
        """
        This function parses the jobstate.log file, loading all job information
        """
        # Open log file
        try:
            JSDL = open(jobstate_fn)
        except Exception:
            raise AnalyzerError(f"could not open file {jobstate_fn}: exiting...")

        # Loop through the log file
        for line in JSDL:
            sp = line.split()
            # Skip lines that don't have enough items
            if len(sp) < 6:
                continue
            # Skip monitord comments
            if sp[1] == "INTERNAL":
                continue

            # Ok, we have a valid job
            jobname = sp[1]
            jobstate = sp[2]
            condor_id = sp[3]

            # Add to job list if we have never seen this job before
            if not self.has_seen(jobname):
                logger.warning("job %s not present in dag file" % (jobname))
                self.add_job(jobname, jobstate)
                if jobname.startswith("pegasus-plan") or jobname.startswith("subdax_"):
                    # Mark job as subdax
                    self.jobs[jobname].is_subdax = True
            else:
                # Update job state
                self.update_job_state(jobname, jobstate)

            # Update condor id if we reached the SUBMIT state
            if jobstate == "SUBMIT":
                self.update_job_condor_info(jobname, condor_id)
                # Keep track of retries
                if self.jobs[jobname].retries is None:
                    self.jobs[jobname].retries = 0
                else:
                    self.jobs[jobname].retries = self.jobs[jobname].retries + 1

        # Close log file
        JSDL.close()

    def has_seen(self, job_name):
        """
        This function returns true if we are already tracking job_name
        """
        if job_name in self.jobs:
            return True
        return False

    def add_job(self, job_name, job_state=""):
        """
        This function adds a job to our list
        """
        # Don't add the same job twice
        if job_name in self.jobs:
            return

        newjob = Job(job_name, job_state)
        self.jobs[job_name] = newjob

    def update_job_state(self, job_name, job_state=""):
        """
        This function updates the job state of a given job
        """
        # Make sure we have this job
        if not job_name in self.jobs:
            # Print a warning message
            logger.error("could not find job %s" % (job_name))
            return

        self.jobs[job_name].set_state(job_state)

    def update_job_condor_info(self, job_name, condor_id="-"):
        """
        This function updates a job's condor_id (it splits it into process
        and cluster)
        """
        # Make sure we have this job
        if not job_name in self.jobs:
            # Print a warning message
            logger.error("could not find job %s" % (job_name))
            return

        # Nothing to do if condor_id is not defined
        if condor_id == "-":
            return

        my_split = condor_id.split(".")

        # First part is cluster id
        self.jobs[job_name].cluster = my_split[0]

        # If we have two pieces, second piece is process
        if len(my_split) >= 2:
            self.jobs[job_name].process = my_split[1]

    def analyze(self):
        """
        This function processes all currently known jobs, generating some statistics
        """

        for my_job in self.jobs:
            self.counts.total += 1
            if (
                self.jobs[my_job].state == "POST_SCRIPT_SUCCESS"
                or self.jobs[my_job].state == "JOB_SUCCESS"
            ):
                self.counts.success = self.counts.success + 1
            elif (
                self.jobs[my_job].state == "POST_SCRIPT_FAILURE"
                or self.jobs[my_job].state == "JOB_FAILURE"
            ):
                self.counts.failed_jobs.append(my_job)
                self.counts.failed += 1
            elif self.jobs[my_job].state == "UNSUBMITTED":
                self.counts.unsubmitted = self.counts.unsubmitted + 1
            else:
                # It seems we don't have a final result for this job
                self.counts.unknown_jobs.append(my_job)
                self.counts.unknown += 1

    def get_output_error(self, job):
        """
        This function outputs both output and error files for a given job.
        """
        out_file = self.find_latest_log(job.stdout_file)
        err_file = self.find_latest_log(job.stderr_file)
        tasks = job.tasks
        my_parser = kickstart_parser.Parser(out_file)
        my_output = my_parser.parse_stdout_stderr()
        my_task_id = 0

        if len(my_output) > 0:
            # Ok, we got valid kickstart records, output stdout and stderr for tasks that failed
            for entry in my_output:
                # Count tasks, the same way as pegasus-monitord for Stampede
                my_task_id = my_task_id + 1
                tasks[my_task_id] = Task(task_submit_seq=my_task_id)
                if not "derivation" in entry or not "transformation" in entry:
                    continue
                if not "exitcode" in entry and not "error" in entry:
                    continue
                if "exitcode" in entry:
                    try:
                        if int(entry["exitcode"]) == 0:
                            # Skip tasks with exitcode equals to zero
                            continue
                    except Exception:
                        logger.warning("couldn't convert exitcode to integer!")
                        continue
                else:
                    # We must have "error" in entry
                    pass

                job.site = entry.get("resource", "-")
                job.hostname = entry.get("hostname", "-")
                job.stdout_text = entry.get("stdout", "-")
                job.stderr_text = entry.get("stderr", "-")
                job.work_dir = entry.get("cwd", "-")
                tasks[my_task_id].executable = entry.get("name", "-")
                tasks[my_task_id].arguments = entry.get("argument-vector", "-")
                tasks[my_task_id].exitcode = entry.get("exitcode", "-")
                tasks[my_task_id].transformation = entry.get("transformation", "-")
                tasks[my_task_id].abs_task_id = entry.get("derivation", "-")

        else:
            # Not able to parse the kickstart output file, let's just dump the out and err files
            # Print outfile to screen
            job.stdout_text = self.dump_file(out_file)
            # Print errfile to screen
            job.stderr_text = self.dump_file(err_file)

    def find_latest_log(self, log_file_base):
        """
        This function tries to locate the latest log file
        """
        last_log = None
        curr_log = None

        if os.access(log_file_base, os.F_OK):
            last_log = log_file_base

        # Starts from .000
        sf = 0

        while sf < BaseAnalyze.MAXLOGFILE:
            curr_log = log_file_base + ".%03d" % (sf)
            if os.access(curr_log, os.F_OK):
                last_log = curr_log
                sf = sf + 1
            else:
                break

        return last_log

    def dump_file(self, file):
        """
        This function dumps a file to our stdout
        """
        data = ""
        if file is not None:
            try:
                with open(file) as file:
                    data = file.read().rstrip()
            except Exception:
                logger.warning("*** Cannot access: %s" % (file))
        return data


class DebugWF(BaseAnalyze):
    def __init__(self, options):
        self.options = options

    def debug_workflow(self):
        """
        This function handles the mode where the analyzer
        is used to debug a job in a workflow
        """

        # Check if we can find this job's submit file
        if not self.options.debug_job.endswith(".sub"):
            self.options.debug_job = self.options.debug_job + ".sub"
        # Figure out job name
        jobname = os.path.basename(self.options.debug_job)
        jobname = jobname[0 : jobname.find(".sub")]
        # Create job class
        my_job = Job(jobname)
        my_job.sub_file = self.options.debug_job

        if not os.access(self.options.debug_job, os.R_OK):
            raise AnalyzerError(
                f"cannot access job submit file: {self.options.debug_job}"
            )

        # Handle the temporary directory option
        if self.options.debug_dir is None:
            # Create temporary directory
            try:
                self.options.debug_dir = tempfile.mkdtemp()
            except Exception:
                raise AnalyzerError(f"could not create temporary directory!")

        else:
            # Make sure directory specified is writable
            self.options.debug_dir = os.path.abspath(self.options.debug_dir)
            if not os.access(self.options.debug_dir, os.F_OK):
                # Create directory if it does not exist
                try:
                    os.mkdir(self.options.debug_dir)
                except Exception:
                    logger.error(
                        "cannot create debug directory: %s" % (self.options.debug_dir)
                    )

            # Check if we can write to the debug directory
            if not os.access(self.options.debug_dir, os.W_OK):
                raise AnalyzerError(
                    f"not able to write to temporary directory: {self.options.debug_dir}"
                )

        # Handle workflow type
        if self.options.workflow_type is not None:
            if self.options.workflow_type.lower() == "condor":
                logger.info("debugging condor type workflow")
                self.debug_condor(my_job)
            else:
                raise AnalyzerError(
                    f"workflow type {self.options.workflow_type} not supported!"
                )
        else:
            logger.info("debugging condor type workflow")
            self.debug_condor(my_job)

        # All done, in case we are back here!
        return

    def debug_condor(self, my_job):
        """
        This function is used to debug a condor job. It creates a
        shell script in the debug_dir directory that is used to
        copy all necessary files to the (local) debug_dir directory
        and then execute the job locally.
        """

        # Set strict mode in order to parse everything in the submit file
        self.options.strict_mode = True
        # Parse submit file
        BaseAnalyze.parse_submit_file(my_job, self.options)

        # Create script name
        debug_script_basename = "debug_" + my_job.name + ".sh"
        debug_script_err_basename = "debug_" + my_job.name + ".err"
        debug_script_name = os.path.join(self.options.debug_dir, debug_script_basename)

        try:
            debug_script = open(debug_script_name, "w")
        except Exception:
            raise AnalyzerError(f"cannot create debug script {debug_script}")

        try:
            # Start with the bash line
            debug_script.write("#!/bin/bash\n")
            debug_script.write("\n")
            debug_script.write("set -e\n")
            debug_script.write("\n")

            debug_script.write(
                """
    check_file() {
    file=$1
    if [ -e $file ]; then
        return 0
    else
        echo "ERROR: output file $file not generated"
        STATUS=$(($STATUS + 1))
    fi
    }
             """
            )
            debug_script.write("\n")
            if my_job.wf_submit_dir:
                debug_script.write("wf_submit_dir=%s" % my_job.wf_submit_dir)
                debug_script.write("\n")
            debug_script.write(
                "export pegasus_lite_work_dir=%s" % self.options.debug_dir
            )
            debug_script.write("\n")
            debug_script.write("# Copy any files that are needed\n")

            debug_script.write('echo "copying input files..."\n')
            debug_script.write("\n")
            # Copy all files that we need
            for my_file in my_job.transfer_input_files.split(","):
                if len(my_file):
                    if my_job.wf_submit_dir:
                        # make sure we substitute
                        my_file = my_file.replace(
                            "$(wf_submit_dir)", "${wf_submit_dir}"
                        )

                    if len(my_job.initial_dir) and not (
                        my_file.startswith("/")
                        or my_file.startswith("${wf_submit_dir}")
                    ):
                        # Add the initial dir to all files to be copied
                        # as long as they dont start with / or ${wf_submit_dir}
                        my_file = os.path.join(my_job.initial_dir, my_file)
                    debug_script.write(f"cp {my_file} {self.options.debug_dir}\n")

            # Extra newline before executing the job
            debug_script.write("\n")
            debug_script.write('echo "copying input files completed."\n')
            debug_script.write("\n")

            # check if the job is a Pegasus Lite wrapped job or an earlier type
            job_pegasus_lite_wrapper = self.get_pegasus_lite_wrapper(my_job)
            job_executable = my_job.executable
            if job_pegasus_lite_wrapper is None:
                # older non pegasus lite mode /sipht case?
                debug_script.write("# Set the execute bit on the executable\n")
                debug_script.write(
                    "chmod +x %s\n"
                    % (os.path.join(self.options.debug_dir, my_job.executable))
                )
                debug_script.write("\n")
            else:
                # generate a separate sh file that parts of pegasus-lite script
                debug_wrapper = self.generate_pegasus_lite_debug_wrapper(
                    job_pegasus_lite_wrapper
                )
                job_executable = debug_wrapper

            job_executable = (
                os.path.join(self.options.debug_dir, job_executable) + my_job.arguments
            )

            debug_script.write('echo "executing job: %s"\n' % (job_executable))
            debug_script.write("\n")
            debug_script.write("# Now, execute the job\n")
            # disable fail on error before launching
            debug_script.write("set +e\n")
            debug_script.write("%s" % (job_executable))

            # redirect stderr for pegasus lite jobs to separate err file
            if job_pegasus_lite_wrapper is not None:
                debug_script.write(
                    " 2> "
                    + os.path.join(self.options.debug_dir, debug_script_err_basename)
                )

            debug_script.write("\n\n")

            # Remember not to put anything between running the executable
            # and checking the exit code, otherwise $? will break...
            debug_script.write("# Check error code\n")
            debug_script.write("rc=$?\n")
            # reenable fail on error before launching
            debug_script.write("set -e\n")
            debug_script.write("if [ $rc -eq 0 ]; then\n")
            debug_script.write('   echo "executable ran successfully"\n')
            debug_script.write("else\n")
            debug_script.write('   echo "executable failed with error $?"\n')
            debug_script.write("   exit $rc\n")
            debug_script.write("fi\n")

            # PM-989 add existence check for all output files transferred
            # by condor file IO
            debug_script.write("STATUS=0\n")
            debug_script.write("set +e \n")
            for out_file in my_job.transfer_output_files.split(","):
                if len(out_file):
                    debug_script.write("check_file %s\n" % out_file)

            debug_script.write(
                """
    if [ $STATUS -eq 0 ]; then
        echo "all output files were created"
    else
        echo "ERROR: Not all output files were created"
    fi

    exit $STATUS
            """
            )

        except Exception:
            raise AnalyzerError(f"cannot write to file {debug_script}")

        # We are done writing the file!
        debug_script.close()

        try:
            # Make our debug script executable
            os.chmod(debug_script_name, 0o755)
        except Exception:
            raise AnalyzerError(
                f"cannot change permissions for the debug script {debug_script}"
            )

        # Print next step
        print()
        print("%s: finished generating job debug script!" % (prog_base))
        print()
        print("To run it, you need to type:")
        print("   $ cd %s" % (self.options.debug_dir))
        print("   $ ./%s" % (debug_script_basename))
        print()

    def get_pegasus_lite_wrapper(self, my_job):
        """
        This function returns whether a Pegasus Lite Wrapper
        exists for the job or not. Returns the path to wrapper if it exists
        """
        # First we check if this is a SUBDAG job from the dag file
        if my_job.is_subdag:
            # Nothing to do here
            return None

        if my_job.sub_file == "":
            # Create full path for the submit file if we already don't have the sub file set up
            my_job.sub_file = os.path.join(self.options.input_dir, my_job.name + ".sub")

        # Create full path for the pegasus_lite_wrapped job on basis of where submit file is
        pegasus_lite_wrapper = os.path.join(
            os.path.dirname(my_job.sub_file), my_job.name + ".sh"
        )

        # Try to access submit file
        if os.access(pegasus_lite_wrapper, os.R_OK):
            # Open submit file
            try:
                SUB = open(pegasus_lite_wrapper)
            except Exception:
                # print "error opening submit file: %s" % (my_job.sub_file)
                raise AnalyzerError("error opening submit file: " + my_job.sub_file)

            else:
                SUB.close()

        return pegasus_lite_wrapper

    def generate_pegasus_lite_debug_wrapper(self, pegasus_lite_wrapper):
        """
        This generates a debug wrapper for the pegasus lite job
        It copies the the pegasus lite job till part of the stage out of outputs
        """
        if pegasus_lite_wrapper is None:
            return None

        debug_wrapper = os.path.join(
            self.options.debug_dir,
            "stripped_pl_" + os.path.basename(pegasus_lite_wrapper),
        )
        try:
            DEBUG_WRAPPER = open(debug_wrapper, "w")
            WRAPPER = open(pegasus_lite_wrapper)
            for line in WRAPPER:
                if line.startswith("# stage out"):
                    # we only continue till we hit the stage out part
                    break

                # line = line.strip(" \t") # Remove leading and trailing spaces
                if self.options.debug_job_local_executable is not None:
                    # enable matching to replace the kickstart invocation with local path
                    ks_invocation = BaseAnalyze.re_ks_invocation.search(line)

                    if ks_invocation and ks_invocation.groups() > 0:
                        logger.debug(
                            "Match found for kickstart invocation in pegasus lite wrapper %s"
                            % ks_invocation.group(0)
                        )
                        logger.debug(
                            "Executable to be replaced is %s" % ks_invocation.group(1)
                        )
                        substituted_string = (
                            line[: ks_invocation.start(1)]
                            + self.options.debug_job_local_executable
                            + line[ks_invocation.end(1) :]
                        )
                        line = substituted_string
                        logger.debug("Substituted invocation is %s" % line)

                DEBUG_WRAPPER.write(line)

        except Exception:
            # fail silently for now...
            return None
        else:
            if DEBUG_WRAPPER is not None:
                DEBUG_WRAPPER.close()
            if WRAPPER is not None:
                WRAPPER.close()

        # set the x bit
        os.chmod(debug_wrapper, 0o755)

        return debug_wrapper
