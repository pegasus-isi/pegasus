"""
This file implements the Job class for pegasus-monitord.
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

import collections
import json
import logging
import os
import re
from io import StringIO

from Pegasus.tools import utils

logger = logging.getLogger(__name__)

# Global variables
good_rsl = {"maxcputime": 1, "maxtime": 1, "maxwalltime": 1}
MAX_OUTPUT_LENGTH = 2 ** 16 - 1  # Only keep stdout to 64K

# some constants
NOOP_JOB_PREFIX = (
    "noop_"  # prefix for noop jobs for which .out and err files are not created
)

MONITORING_EVENT_START_MARKER = "@@@MONITORING_PAYLOAD - START@@@"
MONITORING_EVENT_END_MARKER = "@@@MONITORING_PAYLOAD - END@@@"

# Used in parse_sub_file
re_rsl_string = re.compile(r"^\s*globusrsl\W", re.IGNORECASE)
re_rsl_clean = re.compile(r"([-_])")
re_site_parse_gvds = re.compile(
    r"^\s*\+(pegasus|wf)_(site|resource)\s*=\s*([\'\"])?(\S+)\3"
)
re_parse_transformation = re.compile(r"^\s*\+pegasus_wf_xformation\s*=\s*(\S+)")
re_parse_derivation = re.compile(r"^\s*\+pegasus_wf_dax_job_id\s*=\s*(\S+)")
re_parse_multiplier_factor = re.compile(r"^\s*\+pegasus_cores\s=\s(\S+)")
re_parse_executable = re.compile(r"^\s*executable\s*=\s*(\S+)")
re_parse_arguments = re.compile(r'^\s*arguments\s*=\s*"([^"\r\n]*)"')
re_parse_environment = re.compile(r"^\s*environment\s*=\s*(.*)")
re_site_parse_euryale = re.compile(r"^\#!\s+site=(\S+)")
re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")
re_parse_input = re.compile(r"^\s*intput\s*=\s*(\S+)")
re_parse_output = re.compile(r"^\s*output\s*=\s*(\S+)")
re_parse_error = re.compile(r"^\s*error\s*=\s*(\S+)")
re_parse_job_class = re.compile(r"^\s*\+pegasus_job_class\s*=\s*(\S+)")
re_parse_pegasuslite_hostname = re.compile(
    r"^.*Executing on host\s*(\S+)(?:\s*IP=(\S+))*.*$", re.MULTILINE
)


TaskOutput = collections.namedtuple("TaskOutput", ["user_data", "events"])


class IntegrityMetric:
    """
    Class for storing integrity metrics and combining them based solely on type and file type combination
    """

    def __init__(self, type, file_type, count=0, succeeded=0, failed=0, duration=0.0):
        self.type = type
        self.file_type = file_type
        self.count = count
        self.succeeded = succeeded
        self.failed = failed
        self.duration = duration

    def __eq__(self, other):
        return self.type == other.type and self.file_type == other.file_type

    def __hash__(self):
        return hash(self.key())

    def __str__(self):
        return "({},{},{}, {}, {} , {})".format(
            self.type,
            self.file_type,
            self.count,
            self.succeeded,
            self.failed,
            self.duration,
        )

    def key(self):
        return self.type + ":" + self.file_type

    def merge(self, other):
        if self == other:
            self.count += other.count
            self.succeeded += other.succeeded
            self.failed += other.failed
            self.duration += other.duration
            return
        raise KeyError(f"Objects not compatible {self} {other}")


class Job:
    """
    Class used to keep information needed to track a particular job
    """

    JOBTYPE_TO_DESC = {
        "0": "unassigned",
        "1": "compute",
        "2": "stage-in-tx",
        "3": "stage-out-tx",
        "4": "registration",
        "5": "inter-site-tx",
        "6": "create-dir",
        "7": "stage_worker_tx",
        "8": "cleanup",
        "9": "chmod",
        "10": "dax",
        "11": "dag",
    }

    # Variables that describe a job, as per the Stampede schema
    # Some will be initialized in the init method, others will
    # get their values from the kickstart output file when a job
    # finished

    def __init__(self, wf_uuid, name, job_submit_dir, job_submit_seq):
        """
        This function initializes the job parameters with the
        information available when a job is detected in the
        "PRE_SCRIPT_STARTED" or the "SUBMIT" state. Other parameters
        will remain None until a job finishes and a kickstart output
        file can be parsed.
        """
        self._wf_uuid = wf_uuid
        self._exec_job_id = name
        self._job_submit_seq = job_submit_seq
        self._job_submit_dir = (
            job_submit_dir  # the submit directory all the job related files exist
        )
        self._sched_id = None
        self._site_name = None
        self._host_id = None
        self._host_ip = None
        self._remote_user = None
        self._remote_working_dir = None
        self._cluster_start_time = None
        self._cluster_duration = None
        self._job_type = None
        self._job_state = None
        self._job_state_seq = 0
        self._job_state_timestamp = None
        self._job_output_counter = 0
        self._pre_script_start = None
        self._pre_script_done = None
        self._pre_script_exitcode = None
        self._main_job_start = None
        self._main_job_done = None
        self._main_job_transformation = None
        self._main_job_derivation = None
        self._main_job_executable = None
        self._main_job_arguments = None
        self._main_job_exitcode = None
        self._main_job_multiplier_factor = None
        self._post_script_start = None
        self._post_script_done = None
        self._post_script_exitcode = None
        self._input_file = None
        self._output_file = None
        self._error_file = None
        self._stdout_text = None
        self._stderr_text = None
        self._additional_monitoring_events = []
        self._multipart_events = []
        self._cpu_attribs = None
        self._job_dagman_out = None  # _CONDOR_DAGMAN_LOG from environment
        # line for pegasus-plan and subdax_ jobs
        self._kickstart_parsed = False  # Flag indicating if the kickstart
        # output for this job was parsed or not
        self._has_rotated_stdout_err_files = (
            False  # Flag indicating whether we detected that job stdout|stderr
        )
        # was rotated or not, as is the default case.
        self._deferred_job_end_kwargs = None
        self._integrity_metrics = set()

    def _get_jobtype_desc(self):
        """
        Returns a textual description of the job type
        :return:
        """
        desc = "unknown"

        if self._job_type is None:
            return desc

        if self._job_type in Job.JOBTYPE_TO_DESC:
            desc = Job.JOBTYPE_TO_DESC[self._job_type]
        else:
            logger.error(
                "Unknown job type %s encountered for job %s"
                % (self._job_type, self._exec_job_id)
            )

        return desc

    def _add_multipart_events(self, events):
        """
        add multipart events to the job, separating any integrity metrics,
        since Integrity metrics are stored internally not as addditonal monitoring event
        :param events:
        :return:
        """
        for event in events:
            if not "multipart" in event:
                # Not this one... skip to the next
                logger.error(
                    " Mismatched multipart record %s in job %s"
                    % (event, self._exec_job_id)
                )
                continue
            if "integrity_summary" in event:
                # PM-1390 multipart events
                m = event["integrity_summary"]
                metric = IntegrityMetric(
                    type="check",  # For time being they always refer to verification
                    file_type="input",  # should be specified in multipart
                    succeeded=m["succeeded"] if "succeeded" in m else 0,
                    failed=m["failed"] if "failed" in m else 0,
                    duration=m["duration"] if "duration" in m else 0.0,
                )
                self.add_integrity_metric(metric)
            else:  # catch all
                self._multipart_events.append(event)

    def _add_additional_monitoring_events(self, events):
        """
        add monitoring events to the job, separating any integrity metrics,
        since Integrity metrics are stored internally not as addditonal monitoring event
        :param events:
        :return:
        """
        for event in events:
            if "monitoring_event" in event:
                # this is how integrity metrics were reported in 4.9.x series
                # as monitoring events; NOT multipart records in the job.out files
                name = event["monitoring_event"]
                if name == "int.metric":
                    # split elements in payload to IntegrityMetric
                    # add it internally for aggregation
                    for m in event["payload"]:
                        metric = IntegrityMetric(
                            type=m.get("event"),
                            file_type=m.get("file_type"),
                            count=m["count"] if "count" in m else 0,
                            succeeded=m["succeeded"] if "succeeded" in m else 0,
                            failed=m["failed"] if "failed" in m else 0,
                            duration=m["duration"] if "duration" in m else 0.0,
                        )
                        self.add_integrity_metric(metric)
                else:
                    self._additional_monitoring_events.append(event)
            else:  # catch all
                self._additional_monitoring_events.append(event)

    def add_integrity_metric(self, metric):
        """
        adds an integrity metric, if a metric with the same key already exists we retrive
        existing value and add the contents of metric passed
        :param metric:
        :return:
        """
        if metric is None:
            return

        for m in self._integrity_metrics:
            if metric == m:
                # add to existing metric
                m.merge(metric)
                break
        else:
            self._integrity_metrics.add(metric)

    def set_job_state(self, job_state, sched_id, timestamp, status):
        """
        This function sets the job state for this job. It also updates
        the times the main job and PRE/POST scripts start and finish.
        """
        self._job_state = job_state
        self._job_state_timestamp = int(timestamp)
        # Increment job state sequence
        self._job_state_seq = self._job_state_seq + 1

        # Set sched_id if we don't already have it...
        if self._sched_id is None:
            self._sched_id = sched_id
        # Record timestamp for certain job states
        if job_state == "PRE_SCRIPT_STARTED":
            self._pre_script_start = int(timestamp)
        elif job_state == "PRE_SCRIPT_SUCCESS" or job_state == "PRE_SCRIPT_FAILURE":
            self._pre_script_done = int(timestamp)
            self._pre_script_exitcode = utils.regular_to_raw(status)
        elif job_state == "POST_SCRIPT_STARTED":
            self._post_script_start = int(timestamp)
        elif job_state == "POST_SCRIPT_TERMINATED":
            self._post_script_done = int(timestamp)
        elif job_state == "EXECUTE":
            self._main_job_start = int(timestamp)
        elif job_state == "JOB_TERMINATED":
            self._main_job_done = int(timestamp)
        elif (
            job_state == "JOB_ABORTED"
            or job_state == "SUBMIT_FAILED"
            or job_state == "GLOBUS_SUBMIT_FAILED"
            or job_state == "GRID_SUBMIT_FAILED"
        ):
            self._main_job_done = int(
                timestamp
            )  # PM-805, PM-877 job was aborted or submit failed, good chance job terminated event did not happen.
        elif job_state == "JOB_SUCCESS" or job_state == "JOB_FAILURE":
            self._main_job_exitcode = utils.regular_to_raw(status)
        elif job_state == "POST_SCRIPT_SUCCESS" or job_state == "POST_SCRIPT_FAILURE":
            self._post_script_exitcode = utils.regular_to_raw(status)
            if self._main_job_done is None:
                # PM-1016 Missing JOB_TERMINATED event.
                self._main_job_done = int(timestamp)

    def parse_sub_file(self, stamp, submit_file):
        """
        This function parses a job's submit file and returns job
        planning information. In addition, we try to populate the job
        type from information in the submit file.
        # paramtr: stamp(IN): timestamp associated with the log line
        # paramtr: submit_file(IN): submit file name
        # globals: good_rsl(IN): which RSL keys constitute time requirements
        # returns: (largest job time requirement in minutes, destination site)
        # returns: (None, None) if sub file not found
        """
        parse_environment = False
        my_result = None
        my_site = None

        # Update stat record for submit file
        try:
            my_stats = os.stat(submit_file)
        except OSError:
            # Could not stat file
            logger.error("stat %s" % (submit_file))
            return my_result, my_site

        # Check submit file timestamp
        if stamp < my_stats[8]:  # mtime
            logger.info(
                "%s: sub file modified: job timestamp=%d, file mtime=%d, diff=%d"
                % (submit_file, stamp, my_stats[8], my_stats[8] - stamp)
            )

        # Check if we need to parse the environment line
        if self._exec_job_id.startswith("pegasus-plan") or self._exec_job_id.startswith(
            "subdax_"
        ):
            parse_environment = True

        try:
            SUB = open(submit_file)
        except OSError:
            logger.error("unable to parse %s" % (submit_file))
            return my_result, my_site

        # Parse submit file
        for my_line in SUB:
            if re_rsl_string.search(my_line):
                # Found RSL string, do parse now
                for my_match in re.findall(r"\(([^)]+)\)", my_line):
                    # Split into key and value
                    my_k, my_v = my_match.split("=", 1)
                    # Remove _- characters from string
                    my_k = re_rsl_clean.sub("", my_k)
                    if my_k.lower() in good_rsl and my_v > my_result:
                        try:
                            my_result = int(my_v)
                        except ValueError:
                            my_result = None
            elif re_site_parse_gvds.search(my_line):
                # GVDS agreement
                my_site = re_site_parse_gvds.search(my_line).group(4)
                self._site_name = my_site
            elif re_site_parse_euryale.search(my_line):
                # Euryale specific comment
                my_site = re_site_parse_euryale.search(my_line).group(1)
                self._site_name = my_site
            elif re_parse_transformation.search(my_line):
                # Found line with job transformation
                my_transformation = re_parse_transformation.search(my_line).group(1)
                # Remove quotes, if any
                my_transformation = my_transformation.strip('"')
                self._main_job_transformation = my_transformation
            elif re_parse_derivation.search(my_line):
                # Found line with job derivation
                my_derivation = re_parse_derivation.search(my_line).group(1)
                # Remove quotes, if any
                my_derivation = my_derivation.strip('"')
                if my_derivation == "null":
                    # If derivation is the "null" string, we don't want to take it
                    self._main_job_derivation = None
                else:
                    self._main_job_derivation = my_derivation
            elif re_parse_executable.search(my_line):
                # Found line with executable
                my_executable = re_parse_executable.search(my_line).group(1)
                # Remove quotes, if any
                my_executable = my_executable.strip('"')
                self._main_job_executable = my_executable
            elif re_parse_arguments.search(my_line):
                # Found line with arguments
                my_arguments = re_parse_arguments.search(my_line).group(1)
                # Remove quotes, if any
                my_arguments = my_arguments.strip('"')
                self._main_job_arguments = my_arguments
            elif re_parse_multiplier_factor.search(my_line):
                # Found line with multiplier_factor
                my_multiplier_factor = re_parse_multiplier_factor.search(my_line).group(
                    1
                )
                try:
                    self._main_job_multiplier_factor = int(my_multiplier_factor)
                except ValueError:
                    logger.warning(
                        "%s: cannot convert multiplier factor: %s"
                        % (os.path.basename(submit_file), my_multiplier_factor)
                    )
                    self._main_job_multiplier_factor = None
            elif re_parse_input.search(my_line):
                # Found line with input file
                my_input = re_parse_input.search(my_line).group(1)
                # Remove quotes, if any
                my_input = my_input.strip('"')
                self._input_file = os.path.normpath(my_input)
            elif re_parse_output.search(my_line):
                # Found line with output file
                my_output = re_parse_output.search(my_line).group(1)
                # Remove quotes, if any
                my_output = my_output.strip('"')
                self._output_file = os.path.normpath(my_output)
            elif re_parse_error.search(my_line):
                # Found line with error file
                my_error = re_parse_error.search(my_line).group(1)
                # Remove quotes, if any
                my_error = my_error.strip('"')
                self._error_file = os.path.normpath(my_error)
            elif parse_environment and re_parse_environment.search(my_line):
                self._job_dagman_out = self.extract_dagman_out_from_condor_env(my_line)
                if self._job_dagman_out is None:
                    logger.error(
                        "Unable to parse dagman out file from environment key %s in submit file for job %s"
                        % (my_line, self._exec_job_id)
                    )
            elif re_parse_job_class.search(my_line):
                self._job_type = re_parse_job_class.search(my_line).group(1)
        SUB.close()

        # All done!
        return my_result, my_site

    def extract_dagman_out_from_condor_env(self, condor_env):
        """
        This function extracts the dagman out file from the condor environment
        if one is specified

        :param condor_env: the environment line from the condor submit file
        :return: the dagman out file if detected else None
        """
        # Found line with environment
        env_value = re_parse_environment.search(condor_env).group(1)

        # strip any enclosing quotes if any
        stripped_env_value = re.sub(r'^"|"$', "", env_value)

        if len(env_value) == len(stripped_env_value):
            # we have old style condor environment with environment NOT ENCLOSED in double quotes
            # and split by ;
            sub_props = stripped_env_value.split(";")
        else:
            # we have new style condor environment with environment enclosed in double quotes
            # and split by whitespace
            sub_props = stripped_env_value.split(" ")

        dagman_out = None
        for sub_prop_line in sub_props:
            sub_prop_line = sub_prop_line.strip()  # Remove any spaces
            if len(sub_prop_line) == 0:
                continue
            sub_prop = re_parse_property.search(sub_prop_line)
            if sub_prop:
                if sub_prop.group(1) == "_CONDOR_DAGMAN_LOG":
                    dagman_out = sub_prop.group(2)
                    break

        return dagman_out

    def extract_job_info(self, kickstart_output):
        """
        This function reads the output from the kickstart parser and
        extracts the job information for the Stampede schema. It first
        looks for an invocation record, and then for a clustered
        record.

        Returns None if an error occurs, True if an invocation record
        was found, and False if it wasn't.
        """

        # Check if we have anything
        if len(kickstart_output) == 0:
            return None

        # Kickstart was parsed
        self._kickstart_parsed = True

        # PM-1157 we construct run dir from job submit dir
        self._job_submit_dir

        # Let's try to find an invocation record...
        my_invocation_found = False
        my_task_number = 0
        self._stdout_text = ""  # Initialize stdout
        stdout_text_list = []
        stdout_size = 0
        for my_record in kickstart_output:
            if "multipart" in my_record:
                # PM-1390 convert to integrity metrics
                logger.debug("Multipart record %s", my_record)
                self._add_multipart_events([my_record])
            elif not "invocation" in my_record:
                # Not this one... skip to the next
                logger.trace("Skipping %s", my_record)
                continue
            # Ok, we have an invocation record, extract the information we
            # need. Note that this may overwrite information obtained from
            # the submit file (e.g. the site_name).

            # Increment task_number
            my_task_number = my_task_number + 1

            if not my_invocation_found:
                # Things we only need to do once
                if "resource" in my_record:
                    self._site_name = my_record["resource"]
                if "user" in my_record:
                    self._remote_user = my_record["user"]
                if "cwd" in my_record:
                    self._remote_working_dir = my_record["cwd"]
                # We are done with this part
                my_invocation_found = True

            # PM-1488 for containers case we have to remap hostname for all invocation
            # records in a clustered job, not just the first one. otherwise the sqlite db
            # will have dangling host records in the host table that has docker container generated
            # names but no jobs associated with them.
            if "hostname" in my_record:
                ks_hostname = my_record["hostname"]
                if self._host_id is None:
                    # PM-1488 only set the hostname to kickstart reported one only if
                    # it is not determined already (PegasusLite case) by parsing the job err file
                    self._host_id = ks_hostname
                elif self._host_id != ks_hostname:
                    ks_hostaddr = my_record["hostaddr"]
                    if self._host_ip is not None:
                        # for 4.9 backward compatibilty where PegasusLite does not record IP
                        # we keep the kickstart reported ip address to allow for database
                        # population as host table requires an ip to be not null
                        my_record["hostaddr"] = self._host_ip

                    my_record["hostname"] = self._host_id
                    logger.trace(
                        "For job %s preferring %s %s over kickstart reported hostname %s %s"
                        % (
                            self._exec_job_id,
                            my_record["hostname"],
                            my_record["hostaddr"],
                            ks_hostname,
                            ks_hostaddr,
                        )
                    )

            # PM-1109 encode signal information if it exists
            signal_message = " "
            if "signalled" in my_record:
                # construct our own error message
                attrs = my_record["signalled"]
                signal_message = "Job was "
                if "action" in attrs:
                    signal_message += attrs["action"]
                if "signal" in attrs:
                    signal_message += " with signal " + attrs["signal"]

            # PM-641 optimization Modified string concatenation to a list join
            if "stdout" in my_record:
                task_output = self.split_task_output(my_record["stdout"])
                self._add_additional_monitoring_events(task_output.events)
                # PM-1152 we always attempt to store upto MAX_OUTPUT_LENGTH
                stdout = self.get_snippet_to_populate(
                    task_output.user_data, my_task_number, stdout_size, "stdout"
                )
                if stdout is not None:
                    try:
                        stdout_text_list.append(
                            utils.quote("#@ %d stdout\n" % (my_task_number))
                        )
                        stdout_text_list.append(utils.quote(stdout))
                        stdout_text_list.append(utils.quote("\n"))
                        stdout_size += len(stdout) + 20
                    except KeyError:
                        logger.exception(
                            "Unable to parse stdout section from kickstart record for task %s from file %s "
                            % (my_task_number, self.get_rotated_out_filename())
                        )

            if "stderr" in my_record:
                task_error = self.split_task_output(my_record["stderr"])
                # add the events to those retrieved from the application stderr
                self._add_additional_monitoring_events(task_error.events)
                # Note: we are populating task stderr from kickstart record to job stdout only
                stderr = self.get_snippet_to_populate(
                    signal_message + task_error.user_data,
                    my_task_number,
                    stdout_size,
                    "stderr",
                )
                if stderr is not None:
                    try:
                        stdout_text_list.append(
                            utils.quote("#@ %d stderr\n" % (my_task_number))
                        )
                        stdout_text_list.append(utils.quote(stderr))
                        stdout_text_list.append(utils.quote("\n"))
                        stdout_size += len(stderr) + 20
                    except KeyError:
                        logger.exception(
                            "Unable to parse stderr section from kickstart record for task %s from file %s "
                            % (my_task_number, self.get_rotated_out_filename())
                        )

            # PM-1398 pass cpu info
            if "cpu" in my_record:
                self._cpu_attribs = my_record["cpu"]

        if len(stdout_text_list) > 0:
            self._stdout_text = "".join(stdout_text_list)

            # PM-641 optimization merged encoding above
        # Now, we encode it!
        #        if self._stdout_text != "":
        #            self._stdout_text = utils.quote(self._stdout_text)

        if not my_invocation_found:
            logger.debug("cannot find invocation record in output")

        # Look for clustered record...
        my_cluster_found = False
        for my_record in kickstart_output:
            if not "clustered" in my_record:
                # Not this one... skip to the next
                continue
            # Ok found it, fill in cluster parameters
            if "duration" in my_record:
                self._cluster_duration = my_record["duration"]
            if "start" in my_record:
                # Convert timestamp to EPOCH
                my_start = utils.epochdate(my_record["start"])
                if my_start is not None:
                    self._cluster_start_time = my_start
            # No need to look further...
            my_cluster_found = True
            break

        if not my_cluster_found:
            logger.debug("cannot find cluster record in output")

        # Done populating Job class with information from the output file
        return my_invocation_found

    def get_job_state(self):
        """
        Returns the current job state
        :return:
        """
        return self._job_state

    def get_rotated_out_filename(self):
        """
        Returns the name of the rotated .out file for the job on the basis
        of the current counter
        """

        basename = self._output_file
        if self._has_rotated_stdout_err_files:
            basename += ".%03d" % (self._job_output_counter)

        return basename

    def get_rotated_err_filename(self):
        """
        Returns the name of the rotated .err file for the job on the basis
        of the current counter
        """

        basename = self._exec_job_id + ".err"
        if self._has_rotated_stdout_err_files:
            basename += ".%03d" % (self._job_output_counter)

        return basename

    def read_job_error_file(self, store_monitoring_events=True):
        """
        Reads the job error file and updates job structures to store the
        the stderr of the condor job and also attempts to parse the hostname
        from the stderr of the job

        :param store_monitoring_events: whether to store any parsed monitoring events in the job
        :return:
        """
        my_max_encoded_length = MAX_OUTPUT_LENGTH - 2000
        if self._error_file is None:
            # This is the case for SUBDAG jobs
            self._stderr_text = None
            return

        # Finally, read error file only
        run_dir = self._job_submit_dir
        basename = self.get_rotated_err_filename()
        my_err_file = os.path.join(run_dir, basename)

        try:
            ERR = open(my_err_file)
            # PM-1274 parse any monitoring events such as integrity related
            # from PegasusLite .err file
            job_stderr = self.split_task_output(ERR.read())
            buf = job_stderr.user_data

            if len(buf) > my_max_encoded_length:
                buf = buf[:my_max_encoded_length]
            self._stderr_text = utils.quote(buf)

            if store_monitoring_events:
                self._add_additional_monitoring_events(job_stderr.events)

            # PM-1355 attempt to determine the hostname from the pegasus lite job
            hostname_match = re_parse_pegasuslite_hostname.search(job_stderr.user_data)
            if hostname_match:
                # a match yes it is a PegasusLite job . gleam the hostname
                self._host_id = hostname_match.group(1)
                self._host_ip = hostname_match.group(2)

        except OSError:
            self._stderr_text = None
            if not self.is_noop_job():
                logger.warning(
                    "unable to read error file: %s, continuing..." % (my_err_file)
                )
        else:
            ERR.close()

    def read_job_out_file(self, out_file=None, store_monitoring_events=True):
        """
        This function reads both stdout and stderr files and populates
        these fields in the Job class.
        """
        my_max_encoded_length = MAX_OUTPUT_LENGTH - 2000
        if self._output_file is None:
            # This is the case for SUBDAG jobs
            self._stdout_text = None

        if out_file is None:
            # PM-1297 only construct relative path if out_file is not explicitly specified
            run_dir = self._job_submit_dir

            # PM-1157 output file has absolute path from submit file
            # interferes with replay mode on another directory
            # basename = self._output_file

            basename = self._exec_job_id + ".out"
            if self._has_rotated_stdout_err_files:
                basename += ".%03d" % (self._job_output_counter)
            out_file = os.path.join(run_dir, basename)

        try:
            OUT = open(out_file)
            job_stdout = self.split_task_output(OUT.read())
            buf = job_stdout.user_data
            if len(buf) > my_max_encoded_length:
                buf = buf[:my_max_encoded_length]
            self._stdout_text = utils.quote("#@ 1 stdout\n" + buf)

            if store_monitoring_events:
                self._add_additional_monitoring_events(job_stdout.events)
        except OSError:
            self._stdout_text = None
            if not self.is_noop_job():
                logger.warning(
                    "unable to read output file: %s, continuing..." % (out_file)
                )
        else:
            OUT.close()

    def is_noop_job(self):
        """
        A convenience method to indicate whether a job is a NOOP job or not
        :return:  True if a noop job else False
        """
        if self._exec_job_id is not None and self._exec_job_id.startswith(
            NOOP_JOB_PREFIX
        ):
            return True

        return False

    def get_snippet_to_populate(
        self, task_output, task_number, current_buffer_size, type
    ):
        """

        :param task_output:
        :param task number:          the task number
        :param current_buffer_size:  the current size of the buffer that is storing job stdout for all tasks
        :param type:                 whether stdout or stderr for logging
        :return:
        """
        # PM-1152 we always attempt to store upto MAX_OUTPUT_LENGTH
        # 20 is the rough estimate of number of extra characters added by URL encoding
        remaining = MAX_OUTPUT_LENGTH - current_buffer_size - 20
        task_output_size = len(task_output)
        stdout = None
        if task_output_size <= remaining:
            # we can store the whole stdout
            stdout = task_output
        else:
            logger.debug(
                "Only grabbing %s of %s for task %s from file %s "
                % (remaining, type, task_number, self.get_rotated_out_filename())
            )
            if remaining > 0:
                # we store only the first remaining chars
                stdout = task_output[: -(task_output_size - remaining)]

        return stdout

    def split_task_output(self, task_output):
        """
        Splits task output in to user app data and monitoring events for pegasus use
        :param task_output:
        :param type:                 whether stdout or stderr for logging
        :return:
        """

        events = []
        start = 0
        end = 0

        # print task_output
        start = task_output.find(MONITORING_EVENT_START_MARKER, start)

        if start == -1:
            # no monitoring marker found
            return TaskOutput(task_output, events)

        task_data = StringIO()
        try:
            while start != -1:
                task_data.write(task_output[end:start])
                end = task_output.find(MONITORING_EVENT_END_MARKER, start)
                payload = task_output[start + len(MONITORING_EVENT_START_MARKER) : end]
                try:
                    events.append(json.loads(payload))
                except Exception:
                    logger.error("Unable to convert payload %s to JSON" % payload)
                start = task_output.find(MONITORING_EVENT_START_MARKER, end)

            task_data.write(task_output[end + len(MONITORING_EVENT_END_MARKER) :])
        except Exception as e:
            logger.error(
                "Unable to parse monitoring events from job stdout for job %s"
                % self._exec_job_id
            )
            logger.exception(e)
            # return the whole task output as is
            return TaskOutput(task_data.getvalue(), events)

        return TaskOutput(task_data.getvalue(), events)

    def create_composite_job_event(self, job_inst_kwargs):
        """
        this creates a composite job event that also includes all information included in a job_inst.end event

        :param my_job:
        :param job_inst_kwargs:
        :return:
        """

        kwargs = {}

        # add on events associated to populate the job_instance table
        kwargs.update(job_inst_kwargs)

        # count any integrity errors
        error_count = 0
        for metric in self._integrity_metrics:
            error_count += metric.failed

        kwargs["int_error_count"] = error_count

        if self._host_id:
            kwargs["hostname"] = self._host_id

        job_type = self._get_jobtype_desc()
        kwargs["jobtype"] = job_type

        if self._cpu_attribs:
            for key in self._cpu_attribs:
                kwargs[key] = self._cpu_attribs[key]

        # PM-1398 for DIBBS we want task monitoring event that has metadata
        # to be included in the composite event also
        if self._additional_monitoring_events:
            for event in self._additional_monitoring_events:
                event_name = (
                    event["monitoring_event"]
                    if "monitoring_event" in event
                    else "monitoring.additional"
                )
                if event_name == "metadata":
                    # flatten out metadata key values into event
                    """
                    # sample event we want to be able to parse
                    {
                     "ts": 1437688574,
                     "monitoring_event": "metadata",
                     "payload": [
                       {
                         "name": "num_template_banks",
                         "value" : 3
                       },
                       {
                         "name": "event_name",
                         "value" : "binary start merger"
                       }
                      ]
                    }

                    """
                    payload = event["payload"] if "payload" in event else None
                    if payload is None:
                        logger.error("No payload retrieved from event %s" % event)
                    for m in event["payload"]:
                        if "name" in m and "value" in m:
                            kwargs["metadata__" + m["name"]] = m["value"]
                        else:
                            logger.error(
                                "Additional monitoring event of type metadata can only have name value pairs in payload %s"
                                % event
                            )

        # sanity check
        if job_type == "unknown" or job_type == "unassigned":
            logger.warning(f"Job {self._exec_job_id} has unknown type {job_type}")

        # if error_count > 0:
        #  print kwargs

        for event in self._multipart_events:
            # if len(event) != 1:
            #    # should be a dictionary with exactly one key and value
            #    logger.error("Malformed multipart event %s for job %s" %(event,self._exec_job_id))
            #    continue
            for key in event.keys():
                if key == "multipart":
                    continue

                # GH-2123 the key may already exist (need to merge then). for example multiple transfer_attempts and
                # integrity_verification_attempts multipart records in the same job.out file
                if key in kwargs:
                    logger.debug(
                        "key %s exists already. Need to merge into existing composite event for job %s"
                        % (key, self._exec_job_id)
                    )
                    existing = kwargs[key]
                    for subevent in event[key]:
                        # print(subevent) only print for debugging. too verbose for debug too
                        existing.append(subevent)
                else:
                    kwargs[key] = event[key]

        return kwargs
