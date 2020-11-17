"""
This file implements the Workflow class for pegasus-monitord.
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

import logging
import os
import re
import socket
import sys
import time
import traceback

from Pegasus.monitoring.job import IntegrityMetric, Job
from Pegasus.tools import kickstart_parser, utils

logger = logging.getLogger(__name__)

# Optional imports, only generate 'warnings' if they fail
NLSimpleParser = None


try:
    from Pegasus.netlogger.parsers.base import NLSimpleParser
except Exception:
    logger.info("cannot import NL parser")
    print(traceback.format_exc())

# Compile our regular expressions

# Used while reading the DAG file
re_parse_dag_submit_files = re.compile(r"JOB\s+(\S+)\s(\S+)(\s+DONE)?", re.IGNORECASE)
re_parse_pmc_submit_files = re.compile(r"TASK\s+(\S*)\s(\S+)", re.IGNORECASE)
re_parse_dag_script = re.compile(
    r"SCRIPT (?:PRE|POST)\s+(\S+)\s(\S+)\s(.*)", re.IGNORECASE
)
re_parse_dag_subdag = re.compile(
    r"SUBDAG EXTERNAL\s+(\S+)\s(\S+)\s?(?:DIR)?\s?(\S+)?", re.IGNORECASE
)
re_parse_planner_args = re.compile(r"\s*-Dpegasus.log.\*=(\S+)\s.*", re.IGNORECASE)
# used while parsing the job .err file.
re_parse_pegasuslite_ec = re.compile(r"^PegasusLite: exitcode (\d+)$", re.MULTILINE)
# re_parse_register_input_files = re.compile(r'^([a-zA-z\.\d\\_-]+)\s+([\w]+://[\w\.\-:@]*/[\S ]*)\s+(\w=\".*\")*')


# Constants
MONITORD_START_FILE = "monitord.started"  # filename for writing when monitord starts
MONITORD_DONE_FILE = "monitord.done"  # filename for writing when monitord finishes
MONITORD_STATE_FILE = "monitord.info"  # filename for writing monitord state information
MONITORD_RECOVER_FILE = (
    "monitord.recover"  # filename for writing monitord recovery information
)
PRESCRIPT_TASK_ID = -1  # id for prescript tasks
POSTSCRIPT_TASK_ID = -2  # id for postscript tasks
MAX_OUTPUT_LENGTH = (
    2 ** 16 - 1
)  # in bytes, maximum we can put into the database for job's stdout and stderr
UNKNOWN_FAILURE_CODE = 2  # unknown failure code when inserting an END event betweeen consecutive workflow start events

# Other variables
condor_dagman_executable = None  # condor_dagman binary location

# Find condor_dagman
condor_dagman_executable = utils.find_exec("condor_dagman")
if condor_dagman_executable is None:
    # Default value
    condor_dagman_executable = "condor_dagman"


class Workflow:
    """
    Class used to keep everything needed to track a particular workflow
    """

    # Class variables, used to send link parent jobs to sub workflows
    wf_list = {}

    @staticmethod
    def get_numeric_version(major, minor, patch):
        """

        :param major:
        :param minor:
        :param patch:
        :return: int version
        """

        version = "%02d%02d%02d" % (major, minor, patch)
        return int(version)

    # class level variable constant
    CONDOR_VERSION_8_3_3 = int("080303")
    CONDOR_VERSION_8_2_8 = int(
        "080208"
    )  # last stable release that did not report held job reasons

    def output_to_db(self, event, kwargs):
        """
        This function sends an NetLogger event to the loader class.
        """

        # Sanity check (should also be done elsewhere, but repeated here)
        if self._sink is None:
            return

        # Don't output anything if we have disabled events to the database
        if self._database_disabled is True:
            return

        #        # PM-1355 add on the fixed attributes
        if event != "xwf.map.subwf_job":
            # we can add fixed attributes for all events other than
            # subworklow mapping event, as for that event the xwf__id
            # in the event is from the root/parent workflow and does not match
            # the one in the fixed addon attributes which has the
            # sub workflow xwf__id
            kwargs.update(self._fixed_addon_attrs)

        try:
            # Send event to corresponding sink
            logger.trace("Sending record to DB %s, %s", event, kwargs)
            self._sink.send(event, kwargs)
        except Exception as e:
            # Error sending this event... disable the sink from now on...
            logger.warning(
                "error sending event for %s: %s, %s", self._wf_uuid, event, kwargs
            )
            logger.exception(e)
            logger.error(
                "Disabling database population for workflow %s in directory %s",
                self._wf_uuid,
                self._submit_dir or "unknown",
            )
            self._database_disabled = True

    def output_to_dashboard_db(self, event, kwargs):
        """
        This function sends an NetLogger event to the loader class.
        """

        # Sanity check (should also be done elsewhere, but repeated here)
        if self._dashboard_sink is None:
            return

        # Don't output anything if we have disabled events to the database
        if self._database_disabled is True:
            return

        try:
            # Send event to corresponding sink
            self._dashboard_sink.send(event, kwargs)
        except Exception:
            # Error sending this event... disable the sink from now on...
            logger.warning(
                "DASHBOARD DB NL-LOAD-ERROR --> %s - %s"
                % (
                    self._wf_uuid,
                    (
                        (self._dax_label or "unknown")
                        + "-"
                        + (self._dax_index or "unknown")
                    ),
                )
            )
            logger.warning(
                "error sending event to dashboard db: {} --> {}".format(event, kwargs)
            )
            logger.warning(traceback.format_exc())
            self._database_disabled = True

    def parse_dag_file(self, dag_file):
        """
        This function parses the DAG file and determines submit file locations
        """

        # If we already have jobs in our _job_info dictionary, skip reading the dag file
        if len(self._job_info) > 0:
            logger.warning(
                "skipping parsing the dag file, already have job info loaded..."
            )
            return

        dag_file = os.path.join(self._run_dir, dag_file)

        try:
            DAG = open(dag_file)
        except Exception:
            logger.warning("unable to read %s!" % (dag_file))
        else:
            logger.info("Parsing DAG file %s" % dag_file)
            for dag_line in DAG:
                lc_dag_line = dag_line.lower().lstrip()
                if lc_dag_line.startswith("job"):
                    # Found Job line, parse it
                    my_match = re_parse_dag_submit_files.search(dag_line)
                    if my_match:
                        if not my_match.group(3):
                            my_jobid = my_match.group(1)
                            my_sub = os.path.join(self._run_dir, my_match.group(2))
                            # Found submit file for not-DONE job
                            if my_jobid in self._job_info:
                                # Entry already exists for this job, just collect submit file info
                                self._job_info[my_jobid][0] = my_sub
                            else:
                                # No entry for this job, let's create a new one
                                self._job_info[my_jobid] = [
                                    my_sub,
                                    None,
                                    None,
                                    None,
                                    None,
                                    False,
                                    None,
                                    None,
                                    None,
                                ]
                elif lc_dag_line.startswith("task"):
                    # This is a PMC DAG entry
                    my_match = re_parse_pmc_submit_files.search(dag_line)
                    if my_match:
                        my_jobid = my_match.group(1)
                        # In the PMC case there is no submit script
                        if my_jobid in self._job_info:
                            self._job_info[my_jobid][0] = None
                        else:
                            self._job_info[my_jobid] = [
                                None,
                                None,
                                None,
                                None,
                                None,
                                False,
                                None,
                                None,
                                None,
                            ]
                        # PM-793 PMC only case always have rotated stdout and stderr
                        self._is_pmc_dag = True
                elif lc_dag_line.startswith("script post"):
                    # Found SCRIPT POST line, parse it
                    my_match = re_parse_dag_script.search(dag_line)
                    if my_match:
                        my_jobid = my_match.group(1)
                        my_exec = my_match.group(2)
                        my_args = my_match.group(3)
                        if my_jobid in self._job_info:
                            # Entry already exists for this job, just collect post script info
                            self._job_info[my_jobid][3] = my_exec
                            self._job_info[my_jobid][4] = my_args
                        else:
                            # No entry for this job, let's create a new one
                            self._job_info[my_jobid] = [
                                None,
                                None,
                                None,
                                my_exec,
                                my_args,
                                False,
                                None,
                                None,
                                None,
                            ]
                elif lc_dag_line.startswith("script pre"):
                    # Found SCRIPT PRE line, parse it
                    my_match = re_parse_dag_script.search(dag_line)
                    if my_match:
                        my_jobid = my_match.group(1)
                        my_exec = my_match.group(2)
                        my_args = my_match.group(3)

                        my_pegasus_pre_log = None
                        if my_args is not None:
                            # try and determine the pegasus plan pre log from the arguments
                            my_args_match = re_parse_planner_args.search(my_args)
                            if my_args_match:
                                my_pegasus_pre_log = my_args_match.group(1)

                        if my_jobid in self._job_info:
                            # Entry already exists for this job, just collect pre script info
                            self._job_info[my_jobid][1] = my_exec
                            self._job_info[my_jobid][2] = my_args
                            self._job_info[my_jobid][8] = my_pegasus_pre_log
                        else:
                            # No entry for this job, let's create a new one
                            self._job_info[my_jobid] = [
                                None,
                                my_exec,
                                my_args,
                                None,
                                None,
                                False,
                                None,
                                None,
                                my_pegasus_pre_log,
                            ]
                elif lc_dag_line.startswith("subdag external"):
                    # Found SUBDAG line, parse it
                    my_match = re_parse_dag_subdag.search(dag_line)
                    if my_match:
                        my_jobid = my_match.group(1)
                        my_dag = my_match.group(2)
                        my_dir = my_match.group(3)
                        if my_dir is None:
                            # SUBDAG EXTERNAL line without DIR, let's get it from the DAG path
                            if my_dag is not None:
                                my_dir = os.path.dirname(my_dag)
                        if my_jobid in self._job_info:
                            # Entry already exists for this job, just set subdag flag, and dag/dir info
                            self._job_info[my_jobid][5] = True
                            self._job_info[my_jobid][6] = my_dag
                            self._job_info[my_jobid][7] = my_dir
                        else:
                            # No entry for this job, let's create a new one
                            self._job_info[my_jobid] = [
                                None,
                                None,
                                None,
                                None,
                                None,
                                True,
                                my_dag,
                                my_dir,
                                None,
                            ]

            try:
                DAG.close()
            except Exception:
                pass

        # POST-CONDITION: _job_info contains only submit-files of jobs
        # that are not yet done. Normally, this are all submit
        # files. In rescue DAGS, that is an arbitrary subset of all
        # jobs. In addition, _job_info should contain all PRE and POST
        # script information for job in this workflow, and all subdag
        # jobs, with the their dag files, and directories

    def job_has_postscript(self, jobid):
        # This function returns whether a job matching a jobid in the workflow
        # has a postscript associated with or not
        return self._job_info[jobid][3] is not None

    def parse_in_file(self, job, tasks):
        """
        This function parses the in file for a given job, reading the
        task information and adding to the dictionary of tasks. It
        returns True if parsing was successful, or None, if an error
        was found.
        """
        jobname = job._exec_job_id
        in_file = os.path.join(job._job_submit_dir, jobname) + ".in"

        try:
            IN = open(in_file)
        except Exception:
            logger.warning("unable to read %s!" % (in_file))
            return None

        tasks_found = 0
        for line in IN:
            line = line.strip()
            if len(line) == 0:
                continue
            if line.startswith("#@"):
                line = line[2:]
                line = line.strip()
                try:
                    my_task_id, my_transformation, my_derivation = line.split(None, 2)
                    my_task_id = int(my_task_id)
                except Exception:
                    # Doesn't look line a proper comment line with embedded info, skipping...
                    continue
                # Update information in dictionary
                try:
                    my_task_info = tasks[my_task_id]
                except Exception:
                    logger.warning(
                        "cannot locate task %d in dictionary... skipping this task for job: %s, dag file: %s"
                        % (
                            my_task_id,
                            jobname,
                            os.path.join(self._run_dir, self._dag_file_name),
                        )
                    )
                    continue
                my_task_info["transformation"] = my_transformation
                my_task_info["derivation"] = my_derivation
            elif line.startswith("#") or line.startswith("EDGE"):
                # Regular comment line... just skip it
                # Or it can be the EDGES descriped in input file for pegasus-mpi-cluster
                continue
            else:
                # This is regular line, so we assume it is a task
                split_line = line.split(None, 1)
                if len(split_line) == 0:
                    # Nothing here
                    continue
                my_executable = split_line[0]
                if len(split_line) == 2:
                    my_argv = split_line[1]
                else:
                    my_argv = None
                # Increment the task_found counter, so that we have the correct task index
                tasks_found = tasks_found + 1
                try:
                    my_task_info = tasks[tasks_found]
                except Exception:
                    logger.warning(
                        "cannot locate task %d in dictionary... skipping this task for job: %s, dag file: %s"
                        % (
                            my_task_id,
                            jobname,
                            os.path.join(self._run_dir, self._dag_file_name),
                        )
                    )
                    continue
                my_task_info["argument-vector"] = my_argv
                my_task_info["name"] = my_executable

        try:
            IN.close()
        except Exception:
            pass

        return True

    def read_workflow_state(self):
        """
        This function reads the job_submit_seq and the job_counters
        dictionary from a file in the workflow's run directory. This
        is used for restarting the logging information from where we
        stopped last time.
        """

        if self._output_dir is None:
            my_fn = os.path.join(self._run_dir, MONITORD_STATE_FILE)
        else:
            my_fn = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_STATE_FILE)
            )

        try:
            INPUT = open(my_fn)
        except Exception:
            logger.info(
                "cannot open state file %s, continuing without state..." % (my_fn)
            )
            return

        try:
            for line in INPUT:
                # Split the input line in 2, and make the second part an integer
                my_job, my_count = line.split(" ", 1)
                my_job = my_job.strip()
                my_count = int(my_count.strip())
                if my_job == "monitord_job_sequence":
                    # This is the last job_submit_seq used
                    self._job_submit_seq = my_count
                elif my_job == "monitord_dagman_out_sequence":
                    # This is the line we last read from the dagman.out file
                    self._last_processed_line = my_count
                elif my_job == "monitord_workflow_restart_count":
                    # This is the number of restarts we have seen in the past
                    self._restart_count = my_count
                else:
                    # Another job counter
                    self._job_counters[my_job] = my_count
        except Exception:
            logger.error("error processing state file %s" % (my_fn))

        # Close the file
        try:
            INPUT.close()
        except Exception:
            pass

        # All done!
        return

    def write_workflow_state(self):
        """
        This function writes the job_submit_seq and the job_counters
        dictionary to a file in the workflow's run directory. This can
        be used later for restarting the logging information from
        where we stop. This function will overwrite the log file every
        time is it called.
        """

        if self._output_dir is None:
            my_fn = os.path.join(self._run_dir, MONITORD_STATE_FILE)
        else:
            my_fn = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_STATE_FILE)
            )

        try:
            OUT = open(my_fn, "w")
        except Exception:
            logger.error("cannot open state file %s" % (my_fn))
            return

        try:
            # Write first line with the last job_submit_seq used
            OUT.write("monitord_job_sequence %d\n" % (self._job_submit_seq))
            # Then, write the last line number of the dagman.out file we processed
            if self._line > self._last_processed_line:
                OUT.write("monitord_dagman_out_sequence %s\n" % (self._line))
            else:
                OUT.write(
                    "monitord_dagman_out_sequence %s\n" % (self._last_processed_line)
                )
            # Next, write the restart count
            OUT.write("monitord_workflow_restart_count %d\n" % (self._restart_count))
            # Finally, write all job_counters
            for my_job in self._job_counters:
                OUT.write("%s %d\n" % (my_job, self._job_counters[my_job]))
        except Exception:
            logger.error("cannot write state to log file %s" % (my_fn))

        # Close the file
        try:
            OUT.close()
        except Exception:
            pass

        # All done!
        return

    def read_workflow_progress(self):
        """
        This function reads the workflow progress from a previous
        instance of the monitoring daemon, and keeps track of the last
        time that was processed by pegasus-monitord.
        """
        if self._output_dir is None:
            my_recover_file = os.path.join(self._run_dir, MONITORD_RECOVER_FILE)
        else:
            my_recover_file = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_RECOVER_FILE)
            )

        if os.access(my_recover_file, os.F_OK):
            try:
                RECOVER = open(my_recover_file)
                for line in RECOVER:
                    line = line.strip()
                    my_key, my_value = line.split(" ", 1)
                    if my_key == "line_processed":
                        self._previous_processed_line = int(my_value.strip())
                        logger.info(
                            "monitord last processed line: %d"
                            % (self._previous_processed_line)
                        )
                        break
                RECOVER.close()
            except Exception:
                logger.info(
                    "couldn't open/parse recover file information: %s"
                    % (my_recover_file)
                )

    def write_workflow_progress(self):
        """
        This function writes the workflow progress so that a future
        instance of the monitoring daemon can figure out where we were
        in case of failure.
        """
        # Nothing to do if we still haven't caught up with the last instance's progress...
        if self._line < self._previous_processed_line:
            return

        if self._output_dir is None:
            my_recover_file = os.path.join(self._run_dir, MONITORD_RECOVER_FILE)
        else:
            my_recover_file = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_RECOVER_FILE)
            )
        try:
            RECOVER = open(my_recover_file, "w")
        except Exception:
            logger.error("cannot open recover file: %s" % (my_recover_file))
            return

        try:
            # Write line with information about where we are in the dagman.out file
            RECOVER.write("line_processed %s\n" % (self._line))
        except Exception:
            logger.error(
                "cannot write recover information to file: %s" % (my_recover_file)
            )

        # Close the file
        try:
            RECOVER.close()
        except Exception:
            pass

        return

    def db_send_wf_info(self):
        """
        This function sends to the DB information about the workflow
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}
        # Make sure we include the wf_uuid
        kwargs["xwf__id"] = self._wf_uuid
        # Now include others, if they are defined
        if self._dax_label is not None:
            kwargs["dax__label"] = self._dax_label
        if self._dax_version is not None:
            kwargs["dax__version"] = self._dax_version
        if self._dax_index is not None:
            kwargs["dax__index"] = self._dax_index
        if self._dax_file is not None:
            kwargs["dax__file"] = self._dax_file
        if self._dag_file_name is not None:
            kwargs["dag__file__name"] = self._dag_file_name
        if self._timestamp is not None:
            kwargs["ts"] = self._timestamp
        if self._submit_hostname is not None:
            kwargs["submit__hostname"] = self._submit_hostname
        if self._submit_dir is not None:
            kwargs["submit__dir"] = self._submit_dir
        if self._planner_arguments is not None:
            kwargs["argv"] = self._planner_arguments.strip('" \t\n\r')
        if self._user is not None:
            kwargs["user"] = self._user
        if self._grid_dn is not None:
            if self._grid_dn != "null":
                # Only add it if it is not "null"
                kwargs["grid_dn"] = self._grid_dn
        if self._planner_version is not None:
            kwargs["planner__version"] = self._planner_version
        if self._parent_workflow_id is not None:
            kwargs["parent__xwf__id"] = self._parent_workflow_id
        if self._root_workflow_id is not None:
            kwargs["root__xwf__id"] = self._root_workflow_id

        # Send workflow event to database
        self.output_to_db("wf.plan", kwargs)

        # send the workflow event to dashboard database
        if self._dashboard_sink is None:
            return
        """
        dashboard_args = {}
        dashboard_args["xwf__id"] = self._wf_uuid
        if self._dashboard_database_url is not None:
            dashboard_args["db__url"] = self._dashboard_database_url

        #add the keys we want

        if self._dax_label is not None:
            dashboard_args["dax__label"] = self._dax_label

        if self._timestamp is not None:
            dashboard_args["ts"] = self._timestamp
        if self._submit_hostname is not None:
            dashboard_args["submit__hostname"] = self._submit_hostname
        if self._submit_dir is not None:
            dashboard_args["submit__dir"] = self._submit_dir
        """
        # to the dashboard db we had the connection details
        # rest remains the same
        if self._database_url is not None:
            kwargs["db_url"] = self._database_url

        self.output_to_dashboard_db("wf.plan", kwargs)

    def db_send_subwf_link(
        self, wf_uuid, parent_workflow_id, parent_jobid, parent_jobseq
    ):
        """
        This function sends to the DB the information linking a subwf
        to its parent job. Hack: Note that in most cases wf_uuid and
        parent_workflow_id would be instance variables, but there is
        also the case where these variables are cached in the Workflow
        class from a previous instance (that is why they are
        explicitly passed into this function).
        """
        # Check if database is configured
        if self._sink is None:
            return

        # And if we have all needed parameters
        if (
            wf_uuid is None
            or parent_workflow_id is None
            or parent_jobid is None
            or parent_jobseq is None
        ):
            return

        # Start empty
        kwargs = {}
        # Make sure we include the wf_uuid, but note that in this
        # particular event, the xwf.id key refers to the parent
        # workflow, while the subwf.id key refers to this workflow
        kwargs["xwf__id"] = parent_workflow_id
        if self._timestamp is not None:
            kwargs["ts"] = self._timestamp

        kwargs["subwf__id"] = wf_uuid
        kwargs["job__id"] = parent_jobid
        kwargs["job_inst__id"] = parent_jobseq

        # Send sub-workflow event to database
        self.output_to_db("xwf.map.subwf_job", kwargs)

    def db_send_wf_state(self, state, timestamp=None, reason=None):
        """
        This function sends to the DB information about the current
        workflow state to both the stampede database and dashboard database
        """
        # Check if database is configured
        if self._sink is None:
            return
        # Make sure parameters are not None
        if state is None:
            return

        if timestamp is None:
            timestamp = self._current_timestamp

        # Start empty
        kwargs = {}
        # Make sure we include the wf_uuid
        kwargs["xwf__id"] = self._wf_uuid
        kwargs["ts"] = timestamp
        kwargs["reason"] = reason
        # Always decrement the restart count by 1
        kwargs["restart_count"] = self._restart_count - 1
        if state == "end":
            # Add status field for workflow.end event
            kwargs["status"] = self._dagman_exit_code
            if self._dagman_exit_code != 0:
                # Set level to Error if workflow did not finish successfully
                kwargs["level"] = "Error"
            if self._dagman_exit_code is None:
                logger.warning(
                    "%s - %s - %s - %s: DAGMan exit code hasn't been set..."
                    % (
                        self._wf_uuid,
                        (
                            (self._dax_label or "unknown")
                            + "-"
                            + (self._dax_index or "unknown")
                        ),
                        self._line,
                        self._out_file,
                    )
                )
                kwargs["status"] = 0
        state = "xwf." + state

        # Send workflow state event to stampede database
        self.output_to_db(state, kwargs)
        self.output_to_dashboard_db(state, kwargs)

    def change_wf_state(self, state):
        """
        This function changes the workflow state, and sends the state
        change to the DB. This function is called as response to
        DAGMan starting/stopping.
        """

        if self._last_known_state is not None:
            # sanity check
            if state == "start" and self._last_known_state == state:
                # we have two consecutive start events from DAGMAN log
                # can happen in case of power failure or condor crashing.
                # we insert a DAGMAN FINISHED event PM-723
                # PM-1062 subtract 1 second from the timestamp
                prev_wf_end_timestamp = self._current_timestamp - 1
                logger.warning(
                    "Consecutive workflow START events detected for workflow with condor id %s running in directory %s ."
                    % (self._dagman_condor_id, self._submit_dir)
                    + " Inserting Workflow END event with timestamp %s"
                    % (prev_wf_end_timestamp)
                )
                self._dagman_exit_code = UNKNOWN_FAILURE_CODE
                self.write_to_jobstate(
                    "%d INTERNAL *** DAGMAN_FINISHED %s ***\n"
                    % (prev_wf_end_timestamp, self._dagman_exit_code)
                )
                self.db_send_wf_state("end", prev_wf_end_timestamp)
                # PM-1217 reset exitcode to None as we don't want monitord to stop monitoring this workflow
                self._dagman_exit_code = None

        if state == "start":
            logger.info("DAGMan starting with condor id %s" % (self._dagman_condor_id))
            self.write_to_jobstate(
                "%d INTERNAL *** DAGMAN_STARTED %s ***\n"
                % (self._current_timestamp, self._dagman_condor_id)
            )
            self._restart_count = self._restart_count + 1
        elif state == "end":
            self.write_to_jobstate(
                "%d INTERNAL *** DAGMAN_FINISHED %s ***\n"
                % (self._current_timestamp, self._dagman_exit_code)
            )

        # Take care of workflow-level notifications
        if (
            self.check_notifications() is True
            and self._notifications_manager is not None
        ):
            self._notifications_manager.process_workflow_notifications(self, state)

        self.db_send_wf_state(state, reason=self._current_state_reason)
        self._last_known_state = state
        self._current_state_reason = (
            None  # PM-1121 reset state reason after we have pushed to db
        )

    def start_wf(self):
        """
        This function initializes basic parameters in the Workflow class. It should
        be called every time DAGMAN starts so that we can wipe out any old state
        in case of restarts.
        """
        # We only wipe state about jobs that have completed
        logger.debug("DAGMan restarted, cleaning up old job information...")
        # Keep list of jobs whose information we want to delete
        jobs_to_delete = []

        # Compile list of jobs whose information we don't need anymore...
        for (my_jobid, my_job_submit_seq) in self._jobs:
            my_job = self._jobs[my_jobid, my_job_submit_seq]
            my_job_state = my_job._job_state
            if my_job_state == "POST_SCRIPT_SUCCESS":
                # This job is done
                jobs_to_delete.append((my_jobid, my_job_submit_seq))
            elif my_job_state == "JOB_SUCCESS":
                if my_jobid in self._job_info and self._job_info[my_jobid][3] is None:
                    # No postscript for this job
                    jobs_to_delete.append((my_jobid, my_job_submit_seq))
                else:
                    logger.debug("keeping job %s..." % (my_jobid))
            else:
                logger.debug("keeping job %s..." % (my_jobid))

        # Delete jobs...
        for (my_jobid, my_job_submit_seq) in jobs_to_delete:
            if my_jobid in self._walltime:
                del self._walltime[my_jobid]
            if my_jobid in self._job_site:
                del self._job_site[my_jobid]
            if my_jobid in self._jobs_map:
                del self._jobs_map[my_jobid]
            if (my_jobid, my_job_submit_seq) in self._jobs:
                del self._jobs[(my_jobid, my_job_submit_seq)]

        # Done!
        return

    def check_notifications(self):
        """
        This function returns True if we need to check notifications, or False
        if we should skip notification checking.
        """
        # Skip, if notificatications for this workflow are disabled
        if not self._enable_notifications:
            return False

        if self._line < self._previous_processed_line:
            # Recovery mode, skip notification that we already did.
            logger.debug(
                "Recovery mode: skipping notification already issued... line %s"
                % (self._line)
            )
            return False

        return True

    def init_clean(self):
        """
        Remove monitord.done file if it already exists.
        """
        if os.path.isfile(os.path.join(self._run_dir, MONITORD_DONE_FILE)):
            try:
                os.remove(os.path.join(self._run_dir, MONITORD_DONE_FILE))
            except BaseException:
                pass

    def __init__(
        self,
        rundir,
        outfile,
        database=None,
        database_url=None,
        dashboard_database=None,
        workflow_config_file=None,
        jsd=None,
        root_id=None,
        parent_id=None,
        parent_jobid=None,
        parent_jobseq=None,
        enable_notifications=True,
        replay_mode=False,
        store_stdout_stderr=True,
        output_dir=None,
        notifications_manager=None,
    ):
        """
        This function initializes the workflow object. It looks for
        the workflow configuration file (or for workflow_config_file,
        if specified). Here we also open the jobstate.log file, and
        parse the dag.
        """
        # Initialize class variables from creator parameters
        self._out_file = outfile
        self._run_dir = rundir
        self._parent_workflow_id = parent_id
        self._root_workflow_id = root_id
        self._sink = database
        self._dashboard_sink = dashboard_database
        self._database_url = database_url
        self._database_disabled = False
        self._workflow_start = int(time.time())
        self._enable_notifications = enable_notifications
        self._replay_mode = replay_mode
        self._notifications_manager = notifications_manager
        self._output_dir = output_dir
        self._store_stdout_stderr = store_stdout_stderr
        # self._last_known_state = last_known_state  #last known state of the workflow. updated whenever change_wf_state is called

        # Initialize other class variables
        self._wf_uuid = None
        self._dag_file_name = None
        self._static_bp_file = None
        self._dax_label = None
        self._dax_version = None
        self._dax_file = None
        self._dax_index = None
        self._timestamp = None
        self._submit_hostname = None
        self._submit_dir = None  # submit dir from braindump file (run dir, if submit_dir key is not found)
        self._original_submit_dir = None  # submit dir from braindump file (jsd dir, if submit_dir key is not found)
        self._planner_arguments = None
        self._user = None
        self._grid_dn = None
        self._planner_version = None
        self._dagman_version = None  # dagman version as integer
        self._last_submitted_job = None
        self._jobs_map = {}
        self._jobs = {}
        self._job_submit_seq = 1
        self._log_file = None  # monitord.log file
        self._jsd_file = None  # jobstate.log file
        self._notify_file = None  # notification file
        self._notifications = None  # list of notifications for this workflow
        self._JSDB = None  # Handle for jobstate.log file
        self._job_counters = (
            {}
        )  # Job counters for figuring out which output file to parse
        self._job_info = (
            {}
        )  # jobid --> [sub_file, pre_exec, pre_args, post_exec, post_args, is_subdag, subdag_dag, subdag_dir, prescript_log]
        self._valid_braindb = (
            True  # Flag for creating a new brain db if we don't find one
        )
        self._line = 0  # line number from dagman.out file
        self._last_processed_line = 0  # line last processed by the monitoring daemon
        self._previous_processed_line = (
            0  # line last processed by a previous instance of monitord
        )
        self._restart_count = (
            0  # Keep track of how many times the workflow was restarted
        )
        self._skipping_recovery_lines = (
            False  # Flag for skipping the repeat duplicate messages generated by DAGMan
        )
        self._dagman_condor_id = None  # Condor id of the current DAGMan
        self._dagman_pid = 0  # Condor DAGMan's PID
        self._current_timestamp = 0  # Last timestamp from DAGMan
        self._dagman_exit_code = None  # Keep track of when to finish this workflow
        self._monitord_exit_code = 0  # Keep track of errors inside monitord
        self._finished = False  # keep track so we don't finish multiple times
        self._condorlog = None  # Condor common logfile
        self._multiline_file_flag = (
            False  # Track multiline user log files, DAGMan > 6.6
        )
        self._walltime = {}  # jid --> walltime
        self._job_site = {}  # last site a job was planned for
        self._last_known_state = None  # last known state of the workflow. updated whenever change_wf_state is called
        self._current_state_reason = (
            None  # the reason if any for the current known state of the workflow
        )
        self._last_known_job = (
            None  # last know job, used for tracking job held reason PM-749
        )
        self._is_pmc_dag = False  # boolean to track whether monitord is parsing a PMC DAG i.e pmc-only mode of Pegasus
        self._fixed_addon_attrs = {}  # sent with all events related to this workflow
        self.init_clean()

        # Parse the braindump file
        wfparams = utils.slurp_braindb(rundir, workflow_config_file)

        if len(wfparams) == 0:
            # Set flag for creating a braindb file if nothing was read
            self._valid_braindb = False

        # Go through wfparams, and read what we need
        if "wf_uuid" in wfparams:
            if wfparams["wf_uuid"] is not None:
                self._wf_uuid = wfparams["wf_uuid"]
                self._fixed_addon_attrs["xwf__id"] = self._wf_uuid
        else:
            logger.error(
                "wf_uuid not specified in braindump, skipping this (sub-)workflow. %s %s "
                % (rundir, workflow_config_file)
            )
            self._monitord_exit_code = 1
            return
        # Now that we have the wf_uuid, set root_wf_uuid if not already set
        if self._root_workflow_id is None:
            self._root_workflow_id = self._wf_uuid

        self._fixed_addon_attrs["root__xwf__id"] = self._root_workflow_id

        if "dax_label" in wfparams:
            self._dax_label = wfparams["dax_label"]
        else:
            # Use "label" if "dax_label" not found
            if "label" in wfparams:
                self._dax_label = wfparams["label"]
        if "dax_index" in wfparams:
            self._dax_index = wfparams["dax_index"]
        if "dax_version" in wfparams:
            self._dax_version = wfparams["dax_version"]
        if "dax" in wfparams:
            self._dax_file = wfparams["dax"]
            self._fixed_addon_attrs["dax"] = self._dax_file
        if "dag" in wfparams:
            self._dag_file_name = wfparams["dag"]
            self._fixed_addon_attrs["dag"] = self._dag_file_name
        else:
            logger.error(
                "dag not specified in braindump, skipping this (sub-)workflow..."
            )
            self._monitord_exit_code = 1
            return
        if "timestamp" in wfparams:
            self._timestamp = wfparams["timestamp"]
        else:
            # Use "pegasus_wf_time" if "timestamp" not found
            if "pegasus_wf_time" in wfparams:
                self._timestamp = wfparams["pegasus_wf_time"]
        # Convert timestamp from YYYYMMDDTHHMMSSZZZZZ to Epoch
        if self._timestamp is not None:
            # Convert timestamp to epoch
            wf_timestamp = utils.epochdate(self._timestamp)
            if wf_timestamp is not None:
                self._timestamp = wf_timestamp
            else:
                # Couldn't do it, let's just use the current time
                self._timestamp = int(time.time())
        else:
            # No timestamp information is available, just use current time
            self._timestamp = int(time.time())

        self._fixed_addon_attrs["wf__ts"] = self._timestamp

        if "submit_dir" in wfparams:
            self._submit_dir = wfparams["submit_dir"]
            self._original_submit_dir = os.path.normpath(wfparams["submit_dir"])
        else:
            # Use "run" if "submit_dir" not found
            if "run" in wfparams:
                self._submit_dir = wfparams["run"]
            # Use "jsd" if "submit_dir" is not found
            if "jsd" in wfparams:
                self._original_submit_dir = os.path.dirname(
                    os.path.normpath(wfparams["jsd"])
                )

        self._fixed_addon_attrs["submit__dir"] = self._submit_dir

        if "planner_version" in wfparams:
            self._planner_version = wfparams["planner_version"]
        else:
            # Use "pegasus_version" if "planner_version" not found
            if "pegasus_version" in wfparams:
                self._planner_version = wfparams["pegasus_version"]
        self._fixed_addon_attrs["pegasus__version"] = self._planner_version

        if "planner_arguments" in wfparams:
            self._planner_arguments = wfparams["planner_arguments"]
        if "submit_hostname" in wfparams:
            self._submit_hostname = wfparams["submit_hostname"]
            self._fixed_addon_attrs["submit__hostname"] = self._submit_hostname
        if "user" in wfparams:
            self._user = wfparams["user"]
            # make it clear it is the workflow user.
            # jobs can run under some other user
            self._fixed_addon_attrs["wf__user"] = self._user
        if "grid_dn" in wfparams:
            self._grid_dn = wfparams["grid_dn"]

        if not self._replay_mode:
            # Recover state from a previous run
            self.read_workflow_progress()
            if self._previous_processed_line != 0:
                # Recovery mode detected, reset last_processed_line so
                # that we start from the beginning of the dagman.out
                # file...
                logger.info(
                    "Setting last processed line to 0 in recovery mode to ensure population starts afresh"
                )
                self._last_processed_line = 0
            else:
                # PM-1209 we only read workflow state when we know it is not the monitord recovery mode
                self.read_workflow_state()

        # Determine location of jobstate.log file
        my_jsd = jsd or utils.jobbase

        if self._output_dir is None:
            # Make sure we have an absolute path
            self._jsd_file = os.path.join(rundir, my_jsd)
        else:
            self._jsd_file = os.path.join(
                rundir, self._output_dir, "{}-{}".format(self._wf_uuid, my_jsd)
            )

        if not os.path.isfile(self._jsd_file):
            logger.info("creating new file %s" % (self._jsd_file))

        try:
            # Create new file, or append to an existing one
            if not self._replay_mode and self._previous_processed_line == 0:
                # Append to current one if not in replay mode and not
                # in recovering from previous errors
                # this is for rescue dags and when a workflow is run for the first time
                logger.info(
                    "Appending to existing jobstate.log replay_mode %s previous_processed_line %s"
                    % (self._replay_mode, self._previous_processed_line)
                )
                self._JSDB = open(self._jsd_file, "ab", 0)
            else:
                # Rotate jobstate.log file, if any in case of replay
                # mode of if we are starting from the beginning
                # because of a previous failure
                # or the recover mode
                utils.rotate_log_file(self._jsd_file)
                logger.info(
                    " Rotating jobstate.log replay_mode %s previous_processed_line %s"
                    % (self._replay_mode, self._previous_processed_line)
                )
                self._JSDB = open(self._jsd_file, "wb", 0)
        except Exception:
            logger.critical("error creating/appending to %s!" % (self._jsd_file))
            self._monitord_exit_code = 1
            print(traceback.format_exc())
            return

        # Skip notifications, if disabled
        if self._enable_notifications and self._notifications_manager is not None:
            if "notify" in wfparams:
                self._notify_file = wfparams["notify"]
                # Add rundir to notifications filename
                if self._run_dir is not None:
                    self._notify_file = os.path.join(self._run_dir, self._notify_file)
                # Read notification file
                if (
                    self._notifications_manager.read_notification_file(
                        self._notify_file, self._wf_uuid
                    )
                    == 0
                ):
                    # Disable notifications, if this workflow doesn't include any...
                    self._enable_notifications = False

        # Say hello.... add start information to JSDB
        self.write_to_jobstate(
            "%d INTERNAL *** MONITORD_STARTED ***\n" % (self._workflow_start)
        )

        # Write monitord.started file
        if self._output_dir is None:
            my_start_file = os.path.join(self._run_dir, MONITORD_START_FILE)
        else:
            my_start_file = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_START_FILE)
            )

        my_now = int(time.time())
        utils.write_pid_file(my_start_file, my_now)

        # Remove monitord.done file, if it is there
        if self._output_dir is None:
            my_touch_name = os.path.join(self._run_dir, MONITORD_DONE_FILE)
        else:
            my_touch_name = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_DONE_FILE)
            )

        try:
            os.unlink(my_touch_file)
        except Exception:
            pass

        # Add this workflow to Workflow's class master list
        if not rundir in Workflow.wf_list:
            Workflow.wf_list[rundir] = {
                "wf_uuid": self._wf_uuid,
                "parent_workflow_id": self._parent_workflow_id,
            }

        # All done... last step is to send to the database the workflow plan event,
        # along with all the static information generated by pegasus-plan
        # However, we only do this, if this is the first time we run
        if self._sink is not None and self._last_processed_line == 0:
            # Make sure NetLogger parser is available
            if NLSimpleParser is None:
                logger.critical("NetLogger parser is not loaded, exiting...")
                sys.exit(1)
            # Create NetLogger parser
            my_bp_parser = NLSimpleParser(parse_date=False)
            # Figure out static data filename, and create full path name
            my_bp_file = os.path.splitext(self._dag_file_name)[0] + ".static.bp"
            self._static_bp_file = os.path.join(self._run_dir, my_bp_file)

            # Open static bp file
            try:
                my_static_file = open(self._static_bp_file)
            except Exception:
                logger.critical(
                    "cannot find static bp file %s, exiting..." % (self._static_bp_file)
                )
                sys.exit(1)

            # Send workflow plan info to database
            self.db_send_wf_info()

            # Send event to mark the start of the static content
            self.output_to_db("static.start", {})

            # Process static bp file
            try:
                for my_line in my_static_file:
                    my_keys = {}
                    my_keys = my_bp_parser.parseLine(my_line)
                    if len(my_keys) == 0:
                        continue
                    if not "event" in my_keys:
                        logger.error(
                            "bad event in static bp file: %s, continuing..." % (my_line)
                        )
                        continue
                    my_event = my_keys["event"]
                    del my_keys["event"]
                    # Convert timestamp to epochtime
                    if "ts" in my_keys:
                        my_new_ts = utils.epochdate(my_keys["ts"])
                        if my_new_ts is not None:
                            my_keys["ts"] = my_new_ts

                    # PM-1355 the static.bp file is netlogger formatted.
                    # so the id keys have a . before them. Replace them with __
                    remapped_keys = {}
                    for k, v in my_keys.items():
                        remapped_keys[k.replace(".id", "__id")] = v

                    # Send event to database
                    self.output_to_db(my_event, remapped_keys)
            except Exception:
                logger.critical(
                    "error processing static bp file %s, exiting..."
                    % (self._static_bp_file)
                )
                logger.critical(traceback.format_exc())
                sys.exit(1)
            # Close static bp file
            try:
                my_static_file.close()
            except Exception:
                logger.warning(
                    "error closing static bp file %s, continuing..."
                    % (self._static_bp_file)
                )

            # Send event to mark the end of the static content
            self.output_to_db("static.end", {})

        # If this workflow is a subworkflow and has a parent_id,
        # parent_jobid and parent_jobseq, we send an event to link
        # this workflow's id to the parent job...
        if (
            self._sink is not None
            and self._parent_workflow_id is not None
            and parent_jobid is not None
            and parent_jobseq is not None
        ):
            self.db_send_subwf_link(
                self._wf_uuid, self._parent_workflow_id, parent_jobid, parent_jobseq
            )

        # PM-1334 parse the dag file always in the constructor
        self.parse_dag_file(self._dag_file_name)

    def map_subwf(self, parent_jobid, parent_jobseq, wf_info):
        """
        This function creates a link between a subworkflow and its parent job
        """
        # If this workflow is a subworkflow and has a parent_id,
        # parent_jobid and parent_jobseq, we send an event to link
        # this workflow's id to the parent job...
        if "wf_uuid" in wf_info:
            sub_wf_id = wf_info["wf_uuid"]
        else:
            sub_wf_id = None
        if "parent_workflow_id" in wf_info:
            parent_wf_id = wf_info["parent_workflow_id"]
        else:
            parent_wf_id = None
        if (
            self._sink is not None
            and sub_wf_id is not None
            and parent_wf_id is not None
            and parent_jobid is not None
            and parent_jobseq is not None
        ):
            self.db_send_subwf_link(
                sub_wf_id, parent_wf_id, parent_jobid, parent_jobseq
            )

    def end_workflow(self):
        """
        This function writes the last line in the jobstate.log and closes
        the file.
        """
        if self._finished:
            return

        my_workflow_end = int(time.time())

        if self._output_dir is None:
            my_recover_file = os.path.join(self._run_dir, MONITORD_RECOVER_FILE)
        else:
            my_recover_file = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_RECOVER_FILE)
            )

        self.write_to_jobstate(
            "%d INTERNAL *** MONITORD_FINISHED %d ***\n"
            % (my_workflow_end, self._monitord_exit_code)
        )
        self._JSDB.close()

        # Save all state to disk so that we can start again later
        self.write_workflow_state()

        # Delete recovery file
        try:
            os.unlink(my_recover_file)
            logger.info("recovery file deleted: %s" % (my_recover_file))
        except Exception:
            logger.warning("unable to remove recover file: %s" % (my_recover_file))

        # Write monitord.done file
        if self._output_dir is None:
            my_touch_name = os.path.join(self._run_dir, MONITORD_DONE_FILE)
        else:
            my_touch_name = os.path.join(
                self._output_dir, "{}-{}".format(self._wf_uuid, MONITORD_DONE_FILE)
            )
        try:
            TOUCH = open(my_touch_name, "w")
            TOUCH.write(
                "%s %.3f\n"
                % (
                    utils.isodate(my_workflow_end),
                    (my_workflow_end - self._workflow_start),
                )
            )
            TOUCH.close()
        except Exception:
            logger.error("writing %s" % (my_touch_name))

        # Remove our notifications from the notification lists
        if self._notifications_manager is not None:
            self._notifications_manager.remove_notifications(self._wf_uuid)

        if not self._replay_mode:
            # Attempt to copy the condor common logfile to the current directory
            if self._condorlog is not None:
                if (
                    os.path.isfile(self._condorlog)
                    and os.access(self._condorlog, os.R_OK)
                    and self._condorlog.find("/") == 0
                ):

                    # Copy common condor log to local directory
                    my_log = utils.out2log(self._run_dir, self._out_file)[0]
                    my_cmd = "/bin/cp -p {} {}.copy".format(self._condorlog, my_log)
                    my_status, my_output = commands.getstatusoutput(my_cmd)

                    if my_status == 0:
                        # Copy successful
                        try:
                            os.unlink(my_log)
                        except Exception:
                            logger.error("removing %s" % (my_log))
                        else:
                            try:
                                os.rename("%s.copy" % (my_log), my_log)
                            except Exception:
                                logger.error(
                                    "renaming {}.copy to {}".format(my_log, my_log)
                                )
                            else:
                                logger.info("copied common log to %s" % (self._run_dir))
                    else:
                        logger.info("%s: %d:%s" % (my_cmd, my_status, my_output))

    def find_jobid(self, jobid):
        """
        This function finds the job_submit_seq of a given jobid by
        checking the _jobs_map dict. Since add_job will update
        _jobs_map, this function will return the job_submit_seq of the
        latest jobid added to the workflow
        """
        if jobid in self._jobs_map:
            return self._jobs_map[jobid]

        # Not found, return None
        return None

    def find_job_submit_seq(self, jobid, sched_id=None):
        """
        If a jobid already exists and is in the PRE_SCRIPT_SUCCESS
        mode, this function returns its job_submit_seq. Otherwise, it
        returns None, meaning a new job needs to be created
        """
        # Look for a jobid
        my_job_submit_seq = self.find_jobid(jobid)

        # No such job, return None
        if my_job_submit_seq is None:
            return None

        # Make sure the job is there
        if not (jobid, my_job_submit_seq) in self._jobs:
            logger.warning("cannot find job: {}, {}".format(jobid, my_job_submit_seq))
            return None

        my_job = self._jobs[jobid, my_job_submit_seq]
        if (
            my_job._job_state == "PRE_SCRIPT_SUCCESS"
            or my_job._job_state == "DAGMAN_SUBMIT"
        ):
            # jobid is in "PRE_SCRIPT_SUCCESS" or "DAGMAN_SUBMIT"  state,
            # just return job_submit_seq
            return my_job_submit_seq

        # Ok, check sched_id if it is not None...
        if sched_id is not None:
            if my_job._sched_id == sched_id:
                # sched_id matches job we already have... must be an
                # out-of-order submit event...
                return my_job_submit_seq

        # jobid is in another state, return None
        return None

    def db_send_job_brief(self, my_job, event, status=None, reason=None):
        """
        This function sends to the DB basic state events for a
        particular job

        :param my_job:
        :param event:
        :param status:
        :param reason: reason for the job state event
        :return:
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq
        kwargs["ts"] = my_job._job_state_timestamp
        kwargs["js__id"] = my_job._job_state_seq
        kwargs["reason"] = reason
        if my_job._sched_id is not None:
            kwargs["sched__id"] = my_job._sched_id
        if status is not None:
            kwargs["status"] = status
            if status != 0:
                kwargs["level"] = "Error"

        if event == "post.end":
            # For post-script SUCCESS/FAILED, we send the exitcode
            kwargs["exitcode"] = str(my_job._post_script_exitcode)

        if event == "pre.end":
            # For pre-script SUCCESS/FAILED, we send the exitcode
            kwargs["exitcode"] = str(my_job._pre_script_exitcode)

        if event == "submit.start":
            if my_job._site_name is not None:
                # PM-1196 put in the site information that we have from
                # parsing the job submit file
                kwargs["site"] = my_job._site_name

        # Send job state event to database
        self.output_to_db("job_inst." + event, kwargs)

    def db_send_job_start(self, my_job):
        """
        This function sends to the DB the main.start event
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq
        kwargs["ts"] = my_job._job_state_timestamp
        kwargs["js__id"] = my_job._job_state_seq

        if my_job._input_file is not None:
            kwargs["stdin__file"] = my_job._input_file
        if my_job._output_file is not None:
            kwargs["stdout__file"] = my_job._output_file
        if my_job._error_file is not None:
            kwargs["stderr__file"] = my_job._error_file
        if my_job._sched_id is not None:
            kwargs["sched__id"] = my_job._sched_id

        # Send job state event to database
        self.output_to_db("job_inst.main.start", kwargs)

    def db_send_job_end(self, my_job, status=None, flush_to_stampede=True):
        """
        This function sends to the DB the main.end event
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq
        kwargs["ts"] = my_job._job_state_timestamp
        kwargs["js__id"] = my_job._job_state_seq

        if my_job._site_name is not None:
            kwargs["site"] = my_job._site_name
        else:
            kwargs["site"] = ""
        if my_job._remote_user is not None:
            kwargs["user"] = my_job._remote_user
        else:
            if self._user is not None:
                kwargs["user"] = self._user

        if my_job._main_job_start is not None and my_job._main_job_done is not None:
            # If we have both timestamps, let's try to compute the local duration
            try:
                my_duration = int(my_job._main_job_done) - int(my_job._main_job_start)
                kwargs["local__dur"] = my_duration
            except Exception:
                # Nothing to do, this is not mandatory
                pass
        if my_job._input_file is not None:
            kwargs["stdin_file"] = my_job._input_file
        else:
            # This is not mandatory, according to the schema
            pass

        # Use constant for now... will change it
        if my_job._main_job_multiplier_factor is not None:
            kwargs["multiplier_factor"] = str(my_job._main_job_multiplier_factor)

        # Use the job exitcode for now (if the job has a postscript, it will get updated later
        kwargs["exitcode"] = str(my_job._main_job_exitcode)

        if my_job._sched_id is not None:
            kwargs["sched__id"] = my_job._sched_id
        if status is not None:
            kwargs["status"] = status
            if status != 0:
                kwargs["level"] = "Error"
        else:
            kwargs["status"] = -1
            kwargs["level"] = "Error"

        if flush_to_stampede:
            self.flush_db_send_job_end(my_job, kwargs)
        else:
            # PM-793 we cannot load the stdout stderr right now
            # have to wait for the postscript to finish
            my_job._deferred_job_end_kwargs = kwargs

    def flush_db_send_job_end(self, my_job, kwargs):
        """
        This function sends to the DB the main.end event
        Note: this is a soft flush from a monitord to the stampede loader
        Not the stampede loader, that has a separate mechanism
        to batch and load events into the database
        """

        # PM-814 remote working directory , cluster_start and cluster duration are
        # all parsed from the job output file. So these can only be set after
        # the job output has been parsed
        if my_job._remote_working_dir is not None:
            kwargs["work_dir"] = my_job._remote_working_dir
        else:
            if self._original_submit_dir is not None:
                kwargs["work_dir"] = self._original_submit_dir
        if my_job._cluster_start_time is not None:
            kwargs["cluster__start"] = my_job._cluster_start_time
        if my_job._cluster_duration is not None:
            kwargs["cluster__dur"] = my_job._cluster_duration

        self.load_stdout_err_in_job_instance(my_job, kwargs)
        # Send job state event to database
        self.output_to_db("job_inst.main.end", kwargs)

        # create a composite job event
        composite_kwargs = my_job.create_composite_job_event(kwargs)
        self.output_to_db("job_inst.composite", composite_kwargs)

        # Clean up stdout and stderr, to avoid memory issues...
        if my_job._deferred_job_end_kwargs is not None:
            my_job._deferred_job_end_kwargs = None

        if my_job._stdout_text is not None:
            my_job._stdout_text = None
        if my_job._stderr_text is not None:
            my_job._stderr_text = None
        return

    def load_stdout_err_in_job_instance(self, my_job, kwargs):
        """
        Loads the information from the job stdout and stderr into the job_instance event's kwargs

        :param my_job:
        :param kwargs:
        :return:
        """
        if my_job._output_file is not None:
            if my_job._kickstart_parsed or my_job._has_rotated_stdout_err_files:
                # Only use rotated filename for job with kickstart output
                kwargs["stdout__file"] = my_job._output_file + ".%03d" % (
                    my_job._job_output_counter
                )
            else:
                kwargs["stdout__file"] = my_job._output_file
        else:
            kwargs["stdout__file"] = ""
        if my_job._error_file is not None:
            if my_job._kickstart_parsed or my_job._has_rotated_stdout_err_files:
                # Only use rotated filename for job with kickstart output
                kwargs["stderr__file"] = my_job._error_file + ".%03d" % (
                    my_job._job_output_counter
                )
            else:
                kwargs["stderr__file"] = my_job._error_file
        else:
            kwargs["stderr__file"] = ""
        if self._store_stdout_stderr:
            # Only add stdout and stderr text fields if user hasn't disabled it
            if my_job._stdout_text is not None:
                if len(my_job._stdout_text) > MAX_OUTPUT_LENGTH:
                    # Need to truncate to avoid database problems...
                    kwargs["stdout__text"] = my_job._stdout_text[:MAX_OUTPUT_LENGTH]
                    logger.warning(
                        "truncating stdout for job %s" % (my_job._exec_job_id)
                    )
                else:
                    # Put everything in
                    kwargs["stdout__text"] = my_job._stdout_text
            if my_job._stderr_text is not None:
                if len(my_job._stderr_text) > MAX_OUTPUT_LENGTH:
                    # Need to truncate to avoid database problems...
                    kwargs["stderr__text"] = my_job._stderr_text[:MAX_OUTPUT_LENGTH]
                    logger.warning(
                        "truncating stderr for job %s" % (my_job._exec_job_id)
                    )
                else:
                    # Put everything in
                    kwargs["stderr__text"] = my_job._stderr_text

    def db_send_task_start(
        self, my_job, task_type, task_id=None, invocation_record=None
    ):
        """
        This function sends to the database task start
        events. task_type is either "PRE_SCRIPT", "MAIN_JOB", or
        "POST_SCRIPT"
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        if invocation_record is None:
            invocation_record = {}

        # Sanity check, verify task type
        if (
            task_type != "PRE_SCRIPT"
            and task_type != "POST_SCRIPT"
            and task_type != "MAIN_JOB"
        ):
            logger.warning("unknown task type: %s" % (task_type))
            return

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq

        if task_type == "PRE_SCRIPT":
            # This is a PRE SCRIPT invocation
            # Add PRE_SCRIPT task id to this event
            kwargs["inv__id"] = PRESCRIPT_TASK_ID
            kwargs["ts"] = my_job._pre_script_start
        elif task_type == "POST_SCRIPT":
            # This is a POST SCRIPT invocation
            kwargs["inv__id"] = POSTSCRIPT_TASK_ID
            kwargs["ts"] = my_job._post_script_start
        elif task_type == "MAIN_JOB":
            # This is a MAIN JOB invocation
            if task_id is not None:
                kwargs["inv__id"] = task_id
            else:
                logger.warning("warning: task id is not specified... skipping task...")
                return
            if "start" in invocation_record:
                # Need to convert it to epoch data
                my_start = utils.epochdate(invocation_record["start"])
            else:
                # Not in the invocation record, let's use our own time keeping
                my_start = my_job._main_job_start
                if my_start is None:
                    # This must be a zero duration job (without an ULOG_EXECUTE), just use the end time
                    my_start = my_job._main_job_done
            if my_start is not None:
                kwargs["ts"] = my_start

        # Send job event to database
        self.output_to_db("inv.start", kwargs)

    def db_send_task_end(self, my_job, task_type, task_id=None, invocation_record=None):
        """
        This function sends to the database task end events with all
        the information. task_type is either "PRE_SCRIPT", "MAIN_JOB",
        or "POST_SCRIPT"
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        if invocation_record is None:
            invocation_record = {}

        # Sanity check, verify task type
        if (
            task_type != "PRE_SCRIPT"
            and task_type != "POST_SCRIPT"
            and task_type != "MAIN_JOB"
        ):
            logger.warning("unknown task type: %s" % (task_type))
            return

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq

        if task_type == "PRE_SCRIPT":
            # This is a PRE SCRIPT invocation
            kwargs["inv__id"] = PRESCRIPT_TASK_ID
            kwargs["transformation"] = "dagman::pre"
            # For prescript tasks, nothing to put in the task_id field
            if my_job._pre_script_start is not None:
                kwargs["start_time"] = my_job._pre_script_start
            else:
                kwargs["start_time"] = my_job._pre_script_done
            try:
                kwargs["dur"] = my_job._pre_script_done - my_job._pre_script_start
                kwargs["remote_cpu_time"] = (
                    my_job._pre_script_done - my_job._pre_script_start
                )
            except Exception:
                # Duration cannot be determined, possibly a missing PRE_SCRIPT_START event
                kwargs["dur"] = 0
            kwargs["exitcode"] = str(my_job._pre_script_exitcode)
            if my_job._exec_job_id in self._job_info:
                if self._job_info[my_job._exec_job_id][1] is not None:
                    kwargs["executable"] = self._job_info[my_job._exec_job_id][1]
                else:
                    kwargs["executable"] = ""
                if self._job_info[my_job._exec_job_id][2] is not None:
                    kwargs["argv"] = self._job_info[my_job._exec_job_id][2]
            else:
                kwargs["executable"] = ""
            kwargs["ts"] = my_job._pre_script_done
        elif task_type == "POST_SCRIPT":
            # This is a POST SCRIPT invocation
            kwargs["inv__id"] = POSTSCRIPT_TASK_ID
            kwargs["transformation"] = "dagman::post"
            # For postscript tasks, nothing to put in the task_id field
            if my_job._post_script_start is not None:
                kwargs["start_time"] = my_job._post_script_start
            else:
                kwargs["start_time"] = my_job._post_script_done
            try:
                kwargs["dur"] = my_job._post_script_done - my_job._post_script_start
                kwargs["remote_cpu_time"] = (
                    my_job._post_script_done - my_job._post_script_start
                )
            except Exception:
                # Duration cannot be determined, possibly a missing POST_SCRIPT_START event
                kwargs["dur"] = 0
            kwargs["exitcode"] = str(my_job._post_script_exitcode)
            if my_job._exec_job_id in self._job_info:
                if self._job_info[my_job._exec_job_id][3] is not None:
                    kwargs["executable"] = self._job_info[my_job._exec_job_id][3]
                else:
                    kwargs["executable"] = ""
                if self._job_info[my_job._exec_job_id][4] is not None:
                    kwargs["argv"] = self._job_info[my_job._exec_job_id][4]
            else:
                kwargs["executable"] = ""
            kwargs["ts"] = my_job._post_script_done
        elif task_type == "MAIN_JOB":
            # This is a MAIN JOB invocation
            if task_id is not None:
                kwargs["inv__id"] = task_id
            else:
                logger.warning("warning: task id is not specified... skipping task...")
                return
            if "transformation" in invocation_record:
                kwargs["transformation"] = invocation_record["transformation"]
            else:
                if my_job._main_job_transformation is not None:
                    kwargs["transformation"] = my_job._main_job_transformation
                else:
                    if (
                        my_job._exec_job_id in self._job_info
                        and self._job_info[my_job._exec_job_id][5] is True
                    ):
                        kwargs["transformation"] = "condor::dagman"
            if "derivation" in invocation_record:
                if invocation_record["derivation"] != "null":
                    # Make sure it is not "null"
                    kwargs["task__id"] = invocation_record["derivation"]
            else:
                # Lets see if we have the derivation from the submit file
                if my_job._main_job_derivation is not None:
                    kwargs["task__id"] = my_job._main_job_derivation
                else:
                    # Nothing to do if we cannot get the derivation
                    # from the kickstart record or submit file
                    pass
            if "start" in invocation_record:
                # Need to convert it to epoch data
                my_start = utils.epochdate(invocation_record["start"])
            else:
                # Not in the invocation record, let's use our own time keeping
                my_start = my_job._main_job_start
                if my_start is None:
                    # This must be a zero duration job (without an ULOG_EXECUTE), just use the end time
                    my_start = my_job._main_job_done
            if my_start is not None:
                kwargs["start_time"] = my_start
            if "duration" in invocation_record:
                kwargs["dur"] = invocation_record["duration"]
            else:
                # Duration not in the invocation record
                if (
                    my_job._main_job_start is not None
                    and my_job._main_job_done is not None
                ):
                    try:
                        my_duration = int(my_job._main_job_done) - int(
                            my_job._main_job_start
                        )
                    except Exception:
                        my_duration = None
                    if my_duration is not None:
                        kwargs["dur"] = my_duration
                elif my_job._main_job_done is not None:
                    # This must be a zero duration job (without an ULOG_EXECUTE)
                    # In this case, duration should be set to ZERO
                    kwargs["dur"] = 0
            if "utime" in invocation_record and "stime" in invocation_record:
                try:
                    kwargs["remote_cpu_time"] = float(
                        invocation_record["utime"]
                    ) + float(invocation_record["stime"])
                    # PM-1612 compute avg_cpu as (stime + utime)/duration
                    kwargs["avg_cpu"] = kwargs["remote_cpu_time"] / float(kwargs["dur"])
                except ValueError:
                    pass

            if "maxrss" in invocation_record:
                kwargs["maxrss"] = invocation_record["maxrss"]

            if my_start is not None and "duration" in invocation_record:
                # Calculate timestamp for when this task finished
                try:
                    kwargs["ts"] = int(my_start + float(invocation_record["duration"]))
                except Exception:
                    # Something went wrong, just use the time the main job finished
                    kwargs["ts"] = my_job._main_job_done
            else:
                kwargs["ts"] = my_job._main_job_done
            if "raw" in invocation_record:
                kwargs["exitcode"] = invocation_record["raw"]
            else:
                if my_job._main_job_exitcode is not None:
                    kwargs["exitcode"] = str(my_job._main_job_exitcode)

            if "name" in invocation_record:
                kwargs["executable"] = invocation_record["name"]
            else:
                if my_job._main_job_executable is not None:
                    kwargs["executable"] = my_job._main_job_executable
                else:
                    if (
                        my_job._exec_job_id in self._job_info
                        and self._job_info[my_job._exec_job_id][5] is True
                    ):
                        kwargs["executable"] = condor_dagman_executable
                    else:
                        kwargs["executable"] = ""
            if "argument-vector" in invocation_record:
                if (
                    invocation_record["argument-vector"] is not None
                    and invocation_record["argument-vector"] != ""
                ):
                    kwargs["argv"] = invocation_record["argument-vector"]
            else:
                if (
                    my_job._main_job_arguments is not None
                    and my_job._main_job_arguments != ""
                ):
                    kwargs["argv"] = my_job._main_job_arguments

        if "exitcode" in kwargs:
            if kwargs["exitcode"] != "0":
                kwargs["level"] = "Error"
        else:
            kwargs["level"] = "Error"

        # sanity check and log error about missing exitcode
        if kwargs["exitcode"] is None:
            logger.error("Exitcode not set for task %s", kwargs)

        # Send job event to database
        self.output_to_db("inv.end", kwargs)

    def db_send_host_info(self, my_job, record):
        """
        This function sends host information collected from the
        kickstart record to the database.
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        kwargs = {}

        # Make sure we include the wf_uuid, name, and job_submit_seq
        kwargs["xwf__id"] = my_job._wf_uuid
        kwargs["job__id"] = my_job._exec_job_id
        kwargs["job_inst__id"] = my_job._job_submit_seq

        # Add information about the host
        if "hostname" in record:
            kwargs["hostname"] = record["hostname"]
        else:
            # Don't know what the hostname is
            kwargs["hostname"] = "unknown"
        if "hostaddr" in record:
            kwargs["ip"] = record["hostaddr"]
        else:
            # Don't know what the ip address is
            kwargs["ip"] = "unknown"
        if "resource" in record:
            kwargs["site"] = record["resource"]
        else:
            # Don't know what the site name is
            kwargs["site"] = "unknown"
        if "total" in record:
            kwargs["total_memory"] = record["total"]
        else:
            # This is not mandatory
            pass
        if "system" in record and "release" in record and "machine" in record:
            kwargs["uname"] = (
                record["system"] + "-" + record["release"] + "-" + record["machine"]
            )
        else:
            # This is not mandatory
            pass

        # Add timestamp
        kwargs["ts"] = self._current_timestamp

        # Send host event to database
        self.output_to_db("job_inst.host.info", kwargs)

    def db_send_files_metadata(self, my_job, my_task_id, files):
        """
        This function sends metadata population events for each files
        :param my_job:
        :param my_task_id:
        :param files: a dictionary indexed by lfn where value is a map of metadata attributes
        :return:
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        for lfn in files.keys():
            metadata = files[lfn]
            logger.debug(
                "Generating metadata events for file {} {}".format(lfn, metadata)
            )
            kwargs = {}

            # sample event generated by planner for rc meta
            # ts=2015-10-15T20:57:32.790870Z event=rc.meta xwf.id=f9eb9b2c-60b7-43ce-be0c-6b9dd8ab807d lfn.id="f.b1" key="final_output" value="true"
            # shoudl be consistent

            # Make sure we include the wf_uuid ,
            kwargs["xwf__id"] = my_job._wf_uuid
            kwargs["lfn__id"] = lfn

            # PM-1307 Add timestamp
            kwargs["ts"] = self._current_timestamp
            for key in metadata.get_attribute_keys():
                # send an event per metadata key value pair
                kwargs["key"] = key
                kwargs["value"] = metadata.get_attribute_value(key)
                self.output_to_db("rc.meta", kwargs)

    def compute_integrity_metric(self, files):
        """
        This function computes integrity metric from a single kickstart record
        :param my_job:
        :param my_task_id:
        :param files: a dictionary indexed by lfn where value is a map of metadata attributes
        :return:
        """
        if files is None:
            return None

        count = 0
        duration = 0.0
        timing_key = "checksum.timing"

        for file in files:
            if timing_key in file.get_attribute_keys():
                count = count + 1
                duration += float(file.get_attribute_value(timing_key))

        return (
            IntegrityMetric("compute", "output", count=count, duration=duration)
            if count > 0
            else None
        )

    def db_send_integrity_metrics(self, my_job, my_task_id):
        """
        This function sends aggregated integrity metadata if found.
        :param my_job:
        :param my_task_id:
        :param files: a dictionary indexed by lfn where value is a map of metadata attributes
        :return:
        """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        logger.debug(
            "Generating output integrity metric events for job %s "
            % (my_job._exec_job_id)
        )

        for metric in my_job._integrity_metrics:
            kwargs = {}
            # Make sure we include the wf_uuid, name, and job_submit_seq
            kwargs["xwf__id"] = my_job._wf_uuid
            kwargs["job__id"] = my_job._exec_job_id
            kwargs["job_inst__id"] = my_job._job_submit_seq
            kwargs["type"] = metric.type
            kwargs["file_type"] = metric.file_type
            kwargs["count"] = (
                metric.count if metric.count > 0 else metric.succeeded + metric.failed
            )
            kwargs["duration"] = metric.duration
            # PM-1307 Add timestamp
            kwargs["ts"] = self._current_timestamp
            self.output_to_db("int.metric", kwargs)

            if metric.failed > 0:
                # PM-1295 setup an event to populate the tags table
                nkwargs = kwargs.copy()
                del nkwargs["duration"]
                del nkwargs["type"]
                del nkwargs["file_type"]
                nkwargs["name"] = "int.error"
                nkwargs["count"] = metric.failed
                self.output_to_db("job_inst.tag", nkwargs)

    def db_send_task_monitoring_events(self, my_job, task_id, events):
        """
               This function sends additional monitoring events
               :param my_job:
               :param my_task_id:
               :param events: list of events with event and payload elements
               :return:
               """
        # Check if database is configured
        if self._sink is None:
            return

        # Start empty
        for event in events:
            logger.debug(
                "Generating additional monitoring events for task %s in job %s"
                % (task_id, my_job._exec_job_id)
            )
            kwargs = {}

            # sample event generated by planner for rc meta
            # ts=2015-10-15T20:57:32.790870Z event=rc.meta xwf.id=f9eb9b2c-60b7-43ce-be0c-6b9dd8ab807d lfn.id="f.b1" key="final_output" value="true"
            # shoudl be consistent

            # Make sure we include the wf_uuid ,
            kwargs["xwf__id"] = my_job._wf_uuid
            kwargs["job__id"] = my_job._exec_job_id
            kwargs["job_inst__id"] = my_job._job_submit_seq
            if my_job._sched_id is not None:
                kwargs["sched__id"] = my_job._sched_id
            kwargs["monitoring_event"] = (
                event["monitoring_event"]
                if "monitoring_event" in event
                else "monitoring.additional"
            )

            # PM-1307 check if ts is defined in the event
            kwargs["ts"] = event["ts"] if "ts" in event else self._current_timestamp

            payload = event["payload"] if "payload" in event else None
            if payload is None:
                logger.error("No payload retrieved from event %s" % event)

            # each item in list has to be flattened and put in separate event?
            for item in payload:
                item_kwargs = dict(kwargs)
                item_kwargs.update(item)
                self.output_to_db("task.monitoring", item_kwargs)

    def parse_job_output(self, my_job, job_state):
        """
        This function tries to parse the kickstart output file of a
        given job and collect information for the stampede schema.

        Return an the application exitcode from the kickstart file only if there is exactly
        one kickstart record, else None
        """
        my_output = []
        parse_kickstart = True
        app_exitcode = None

        # a boolean to track if the job has rotated stdout/stderr files
        # used to track the case where we have rotated files for non kickstart jobs

        # Check if this is a subdag job
        if (
            my_job._exec_job_id in self._job_info
            and self._job_info[my_job._exec_job_id][5] is True
        ):
            # Disable kickstart_parsing...
            parse_kickstart = False

        # If job is a subdag job, skip looking for its kickstart output
        if parse_kickstart:
            # Compose kickstart output file name (base is the filename before rotation)
            my_job_output_fn_base = (
                os.path.join(my_job._job_submit_dir, my_job._exec_job_id) + ".out"
            )

            # PM-793 if there is a postscript associated then a job has rotated stdout|stderr
            # OR we are in the PMC only mode where there are no postscripts associated, but
            # still we have rotated logs
            my_job_output_fn = my_job_output_fn_base
            if self.job_has_postscript(my_job._exec_job_id) or self._is_pmc_dag:
                my_job_output_fn = my_job_output_fn_base + ".%03d" % (
                    my_job._job_output_counter
                )
                my_job._has_rotated_stdout_err_files = True

            # First assume we will find rotated file
            my_parser = kickstart_parser.Parser(my_job_output_fn)
            my_output = my_parser.parse_stampede()

            # Check if successful
            if my_parser._open_error is True and not my_job.is_noop_job():
                logger.error(
                    "unable to read output file %s for job %s"
                    % (my_job_output_fn, my_job._exec_job_id)
                )

        # Initialize task id counter
        my_task_id = 1

        # PM-733 update the job with PegasusLite exitcode if it is one.
        my_pegasuslite_ec = self.get_pegasuslite_exitcode(my_job)
        if my_pegasuslite_ec is not None:
            # update the main job exitcode
            my_job._main_job_exitcode = my_pegasuslite_ec
            logger.debug(
                "Pegasus Lite Exitcode for job %s is %s"
                % (my_job._exec_job_id, my_pegasuslite_ec)
            )

        # PM-1295 attempt to read the job stderr file always
        my_job.read_job_error_file()

        if len(my_output) > 0:
            # Parsing the output file resulted in some info... let's parse it

            # Add job information to the Job class.
            logger.debug(
                "Starting extraction of job_info from job output file %s "
                % my_job_output_fn
            )
            my_invocation_found = my_job.extract_job_info(my_output)
            logger.debug(
                "Completed extraction of job_info from job output file %s "
                % my_job_output_fn
            )

            if my_invocation_found:
                num_records = len(my_output)
                # Loop through all records
                for record in my_output:
                    # Skip non-invocation records
                    if not "invocation" in record:
                        continue

                    # Take care of invocation-level notifications
                    if (
                        self.check_notifications() is True
                        and self._notifications_manager is not None
                    ):
                        self._notifications_manager.process_invocation_notifications(
                            self, my_job, my_task_id, record
                        )

                        if num_records == 1:
                            # PM-1176 for clustered records we send INVOCATION notifications because that is how
                            # the planner generated it. Send job notification only for a non clustered job
                            # we have now real app exitcode
                            if "exitcode" in record:
                                app_exitcode = record["exitcode"]

                    # Send task information to the database
                    self.db_send_task_start(my_job, "MAIN_JOB", my_task_id, record)
                    self.db_send_task_end(my_job, "MAIN_JOB", my_task_id, record)

                    # PM-1265 send additional monitoring events if any
                    if my_job._additional_monitoring_events:
                        self.db_send_task_monitoring_events(
                            my_job, my_task_id, my_job._additional_monitoring_events
                        )

                    # PM-992
                    # for outputs in xml record send information as file metadata
                    if "outputs" in record:
                        # Start empty
                        output_files = []
                        for lfn in record["outputs"].keys():
                            output_files.append(record["outputs"][lfn])
                        my_job.add_integrity_metric(
                            self.compute_integrity_metric(output_files)
                        )
                        self.db_send_files_metadata(
                            my_job, my_task_id, record["outputs"]
                        )

                    # Increment task id counter
                    my_task_id = my_task_id + 1

                    # Send host information to the database
                    self.db_send_host_info(my_job, record)
            else:
                # No invocation found, but possibly task records are present...
                # This can be the case for clustered jobs when Kickstart is not used.
                my_tasks = {}
                for record in my_output:
                    if "task" in record:
                        # Ok, this is a task record
                        if not "id" in record:
                            logger.warning(
                                "id missing from task record... skipping to next one"
                            )
                            continue
                        try:
                            my_id = int(record["id"])
                        except Exception:
                            logger.warning(
                                "task id looks invalid, cannot convert it to int: %s skipping to next"
                                % (record["id"])
                            )
                            continue
                        # Add to our list
                        my_tasks[my_id] = record

                if len(my_tasks) > 0:
                    # Now, bring information from the .in file
                    my_status = self.parse_in_file(my_job, my_tasks)
                    if my_status is True:
                        # Parsing the in file completed, now generate tasks by task order
                        for i in sorted(my_tasks):
                            record = my_tasks[i]
                            # Take care of renaming the exitcode field
                            if "status" in record:
                                record["exitcode"] = record[
                                    "status"
                                ]  # This should not be needed anymore...
                                record["raw"] = record["status"]
                            # Validate record
                            if (
                                not "transformation" in record
                                or not "derivation" in record
                                or not "start" in record
                                or not "duration" in record
                                or not "name" in record
                                or not "argument-vector" in record
                            ):
                                logger.info(
                                    "task %d has incomplete information, skipping it..."
                                    % (i)
                                )
                                continue

                            # Take care of invocation-level notifications
                            if (
                                self.check_notifications() is True
                                and self._notifications_manager is not None
                            ):
                                self._notifications_manager.process_invocation_notifications(
                                    self, my_job, my_task_id, record
                                )

                            # Ok, it all validates, send task information to the database
                            self.db_send_task_start(
                                my_job, "MAIN_JOB", my_task_id, record
                            )
                            self.db_send_task_end(
                                my_job, "MAIN_JOB", my_task_id, record
                            )

                            # Increment task id counter
                            my_task_id = my_task_id + 1
                else:
                    # No tasks found...
                    logger.info("no tasks found for job %s..." % (my_job._exec_job_id))
        else:
            # This is the case where we cannot find kickstart records
            # in the output file, this will be true for SUBDAG jobs as well

            # Take care of invocation-level notifications
            if (
                self.check_notifications() is True
                and self._notifications_manager is not None
            ):
                self._notifications_manager.process_invocation_notifications(
                    self, my_job, my_task_id
                )

            # If we don't have any records, we only generate 1 task
            self.db_send_task_start(my_job, "MAIN_JOB", my_task_id)
            self.db_send_task_end(my_job, "MAIN_JOB", my_task_id)

            # Read stdout/stderr files, if not disabled by user
            if self._store_stdout_stderr:
                my_job.read_job_out_file()

            # parse_kickstart will be False for subdag jobs
            if my_job._exec_job_id.startswith("subdax_") or not parse_kickstart:
                # For subdag and subdax jobs, we also generate a host event
                record = {}
                record["hostname"] = socket.getfqdn()
                try:
                    record["hostaddr"] = socket.gethostbyname(socket.getfqdn())
                except Exception:
                    record["hostaddr"] = "unknown"
                record["resource"] = my_job._site_name
                # Send event to the database
                self.db_send_host_info(my_job, record)

        # send any integrity related metrics computed for the jobinstance
        self.db_send_integrity_metrics(my_job, my_task_id)

        # register any files associated with the job
        self.register_files(my_job)
        return app_exitcode

    def register_files(self, job):
        """
        Registers files associated with a registration job .
        """
        # PM-918 check if it is a registration job and succeeded
        if not job._exec_job_id.startswith("register_") or job._main_job_exitcode != 0:
            return

        basename = "%s.in" % (job._exec_job_id)
        # PM-833 the .in file should be picked up from job submit directory
        input_file = os.path.join(job._job_submit_dir, basename)
        logger.info(
            "Populating locations corresponding to succeeded registration job  %s "
            % input_file
        )

        try:
            SUB = open(input_file)
        except OSError:
            logger.error("unable to parse %s" % (input_file))
            return None

        # Parse input file
        for my_line in SUB:
            # split on whitespace - doing lazy parsing
            entry = my_line.split()
            lfn = entry[0]
            pfn = entry[1]
            site = None
            for i in range(2, len(entry)):
                kv = entry[i]
                (key, value) = kv.split("=")
                value = value.strip('"')
                if key == "site":
                    # all other attributes should have been populated while
                    # parsing the static.bp file
                    site = value
                    break

            # create the event for replica catalog to populate the pfn
            # Start empty
            kwargs = {}
            # Make sure we include the wf_uuid
            kwargs["xwf__id"] = self._wf_uuid
            kwargs["lfn__id"] = lfn
            kwargs["pfn"] = pfn
            kwargs["site"] = site

            # Send rc.pfn event to the database
            self.output_to_db("rc.pfn", kwargs)

    def get_pegasuslite_exitcode(self, job):
        """
        Determine if the stderr contains PegasusLite output, and if so returns the PegasusLite exitcode
        if found , else None
        """

        ec = None

        # if the stdout/stderr files are rotated
        # for non kickstart and kickstart launched jobs both
        error_basename = job._error_file

        # sanity check subdax or subdag jobs can have no error files
        if error_basename is None:
            return ec

        if job._has_rotated_stdout_err_files:
            error_basename += ".%03d" % (job._job_output_counter)

        errfile = os.path.join(self._run_dir, error_basename)
        if errfile is None or not os.path.isfile(errfile):
            return ec

        # Read the file first
        f = open(errfile)
        txt = f.read()
        f.close()

        # try and determine the exitcode from .err file
        ec_match = re_parse_pegasuslite_ec.search(txt)
        if ec_match:
            # a match yes it is a PegasusLite job . gleam the exitcode
            ec = ec_match.group(1)

        return ec

    def add_job(self, jobid, job_state, sched_id=None):
        """
        This function adds a new job to our list of jobs. It first checks if
        the job is already in our list in the PRE_SCRIPT_SUCCESS state, if so,
        we just update its sched id. Otherwise we create a new Job container.
        In any case, we always set the job state to job_state.
        """
        my_job_submit_seq = self.find_job_submit_seq(jobid, sched_id)

        if my_job_submit_seq is not None:
            # Job already exists
            if not (jobid, my_job_submit_seq) in self._jobs:
                logger.warning(
                    "cannot find job: {}, {}".format(jobid, my_job_submit_seq)
                )
                return

            my_job = self._jobs[jobid, my_job_submit_seq]

            # Set sched_id
            if sched_id is not None:
                my_job._sched_id = sched_id

            # Update job state
            my_job._job_state = job_state
            my_job._job_state_timestamp = int(self._current_timestamp)
        else:
            # This is a new job, we have to do everything from scratch
            my_job_submit_seq = self._job_submit_seq

            # Make sure job is not already there
            if (jobid, my_job_submit_seq) in self._jobs:
                logger.warning(
                    "trying to add job twice: {}, {}".format(jobid, my_job_submit_seq)
                )
                return

            # PM-1334 log extra errors if dag file is not populated
            job_submit_dir = self._run_dir
            if not self._job_info:
                logger.error(
                    "_job_info not populated for dag . Check if dag file was parsed by monitord %s %s"
                    % (self._dag_file_name, self._out_file)
                )
                logger.error(
                    "Using workflow submit directory %s as job submit dir for %s "
                    % (self._run_dir, jobid)
                )
            else:
                # PM-833 determine the job submit directory based on the path to the submit file
                try:
                    job_submit_dir = self.determine_job_submit_directory(
                        jobid, self._job_info[jobid][0]
                    )
                except KeyError:
                    logger.error(
                        "Job %s not in _job_info for %s %s"
                        % (jobid, self._out_file, self._dag_file_name)
                    )
                    logger.error(
                        "Using workflow submit directory %s as job submit dir for %s "
                        % (self._run_dir, jobid)
                    )

            # Create new job container
            my_job = Job(self._wf_uuid, jobid, job_submit_dir, my_job_submit_seq)
            # Set job state
            my_job._job_state = job_state
            my_job._job_state_timestamp = int(self._current_timestamp)
            # Set sched_id
            my_job._sched_id = sched_id
            # Add job to our list of jobs
            self._jobs[jobid, my_job_submit_seq] = my_job

            # Add/Update job in our job map
            self._jobs_map[jobid] = my_job_submit_seq

            # Update job_submit_seq
            self._job_submit_seq = self._job_submit_seq + 1

        # Update job counter if this job is in the SUBMIT state
        if job_state == "SUBMIT":
            # Now, we set the job output counter for this particular job
            my_job._job_output_counter = self.increment_job_counter(jobid)

        return my_job_submit_seq

    def increment_job_counter(self, jobid):
        """
        This function increments the job counter by 1 in the internal job counters map
        and returns the value.
        If it does not exist for a job it is set to 0
        """
        if jobid in self._job_counters:
            # Counter already exists for this job, just increate it by 1
            self._job_counters[jobid] = self._job_counters[jobid] + 1
        else:
            # No counter for this job yet
            self._job_counters[jobid] = 0

        return self._job_counters[jobid]

    def job_update_info(self, jobid, job_submit_seq, sched_id=None):
        """
        This function adds info to an exising job.
        """

        # Make sure job is already there
        if not (jobid, job_submit_seq) in self._jobs:
            logger.warning("cannot find job: {}, {}".format(jobid, job_submit_seq))
            return

        my_job = self._jobs[jobid, job_submit_seq]
        # Set sched_id
        my_job._sched_id = sched_id

        # Everything done
        return

    def update_job_state(
        self, jobid, sched_id, job_submit_seq, job_state, status, walltime, reason=None
    ):
        """
        This function updates a	job's state, and also writes
        a line in our jobstate.out file.
        """
        # Find job
        if job_submit_seq is None:
            # Need to get job_submit_seq from our hash table
            if jobid in self._jobs_map:
                job_submit_seq = self._jobs_map[jobid]
        if not (jobid, job_submit_seq) in self._jobs:
            logger.warning("cannot find job: {}, {}".format(jobid, job_submit_seq))
            return
        # Got it
        my_job = self._jobs[jobid, job_submit_seq]

        # PM-749 track last job
        if job_state is not None:
            self._last_known_job = my_job

        # Check for the out of order submit event case
        if my_job._sched_id is None and sched_id is not None:
            my_out_of_order_events_detected = True
        else:
            my_out_of_order_events_detected = False

        # PM-1281 track previous job state
        previous_job_state = my_job.get_job_state()
        # Update job state
        my_job.set_job_state(job_state, sched_id, self._current_timestamp, status)

        # Make status a string so we can print properly
        if status is not None:
            status = str(status)

        # Create content -- use one space only
        my_line = "%d %s %s %s %s %s %d" % (
            self._current_timestamp,
            jobid,
            job_state,
            status or my_job._sched_id or "-",
            my_job._site_name or "-",
            walltime or "-",
            job_submit_seq or "-",
        )
        logger.debug("new state: %s" % (my_line))

        # Prepare for atomic append
        self.write_to_jobstate("%s\n" % (my_line))

        if self._sink is None and not self._enable_notifications:
            # Not generating events and notifcations, nothing else to do
            return

        # PM-749 only if condor version > 8.3.3 we look for JOB_HELD_REASON state
        job_held_state = "JOB_HELD"
        if (
            self._dagman_version is not None
            and self._dagman_version >= Workflow.CONDOR_VERSION_8_3_3
        ):
            job_held_state = "JOB_HELD_REASON"

        # PM-793 only parse job output here if a postscript is NOT associated with
        # the job in the .dag file
        # OR we are in the PMC only mode where there are no postscripts associated
        parse_job_output_on_job_success_failure = (
            not self.job_has_postscript(jobid) or self._is_pmc_dag
        )

        # PM-1176 track kickstart app exitcode only for non clustered jobs
        real_app_exitcode = None

        # Parse the kickstart output file, also send mainjob tasks, if needed
        if job_state == "JOB_SUCCESS" or job_state == "JOB_FAILURE":
            # Main job has ended
            if parse_job_output_on_job_success_failure:
                # PM-793 only parse job output here if a postscript is NOT associated with
                # the job in the .dag file
                # OR we are in the PMC only mode where there are no postscripts associated
                real_app_exitcode = self.parse_job_output(my_job, job_state)

        if self._sink is None:
            # Not generating events, nothing else to do except clean
            # up stdout and stderr, to avoid memory issues...
            if my_job._stdout_text is not None:
                my_job._stdout_text = None
            if my_job._stderr_text is not None:
                my_job._stderr_text = None
            return

        if my_out_of_order_events_detected:
            # We need to send a submit.start event in order to create
            # the database entry for this job
            self.db_send_job_brief(my_job, "submit.start")

        # Check if we need to send any tasks to the database
        if job_state == "PRE_SCRIPT_SUCCESS" or job_state == "PRE_SCRIPT_FAILURE":
            # PRE script finished
            self.db_send_task_start(my_job, "PRE_SCRIPT")
            self.db_send_task_end(my_job, "PRE_SCRIPT")
        elif job_state == "POST_SCRIPT_SUCCESS" or job_state == "POST_SCRIPT_FAILURE":
            if my_job._main_job_exitcode is None:
                # PM-1070 set the main exitcode to the postscript exitcode
                # No JOB_TERMINATED OR JOB_SUCCESS OR JOB_FAILURE for this job instance
                my_job._main_job_exitcode = my_job._post_script_exitcode
                logger.warning(
                    "Set main job exitcode for %s to post script failure code %s"
                    % (my_job._exec_job_id, my_job._post_script_exitcode)
                )

            # PM-793 we parse the job.out and .err files when postscript finishes
            real_app_exitcode = self.parse_job_output(my_job, job_state)

            # check to see if there is a deferred job_inst.main.end event that
            # has to be sent to the database
            if my_job._deferred_job_end_kwargs is not None:
                self.flush_db_send_job_end(my_job, my_job._deferred_job_end_kwargs)

            # POST script finished
            self.db_send_task_start(my_job, "POST_SCRIPT")
            self.db_send_task_end(my_job, "POST_SCRIPT")

        # PM-1176 only send any notifications after we have parsed .out file if required
        # Take care of job-level notifications
        if (
            self.check_notifications() is True
            and self._notifications_manager is not None
        ):
            if real_app_exitcode is not None:
                status = real_app_exitcode
            self._notifications_manager.process_job_notifications(
                self, job_state, my_job, status
            )

        # Now, figure out what state event we need to send to the database
        if job_state == "PRE_SCRIPT_STARTED":
            self.db_send_job_brief(my_job, "pre.start")
        elif job_state == "PRE_SCRIPT_TERMINATED":
            self.db_send_job_brief(my_job, "pre.term")
        elif job_state == "PRE_SCRIPT_SUCCESS":
            self.db_send_job_brief(my_job, "pre.end", 0)
        elif job_state == "PRE_SCRIPT_FAILURE":
            # PM-704 set the main exitcode to the prescript exitcode
            my_job._main_job_exitcode = my_job._pre_script_exitcode

            # PM-704 the job counters need to be updated in case of retries for pre script failures
            # Now, we set the job output counter for this particular job
            my_job._job_output_counter = self.increment_job_counter(jobid)

            # record the job output for pegasus plan prescript logs
            # we only do for prescript failures. once job starts running
            # the dagman output gets populated
            if self._job_info[my_job._exec_job_id][8] is not None:
                my_job._output_file = self._job_info[my_job._exec_job_id][
                    8
                ] + ".%03d" % (my_job._job_output_counter)
                my_job.read_job_out_file(my_job._output_file)

            # PM-704 and send the job end event to record failure
            # in addition to the brief
            self.db_send_job_brief(my_job, "pre.end", -1)
            self.db_send_job_end(my_job, -1, True)
        elif job_state == "SUBMIT":
            self.db_send_job_brief(my_job, "submit.start")
            self.db_send_job_brief(my_job, "submit.end", 0)
        elif job_state == "GRID_SUBMIT":
            self.db_send_job_brief(my_job, "grid.submit.start")
            self.db_send_job_brief(my_job, "grid.submit.end", 0)
        elif job_state == "GLOBUS_SUBMIT":
            self.db_send_job_brief(my_job, "globus.submit.start")
            self.db_send_job_brief(my_job, "globus.submit.end", 0)
        elif job_state == "SUBMIT_FAILED":
            # PM-877 set main job exitcode to prevent integrity error on invocation
            my_job._main_job_exitcode = 1
            self.db_send_job_brief(my_job, "submit.start")
            self.db_send_job_brief(my_job, "submit.end", -1)
        elif job_state == "GLOBUS_SUBMIT_FAILED":
            # PM-877 set main job exitcode to prevent integrity error on invocation
            my_job._main_job_exitcode = 1
            self.db_send_job_brief(my_job, "globus.submit.start")
            self.db_send_job_brief(my_job, "globus.submit.end", -1)
        elif job_state == "GRID_SUBMIT_FAILED":
            # PM-877 set main job exitcode to prevent integrity error on invocation
            my_job._main_job_exitcode = 1
            self.db_send_job_brief(my_job, "grid.submit.start")
            self.db_send_job_brief(my_job, "grid.submit.end", -1)
        elif job_state == "EXECUTE":
            self.db_send_job_start(my_job)
        elif job_state == "REMOTE_ERROR":
            self.db_send_job_brief(my_job, "remote_error")
        elif job_state == "IMAGE_SIZE":
            self.db_send_job_brief(my_job, "image.info")
        elif job_state == "JOB_TERMINATED":
            self.db_send_job_brief(my_job, "main.term", 0)
        elif job_state == "JOB_SUCCESS":
            self.db_send_job_end(my_job, 0, parse_job_output_on_job_success_failure)
        elif job_state == "JOB_FAILURE":
            self.db_send_job_end(my_job, -1, parse_job_output_on_job_success_failure)
        elif job_state == "JOB_ABORTED":
            # job abort should trigger a job failure to account for case
            # when no postscript is associated and failure does not get
            # captured.
            my_job._main_job_exitcode = 1
            self.db_send_job_brief(my_job, "abort.info")
            # PM-1281 we only flush to stampede backend if job was not
            # aborted because of JOB_HELD reasons.
            # This allows us to update the event with task stdout and stderr from kickstart record
            # when the associated POSTSCRIPT fails
            flush = True
            if previous_job_state is not None and (
                previous_job_state == "JOB_HELD"
                or previous_job_state == "JOB_HELD_REASON"
            ):
                flush = parse_job_output_on_job_success_failure
            self.db_send_job_end(my_job, -1, flush)
        elif job_state == job_held_state:  # JOB_HELD_REASON or JOB_HELD states
            # PM-749 we send the JOB_HELD event once we know the reason for it.
            self.db_send_job_brief(my_job, "held.start", reason=reason)
        elif job_state == "JOB_EVICTED":
            self.db_send_job_brief(my_job, "main.term", -1)
        elif job_state == "JOB_RELEASED":
            self.db_send_job_brief(my_job, "held.end", 0)
        elif job_state == "POST_SCRIPT_STARTED":
            self.db_send_job_brief(my_job, "post.start")
        elif job_state == "POST_SCRIPT_TERMINATED":
            self.db_send_job_brief(my_job, "post.term")
        elif job_state == "POST_SCRIPT_SUCCESS":
            self.db_send_job_brief(my_job, "post.end", 0)
        elif job_state == "POST_SCRIPT_FAILURE":
            self.db_send_job_brief(my_job, "post.end", -1)

        if (
            job_state == "SUBMIT_FAILED"
            or job_state == "GLOBUS_SUBMIT_FAILED"
            or job_state == "GRID_SUBMIT_FAILED"
        ):
            # PM-1061 for any job submission failure case, set it as job stderr, so that
            # it gets populated with the job instance end event
            my_job._stderr_text = utils.quote(
                "Job submission failed because of HTCondor event %s" % job_state
            )
            self.db_send_job_end(my_job, -1, True)

    def determine_job_submit_directory(self, job_id, submit_file):
        """
        Returns the submit directory where the job.out|.err files reside

        :param job_id:       the job name
        :param submit_file:  the path to the job submit file
        :return:
        """
        if submit_file is None:
            # PM-833 only for pmc only case rely on the workflow dag file
            if self._is_pmc_dag:
                return self._run_dir
            else:
                logger.error("Submit file path not specified for job %s" % job_id)
            return None

        # return the directory component of the path
        return os.path.dirname(submit_file)

    def parse_job_sub_file(self, jobid, job_submit_seq):
        """
        This function calls a function in the Job class to parse
        a job's submit file and extract planning information
        """

        # Find job
        if not (jobid, job_submit_seq) in self._jobs:
            logger.warning("cannot find job: {}, {}".format(jobid, job_submit_seq))
            return None, None

        # Check if we have an entry for this job
        if not jobid in self._job_info:
            return None, None

        # Get corresponding job
        my_job = self._jobs[jobid, job_submit_seq]

        # Make sure if we have a file for this entry
        # (should always be there, except for SUBDAG jobs and PMC)
        if self._job_info[jobid][0] is None:
            if self._job_info[jobid][5] is True:
                # Yes, this is a SUBDAG job... let's set the site as local for this job
                my_job._site_name = "local"
            else:
                # In the PMC case we need to set the names of the out and error
                # file because we can't parse the .sub file, which doesn't exist
                my_job._input_file = None
                my_job._output_file = "%s.out" % my_job._exec_job_id
                my_job._error_file = "%s.err" % my_job._exec_job_id
                # TODO Find the actual site name for PMC tasks
            return None, None

        # Parse sub file
        my_diff, my_site = my_job.parse_sub_file(
            self._current_timestamp, self._job_info[jobid][0]
        )

        # Change input, output, and error files to be relative to the submit directory
        try:
            if my_job._input_file.find(self._original_submit_dir) >= 0:
                # Path to file includes original submit_dir, let's try to remove it
                my_job._input_file = os.path.normpath(
                    my_job._input_file.replace(
                        (self._original_submit_dir + os.sep), "", 1
                    )
                )
        except Exception:
            # Something went wrong, let's just keep what we had...
            pass
        try:
            if my_job._output_file.find(self._original_submit_dir) >= 0:
                # Path to file includes original submit_dir, let's try to remove it
                my_job._output_file = os.path.normpath(
                    my_job._output_file.replace(
                        (self._original_submit_dir + os.sep), "", 1
                    )
                )
        except Exception:
            # Something went wrong, let's just keep what we had...
            pass

        try:
            if my_job._error_file.find(self._original_submit_dir) >= 0:
                # Path to file includes original submit_dir, let's try to remove it
                my_job._error_file = os.path.normpath(
                    my_job._error_file.replace(
                        (self._original_submit_dir + os.sep), "", 1
                    )
                )
        except Exception:
            # Something went wrong, let's just use what we had...
            pass

        # All done
        return my_diff, my_site

    def has_subworkflow(self, jobid, wf_retries):
        """
        This function returns a new dagman.out file to follow if the
        job is either a SUBDAG job, a pegasus-plan, or a subdax_
        job. Otherwise, it returns None.
        """
        # This shouldn't be the case...
        if not jobid in self._job_info:
            return None

        # First we take care of SUBDAG jobs
        if self._job_info[jobid][5] is True:
            # We cannot go into SUBDAG workflows as they are not
            # planned by Pegasus and do not contain the information
            # needed by the 3.1 Stampede schema.
            return None
        #            # This is a SUBDAG job, first check if dag is there
        #            if self._job_info[jobid][6] is None:
        #                return None
        #            # Looks ok, return new dagman.out
        #            my_dagman_out = self._job_info[jobid][6] + ".dagman.out"
        else:
            # Now check if this is a pegasus-plan or a subdax_ job

            # First, look for a jobid
            my_job_submit_seq = self.find_jobid(jobid)

            # No such job, return None
            if my_job_submit_seq is None:
                return None

            # Make sure the job is there
            if not (jobid, my_job_submit_seq) in self._jobs:
                logger.warning(
                    "cannot find job: {}, {}".format(jobid, my_job_submit_seq)
                )
                return None

            my_job = self._jobs[jobid, my_job_submit_seq]
            my_dagman_out = my_job._job_dagman_out
            if my_dagman_out is None:
                # PM-951 log error only for subdax jobs
                if my_job._exec_job_id.startswith("subdax_"):
                    logger.error(
                        "unable to determine the dagman.out file to track for job %s %s "
                        % (jobid, my_job_submit_seq)
                    )

                return None

        # Got it!
        my_dagman_out = os.path.normpath(my_dagman_out)

        if my_dagman_out.find(self._original_submit_dir) >= 0:
            # Path to new dagman.out file includes original submit_dir, let's try to change it
            my_dagman_out = os.path.normpath(
                my_dagman_out.replace((self._original_submit_dir + os.sep), "", 1)
            )
            # Join with current run directory
            my_dagman_out = os.path.join(self._run_dir, my_dagman_out)

        #        try:
        #            my_dagman_out = os.path.relpath(my_dagman_out, self._original_submit_dir)
        #        except Exception:
        #            pass

        # Split filename into dir and base names
        my_dagman_dir = os.path.dirname(my_dagman_out)
        my_dagman_file = os.path.basename(my_dagman_out)

        if wf_retries is None:
            logger.warning(
                "persistent wf_retry not available... using sub-workflow directory: %s"
                % (my_dagman_dir)
            )
            return my_dagman_out

        # Check if we have seen this sub-workflow before
        """
        if my_dagman_dir in wf_retries:
            # Yes, increment out retry counter...
            my_retry = wf_retries[my_dagman_dir]
            my_retry = my_retry + 1
            wf_retries[my_dagman_dir] = my_retry
        else:
            # No, this is the first time we get to this sub-workflow
            wf_retries[my_dagman_dir] = 0
            my_retry = 0
        """
        # PM-704 the retries for sub workflows are tracked solely on basis
        # of job counters. this handles case where we have prescript errors
        # workflow fails. we fix prescript error and submit rescue dag
        my_retry = self._job_counters[jobid]
        if my_retry is None:
            my_retry = 0

        wf_retries[my_dagman_dir] = my_retry

        # Compose directory... assuming replanning mode
        my_retry_dir = my_dagman_dir + ".%03d" % (my_retry)

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
                    "sub-workflow directory %s does not exist! Skipping this sub-workflow..."
                    % (my_retry_dir)
                )
                return None

        # Found sub-workflow directory, let's compose the final path to the new dagman.out file...
        my_dagman_out = os.path.join(my_retry_dir, my_dagman_file)

        return my_dagman_out

    def set_dagman_version(self, major, minor, patch):
        """
        Sets the dagman version

        :param major:
        :param minor:
        :param patch:
        :return:
        """
        try:
            self._dagman_version = Workflow.get_numeric_version(major, minor, patch)
        except Exception:
            # failsafe. default to 8.2.8 last stable release that did not report held job reasons
            self._dagman_version = Workflow.CONDOR_VERSION_8_2_8

    def get_dagman_version(self):
        """
        Return the dagman version as integer

        :return:
        """
        return self._dagman_version

    def write_to_jobstate(self, str):
        """
        Encodes a string to utf-8 write out to the jobstate.log file
        :param str:
        :return:
        """
        if str is not None:
            self._JSDB.write(str.encode("utf-8"))


# End of Workflow Class
