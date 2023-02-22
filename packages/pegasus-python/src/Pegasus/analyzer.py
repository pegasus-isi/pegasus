#!/usr/bin/env python3

"""
Pegasus utility for parsing jobstate.log and reporting succesful and failed jobs

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


import logging
import optparse
import os
import re
import subprocess
import sys
import tempfile
import traceback
from enum import Enum

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.workflow import stampede_statistics
from Pegasus.tools import kickstart_parser, utils

logger = logging.getLogger("pegasus-newanalyzer")

utils.configureLogging(level=logging.WARNING)

# --- regular expressions -------------------------------------------------------------

re_parse_property = re.compile(r"([^:= \t]+)\s*[:=]?\s*(.*)")
re_parse_script_pre = re.compile(r"^SCRIPT PRE (\S+) (.*)")
re_parse_condor_subs = re.compile(r'(\S+)="([^"]+)"')
re_collapse_condor_subs = re.compile(r"\$\([^\)]*\)")
re_ks_invocation = re.compile(r"^[/\w]*pegasus-kickstart\b[^/]*[\s]+([/\w-]*)\b.*")


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
        self.condor_subs = {}  # Lits of condor substitutions rom DAG VARS line

    def set_state(self, new_state):
        """
        This function updates a job state
        """
        self.state = new_state


# --- exceptions ----------------------------------------------------------------------
class AnalyzerError(Exception):
    pass


class WorkflowFailureError(Exception):
    def __init__(self, message, wf_uuid=None, submit_dir=None):
        """
        :param message: Exception message
        :param wf_uuid: workflow_uuid of the failed workflow
        :param submit_dir: submit dir for workflow failed
        """
        super().__init__(message)

        self.wf_uuid = wf_uuid
        self.submit_dir = submit_dir

    def __str__(self):
        message = super().__str__()
        if self.wf_uuid:
            message += " wf_uuid: " + self.wf_uuid
        if self.submit_dir:
            message += " submit dir: " + self.submit_dir
        return message


# --- constants -----------------------------------------------------------------------

MAXLOGFILE = 1000  # For log file rotation, check files .000 to .999
debug_level = logging.WARNING  # For now

# --- global variables regarding user options----------------------------------------------------------------

prog_base = os.path.split(sys.argv[0])[1].replace(".py", "")  # Name of this program
input_dir = None  # Directory given in -i command line option
dag_path = None  # Path of the dag file
debug_job = None  # Path of a submit file to debug
debug_dir = None  # Temp directory to use while debugging a job
debug_job_local_executable = None  # the local path to user executable for debugging job
workflow_type = None  # Type of the workflow being debugged
workflow_base_dir = ""  # Workflow submit_dir or dirname(jsd) from braindump file
run_monitord = 0  # Run monitord before trying to analyze the output
output_dir = None  # Output_dir for all files written by monitord
top_dir = None  # Top_dir of the main workflow, for obtaining the db location
use_files = False  # Flag for using files in the workflow dir instead of the db
quiet_mode = False  # Prints out/err filenames instead of dumping their contents
strict_mode = False  # Gets out/err filenames from submit file
summary_mode = False  # Print just the summary output
debug_mode = False  # Mode that enables debugging a single job
recurse_mode = False  # Mode that automatically recurses into failed sub workflows.
traverse_all = False  # Mode that automatically traverses all descendant sub workflows.
indent_length = 0  # the number of tabs to print before printing to console
indent = ""  # the corresponding indent string
print_invocation = 0  # Prints invocation command for failed jobs
print_pre_script = 0  # Prints the SCRIPT PRE line for failed jobs, if present
total = 0  # Number of total jobs
success = 0  # Number of successful jobs
failed = 0  # Number of failed jobs
unsubmitted = 0  # Number of unsubmitted jobs
unknown = 0  # Number of jobs in an unknown state
held = 0  # number of held jobs
failed_jobs = []  # List of jobs that failed
unknown_jobs = []  # List of jobs that neither succeeded nor failed



# --- Analyze classes to run analyzer ----------------------------------------------------------------

class AnalyzeFiles:
    def __init__(self):
        self.jsdl_filename = "jobstate.log"  # Default name of the log file to use
        self.jobs = {}  # List of jobs found in the jobstate.log file
        
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

        global workflow_base_dir, dag_path

        # Get the dag file if it was not specified by the user
        if dag_path is None:
            dag_path = self.find_file(input_dir, ".dag")
            logger.info("using %s, use the --dag option to override" % (dag_path))

        # Build dagman.out path
        dagman_out_path = dag_path + ".dagman.out"

        # Check if we can write to the run directory
        run_directory_writable = os.access(input_dir, os.W_OK)

        # Invoke monitord if requested
        if run_monitord:
            if output_dir is not None:
                # If output_dir is specified, invoke monitord with that path
                self.invoke_monitord("%s.dagman.out" % (dag_path), output_dir)
                # jobstate.log file uses wf_uuid as prefix
                jsdl_path = os.path.join(output_dir, self.get_jsdl_filename(input_dir))
            else:
                if run_directory_writable:
                    # Run directory is writable, write monitord output to jobstate.log file
                    jsdl_path = os.path.join(input_dir, self.jsdl_filename)
                    # Invoke monitord
                    self.invoke_monitord("%s.dagman.out" % (dag_path), None)
                else:
                    # User must provide the --output-dir option
                    raise AnalyzerError(input_dir 
                                        + " is not writable. "
                                        + "User must specify directory for new monitord logs with the --output-dir option,"
                                        + " exiting...")                             
        else:
            if output_dir is not None:
                # jobstate.log file uses wf_uuid as prefix and is inside output_dir
                jsdl_path = os.path.join(output_dir, self.get_jsdl_filename(input_dir))
            else:
                jsdl_path = os.path.join(input_dir, self.jsdl_filename)

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
        wfparams = utils.slurp_braindb(input_dir)
        if "submit_dir" in wfparams:
            workflow_base_dir = os.path.normpath(wfparams["submit_dir"])
        elif "jsd" in wfparams:
            workflow_base_dir = os.path.dirname(os.path.normpath(wfparams["jsd"]))

        # First we learn about jobs by going through the dag file
        self.parse_dag_file(dag_path)

        # Read logfile
        self.parse_jobstate_log(jsdl_path)

        # Process our jobs
        self.analyze()

        # Print summary of our analysis
        if summary_mode:
            # PM-1762 in the files mode we don't determine workflow status
            print_top_summary(workflow_status=None)
        else:
            # This is non summary mode despite of the name (go figure)
            self.print_summary()

        # PM-1039
        check_for_wf_start()

        if failed > 0:
            # Workflow has failures, exit with exitcode 2
            raise WorkflowFailureError("One or more workflows failed")
        
        # Workflow has no failures, exit with exitcode 0
        return


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
            # Add output_dir, if given
            monitord_cmd = monitord_cmd + " --output-dir " + output_dir
        monitord_cmd = monitord_cmd + " " + dagman_out_file
        logger.info("running: %s" % (monitord_cmd))

        try:
            #status, output = commands.getstatusoutput(monitord_cmd)
            #commenting it out, couldn't find .getstatusoutput in commands module
            backticks(monitord_cmd)
        except Exception:
            raise AnalyzerError("could not invoke monitord, exiting...")    
            
            
    def get_jsdl_filename(self, input_dir):
        """
        This function parses the braindump file in the input_dir,
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
                    logger.warn("confused parsing dag line: %s" % (line))
                    continue
                if not self.has_seen(my_job[1]):
                    self.add_job(my_job[1], "UNSUBMITTED")
                    # Get submit file information from dag file
                    self.jobs[my_job[1]].sub_file = os.path.join(input_dir, my_job[2])
                    if my_job[1].startswith("pegasus-plan") or my_job[1].startswith(
                        "subdax_"
                    ):
                        # Mark job as subdax
                        self.jobs[my_job[1]].is_subdax = True
                else:
                    logger.warn("job appears twice in dag file: %s" % (my_job[1]))
            if line.startswith("SUBDAG EXTERNAL"):
                # This is a subdag line, parse it to get job name and directory
                my_job = line.split()
                if len(my_job) != 6:
                    logger.warn("confused parsing dag line: %s" % (line))
                    continue
                if not self.has_seen(my_job[2]):
                    self.add_job(my_job[2], "UNSUBMITTED")
                    self.jobs[my_job[2]].is_subdag = True
                    self.jobs[my_job[2]].dag_path = my_job[3]
                    self.jobs[my_job[2]].subdag_dir = my_job[5]
                else:
                    logger.warn("job appears twice in dag file: %s" % (my_job[2]))
            if line.startswith("SCRIPT PRE"):
                # This is a SCRIPT PRE line, parse it to get the script for the job
                my_script = re_parse_script_pre.search(line)
                if my_script is None:
                    # Couldn't parse line
                    logger.warn("confused parsing dag line: %s" % (line))
                    continue
                # Get job name, and check if we have it
                my_job = my_script.group(1)
                if not self.has_seen(my_job):
                    # Cannot find this job, ignore this line
                    logger.warn(
                        "couldn't find job: %s for PRE SCRIPT line in dag file" % (my_job)
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
                        logger.warn(
                            "couldn't find job: %s for VARS line in dag file" % (my_job)
                        )
                        continue
                    # Good, parse the condor substitutions, and create substitution dictionary
                    for my_key, my_val in re_parse_condor_subs.findall(line):
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
                logger.warn("job %s not present in dag file" % (jobname))
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
        global total, success, failed, unsubmitted, unknown

        for my_job in self.jobs:
            total = total + 1
            if (
                self.jobs[my_job].state == "POST_SCRIPT_SUCCESS"
                or self.jobs[my_job].state == "JOB_SUCCESS"
            ):
                success = success + 1
            elif (
                self.jobs[my_job].state == "POST_SCRIPT_FAILURE"
                or self.jobs[my_job].state == "JOB_FAILURE"
            ):
                failed_jobs.append(my_job)
                failed = failed + 1
            elif self.jobs[my_job].state == "UNSUBMITTED":
                unsubmitted = unsubmitted + 1
            else:
                # It seems we don't have a final result for this job
                unknown_jobs.append(my_job)
                unknown = unknown + 1
                

    def print_summary(self):
        """
        This function prints the analyzer report summary
        """

        # First print the summary section
        print_top_summary()

        # Print information about failed jobs
        if len(failed_jobs):
            print_console("Failed jobs' details".center(80, "*"))
            for job in failed_jobs:
                self.print_job_info(job)

        # Print information about unknown jobs
        if len(unknown_jobs):
            print_console("Unknown jobs' details".center(80, "*"))
            for job in unknown_jobs:
                self.print_job_info(job)

        

    def print_job_info(self, job):
        """
        This function prints the information about a particular job
        """
        print_console()
        print_console(job.center(80, "="))
        print_console()
        print_console(" last state: %s" % (jobs[job].state))
        parse_submit_file(jobs[job])

        # Handle subdag jobs from the dag file
        if self.jobs[job].is_subdag is True:
            print_console(" This is a SUBDAG job:")
            print_console(" For more information, please run the following command:")
            user_cmd = " %s -s " % (prog_base)
            if output_dir is not None:
                user_cmd = user_cmd + " --output-dir %s" % (output_dir)
            print_console(f"{user_cmd} -f {self.jobs[job].dag_path}")
            print_console()
            return

        sub_wf_cmd = None
        if self.jobs[job].sub_file_parsed is False:
            print_console("       site: submit file not available")
        else:
            print_console("       site: %s" % (self.jobs[job].site or "-"))
        print_console("submit file: %s" % (self.jobs[job].sub_file))
        print_console("output file: %s" % (self.find_latest_log(self.jobs[job].out_file)))
        print_console(" error file: %s" % (self.find_latest_log(self.jobs[job].err_file)))
        if print_invocation:
            print_console()
            print_console(
                "To re-run this job, use: %s %s"
                % (self.jobs[job].executable, self.jobs[job].arguments)
            )
            print_console()
        if print_pre_script and len(self.jobs[job].pre_script) > 0:
            print_console()
            print_console("SCRIPT PRE:")
            print_console(self.jobs[job].pre_script)
            print_console()
        if len(self.jobs[job].dagman_out) > 0:
            # This job has a sub workflow
            user_cmd = " %s" % (prog_base)
            if output_dir is not None:
                user_cmd = user_cmd + " --output-dir %s" % (output_dir)

            # get any options that need to be invoked for the sub workflow
            extraOptions = addon(options)
            sub_wf_cmd = "{} {} -d {}".format(
                user_cmd, extraOptions, os.path.split(self.jobs[job].dagman_out)[0],
            )

            if not recurse_mode:
                print_console(" This job contains sub workflows!")
                print_console(" Please run the command below for more information:")
                print_console(sub_wf_cmd)

            print_console()
        print_console()

        # Now dump file contents to screen if we are not in quiet mode
        if not quiet_mode:
            self.print_output_error(self.jobs[job])

        # recurse for sub workflow
        if sub_wf_cmd is not None and recurse_mode:
            print_console(("Failed Sub Workflow").center(80, "="))
            subprocess.Popen(sub_wf_cmd, shell=True).communicate()[0]
            print_console(("").center(80, "="))
            
            
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

        while sf < MAXLOGFILE:
            curr_log = log_file_base + ".%03d" % (sf)
            if os.access(curr_log, os.F_OK):
                last_log = curr_log
                sf = sf + 1
            else:
                break

        return last_log
            

    def print_output_error(self, job):
        """
        This function outputs both output and error files for a given job.
        """
        out_file = self.find_latest_log(job.out_file)
        err_file = self.find_latest_log(job.err_file)

        my_parser = kickstart_parser.Parser(out_file)
        my_output = my_parser.parse_stdout_stderr()
        my_task_id = 0

        if len(my_output) > 0:
            # Ok, we got valid kickstart records, output stdout and stderr for tasks that failed
            for entry in my_output:
                # Count tasks, the same way as pegasus-monitord for Stampede
                my_task_id = my_task_id + 1
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
                        logger.warn("couldn't convert exitcode to integer!")
                        continue
                else:
                    # We must have "error" in entry
                    pass
                # Got a task with a non-zero exitcode
                print_console(("Task #" + str(my_task_id) + " - Summary").center(80, "-"))
                print_console()
                if "resource" in entry:
                    print_console("site        : %s" % (entry["resource"]))
                if "hostname" in entry:
                    print_console("hostname    : %s" % (entry["hostname"]))
                if "name" in entry:
                    print_console("executable  : %s" % (entry["name"]))
                if "argument-vector" in entry:
                    print_console("arguments   : %s" % (entry["argument-vector"]))
                if "exitcode" in entry:
                    print_console("exitcode    : %s" % (entry["exitcode"]))
                else:
                    if "error" in entry:
                        print_console("error       : %s" % (entry["error"]))
                if "cwd" in entry:
                    print_console("working dir : %s" % (entry["cwd"]))
                print_console()
                # Now let's display stdout and stderr
                if "stdout" in entry:
                    if len(entry["stdout"]) > 0:
                        # Something to display
                        print_console(
                            (
                                "Task #"
                                + str(my_task_id)
                                + " - "
                                + entry["transformation"]
                                + " - "
                                + entry["derivation"]
                                + " - stdout"
                            ).center(80, "-")
                        )
                        print_console()
                        print_console(entry["stdout"])
                        print_console()
                if "stderr" in entry:
                    if len(entry["stderr"]) > 0:
                        # Something to display
                        print_console(
                            (
                                "Task #"
                                + str(my_task_id)
                                + " - "
                                + entry["transformation"]
                                + " - "
                                + entry["derivation"]
                                + " - stderr"
                            ).center(80, "-")
                        )
                        print_console()
                        print_console(entry["stderr"])
                        print_console()
        else:
            # Not able to parse the kickstart output file, let's just dump the out and err files

            # Print outfile to screen
            self.dump_file(out_file)

            # Print errfile to screen
            self.dump_file(err_file)

        
    def dump_file(self, file):
        """
        This function dumps a file to our stdout
        """
        if file is not None:
            try:
                OUT = open(file)
            except Exception:
                logger.warn("*** Cannot access: %s" % (file))
                print_console()
            else:
                print_console(os.path.split(file)[1].center(80, "-"))
                print_console()
                # Dump file contents to terminal
                line = OUT.readline()
                while line:
                    line = line.strip()
                    print_console(line)
                    line = OUT.readline()

                OUT.close()
                print_console()
            

class AnalyzeDB:

    def analyze_db(self, config_properties):
        """
        This function runs the analyzer using data from the database.
        """
        global total, success, failed, unsubmitted, unknown, held

        # Get the database URL
        try:
            output_db_url = connection.url_by_submitdir(
                input_dir, connection.DBType.WORKFLOW, config_properties, top_dir
            )
            wf_uuid = connection.get_wf_uuid(input_dir)
        except connection.ConnectionError as e:
            raise AnalyzerError(
                "Unable to connect to database for workflow in %s", input_dir, e
            )

        # Nothing to do if we cannot resolve the database URL
        if output_db_url is None:
            raise ValueError("Database URL is required")

        # Now, let's try to access the database
        workflow_stats = None
        try:
            workflow_stats = stampede_statistics.StampedeStatistics(output_db_url, False)
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
                "Unable to determine the database id for workflow with uuid %s" % wf_uuid
            )

        wf_ids = []
        if traverse_all:
            wf_ids.extend(descendant_wfs_ids)
        else:
            # we only do the workflow on which submit directory analyzer is called
            wf_ids.append(wf_id)

        for wf_id in wf_ids:
            wf_detail = wf_details[wf_id]
            logger.debug(
                "Running analyzer on workflow with database id {} in submit dir {}".format(
                    wf_id, wf_detail.submit_dir
                )
            )

            failed = 0
            workflow_stats = None
            try:
                # for each wf initialize a new workflow stats object
                workflow_stats = stampede_statistics.StampedeStatistics(
                    output_db_url, False
                )
                workflow_stats.initialize(root_wf_id=wf_id)
                self.analyze_db_for_wf(
                    workflow_stats, wf_detail.wf_uuid, wf_detail.submit_dir,
                )
            except DBAdminError as e:
                raise AnalyzerError("Failed to load the database - " + output_db_url, e)
            except WorkflowFailureError as e:
                logger.error(e)
                failed += 1
            finally:
                if workflow_stats:
                    workflow_stats.close()

        # TODO: throw exceptions. catch them and determine exitcode
        if failed > 0:
            raise WorkflowFailureError("One or more workflows failed")



    def analyze_db_for_wf(self, workflow_stats, wf_uuid, submit_dir):
        """
        This function runs the analyzer using data from the database.
        :param workflow_stats: the stampede statistics object initialized with the workflow, we want to analyze
        :param wf_uuid:  the uuid of the workflow we are analyzing
        :param submit_dir: the submit dir for the workflow
        :return:
        """
        global total, success, failed, unsubmitted, unknown, held

        total = workflow_stats.get_total_jobs_status()
        total_success_failed = workflow_stats.get_total_succeeded_failed_jobs_status()
        success = total_success_failed.succeeded
        failed = total_success_failed.failed

        held_jobs = workflow_stats.get_total_held_jobs()
        held = len(held_jobs)

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
            "Workflow state determined from workflow database is %s" % workflow_status.value
        )

        # PM-1039
        if success is None:
            success = 0
        if failed is None:
            failed = 0

        unsubmitted = total - success - failed

        # PM-1762 add a helpful message in case failed jobs are zero and workflow failed
        if workflow_status is WORKFLOW_STATUS.FAILURE and failed == 0:
            print_console(
                "It seems your workflow failed with zero failed jobs. Please check the dagman.out and the monitord.log file in %s"
                % (input_dir or top_dir)
            )

        # Let's print the results
        print_top_summary(workflow_status, submit_dir)
        check_for_wf_start()

        # Exit if summary mode is on
        if summary_mode:
            if workflow_status is WORKFLOW_STATUS.FAILURE or failed > 0:
                # Workflow has failures, exit with exitcode 2
                raise WorkflowFailureError(
                    "Workflow Failed ", wf_uuid=wf_uuid, submit_dir=submit_dir
                )
            # Workflow has no failures, exit with exitcode 0
            return

        # PM-1126 print information about held jobs
        if held > 0:
            # Print header
            print_console("Held jobs' details".center(80, "*"))
            print_console()

            for held_job in held_jobs:
                # each tuple is max_ji_id, jobid, jobname, reason
                # first two are database id's for debugging
                print_console(held_job.jobname.center(80, "="))
                print_console()
                print_console(
                    "submit file            : %s" % (held_job.jobname + ".sub" or "-")
                )
                print_console("last_job_instance_id   : %s" % (held_job[0] or "-"))
                print_console("reason                 : %s" % (held_job.reason or "-"))
                print_console()

        # PM-1890 print failing jobs before failed jobs
        if len(failing_jobs) > 0:
            print_console("Failing jobs' details".center(80, "*"))
            for i in range(len(failing_jobs)):
                failing_jobs[i] = failing_jobs[i]._asdict()
                # print(json.dumps(failing_jobs[i]))
                failing_job_id = failing_jobs[i]["job_instance_id"]
                job_tasks = workflow_stats.get_invocation_info(failing_job_id)
                self.print_job_instance(
                    failing_job_id,
                    workflow_stats.get_job_instance_info(failing_job_id)[0],
                    job_tasks,
                )

        # Now, print information about jobs that failed...
        if failed > 0:
            # Get list of failed jobs from database
            failed_jobs = workflow_stats.get_failed_job_instances()

            # Print header
            print_console("Failed jobs' details".center(80, "*"))

            # Now process one by one...
            for my_job in failed_jobs:
                job_instance_info = workflow_stats.get_job_instance_info(my_job[0])[0]
                job_tasks = workflow_stats.get_invocation_info(my_job[0])

                sub_wf_cmd = self.print_job_instance(
                    job_instance_info.job_instance_id,
                    workflow_stats.get_job_instance_info(job_instance_info.job_instance_id)[
                        0
                    ],
                    job_tasks,
                )

                # recurse for sub workflow
                if sub_wf_cmd is not None and recurse_mode:
                    print_console(("Failed Sub Workflow").center(80, "="))
                    subprocess.Popen(sub_wf_cmd, shell=True).communicate()[0]
                    print_console(("").center(80, "="))

        if workflow_status is WORKFLOW_STATUS.FAILURE or failed > 0:
            # Workflow has failures, exit with exitcode 2
            raise WorkflowFailureError(
                "Workflow Failed ", wf_uuid=wf_uuid, submit_dir=submit_dir
            )

        # Workflow has no failures, exit with exitcode 0
        return
     

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
                WORKFLOW_STATUS.SUCCESS if last_wf_status == 0 else WORKFLOW_STATUS.FAILURE
            )
        else:
            raise ValueError("Invalid worklfow state %s" % last_wf_state)

        return workflow_status


    def print_job_instance(self, job_instance_id, job_instance_info, job_tasks):
        """
        The function prints information related to one job instance retried from the
        stampede database
        :param job_instance_id the job_instance_id from the job_instance table
        :param job_instance_info: the job instance with which the tasks are associated
        :param job_tasks: all the tasks (invocation records) for a particular condor job/job instance
        :return subwf_cmd: if the job is a sub workflow job, then returns the command that is
                           required to invoke pegasus-analyzer on the sub workflow.
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
        if print_invocation:
            print_console()
            print_console(
                "To re-run this job, use: %s %s"
                % ((job_instance_info.executable or "-"), (job_instance_info.argv or "-"))
            )
            print_console()
        if print_pre_script and len(job_instance_info.pre_executable or "") > 0:
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
        if job_instance_info.subwf_dir is not None:
            # This job has a sub workflow
            user_cmd = " %s" % (prog_base)
            my_wfdir = os.path.normpath(job_instance_info.subwf_dir)
            if my_wfdir.find(job_instance_info.submit_dir) >= 0:
                # Path to dagman_out file includes original submit_dir, let's try to change it...
                my_wfdir = os.path.normpath(
                    my_wfdir.replace((job_instance_info.submit_dir + os.sep), "", 1)
                )
                my_wfdir = os.path.join(input_dir, my_wfdir)

            # get any options that need to be invoked for the sub workflow
            extraOptions = addon(options)
            sub_wf_cmd = "{} {} -d {} --top-dir {}".format(
                user_cmd, extraOptions, my_wfdir, (top_dir or input_dir),
            )
            if not recurse_mode:
                # we print only if recurse mode is disabled
                print_console(" This job contains sub workflows!")
                print_console(" Please run the command below for more information:")
                print_console(sub_wf_cmd)
                # print "%s -d %s --top-dir %s" % (user_cmd, my_wfdir, (top_dir or input_dir))
            print()
        print()

        # print all the tasks associated with the job
        self.print_tasks_info(job_instance_id, job_instance_info, job_tasks)
        return sub_wf_cmd


    def print_tasks_info(self, job_instance_id, job_instance_info, job_tasks):
        """
           The function prints information related to one job instance retried from the
           stampede database
           :param job_instance_id: the job_instance_id from the job_instance table
           :param job_instance_info: the job instance with which the tasks are associated
           :param job_tasks: all the tasks (invocation records) for a particular condor job/job instance
           :return:
           """

        # Unquote stdout and stderr
        ji_stdout_text = utils.unquote(job_instance_info.stdout_text or "").decode("UTF-8")
        ji_stderr_text = utils.unquote(job_instance_info.stderr_text or "").decode("UTF-8")
        ji_stdout_text = ji_stdout_text.strip(" \n\r\t")
        ji_stderr_text = ji_stderr_text.strip(" \n\r\t")

        some_tasks_failed = False
        for my_task in job_tasks:
            # PM-798 we want to detect if some tasks failed or not
            if my_task[0] < -1:
                # Skip only post script tasks. Pre script invocations have task_submit_seq as -1
                continue
            some_tasks_failed = some_tasks_failed or (my_task[1] != 0)

        # PM-798 track whether we need to actually print the condor job stderr or not
        # we only print if there is no information in the kickstart record
        my_print_job_stderr = True

        # Now, print task information
        for my_task in job_tasks:
            if my_task[0] < -1:
                # Skip only post script tasks. Pre script invocations have task_submit_seq as -1
                continue

            if my_task[1] == 0 and some_tasks_failed:
                # Skip tasks that succeeded only if we know some tasks did fail
                continue

            # Got a task with a non-zero exitcode
            my_exitcode = utils.raw_to_regular(my_task[1])

            # Print task summary
            print_console(("Task #" + str(my_task[0]) + " - Summary").center(80, "-"))
            print_console()
            print_console("site        : %s" % (job_instance_info.site or "-"))
            print_console("hostname    : %s" % (job_instance_info.hostname or "-"))
            print_console("executable  : %s" % (str(my_task[2] or "-")))
            print_console("arguments   : %s" % (str(my_task[3] or "-")))
            print_console("exitcode    : %s" % (str(my_exitcode)))
            print_console("working dir : %s" % (job_instance_info.work_dir or "-"))
            print_console()

            if not quiet_mode:
                # Now, print task stdout and stderr, if anything is there
                my_stdout_str = "#@ %d stdout" % (my_task[0])
                my_stderr_str = "#@ %d stderr" % (my_task[0])

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
                        print_console(
                            (
                                "Task #"
                                + str(my_task[0])
                                + " - "
                                + str(my_task[4])
                                + " - "
                                + str(my_task[5])
                                + " - stdout"
                            ).center(80, "-")
                        )
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
                                + str(my_task[0])
                                + " - "
                                + str(my_task[4])
                                + " - "
                                + str(my_task[5])
                                + " - Kickstart stderr"
                            ).center(80, "-")
                        )
                        print_console()
                        print_console(ji_stdout_text[my_stderr_start:my_stderr_end])
                        print_console()

                # PM-808 print jobinstance stdout for prescript failures only.
                if my_task[0] == -1 and ji_stdout_text is not None:
                    print_console(
                        (
                            "Task #"
                            + str(my_task[0])
                            + " - "
                            + str(my_task[4])
                            + " - "
                            + str(my_task[5])
                            + " - stdout"
                        ).center(80, "-")
                    )
                    print_console()
                    print_console(ji_stdout_text)
                    print_console()

        # Now print the stderr output from the .err file
        if ji_stderr_text.strip("\n\t \r") != "" and my_print_job_stderr:
            # Something to display
            # if task exitcode is 0, it should indicate PegasusLite case compute job succeeded but stageout failed
            print_console(
                ("Job stderr file - %s" % (job_instance_info.stderr_file or "-")).center(
                    80, "-"
                )
            )
            print_console()
            print_console(ji_stderr_text)
            print_console()


    
class DebugWF:

    def debug_workflow(self):
        """
        This function handles the mode where the analyzer
        is used to debug a job in a workflow
        """
        global debug_job, debug_dir

        # Check if we can find this job's submit file
        if not debug_job.endswith(".sub"):
            debug_job = debug_job + ".sub"
        # Figure out job name
        jobname = os.path.basename(debug_job)
        jobname = jobname[0 : jobname.find(".sub")]
        # Create job class
        my_job = Job(jobname)
        my_job.sub_file = debug_job

        if not os.access(debug_job, os.R_OK):
            raise AnalyzerError(f"cannot access job submit file: {debug_job}")

        # Handle the temporary directory option
        if debug_dir is None:
            # Create temporary directory
            try:
                debug_dir = tempfile.mkdtemp()
            except Exception:
                raise AnalyzerError(f"could not create temporary directory!")

        else:
            # Make sure directory specified is writable
            debug_dir = os.path.abspath(debug_dir)
            if not os.access(debug_dir, os.F_OK):
                # Create directory if it does not exist
                try:
                    os.mkdir(debug_dir)
                except Exception:
                    logger.error("cannot create debug directory: %s" % (debug_dir))

            # Check if we can write to the debug directory
            if not os.access(debug_dir, os.W_OK):
                raise AnalyzerError(f"not able to write to temporary directory: {debug_dir}")

        # Handle workflow type
        if workflow_type is not None:
            if workflow_type.lower() == "condor":
                logger.info("debugging condor type workflow")
                self.debug_condor(my_job)
            else:
                raise AnalyzerError(f"workflow type {workflow_type} not supported!")
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
        global strict_mode

        # Set strict mode in order to parse everything in the submit file
        strict_mode = 1
        # Parse submit file
        parse_submit_file(my_job)

        # Create script name
        debug_script_basename = "debug_" + my_job.name + ".sh"
        debug_script_err_basename = "debug_" + my_job.name + ".err"
        debug_script_name = os.path.join(debug_dir, debug_script_basename)

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

            debug_script.write("export pegasus_lite_work_dir=%s" % debug_dir)
            debug_script.write("\n")
            debug_script.write("# Copy any files that are needed\n")

            debug_script.write('echo "copying input files..."\n')
            debug_script.write("\n")
            # Copy all files that we need
            for my_file in my_job.transfer_input_files.split(","):
                if len(my_file):
                    if len(my_job.initial_dir):
                        # Add the initial dir to all files to be copied
                        my_file = os.path.join(my_job.initial_dir, my_file)
                    debug_script.write(f"cp {my_file} {debug_dir}\n")

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
                    "chmod +x %s\n" % (os.path.join(debug_dir, my_job.executable))
                )
                debug_script.write("\n")
            else:
                # generate a separate sh file that parts of pegasus-lite script
                debug_wrapper = self.generate_pegasus_lite_debug_wrapper(
                    job_pegasus_lite_wrapper
                )
                job_executable = debug_wrapper

            job_executable = os.path.join(debug_dir, job_executable) + my_job.arguments

            debug_script.write('echo "executing job: %s"\n' % (job_executable))
            debug_script.write("\n")
            debug_script.write("# Now, execute the job\n")
            # disable fail on error before launching
            debug_script.write("set +e\n")
            debug_script.write("%s" % (job_executable))

            # redirect stderr for pegasus lite jobs to separate err file
            if job_pegasus_lite_wrapper is not None:
                debug_script.write(
                    " 2> " + os.path.join(debug_dir, debug_script_err_basename)
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
            raise AnalyzerError(f"cannot change permissions for the debug script {debug_script}")

        # Print next step
        print()
        print("%s: finished generating job debug script!" % (prog_base))
        print()
        print("To run it, you need to type:")
        print("   $ cd %s" % (debug_dir))
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
            my_job.sub_file = os.path.join(input_dir, my_job.name + ".sub")

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
            debug_dir, "stripped_pl_" + os.path.basename(pegasus_lite_wrapper)
        )
        try:
            DEBUG_WRAPPER = open(debug_wrapper, "w")
            WRAPPER = open(pegasus_lite_wrapper)
            for line in WRAPPER:
                if line.startswith("# stage out"):
                    # we only continue till we hit the stage out part
                    break

                # line = line.strip(" \t") # Remove leading and trailing spaces
                if debug_job_local_executable is not None:
                    # enable matching to replace the kickstart invocation with local path
                    ks_invocation = re_ks_invocation.search(line)

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
                            + debug_job_local_executable
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
    

# --- Common methods used by all classes above ----------------------------------------------------------------    
    
def check_for_wf_start():
    """
    This function checks if workflow did start.
    If not then print helpful message
    :return:
    """
    global total, success, failed, unsubmitted, unknown

    if unsubmitted == total:
        # PM-1039 either the workflow did not start or other errors
        # check the dagman.out file
        print_console(" Looks like workflow did not start".center(80, "*"))
        print_console()
        if input_dir is not None:
            dagman_out = backticks(
                "ls " + input_dir + "/*.dag.dagman.out" + " 2>/dev/null"
            )

            if dagman_out is not None and dagman_out != "":
                nfs_error_string = backticks(
                    'grep -i ".*Error.*NFS$" ' + dagman_out + " 2>/dev/null"
                )
                if nfs_error_string is not None and nfs_error_string != "":
                    header = " Error detected in *.dag.dagman.out "
                    print_console(header.center(80, "="))
                    print_console(
                        " HTCondor DAGMan NFS ERROR condition detected in " + dagman_out
                    )
                    print_console(" " + nfs_error_string)
                    print_console(
                        " HTCondor DAGMan expects submit directories to be NOT NFS mounted"
                    )
                    print_console(
                        " Set your submit directory to a directory on the local filesystem OR "
                    )
                    print_console(
                        "    Set HTCondor configuration CREATE_LOCKS_ON_LOCAL_DISK and ENABLE_USERLOG_LOCKING to True. Check HTCondor documentation for further details."
                    )
                    print_console()

            # PM-1040 check for dagman.lib.err
            dagman_lib_err = backticks(
                "ls " + input_dir + "/*.dag.lib.err" + " 2>/dev/null"
            )
            if dagman_lib_err is not None and dagman_lib_err != "":
                dagman_lib_err_contents = backticks(
                    "cat " + dagman_lib_err + " 2>/dev/null"
                )
                if (
                    dagman_lib_err_contents is not None
                    and dagman_lib_err_contents != ""
                ):
                    header = " Error detected in *.dag.lib.err "
                    print_console(header.center(80, "="))
                    print_console(" Contents of " + dagman_lib_err)
                    print_console(" " + dagman_lib_err_contents)
                    
    
def print_top_summary(workflow_status=None, submit_dir=input_dir):
    """
    This function prints the summary for the analyzer report,
    which is the same for the long and short output versions
    """
    submit_dir=input_dir
    print_console()
    summary = "Summary".center(80, "*")
    print_console(summary)
    print_console()
    print_console(" Submit Directory   : %s" % (submit_dir))
    if workflow_status:
        print_console(" Workflow Status    : %s" % (workflow_status.value))

    print_console(
        " Total jobs         : % 6d (%3.2f%%)"
        % (total, 100 * (1.0 * total / (total or 1)))
    )
    print_console(
        " # jobs succeeded   : % 6d (%3.2f%%)"
        % (success, 100 * (1.0 * success / (total or 1)))
    )
    print_console(
        " # jobs failed      : % 6d (%3.2f%%)"
        % (failed, 100 * (1.0 * failed / (total or 1)))
    )
    print_console(
        " # jobs held        : % 6d (%3.2f%%)"
        % (held, 100 * (1.0 * held / (total or 1)))
    )
    print_console(
        " # jobs unsubmitted : % 6d (%3.2f%%)"
        % (unsubmitted, 100 * (1.0 * unsubmitted / (total or 1)))
    )
    if unknown > 0:
        print_console(
            " # jobs unknown     : % 6d (%3.2f%%)"
            % (unknown, 100 * (1.0 * unknown / (total or 1)))
        )
    print()


def print_console(stmt=""):
    """
    A utilty function to print to console with the correct indentation
    """
    print(indent + stmt)
    

def backticks(cmd_line):
    """
    what would a python program be without some perl love?
    """
    o = subprocess.Popen(cmd_line, shell=True, stdout=subprocess.PIPE).communicate()[0]
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


def parse_submit_file(my_job):
    """
    This function opens a submit file and reads site
    and condor dagman log information
    """
    # First we check if this is a SUBDAG job from the dag file
    if my_job.is_subdag:
        # Nothing to do here
        return

    if my_job.sub_file == "":
        # Create full path for the submit file if we already don't have the sub file set up
        my_job.sub_file = os.path.join(input_dir, my_job.name + ".sub")
    my_job.out_file = os.path.join(input_dir, my_job.name + ".out")
    my_job.err_file = os.path.join(input_dir, my_job.name + ".err")

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
            prop = re_parse_property.search(line)
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
                        sub_prop = re_parse_property.search(sub_prop_line)
                        if sub_prop:
                            if sub_prop.group(1) == "_CONDOR_DAGMAN_LOG":
                                my_job.dagman_out = sub_prop.group(2)
                                my_job.dagman_out = os.path.normpath(my_job.dagman_out)
                                if my_job.dagman_out.find(workflow_base_dir) >= 0:
                                    # Path to dagman_out file includes original submit_dir, let's try to change it
                                    my_job.dagman_out = os.path.normpath(
                                        my_job.dagman_out.replace(
                                            (workflow_base_dir + os.sep), "", 1
                                        )
                                    )
                                    # Join with current input_dir
                                    my_job.dagman_out = os.path.join(
                                        input_dir, my_job.dagman_out
                                    )

                                    # Now, figure out the correct directory, accounting for
                                    # replanning and rescue modes

                                    # Split filename into dir and base names
                                    my_dagman_dir = os.path.dirname(my_job.dagman_out)
                                    my_dagman_file = os.path.basename(my_job.dagman_out)
                                    my_retry = my_job.retries
                                    if my_retry is None:
                                        logger.warning(
                                            "sub-workflow retry counter not initialized... continuing..."
                                        )
                                        continue

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
                                                "sub-workflow directory %s does not exist!"
                                                % (my_retry_dir)
                                            )
                                            continue

                                    # Found sub-workflow directory, let's compose the final path to the new dagman.out file...
                                    my_job.dagman_out = os.path.join(
                                        my_retry_dir, my_dagman_file
                                    )

                # Only parse following keys if we are running in strict mode
                if strict_mode:
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
                        v = re_collapse_condor_subs.sub("", v)

                        # Make sure we have an absolute path
                        if not os.path.isabs(v):
                            v = os.path.join(input_dir, v)

                        # Done! Replace out/err filenames with what we have
                        if k == "output":
                            my_job.out_file = v
                        else:
                            my_job.err_file = v
                # Only parse following keys if we are debugging a job
                if debug_mode:
                    # Get transfer input files and output files
                    if k == "transfer_input_files":
                        my_job.transfer_input_files = v

                    if k == "transfer_output_files":
                        my_job.transfer_output_files = v
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
