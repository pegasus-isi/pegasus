#!/usr/bin/env python3

"""
Logging daemon process to update the jobstate.log file from DAGMan logs.
This program is to be run automatically by the pegasus-run command.

Usage: pegasus-monitord [options] dagoutfile
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

import atexit
import calendar
import datetime
import errno
import logging
import optparse
import os
import re
import shelve
import signal
import sys
import time
import traceback

from Pegasus.db import connection
from Pegasus.monitoring import event_output as eo
from Pegasus.monitoring import notifications
from Pegasus.monitoring.workflow import MONITORD_RECOVER_FILE, Workflow
from Pegasus.tools import properties, utils

# Save our own basename
prog_base = os.path.split(sys.argv[0])[1]


utils.configureLogging()

root_logger = logging.getLogger()
logger = logging.getLogger("pegasus-monitord")

# Ordered logging levels
_LEVELS = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, logging.TRACE]

logger.info("pegasus-monitord starting - pid %d " % (os.getpid()))

# Add SEEK_CUR to os if Python version < 2.5
if sys.version_info < (2, 5):
    os.SEEK_CUR = 1

re_parse_dag_name = re.compile(r"Parsing (.+) ...$")
re_parse_timestamp = re.compile(
    r"\s*(\d{1,2})\/(\d{1,2})(\/(\d{1,2}))?\s+(\d{1,2}):(\d{2}):(\d{2})"
)
re_parse_iso_stamp = re.compile(
    r"^\s*(\d{4}).?(\d{2}).?(\d{2}).(\d{2}).?(\d{2}).?(\d{2})([.,]\d+)?([Zz]|[-+](\d{2}).?(\d{2}))"
)
re_parse_event = re.compile(
    r"Event:\s+ULOG_(\S+) for (?:HT|)Condor (?:Job|Node) (\S+)\s+\((-?[0-9]+\.[0-9]+)(\.[0-9]+)?\).*"
)
re_parse_script_running = re.compile(
    r"\d{2}\s(?:\(D_\w*\)\s)?Running (PRE|POST) script of (?:Job|Node) (.+)\.{3}"
)
re_parse_script_done = re.compile(
    r"\d{2}\s(?:\(D_\w*\)\s)?(PRE|POST) Script of (?:Job|[nN]ode) (\S+)"
)
re_parse_script_successful = re.compile(r"completed successfully\.$")
re_parse_script_failed = re.compile(r"failed with status\s+(-?\d+)\.?$")
re_parse_job_submit = re.compile(r"Submitting (?:HT|)Condor Node (.+) job")
re_parse_job_submit_error = re.compile(r"ERROR: submit attempt failed")
re_parse_job_failed = re.compile(
    r"\d{2}\s(?:\(D_\w*\)\s)?Node (\S+) job proc \(([0-9\.]+)\) failed with (status|signal)\s+(-?\d+)\.$"
)
re_parse_job_successful = re.compile(
    r"\d{2}\s(?:\(D_\w*\)\s)?Node (\S+) job proc \(([0-9\.]+)\) completed successfully\.$"
)
re_parse_retry = re.compile(r"Retrying node (\S+) \(retry \#(\d+) of (\d+)\)")
re_parse_dagman_condor_id = re.compile(
    r"\*\* condor_scheduniv_exec\.([0-9\.]+) \(CONDOR_DAGMAN\) STARTING UP"
)
re_parse_dagman_finished = re.compile(
    r"\(condor_DAGMAN\)[\w\s]+EXITING WITH STATUS (\d+)$"
)
re_parse_dagman_pid = re.compile(r"\*\* PID = (\d+)$")
re_parse_condor_version = re.compile(r"\*\* \$CondorVersion: ((\d+)\.(\d+)\.(\d+))")
re_parse_condor_logfile = re.compile(r"Condor log will be written to ([^,]+)")
re_parse_condor_logfile_insane = re.compile(r"\d{2}\s{3,}(\S+)")
re_parse_multiline_files = re.compile(r"All DAG node user log files:")
re_parse_dagman_aborted = re.compile(r"Received SIGUSR1")
re_parse_job_held = re.compile(r"\s*Hold reason:(.*)")

# Constants
MONITORD_WF_RETRY_FILE = (
    "monitord.subwf"  # filename for writing persistent sub-workflow retry information
)
MONITORD_LOG_FILE = "monitord.log"
MAX_SLEEP_TIME = 10  # in seconds
SLEEP_WAIT_NOTIFICATION = 5  # in seconds
DAGMAN_OUT_MAX_READ_SIZE = (
    32768  # at most the maximum bytes to read while parsing dagman.out file
)
DEFAULT_ENCODING = "bp"  # default way of encoding events generated

unsubmitted_events = {
    "UN_READY": 1,
    "PRE_SCRIPT_STARTED": 1,
    "PRE_SCRIPT_SUCCESS": 1,
    "PRE_SCRIPT_FAILURE": 1,
}

# Global variables
wfs = []  # list of workflow entries monitord is tracking
tracked_workflows = []  # list of workflows we have started tracking
wf_retry_dict = None  # File-based dictionary keeping track of sub-workflows retries, opened later...
follow_subworkflows = True  # Flag for tracking sub-workflows
root_wf_id = None  # Workflow id of the root workflow
replay_mode = 0  # disable checking if DAGMan's pid is gone
keep_state = (
    0  # Flag for keeping a Workflow's state across several DAGMan start/stop cycles
)
db_stats = False  # collect and print database stats at the end of execution
no_events = False  # Flag for disabling event output altogether
event_dest = None  # URL containing the destination of the events
dashboard_event_dest = (
    None  # URL containing the destination of events for the dashboard
)
encoding = None  # Way to encode the data
monitord_exit_code = 0  # Exit code for pegasus-monitord
do_notifications = True  # Flag to enable notifications
skip_pid_check = False  # Flag to skip checking if a previous monitord is still running using the pid file
monitord_notifications = None  # Notifications' manager class
max_parallel_notifications = 10  # Maximum number of notifications we can do in parallel
notifications_timeout = (
    0  # Time to wait for notification scripts to finish (0 means wait forever)
)
store_stdout_stderr = True  # Flag for storing jobs' stdout and stderr in our output
fast_start_mode = True  # Flag to indicate that only sleep once monitord has caught up with the dagman.out file
wf_event_sink = None  # Where wf events go
out = None  # .dag.dagman.out file from command-line
run = None  # run directory from command-line dagman.out file
output_dir = None  # output_dir for all files written by monitord
jsd = None  # location of jobstate.log file
millisleep = None  # emulated run mode delay
adjustment = 0  # time zone adjustment (@#~! Condor)

#
# --- at exit handlers -------------------------------------------------------------------
#


def delete_pid_file():
    """
    This function deletes the pid file when exiting.
    """
    try:
        os.unlink(pid_filename)
    except OSError:
        logger.error("cannot delete pid file %s" % (pid_filename))


def close_wf_retry_file():
    """
    This function closes the persistent storage file containing sub-workflow retry information.
    """
    if wf_retry_dict is not None:
        wf_retry_dict.close()


def finish_notifications():
    """
    This function flushes all notifications, and closes the
    notifications' log file. It also logs all pending (but not yet
    issued) notifications.
    """
    if monitord_notifications is not None:
        monitord_notifications.finish_notifications()


def finish_stampede_loader():
    """
    This function is called by the atexit module when monitord exits.
    It is used to make sure the loader has finished loading all data
    into the database. It will also produce stats for benchmarking.
    """
    logger.info("DB flushing beginning")
    sinks = [wf_event_sink, dashboard_event_sink]
    for sink in sinks:
        if sink is not None:
            try:
                if db_stats and root_logger.getEffectiveLevel() > logging.INFO:
                    # Make sure log level is enough to display database
                    # benchmarking information
                    root_logger.setLevel(logging.INFO)
                sink.close()
            except Exception:
                logger.warning(
                    "could not call the finish method "
                    + "in the nl loader class... exiting anyway"
                )
                logger.warning(traceback.format_exc())
    logger.info("DB flushing ended")


class WorkflowEntry:
    """
    Class used to store one workflow entry
    """

    run_dir = None  # Run directory for the workflow
    dagman_out = None  # Location of the dagman.out file
    n_retries = 0  # Number of retries for looking for the dagman.out file
    wf = None  # Pointer to the Workflow class for this Workflow
    DMOF = None  # File pointer once we open the dagman.out file
    ml_buffer = ""  # Buffer for reading the dagman.out file
    ml_retries = 0  # Keep track of how many times we have looked for new content
    ml_current = 0  # Keep track of where we are in the dagman.out file
    delete_workflow = False  # Flag for dropping this workflow
    sleep_time = None  # Time to sleep for this workflow
    caught_up_with_dagman_out = (
        False  # indicates monitord has caught up with the dagman.out file
    )
    dagman_out_appeared = False  # indicates whether dagman.out file has appeared or not


# Parse command line options
prog_usage = "usage: %s [options] workflow.dag.dagman.out" % (prog_base)
prog_desc = """Mandatory arguments: outfile is the log file produced by Condor DAGMan, usually ending in the suffix ".dag.dagman.out"."""

parser = optparse.OptionParser(usage=prog_usage, description=prog_desc)

parser.add_option(
    "-a",
    "--adjust",
    action="store",
    type="int",
    dest="adjustment",
    help="adjust for time zone differences by i seconds, default 0",
)
parser.add_option(
    "-N",
    "--condor-daemon",
    action="store_const",
    default=False,
    dest="condor_daemon",
    help="Condor daemon mode. Used by pegasus-dagman.",
)
parser.add_option(
    "-j",
    "--job",
    action="store",
    type="string",
    dest="jsd",
    help="Alternative job state file to write, default is %s in the workflow's directory"
    % (utils.jobbase),
)
parser.add_option(
    "-o",
    "--output-dir",
    action="store",
    type="string",
    dest="output_dir",
    help="provides an output directory for all monitord log files",
)
parser.add_option(
    "--conf",
    action="store",
    type="string",
    dest="config_properties",
    help="specifies the properties' file to use. This option overrides all other property files.",
)
parser.add_option(
    "--no-recursive",
    action="store_const",
    const=1,
    dest="disable_subworkflows",
    help="disables pegasus-monitord to automatic follow any sub-workflows that are found",
)
parser.add_option(
    "--no-database",
    "--nodatabase",
    "--no-events",
    action="store_const",
    const=0,
    dest="no_events",
    help="turn off event generation completely, and overrides the URL in the -d option",
)
parser.add_option(
    "--no-notifications",
    "--no-notification",
    action="store_const",
    const=0,
    dest="no_notify",
    help="turn off notifications completely",
)
parser.add_option(
    "--notifications-max",
    action="store",
    type="int",
    dest="notifications_max",
    help="maximum number of concurrent notification concurrent notification scripts, 0 disable notifications, default is %d"
    % (max_parallel_notifications),
)
parser.add_option(
    "--notifications-timeout",
    action="store",
    type="int",
    dest="notifications_timeout",
    help="time to wait for notification scripts to finish before terminating them, 0 allows scripts to run indefinitely",
)
parser.add_option(
    "-S",
    "--sim",
    action="store",
    type="int",
    dest="millisleep",
    help="Developer: simulate delays between reads by sleeping ms milliseconds",
)
parser.add_option(
    "-r",
    "--replay",
    action="store_const",
    const=1,
    dest="replay_mode",
    help="disables checking for DAGMan's pid while running %s" % (prog_base),
)
parser.add_option(
    "--db-stats",
    action="store_true",
    dest="db_stats",
    help="collect and print database stats at the end",
)
parser.add_option(
    "--keep-state",
    action="store_const",
    const=1,
    dest="keep_state",
    help="keep state across several DAGMan start/stop cycles (development option)",
)
parser.add_option(
    "--skip-stdout",
    action="store_const",
    const=0,
    dest="skip_stdout",
    help="disables storing both stdout and stderr in our output",
)
parser.add_option(
    "-f",
    "--force",
    action="store_const",
    const=1,
    dest="skip_pid_check",
    help="runs pegasus-monitord even if it detects a previous instance running",
)
parser.add_option(
    "-v",
    "--verbose",
    action="count",
    default=0,
    dest="vb",
    help="Increase verbosity, repeatable",
)
parser.add_option(
    "--fast-start",
    action="store_true",
    dest="fast_start",
    help="catch up with all dagman.out files before sleeping intermittently",
)
grp = optparse.OptionGroup(parser, "Output options")
grp.add_option(
    "-d",
    "--dest",
    action="store",
    dest="event_dest",
    metavar="PATH or URL",
    help="Output destination URL [<scheme>]<params>, where "
    "scheme = [empty] | x-tcp:// | DB-dialect+driver://. "
    "For empty scheme, params are a file path with '-' meaning standard output. "
    "For x-tcp scheme, params are TCP host[:port=14380]. "
    "For DB, use SQLAlchemy engine URL. "
    "(default=sqlite:///<dagman-output-file>.stampede.db)",
    default=None,
)
grp.add_option(
    "-e",
    "--encoding",
    action="store",
    dest="enc",
    metavar="FORMAT",
    help="How to encode log events: bson | json| bp (default=bp)",
)
parser.add_option_group(grp)

# Parse command-line options
(options, args) = parser.parse_args()

# Remaining argument is .dag.dagman.out file
if len(args) != 1:
    parser.print_help()
    sys.exit(1)

out = args[0]

if not out.endswith(".dagman.out"):
    parser.print_help()
    sys.exit(1)

# Turn into absolute filename
out = os.path.abspath(out)

# Infer run directory
run = os.path.dirname(out)
if not os.path.isdir(run):
    logger.critical("Run directory %s does not exist" % run)
    exit(1)
os.chdir(run)

# Get the location of the properties file from braindump
top_level_wf_params = utils.slurp_braindb(run)
top_level_prop_file = None
if "properties" in top_level_wf_params:
    top_level_prop_file = top_level_wf_params["properties"]
    # Create the full path by using the submit_dir key from braindump
    if "submit_dir" in top_level_wf_params:
        top_level_prop_file = os.path.join(
            top_level_wf_params["submit_dir"], top_level_prop_file
        )

# Parse, and process properties
props = properties.Properties()
props.new(config_file=options.config_properties, rundir_propfile=top_level_prop_file)

# PM-948 check to see if any addon command line
# options are specified in properties
addon_cmd_prop = props.property("pegasus.monitord.arguments")
addon_cmd_options = []
if addon_cmd_prop is not None:
    addon_cmd_options = addon_cmd_prop.split(" ")

cmd_options = sys.argv[1:] + addon_cmd_options

# parse again with extra options that might
# been specified in the properties file
logger.info("Final Command line options are: %s" % cmd_options)
(options, args) = parser.parse_args(cmd_options)

# Set logging level
if options.vb <= 0:
    lvl = logging.INFO
elif options.vb == 1:
    lvl = logging.DEBUG
else:
    lvl = logging.TRACE
root_logger.setLevel(lvl)

# Resolve command-line options conflicts
if options.event_dest is not None and options.no_events is not None:
    logger.critical(
        "the --no-events and --dest options conflict, please use only one of them"
    )
    sys.exit(1)

# Check if user wants to override pid checking
if options.skip_pid_check is not None:
    skip_pid_check = True

# Make sure no other pegasus-monitord instances are running...
pid_filename = os.path.join(run, "monitord.pid")
if not skip_pid_check and utils.pid_running(pid_filename):
    logger.critical(
        "it appears that pegasus-monitord is still running on this workflow... exiting"
    )
    sys.exit(43)
utils.write_pid_file(pid_filename)
atexit.register(delete_pid_file)

# Parse notification-related properties
if int(props.property("pegasus.monitord.notifications.timeout") or -1) >= 0:
    notifications_timeout = int(
        props.property("pegasus.monitord.notifications.timeout")
    )
if int(props.property("pegasus.monitord.notifications.max") or -1) >= 0:
    max_parallel_notifications = int(
        props.property("pegasus.monitord.notifications.max")
    )
if max_parallel_notifications == 0:
    logger.warning(
        "maximum parallel notifications set to 0, disabling notifications..."
    )
    do_notifications = False
if not utils.make_boolean(props.property("pegasus.monitord.notifications") or "true"):
    do_notifications = False

# Parse stdout/stderr disable parsing property
if utils.make_boolean(
    props.property("pegasus.monitord.stdout.disable.parsing") or "false"
):
    store_stdout_stderr = False

if options.adjustment is not None:
    adjustment = options.adjustment
condor_daemon = options.condor_daemon
if options.jsd is not None:
    jsd = options.jsd
if options.millisleep is not None:
    millisleep = options.millisleep
if options.replay_mode is not None:
    replay_mode = options.replay_mode
    # Replay mode always runs in foreground
    condor_daemon = False
    # No notifications in replay mode
    do_notifications = False
    # replay mode: manually run by user on the command line.
    # we take a backup of monitord.log file if it exists to ensure
    # pegasus-statistics does not complain while generating statistics
    # Normally, backup of monitord.log file is taken by pegasus-dagman
    utils.rotate_log_file(MONITORD_LOG_FILE)
if options.no_notify is not None:
    do_notifications = False
if options.notifications_max is not None:
    max_parallel_notifications = options.notifications_max
    if max_parallel_notifications == 0:
        do_notifications = False
    if max_parallel_notifications < 0:
        logger.critical("notifications-max must be integer >= 0")
        sys.exit(1)
if options.notifications_timeout is not None:
    notifications_timeout = options.notifications_timeout
    if notifications_timeout < 0:
        logger.critical("notifications-timeout must be integer >= 0")
        sys.exit(1)
    if notifications_timeout > 0 and notifications_timeout < 5:
        logger.warning(
            "notifications-timeout set too low... notification scripts may not have enough time to complete... continuing anyway..."
        )
if options.disable_subworkflows is not None:
    follow_subworkflows = False
if options.db_stats is not None:
    db_stats = options.db_stats
if options.keep_state is not None:
    keep_state = options.keep_state
if options.skip_stdout is not None:
    store_stdout_stderr = False
if options.output_dir is not None:
    output_dir = options.output_dir
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    except OSError:
        logger.critical("cannot create directory %s. exiting..." % (output_dir))
        sys.exit(1)
if options.event_dest is None:
    if options.no_events is not None:
        # Turn off event generation
        no_events = True
    else:
        if props.property("pegasus.monitord.events") is not None:
            # Set event generation according to properties (default is True)
            no_events = not utils.make_boolean(
                props.property("pegasus.monitord.events")
            )
        else:
            # Default is to generate events
            no_events = False

    event_dest = connection.url_by_properties(
        options.config_properties,
        connection.DBType.WORKFLOW,
        run,
        rundir_properties=top_level_prop_file,
    )
else:
    # Use command-line option
    event_dest = options.event_dest

if options.fast_start is False:
    fast_start_mode = False

# check if specified in properties and override from options
fast_start_property = props.property("pegasus.monitord.fast_start")
if fast_start_property is not None:
    fast_start_mode = utils.make_boolean(fast_start_property)

dashboard_event_dest = connection.url_by_properties(
    options.config_properties,
    connection.DBType.MASTER,
    rundir_properties=top_level_prop_file,
)

if options.enc is not None:
    # Get encoding from command-line options
    encoding = options.enc
else:
    if props.property("pegasus.monitord.encoding") is not None:
        # Get encoding from property
        encoding = props.property("pegasus.monitord.encoding")

if encoding is None:
    encoding = DEFAULT_ENCODING

# Check if the user-provided jsd file is an absolute path, if so, we
# disable recursive mode
if jsd is not None:
    if os.path.isabs(jsd):
        # Yes, this is an absolute path
        follow_subworkflows = False
        logger.warning(
            "jsd file is an absolute filename, disabling sub-workflow tracking"
        )

#
# --- functions ---------------------------------------------------------------------------
#


def add(wf, jobid, event, sched_id=None, status=None, reason=None):
    """
    This function processes events related to jobs' state changes. It
    creates a new job, when needed, and by calling the workflow's
    update_job_state method, it causes output to be generated (both to
    jobstate.log and to the backend configured to receive events). wf
    is the workflow object for this operation, jobid is the id for the
    job (job_name), event is the actual state associated with this
    event (SUBMIT, EXECUTE, etc). sched_id is the scheduler's id for
    this particular job instance, and status is the exitcode for the
    job. This function returns the job_submit_seq for the
    corresponding jobid.
    """

    my_site = None
    my_time = None
    my_job_submit_seq = None

    # Remove existing site info during replanning
    if event in unsubmitted_events:
        if jobid in wf._job_site:
            del wf._job_site[jobid]
        if jobid in wf._walltime:
            del wf._walltime[jobid]

    # Variables originally from submit file information
    if jobid in wf._job_site:
        my_site = wf._job_site[jobid]
    if jobid in wf._walltime:
        my_time = wf._walltime[jobid]

    # A PRE_SCRIPT_START event always means a new job
    if event == "PRE_SCRIPT_STARTED":
        # This is a new job, we need to add it to the workflow
        my_job_submit_seq = wf.add_job(jobid, event)
        # PM-1390 ensure you parse the condor submit file when
        # prescript starts. as we still need to send composite event
        # when prescript fails for a job

        # Obtain planning information from the submit file when entering Condor,
        # Figure out how long the job _intends_ to run maximum
        my_time, my_site = wf.parse_job_sub_file(jobid, my_job_submit_seq)
        if my_site == "!!SITE!!":
            my_site = None

        # Remember the run-site
        if my_site is not None:
            logger.debug("job {} is planned for site {}".format(jobid, my_site))
            wf._job_site[jobid] = my_site
        else:
            logger.debug("job %s does not have a site information!" % (jobid))

    # A DAGMAN_SUBMIT event requires a new job (unless this was
    # already done by a PRE_SCRIPT_STARTED event, but we let the
    # add_job function figure this out).
    # DAGMAN_SUBMIT corresponds to lines in dagman.out matching  Submitting Condor Node *...
    if event == "DAGMAN_SUBMIT":
        wf._last_submitted_job = jobid
        my_job_submit_seq = wf.add_job(jobid, event)

        # PM-1068 parse the job submit file on DAGMAN_SUBMIT event instead of SUBMIT or SUBMIT_FAILED

        # Obtain planning information from the submit file when entering Condor,
        # Figure out how long the job _intends_ to run maximum
        my_time, my_site = wf.parse_job_sub_file(jobid, my_job_submit_seq)

        if my_site == "!!SITE!!":
            my_site = None

        # If not None, convert into seconds
        if my_time is not None:
            my_time = my_time * 60
            logger.debug("job %s requests %d s walltime" % (jobid, my_time))
            wf._walltime[jobid] = my_time
        else:
            logger.debug("job %s does not request a walltime" % (jobid))

        # Remember the run-site
        if my_site is not None:
            logger.debug("job {} is planned for site {}".format(jobid, my_site))
            wf._job_site[jobid] = my_site
        else:
            logger.debug("job %s does not have a site information!" % (jobid))

        # Nothing else to do... we should stop here...
        return my_job_submit_seq

    # A SUBMIT event brings sched id and job type information (it can also be
    # a new job for us when there is no PRE_SCRIPT)
    if event == "SUBMIT" or event == "SUBMIT_FAILED":
        # Add job to our workflow (if not alredy there), will update sched_id in both cases
        my_job_submit_seq = wf.add_job(jobid, event, sched_id=sched_id)

    # Get job_submit_seq if we don't already have it
    if my_job_submit_seq is None:
        my_job_submit_seq = wf.find_jobid(jobid)

    if my_job_submit_seq is None:
        logger.warning("cannot find job_submit_seq for job: %s" % (jobid))
        # Nothing else to do...
        return None

    # Make sure job has the updated state
    wf.update_job_state(
        jobid, sched_id, my_job_submit_seq, event, status, my_time, reason
    )

    return my_job_submit_seq


def process_dagman_out(wf, log_line):
    """
    This function processes a log line from the dagman.out file and
    calls either the add function to generate a jobstate.log output
    line, or calls the corresponding workflow class method in order to
    track the various events that happen during the life of a
    workflow. It returns a tuple containing the new DAGMan output
    file, with the parent jobid and sequence number if we need to
    follow a sub-workflow.
    """

    # Keep track of line count
    wf._line = wf._line + 1

    # Make sure we have not already seen this line
    # This is used in the case of rescue dags, for skipping
    # what we have already seen in the dagman.out file
    if wf._line <= wf._last_processed_line:
        return

    # Strip end spaces, tabs, and <cr> and/or <lf>
    log_line = log_line.rstrip()

    # Check log_line for timestamp at the beginning
    timestamp_found = False

    my_expr = None
    # PM-1030 there should be only at max two timestamps recorded
    # and we prefer the second one if present.
    for my_expr in re_parse_timestamp.finditer(log_line):
        pass

    if my_expr is not None:
        # Found time stamp, let's assume valid log line
        curr_time = time.localtime()
        adj_time = list(curr_time)
        adj_time[1] = int(my_expr.group(1))  # Month
        adj_time[2] = int(my_expr.group(2))  # Day
        adj_time[3] = int(my_expr.group(5))  # Hours
        adj_time[4] = int(my_expr.group(6))  # Minutes
        adj_time[5] = int(my_expr.group(7))  # Seconds
        adj_time[8] = -1  # DST, let Python figure it out

        if my_expr.group(3) is not None:
            # New timestamp format
            adj_time[0] = int(my_expr.group(4)) + 2000  # Year

        wf._current_timestamp = time.mktime(tuple(adj_time)) + adjustment
        timestamp_found = True
    else:
        # FIXME: Use method from utils.py, do not re-invent the wheel!
        # FIXME: Slated for 3.1
        my_expr = re_parse_iso_stamp.search(log_line)
        if my_expr is not None:
            # /^\s*(\d{4}).?(\d{2}).?(\d{2}).(\d{2}).?(\d{2}).?(\d{2})([.,]\d+)?([Zz]|[-+](\d{2}).?(\d{2}))/
            dt = "%04d-%02d-%02d %02d:%02d:%02d" % (
                int(my_expr.group(1)),
                int(my_expr.group(2)),
                int(my_expr.group(3)),
                int(my_expr.group(4)),
                int(my_expr.group(5)),
                int(my_expr.group(6)),
            )
            my_time = datetime.datetime(*(time.strptime(dt, "%Y-%m-%d %H:%M:%S")[0:6]))

            tz = my_expr.group(8)
            if tz.upper() != "Z":
                # no zulu time, has zone offset
                my_offset = datetime.timedelta(
                    hours=int(my_expr.group(9)), minutes=int(my_expr.group(10))
                )

                # adjust for time zone offset
                if tz[0] == "-":
                    my_time = my_time + my_offset
                else:
                    my_time = my_time - my_offset

            # Turn my_time into Epoch format
            wf._current_timestamp = (
                int(calendar.timegm(my_time.timetuple())) + adjustment
            )
            timestamp_found = True

    if timestamp_found:
        split_log_line = log_line.split(None, 3)
        if len(split_log_line) >= 3:
            logger.trace("debug: ## %d: %s" % (wf._line, split_log_line[2][:64]))

        # If in recovery mode, check if we reached the end of it
        # This is the DAGMan recovery mode . Not monitord recovery mode! Karan
        if wf._skipping_recovery_lines:
            if log_line.find("...done with RECOVERY mode") >= 0:
                logger.info("DONE with DAGMAN RECOVERY MODE")
                wf._skipping_recovery_lines = False
            return

        # Search for more content
        if re_parse_event.search(log_line) is not None:
            # Found ULOG Event
            my_expr = re_parse_event.search(log_line)
            # groups = jobid, event, sched_id
            my_event = my_expr.group(1)
            my_jobid = my_expr.group(2)
            my_sched_id = my_expr.group(3)
            my_job_submit_seq = add(wf, my_jobid, my_event, sched_id=my_sched_id)
            if my_event == "SUBMIT" and follow_subworkflows is True:
                # For SUBMIT ULOG events, check if this is a sub-workflow
                my_new_dagman_out = wf.has_subworkflow(my_jobid, wf_retry_dict)
                # Ok, return result to main loop
                return (my_new_dagman_out, my_jobid, my_job_submit_seq)
        elif re_parse_job_submit.search(log_line) is not None:
            # Found a DAGMan job submit event
            my_expr = re_parse_job_submit.search(log_line)
            # groups = jobid
            add(wf, my_expr.group(1), "DAGMAN_SUBMIT")
        elif re_parse_job_submit_error.search(log_line) is not None:
            # Found a DAGMan job submit error event
            if wf._last_submitted_job is not None:
                add(wf, wf._last_submitted_job, "SUBMIT_FAILED")
            else:
                logger.warning(
                    "found submit error in dagman.out, but last job is not set"
                )
        elif re_parse_script_running.search(log_line) is not None:
            # Pre scripts are not regular Condor event
            # Starting of scripts is not a regular Condor event
            my_expr = re_parse_script_running.search(log_line)
            # groups = script, jobid
            my_script = my_expr.group(1).upper()
            my_jobid = my_expr.group(2)
            add(wf, my_jobid, "%s_SCRIPT_STARTED" % (my_script))
        elif re_parse_script_done.search(log_line) is not None:
            my_expr = re_parse_script_done.search(log_line)
            # groups = script, jobid
            my_script = my_expr.group(1).upper()
            my_jobid = my_expr.group(2)
            if my_script == "PRE":
                # Special case for PRE_SCRIPT_TERMINATED, as Condor
                # does not generate a PRE_SCRIPT_TERMINATED ULOG event
                add(wf, my_jobid, "PRE_SCRIPT_TERMINATED")
            if re_parse_script_successful.search(log_line) is not None:
                # Remember success with artificial jobstate
                add(wf, my_jobid, "%s_SCRIPT_SUCCESS" % (my_script), status=0)
            elif re_parse_script_failed.search(log_line) is not None:
                # Remember failure with artificial jobstate
                my_expr = re_parse_script_failed.search(log_line)
                # groups = exit code (error status)
                try:
                    my_exit_code = int(my_expr.group(1))
                except ValueError:
                    # Unable to convert exit code to integer -- should not happen
                    logger.warning("unable to convert exit code to integer!")
                    my_exit_code = 1
                add(
                    wf, my_jobid, "%s_SCRIPT_FAILURE" % (my_script), status=my_exit_code
                )
            else:
                # Ignore
                logger.warning("unknown pscript state: %s" % (log_line[-14:]))
        elif re_parse_job_failed.search(log_line) is not None:
            # Job has failed
            my_expr = re_parse_job_failed.search(log_line)
            # groups = jobid, schedid, jobstatus
            my_jobid = my_expr.group(1)
            my_sched_id = my_expr.group(2)
            my_expr.group(3)
            try:
                my_jobstatus = int(my_expr.group(4))
            except ValueError:
                # Unable to convert exit code to integet -- should not happen
                logger.warning("unable to convert exit code to integer!")
                my_jobstatus = 1
            # remember failure with artificial jobstate
            add(wf, my_jobid, "JOB_FAILURE", sched_id=my_sched_id, status=my_jobstatus)
        elif re_parse_job_successful.search(log_line) is not None:
            # Job succeeded
            my_expr = re_parse_job_successful.search(log_line)
            my_jobid = my_expr.group(1)
            my_sched_id = my_expr.group(2)
            # remember success with artificial jobstate
            add(wf, my_jobid, "JOB_SUCCESS", sched_id=my_sched_id, status=0)
        elif re_parse_dagman_finished.search(log_line) is not None:
            # DAG finished -- done parsing
            my_expr = re_parse_dagman_finished.search(log_line)
            # groups = exit code
            try:
                wf._dagman_exit_code = int(my_expr.group(1))
            except ValueError:
                # Cannot convert exit code to integer!
                logger.warning("cannot convert DAGMan's exit code to integer!")
                wf._dagman_exit_code = 0
                wf._monitord_exit_code = 1
            logger.info(
                "DAGMan {} finished with exit code {}".format(
                    wf._dag_file_name, wf._dagman_exit_code
                )
            )
            # Send info to database
            wf.change_wf_state("end")
        elif re_parse_dagman_condor_id.search(log_line) is not None:
            # DAGMan starting, capture its condor id
            my_expr = re_parse_dagman_condor_id.search(log_line)
            wf._dagman_condor_id = my_expr.group(1)
            if not keep_state:
                # Initialize workflow parameters
                wf.start_wf()
        elif re_parse_dagman_pid.search(log_line) is not None and not replay_mode:
            # DAGMan's pid, but only set pid if not running in replay mode
            # (otherwise pid may belong to another process)
            my_expr = re_parse_dagman_pid.search(log_line)
            # groups = DAGMan's pid
            try:
                wf._dagman_pid = int(my_expr.group(1))
            except ValueError:
                logger.critical("cannot set pid: %s" % (my_expr.group(1)))
                sys.exit(42)
            logger.info("DAGMan runs at pid %d" % (wf._dagman_pid))
        elif re_parse_dag_name.search(log_line) is not None:
            # Found the dag filename, read dag, and generate start event for the database
            my_expr = re_parse_dag_name.search(log_line)
            my_dag = my_expr.group(1)
            # Parse dag file
            logger.info("using dag %s" % (my_dag))

            # PM-1334 potential fix. we parse dag file now in workflow constructor when we determine
            # there is a new workflow to track
            # wf.parse_dag_file(my_dag)

            # Send the delayed workflow start event to database
            wf.change_wf_state("start")
        elif re_parse_condor_version.search(log_line) is not None:
            # Version of this logfile format
            my_expr = re_parse_condor_version.search(log_line)
            # groups = condor version, condor major
            my_condor_version = my_expr.group(1)
            my_condor_major = int(my_expr.group(2))
            my_condor_minor = int(my_expr.group(3))
            my_condor_patch = int(my_expr.group(4))
            wf.set_dagman_version(my_condor_major, my_condor_minor, my_condor_patch)
            logger.info(
                "Using DAGMan version %s %d"
                % (my_condor_version, wf.get_dagman_version())
            )
        elif (
            re_parse_condor_logfile.search(log_line) is not None
            or wf._multiline_file_flag is True
            and re_parse_condor_logfile_insane.search(log_line) is not None
        ):
            # Condor common log file location, DAGMan 6.6
            if re_parse_condor_logfile.search(log_line) is not None:
                my_expr = re_parse_condor_logfile.search(log_line)
            else:
                my_expr = re_parse_condor_logfile_insane.search(log_line)
            wf._condorlog = my_expr.group(1)
            logger.info("Condor writes its logfile to %s" % (wf._condorlog))

            # Make a symlink for NFS-secured files
            my_log, my_base = utils.out2log(wf._run_dir, wf._out_file)
            if os.path.islink(my_log):
                logger.info("symlink %s already exists" % (my_log))
            elif os.access(my_log, os.R_OK):
                logger.info("%s is a regular file, not touching" % (my_base))
            else:
                logger.info("trying to create local symlink to common log")
                if os.access(wf._condorlog, os.R_OK) or not os.access(
                    wf._condorlog, os.F_OK
                ):
                    if os.access(my_log, os.R_OK):
                        try:
                            os.rename(my_log, "%s.bak" % (my_log))
                        except OSError:
                            logger.warning(
                                "error renaming {} to {}.bak".format(my_log, my_log)
                            )
                    try:
                        os.symlink(wf._condorlog, my_log)
                    except OSError:
                        logger.info("unable to symlink %s" % (wf._condorlog))
                    else:
                        logger.info("symlink {} -> {}".format(wf._condorlog, my_log))
                else:
                    logger.info("%s exists but is not readable!" % (wf._condorlog))
            # We only expect one of such files
            wf._multiline_file_flag = False
        elif re_parse_multiline_files.search(log_line) is not None:
            # Multiline user log files, DAGMan > 6.6
            wf._multiline_file_flag = True
        elif log_line.find("Running in RECOVERY mode...") >= 0:
            # Entering recovery mode, skip lines until we reach the end
            logger.info("Enabling DAGMAN RECOVERY MODE")
            wf._skipping_recovery_lines = True
            return
        elif re_parse_dagman_aborted.search(log_line) is not None:
            # PM-767 dagman was aborted. just log in monitord log
            # eventually the dagman exit line will trigger failure in the DB
            logger.warning(
                "DAGMan was aborted for workflow running in directory %s" % wf._run_dir
            )
            wf._current_state_reason = "DAGMan aborted as it received SIGUSR1 signal."
            return
        elif re_parse_job_held.search(log_line) is not None:
            # PM-749  figure out reason for job held
            my_expr = re_parse_job_held.search(log_line)
            my_held_reason = my_expr.group(1)

            # figure out which job  was held
            # the log line has no info. so try to fallback on last job
            last_job = wf._last_known_job
            if last_job is None or last_job._job_state != "JOB_HELD":
                logger.error(
                    "Last known job %s is not held. Parsed  held job with reason %s "
                    % (last_job, my_held_reason)
                )

            # PM-749 insert a new JOB_HELD_REASON event that connects to held job
            my_event = "JOB_HELD_REASON"
            my_jobid = last_job._exec_job_id
            my_sched_id = last_job._sched_id
            my_job_submit_seq = add(
                wf, my_jobid, my_event, sched_id=my_sched_id, reason=my_held_reason
            )
    else:
        # Could not parse timestamp
        logger.info("time stamp format not recognized")


def sleeptime(retries):
    """
    purpose: compute suggested sleep time as a function of retries
    paramtr: $retries (IN): number of retries so far
    returns: recommended sleep time in seconds
    """
    if retries < 5:
        my_y = 1
    elif retries < 50:
        my_y = 5
    elif retries < 500:
        my_y = 30
    else:
        my_y = 60

    return my_y


#
# --- signal handlers -------------------------------------------------------------------
#


def prog_sighup_handler(signum, frame):
    """
    This function catches SIGHUP.
    """
    logger.info("ignoring signal %d" % (signum))


def prog_sigint_handler(signum, frame):
    """
    This function catches SIGINT.
    """
    logger.warning("graceful exit on signal %d" % (signum))
    # Go through all workflows we are tracking
    for my_wf in wfs:
        if my_wf.wf is not None:
            # Update monitord exit code
            if my_wf.wf._monitord_exit_code == 0:
                my_wf.wf._monitord_exit_code = 1
            # Close open files
            my_wf.wf.end_workflow()
    # All done!
    sys.exit(1)


def prog_sigusr1_handler(signum, frame):
    """
    This function increases the log level to the next one.
    """
    cur_level = root_logger.getEffectiveLevel()

    try:
        idx = _LEVELS.index(cur_level)
        if idx + 1 < len(_LEVELS):
            root_logger.setLevel(_LEVELS[idx + 1])
    except ValueError:
        root_logger.setLevel(logging.INFO)
        logger.error("Unknown current level = %s, setting to INFO" % (cur_level))


def prog_sigusr2_handler(signum, frame):
    """
    This function decreases the log level to the previous one.
    """
    cur_level = root_logger.getEffectiveLevel()

    try:
        idx = _LEVELS.index(cur_level)
        if idx > 0:
            root_logger.setLevel(_LEVELS[idx - 1])
    except ValueError:
        root_logger.setLevel(logging.WARN)
        logger.error("Unknown current level = %s, setting to WARN" % (cur_level))


#
# --- main ------------------------------------------------------------------------------
#

if condor_daemon:
    # This is the case when being called from pegasus-dagman
    # Although we cannot set sid, we can still become process group leader
    os.setpgid(0, 0)

# Ignore dying shells
signal.signal(signal.SIGHUP, prog_sighup_handler)

# Die nicely when asked to (Ctrl+C, system shutdown)
signal.signal(signal.SIGINT, prog_sigint_handler)

# Permit dynamic changes of debug level
signal.signal(signal.SIGUSR1, prog_sigusr1_handler)
signal.signal(signal.SIGUSR2, prog_sigusr2_handler)

# Log recover mode
if os.access(os.path.join(run, MONITORD_RECOVER_FILE), os.F_OK):
    logger.warning(
        "monitord entering it's own recovery mode. Population will start again for the workflow.."
    )

# Create wf_event_sink object
restart_logging = False
if no_events:
    wf_event_sink = None  # Avoid parsing kickstart output if not
    # generating bp file or database events
    dashboard_event_sink = None
else:
    if replay_mode or os.access(os.path.join(run, MONITORD_RECOVER_FILE), os.F_OK):
        restart_logging = True

    # PM-652 if there is sqlite db then just take a backup
    # by rotating the db file. possible as we have only one root workflow per sqlitedb
    # PM-689 rotation happens both in replay and monitord recovery mode ( where recover file exists)
    backup = False
    if restart_logging and event_dest.startswith("sqlite:"):
        try:
            event_dest.index("sqlite:///")
        except ValueError:
            logger.error("Invalid sqlite connection string passed %s " % event_dest)
        backup = True

    try:
        wf_event_sink = eo.create_wf_event_sink(
            event_dest,
            db_stats=db_stats,
            restart=restart_logging,
            enc=encoding,
            props=props,
            db_type=connection.DBType.WORKFLOW,
            backup=backup,
        )
        atexit.register(finish_stampede_loader)
    except Exception:
        logger.error(traceback.format_exc())
        logger.error("cannot create events output... disabling event output!")
        wf_event_sink = None
    else:
        try:
            if restart_logging and isinstance(wf_event_sink, eo.DBEventSink):
                # If in replay mode or recovery mode and it is a DB,
                # attempt to purge wf_uuid_first
                eo.purge_wf_uuid_from_database(run, event_dest)
        except Exception as e:
            logger.exception(e)
            logger.error(
                "error flushing previous wf_uuid from database... continuing..."
            )
            logger.error("cannot create events output... disabling event output!")
            wf_event_sink = None

    # create the stampede_dashboard_loader
    try:
        dashboard_event_sink = eo.create_wf_event_sink(
            dashboard_event_dest,
            restart=restart_logging,
            prefix=eo.DASHBOARD_NS,
            db_stats=db_stats,
            props=props,
            db_type=connection.DBType.MASTER,
        )
    except Exception:
        logger.error(traceback.format_exc())
        dashboard_event_sink = None
    else:
        try:
            if restart_logging and isinstance(dashboard_event_sink, eo.DBEventSink):
                # If in replay mode or recovery mode and it is a DB,
                # attempt to purge wf_uuid_first
                eo.purge_wf_uuid_from_dashboard_database(run, dashboard_event_dest)
        except Exception as e:
            logger.exception(e)
            logger.error(
                "error flushing previous wf_uuid from dashboard database... continuing..."
            )
            logger.error("cannot create events output... disabling event output!")
            dashboard_event_dest = None

    if dashboard_event_dest is None:
        logger.error(
            "cannot create dashboard events output... disabling dashboard event output!"
        )

if millisleep is not None:
    logger.info("using simulation delay of %d ms" % (millisleep))

if fast_start_mode:
    logger.info("monitord started in fast start mode")

# Build sub-workflow retry filename
if output_dir is None:
    wf_retry_fn = os.path.join(run, MONITORD_WF_RETRY_FILE)
    wf_notification_fn_prefix = run
else:
    wf_retry_fn = os.path.join(run, output_dir, MONITORD_WF_RETRY_FILE)
    wf_notification_fn_prefix = os.path.join(run, output_dir)

# Empty sub-workflow retry information if in replay mode
# PM-704 in replay or restart logging case we always take a backup
# of monitord.subwf.db . Otherwise monitord loses track of workflow
# retries workflow.has_subworkflow() function where it tries to determine
# submit directory for the sub workflow
if restart_logging:
    subworkflow_db_file = wf_retry_fn + ".db"
    if os.path.isfile(subworkflow_db_file):
        logger.info("Rotating sub workflow db file %s" % subworkflow_db_file)
        utils.rotate_log_file(subworkflow_db_file)


# Link wf_retry_dict to persistent storage
try:
    wf_retry_dict = shelve.open(wf_retry_fn)
    atexit.register(close_wf_retry_file)
except Exception:
    logger.critical(
        "cannot create persistent storage file for sub-workflow retry information... exiting... %s"
        % wf_retry_fn
    )
    logger.error(traceback.format_exc())
    sys.exit(1)

# Open notifications' log file
if do_notifications is True:
    monitord_notifications = notifications.Notifications(
        wf_notification_fn_prefix,
        max_parallel_notifications=max_parallel_notifications,
        notifications_timeout=notifications_timeout,
    )
    atexit.register(finish_notifications)

# Ok! Let's start now...

# Instantiate workflow class
wf = Workflow(
    run,
    out,
    database=wf_event_sink,
    dashboard_database=dashboard_event_sink,
    database_url=event_dest,
    jsd=jsd,
    enable_notifications=do_notifications,
    replay_mode=replay_mode,
    output_dir=output_dir,
    store_stdout_stderr=store_stdout_stderr,
    notifications_manager=monitord_notifications,
)
# If everything went well, create a workflow entry for this workflow
if wf._monitord_exit_code == 0:
    workflow_entry = WorkflowEntry()
    workflow_entry.run_dir = run
    workflow_entry.dagman_out = out
    workflow_entry.wf = wf
    workflow_entry.caught_up_with_dagman_out = False

    # And add it to our list of workflows
    wfs.append(workflow_entry)
    if replay_mode:
        tracked_workflows.append(out)

    # Also set the root workflow id
    root_wf_id = wf._wf_uuid

#
# --- main loop begin --------------------------------------------------------------------
#

# Loop while we have workflows to follow...
while len(wfs) > 0:
    # Go through each of our workflows
    for workflow_entry in wfs:

        # Check if we are waiting for the dagman.out file to appear...
        if workflow_entry.DMOF is None:

            # Yes... check if it has shown up...

            # First, we test if the file is already there, in case we are running in replay mode
            if replay_mode:
                try:
                    f_stat = os.stat(workflow_entry.dagman_out)
                except OSError:
                    logger.critical(
                        "error: workflow not started, %s does not exist, dropping this workflow..."
                        % (workflow_entry.dagman_out)
                    )
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

            try:
                f_stat = os.stat(workflow_entry.dagman_out)
            except OSError as e:
                if errno.errorcode[e.errno] == "ENOENT":
                    # File doesn't exist yet, keep looking
                    workflow_entry.n_retries = workflow_entry.n_retries + 1
                    if workflow_entry.n_retries > 100:
                        # We tried too long, just exit
                        logger.critical(
                            "%s never made an appearance" % (workflow_entry.dagman_out)
                        )
                        workflow_entry.delete_workflow = True
                        # Close jobstate.log, if any
                        if workflow_entry.wf is not None:
                            workflow_entry.wf.end_workflow()
                        # Go to the next workflow_entry in the for loop
                        continue
                    # Continue waiting
                    logger.info(
                        "waiting for dagman.out file, retry %d"
                        % (workflow_entry.n_retries)
                    )
                    workflow_entry.sleep_time = time.time() + sleeptime(
                        workflow_entry.n_retries
                    )
                else:
                    # Another error
                    logger.critical("stat %s" % (workflow_entry.dagman.out))
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue
            except Exception:
                # Another exception
                logger.critical("stat %s" % (workflow_entry.dagman.out))
                workflow_entry.delete_workflow = True
                # Close jobstate.log, if any
                if workflow_entry.wf is not None:
                    workflow_entry.wf.end_workflow()
                # Go to the next workflow_entry in the for loop
                continue
            else:
                # Found it, open dagman.out file
                try:
                    workflow_entry.DMOF = open(workflow_entry.dagman_out)
                    workflow_entry.dagman_out_appeared = True
                except OSError:
                    logger.critical("opening %s" % (workflow_entry.dagman_out))
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

        if workflow_entry.DMOF is not None:
            try:
                logger.trace("stating file: %s" % (workflow_entry.dagman_out))
                f_stat = os.stat(workflow_entry.dagman_out)
            except OSError:
                # stat error
                logger.critical("stat %s" % (workflow_entry.dagman_out))
                workflow_entry.delete_workflow = True
                # Close jobstate.log, if any
                if workflow_entry.wf is not None:
                    workflow_entry.wf.end_workflow()
                # Go to the next workflow_entry in the for loop
                continue

            # f_stat[6] is the file size
            if f_stat[6] == workflow_entry.ml_current:
                # Death by natural causes
                if workflow_entry.wf._dagman_exit_code is not None and not replay_mode:
                    logger.info("workflow %s ended" % (workflow_entry.dagman_out))
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

                # Check if DAGMan is alive -- if we know where it lives
                if workflow_entry.ml_retries > 10 and workflow_entry.wf._dagman_pid > 0:
                    # Just send signal 0 to check if the pid is ours
                    try:
                        os.kill(int(workflow_entry.wf._dagman_pid), 0)
                    except OSError:
                        logger.critical(
                            "DAGMan is gone! Sudden death syndrome detected!"
                        )
                        workflow_entry.wf._monitord_exit_code = 42
                        workflow_entry.delete_workflow = True
                        # Close jobstate.log, if any
                        if workflow_entry.wf is not None:
                            workflow_entry.wf.end_workflow()
                        # Go to the next workflow_entry in the for loop
                        continue

                # No change, wait a while
                workflow_entry.ml_retries = workflow_entry.ml_retries + 1
                if workflow_entry.ml_retries > 17280:
                    # Too long without change
                    logger.critical(
                        "too long without action, stopping workflow %s"
                        % (workflow_entry.dagman_out)
                    )
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

                # In replay mode, we can be a little more aggresive
                # FIXME Why would you have to wait for 5 tries in replay mode?
                if replay_mode and workflow_entry.ml_retries > 5:
                    # We are in replay mode, so we should have everything here
                    logger.info(
                        "no more action, stopping workflow %s"
                        % (workflow_entry.dagman_out)
                    )
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

            elif f_stat[6] < workflow_entry.ml_current:
                # Truncated file, booh!
                logger.critical(
                    "%s file truncated, time to exit" % (workflow_entry.dagman_out)
                )
                workflow_entry.delete_workflow = True
                # Close jobstate.log, if any
                if workflow_entry.wf is not None:
                    workflow_entry.wf.end_workflow()
                # Go to the next workflow_entry in the for loop
                continue

            elif f_stat[6] > workflow_entry.ml_current:
                # We have something to read!
                try:
                    ml_rbuffer = workflow_entry.DMOF.read(DAGMAN_OUT_MAX_READ_SIZE)
                except Exception:
                    # Error while reading
                    logger.critical("while reading %s" % (workflow_entry.dagman_out))
                    workflow_entry.wf._monitord_exit_code = 42
                    workflow_entry.delete_workflow = True
                    # Close jobstate.log, if any
                    if workflow_entry.wf is not None:
                        workflow_entry.wf.end_workflow()
                    # Go to the next workflow_entry in the for loop
                    continue

                if len(ml_rbuffer) < DAGMAN_OUT_MAX_READ_SIZE:
                    # PM-947 we have caught up with the workflow
                    logger.debug(
                        "monitord has caught up with dagman out file %s"
                        % (workflow_entry.dagman_out)
                    )
                    workflow_entry.caught_up_with_dagman_out = True

                if len(ml_rbuffer) == 0:
                    # Detected EOF
                    logger.critical(
                        "detected EOF, resetting position to %d"
                        % (workflow_entry.ml_current)
                    )
                    workflow_entry.DMOF.seek(workflow_entry.ml_current)
                else:
                    # Something in the read buffer, merge it with our buffer
                    workflow_entry.ml_buffer = workflow_entry.ml_buffer + ml_rbuffer
                    # Look for end of line
                    ml_pos = workflow_entry.ml_buffer.find("\n")
                    while ml_pos >= 0:
                        # Take out 1 line, and adjust buffer
                        process_output = process_dagman_out(
                            workflow_entry.wf, workflow_entry.ml_buffer[0:ml_pos]
                        )
                        workflow_entry.ml_buffer = workflow_entry.ml_buffer[
                            ml_pos + 1 :
                        ]
                        ml_pos = workflow_entry.ml_buffer.find("\n")

                        # Do we need to start following another workflow?
                        if (
                            type(process_output) is tuple
                            and len(process_output) == 3
                            and process_output[0] is not None
                        ):
                            # Unpack the output tuple
                            new_dagman_out = process_output[0]
                            parent_jobid = process_output[1]
                            parent_jobseq = process_output[2]
                            # Only if we are not already tracking it...
                            tracking_already = False
                            new_dagman_out = os.path.abspath(new_dagman_out)
                            # Add the current run directory in case this is a relative path
                            new_dagman_out = os.path.join(
                                workflow_entry.run_dir, new_dagman_out
                            )
                            if replay_mode:
                                # Check if we started tracking this subworkflow in the past
                                if new_dagman_out in tracked_workflows:
                                    # Yes, no need to do it again...
                                    logger.info(
                                        "already tracking workflow: %s, not adding"
                                        % (new_dagman_out)
                                    )
                                    tracking_already = True
                            else:
                                # Not in replay mode, let's check if we are currently tracking this subworkflow
                                for my_wf in wfs:
                                    if (
                                        my_wf.dagman_out == new_dagman_out
                                        and not my_wf.delete_workflow
                                    ):
                                        # Found it, exit loop
                                        tracking_already = True
                                        logger.info(
                                            "already tracking workflow: %s, not adding"
                                            % (new_dagman_out)
                                        )
                                        break
                            if not tracking_already:
                                logger.info(
                                    "found new workflow to track: %s" % (new_dagman_out)
                                )
                                # Not tracking this workflow, let's try to add it to our list
                                new_run_dir = os.path.dirname(new_dagman_out)
                                parent_wf_id = workflow_entry.wf._wf_uuid
                                new_wf = Workflow(
                                    new_run_dir,
                                    new_dagman_out,
                                    database=wf_event_sink,
                                    parent_id=parent_wf_id,
                                    parent_jobid=parent_jobid,
                                    parent_jobseq=parent_jobseq,
                                    root_id=root_wf_id,
                                    jsd=jsd,
                                    replay_mode=replay_mode,
                                    enable_notifications=do_notifications,
                                    output_dir=output_dir,
                                    store_stdout_stderr=store_stdout_stderr,
                                    notifications_manager=monitord_notifications,
                                )

                                if new_wf._monitord_exit_code == 0:
                                    new_workflow_entry = WorkflowEntry()
                                    new_workflow_entry.run_dir = new_run_dir
                                    new_workflow_entry.dagman_out = new_dagman_out
                                    new_workflow_entry.wf = new_wf

                                    # And add it to our list of workflows
                                    wfs.append(new_workflow_entry)
                                    # Don't forget to add it to our list, so we don't do it again in replay mode
                                    if replay_mode:
                                        tracked_workflows.append(new_dagman_out)

                            else:
                                # Just make sure we link the workflow to its parent job,
                                # which in this case is a job retry...
                                if os.path.dirname(new_dagman_out) in Workflow.wf_list:
                                    workflow_entry.wf.map_subwf(
                                        parent_jobid,
                                        parent_jobseq,
                                        Workflow.wf_list[
                                            os.path.dirname(new_dagman_out)
                                        ],
                                    )
                                else:
                                    logger.warning(
                                        "cannot link job %s:%s to its subwf because we don't have info for dir: %s"
                                        % (
                                            parent_jobid,
                                            parent_jobseq,
                                            os.path.dirname(new_dagman_out),
                                        )
                                    )

                        if millisleep is not None:
                            time.sleep(millisleep / 1000.0)

                    ml_pos = workflow_entry.DMOF.tell()
                    logger.debug(
                        "processed chunk of %d bytes"
                        % (
                            ml_pos
                            - workflow_entry.ml_current
                            - len(workflow_entry.ml_buffer)
                        )
                    )
                    workflow_entry.ml_current = ml_pos
                    workflow_entry.ml_retries = 0
                    # Write workflow progress for recovery mode
                    workflow_entry.wf.write_workflow_progress()

            workflow_entry.sleep_time = time.time() + sleeptime(
                workflow_entry.ml_retries
            )

    # End of main for loop, still in the while loop...

    logger.trace("currently tracking %d workflow(s)..." % (len(wfs)))

    # Go through the workflows again, and finish any marked ones
    wf_index = 0
    while wf_index < len(wfs):
        workflow_entry = wfs[wf_index]
        if workflow_entry.delete_workflow is True:
            logger.info("finishing workflow: %s" % (workflow_entry.dagman_out))
            # Close dagman.out file, if any
            if workflow_entry.DMOF is not None:
                workflow_entry.DMOF.close()
            #            # Close jobstate.log, if any
            #            if workflow_entry.wf is not None:
            #                workflow_entry.wf.end_workflow()
            # Delete this workflow from our list
            deleted_entry = wfs.pop(wf_index)
            # Don't move index to next one
        else:
            # Mode index to next workflow
            wf_index = wf_index + 1

    # Service notifications once per while loop, in the future we can
    # move this into the for loop and service notifications more often
    if do_notifications is True and monitord_notifications is not None:
        monitord_notifications.service_notifications()

    # Skip sleeping, if we have no more workflow to track...
    if len(wfs) == 0:
        continue

    # All done... let's figure out how long to sleep...
    time_to_sleep = time.time() + MAX_SLEEP_TIME
    sleep_for_some_time = True

    # Try to flush the sinks
    for sink in (wf_event_sink, dashboard_event_sink):
        if sink:
            sink.flush()

    for workflow_entry in wfs:
        # PM-947 we want to sleep if either dagman out has not appeared or we have caught up with the dagman.out
        sleep_for_some_time = sleep_for_some_time and (
            workflow_entry.caught_up_with_dagman_out
            or not workflow_entry.dagman_out_appeared
        )
        # Figure out if we have anything more urgent to do
        if workflow_entry.sleep_time < time_to_sleep:
            time_to_sleep = workflow_entry.sleep_time

    # PM-947 Sleep if not in replay mode AND (we have caught up with all workflows or are in default/normal mode)
    if not replay_mode and (sleep_for_some_time or not fast_start_mode):
        time_to_sleep = time_to_sleep - time.time()
        if time_to_sleep < 0:
            time_to_sleep = 0
        time.sleep(time_to_sleep)

#
# --- main loop end -----------------------------------------------------------------------
#

if do_notifications is True and monitord_notifications is not None:
    logger.info("finishing notifications...")
    while (
        monitord_notifications.has_active_notifications()
        or monitord_notifications.has_pending_notifications()
    ):
        monitord_notifications.service_notifications()
        time.sleep(SLEEP_WAIT_NOTIFICATION)

logger.info("monitord process %d exiting with 0" % os.getpid())

sys.exit(0)
