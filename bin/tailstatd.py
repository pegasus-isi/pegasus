#!/usr/bin/env python

"""
Small daemon process to update the job state file from DAGMan logs.
This program is to be run automatically by the pegasus-run command.

Usage: tailstatd [options] dagoutfile
"""

##
#  Copyright 2007-2010 University Of Southern California
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

# Revision : $Revision: 2012 $

# Import Python modules
import os
import re
import sys
import time
import uuid
import errno
import atexit
import select
import shelve
import signal
import socket
import logging
import commands
import operator
import optparse

# Initialize logging object
logger = logging.getLogger()
# Set default level to WARNING
logger.setLevel(logging.WARNING)
# Format our log messages the way we want
cl = logging.StreamHandler()
formatter = logging.Formatter("%(filename)s:%(lineno)d: %(levelname)s: %(message)s")
cl.setFormatter(formatter)
logger.addHandler(cl)

# Save our own basename
prog_base = os.path.split(sys.argv[0])[1]

# Import our modules
from Pegasus.tools import filelock
from Pegasus.tools import utils
from Pegasus.tools import properties

# Import netlogger api
from netlogger import nlapi

# Add SEEK_CUR to os if Python version < 2.5
if sys.version_info < (2, 5):
    os.SEEK_CUR = 1

# Compile our regular expressions

# Used in initialization code, while checking for rescue DAGs
re_parse_dag_submit_files = re.compile(r"JOB\s+(\S+)\s(\S+)(\s+DONE)?", re.IGNORECASE)

# Used in out2log
re_remove_extensions = re.compile(r"(?:\.(?:rescue|dag))+$")

# Used in untaint
re_clean_content = re.compile(r"([^-a-zA-z0-9_\s.,\[\]^\*\?\/\+])")

# Used in condor_rm
re_condor_id = re.compile(r"^\d+(\.\d+)?$")
re_condor_rm_output = re.compile(r"(?:Job|Node) [0-9.]+ already marked for removal")

# Used in extract_knowledge
re_rsl_string = re.compile(r"^\s*globusrsl\W", re.IGNORECASE)
re_rsl_clean = re.compile(r"([-_])")
re_site_parse_gvds = re.compile(r"^\s*\+(pegasus|wf)_(site|resource)\s*=\s*([\'\"])?(\S+)\3")
re_site_parse_euryale = re.compile(r"^\#!\s+site=(\S+)")

# Used in process
re_parse_timestamp = re.compile(r"^\s*(\d{1,2})\/(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})")
re_parse_event = re.compile(r"Event:\s+ULOG_(\S+) for Condor (?:Job|Node) (\S+)\s+\(([0-9]+\.[0-9]+)(\.[0-9]+)?\)$")
re_parse_script_running = re.compile(r"\d{2}\sRunning (PRE|POST) script of (?:Job|Node) (.+)\.{3}")
re_parse_script_done = re.compile(r"\d{2}\s(PRE|POST) Script of (?:Job|Node) (\S+)")
re_parse_script_successful = re.compile(r"completed successfully\.$")
re_parse_script_failed = re.compile(r"failed with status\s+(-?\d+)\.?$")
re_parse_job_failed = re.compile(r"\d{2}\sNode (\S+) job proc \(([0-9\.]+)\) failed with status\s+(-?\d+)\.$")
re_parse_job_successful = re.compile(r"\d{2}\sNode (\S+) job proc \(([0-9\.]+)\) completed successfully\.$")
re_parse_retry = re.compile(r"Retrying node (\S+) \(retry \#(\d+) of (\d+)\)")
re_parse_dagman_finished = re.compile(r"\(condor_DAGMAN\)[\w\s]+EXITING WITH STATUS (\d+)$")
re_parse_dagman_pid = re.compile(r"\*\* PID = (\d+)$")
re_parse_condor_version = re.compile(r"\*\* \$CondorVersion: ((\d+\.\d+)\.\d+)")
re_parse_condor_logfile = re.compile(r"Condor log will be written to ([^,]+)")
re_parse_condor_logfile_insane = re.compile(r"\d{2}\s{3,}(\S+)")
re_parse_multiline_files = re.compile(r"All DAG node user log files:")

# Constants
debug_level = 0			# For now
logbase = "tailstatd.log" 	# Basename of daemon logfile
brainkeys = {}
brainkeys["required"] = ["basedir", "vogroup", "label", "rundir"]
brainkeys["optional"] = ["dax", "dag", "jsd", "run", "pegasushome"]
good_rsl = {"maxcputime": 1, "maxtime":1, "maxwalltime": 1}
speak = "TSSP/1.0"
MAXLOGFILE = 1000		# For log rotation, check files from .000 to .999

# Events that constitute a pending job. Not that event SUBMIT is
# excluded on purpose since due to throttling inside DAGMan and
# Condor-G, a locally SUBMITted job may only become remotely
# GLOBUS_SUBMITted as throttles permits
pending_job_events = {"GLOBUS_SUBMIT": 1,
		      "GRID_SUBMIT": 1}

# Events that constitute a running job
running_job_events = {"EXECUTE": 1,
		      "GLOBUS_RESOURCE_DOWN": 1,
		      "GLOBUS_RESOURCE_UP": 1,
		      "JOB_SUSPENDED": 1,
		      "JOB_UNSUSPENDED": 1}

unsubmitted_events = {"UN_READY": 1,
		      "PRE_SCRIPT_STARTED": 1,
		      "PRE_SCRIPT_SUCCESS": 1,
		      "PRE_SCRIPT_FAILURE": 1}

# Global variables
start = int(time.time()) 	# start time for total duration
wf = None			# instance of workflow class
line = 0 			# line number from DAGMan debug file
timestamp = 0 			# time stamp from log file
pid = 0 			# DAGMan's pid -- set later
replay_mode = 0			# disable checking if DAGMan's pid is gone
database = 1			# flag for using the database
subfn = {} 			# all submit files of not-done jobs
seen = {} 			# how often a job was seen
pending = {} 			# remember when GLOBUS_SUBMIT was entered
				# jid --> [stamp, addon, wtime, site]
running = {}			# ditto for EXECUTE for hidden starvation
				# jid --> [stamp, addon, wtime, site]
done = {}			# jid --> stamp
flag = {}			# jid --> retries
job_site = {}			# last site a job was planned for
jobstate = {}			# jid --> [stamp, event, addon, wtime, site]
siteinfo = {}			# site --> { [RPSF] --> [ #, mtime] }
walltime = {}			# jid --> walltime
waiting = {}			# site --> stamp/60 --> [ #P->R, sum(ptime)]
remove = {}			# used with shelve, dabase of removed Condor jobs

# Revision handling
revision = "$Revision$" # Let cvs handle this, do not edit manually

# Remaining variables
out = None			# .dag.dagman.out file
run = None			# run directory
server = None			# server socket
sockfn = None			# socket filename
condorlog = None		# Condor common logfile
terminate = None		# Keep track of terminate condition (and errors for exit code)
condor_version = 0		# Track DAGMan's version
condor_major = 0		# Track DAGMan's major version
multiline_file_flag = False	# Track multiline user log files, DAGMan > 6.6
workdb = None			# Instance of the database

class Job:
    """
    Class used to keep information needed to track a particular job
    """

    # Variables that describe a job, as per the Stampede schema
    # Some will be initialized in the init method, others will
    # get their values from the kickstart output file when a job
    # finished
    _wf_uuid = None
    _condor_id = None
    _name = None
    _jobtype = None
    _clustered = None
    _site_name = None
    _remote_user = None
    _remote_working_dir = None
    _cluster_start_time = None
    _cluster_duration = None
    _job_state = None
    _job_state_timestamp = None

    def __init__(self, wf_uuid, name, condor_id):
	"""
	This function initializes the job parameters with the information
	available when a job is detected "SUBMIT" state. Other parameters
	will remain None until a job finishes and a kickstart output file
	can be parsed.
	"""
	self._wf_uuid = wf_uuid
	self._name = name
	self._condor_id = condor_id

    def set_job_state(self, job_state, timestamp):
	"""
	This function sets the job state for this job
	"""
	self._job_state = job_state
	self._job_state_timestamp = timestamp

class Workflow:
    """
    Class used to keep everything needed to track a particular workflow
    """

    # Variables that describe a workflow, as per the Stampede schema
    # These are initialized in the init method
    _wf_uuid = None
    _dax_label = None
    _timestamp = None
    _submit_hostname = None
    _submit_dir = None
    _planner_arguments = None
    _user = None
    _grid_dn = None
    _planner_version = None
    _parent_workflow_id = None
    _jobs_map = {}
    _jobs = {}

    def __init__(self, wfparams=None):
	"""
	This function initializes the workflow parameters according to the
	keys present in wfparams (if provided). Parameters not present in
	wfparams will remain as None
	"""
	if wfparams is not None:
	    if "wf_uuid" in wfparams:
		if wfparams["wf_uuid"] is not None:
		    self._wf_uuid = wfparams["wf_uuid"]
	    # If _wf_uuid is not defined, we create a random uuid for this workflow
	    if self._wf_uuid is None:
		self._wf_uuid = uuid.uuid4()
	    if "label" in wfparams:
		self._timestamp = wfparams["label"]
	    if "pegasus_wf_time" in wfparams:
		self._timestamp = wfparams["pegasus_wf_time"]
	    if "run" in wfparams:
		self._submit_dir = wfparams["run"]
	    if "pegasus_version" in wfparams:
		self._planner_version = wfparams["pegasus_version"]

    def db_send_wf_info(self):
	"""
	This function sends to the DB information about the workflow
	"""
	# Start empty
	kwargs = {}
	# Make sure we include the wf_uuid
	kwargs["wf_uuid"] = self._wf_uuid
	# Now include others, if they are defined
	if self._dax_label is not None:
	    kwargs["dax_label"] = self._dax_label
	if self._timestamp is not None:
	    kwargs["timestamp"] = self._timestamp
	if self._submit_hostname is not None:
	    kwargs["submit_hostname"] = self._submit_hostname
	if self._submit_dir is not None:
	    kwargs["submit_dir"] = self._submit_dir
	if self._planner_arguments is not None:
	    kwargs["planner_arguments"] = self._planner_arguments
	if self._user is not None:
	    kwargs["user"] = self._user
	if self._grid_dn is not None:
	    kwargs["grid_dn"] = self._grid_dn
	if self._planner_version is not None:
	    kwargs["planner_version"] = self._planner_version
	if self._parent_workflow_id is not None:
	    kwargs["parent_workflow_id"] = self._parent_workflow_id

	# Send workflow event to database
	workdb.write(event="workflow", **kwargs)

    def add_job(self, jobid, condor_id, job_state, timestamp):
	"""
	This function adds a new job to our list of jobs. It
	also sets the job state to job_state.
	"""
	# Make sure job is not already there
	if (jobid, condor_id) in self._jobs:
	    logmsg("warning: trying to add job twice: %s, %s" % (jobid, condor_id))
	    return

	# Create new job container
	new_job = Job(self._wf_uuid, jobid, condor_id)
	# Set job state
	new_job.set_job_state(job_state, timestamp)
	# Add job to our list of jobs
	self._jobs[jobid, condor_id] = new_job

	# Add/Update job in our job map
	self._jobs_map[jobid] = condor_id

    def db_send_job_info(self, jobid, condor_id):
	"""
	This function sends to the DB information about a particular job
	"""
	# Start empty
	kwargs = {}

	# Find job
	if not (jobid, condor_id) in self._jobs:
	    logmsg("warning: cannot find job: %s, %s" % (jobid, condor_id))
	    return
	# Got it
	my_job = self._jobs[jobid, condor_id]

	# Make sure we include the wf_uuid, name, and condor_id
	kwargs["wf_uuid"] = my_job._wf_uuid
	kwargs["name"] = my_job._name
	kwargs["condor_id"] = my_job._condor_id
	if my_job._jobtype is not None:
	    kwargs["jobtype"] = my_job._jobtype
	if my_job._clustered is not None:
	    kwargs["clustered"] = my_job._clustered
	if my_job._site_name is not None:
	    kwargs["site_name"] = my_job._site_name
	if my_job._remote_user is not None:
	    kwargs["remote_user"] = my_job._remote_user
	if my_job._remote_working_dir is not None:
	    kwargs["remote_working_dir"] = my_job._remote_working_dir
	if my_job._cluster_start_time is not None:
	    kwargs["cluster_start_time"] = my_job._cluster_start_time
	if my_job._cluster_duration is not None:
	    kwargs["cluster_duration"] = my_job._cluster_duration
	
	# Send job event to database
	workdb.write(event="job", **kwargs)

    def update_job_state(self, jobid, condor_id, job_state, timestamp):
	"""
	This function updates a job's state
	"""
	# Find job
	if condor_id is None:
	    # Need to get condor_id from our hash table
	    if jobid in self._jobs_map:
		condor_id = self._jobs_map[jobid]
	if not (jobid, condor_id) in self._jobs:
	    logmsg("warning: cannot find job: %s, %s" % (jobid, condor_id))
	    return
	# Got it
	my_job = self._jobs[jobid, condor_id]
	# Update job state
	my_job.set_job_state(job_state, timestamp)

    def db_send_job_state(self, jobid, condor_id):
	"""
	This function sends to the DB job state information for a particular job
	"""
	# Start empty
	kwargs = {}

	# Find job
	if condor_id is None:
	    # Need to get condor_id from our hash table
	    if jobid in self._jobs_map:
		condor_id = self._jobs_map[jobid]
	if not (jobid, condor_id) in self._jobs:
	    logmsg("warning: cannot find job: %s, %s" % (jobid, condor_id))
	    return
	# Got it
	my_job = self._jobs[jobid, condor_id]

	# Make sure we include the wf_uuid, name, and condor_id
	kwargs["wf_uuid"] = my_job._wf_uuid
	kwargs["name"] = my_job._name
	kwargs["condor_id"] = my_job._condor_id
	kwargs["state"] = my_job._job_state
	kwargs["timestamp"] = my_job._job_state_timestamp

	# Send job state event to database
	workdb.write(event="jobstate", **kwargs)

def make_boolean(value):
    # purpose: convert an input string into something boolean
    # paramtr: $x (IN): a property value
    # returns: 0 (false) or 1 (true)
    my_val = str(value)
    if (my_val.lower() == 'true' or
	my_val.lower() == 'on' or
	my_val.lower() == 'yes' or
	my_val.isdigit() and int(value) > 0):
	return 1

    return 0

# Parse properties
props = properties.Properties()
props.new(properties.PARSE_ALL)
doplot = make_boolean(props.property("pegasus.tailstatd.show"))

fuse = int(props.property("pegasus.tailstatd.fuse") or 300)
if fuse < 60:
    fuse = 60

# Remove jobs after xxx seconds in PENDING
idletime = props.property("pegasus.max.idletime")
starvation = int(idletime or 0)		# Default is 0, which disables starvation checks

last_check = int(time.time())		# marker for starvation checks
jsd = None				# location of jobstate.log file
nodaemon = 0				# foreground mode
logfile = None				# location of tailstatd.log file
millisleep = None			# emulated run mode delay
job_check_interval = 300		# interval to check for starvation
liedetect = props.property("pegasus.max.runtime.multiplier")
liedetector = float(liedetect or 5.0)	# Remove after 5.0 times walltime in RUN

config = {}				# braindump database (textual file)
adjustment = 0				# time zone adjustment (@#~! Condor)

# Parse command line options
prog_usage = "usage: %s [options] workflow.dag.dagman.out" % (prog_base)
prog_desc = """Mandatory arguments: outfile is the log file produced by Condor DAGMan, usually ending in the suffix ".dag.dagman.out"."""

parser = optparse.OptionParser(usage=prog_usage, description=prog_desc)

parser.add_option("-d", "--debug", action = "store", type = "int", dest = "debug_level",
		  help = "accumulative, add more messages as repeated, default level %d dynamic adjustments via signals USR1 (incr) and USR2 (decr)"
		  % (debug_level))
parser.add_option("-a", "--adjust", action = "store", type = "int", dest = "adjustment",
		  help = "adjust for time zone differences by i seconds, default 0")
parser.add_option("-c", "--check", action = "store", type = "int", dest = "job_check_interval",
		  help = "check for starvation every i seconds, default %d s" % (job_check_interval))
parser.add_option("-s", "--starve", action = "store", type = "int", dest = "starvation",
		  help = "remove job from Condor after n second spent PENDING remotely, use 0 to disable, defaults to %d s"
		  % (starvation))
parser.add_option("-N", "--foreground", action = "store_const", const = 2, dest = "nodaemon",
		  help = "(Condor) don't daemonize %s; go through motions as if" % (prog_base))
parser.add_option("-n", "--no-daemon", action = "store_const", const = 1, dest = "nodaemon",
		  help = "(debug) don't daemonize %s; keep it in the foreground" % (prog_base))
parser.add_option("--show", action = "store_const", const = 1, dest = "doplot",
		  help = "create diagrams of workflow upon normal exit")
parser.add_option("--fuse", action = "store", type = "int", dest = "fuse",
		  help = "maximum wait for each plotting subscript, default %d s" % (fuse))
parser.add_option("-j", "--job", action = "store", type = "string", dest = "jsd",
		  help = "alternative job state file to write, default is %s in the workflow's directory"
		  % (utils.jobbase))
parser.add_option("-l", "--log", action = "store", type = "string", dest = "logfile",
		  help = "alternative %s log file, default is %s in the workflow's directory"
		  % (prog_base, logbase))
parser.add_option("-C", "--config", action = "append", type = "string", dest = "config_opts",
		  help = "k=v defines configurations instead of reading from braindump.txt. Required keys include %s. Suggested keys include %s"
		  % (brainkeys["required"], brainkeys["optional"]))
parser.add_option("-D", "--database", action = "store_const", const = 1, dest = "database",
		  help = "Turn on Database entries for work DB")
parser.add_option("--nodatabase", action = "store_const", const = 0, dest = "database",
		  help = "Turn off Database entries for work DB")
parser.add_option("-S", "--sim", action = "store", type = "int", dest = "millisleep",
		  help = "Developer: simulate delays between reads by sleeping ms milliseconds")
parser.add_option("-r", "--replay", action = "store_const", const = 1, dest = "replay_mode",
		  help = "disables checking for DAGMan's pid while running %s" % (prog_base))

# Re-insert our base name to avoid optparse confusion when printing error messages
# (options, args) = parser.parse_args(sys.argv[0:]) # Does not work 100%
sys.argv.insert(0, prog_base)
(options, args) = parser.parse_args()

# Copy command line options into our variables
if options.debug_level is not None:
    debug_level = options.debug_level
if options.adjustment is not None:
    adjustment = options.adjustment
if options.job_check_interval is not None:
    job_check_interval = options.job_check_interval
if options.starvation is not None:
    starvation = options.starvation
if options.nodaemon is not None:
    nodaemon = options.nodaemon
if options.doplot is not None:
    doplot = options.doplot
if options.fuse is not None:
    fuse = options.fuse
if options.jsd is not None:
    jsd = options.jsd
if options.logfile is not None:
    logfile = options.logfile
if options.database is not None:
    database = options.database
if options.millisleep is not None:
    millisleep = options.millisleep
if options.replay_mode is not None:
    replay_mode = options.replay_mode

# Walk through any config properties
if options.config_opts is not None:
    for prop in options.config_opts:
	prop = prop.strip()
	try:
	    k, v = prop.split("=", 1)
	except:
	    parser.print_help()
	    sys.exit(1)
	config[k] = v

# Sanity check
if fuse < 60:
    fuse = 60

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

# Infer information from directory contents

# Infer run directory
run = os.path.dirname(out)

# Sanity check contents, at least for keys
if len(config) > 0:
    for k in brainkeys["required"]:
	if not k in config:
	    logger.fatal("Invalid use of --config option: Missing key %s" % (k))
	    sys.exit(1)
else:
    config = utils.slurp_braindb(run)
    if not config:
	logger.warn("Error opening braindump.txt db!")

if not "run" in config:
    # Add run key to our configuration
    config["run"] = run

# For slow start
sitefn = props.property("pegasus.slow.start")
sitedb = None
if not replay_mode:
    if sitefn:
	sitedb = filelock.Intent()
	sitedb.new(os.path.join(run, sitefn))

# Use default tailstatd logfile if user hasn't specified another file
if not logfile:
    logfile = os.path.join(run, logbase)
logfile = os.path.abspath(logfile)

# Determine location of textual jobstate log
if not jsd:
    jsd = os.path.join(run, utils.jobbase)

if not os.path.isfile(jsd):
    logger.warn("Creating new file %s" % (jsd))

try:
    # Create new file, or append to an existing one
    JSDB = open(jsd, 'a')
except:
    logger.fatal("Error appending to %s!" % (jsd))
    sys.exit(1)

# Check for rescue DAG presence; determine submit file location
dag = out[:out.find(".dagman.out")]
dag = os.path.normpath(os.path.join(run, dag))

try:
    DAG = open(dag, "r")
except:
    logger.warn("ERROR: Unable to read %s!" % (dag))
else:
    for dag_line in DAG:
	if (dag_line.lower()).find("job") >=0:
	    my_match = re_parse_dag_submit_files.search(dag_line)
	    if my_match:
		if not my_match.group(3):
		    # Found submit file for not-DONE job
		    subfn[my_match.group(1)] = [os.path.join(run, my_match.group(2)), -1, -1, -1, -1]
    DAG.close()

# POST-CONDITION: subfn contains only submit-files of jobs that are not yet done
# Normally, this is all submit files.
# In rescue DAGS, that is an arbitrary subset of all jobs.

# Check for condor_rm
condor_rm = utils.find_exec("condor_rm")
if not condor_rm:
    logger.fatal("ERROR: Unable to determine where condor_rm is installed!")
    sys.exit(1)

# Untie exit handler
def untie_exit_handler():
    remove.close()
    if terminate == 0:
	try:
	    os.unlink(rmdb)
	except:
	    # fail silently
	    pass

if not replay_mode:
    # Maintain database of removed Condor jobs (for restarts)
    rmdb = os.path.join(run, "remove.db")
    try:
	remove = shelve.open(rmdb)
    except:
	logger.fatal("Error: Unable to create DB file %s!" % (rmdb))
	sys.exit(1)

    atexit.register(untie_exit_handler)

#
# --- functions ---------------------------------------------------------------------------
#

def sendmsg(client_connection, msg):
    # purpose: send all data to socket connection, try several time if necessary
    # paramtr: client_connection(IN): socket connection to send data
    # paramtr: msg(IN): message to send
    # returns: None on error, 1 on success
    my_total_bytes_sent = 0

    while my_total_bytes_sent < len(msg):
	try:
	    my_bytes_sent = client_connection.send(msg[my_total_bytes_sent:])
	except:
	    logmsg("Error: writing to socket!")
	    return None

	my_total_bytes_sent = my_total_bytes_sent + my_bytes_sent

    return 1

def systell(fh):
    # purpose: make things symmetric, have a systell for sysseek
    # paramtr: fh (IO): filehandle
    # returns: current file position
    os.lseek(fh, 0, os.SEEK_CUR)

def logmsg(*args):
    # Get localtime
    my_now = time.time()
    my_tm = time.localtime(my_now)
    # Format string
    my_msg = ("%4d%02d%02dT%02d%02d%02d.%03d [%d]: " % (
	    my_tm[0], my_tm[1], my_tm[2], my_tm[3], my_tm[4], my_tm[5],
	    1000 * abs(int(my_now)-my_now), line))
    # Add any arguments
    for my_arg in args:
	my_msg += str(my_arg)
	# Print message
	print my_msg

def fatal(msg):
    # purpose: log plus exit with failure
    # paramtr: message to output
    logmsg("FATAL: %s" % (msg))
    sys.exit(42)

def out2log(out):
    # purpose: infer output symlink for Condor common user log
    # paramtr: out (IN): the name of the out file we use
    # globals: run (IN): the run directory
    # returns: the name of the log file to use

    # Get the basename
    my_base = os.path.basename(out)
    # NEW: Account for rescue DAGs
    my_base = my_base[:my_base.find(".dagman.out")]
    my_base = re_remove_extensions.sub('', my_base)
    # Add .log extension
    my_base = my_base + ".log"
    # Create path
    my_log = os.path.join(run, my_base)

    return my_log, my_base

def extract_knowledge(stamp, info):
    # purpose: Extract any walltime and site info from a given submit file
    # paramtr: stamp(IN): timestamp associated with the log line
    # paramtr: info(IN, MODIFIED): [subfn, dev#, ino#, size, mtime] reference
    # globals: good_rsl(IN): which RSL keys constitute time requirements
    # returns: (largest job time requirement in minutes, destination site)
    # returns: (None, None) if info not found

    my_result = None
    my_site = None

    if info is None:
	return my_result, my_site

    my_fn = info[0]

    if my_fn is None:
	return my_result, my_site

    # Update stat record for submit file
    try:
	my_stats = os.stat(my_fn)
    except:
	# Could not stat file
	logmsg("Error: stat %s" % (my_fn))
	return my_result, my_site

    info[1] = my_stats[2] # dev #
    info[2] = my_stats[1] # inode #
    info[3] = my_stats[6] # Size
    info[4] = my_stats[8] # mtime

    if stamp < info[4]:
	if debug_level > 1:
	    logmsg("stamp=%d, mtime=%d, diff = %d" % (stamp, info[4], info[4]-stamp))
	logmsg("skipping %s (reparsing events)" % (my_fn))
	return my_result, my_site

    try:
	SUB = open(my_fn, "r")
    except:
	logmsg("unable to parse %s" % (my_fn))
	return my_result, my_site

    for my_line in SUB:
	if re_rsl_string.search(my_line):
	    # Found RSL string, do parse now
	    for my_match in re.findall(r"\(([^)]+)\)", my_line):
		# Split into key and value
		my_k, my_v = my_match.split("=", 1)
		# Remove _- characters from string
		my_k = re_rsl_clean.sub('', my_k)
		if my_k.lower() in good_rsl and my_v > my_result:
		    try:
			my_result = int(my_v)
		    except:
			my_result = None
	elif re_site_parse_gvds.search(my_line):
	    # GVDS agreement
	    my_site = re_site_parse_gvds.search(my_line).group(4)
	elif re_site_parse_euryale.search(my_line):
	    # Euryale specific comment
	    my_site = re.site_parse_euryale.search(my_line).group(1)

    SUB.close()

    return my_result, my_site

def aggregate(site, stamp, pending):
    # purpose: aggregates pending information into raster intervals
    # paramtr: site(IN): run site
    # paramtr: stamp(IN): current timestamp
    # paramtr: pending(IN): pending record

    my_slot = int(stamp / 60)
    my_diff = abs(stamp - pending[0])

    # FIXME: Insert clean-up code here to remove any but last four slots

    # Check if keys are in dictionary
    if not site in waiting:
	waiting[site] = {}
    if not my_slot in waiting[site]:
	waiting[site][my_slot] = [0, 0]

    # Aggregate information
    waiting[site][my_slot][0] = waiting[site][my_slot][0] + 1
    waiting[site][my_slot][1] = waiting[site][my_slot][1] + my_diff

    if debug_level > 1:
	my_n = waiting[site][my_slot][0]
	logmsg("%s:%s %s / %d = %.3f" % (site, slot, my_diff, my_n, my_diff / my_n))

def add(stamp, jobid, event, condor_id=None, addon=None):
    # purpose: append atomically a line to the jobstate file
    # paramtr: stamp(IN): time stamp when the state change was seen
    # paramtr: jobid(IN): what job
    # paramtr: event(IN): new status of job
    # paramtr: condor_id(IN, OPT): condor id
    # paramtr: addon(IN, OPT): additional data
    # returns: Nothing

    my_site = None
    my_time = None
    my_info = None

    # Remove existing site info during replanning
    if event in unsubmitted_events:
	if jobid in job_site:
	    del job_site[jobid]
	if jobid in walltime:
	    del walltime[jobid]

    # Variables originally from submit file information
    if jobid in job_site:
	my_site = job_site[jobid]
    if jobid in walltime:
	my_time = walltime[jobid]

    # Obtain planning information from submit file when entering Condor
    if event == "SUBMIT":
	# Figure out how long the job _intends_ to run maximum
	if jobid in subfn:
	    my_info = subfn[jobid]
	my_time, my_site = extract_knowledge(stamp, my_info)

	if my_site == "!!SITE!!":
	    my_site = None

	# If not None, convert into seconds
	if my_time is not None:
	    my_time = my_time * 60
	    if debug_level > 0:
		logmsg("job %s requests %d s walltime" % (jobid, my_time))
	    walltime[jobid] = my_time
	else:
	    if debug_level > 0:
		logmsg("job %s does not request a walltime" % (jobid))

	# Remember the run-site
	if my_site is not None:
	    if debug_level > 0:
		logmsg("job %s is planned for site %s" % (jobid, my_site))
	    job_site[jobid] = my_site
	else:
	    logmsg("job %s does not have a site information!" % (jobid))

	# Add job to our workflow
	wf.add_job(jobid, condor_id, event, stamp)
	if database == 1:
	    # Send job event to database
	    wf.db_send_job_info(jobid, condor_id)

    # Make sure job has the updated state
    wf.update_job_state(jobid, condor_id, event, stamp)
    if database == 1:
	# Send jobstate event to database
	wf.db_send_job_state(jobid, condor_id)

    # Remember when we changed into a pending state
    if event in pending_job_events:
	if not jobid in pending:
	    # Remember when -- and which Condor ID
	    pending[jobid] = [stamp, addon, my_time, my_site]
	else:
	    if debug_level > 1:
		logmsg("%s remains a pending event for %s" % (event, jobid))
    else:
	# Remember time spent in pending, if previous state was pending
	if jobid in jobstate:
	    if jobstate[jobid][1] in pending_job_events and jobid in pending:
		aggregate(my_site, stamp, pending[jobid])

	# Remove when transitioning into any other state
	if jobid in pending:
	    del pending[jobid]

    # Remember when we changed into a running state
    if event in running_job_events:
	if not jobid in running:
	    # Remember when -- and which Condor ID
	    running[jobid] = [stamp, addon, my_time, my_site]

	    # Increase the window size when in slow start
	    # WARNING: This may be blocking forever!!!!!
	    if (sitedb is not None and
		my_site is not None and
		my_site in siteinfo and
		jobid in siteinfo[my_site] and
		siteinfo[my_site][jobid] == 'P'):
		my_n = sitedb.inc(my_site)
		logmsg("new window size for %s is %d" % (my_site, my_n))
	else:
	    if debug_level > 1:
		logmsg("%s remains a running event for %s" % (event, jobid))
    else:
	# Remove when transitioning into any other state
	if jobid in running:
	    del running[jobid]

    # Create content -- use one space only
    my_line = "%d %s %s %s %s %s" % (stamp, jobid, event, addon or '-', my_site or '-', my_time or '-')
    if debug_level > 1:
	logmsg("new state %s" % (my_line))

    # Prepare for atomic append
    JSDB.write("%s\n" % (my_line))
    jobstate[jobid] = [stamp, event, addon, my_time, my_site]

    # Send jobstate event to database
    if database == 1:
	pass

    # NEW: maintain site statistics
    if my_site is not None:
	if event in pending_job_events:
	    my_state = 'P'
	elif event in running_job_events:
	    my_state = 'R'
	elif event == "POST_SCRIPT_SUCCESS":
	    my_state = 'S'
	elif event == "POST_SCRIPT_FAILURE":
	    my_state = 'F'
	else:
	    my_state = 'O'
    else:
	my_state = None

    if my_site in siteinfo and jobid in siteinfo[my_site]:
	my_old = siteinfo[my_site][jobid]
	if my_old != my_state:
	    # Job changed state
	    if my_old != 'S' and my_old != 'F':
		# Gauge
		if my_old in siteinfo[my_site]:
		    siteinfo[my_site][my_old] = siteinfo[my_site][my_old] - 1
		else:
		    # Should not have to do this!
		    siteinfo[my_site][my_old] = 0
	    if my_state in siteinfo[my_site]:
		siteinfo[my_site][my_state] = siteinfo[my_site][my_state] + 1
	    else:
		siteinfo[my_site][my_state] = 1
    else:
	if my_site is not None:
	    if not my_site in siteinfo:
		# First add first-level dictionary
		siteinfo[my_site] = {}
	    # Add new state
	    siteinfo[my_site][my_state] = 1

    if my_site is not None:
	if my_state == 'S':
	    siteinfo[my_site]["mtime_succ"] = stamp
	if my_state == 'F':
	    siteinfo[my_site]["mtime_fail"] = stamp
	siteinfo[my_site][jobid] = my_state
	siteinfo[my_site]["mtime"] = stamp

    # Count how many times this job changed states
    if jobid in seen:
	seen[jobid] = seen[jobid] + 1
    else:
	seen[jobid] = 1

    return

def process(log_line):
    # purpose: process a log line and look for the information we are interested in
    # paramtr: log_line(IN): the line
    # globals: timestamp(OUT): maintains latest timestamp from DAGMan
    # returns: timestamp
    global multiline_file_flag, terminate, pid, condorlog, condor_version, condor_major, timestamp

    # Strip end spaces, tabs, and <cr> and/or <lf>
    log_line = log_line.rstrip()

    # Check log_line for timestamp at the beginning
    my_expr = re_parse_timestamp.search(log_line)

    if my_expr is not None:
	if debug_level > 2:
	    split_log_line = log_line.split(None, 3)
	    if len(split_log_line) >= 3:
		logger.warn("## %d: %s" % (line, split_log_line[2][:64]))
	
	# Found time stamp, let's assume valid log line
	curr_time = time.localtime()
	adj_time = list(curr_time)
	adj_time[1] = int(my_expr.group(1)) # Month
	adj_time[2] = int(my_expr.group(2)) # Day
	adj_time[3] = int(my_expr.group(3)) # Hours
	adj_time[4] = int(my_expr.group(4)) # Minutes
	adj_time[5] = int(my_expr.group(5)) # Seconds

	timestamp = time.mktime(adj_time) + adjustment

	# Search for more content
	if re_parse_event.search(log_line) is not None:
	    # Found Event
	    my_expr = re_parse_event.search(log_line)
	    # groups = jobid, event, condor
	    add(timestamp, my_expr.group(2), my_expr.group(1), condor_id=my_expr.group(3), addon=my_expr.group(3))
	elif re_parse_script_running.search(log_line) is not None:
	    # Pre scripts are not a regular Condor event
	    # Starting of scripts is not a regular Condor event
	    my_expr = re_parse_script_running.search(log_line)
	    # groups = script, jobid
	    add(timestamp, my_expr.group(2), "%s_SCRIPT_STARTED" % (my_expr.group(1).upper()))
	elif re_parse_script_done.search(log_line) is not None:
	    my_expr = re_parse_script_done.search(log_line)
	    # groups = script, jobid
	    my_script = my_expr.group(1).upper()
	    my_jobid = my_expr.group(2)
	    if re_parse_script_successful.search(log_line) is not None:
		# Remember success with artificial jobstate
		add(timestamp, my_jobid, "%s_SCRIPT_SUCCESS" % (my_script))
		if my_script == "POST":
		    done[my_jobid] = timestamp
	    elif re_parse_script_failed.search(log_line) is not None:
		# Remember failure with artificial jobstate
		my_expr = re_parse_script_failed.search(log_line)
		# groups = exit code (error status)
		add(timestamp, my_jobid, "%s_SCRIPT_FAILURE" % (my_script), addon=my_expr.group(1))
		try:
		    my_exit_code = int(my_expr.group(1))
		except:
		    # Unable to convert exit code to integer -- should not happen
		    logger.warn("unable to convert exit code to integer!")
		    my_exit_code = 0
		if my_exit_code == 42 or my_jobid in flag and flag[my_jobid] > 0:
		    # Detect permanent failure
		    logmsg("detected permanent failure for %s" % (my_jobid))
		    if len(done) == 0:
			logmsg("Warning: No successful jobs so far!")
	    else:
		# Ignore
		logmsg("warning: unknown pscript state: %s" % (log_line[-14:]))
	elif re_parse_job_failed.search(log_line) is not None:
	    # Job has failed
	    my_expr = re_parse_job_failed.search(log_line)
	    # groups = jobid, condorid, jobstatus
	    my_jobid = my_expr.group(1)
	    my_condorid = my_expr.group(2)
	    my_jobstatus = my_expr.group(3)
	    # remember failure with artificial jobstate
	    add(timestamp, my_jobid, "JOB_FAILURE", condor_id=my_condorid, addon=my_jobstatus);
	elif re_parse_job_successful.search(log_line) is not None:
	    # Job succeeded
	    my_expr = re_parse_job_successful.search(log_line)
	    my_jobid = my_expr.group(1)
	    my_condorid = my_expr.group(2)
	    # remember success with artificial jobstate
	    add(timestamp, my_jobid, "JOB_SUCCESS", condor_id=my_condorid, addon=my_condorid);
	elif re_parse_retry.search(log_line) is not None:
	    # Found a retry, save maxed-out retried for later
	    my_expr = re_parse_retry.search(log_line)
	    if my_expr.group(2) == my_expr.group(3):
		try:
		    flag[my_expr.group(1)] = int(my_expr.group(3))
		except:
		    # Cannot convert retry number to integer
		    logger.warn("could not convert retry number to integer!")
	elif re_parse_dagman_finished.search(log_line) is not None:
	    # DAG finished -- done parsing
	    my_expr = re_parse_dagman_finished.search(log_line)
	    # groups = exit code
	    try:
		terminate = int(my_expr.group(1))
	    except:
		# Cannot convert exit code to integer!
		logger.warn("cannot convert DAGMan's exit code to integer!")
		terminate = 0
	    logmsg("DAGMan finished with exit code %s" % (terminate))
	    JSDB.write("%d INTERNAL *** DAGMAN_FINISHED ***\n" % (timestamp))
	elif re_parse_dagman_pid.search(log_line) is not None:
	    # DAGMan's pid
	    if not replay_mode:
		# Only set pid if not running in replay mode
		# (otherwise pid may belong to another process)
		my_expr = re_parse_dagman_pid.search(log_line)
		# groups = DAGMan's pid
		try:
		    pid = int(my_expr.group(1))
		except:
		    fatal("cannot set pid: %s" % (my_expr.group(1)))
	    logmsg("DAGMan runs at pid %d" % (pid))
	    JSDB.write("%d INTERNAL *** DAGMAN_STARTED ***\n" % (timestamp))
	elif re_parse_condor_version.search(log_line) is not None:
	    # Version of this logfile format
	    my_expr = re_parse_condor_version.search(log_line)
	    # groups = condor version, condor major
	    condor_version = my_expr.group(1)
	    condor_major = my_expr.group(2)
	    logmsg("Using DAGMan version %s" % (condor_version))
	elif (re_parse_condor_logfile.search(log_line) is not None or
	      multiline_file_flag == True and re_parse_condor_logfile_insane.search(log_line) is not None):
	    # Condor common log file location, DAGMan 6.6
	    if re_parse_condor_logfile.search(log_line) is not None:
		my_expr = re_parse_condor_logfile.search(log_line)
	    else:
		my_expr = re_parse_condor_logfile_insane.search(log_line)
	    condorlog = my_expr.group(1)
	    logmsg("Condor writes its logfile to %s" % (condorlog))

	    # Make a symlink for NFS-secured files
	    my_log, my_base = out2log(out)
	    if os.path.islink(my_log):
		logmsg("Symlink %s already exists" % (my_log))
	    elif os.access(my_log, os.R_OK):
		logmsg("%s is a regular file, not touching" % (my_base))
	    else:
		logmsg("Trying to create local symlink to common log")
		if os.access(condorlog, os.R_OK) or not os.access(condorlog, os.F_OK):
		    if os.access(my_log, os.R_OK):
			try:
			    os.rename(my_log, "%s.bak" % (my_log))
			except:
			    logger.warn("Error: renaming %s to %s.bak" % (my_log, my_log))
		    try:
			os.symlink(condorlog, my_log)
		    except:
			logmsg("unable to symlink %s" % (condorlog))
		    else:
			logmsg("symlink %s -> %s" % (condorlog, my_log))
		else:
		    logmsg("%s exists but is not readable!" % (condorlog))
	    # We only expect one of such files
	    multiline_file_flag = False
	elif re_parse_multiline_files.search(log_line) is not None:
	    # Multiline user log files, DAGMan > 6.6
	    multiline_file_flag = True

    # Done
    return timestamp

def condor_remove(*args):
    # purpose: run condor_rm on one or more Condor IDs
    # paramtr: args(IN) one or more valid Condor IDs
    # globals: remove(IN/OUT) maintain removals to turn forceful
    # returns: number of successful condor_rm operations
    my_result = 0

    for condor in args:
	if condor.count('.') > 1:
	    # Implement perl's basename function with 2nd argument
	    condor = os.path.basename(condor)
	    if condor.rfind(".0") > 0:
		condor = condor[0:condor.rfind(".0")]

	if re_condor_id.search(condor) is None:
	    logmsg('Invalid condor id "%s"' % (condor))
	    continue

	my_cmd = condor_rm
	if condor in remove:
	    if remove[condor]  > 10:
		my_cmd = my_cmd + " -f"

	my_cmd = my_cmd + " " + condor

	try:
	    my_status, my_output = commands.getstatusoutput(my_cmd)
	except:
	    logmsg("unable to execute %s" % (condor_rm))
	else:

	    # Parse output
	    for my_line in my_output[1].split("\n"):
		logmsg("condor_rm: %s" % (my_line))
		if re_condor_rm_output.search(my_line) is not None:
		    if condor in remove:
			remove[condor] = remove[condor] + 1
		    else:
			remove[condor] = 1

	    if my_status == 0:
		my_result = my_result + 1
		if condor in remove:
		    del remove[condor]

    return my_result

def check_starvation(my_now=int(time.time())):
    # purpose: check if there are starving jobs
    # paramtr: my_now(IN, OPT) current time to check against
    # globals: pending(IN) used to determine starving jobs
    # globals: running(IN) used to determine hidden starvation
    # returns: Nothing

    # Skip starvation checks for Pegasus
    if starvation == 0:
	return

    if debug_level > 1:
	logmsg("Start: checking for starvation")

    # Candidates entered GLOBUS_SUBMIT more than starvation/3 ago
    my_warn = starvation / 3

    # Start with the worst case first
    for my_job in sorted(pending.items(), key=operator.itemgetter(1)):
	my_vals = my_job[1]
	# Skip ones that are not too old
	if my_now - my_vals[0]  <= my_warn:
	    continue
	my_diff = my_now - my_vals[0]
	my_condor = my_vals[1][0:-2]
	logmsg("%s (%s) is pending for %d" % (my_job, my_condor, my_diff))

	# If pending for more than starvation ago, remove job from Condor
	if my_diff > starvation:
	    logmsg("removing idle job %s (%s) from queue due to limit reached" % (my_job, my_condor))
	    condor_remove(my_condor)

    # Candidates for hidden starvation entered EXECUTE a lot longer ago
    # than they intended to obtain walltime for. However, we can do this
    # check only for "nice" jobs that declare their walltime in Globus RSL!

    # Skip walltime starvation execution checks for Pegasus
    if liedetector == 0:
	return

    my_n = 0.75 * liedetector
    for my_job in sorted(running.items(), key=operator.itemgetter(1)):
	my_vals = my_job[1]
	# Nothing we can do about secretive jobs without RSL
	if my_vals[2] is None:
	    continue
	# Skip those that did not run over
	if my_now - my_vals[0] <= my_n * my_vals[2]:
	    continue
	my_diff = my_now - my_vals[0]
	my_condor = my_vals[1]
	my_wall = my_vals[2]
	logmsg("%s (%s) execution: requested=%d s, runtime=%d s (%.1f)" %
	       (my_job, my_condor, my_wall, my_diff, 1.0 * my_diff / my_wall))

	# Hidden starvation for more than my_n * my_wall and 10 minutes
	# JSV: 20050419: added constrain of 10min minimum wait
	if (my_diff > liedetector * my_wall and
	    my_diff > 600):
	    condor_remove(my_condor)

    if debug_level > 1:
	logmsg("Done: checking for starvation")

def server_socket(low, hi, bind_addr="127.0.0.1"):
    # purpose: create a local TCP server socket to listen to sitesel requests
    # paramtr: low (IN): minimum port from bind range
    # paramtr: hi (IN): maximum port from bind range
    # paramtr: bind_addr (IN): optional hostaddr_in to bind to , defaults to LOOPBACK
    # returns: open socket, or None on error

    # Create socket
    try:
	my_socket = socket.socket(socket.AF_INET,
				  socket.SOCK_STREAM,
				  socket.getprotobyname("tcp"))
    except:
	fatal("Error: create socket!")

    # Set options
    try:
	my_socket.setsockopt(socket.SOL_SOCKET,
			     socket.SO_REUSEADDR,
			     1)
    except:
	fatal("Error: setsockopt SO_REUSEADDR!")

    # Bind to a free port
    my_port = low
    for my_port in range(low, hi):
	try:
	    my_res = my_socket.bind((bind_addr, my_port))
	except:
	    # Go to next port
	    continue
	else:
	    break

    if my_port >= hi:
	fatal("Error: No free port to bind to!")

    # Make server socket non-blocking to not have a race condition
    # when doing select() before accept() on a server socket
    try:
	my_socket.setblocking(0)
    except:
	fatal("Error: setblocking!")

    # Start listener
    try:
	my_socket.listen(socket.SOMAXCONN)
    except:
	fatal("Error: Listen!\n")

    # Return socket
    return my_socket

def show_job(client_conn, jid, ref_array=None):
    # purpose: print job information onto the given socket descriptor
    # paramtr: client_conn(IN): socket connection to client
    # paramtr: jid(IN): job id from jobstate
    # paramtr: refarray(IN, OPT): array from jobstate
    # globals: jobstate(IN): jobstate dictionary
    # returns: None on error, 1 on success

    # Get refarray from jobstate if not provided
    if ref_array is None:
	if jid in jobstate:
	    ref_array = jobstate[jid]
	else:
	    return None

    my_line = "%s %u %s %s %s %s\r\n" % (jid, 
					 ref_array[0],
					 ref_array[1],
					 ref_array[2] or '-',
					 ref_array[3] or '-',
					 ref_array[4] or '-')

    return sendmsg(client_conn, my_line)

def service_request_job(client_conn, job=None):
    # purpose: service a request for a job status
    # paramtr: client_conn(IN): socket connection to client
    # paramtr: job(IN, OPT): Either an asterix "*" for all (default), or a specific job
    # returns: number of entries written

    my_count = 0

    if (job is None) or job == '*':
	# All jobs, this is an optimization over regexp match below
	for my_key in jobstate:
	    if show_job(client_conn, my_key, jobstate[my_key]) is None:
		break
	    my_count = my_count + 1
    elif job in jobstate:
	# Looking for a specific job
	if show_job(client_conn, job, jobstate[job]) is not None:
	    my_count = my_count + 1
    else:
	# Treat job as a regular expression
	for my_key in jobstate:
	    if re.search(job, my_key):
		# Match!
		if show_job(client_conn, my_key, jobstate[my_key]) is None:
		    break
		my_count = my_count + 1

    return my_count

def show_site(client_conn, site, site_values=None):
    # purpose: print site information onto the given socket descriptor
    # paramtr: client_conn(IN): socket connection to client
    # paramtr: site(IN): site id from siteinfo
    # paramtr: site_values(IN, OPT): array from siteinfo
    # globals: siteinfo(IN): site dictionary
    # globals: waiting(IN): history of P->R transitions
    # returns: None on error, 1 on success

    # Get site_values from jobstate if not provided
    if site_values is None:
	if site in siteinfo:
	    site_values = siteinfo[site]
	else:
	    return None

    if "mtime_fail" in site_values:
	my_mtime_fail = utils.isodate(site_values["mtime_fail"], False, True)
    else:
	my_mtime_fail = "null"

    if "mtime_succ" in site_values:
	my_mtime_succ = utils.isodate(site_values["mtime_succ"], False, True)
    else:
	my_mtime_succ = "null"

    if "P" in site_values:
	my_state_p = int(site_values["P"])
    else:
	my_state_p = 0
    if "R" in site_values:
	my_state_r = int(site_values["R"])
    else:
	my_state_r = 0
    if "O" in site_values:
	my_state_o = int(site_values["O"])
    else:
	my_state_o = 0
    if "S" in site_values:
	my_state_s = int(site_values["S"])
    else:
	my_state_s = 0
    if "F" in site_values:
	my_state_f = int(site_values["F"])
    else:
	my_state_f = 0

    my_line = "%-16s %-21s %4u %4u %4u %6u %6u %-20s %s\r\n" % (site,
								utils.isodate(site_values["mtime"], False, True),
								my_state_p,
								my_state_r,
								my_state_o,
								my_state_s,
								my_state_f,
								my_mtime_succ,
								my_mtime_fail)

    # Check if we output to a file or to a socket
    if type(client_conn).__name__ == "file":
	# Write information to a file
	client_conn.write(my_line)
    else:
	# Send information to client (type(client_conn).__name__ should be _socketobject)
	if sendmsg(client_conn, my_line) is None:
	    return None

    # No need to proceed if nothing in waiting
    if not site in waiting:
	return 1

    # Sort dictionary and create a list sorted by timestamp (reverse)
    my_sorted_vals = sorted(waiting[site].items(), reverse=True)

    # Sort dictionary by value (not key) and create a list, this is
    # not what we want to do here, as it will create a list sorted by
    # jobs
    # my_sorted_vals = sorted(waiting[site].items(), key=operator.itemgetter(1))

    for i in range(0, len(my_sorted_vals)):
	my_val = my_sorted_vals[i]
	if my_val[1][0] == 0:
	    my_number = my_val[1][1]
	else:
	    my_number = my_val[1][1] / my_val[1][0]
	my_line = "\t%-20s %lu %lu %.3f\r\n" % (utils.isodate(my_val[0] * 60, False, True),
						my_val[1][0],
						my_val[1][1],
						my_number)
	if type(client_conn).__name__ == "file":
	    # Write to file
	    client_conn.write(my_line)
	else:
	    # type(client_conn).__name__ should be _socketobject
	    if sendmsg(client_conn, my_line) is None:
		return None

    # All done!
    return 1

def service_request_site(client_conn, site=None):
    # purpose: service a request for a site status
    # paramtr: client_conn(IN): socket connection to client, or file object to output to a file
    # paramtr: site(IN, OPT): Either an asterix "*" for all (default), or a specific site
    # returns: number of entries written

    my_count = 0

    if (site is None) or site == '*':
	# All sites, this is an optimization over regexp match below
	for my_key in siteinfo:
	    if show_site(client_conn, my_key, siteinfo[my_key]) is None:
		break
	    my_count = my_count + 1
    elif site in siteinfo:
	# Looking for a specific site
	if show_site(client_conn, site, siteinfo[site]) is not None:
	    my_count = my_count + 1
    else:
	# Treat site as a regular expression
	for my_key in siteinfo:
	    if re.search(site, my_key):
		# Match!
		if show_site(client_conn, my_key, siteinfo[my_key]) is None:
		    break
		my_count = my_count + 1

    return my_count

def untaint(text):
    # purpose: do not trust anything we get from the internet
    # paramtr: text(IN): text to untaint
    # returns: cleaned text, without any "special" characters

    if text is None:
	return None

    my_text = re_clean_content.sub('', str(text))

    return my_text

jumptable = {'site': service_request_site,
	     'job': service_request_job}

def service_request(server):
    # purpose: accept an incoming connection and service its request
    # paramtr: server(IN): server socket with a pending connection request
    # returns: number of status lines, or None in case of error

    # First, we accept the connection
    try:
	my_conn, my_addr = server.accept()
    except:
	logmsg("Error: accept!")
	return None

    my_count = 0
    logmsg("processing request from %s:%d" % (my_addr[0], my_addr[1]))

    # TODO: Can only handle 1 line up to 1024 bytes long, should fix this later
    # Read line fron socket
    while True:
	try:
	    my_buffer = my_conn.recv(1024)
	except socket.error, e:
	    if e[0] == 35:
		continue
	    else:
		logmsg("Error: recv: %d:%s" % (e[0], e[1]))
		try:
		    # Close socket
		    my_conn.close()
		except:
		    pass
		return None
	else:
	    # Received line, leave loop
	    break
	    
    if my_buffer == '':
	# Nothing else to read
	try:
	    my_conn.close()
	except:
	    pass
	return my_count

    # Removed leading/trailing spaces/tabs, trailing \r\n
    my_buffer = my_buffer.strip()
    # Do not trust anything we get from the internet
    my_buffer = untaint(my_buffer)
    
    # Create list of tokens
    my_args = my_buffer.split()
    
    if len(my_args) < 3:
	# Clearly not enough information
	sendmsg(my_conn, "%s 204 No Content\r\n" % (speak))
	try:
	    my_conn.close()
	except:
	    pass
	return my_count

    # Read information we need
    my_method = my_args.pop(0)
    my_proto = my_args.pop()
    my_what = my_args.pop().lower()
    
    if my_proto != speak:
	# Illegal or unknown protocol
	sendmsg(my_conn, "%s 400 Bad request\r\n" % (speak))
    elif my_method.upper() != "GET":
	# Unsupported method
	sendmsg(my_conn, "%s 405 Method not allowed\r\n" % (speak))
    elif not my_what in jumptable:
	# Request item is not supported
	sendmsg(my_conn, "%s 501 Not implemented\r\n" % (speak))
    else:
	# OK
	sendmsg(my_conn, "%s 200 OK\r\n" % (speak))
	if len(my_args) > 0:
	    my_count = jumptable[my_what](my_conn, str(my_args.pop(0)))
	else:
	    my_count = jumptable[my_what](my_conn)

    try:
	my_conn.close()
    except:
	pass

    return my_count
	
def check_request(server, timeout=0):
    # purpose: check for a pending service request, and service it
    # paramtr: server(IN): server socket
    # paramtr: timeout(IN, OPT): timeout in seconds, defaults to 0
    # returns: return value of select on server socket

    my_input_list = [server]
    my_input_ready, my_output_ready, my_except_ready = select.select(my_input_list, [], [], timeout)

    if len(my_input_ready) == 1:
	service_request(server)

    return len(my_input_ready)

def sleepy(retries, server=None):
    # purpose: sleep or work on client requests
    # paramtr: retries (IN): number of retries we are in
    # paramtr: server (IN): server listening socket, may be None
    # returns: Nothing
    my_sleeptime = sleeptime(retries)

    if server is not None:
	# elaborate iterative (non-concurrent) internet daemon
	my_start = int(time.time())
	my_diff = 0
	while my_diff < my_sleeptime:
	    my_diff = int(time.time()) - my_start
	    if my_diff < my_sleeptime:
		check_request(server, my_sleeptime - my_diff)
	    else:
		check_request(server, 0)
    else:
	# no server, just sleep regularly
	if my_sleeptime is not None:
	    time.sleep(my_sleeptime)
	

def sleeptime(retries):
    # purpose: compute suggested sleep time as a function of retries
    # paramtr: $retries (IN): number of retries so far
    # returns: recommended sleep time
    if retries < 5:
	my_y = 1
    elif retries < 50:
	my_y = 5
    elif retries < 500:
	my_y = 30
    else:
	my_y = 60

    return my_y

def daemonize(fn):
    # purpose: turn process into a daemon
    # paramtr: fn (IN): name of file to connect stdout to
    # returns: Nothing

    # Go to a safe place that is not susceptible to sudden umounts
    # FIX THIS: It may break some things
    try:
	os.chdir('/')
    except:
	logger.fatal("Error: in chdir!")
	sys.exit(1)

    # Open logfile as stdout
    # Maybe this should be an option in the submit file like "output = fn"
    try:
	sys.stdout = open(fn, "w", 0)
    except:
	logger.fatal("Error opening %s!" % (fn))
	sys.exit(1)

    # Fork and go!
    try:
	my_pid = os.fork()
    except:
	fatal("Error: in fork!")

    if my_pid > 0:
	# Parent exits
	sys.exit(0)

    # Daemon child -- fork again for System-V
    try:
	my_pid = os.fork()
    except:
	fatal("Error: in fork!")

    if my_pid > 0:
	# Parent exits
	sys.exit(0)
	
    # Setsid
    try:
	os.setsid()
    except:
	fatal("Error: in setsid!")

def keep_foreground(fn):
    # purpose: turn into almost a daemon, but keep in foreground for Condor
    # paramtr: fn (IN): name of file to connect stdout to
    # returns: Nothing

    # Go to a safe place that is not susceptible to sudden umounts
    # FIX THIS: It may break some things
    try:
	os.chdir('/')
    except:
	logger.fatal("Error: in chdir!")
	sys.exit(1)

    # Open logfile as stdout
    # Maybe this should be an option in the submit file like "output = fn"
    try:
	sys.stdout = open(fn, "a", 0)
    except:
	logger.fatal("Error opening %s!" % (fn))
	sys.exit(1)
    
    # Although we cannot set sid, we can still become process group leader
    try:
	os.setpgid(0, 0)
    except:
	logger.fatal("Error: in setpgid!")
	sys.exit(1)

def prog_sighup_handler(signum, frame):
    pass

def prog_sigint_handler(signum, frame):
    logmsg("graceful exit on signal %d" % (signum))
    sys.exit(1)

def prog_sigusr1_handler(signum, frame):
    debug_level = debug_level + 1

def prog_sigusr2_handler(signum, frame):
    debug_level = debug_level - 1

def rotate_log_file(input_dir, log_file):
    """
    This function rotates the specified logfile so that an old version
    is not overwritten when we start.
    """
    # First we check if we have the log file
    source_file = os.path.join(input_dir, log_file)
    
    if not os.access(source_file, os.F_OK):
	# File doesn't exist, we don't have to rotate
	return

    # Now we need to find the latest log file

    # We start from .000
    sf = 0

    while (sf < MAXLOGFILE):
	dest_file = source_file + ".%03d" % (sf)
	if os.access(dest_file, os.F_OK):
	    # Continue to the next one
	    sf = sf + 1
	else:
	    break

    # Safety check to see if we have reached the maximum number of log files
    if sf >= MAXLOGFILE:
	print "error: %s exists, cannot rotate log file anymore!" % (dest_file)
	sys.exit(1)

    # Now that we have source_file and dest_file, try to rotate the logs
    try:
	os.rename(source_file, dest_file)
    except:
	print "error: cannot rename %s to %s" % (source_file, dest_file)
	sys.exit(1)

    # Done!
    return

#
# --- at exit handlers -------------------------------------------------------------------
#

def socket_exit_handler():
    if server is not None:
	server.close()
	try:
	    os.unlink(sockfn)
	except:
	    # Just be silent
	    pass

def done_process_exit_handler():
    global timestamp

    # Attempt to copy the condor common logfile to the current directory
    if condorlog is not None:
	if (os.path.isfile(condorlog) and
	    os.access(condorlog, os.R_OK) and
	    condorlog.find('/') == 0):

	    # Copy common condor log to local directory
	    my_log = out2log(out)[0]
	    my_cmd = "/bin/cp -p %s %s.copy" % (condorlog, my_log)
	    my_status, my_output = commands.getstatusoutput(my_cmd)

	    if my_status == 0:
		# Copy successful
		try:
		    os.unlink(my_log)
		except:
		    logmsg("Error: removing %s" % (my_log))
		else:
		    try:
			os.rename("%s.copy" % (my_log), my_log)
		    except:
			logmsg("Error: renaming %s.copy to %s" % (my_log, my_log))
		    else:
			logmsg("copied common log to %s" % (run))
	    else:
		logmsg("%s: %d:%s" % (my_cmd, my_status, my_output))

    # Remember to tell me that it is ok to run post-processing now
    if (terminate is not None) and (run is not None):
	my_touch_name = os.path.join(run, "tailstatd.done")
	try:
	    TOUCH = open(my_touch_name, "w")
	except:
	    logmsg("Error: opening %s" % (my_touch_name))
	else:
	    timestamp = int(time.time())
	    TOUCH.write("%s %.3f\n" % (utils.isodate(timestamp), (timestamp - start)))
	    TOUCH.close()

#
# --- main ------------------------------------------------------------------------------
#

# sanity check: Be permisive
os.umask(0002)

# Turn into daemon process
if nodaemon == 0:
    daemonize(logfile)
elif nodaemon == 2:
    keep_foreground(logfile)
else:
    # Hack to make stdout unbuffered
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)

# Close stdin
sys.stdin.close()
# dup stderr onto stdout
sys.stderr = sys.stdout

# Initialize database
if database == 1:
    rotate_log_file(run, "pegasus.db")
    pegasus_db = os.path.join(run, "pegasus.db")
    workdb = nlapi.Log(level=nlapi.Level.ALL, logfile=pegasus_db)

# Say hello
logmsg("starting [%s], using pid %d" % (revision, os.getpid()))
if millisleep is not None:
    logmsg("using simulation delay of %d ms" % (millisleep))

# Add start information to JSDB
JSDB.write("%d INTERNAL *** TAILSTATD_STARTED ***\n" % (int(time.time())))

# Instantiate workflow class
wf = Workflow(config)
if database == 1:
    # Add workflow info to database
    wf.db_send_wf_info()

# Ignore dying shells
signal.signal(signal.SIGHUP, prog_sighup_handler)

# Die nicely when asked to (Ctrl+C, system shutdown)
signal.signal(signal.SIGINT, prog_sigint_handler)

# Permit dynamic changes of debug level
signal.signal(signal.SIGUSR1, prog_sigusr1_handler)
signal.signal(signal.SIGUSR2, prog_sigusr2_handler)

# No need to create server socket in replay mode
if not replay_mode:
    # Create server socket for communication with site selector
    sockfn = os.path.join(os.path.dirname(out), "tailstatd.sock")
    server = server_socket(49152, 65536)
    # Take care of closing socket when we exit
    atexit.register(socket_exit_handler)

    # Save our address so that site selectors know where to connect
    if server is not None:
	my_host, my_port = server.getsockname()
	try:
	    OUT = open(sockfn, "w")
	    OUT.write("%s %d\n" % (my_host, my_port))
	    OUT.close()
	except:
	    logmsg("Warning: Unable to write %s!" % (sockfn))

# No need to do this when running in replay mode
if not replay_mode:
    # Take care of copying condor log, and creating .done file
    atexit.register(done_process_exit_handler)

# For future reference
plus = ''
if "LD_LIBRARY_PATH" in os.environ:
    for my_path in os.environ["LD_LIBRARY_PATH"].split(':'):
	logmsg("env: LD_LIBRARY_PATH%s=%s" % (plus, my_path))
	plus = '+'

if "GLOBUS_TCP_PORT_RANGE" in os.environ:
    logmsg("env: GLOBUS_TCP_PORT_RANGE=%s" % (os.environ["GLOBUS_TCP_PORT_RANGE"]))
else:
    logmsg("env: GLOBUS_TCP_PORT_RANGE=")
if "GLOBUS_TCP_SOURCE_RANGE" in os.environ:
    logmsg("env: GLOBUS_TCP_SOURCE_RANGE=%s" % (os.environ["GLOBUS_TCP_SOURCE_RANGE"]))
else:
    logmsg("env: GLOBUS_TCP_SOURCE_RANGE=")
if "GLOBUS_LOCATION" in os.environ:
    logmsg("env: GLOBUS_LOCATION=%s" % (os.environ["GLOBUS_LOCATION"]))
else:
    logmsg("env: GLOBUS_LOCATION=")

# Now we wait for the .out file to appear
f_stat = None
n_retries = 0

# Start looking for the file
while True:
    try:
	f_stat = os.stat(out)
    except OSError, e:
	if errno.errorcode[e.errno] == 'ENOENT':
	    # File doesn't exist yet, keep looking
	    n_retries = n_retries + 1
	    if n_retries > 100:
		# We tried too long, just exit
		fatal("%s never made an appearance" % (out))
	    # Continue waiting
	    logmsg("waiting for out file, retry %d" % ( n_retries))
	    sleepy(n_retries, server)
	else:
	    # Another error
	    fatal("stat %s" % (out))
    except:
	fatal("stat %s" % (out))
    else:
	# Found file!
	break

# post condition: stat is a valid stat record from the daglog file

# open daglog file
try:
    DMOF = open(out, "r")
except:
    fatal("opening %s" % (out))

#
# --- main loop --------------------------------------------------------------------------
#

ml_buffer = ''
ml_rbuffer = ''
ml_retries = 0
ml_current = 0
ml_pos = 0

while True:
    # Say Hello
    if debug_level > 1:
	logmsg("wake up and smell the silicon")

    # Periodically check for starving jobs, but not in the reparse mode
    if last_check < timestamp:
	now = int(time.time())
	if now - last_check > job_check_interval:
	    check_starvation(now)
	    last_check = now

    # Periodically check for service requests
    if server is not None:
	check_request(server)

    try:
	f_stat = os.stat(out)
    except:
	# stat error
	fatal("stat %s" % (out))

    if f_stat[6] == ml_current:
	# Death by natural causes
	if terminate is not None:
	    break

	# Check if DAGMan is alive -- if we know where it lives
	if ml_retries > 10 and pid > 0:
	    # Just send signal 0 to check if the pid is ours
	    try:
		os.kill(int(pid), 0)
	    except:
		logmsg("DAGMan is gone! Sudden death syndrome detected!")
		terminate = 42
		break
	
	# No change, wait a whilte
	ml_retries = ml_retries + 1
	if ml_retries > 17280:
	    # Too long without change
	    logmsg("too long without action, self-destructing")
	    break

	sleepy(ml_retries, server)
    elif f_stat[6] < ml_current:
	# Truncated file, booh!
	logmsg("file truncated, time to exit")
	break
    elif f_stat[6] > ml_current:
	# We have something to read!
	try:
	    ml_rbuffer = DMOF.read(32768)
	except:
	    # Error while reading
	    fatal("while reading")
	if len(ml_rbuffer) == 0:
	    # Detected EOF
	    logmsg("detected EOF, resetting position to %d" % (ml_current))
	    DMOF.seek(ml_current)
	else:
	    # Something in the read buffer, merge it with our buffer
	    ml_buffer = ml_buffer + ml_rbuffer
	    # Look for end of line
	    ml_pos = ml_buffer.find('\n')
	    while (ml_pos > 0):
		# Take out 1 line, and adjust buffer
		process(ml_buffer[0:ml_pos])
		ml_buffer = ml_buffer[ml_pos+1:]
		line = line + 1
		ml_pos = ml_buffer.find('\n')

		if millisleep is not None:
		    if server is not None:
			check_request(server, millisleep / 1000.0)
		    else:
			time.sleep(millisleep / 1000.0)

	    ml_pos = DMOF.tell()
	    logmsg("processed chunk of %d byte" % (ml_pos - ml_current -len(ml_buffer)))
	    ml_current = ml_pos
	    ml_retries = 0

DMOF.close()

if not replay_mode:
    # Finish trailing connection requests
    while (check_request(server)):
	pass
    server.close()
    server = None
    try:
	os.unlink(sockfn)
    except:
	# Just be silent
	pass

    # Dump siteinfo when we are done
    try:
	SI = open(os.path.join(run, "sitedump.txt"), 'w')
    except:
	logger.warn("Could not create file %s" % (os.path.join(run, "sitedump.txt")))
    else:
	service_request_site(SI)
	SI.close()
    
# Finish, and close output file
JSDB.write("%d INTERNAL *** TAILSTATD_FINISHED %d ***\n" % (int(time.time()), terminate))
JSDB.close()

# Try to run the hurricane graphics
if doplot:
    pass
else:
    logmsg("skipping plots")

# done
logmsg("finishing, exit with %d" % (terminate))
sys.exit(terminate)

