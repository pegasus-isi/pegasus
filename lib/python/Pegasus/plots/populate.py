#!/usr/bin/env python

import os
import re
import sys
import logging
import optparse
import math
import tempfile
import commands

# Initialize logging object
logger = logging.getLogger()
# Set default level to INFO
logger.setLevel(logging.INFO)

import common
import plot_utils
from Pegasus.tools import utils
from workflow_info import WorkflowInfo, JobInfo
import pegasus_gantt
import pegasus_host_over_time
from netlogger.analysis.workflow.sql_alchemy import *
from datetime import timedelta

#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'

predefined_colors = ["#8dd3c7" , "#4daf4a","#bebada" , "#80b1d3" , "#b3de69" , "#fccde5" , "#d9d9d9" , "#bc80bd" , "#ccebc5" ,  "#fb8072"]
global_transformtion_color_map ={}
global_base_submit_dir = None;
global_braindb_submit_dir =None;


def populate_job_details(job , job_stat, dagman_start_time):
	"""
	Returns the job statistics information
	Param: the job reference
	"""
	state_timestamp_dict ={}
	kickstartDur= 0
	resource_delay = 0
	condor_delay = 0
	runtime = 0
	condorTime = 0
	job_duration = 0
	
	
	# assigning job name
	job_stat.name = job.name
	# assigning site name
	job_stat.site = job.site_name
	#assigning host
	job_stat.host = job.host
	# assigning host name
	# Assumption host name is unique
	job_stat.host_name = job.host.hostname
	
	jobstates = job.all_jobstates
		
	for jobstate in jobstates:
		state_timestamp_dict[jobstate.state] = jobstate.timestamp
	
	# Setting the first state as the job start
	job_duration = 0	
	if len(jobstates) > 0:
		# Setting the first job state as the job start
		job_stat.jobStart = jobstates[0].timestamp
		# Setting the last job state as the job end
		job_duration = jobstates[len(jobstates)-1].timestamp - jobstates[0].timestamp
		job_stat.jobDuration =  convert_to_seconds(job_duration)
	
	# Setting SUBMIT as the job start	
	if len(jobstates) > 0:
		if state_timestamp_dict.has_key('SUBMIT'):
			# Setting the SUBMTI as the start event for the host over time chart , pre script is not used.
			job_stat.jobExecStart = state_timestamp_dict['SUBMIT']
			# Setting the JOB_TERMINATED as the end event for the host over time chart if it is present , otherwise that last event.
			if state_timestamp_dict.has_key('JOB_TERMINATED'):
				job_duration = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['SUBMIT']
			else:
				job_duration = jobstates[len(jobstates)-1].timestamp - state_timestamp_dict['SUBMIT']
			job_stat.jobExecDuration =  convert_to_seconds(job_duration)
			
	#kickstart time
	# Assigning start and duration of kickstart , pre and post
	tasks = job.tasks
	if len(tasks) > 0:
		taskcount =0
		for task in tasks:
			if (task.task_submit_seq > 0):
				if job_stat.kickstartStart is None:
					job_stat.kickstartStart = task.start_time
					job_stat.transformation = task.transformation
				kickstartDur +=task.duration
				taskcount +=1
			# pre job	
			if (task.task_submit_seq == -1):
				job_stat.preStart = task.start_time
				job_stat.preDuration = task.duration
			# post job	
			if (task.task_submit_seq == -2):
				job_stat.postStart = task.start_time
				job_stat.postDuration = task.duration		
		if taskcount > 1 and not (job.clustered) :
			logger.debug(job.name +" has more than one compute task associated to it " )
		#assign kickstartDuration
		if taskcount > 0:
			job_stat.kickstartDuration	= kickstartDur
	else :
		logger.debug(job.name +" has no task associated to it." )
	
	
	# GRID/GLOBUS SUBMIT start and duration
	if state_timestamp_dict.has_key('EXECUTE'):
		if state_timestamp_dict.has_key('GRID_SUBMIT') or state_timestamp_dict.has_key('GLOBUS_SUBMIT'):
			if state_timestamp_dict.has_key('GRID_SUBMIT'):
				resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GRID_SUBMIT']
				job_stat.gridStart = state_timestamp_dict['GRID_SUBMIT']
			else:
				resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GLOBUS_SUBMIT']
				job_stat.gridStart = state_timestamp_dict['GLOBUS_SUBMIT']
			#Assigning resource delay
			resource_delay =convert_to_seconds(resource_delay)
			job_stat.gridDuration = resource_delay
		else :
			logger.debug("Unable to calculate the resource delay. GRID_SUBMIT/GLOBUS_SUBMIT event missing for job "+ job.name)
	else:
		# Means the job failed before reaching this stage
		logger.debug("Unable to calculate the resource delay. EXECUTE event missing for job "+ job.name)
		
	
	
	#runtime
	
	if state_timestamp_dict.has_key('JOB_TERMINATED'):
		if state_timestamp_dict.has_key('EXECUTE'):
			runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['EXECUTE']
			runtime =convert_to_seconds(runtime)
			job_stat.executeStart = state_timestamp_dict['EXECUTE']
			job_stat.executeDuration = runtime	
		elif state_timestamp_dict.has_key('SUBMIT'):
			runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['SUBMIT']
			runtime=convert_to_seconds(runtime)
			job_stat.executeStart = state_timestamp_dict['SUBMIT']
			job_stat.executeDuration = runtime	
		else:
			runtime = None
			job_stat.executeStart = None
			job_stat.executeDuration = None	
			logger.debug("Unable to find the runtime. EXECUTE/SUBMIT event missing for job "+ job.name)	
		#assigning runtime
			
	else :
		# Means the job failed before reaching this stage
		logger.debug("Unable to find the runtime. JOB_TERMINATED event missing for job "+ job.name)
	
	#condor start to end
	if state_timestamp_dict.has_key('JOB_TERMINATED'):
		if state_timestamp_dict.has_key('SUBMIT'):
			condorTime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['SUBMIT']
			condorTime=convert_to_seconds(condorTime)
			job_stat.condorStart = state_timestamp_dict['SUBMIT']
			job_stat.condorDuration = condorTime	
		else:
			runtime = None
			job_stat.condorStart = None
			job_stat.condorDuration = None	
			logger.debug("Unable to find the duration of the job as seen by condor. SUBMIT event missing for job "+ job.name)	
		#assigning runtime
			
	else :
		# Means the job failed before reaching this stage
		logger.debug("Unable to find the duration of the job as seen by condor. JOB_TERMINATED event missing for job "+ job.name)

	
	#Assigning job state
	if job.is_success:
		job_stat.state ='SUCCESS'
	elif job.is_failure:
		job_stat.state ='FAILED'
	else:
		job_stat.state= job.current_state
	return		
	
def parse_submit_file(sub_fn):
	"""
	This function walks through the submit file, looks for initialdir.
	Returns : initial directory for the workflow, None if no value found
	"""
	init_dir_path = None
	if os.access(sub_fn ,os.R_OK):
	# Open dag file
		try:
			SUB = open(sub_fn, "r")
		except:
			logger.debug( "Could not open submit file ." +(sub_fn))
			return init_dir_path
	# Loop through the sub file
	for line in SUB:	
		line = line.strip(" \t")
		# Skip comments
		if line.startswith("#"):
			continue
		line = line.rstrip("\n\r") # Remove new lines, if any
		line = line.split('#')[0] # Remove inline comments too
		line = line.strip() # Remove any remaining spaces at both ends
		# Skip empty lines
		if len(line) == 0:
			continue
		prop = re_parse_property.search(line)
		if prop:
			k = prop.group(1)
			v = prop.group(2)
			if k =="initialdir":
				init_dir_path = v
				break
	else:
		logger.debug("Unable to read the submit file" + sub_fn)
	return init_dir_path	

def rlb(file_path):
	"""
	This function converts the path relative to base path
	Returns : path relative to the base 
	"""
	file_path = plot_utils.rlb(file_path, global_braindb_submit_dir,global_base_submit_dir)
	return file_path
	
							
#------------Gets sub worklows job names----------------
def get_job_name_sub_workflow_map(workflow , sub_dir):
	job_name_wf_uuid_map ={}
	# Parse through the job 
	for job in workflow.jobs:
		if(plot_utils.isSubWfJob(job.name)):
			# pops the sub workflow .sub file name
			sub_submit_file_name = job.name+".sub"
			# builds the sub workflow .sub file path
			sub_wf_submit_file_path =os.path.join(sub_dir,sub_submit_file_name)
			# parses the .sub file for the 'initialdir' value
			sub_init_dir =parse_submit_file(sub_wf_submit_file_path)
			
			if sub_init_dir is not None:
				# creates the sub workflow brain dump file path
				sub_braindb = os.path.join(rlb(sub_init_dir),brainbase)
				
				#Getting values from braindump file
				config = utils.slurp_braindb(rlb(sub_init_dir))
				if not config:
					logger.warning("could not process braindump.txt " + sub_braindb)
					logger.warning("Unable to find map the workflow id to the job " + job.name)
					continue
				wf_uuid = ''
				if (config.has_key('wf_uuid')):
					sub_wf_uuid = config['wf_uuid']
					# stores the wf-uuid value
					job_name_wf_uuid_map[job.name] = sub_wf_uuid
				else:
					logger.warning("workflow id cannot be found in the braindump.txt " + sub_braindb)
					logger.warning("Unable to find map the workflow id to the job " + job.name)
	return job_name_wf_uuid_map



#-------return workflow uuid by parsing db alone-----

def get_sub_workflows_uuid(root_workflow , output_db_url):
	sub_wf_uuid_list =[]
	uuid_list =[]
	# Adds the root workflow to workflow list
	wf_list =[root_workflow]
	while len(wf_list) > 0:
		wf =  wf_list[0]
		wf_list.remove(wf)
		# get the sub wf uuids
		uuid_list = wf.sub_wf_uuids
		while len(uuid_list) >0:
			# pops the sub workflow uuid
			sub_wf_uuid = uuid_list[0]
			uuid_list.remove(sub_wf_uuid)
			sub_wf_uuid_list.append(sub_wf_uuid)
			w = Workflow(output_db_url)
			w.initialize(sub_wf_uuid)
			wf_list.append(w)
	return sub_wf_uuid_list	
				

#-----date conversion----------
def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return plot_utils.convert_to_seconds(time)


# ---------populate_workflow_details--------------------------
def populate_workflow_details(workflow):
	"""
	populates the workflow statistics information
	Param: the workflow reference
	"""
	workflow_stat = WorkflowInfo()
	workflow_run_time = None
	total_jobs =0
	transformation_stats_dict ={}
	job_stats_dict ={}
	job_stats_list =[]
	host_job_mapping ={}
	wf_transformation_color_map ={}
	
	
	if not workflow.is_running :
		workflow_run_time = convert_to_seconds(workflow.total_time)
	else:
		workflow_run_time = convert_to_seconds(workflow.total_time)
	color_count = 0
	# populating statistics details
	dagman_start_time = workflow.start_events[0].timestamp
	# for root jobs,dagman_start_time is required, assumption start_event[0] is not none
	for job in workflow.jobs:
		job_stat = JobInfo()
		job_stats_dict[job.name] = job_stat
		job_stats_list.append(job_stat)
		populate_job_details(job ,job_stat , dagman_start_time)
		# Assigning host to job mapping
		if host_job_mapping.has_key(job_stat.host_name):
			job_list =host_job_mapping[job_stat.host_name]
			job_list.append(job_stat)
		else:
			job_list = []
			job_list.append(job_stat)
			host_job_mapping[job_stat.host_name] = job_list
			
		# Assigning the tranformation name
		if job_stat.transformation is not None:
			transformation_stats_dict[job_stat.transformation] = job_stat.transformation
		if not global_transformtion_color_map.has_key(job_stat.transformation):
			global_transformtion_color_map[job_stat.transformation]= predefined_colors[color_count%len(predefined_colors)]
			color_count +=1
		# Assigning the mapping to the workflow map
		wf_transformation_color_map[job_stat.transformation] =global_transformtion_color_map[job_stat.transformation] 
		
	#Calculating total jobs
	total_jobs = len(job_stats_list)
	# Assigning value to the workflow object
	workflow_stat.wf_uuid = workflow.wf_uuid
	workflow_stat.parent_wf_uuid =workflow.parent_wf_uuid
	workflow_stat.parent_wf_uuid = workflow.parent_wf_uuid
	if workflow_run_time is not None:
		workflow_stat.workflow_run_time =workflow_run_time
	workflow_stat.total_jobs = total_jobs	
	workflow_stat.dagman_start_time = dagman_start_time
	workflow_stat.submit_dir = workflow.submit_dir
	workflow_stat.dax_label = workflow.dax_label		
	workflow_stat.job_statistics_dict = job_stats_dict
	workflow_stat.job_statistics_list =job_stats_list
	workflow_stat.host_job_map = host_job_mapping
	workflow_stat.transformation_statistics_dict = transformation_stats_dict
	workflow_stat.transformation_color_map = wf_transformation_color_map
	return workflow_stat
	
	
def setup_logger(level_str):
	level_str = level_str.lower()
	if level_str == "debug":
		logger.setLevel(logging.DEBUG)
	if level_str == "warning":
		logger.setLevel(logging.WARNING)
	if level_str == "error":
		logger.setLevel(logging.ERROR)
	if level_str == "info":
		logger.setLevel(logging.INFO)
	return
	
def populate_chart(submit_dir):
	# global reference
	global global_base_submit_dir
	global global_braindb_submit_dir
	global_base_submit_dir = submit_dir
	#Getting values from braindump file
	config = utils.slurp_braindb(submit_dir)
	if not config:
		logger.warning("could not process braindump.txt ")
		sys.exit(1)
	wf_uuid = ''
	if (config.has_key('wf_uuid')):
		wf_uuid = config['wf_uuid']
	else:
		logger.error("workflow id cannot be found in the braindump.txt ")
		sys.exit(1)
	
	if (config.has_key('submit_dir')):
		global_braindb_submit_dir =  os.path.abspath(config['submit_dir'])
	else:
		logger.error("Submit directory cannot be found in the braindump.txt ")
		sys.exit(1)
	
	dag_file_name =''
	if (config.has_key('dag')):
		dag_file_name = config['dag']
	else:
		logger.error("Dag file name cannot be found in the braindump.txt ")
		sys.exit(1)	
	
	# Create the sqllite db url
	output_db_file =submit_dir +"/"+ dag_file_name[:dag_file_name.find(".dag")] + ".stampede.db"
	output_db_url = "sqlite:///" + output_db_file
	if os.path.isfile(output_db_file):
		workflow = Workflow(output_db_url)
		workflow.initialize(wf_uuid)
	else:
		logger.error("Unable to find database file in "+submit_dir)
		sys.exit(1)
	workflow_info = populate_workflow_details(workflow)
	if len(workflow.sub_wf_uuids) > 0:
		workflow_info.job_name_sub_wf_uuid_map = get_job_name_sub_workflow_map(workflow , rlb(workflow.submit_dir))
	workflow_info.dag_label = dag_file_name[:dag_file_name.find(".dag")]
	workflow_info.dag_file_path = os.path.join(submit_dir, dag_file_name)
	if (config.has_key('dax')):
		workflow_info.dax_file_path = config['dax']
	workflow_info_list = []
	workflow_info_list.append(workflow_info)
	sub_workflow_list = get_sub_workflows_uuid(workflow, output_db_url)
	for sub_wf_uuid in sub_workflow_list:
		workflow = Workflow(output_db_url)
		workflow.initialize(sub_wf_uuid)
		workflow_info = populate_workflow_details(workflow)
		if len(workflow.sub_wf_uuids) > 0:
			workflow_info.job_name_sub_wf_uuid_map = get_job_name_sub_workflow_map(workflow , rlb(workflow.submit_dir))
		config = utils.slurp_braindb(rlb(workflow_info.submit_dir))
		if (config.has_key('dag')):
			dag_file_name = config['dag']
			workflow_info.dag_label = dag_file_name[:dag_file_name.find(".dag")]
			workflow_info.dag_file_path = os.path.join(rlb(workflow_info.submit_dir), dag_file_name)
		if (config.has_key('dax')):
			workflow_info.dax_file_path = config['dax']
		workflow_info_list.append(workflow_info)
	return workflow_info_list
	
