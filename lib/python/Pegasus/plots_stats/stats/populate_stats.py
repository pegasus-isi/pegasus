#!/usr/bin/env python

import os
import re
import sys
import logging
import optparse
import math
import tempfile

# Initialize logging object
logger = logging.getLogger()

import common
from Pegasus.plots_stats import utils as stats_utils
from workflow_stats import WorkflowStatistics, JobStatistics, TransformationStatistics
from netlogger.analysis.workflow.sql_alchemy import *
from datetime import timedelta

#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'
workflow_statistics_file_name ="workflow";
job_statistics_file_name ="jobs";
logical_transformation_statistics_file_name ="breakdown.txt";

base_submit_dir = None
braindb_submit_dir =None
global_db_url = None
global_top_wf_uuid =None


output_dir=''
condor = 0				# pure condor run - no GRID_SUBMIT events



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

# ---------slurp_braindb--------------------------
def slurp_braindb(sub_dir):
	"""
	Reads extra configuration from braindump database
	Param: sub_dir is the submit directory
	Returns: Dictionary with the configuration, empty if error
	"""
	my_config = {}
	my_braindb = os.path.join(sub_dir, brainbase)

	try:
		my_file = open(my_braindb, 'r')
	except:
	# Error opening file
		return my_config
	
	for line in my_file:
	# Remove \r and/or \n from the end of the line
		line = line.rstrip("\r\n")
	# Split the line into a key and a value
		k, v = line.split(" ", 1)
		v = v.strip()
		my_config[k] = v
	# Close file
	my_file.close()

	# Done!
	#logger.debug("# slurped " + my_braindb)
	return my_config

#-----date conversion----------
def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)


def get_workflows_uuid():
	root_workflow = Workflow(global_db_url)
	root_workflow.initialize(global_top_wf_uuid)
	wf_uuid_list =[root_workflow.wf_uuid]
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
			wf_uuid_list.append(sub_wf_uuid)
			w = Workflow(global_db_url)
			w.initialize(sub_wf_uuid)
			wf_list.append(w)
	return wf_uuid_list
	

# ---------populate_workflow_details--------------------------
def populate_workflow_details(workflow):
	"""
	populates the workflow statistics information
	Param: the workflow reference
	"""
	workflow_stat = WorkflowStatistics()
	transformation_stats_dict ={}
	job_stats_dict ={}
	total_succeeded_tasks =0
	total_failed_tasks =0
	total_jobs =0
	total_jobs_non_sub_wf =0
	succeeded_jobs =0
	failed_jobs =0
	unknown_jobs =0
	unsubmitted_jobs =0
	succeeded_jobs_non_sub_wf =0
	failed_jobs_non_sub_wf =0
	unknown_jobs_non_sub_wf =0
	unsubmitted_jobs_non_sub_wf =0
	workflow_run_time = None
	workflow_cpu_time =0
	workflow_cpu_time_non_sub_wf =0
	
	if workflow.total_time is not None:
		workflow_run_time = convert_to_seconds(workflow.total_time)
	if len(workflow.start_events) < 1:
		logger.warning("Unable to find the start event for the workflow ")
		# Sets everything to default value
		workflow_stat.submit_dir = workflow.submit_dir
		workflow_stat.parent_wf_uuid = workflow.parent_wf_uuid
		workflow_stat.total_jobs = total_jobs
		workflow_stat.total_jobs_non_sub_wf = total_jobs_non_sub_wf
		workflow_stat.succeeded_jobs = succeeded_jobs
		workflow_stat.failed_jobs = failed_jobs
		workflow_stat.unsubmitted_jobs = unsubmitted_jobs
		workflow_stat.unknown_jobs = unknown_jobs
		workflow_stat.succeeded_jobs_non_sub_wf = succeeded_jobs_non_sub_wf
		workflow_stat.failed_jobs_non_sub_wf = failed_jobs_non_sub_wf
		workflow_stat.unsubmitted_jobs_non_sub_wf = unsubmitted_jobs_non_sub_wf
		workflow_stat.unknown_jobs_non_sub_wf = unknown_jobs_non_sub_wf
		workflow_stat.total_succeeded_tasks = total_succeeded_tasks
		workflow_stat.total_failed_tasks =total_failed_tasks
		workflow_stat.job_statistics_dict = job_stats_dict
		workflow_stat.transformation_statistics_dict = transformation_stats_dict
		return workflow_stat
	for job in workflow.jobs:
		for task in job.tasks:
			if (task.task_submit_seq > 0):
				if (task.exitcode == 0):	
					total_succeeded_tasks +=1
				else :
					total_failed_tasks +=1	
	# populating statistics details
	dagman_start_time = workflow.start_events[0].timestamp
	# for root jobs,dagman_start_time is required, assumption start_event[0] is not none
	for job in workflow.jobs:
		if job_stats_dict.has_key(job.name):
			job_stat = job_stats_dict[job.name]
		else:	
			job_stat = JobStatistics()
			job_stats_dict[job.name] = job_stat
			if not stats_utils.isSubWfJob(job.name):
				total_jobs_non_sub_wf +=1
		populate_job_details(job ,job_stat , dagman_start_time)
		tasks = job.tasks
		for task in tasks:
			if (task.task_submit_seq > 0):
				if transformation_stats_dict.has_key(task.transformation):
					trans_stats = transformation_stats_dict[task.transformation]
				else:
					trans_stats = TransformationStatistics()
					transformation_stats_dict[task.transformation] = trans_stats
				populate_transformation_statistics(task ,trans_stats)
	# calculating cpu time
	for job in workflow.jobs:
		if job_stats_dict[job.name].runtime is None or job_stats_dict[job.name].runtime =='-':
			workflow_cpu_time ='-'
			break
		else: 
			workflow_cpu_time +=job_stats_dict[job.name].runtime
			
	for job in workflow.jobs:
		if not stats_utils.isSubWfJob(job.name):
			if job_stats_dict[job.name].runtime is None or job_stats_dict[job.name].runtime =='-':
				workflow_cpu_time_non_sub_wf ='-'
				break
			else:
				workflow_cpu_time_non_sub_wf += job_stats_dict[job.name].runtime
			
	#Calculating total jobs
	total_jobs = len(job_stats_dict)
		
	#Calculating failed and successful jobs	
	for job_stat in job_stats_dict.values():
		is_non_sub_wf_job = 1
		if stats_utils.isSubWfJob(job.name):
			is_non_sub_wf_job = 0
		if job_stat.is_success:
			succeeded_jobs +=1
			if is_non_sub_wf_job:
				succeeded_jobs_non_sub_wf +=1
		elif job_stat.is_failure:
			failed_jobs +=1
			if is_non_sub_wf_job:
				failed_jobs_non_sub_wf +=1
		elif job_stat.state is None:
			unsubmitted_jobs +=1
			if is_non_sub_wf_job:
				unsubmitted_jobs_non_sub_wf +=1
		else:
			unknown_jobs +=1
			if is_non_sub_wf_job:
				unknown_jobs_non_sub_wf +=1
	# Assigning value to the workflow object
	workflow_stat.submit_dir = workflow.submit_dir
	workflow_stat.parent_wf_uuid = workflow.parent_wf_uuid
	if workflow_run_time is not None:
		workflow_stat.workflow_run_time =workflow_run_time
	workflow_stat.workflow_cpu_time = workflow_cpu_time
	workflow_stat.workflow_cpu_time_non_sub_wf = workflow_cpu_time_non_sub_wf
	workflow_stat.total_jobs = total_jobs
	workflow_stat.total_jobs_non_sub_wf = total_jobs_non_sub_wf
	workflow_stat.succeeded_jobs = succeeded_jobs
	workflow_stat.failed_jobs = failed_jobs
	workflow_stat.unsubmitted_jobs = unsubmitted_jobs
	workflow_stat.unknown_jobs = unknown_jobs
	workflow_stat.succeeded_jobs_non_sub_wf = succeeded_jobs_non_sub_wf
	workflow_stat.failed_jobs_non_sub_wf = failed_jobs_non_sub_wf
	workflow_stat.unsubmitted_jobs_non_sub_wf = unsubmitted_jobs_non_sub_wf
	workflow_stat.unknown_jobs_non_sub_wf = unknown_jobs_non_sub_wf
	workflow_stat.total_succeeded_tasks = total_succeeded_tasks
	workflow_stat.total_failed_tasks =total_failed_tasks
	workflow_stat.job_statistics_dict = job_stats_dict
	workflow_stat.transformation_statistics_dict = transformation_stats_dict
	return workflow_stat

def populate_job_details(job , job_stat, dagman_start_time):
	"""
	Returns the job statistics information
	Param: the job reference
	"""
	state_timestamp_dict ={}
	kickstart = 0
	post_script_time = 0
	resource_delay = 0
	condor_delay = 0
	dagman_delay = 0
	runtime = 0
	seqexec = 0
	seqexec_delay =0
	
	# assigning job name
	job_stat.name = job.name
	# assigning site name
	job_stat.site = job.site_name
	
	jobstates = job.all_jobstates
	for jobstate in jobstates:
		state_timestamp_dict[jobstate.state] = jobstate.timestamp
	# default values	
	
	# sanity check
	#if not state_timestamp_dict.has_key('SUBMIT'):
	#	logger.debug("Unable to generate statistics for the job.Failed to find the SUBMIT event for job "+ job.name)
	#	return job_stat
	#kickstart time
	# need to check for the submit_seq_id instead
	tasks = job.tasks
	if len(tasks) > 0:
		taskcount =0
		for task in tasks:
			if (task.task_submit_seq > 0): 
				kickstart +=task.duration
				taskcount +=1
		if taskcount > 1 and not (job.clustered) :
			logger.debug(job.name +" has more than one compute task associated to it " )
		#assign kickstart
		if taskcount > 0:
			if job_stat.kickstart =='-':	
				job_stat.kickstart	= kickstart
			else:
				job_stat.kickstart	+= kickstart
	else :
		logger.debug(job.name +" has no task associated to it." )
	
	# condor_delay
	if job_stat.condor_delay is not None:
		if state_timestamp_dict.has_key('GRID_SUBMIT') or state_timestamp_dict.has_key('GLOBUS_SUBMIT') or state_timestamp_dict.has_key('EXECUTE') :
			if state_timestamp_dict.has_key('GRID_SUBMIT'):
				if state_timestamp_dict.has_key('SUBMIT'):
					condor_delay = state_timestamp_dict['GRID_SUBMIT'] - state_timestamp_dict['SUBMIT']
					condor_delay=convert_to_seconds(condor_delay)
				else:
					condor_delay = None
					logger.debug("Unable to calculate condor delay for the job. SUBMIT event is missing for job "+ job.name)	
			elif state_timestamp_dict.has_key('GLOBUS_SUBMIT'):
				if state_timestamp_dict.has_key('SUBMIT'):
					condor_delay = state_timestamp_dict['GLOBUS_SUBMIT'] - state_timestamp_dict['SUBMIT']
					condor_delay=convert_to_seconds(condor_delay)
				else:
					condor_delay = None
					logger.debug("Unable to calculate condor delay for the job. SUBMIT event is missing for job "+ job.name)	
			else:
				if state_timestamp_dict.has_key('SUBMIT'):
					condor_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['SUBMIT']
					condor_delay=convert_to_seconds(condor_delay)
				else:
					condor_delay = None
					logger.debug("Unable to calculate condor delay for the job.SUBMIT event is missing for job "+ job.name)
			#Assigning condor delay
			if condor_delay is None:
				job_stat.condor_delay = None
			else: 	
				if job_stat.condor_delay =='-':
					job_stat.condor_delay = condor_delay
				else:
					job_stat.condor_delay += condor_delay			
		else :
			# Means the job failed before reaching this stage
			logger.debug("Unable to calculate the condor delay. GRID_SUBMIT/GLOBUS_SUBMIT/EXECUTE event missing for job "+ job.name)
		
	# resource delay there won't be  GRID_SUBMIT/GLOBUS_SUBMIT for pure condor run's
	if job_stat.resource is not None:
		if state_timestamp_dict.has_key('EXECUTE'):
			if state_timestamp_dict.has_key('GRID_SUBMIT') or state_timestamp_dict.has_key('GLOBUS_SUBMIT'):
				if state_timestamp_dict.has_key('GRID_SUBMIT'):
					resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GRID_SUBMIT']
				else:
					resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GLOBUS_SUBMIT']
				#Assigning resource delay
				resource_delay =convert_to_seconds(resource_delay)
				if job_stat.resource =='-':
					job_stat.resource = resource_delay
				else:
					job_stat.resource += resource_delay
			else :
				logger.debug("Unable to calculate the resource delay. GRID_SUBMIT/GLOBUS_SUBMIT event missing for job "+ job.name)
		else:
			# Means the job failed before reaching this stage
			logger.debug("Unable to calculate the resource delay. EXECUTE event missing for job "+ job.name)
		
	
	#Assigning dagmanDelay
	max_parent_end_time = None
	end_time = None
	if job_stat.dagman is not None and state_timestamp_dict.has_key('SUBMIT'):
		if len(job.edge_parents) >0:
			for parent in job.edge_parents:
				parent_job_timestamp_dict ={}
				parent_job_states = parent.all_jobstates
				for parent_jobstate in parent_job_states:
					parent_job_timestamp_dict[parent_jobstate.state] = parent_jobstate.timestamp
				if parent_job_timestamp_dict.has_key('POST_SCRIPT_TERMINATED'):
					end_time = parent_job_timestamp_dict['POST_SCRIPT_TERMINATED']
				elif parent_job_timestamp_dict.has_key('JOB_TERMINATED'):	
					end_time = parent_job_timestamp_dict['JOB_TERMINATED']
				else:
					max_parent_end_time = None	
					logger.debug("Unable to calculate the dagman delay. POST_SCRIPT_TERMINATED or JOB_TERMINATED event missing for the parent job "+parent.name + " of "+ job.name)
					break
					
				if max_parent_end_time is None:
					max_parent_end_time = end_time
				else:
					if end_time > max_parent_end_time:
						max_parent_end_time = end_time	
		else:
			max_parent_end_time = dagman_start_time
		if max_parent_end_time is not None:
			dagman_delay =  convert_to_seconds(state_timestamp_dict['SUBMIT'] - max_parent_end_time)
			job_stat.dagman = dagman_delay 		
		else:
			job_stat.dagman = None
	#runtime
	if job_stat.runtime is not None:
		if state_timestamp_dict.has_key('JOB_TERMINATED'):
			if state_timestamp_dict.has_key('EXECUTE'):
				runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['EXECUTE']
				runtime =convert_to_seconds(runtime)
			elif state_timestamp_dict.has_key('SUBMIT'):
				runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['SUBMIT']
				runtime=convert_to_seconds(runtime)
			else:
				runtime = None
				logger.debug("Unable to find the runtime. EXECUTE/SUBMIT event missing for job "+ job.name)	
			#assigning runtime
			if runtime is None:
				job_stat.runtime = None
			else:	
				if job_stat.runtime == '-':
					job_stat.runtime = runtime
				else:
					job_stat.runtime += runtime		
		else :
			# Means the job failed before reaching this stage
			logger.debug("Unable to find the runtime. JOB_TERMINATED event missing for job "+ job.name)
	
	#seqexec and seqexec-delay
	if job.clustered:
		if job.cluster_duration is not None:
			seqexec = str(job.cluster_duration)
			seqexec_delay = str(job.cluster_duration-kickstart)
			#assigning 	seqexec and seqexec-delay
			if job_stat.seqexec =='-':		
				job_stat.seqexec = seqexec
				job_stat.seqexec_delay = seqexec_delay
			else:	
				job_stat.seqexec += seqexec
				job_stat.seqexec_delay += seqexec_delay
		else:
			# Means the job failed before reaching this stage
			logger.debug("Unable to calculate the seqexec time. cluster duration is missing for job "+ job.name)
	
	# post script time
	if job_stat.post is not None:
		if state_timestamp_dict.has_key('POST_SCRIPT_TERMINATED'):
			if state_timestamp_dict.has_key('POST_SCRIPT_STARTED'):
				post_script_time = state_timestamp_dict['POST_SCRIPT_TERMINATED'] - state_timestamp_dict['POST_SCRIPT_STARTED']
				post_script_time = convert_to_seconds(post_script_time)
			elif state_timestamp_dict.has_key('JOB_TERMINATED'):
				post_script_time = state_timestamp_dict['POST_SCRIPT_TERMINATED'] - state_timestamp_dict['JOB_TERMINATED']
				post_script_time = convert_to_seconds(post_script_time)
			else :
				post_script_time = None
				logger.debug("Unable to calculate the post time. POST_SCRIPT_STARTED /JOB_TERMINATED event missing for job "+ job.name)
			#assign post_script_time
			if post_script_time is None:
				job_stat.post = None
			else:
				if job_stat.post =='-':
					job_stat.post = post_script_time
				else:
					job_stat.post += post_script_time		
		else:
			# Means the job failed before reaching this stage
			logger.debug("Unable to calculate the post time. POST_SCRIPT_TERMINATED event missing for job "+ job.name)
	
	#Assigning job state
	if job.is_success:
		job_stat.state ='SUCCESS'
	elif job.is_failure:
		job_stat.state ='FAILED'
	else:
		job_stat.state= job.current_state
	return		
	
	
def populate_transformation_statistics(task, stats):
	"""
	Returns the transformation statistics information
	Param: the job reference
	
	"""
	if (task.task_submit_seq > 0):
		if stats.count > 0:
			stats.count +=1
			stats.total_runtime +=  task.duration
			if stats.min > task.duration :
				stats.min = task.duration
			if stats.max < task.duration :
				stats.max = task.duration
			stats.sum_of_squares += math.pow(task.duration,2)
			if task.exitcode == 0:
				stats.successcount +=1
			else:
				stats.failcount +=1
		else:
			stats.count =1
			stats.total_runtime = task.duration
			stats.sum_of_squares = math.pow(task.duration,2)
			stats.min = task.duration
			stats.max = task.duration
			if task.exitcode == 0:
				stats.successcount = 1
			else:
				stats.failcount =1
			
	return

def populate_stats(wf_uuid):
	workflow = Workflow(global_db_url)
	workflow.initialize(wf_uuid)
	workflow_stats = populate_workflow_details(workflow)
	return workflow_stats
	
def setup(submit_dir):
	global base_submit_dir
	global braindb_submit_dir
	global global_db_url
	global global_top_wf_uuid
	base_submit_dir = submit_dir
	#Getting values from braindump file
	config = slurp_braindb(submit_dir)
	if not config:
		logger.warning("could not process braindump.txt ")
		sys.exit(1)
	wf_uuid = ''
	if (config.has_key('wf_uuid')):
		global_top_wf_uuid = config['wf_uuid']
	else:
		logger.error("workflow id cannot be found in the braindump.txt ")
		sys.exit(1)
	
	if (config.has_key('submit_dir') or config.has_key('run')):
		if config.has_key('submit_dir'):
			braindb_submit_dir =  os.path.abspath(config['submit_dir'])
		else:
			braindb_submit_dir =  os.path.abspath(config['run'])
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
	global_db_url = "sqlite:///" + output_db_file
	if not os.path.isfile(output_db_file):
		logger.error("Unable to find database file in "+submit_dir)
		sys.exit(1)
		
	

