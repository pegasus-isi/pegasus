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
from Pegasus.tools import utils
from Pegasus.plots_stats import utils as plot_utils
from workflow_info import WorkflowInfo, JobInfo
import pegasus_gantt
import pegasus_host_over_time

from netlogger.analysis.workflow.stampede_statistics import StampedeStatistics
from datetime import timedelta
from datetime import datetime

#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'

predefined_colors = ["#8dd3c7" , "#4daf4a","#bebada" , "#80b1d3" , "#b3de69" , "#fccde5" , "#d9d9d9" , "#bc80bd" , "#ccebc5" ,  "#fb8072"]
global_transformtion_color_map ={}
global_base_submit_dir = None
global_braindb_submit_dir =None
global_db_url = None
global_top_wf_uuid =None
global_wf_id_uuid_map = {}

def populate_job_details(job_states , job_stat, dagman_start_time , isFailed , retry_count):
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
	job_stat.name = job_states.job_name
	# assigning site name
	job_stat.site = job_states.site
	# assigning instance id
	job_stat.instance_id = job_states.job_instance_id
	# Assumption host name is unique
	job_stat.host_name = job_states.host_name
	
	# Setting the first job state as the job start
	job_stat.jobStart = job_states.jobS
	# Setting the last job state as the job end
	job_stat.jobDuration =  job_states.jobDuration
	
	# Setting SUBMIT as the execution job start	
	job_stat.jobExecStart = job_states.condor_start
	# Setting the JOB_TERMINATED as the end event for the host over time chart if it is present , otherwise that last event.
	if job_states.condor_duration is not None:
		job_duration = job_states.condor_duration
	else:
		job_duration = job_states.jobDuration
	job_stat.jobExecDuration =  job_duration
			
	#kickstart time
	# Assigning start and duration of kickstart , pre and post
	job_stat.kickstartStart = job_states.kickstart_start 
	job_stat.kickstartDuration = job_states.kickstart_duration
	
	#transformations associated with job
	job_stat.transformation = job_states.transformation
	
	# pre script time
	job_stat.preStart = job_states.pre_start
	job_stat.preDuration = job_states.pre_duration
	
	# post script time
	job_stat.postStart = job_states.post_start
	job_stat.postDuration = job_states.post_duration
	
	
	# GRID/GLOBUS SUBMIT start and duration
	job_stat.gridStart = job_states.grid_start
	job_stat.gridDuration = job_states.grid_duration
	
	#runtime
	job_stat.executeStart = job_states.exec_start
	job_stat.executeDuration = job_states.exec_duration
	
	#condor start to end
	job_stat.condorStart = job_states.condor_start
	job_stat.condorDuration = job_states.condor_duration
	
	
	#Assigning job state
	if isFailed:
		job_stat.state ='FAILED'
	else:
		job_stat.state= "Unknown"
	job_stat.retry_count = retry_count
	return		
	

def rlb(file_path):
	"""
	This function converts the path relative to base path
	Returns : path relative to the base 
	"""
	file_path = plot_utils.rlb(file_path, global_braindb_submit_dir,global_base_submit_dir)
	return file_path
	
							
#------------Gets sub worklows job names----------------
def get_job_inst_sub_workflow_map(workflow ):
	job_inst_wf_uuid_map ={}
	jb_inst_sub_wf_list = workflow.get_job_instance_sub_wf_map()
	for jb_inst_sub_wf in jb_inst_sub_wf_list:
		job_inst_wf_uuid_map[jb_inst_sub_wf.job_instance_id] = global_wf_id_uuid_map[jb_inst_sub_wf.subwf_id]
	return job_inst_wf_uuid_map



#-------return workflow uuid by parsing db alone-----

def get_workflows_uuid():
	# expand = True
	expanded_workflow_stats = StampedeStatistics(global_db_url)
 	expanded_workflow_stats.initialize(global_top_wf_uuid)
 	expanded_workflow_stats.set_job_filter('all')
 	#expand = False
 	root_workflow_stats = StampedeStatistics(global_db_url , False)
 	root_workflow_stats.initialize(global_top_wf_uuid)
 	root_workflow_stats.set_job_filter('all')
 	
 	wf_det = root_workflow_stats.get_workflow_details()[0]
 	# print workflow statistics
 	global global_wf_id_uuid_map
 	global_wf_id_uuid_map[wf_det.wf_id] = global_top_wf_uuid
 	wf_uuid_list = [global_top_wf_uuid]
	desc_wf_uuid_list = expanded_workflow_stats.get_descendant_workflow_ids()
	for wf_det in desc_wf_uuid_list:
		global_wf_id_uuid_map[wf_det.wf_id] = wf_det.wf_uuid
		wf_uuid_list.append(wf_det.wf_uuid)
	return wf_uuid_list
				


# ---------populate_workflow_details--------------------------
def populate_workflow_details(workflow):
	"""
	populates the workflow statistics information
	Param: the workflow reference
	"""
	workflow_stat = WorkflowInfo()
	workflow_run_time = None
	total_jobs =0
	total_tasks = 0
	total_job_instances = 0
	transformation_stats_dict ={}
	job_stats_list =[]
	host_job_mapping ={}
	job_name_retry_count_dict ={}
	wf_transformation_color_map ={}
	
	worklow_states_list = workflow.get_workflow_states()
	if len(worklow_states_list) < 1:
		logger.warning("Unable to find the start event for the workflow ")
		workflow_stat.wf_uuid = wf_det.wf_uuid
		workflow_stat.parent_wf_uuid =global_wf_id_uuid_map[wf_det.parent_wf_uuid]
		workflow_stat.submit_dir = wf_det.submit_dir
		workflow_stat.dax_label = wf_det.dax_label
		return workflow_stat
	if worklow_states_list[0].state != "WORKFLOW_STARTED":
		logger.warning("Mismatch in the order of the events. Taking the first state in the database as the start event  ")
	walltime = plot_utils.get_workflow_wall_time(worklow_states_list)
	if walltime is not None:
		workflow_run_time = walltime
	color_count = 0
	# populating statistics details
	wf_det = workflow.get_workflow_details()[0]
	
	
	
	failed_job_list = workflow.get_failed_job_instances()
	dagman_start_time = worklow_states_list[0].timestamp
	# for root jobs,dagman_start_time is required, assumption start_event[0] is not none
	job_states_list =  workflow.get_job_states()
	for job_states in job_states_list:
		job_stat = JobInfo()
		job_stats_list.append(job_stat)
		is_job_failed = False
		restart_count =0
		if job_name_retry_count_dict.has_key(job_states.job_name):
			restart_count = job_name_retry_count_dict[job_states.job_name]
			restart_count +=1
		job_name_retry_count_dict[job_states.job_name] = restart_count
		if job_states.job_instance_id in failed_job_list:
			is_job_failed = True	
		populate_job_details(job_states ,job_stat , dagman_start_time , is_job_failed , job_name_retry_count_dict[job_states.job_name])
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
	total_job_instances = len(job_stats_list)
	total_jobs = workflow.get_total_jobs_status() 
	total_tasks = workflow.get_total_tasks_status()
	# Assigning value to the workflow object
	workflow_stat.wf_uuid = wf_det.wf_uuid
	
	if global_wf_id_uuid_map.has_key(wf_det.parent_wf_id):
		workflow_stat.parent_wf_uuid =global_wf_id_uuid_map[wf_det.parent_wf_id]
		
	workflow_stat.submit_dir = wf_det.submit_dir
	workflow_stat.dax_label = wf_det.dax_label
	if workflow_run_time is not None:
		workflow_stat.workflow_run_time =workflow_run_time
	workflow_stat.total_jobs = total_jobs
	workflow_stat.total_job_instances = total_job_instances
	workflow_stat.total_tasks = total_tasks
	workflow_stat.dagman_start_time = dagman_start_time
	workflow_stat.job_statistics_list =job_stats_list
	workflow_stat.host_job_map = host_job_mapping
	workflow_stat.transformation_statistics_dict = transformation_stats_dict
	workflow_stat.transformation_color_map = wf_transformation_color_map
	workflow_stat.wf_env = plot_utils.parse_workflow_environment(wf_det)
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
	
def populate_chart(wf_uuid):
	"""
	Populates the workflow info object corresponding to the wf_uuid
	"""
	workflow = StampedeStatistics(global_db_url , False)
 	workflow.initialize(wf_uuid)
	workflow_info = populate_workflow_details(workflow)
	sub_wf_uuids = workflow.get_sub_workflow_ids()
	if len(sub_wf_uuids) > 0:
		workflow_info.job_instance_id_sub_wf_uuid_map = get_job_inst_sub_workflow_map(workflow )
	config = utils.slurp_braindb(rlb(workflow_info.submit_dir))
	if (config.has_key('dag')):
		dag_file_name = config['dag']
		workflow_info.dag_label = dag_file_name[:dag_file_name.find(".dag")]
		workflow_info.dag_file_path = os.path.join(rlb(workflow_info.submit_dir), dag_file_name)
	if (config.has_key('dax')):
		workflow_info.dax_file_path = config['dax']
	return workflow_info
	


def setup(submit_dir , config_properties):
	# global reference
	global global_base_submit_dir
	global global_braindb_submit_dir
	global global_db_url
	global global_top_wf_uuid
	global_base_submit_dir = submit_dir
	#Getting values from braindump file
	config = utils.slurp_braindb(submit_dir)
	if (config.has_key('submit_dir') or config.has_key('run')):
		if config.has_key('submit_dir'):
			global_braindb_submit_dir =  os.path.abspath(config['submit_dir'])
		else:
			global_braindb_submit_dir =  os.path.abspath(config['run'])
	else:
		logger.error("Submit directory cannot be found in the braindump.txt . ")
		sys.exit(1)
	
	# Create the sqllite db url
	global_db_url , global_top_wf_uuid = plot_utils.get_db_url_wf_uuid(submit_dir , config_properties)
	if global_db_url is None:
		sys.exit(1)

	
