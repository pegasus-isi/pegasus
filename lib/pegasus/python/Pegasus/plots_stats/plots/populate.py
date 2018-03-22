#!/usr/bin/env python

from __future__ import absolute_import
import os
import re
import sys
import logging
import optparse
import math
import tempfile
import commands

logger = logging.getLogger(__name__)

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.tools import utils
from Pegasus.plots_stats import utils as plot_utils
from .workflow_info import WorkflowInfo, JobInfo , TransformationInfo
from . import pegasus_gantt
from . import pegasus_host_over_time
import traceback

from Pegasus.db.workflow.stampede_statistics import StampedeStatistics
from datetime import timedelta
from datetime import datetime

#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'

predefined_colors = ["#8dd3c7" , "#4daf4a","#bebada" , "#80b1d3" , "#b3de69" , "#fccde5" , "#d9d9d9" , "#bc80bd" , "#ccebc5" ,  "#fb8072"]
exclude_transformations =['dagman::post' , 'dagman::pre']
global_transformtion_color_map ={}
global_base_submit_dir = None
global_braindb_submit_dir =None
global_db_url = None
global_top_wf_uuid =None
global_wf_id_uuid_map = {}
color_count =0

def populate_individual_job_instance_details(job_states , job_stat , isFailed , retry_count):
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
	"""
	Returns the mapping of sub workflow jobs to the corresponding wf_uuid
	@workflow  StampedeStatistics object reference.
	"""
	job_inst_wf_uuid_map ={}
	jb_inst_sub_wf_list = workflow.get_job_instance_sub_wf_map()
	for jb_inst_sub_wf in jb_inst_sub_wf_list:
		if jb_inst_sub_wf.subwf_id is not None:
			job_inst_wf_uuid_map[jb_inst_sub_wf.job_instance_id] = global_wf_id_uuid_map[jb_inst_sub_wf.subwf_id]
	return job_inst_wf_uuid_map



#-------return workflow uuid by parsing db alone-----

def get_workflows_uuid():
	"""
	Returns the workflow uuid of a given workflow , this includes the id of all sub workflows.
	"""
	# expand = True
	try:
		expanded_workflow_stats = StampedeStatistics(global_db_url)
	 	expanded_workflow_stats.initialize(global_top_wf_uuid)
	 	expanded_workflow_stats.set_job_filter('all')
 	except:
 		logger.error("Failed to load the database." + global_db_url )
		sys.exit(1)
 	#expand = False
 	try:
	 	root_workflow_stats = StampedeStatistics(global_db_url , False)
	 	root_workflow_stats.initialize(global_top_wf_uuid)
	 	root_workflow_stats.set_job_filter('all')
 	except:
 		logger.error("Failed to load the database." + global_db_url )
		sys.exit(1)
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
def populate_workflow_details(workflow_stats):
	"""
	populates the workflow statistics information
	@param workflow_stats the StampedeStatistics object reference
	"""
	workflow_info = WorkflowInfo()
	
	# Getting the workflow details
	wf_det = workflow_stats.get_workflow_details()[0]
	
	
	# Populating workflow details
	workflow_info.wf_uuid = wf_det.wf_uuid
	if wf_det.parent_wf_id in global_wf_id_uuid_map:
		workflow_info.parent_wf_uuid =global_wf_id_uuid_map[wf_det.parent_wf_id]
	workflow_info.submit_dir = wf_det.submit_dir
	workflow_info.dax_label = wf_det.dax_label
	workflow_info.wf_env = plot_utils.parse_workflow_environment(wf_det)
	return workflow_info


def populate_job_details(workflow_stats , workflow_info):
	"""
	populates the job details of the workflow
	@param workflow_stats the StampedeStatistics object reference
	@param workflow_info the WorkflowInfo object reference 
	"""
	total_jobs =0
	total_jobs = workflow_stats.get_total_jobs_status()
	workflow_info.total_jobs = total_jobs
	

def populate_task_details(workflow_stats, workflow_info):
	"""
	populates the task details of the workflow
	@param workflow_stats the StampedeStatistics object reference
	@param workflow_info the WorkflowInfo object reference 
	"""
	total_tasks = 0
	total_tasks = workflow_stats.get_total_tasks_status()
	workflow_info.total_tasks = total_tasks

def populate_job_instance_details(workflow_stats , workflow_info):
	"""
	populates the job instances details of the workflow
	@param workflow_stats the StampedeStatistics object reference
	@param workflow_info the WorkflowInfo object reference 
	"""
	workflow_run_time = None
	total_job_instances = 0
	transformation_stats_dict ={}
	job_stats_list =[]
	host_job_mapping ={}
	job_name_retry_count_dict ={}
	wf_transformation_color_map ={}
	global color_count
	start_event = sys.maxsize
	end_event = -sys.maxsize -1
	
	worklow_states_list = workflow_stats.get_workflow_states()
	if len(worklow_states_list) > 0:
		if worklow_states_list[0].state != "WORKFLOW_STARTED":
			logger.warning("Mismatch in the order of the events. Taking the first state in the database as the start event  ")
		
		# Storing the start and end event from the workflow states
		start_event = worklow_states_list[0].timestamp
		end_event = worklow_states_list[len(worklow_states_list)-1].timestamp
	else:
		logger.warning("Workflow states are missing for workflow  " + workflow_info.wf_uuid)
	failed_job_list = workflow_stats.get_plots_failed_job_instances()
	job_states_list =  workflow_stats.get_job_states()
	for job_states in job_states_list:
		# Additional check for the case where "WORKFLOW_STARTED" event is missing
		if job_states.jobS is not None:
			start_event = min(int(start_event) , int(job_states.jobS) )
			if job_states.jobDuration is not None:
				end_event = max(int(end_event) , int(job_states.jobS + job_states.jobDuration))
		job_stat = JobInfo()
		job_stats_list.append(job_stat)
		is_job_failed = False
		restart_count =0
		if job_states.job_name in job_name_retry_count_dict:
			restart_count = job_name_retry_count_dict[job_states.job_name]
			restart_count +=1
		job_name_retry_count_dict[job_states.job_name] = restart_count
		if job_states.job_instance_id in failed_job_list:
			is_job_failed = True	
		populate_individual_job_instance_details(job_states ,job_stat , is_job_failed , job_name_retry_count_dict[job_states.job_name])
		# Assigning host to job mapping
		if job_stat.host_name in host_job_mapping:
			job_list =host_job_mapping[job_stat.host_name]
			job_list.append(job_stat)
		else:
			job_list = []
			job_list.append(job_stat)
			host_job_mapping[job_stat.host_name] = job_list
			
		# Assigning the tranformation name
		if job_stat.transformation is not None:
			transformation_stats_dict[job_stat.transformation] = None
		if job_stat.transformation not in global_transformtion_color_map:
			global_transformtion_color_map[job_stat.transformation]= predefined_colors[color_count%len(predefined_colors)]
			color_count +=1
		# Assigning the mapping to the workflow map
		wf_transformation_color_map[job_stat.transformation] =global_transformtion_color_map[job_stat.transformation]
	
		
	if (start_event != sys.maxsize) and  (end_event != (-sys.maxsize -1)):
		workflow_info.workflow_run_time = end_event - start_event
	else:
		logger.error("Unable to find the start and event event for the workflow  " + workflow_info.wf_uuid)
	total_job_instances = len(job_stats_list)
	workflow_info.dagman_start_time = start_event 
	workflow_info.job_statistics_list =job_stats_list
	workflow_info.host_job_map = host_job_mapping
	workflow_info.transformation_statistics_dict = transformation_stats_dict
	workflow_info.transformation_color_map = wf_transformation_color_map
	workflow_info.total_job_instances = total_job_instances
	return workflow_info
	

def populate_transformation_details(workflow_stats , workflow_info):
	"""
	populates the transformation details of the workflow
	@param workflow_stats the StampedeStatistics object reference
	@param workflow_info the WorkflowInfo object reference 
	"""
	transformation_stats_dict ={}
	wf_transformation_color_map ={}
	global color_count
	transformation_stats_list= workflow_stats.get_transformation_statistics()
	for trans_stats in transformation_stats_list:
		if trans_stats.transformation.strip() in exclude_transformations:
			continue
		trans_info = TransformationInfo()
		trans_info.name = trans_stats.transformation
		trans_info.count = trans_stats.count
		trans_info.succeeded_count = trans_stats.success
		trans_info.failed_count = trans_stats.failure
		trans_info.min = trans_stats.min
		trans_info.max = trans_stats.max
		trans_info.avg = trans_stats.avg
		trans_info.total_runtime = trans_stats.sum
		transformation_stats_dict[trans_stats.transformation] = trans_info
		if trans_stats.transformation not in global_transformtion_color_map:
			global_transformtion_color_map[trans_stats.transformation]= predefined_colors[color_count%len(predefined_colors)]
			color_count +=1
		# Assigning the mapping to the workflow map
		wf_transformation_color_map[trans_stats.transformation] =global_transformtion_color_map[trans_stats.transformation]
	workflow_info.transformation_statistics_dict = transformation_stats_dict
	workflow_info.transformation_color_map = wf_transformation_color_map
	
def get_wf_stats(wf_uuid,expand = False):
	workflow_stampede_stats = None
	try:
		workflow_stampede_stats = StampedeStatistics(global_db_url , expand)
		workflow_stampede_stats.initialize(wf_uuid)
        except (connection.ConnectionError, DBAdminError) as e:
                logger.error("------------------------------------------------------")
                logger.error(e)
                sys.exit(1)
	except:
 		logger.error("Failed to load the database." + global_db_url )
 		logger.warning(traceback.format_exc())
		sys.exit(1)
	return workflow_stampede_stats


def populate_chart(wf_uuid , expand = False):
	"""
	Populates the workflow info object corresponding to the wf_uuid
	@param wf_uuid the workflow uuid
	@param expand expand workflow or not.
	"""
	workflow_stampede_stats = get_wf_stats(wf_uuid , expand)
	workflow_info = populate_workflow_details(workflow_stampede_stats)
	sub_wf_uuids = workflow_stampede_stats.get_sub_workflow_ids()
	workflow_info.sub_wf_id_uuids = sub_wf_uuids
	if len(sub_wf_uuids) > 0:
		workflow_info.job_instance_id_sub_wf_uuid_map = get_job_inst_sub_workflow_map(workflow_stampede_stats )
	config = utils.slurp_braindb(rlb(workflow_info.submit_dir))
	if ('dag' in config):
		dag_file_name = config['dag']
		workflow_info.dag_label = dag_file_name[:dag_file_name.find(".dag")]
		workflow_info.dag_file_path = os.path.join(rlb(workflow_info.submit_dir), dag_file_name)
	if ('dax' in config):
		workflow_info.dax_file_path = config['dax']
	return workflow_stampede_stats, workflow_info 
	
def populate_time_details(workflow_stats, wf_info ):
	"""
	Populates the job instances and invocation time and runtime statistics sorted by time.
	@param workflow_stats the StampedeStatistics object reference
	@param workflow_info the WorkflowInfo object reference 
	"""
	workflow_stats.set_job_filter('nonsub')
	# day is calculated from hour.
	workflow_stats.set_time_filter('hour')
	
	job_stats_by_time = workflow_stats.get_jobs_run_by_time()
	workflow_stats.set_transformation_filter(exclude=['condor::dagman'])
	inv_stats_by_time = workflow_stats.get_invocation_by_time()
	populate_job_invocation_time_details(wf_info,job_stats_by_time,inv_stats_by_time ,'hour')
	populate_job_invocation_time_details(wf_info, job_stats_by_time,inv_stats_by_time ,'day')
	

def populate_job_invocation_time_details(wf_info, job_stats, invocation_stats ,date_time_filter):
	"""
	Populates the job instances and invocation time and runtime statistics sorted by time.
	@param workflow_info the WorkflowInfo object reference 
	@param job_stats the job statistics by time tuple
	@param invocation_stats the invocation statisctics by time tuple
	@param date_time_filter date time filter
	"""
	formatted_stats_list = plot_utils.convert_stats_to_base_time(job_stats , date_time_filter)
 	jobs_time_list =[]
	for stats in formatted_stats_list:
		content = [stats['date_format'] , stats['count'],stats['runtime']]
		jobs_time_list.append(content)
	wf_info.wf_job_instances_over_time_statistics[date_time_filter] = jobs_time_list
	
	formatted_stats_list = plot_utils.convert_stats_to_base_time(invocation_stats , date_time_filter)
	invoc_time_list = []
	for stats in formatted_stats_list:
		content = [stats['date_format'] , stats['count'],stats['runtime']]
		invoc_time_list.append(content)
	wf_info.wf_invocations_over_time_statistics[date_time_filter] = invoc_time_list

def setup(submit_dir , config_properties):
	"""
	Setup the populate module
	@submit_dir submit directory path of the workflow run
	@config_properties path to the propery file
	"""
	# global reference
	global global_base_submit_dir
	global global_braindb_submit_dir
	global global_db_url
	global global_top_wf_uuid
	global_base_submit_dir = submit_dir
	#Getting values from braindump file
	config = utils.slurp_braindb(submit_dir)
	if ('submit_dir' in config or 'run' in config):
		if 'submit_dir' in config:
			global_braindb_submit_dir =  os.path.abspath(config['submit_dir'])
		else:
			global_braindb_submit_dir =  os.path.abspath(config['run'])
	else:
		logger.error("Submit directory cannot be found in the braindump.txt . ")
		sys.exit(1)
	
	# Create the sqllite db url
        global_db_url = connection.url_by_submitdir(submit_dir, connection.DBType.WORKFLOW, config_properties)
        global_top_wf_uuid = connection.get_wf_uuid(submit_dir)
	if global_db_url is None:
		sys.exit(1)
