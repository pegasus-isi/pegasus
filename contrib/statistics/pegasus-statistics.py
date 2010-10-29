#!/usr/bin/env python

from netlogger.analysis.workflow.sql_alchemy import *
from datetime import timedelta
import os
import re
import sys
import logging
import optparse
import math
import tempfile


#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'

base_submit_dir = None;
braindb_submit_dir =None;
job_detail_title =['#Job','Site','Kickstart','Post' ,'DAGMan','CondorQTime','Resource','Runtime','Seqexec','Seqexec-Delay']
job_statistics_size =[4,4,9,4,6,11,8,7,7,12]
transformation_statistics_title =['#Transformation','Count' ,'Succeeded', 'Failed','Mean' ,'Variance','Min' ,'Max' ,'Total']
transformation_statistics_size =[13,5,9,6,4,8,3,3,5]
output_dir=''
cell_spacing =1
job_run_statistics_size = 50
condor = 0				# pure condor run - no GRID_SUBMIT events



# Initialize logging object
logger = logging.getLogger()

#---workflow statistics

class WorkflowStatistics:
	def __init__(self):
		self.submit_dir ='-'
		self.workflow_run_time = '-'
		self.workflow_run_wall_time ='-'
		self.total_jobs = '-'
		self.succeeded_jobs ='-'
		self.failed_jobs ='-'
		self.unsubmitted_jobs ='-'
		self.unknown_jobs ='-'
		self.total_succeeded_tasks ='-'
		self.total_failed_tasks ='-'
		self.job_statistics_dict ={}
		self.transformation_statistics_dict ={}
	
	
	def get_formatted_workflow_info(self):
		
		workflow_info = ''
		workflow_info +=("#" + self.submit_dir)
		workflow_info +=( "\n")
		workflow_info +=(("Total workflow execution time      :" + str(self.workflow_run_time)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("Total workflow execution wall time :" + str(self.workflow_run_wall_time)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("Total jobs run                     :" + str(self.total_jobs)).ljust(job_run_statistics_size))
		workflow_info +=("\n")	
		workflow_info +=(("# jobs succeeded                   :" + str(self.succeeded_jobs)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("# jobs failed                      :" + str(self.failed_jobs)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("# jobs unsubmitted                 :" + str(self.unsubmitted_jobs)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("# jobs unknown                     :" + str(self.unknown_jobs)).ljust(job_run_statistics_size))
		workflow_info +=("\n")
		workflow_info +=(("# Total tasks succeeded            :" + str(self.total_succeeded_tasks)).ljust(job_run_statistics_size))
		workflow_info +=("\n")	
		workflow_info +=(("# Total tasks failed               :" + str(self.total_failed_tasks)).ljust(job_run_statistics_size))
		workflow_info +=("\n")	
		return workflow_info
		
	# ---------pretty_print--------------------------

	def pretty_print(self,value_list ,size_list):
		"""
		Assigns column size value for pretty print 
		Param: value_list list of column values
		Param: value_list list of column sizes
		"""
		for ind in range(len(value_list)):
			if isinstance(value_list[ind],basestring):
				if size_list[ind] < len(value_list[ind]):
					size_list[ind]= len(value_list[ind])
			else:
				if size_list[ind] < len(str(value_list[ind])):		
					size_list[ind]= len(str(value_list[ind]))
		return
			
	def get_formatted_job_info(self):
		# find the pretty print length
		job_info = ''
		job_info +=("#" + self.submit_dir)
		job_info +=( "\n")
		for job_stat in self.job_statistics_dict.values():
			job_det = [job_stat.name,job_stat.site,job_stat.kickstart,job_stat.post, job_stat.dagman, job_stat.condor_delay , job_stat.resource, job_stat.runtime,job_stat.seqexec, job_stat.seqexec_delay]
			self.pretty_print(job_det,job_statistics_size)
		# print title
		for index in range(len(job_detail_title)):
			job_info += (job_detail_title[index].ljust(job_statistics_size[index]+ cell_spacing))
		#Add sort
		keys = self.job_statistics_dict.keys()
		keys.sort()	
		for key in keys:
			job_stat = self.job_statistics_dict[key]
			job_info += ( "\n")
			job_det = [job_stat.name,job_stat.site,job_stat.kickstart,job_stat.post,job_stat.dagman,job_stat.condor_delay, job_stat.resource, job_stat.runtime,job_stat.seqexec, job_stat.seqexec_delay]
			for index in range(len(job_det)):
				if isinstance(job_det[index],basestring):
					job_info += (job_det[index].ljust(job_statistics_size[index] + cell_spacing))
				else:
					job_info += (str(job_det[index]).ljust(job_statistics_size[index] + cell_spacing))
		job_info +=( "\n")
		return job_info
						
	def get_formatted_transformation_info(self):
		trans_info = ''
		trans_info +=("#" + self.submit_dir)
		trans_info +=( "\n")
		for k,v in self.transformation_statistics_dict.items():
			trans_det = [k,v.count, v.successcount, v.failcount, v.mean, v.variance,v.min, v.max , v.total_runtime]
			self.pretty_print( trans_det,transformation_statistics_size)			
		# print title
		for index in range(len(transformation_statistics_title)):
			trans_info += (transformation_statistics_title[index].ljust(transformation_statistics_size[index]+ cell_spacing))
		keys = 	self.transformation_statistics_dict.keys()
		keys.sort()
		# Sorting by name
		for k in keys:
			v = self.transformation_statistics_dict[k]
			trans_info += ( "\n")
			trans_det = [k,v.count,v.successcount,v.failcount, v.mean, v.variance,v.min, v.max , v.total_runtime]
			for index in range(len(trans_det)):
				if isinstance(trans_det[index],basestring):
					trans_info += (trans_det[index].ljust(transformation_statistics_size[index] + cell_spacing))
				else:
					trans_info +=( str(trans_det[index]).ljust(transformation_statistics_size[index]+ cell_spacing))
		trans_info +=( "\n")
		return trans_info
#---job statistics ---

class JobStatistics:
	def __init__(self):
		self.name ='-'
		self.site ='-'
		self.kickstart ='-'
		self.post ='-'
		self.dagman ='-'
		self.condor_delay ='-'
		self.resource ='-'
		self.runtime ='-'
		self.condorQlen ='-'
		self.seqexec='-'
		self.seqexec_delay ='-'
		self.state = None
		
	@property
	def is_success(self):
		if self.state =='SUCCESS':
			return True
		return False
	
	@property
	def is_failure(self):
		if self.state =='FAILED':
			return True
		return False

#---transformation-statistics-----

class TransformationStatistics:
	
	def __init__(self):
		self.count =0
		self.__variance =0.0
		self.total_runtime =0.0
		self.sum_of_squares =0.0
		self.__mean =0.0
		self.min =0.0
		self.max =0.0
		self.failcount = 0
		self.successcount =0
		
	@property	
	def mean(self):
		self.__mean = self.total_runtime/self.count
		return self.__mean
		
	@property	
	def variance(self):
		if self.count > 0 :
			self.__variance = (self.sum_of_squares/self.count)- math.pow(self.mean,2)
		else:
			self.__variance = 0.0
		return self.__variance	

def setup_logger(level_str):

	# log to the console
	console = logging.StreamHandler()
	# default log level - make logger/console match
	logger.setLevel(logging.INFO)
	console.setLevel(logging.INFO)
	# level - from the command line
	level_str = level_str.lower()
	if level_str == "debug":
		logger.setLevel(logging.DEBUG)
		console.setLevel(logging.DEBUG)
	if level_str == "warning":
		logger.setLevel(logging.WARNING)
		console.setLevel(logging.WARNING)
	if level_str == "error":
		logger.setLevel(logging.ERROR)
		console.setLevel(logging.ERROR)
	# formatter
	formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
	console.setFormatter(formatter)
	logger.addHandler(console)
	logger.debug("Logger has been configured")
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

#----parse dag file----------

def parse_dag_file(dag_fn):
	"""
	This function walks through the dag file, looks for sub workflow
	jobs.
	Returns : sub workflow .sub file name list, empty if there is no sub workflow
	"""
	job_list =[]
	# Open dag file
	try:
		DAG = open(dag_fn, "r")
	except:
		logger.error( "Could not open dag file ." +(dag_fn))
		sys.exit(1)
	
	# Loop through the dag file
	for line in DAG:
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
		if line.startswith("JOB"):
		# This is a job line, let's parse it
			my_job = line.split()
			if len(my_job) != 3:
				logger.debug( "Unable to parse dag line:" + line)
				continue
			job = my_job[1].strip()	
			if job.startswith("pegasus-plan") or job.startswith("subdax_"):	
				job_list.append(my_job[2])
	return job_list


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
	return rlb(init_dir_path)

def rlb(file_path):
	file_path = file_path.replace(braindb_submit_dir,base_submit_dir)
	return file_path
							
#------------get_sub_worklows----------------
def get_sub_workflows(root_submit_dir , dag_file_name):
	sub_wf_uuid_list =[]
	submit_file_list =[]
	root_dag_file_path =os.path.join(root_submit_dir , dag_file_name)
	# Adds the root dag to the dag file list 
	dag_file_list =[root_dag_file_path]
	while len(dag_file_list) > 0:
		#pops the dag file path
		dag_file_path =  dag_file_list[0]
		dag_file_list.remove(dag_file_path)
		# parses the dag file to get the sub workflow .sub file names
		submit_file_list = parse_dag_file(dag_file_path)
		# split the dag file path to get the submit directory
		sub_dir = os.path.split(dag_file_path)[0]
		# parses through the .sub file names
		while len(submit_file_list) >0:
			# pops the sub workflow .sub file name
			sub_submit_file_name = submit_file_list[0]
			submit_file_list.remove(sub_submit_file_name)
			# builds the sub workflow .sub file path
			sub_wf_submit_file_path =os.path.join(sub_dir,sub_submit_file_name)
			# parses the .sub file for the 'initialdir' value
			sub_init_dir =parse_submit_file(sub_wf_submit_file_path) 
			if  sub_init_dir is not None:
				# creates the sub workflow brain dump file path
				sub_braindb = os.path.join(sub_init_dir,brainbase)
				#Getting values from braindump file
				config = slurp_braindb(sub_init_dir)
				if not config:
					logger.warning("could not process braindump.txt ")
					continue
				wf_uuid = ''
				if (config.has_key('wf_uuid')):
					sub_wf_uuid = config['wf_uuid']
					# stores the wf-uuid value
					sub_wf_uuid_list.append(sub_wf_uuid)
				else:
					logger.debug("workflow id cannot be found in the braindump.txt ")
				sub_dag_file_name =''
				if (config.has_key('dag')):
					sub_dag_file_name = config['dag']
				else:
					logger.debug("Dag file name cannot be found in the braindump.txt ")
					continue
				# adds the sub workflow dag file to the list	
				sub_dag_file_path = os.path.join(sub_init_dir,sub_dag_file_name)		
				dag_file_list.append(sub_dag_file_path)
	return sub_wf_uuid_list	
				

#-----date conversion----------
def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)


def formatted_job_stats_legends():
	formatted_job_stats_legend=("#legends \n")
	formatted_job_stats_legend +=("#Job - the name of the job \n")
	formatted_job_stats_legend +=("#Site - the site where the job ran \n")
	formatted_job_stats_legend +=("#Kickstart - the actual duration of the job in seconds on the remote compute node \n")
	formatted_job_stats_legend +=("#Post - the postscript time as reported by DAGMan \n")
	formatted_job_stats_legend +=("#DAGMan - the time between the last parent job  of a job completes and the job gets submitted.For root jobs it is the time between the dagman start time and the time job gets submitted.\n")
	formatted_job_stats_legend +=("#CondorQTime - the time between submission by DAGMan and the remote Grid submission. It is an estimate of the time spent in the condor q on the submit node \n")
	formatted_job_stats_legend +=("#Resource - the time between the remote Grid submission and start of remote execution . It is an estimate of the time job spent in the remote queue \n")
	formatted_job_stats_legend +=("#Runtime - the time spent on the resource as seen by Condor DAGMan . Is always >=kickstart \n")
	#formatted_job_stats_legend +=("#CondorQLen - the number of outstanding jobs in the queue when this job was released \n")
	formatted_job_stats_legend +=("#Seqexec -  the time taken for the completion of a clustered job\n")
	formatted_job_stats_legend +=("#Seqexec-Delay - the time difference between the time for the completion of a clustered job and sum of all the individual tasks kickstart time\n")
	formatted_job_stats_legend +=( "\n")
	return formatted_job_stats_legend
	


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
	failed_jobs =0
	succeeded_jobs =0
	unknown_jobs =0
	unsubmitted_jobs =0
	workflow_run_time = None
	workflow_run_wall_time =0
	if not workflow.is_running :
		workflow_run_time = convert_to_seconds(workflow.total_time)
	for job in workflow.jobs:
		for task in job.tasks:
			if (task.task_submit_seq > 0):
				workflow_run_wall_time +=task.duration
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
	
	#Calculating total jobs
	total_jobs = len(job_stats_dict)
		
	#Calculating failed and successful jobs	
	for job_stat in job_stats_dict.values():
		if job_stat.is_success:
			succeeded_jobs +=1
		elif job_stat.is_failure:
			failed_jobs +=1
		elif job_stat.state is none:
			unsubmitted_jobs +=1
		else:
			unknown_jobs +=1
	# Assigning value to the workflow object
	workflow_stat.submit_dir = workflow.submit_dir		
	if workflow_run_time is not None:
		workflow_stat.workflow_run_time =workflow_run_time
	workflow_stat.workflow_run_wall_time = workflow_run_wall_time
	workflow_stat.total_jobs = total_jobs
	workflow_stat.succeeded_jobs = succeeded_jobs
	workflow_stat.failed_jobs = failed_jobs
	workflow_stat.unsubmitted_jobs = unsubmitted_jobs
	workflow_stat.unknown_jobs = unknown_jobs
	workflow_stat.total_succeeded_tasks = total_succeeded_tasks
	workflow_stat.total_failed_tasks =total_failed_tasks
	workflow_stat.job_statistics_dict = job_stats_dict
	workflow_stat.transformation_statistics_dict = transformation_stats_dict
	return workflow_stat

#----------print workflow details--------
"""
	Prints the workflow statistics information
	Param: the workflow reference
	"""
def print_workflow_details(workflow_stat_list):
	# print workflow statistics
	all_stat = WorkflowStatistics()
	wf_stats_file = os.path.join(output_dir,  "workflow")
	try:
		fh = open(wf_stats_file, "w")
		all_stat.submit_dir = "All"
		all_stat.workflow_run_time =workflow_stat_list[0].workflow_run_time
		all_stat.workflow_run_wall_time = workflow_stat_list[0].workflow_run_wall_time
		all_stat.total_jobs = 0
		all_stat.succeeded_jobs  =0
		all_stat.failed_jobs  = 0
		all_stat.unsubmitted_jobs =0
		all_stat.unknown_jobs  =0
		all_stat.total_succeeded_tasks  =0
		all_stat.total_failed_tasks  =0
		#print job run statistics
		for workflow_stat in workflow_stat_list:
			wf_info = workflow_stat.get_formatted_workflow_info()
			all_stat.total_jobs += workflow_stat.total_jobs
			all_stat.succeeded_jobs += workflow_stat.succeeded_jobs
			all_stat.failed_jobs += workflow_stat.failed_jobs
			all_stat.unsubmitted_jobs += workflow_stat.unsubmitted_jobs
			all_stat.unknown_jobs += workflow_stat.unknown_jobs
			all_stat.total_succeeded_tasks += workflow_stat.total_succeeded_tasks
			all_stat.total_failed_tasks +=workflow_stat.total_failed_tasks
			fh.write(wf_info)
			fh.write( "\n")
		wf_info = all_stat.get_formatted_workflow_info()	
		fh.write(wf_info)
		fh.write("\n")
	except IOError:
		logger.error("Unable to write to file " + wf_stats_file)
		sys.exit(1)
	else:
		fh.close()
	# print job statistics
	jobs_stats_file = os.path.join(output_dir,  "jobs")
	try:
		fh = open(jobs_stats_file, "w")
		# print legends
		job_stats_legend = formatted_job_stats_legends()
		fh.write(job_stats_legend)
		fh.write( "\n")
		#print job statistics
		for workflow_stat in workflow_stat_list:
			job_info = workflow_stat.get_formatted_job_info()
			fh.write(job_info)
			fh.write( "\n")
	except IOError:
		logger.error("Unable to write to file " + jobs_stats_file)
		sys.exit(1)
	else:
		fh.close()
	# printing transformation stats
	all_trans_stats_dict = {}
	transformation_stats_file = os.path.join(output_dir,  "breakdown.txt")
	try:
		fh = open(transformation_stats_file, "w")
		for workflow_stat in workflow_stat_list:
			trans_info = workflow_stat.get_formatted_transformation_info()
			fh.write(trans_info)
			fh.write( "\n")
			wk_trans_stat = workflow_stat.transformation_statistics_dict
			for trans_name,trans_stats in wk_trans_stat.items():
				if all_trans_stats_dict.has_key(trans_name):
					stats = all_trans_stats_dict[trans_name]
					stats.count +=trans_stats.count
					stats.total_runtime  += trans_stats.total_runtime
					if stats.min > trans_stats.min :
						stats.min = trans_stats.min
					if stats.max < trans_stats.max :
						stats.max = trans_stats.max
					stats.sum_of_squares += trans_stats.sum_of_squares
					stats.successcount +=trans_stats.successcount
					stats.failcount +=trans_stats.failcount
				else:
					stats = TransformationStatistics()
					all_trans_stats_dict[trans_name] = stats
					stats.count =trans_stats.count
					stats.total_runtime =  trans_stats.total_runtime
					stats.min = trans_stats.min
					stats.max = trans_stats.max
					stats.sum_of_squares = trans_stats.sum_of_squares
					stats.successcount =trans_stats.successcount
					stats.failcount =trans_stats.failcount
		all_stat.transformation_statistics_dict = all_trans_stats_dict
		trans_info = all_stat.get_formatted_transformation_info()
		fh.write(trans_info)
		fh.write( "\n")			
	except IOError:
		logger.error("Unable to write to file " + transformation_stats_file)
		sys.exit(1)
	else:
		fh.close()			
	logger.info("Job statistics at " + jobs_stats_file)
	logger.info("Logical transformation statistics at " + transformation_stats_file)
	return

def populate_job_details(job , job_stat, dagman_start_time):
	"""
	Returns the job statistics information
	Param: the job reference
	"""
	state_timestamp_dict ={}
	kickstart = '-'
	post_script_time ='-'
	resource_delay ='-'
	condor_delay ='-'
	dagman_delay ='-'
	runtime ='-'
	seqexec ='-'
	seqexec_delay ='-'
	
	# assigning job name
	job_stat.name = job.name
	# assigning site name
	job_stat.site = job.site_name
	
	jobstates = job.all_jobstates
	for jobstate in jobstates:
		state_timestamp_dict[jobstate.state] = jobstate.timestamp
	# default values	
	
	# sanity check
	if not state_timestamp_dict.has_key('SUBMIT'):
		logger.debug("Unable to generate statistics for the job.Failed to find the SUBMIT event for job "+ job.name)
		return job_stat
	#kickstart time
	# need to check for the submit_seq_id instead
	tasks = job.tasks
	if len(tasks) > 0:
		if job_stat.kickstart =='-':
			kickstart = 0
		else:
			kickstart = job_stat.kickstart	
		taskcount =0
		for task in tasks:
			if (task.task_submit_seq > 0): 
				kickstart +=task.duration
				taskcount +=1
		if taskcount > 1 and not (job.clustered) :
			logger.debug(job.name +" has more than one compute task associated to it " )
		#assign kickstart	
		job_stat.kickstart	= kickstart
	else :
		logger.debug(job.name +" has no task associated to it." )
	
	# condor_delay and resource delay
	if state_timestamp_dict.has_key('GRID_SUBMIT'):
		condor_delay = state_timestamp_dict['GRID_SUBMIT'] - state_timestamp_dict['SUBMIT']
		condor_delay=convert_to_seconds(condor_delay)
		if state_timestamp_dict.has_key('EXECUTE'):
			resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GRID_SUBMIT']
			resource_delay =convert_to_seconds(resource_delay)
		else :
			logger.debug("Unable to find the resource delay.Failed to find EXECUTE event for job "+ job.name)
	elif state_timestamp_dict.has_key('GLOBUS_SUBMIT'):
		condor_delay = state_timestamp_dict['GLOBUS_SUBMIT'] - state_timestamp_dict['SUBMIT']
		condor_delay=convert_to_seconds(condor_delay)
		if state_timestamp_dict.has_key('EXECUTE'):
			resource_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['GLOBUS_SUBMIT']
			resource_delay =convert_to_seconds(resource_delay)
		else :
			logger.debug("Unable to find the resource delay.Failed to find EXECUTE event for job "+ job.name)		
	else :
		if state_timestamp_dict.has_key('EXECUTE'):
			condor_delay = state_timestamp_dict['EXECUTE'] - state_timestamp_dict['SUBMIT']
			condor_delay=convert_to_seconds(condor_delay)
		else :
			logger.debug("Unable to find the condor delay.Failed to find EXECUTE event for job "+ job.name)
		logger.debug("Unable to find the resource delay.Failed to find GRID_SUBMIT or GLOBUS_SUBMIT event for job "+ job.name)
	
	#Assigning condor delay and resource delay
	job_stat.resource = resource_delay
	job_stat.condor_delay = condor_delay
	
	#Assigning dagmanDelay
	max_parent_end_time = None
	end_time = None
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
				logger.debug("Unable to find the dagman delay.Failed to find POST_SCRIPT_TERMINATED or JOB_TERMINATED event for the parent job "+parent.name + " of "+ job.name)
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
	
	#runtime
	if state_timestamp_dict.has_key('JOB_TERMINATED'):
		if state_timestamp_dict.has_key('EXECUTE'):
			runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['EXECUTE']
			runtime =convert_to_seconds(runtime)
		else: 
			runtime = state_timestamp_dict['JOB_TERMINATED'] - state_timestamp_dict['SUBMIT']
			runtime=convert_to_seconds(runtime)
		#assigning runtime
		job_stat.runtime = runtime	
	else :
		logger.debug("Unable to find the runtime.Failed to find JOB_TERMINATED event for job "+ job.name)
	
	#seqexec and seqexec-delay
	if job.clustered:
		seqexec = str(job.cluster_duration)
		seqexec_delay = str(job.cluster_duration-kickstart)
		#assigning 	seqexec and seqexec-delay		
		job_stat.seqexec = seqexec
		job_stat.seqexec_delay = seqexec_delay
	
	# post script time
	if state_timestamp_dict.has_key('POST_SCRIPT_TERMINATED'):
		if state_timestamp_dict.has_key('POST_SCRIPT_STARTED'):
			post_script_time = state_timestamp_dict['POST_SCRIPT_TERMINATED'] - state_timestamp_dict['POST_SCRIPT_STARTED']
			post_script_time = convert_to_seconds(post_script_time)
		elif state_timestamp_dict.has_key('JOB_TERMINATED'):
			post_script_time = state_timestamp_dict['POST_SCRIPT_TERMINATED'] - state_timestamp_dict['JOB_TERMINATED']
			post_script_time = convert_to_seconds(post_script_time)
		else :
			logger.debug("Unable to find the post time.Failed to find POST_SCRIPT_STARTED or JOB_TERMINATED event for job "+ job.name)
		#assign post_script_time
		job_stat.post = post_script_time	
	else:
		logger.debug("Unable to find the post time.Failed to find event POST_SCRIPT_TERMINATED for job "+ job.name)
	
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
	
	
# ---------main----------------------------------------------------------------------------

# Configure command line option parser
prog_usage = prog_base +" [options] SUBMIT DIRECTORY" 
parser = optparse.OptionParser(usage=prog_usage)
parser.add_option("-o", "--output", action = "store", dest = "output_dir",
		help = "writes the output to given directory.")
parser.add_option("-c", "--condor", action = "store_const", const = 1, dest = "condor",
		help = "pure condor run - no GRID_SUBMIT events")
parser.add_option("-l", "--loglevel", action = "store", dest = "log_level",
		help = "Log level. Valid levels are: debug,info,warning,error, Default is info.")
# Parse command line options
(options, args) = parser.parse_args()

print prog_base +" : initializing..."
if len(args) < 1:
	parser.error("Please specify Submit Directory")
	sys.exit(1)

if len(args) > 1:
	parser.error("Invalid argument")
	sys.exit(1) 

submit_dir = os.path.abspath(args[0])
base_submit_dir = submit_dir

# Copy options from the command line parser
if options.condor is not None:
	condor = options.condor
if options.output_dir is not None:
	output_dir = options.output_dir
	if not os.path.isdir(output_dir):
		logger.warning("Output directory doesn't exists. Creating directory... ")
		try:
			os.mkdir(output_dir)
		except:
			logger.error("Unable to create output directory."+output_dir)
			sys.exit(1) 	
else :
	output_dir = tempfile.mkdtemp()
if options.log_level == None:
	options.log_level = "info"
setup_logger(options.log_level)	
#Getting values from braindump file
config = slurp_braindb(submit_dir)
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
	braindb_submit_dir =  os.path.abspath(config['submit_dir'])
else:
	logger.error("Submit directory cannot be found in the braindump.txt ")
	sys.exit(1)

dag_file_name =''
if (config.has_key('dag')):
	dag_file_name = config['dag']
else:
	logger.error("Dag file name cannot be found in the braindump.txt ")
	sys.exit(1)	

sub_workflow_list = get_sub_workflows(submit_dir, dag_file_name)

# Create the sqllite db url
output_db_file =submit_dir +"/"+ dag_file_name[:dag_file_name.find(".dag")] + ".stampede.db"
output_db_url = "sqlite:///" + output_db_file
if os.path.isfile(output_db_file):
	w = Workflow(output_db_url)
	w.initialize(wf_uuid)
else:
	logger.error("Unable to find database file in "+submit_dir)
	sys.exit(1)
workflow_stat = populate_workflow_details(w)
workflow_stat_list = []
workflow_stat_list.append(workflow_stat)
for sub_wf_uuid in sub_workflow_list:
	w = Workflow(output_db_url)
	w.initialize(sub_wf_uuid)
	workflow_stat = populate_workflow_details(w)
	workflow_stat_list.append(workflow_stat)
print_workflow_details(workflow_stat_list)
sys.exit(0)

