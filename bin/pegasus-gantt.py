#!/usr/bin/env python
"""
Pegasus utility for generating workflow execution gantt chart

Usage: pegasus-gantt [options] submit directory

"""

##
#  Copyright 2010-2011 University Of Southern California
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

# Revision : $Revision$
import os
import re
import sys
import logging
import optparse
import math
import tempfile
import shutil

# Initialize logging object
logger = logging.getLogger()

import common
from netlogger.analysis.workflow.sql_alchemy import *
from datetime import timedelta
from datetime import datetime


#regular expressions
re_parse_property = re.compile(r'([^:= \t]+)\s*[:=]?\s*(.*)')

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
brainbase ='braindump.txt'
job_name_wf_uuid_map ={}
wf_uuid_workflow_map ={}
predefined_colors = ["#8dd3c7" , "#4daf4a","#bebada" , "#80b1d3" , "#b3de69" , "#fccde5" , "#d9d9d9" , "#bc80bd" , "#ccebc5" ,  "#fb8072"]
transformation_color_map ={}
base_submit_dir = None;
braindb_submit_dir =None;
output_dir=''



#TODO's 
# print warning if kickstart time doesn't fit in the condor window
# Handle  when it is running
# handle the case when it is last retry mode 
# need to handle cluster job case
#---workflow statistics

class WorkflowStatistics:
	
	def __init__(self):
		self.submit_dir ='-'
		self.wf_uuid = "-"
		self.dax_label ="-"
		self.parent_wf_uuid = "-"
		self.dagman_start_time ='-'
		self.workflow_run_time = '-'
		self.total_jobs='-'
		self.job_statistics_list =[]
		self.job_statistics_dict ={}
		self.transformation_statistics_dict ={}
		
	def get_formatted_job_data(self ):
		# find the pretty print length
		job_info = ''
		for job_stat in self.job_statistics_list:
			job_stat_det = job_stat.getJobDetails(self.dagman_start_time)
			job_info +=("{")
			job_info +=( "\n")
			job_info += ( "\"name\":"  + "\"" + job_stat_det['name']+ "\" , ")
			job_info += ( "\"preS\":" +  job_stat_det['preS'] +" , ")
			job_info += ( "\"preD\":" +  job_stat_det['preD'] +" , ")
			job_info += ( "\"cS\":"  +  job_stat_det['cS'] +" , ")
			job_info += ( "\"cD\":"    +  job_stat_det['cD']  +" , ")
			job_info += ( "\"gS\":"    +  job_stat_det['gS']  +" , ")
			job_info += ( "\"gD\":"    +  job_stat_det['gD']  +" , ")
			job_info += ( "\"eS\":"    +  job_stat_det['eS']  +" , ")
			job_info += ( "\"eD\":"    +  job_stat_det['eD']  +" , ")
			job_info += ( "\"kS\":"    +  job_stat_det['kS']  +" , ")
			job_info += ( "\"kD\":"    +  job_stat_det['kD']  +" , ")
			job_info += ( "\"postS\":" +  job_stat_det['postS'] +" , ")
			job_info += ( "\"postD\":" +  job_stat_det['postD'] +" , ")
			job_info += ( "\"state\":" +  job_stat_det['state'] +" , ")
			job_info += ( "\"transformation\": \"" +  job_stat_det['transformation'] +"\"  , ")
			if transformation_color_map.has_key(job_stat_det['transformation']):
				job_info += ( "\"color\": \"" +  transformation_color_map[job_stat_det['transformation']] +"\"  , ")
			else:
				# there is no compute task
				job_info += ( "\"color\": 'white' , ")
			if isSubWfJob(job_stat_det['name']):
				job_info += ( "\"sub_wf\":1 , "  )
				corresponding_dax =''
				if (job_name_wf_uuid_map.has_key(job_stat_det['name'])): 
					corresponding_dax = job_name_wf_uuid_map[job_stat_det['name']]
					job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ ".html\"")
				else:
					job_info += ( "\"sub_wf_name\":''")	
					
			else:
				job_info += ( "\"sub_wf\":0 , " )	
				job_info += ( "\"sub_wf_name\":''")
			job_info +=( "},\n")
		return job_info	
						
	
#---job statistics ---

class JobStatistics:
	def __init__(self):
		self.name = None
		self.site = None
		self.kickstartStart = None
		self.kickstartDuration = None
		self.postStart = None
		self.postDuration = None
		self.preStart = None
		self.preDuration = None
		self.condorStart = None
		self.condorDuration = None
		self.gridStart = None
		self.gridDuration = None
		self.executeStart = None
		self.executeDuration = None
		self.transformation = None
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
	
	def getJobDetails(self , global_start_time):
		job_details ={}
		job_details['name'] = self.name
		job_details['site'] = self.site
			
		if self.preStart is not None and  self.preDuration is not None:
			job_details['preS'] = str(convert_to_seconds(self.preStart - global_start_time))
			job_details['preD'] = str(self.preDuration)	  
		else:
			job_details['preS'] = "''"
			job_details['preD'] ="''"
			
		if self.condorStart is not None and  self.condorDuration is not None:
			job_details['cS'] = str(convert_to_seconds(self.condorStart - global_start_time))
			job_details['cD'] = str(self.condorDuration)	  
		else:
			job_details['cS'] = "''"
			job_details['cD'] ="''"
			
		if self.gridStart is not None and  self.gridDuration is not None:
			job_details['gS'] = str(convert_to_seconds(self.gridStart - global_start_time))
			job_details['gD'] = str(self.gridDuration)	  
		else:
			job_details['gS'] = "''"
			job_details['gD'] ="''"
		if self.executeStart is not None and  self.executeDuration is not None:
			job_details['eS'] = str(convert_to_seconds(self.executeStart - global_start_time))
			job_details['eD'] = str(self.executeDuration)	  
		else:
			job_details['eS'] = "''"
			job_details['eD'] ="''"
			
		if self.kickstartStart is not None and  self.kickstartDuration is not None:
			job_details['kS'] = str(convert_to_seconds(self.kickstartStart - global_start_time))
			job_details['kD'] = str(self.kickstartDuration)	  
		else:
			job_details['kS'] = "''"
			job_details['kD'] ="''"
			
		if self.postStart is not None and  self.postDuration is not None:
			job_details['postS'] = str(convert_to_seconds(self.postStart - global_start_time))
			job_details['postD'] = str(self.postDuration)	  
		else:
			job_details['postS'] = "''"
			job_details['postD'] ="''"
		if self.transformation is not None:
			job_details['transformation'] = self.transformation
		else:
			job_details['transformation'] = ""
		if self.is_failure:
			job_details['state'] = "0"
		elif self.is_success:
			job_details['state'] = "1"
		elif self.state is None:
			job_details['state'] = "2"
		else:
			job_details['state'] = "3"
		return job_details
	
	
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
	
	
	# assigning job name
	job_stat.name = job.name
	# assigning site name
	job_stat.site = job.site_name
	
	jobstates = job.all_jobstates
	for jobstate in jobstates:
		state_timestamp_dict[jobstate.state] = jobstate.timestamp
	# default values	

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
	file_path = file_path.replace(braindb_submit_dir,base_submit_dir)
	return file_path
	

def isSubWfJob(job_name):
	if job_name.lstrip().startswith('subdax_'):
		return True;
	return False;		
							
#------------Gets sub worklows job names----------------
def update_job_name_sub_workflow_map(workflow , sub_dir):
	# Parse through the job 
	for job in workflow.jobs:
		if(isSubWfJob(job.name)):
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
				config = slurp_braindb(rlb(sub_init_dir))
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
	return


#-------return workflow uuid by parsing db alone-----
def get_sub_workflows_uuid(root_workflow , output_db_url):
	sub_wf_uuid_list =[]
	uuid_list =[]
	# Adds the root workflow to workflow list
	wf_list =[root_workflow]
	while len(wf_list) > 0:
		#pops the dag file path
		wf =  wf_list[0]
		wf_uuid_workflow_map[wf.wf_uuid] = wf
		wf_list.remove(wf)
		if wf.submit_dir is None:
			logger.warning("Invalid submit directory for workflow  " + wf.wf_uuid)
		else:
			update_job_name_sub_workflow_map(wf , rlb(wf.submit_dir))
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
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)


# ---------populate_workflow_details--------------------------
def populate_workflow_details(workflow):
	"""
	populates the workflow statistics information
	Param: the workflow reference
	"""
	workflow_stat = WorkflowStatistics()
	workflow_run_time = None
	total_jobs =0
	transformation_stats_dict ={}
	job_stats_dict ={}
	job_stats_list =[]
	 
	if not workflow.is_running :
		workflow_run_time = convert_to_seconds(workflow.total_time)
	else:
		workflow_run_time = convert_to_seconds(workflow.total_time)
	color_count = 0
	# populating statistics details
	dagman_start_time = workflow.start_events[0].timestamp
	# for root jobs,dagman_start_time is required, assumption start_event[0] is not none
	for job in workflow.jobs:
		job_stat = JobStatistics()
		job_stats_dict[job.name] = job_stat
		job_stats_list.append(job_stat)
		populate_job_details(job ,job_stat , dagman_start_time)
		# Assigning the tranformation name
		
		if job_stat.transformation is not None:
			transformation_stats_dict[job_stat.transformation] = job_stat.transformation
		if not transformation_color_map.has_key(job_stat.transformation):
			transformation_color_map[job_stat.transformation]= predefined_colors[color_count%len(predefined_colors)]
			color_count +=1
	
		
	#Calculating total jobs
	total_jobs = len(job_stats_list)
	# Assigning value to the workflow object
	workflow_stat.wf_uuid = workflow.wf_uuid
	workflow_stat.parent_wf_uuid = workflow.parent_wf_uuid
	if workflow_run_time is not None:
		workflow_stat.workflow_run_time =workflow_run_time
	workflow_stat.total_jobs = total_jobs	
	workflow_stat.dagman_start_time = dagman_start_time
	workflow_stat.submit_dir = workflow.submit_dir
	workflow_stat.dax_label = workflow.dax_label		
	workflow_stat.job_statistics_dict = job_stats_dict
	workflow_stat.job_statistics_list =job_stats_list
	workflow_stat.transformation_statistics_dict = transformation_stats_dict
	return workflow_stat

#----------print workflow details--------
"""
	Prints the workflow statistics information
	Param: the workflow reference
	"""
def print_workflow_details(workflow_stat_list):
	
	# print javascript file
	for workflow_stat in workflow_stat_list:
		data_file = os.path.join(output_dir,  workflow_stat.wf_uuid+"_data.js")
		try:
			fh = open(data_file, "w")
			fh.write( "\n")
			fh.write( "var data = [")
			job_info = workflow_stat.get_formatted_job_data()
			fh.write(job_info)
			fh.write( "];")
		except IOError:
			logger.error("Unable to write to file " + data_file)
			sys.exit(1)
		else:
			fh.close()	
	return
	
def print_braindump_file(braindb_path):
	
	config = slurp_braindb(braindb_path)
	brain_db_content =''
	if not config:
		logger.warning("could not process braindump.txt " + braindb_path)
		brain_db_content ="<div style='color:red;'>Unable to read braindump file </div>"
	else:
		brain_db_content ="<div style='width:1200;margin : 0 auto;'><table border = 1 style='color:#600000;'>"
		for key, value in config.items():
			brain_db_content += "<tr><th style ='color:#600000'>"+ key +"</th><td style ='color:#888888'>" +value +"</td></tr>"
		brain_db_content +="</table></div>"
	return brain_db_content
		

def create_action_script():
# print action script
	data_file = os.path.join(output_dir,  "action.js")
	try:
		fh = open(data_file, "w")
		action_content = "\n\
function barvisibility(d , index){\n\
if(!d){\n\
 return false;\n\
}\n\
var yPos = index * bar_spacing;\n\
if(yPos < curY || yPos > curEndY ){\n\
return false;\n\
}else{\n\
return true;\n\
}\n\
}\n\n\
function openWF(url){\n\
if(isNewWindow){\n\
window.open(url);\n\
}else{\n\
self.location = url;\n\
}\n\
}\n\n\
function printJobDetails(d){\n\
var job_details = \"Job name :\"+d.name;\n\
if(d.preD){\n\
job_details +=\"\\nPre script duration :\"+d.preD +\" sec.\";\n\
}\n\
if(d.gD){\n\
job_details +=\"\\nResource delay :\"+d.gD +\" sec.\";\n\
}\n\
if(d.eD){\n\
job_details +=\"\\nRuntime as seen by dagman :\"+d.eD +\" sec.\";\n\
}\n\
if(d.kD){\n\
job_details +=\"\\nKickstart duration :\"+d.kD +\" sec.\";\n\
}\n\
if(d.postD){\n\
job_details +=\"\\nPost script duration :\"+d.postD +\" sec.\";\n\
}\n\
job_details +=\"\\nMain task :\"+d.transformation ;\n\
alert(job_details);\n\
}\n\n\
function getJobBorder(d){\n\
if(!d.state){\n\
return 'red';\n\
}\n\
else if(d.sub_wf){\n\
return 'orange';\n\
}else{\n\
return null;\n\
}\n\
}\n\
function getPreTime(d) {\n\
var preWidth = 0; \n\
if(d.preD){\n\
preWidth = xScale(d.preS + d.preD) -xScale(d.preS);\n\
}\n\
if(preWidth > 0 && preWidth < 1 ){\n\
	preWidth = 1;\n\
}\n\
return preWidth;\n\
}\n\n\
function getCondorTime(d) {\n\
var cDWidth = 0;\n\
if(d.cD){\n\
cDWidth = xScale(d.cS + d.cD) - xScale(d.cS)\n\
}\n\
if(cDWidth > 0 && cDWidth < 1 ){\n\
cDWidth = 1;\n\
}\n\
return cDWidth;\n\
}\n\n\
function getResourceDelay(d) {\n\
var gWidth = 0;\n\
if(d.gS){\n\
gWidth = xScale(d.gS + d.gD) - xScale(d.gS + d.gD);\n\
}\n\
if(gWidth > 0 && gWidth < 1 ){\n\
	gWidth = 1;\n\
}\n\
return gWidth;\n\
}\n\n\
function getRunTime(d) {\n\
var rtWidth = 0;\n\
if(d.eD){\n\
rtWidth = xScale(d.eS + d.eD) -xScale(d.eS);\n\
}\n\
if(rtWidth > 0 && rtWidth < 1 ){\n\
	rtWidth = 1;\n\
}\n\
return rtWidth;\n\
}\n\n\
function getKickStartTime(d) {\n\
var kickWidth = 0;\n\
if(d.kD){\n\
kickWidth = xScale(d.kS + d.kD) -xScale(d.kS);\n\
}\n\
if(kickWidth > 0 && kickWidth < 1 ){\n\
	kickWidth = 1;\n\
}\n\
return kickWidth;\n\
}\n\n\
function getPostTime(d) {\n\
var postWidth = 0;\n\
if(d.postD){\n\
postWidth = xScale(d.postS + d.postD) -xScale(d.postS);\n\
}\n\
if(postWidth > 0 && postWidth < 1 ){\n\
	postWidth = 1;\n\
}\n\
return postWidth;\n\
}\n\n\
function setShowLabel(){\n\
if(showName){\n\
	return 'Hide Job name';\n\
}else{\n\
	return 'Show Job name';\n\
}\n\
}\n\n\
function setShowName(){\n\
if(showName){\n\
	showName = false;\n\
}else{\n\
	showName = true;\n\
}\n\
rootPanel.render();\n\
return;\n\
}\n\n\
function fadeRight(){\n\
if(curX == 0){\n\
	return \"images/right-fade.png\"\n\
}\n\
return \"images/right.png\"\n\
}\n\n\
function fadeDown(){\n\
if(curY == 0){\n\
	return \"images/down-fade.png\"\n\
}\n\
return \"images/down.png\"\n\
}\n\
\n\
function panLeft(){\n\
var panBy = (curEndX -curX)/panXFactor;\n\
curX +=panBy;\n\
curEndX +=panBy;\n\
xScale.domain(curX ,curEndX );\n\
rootPanel.render();\n\
headerPanel.render();\n\
}\n\
\n\
function panRight(){\n\
var panBy = (curEndX -curX)/panXFactor;\n\
if(curX > 0){\n\
curX -=panBy;\n\
curEndX -=panBy;\n\
if(curX <= 0){\n\
curEndX += (curX + panBy)\n\
curX = 0;\n\
}\n\
xScale.domain(curX ,curEndX );\n\
rootPanel.render();\n\
headerPanel.render();\n\
}\n\
}\n\
\n\
function panUp(){\n\
var panBy = (curEndY -curY)/panYFactor;\n\
curY +=panBy;\n\
curEndY += panBy;\n\
yScale.domain(curY ,curEndY);\n\
rootPanel.render();\n\
headerPanel.render();\n\
}\n\
\n\
function panDown(){\n\
var panBy = (curEndY -curY)/panYFactor;\n\
if(curY > 0){\n\
curY -= panBy;\n\
curEndY -= panBy;\n\
if(curY< 0){\n\
curEndY += (curY + panBy);\n\
curY = 0;\n\
}\n\
yScale.domain(curY ,curEndY );\n\
rootPanel.render();\n\
headerPanel.render();\n\
}\n\
}\n\
\n\
function zoomOut(){\n\
var newX = 0;\n\
var newY = 0;\n\
\n\
newX = curEndX  + curEndX*0.1;\n\
newY = curEndY  + curEndY*0.1;\n\
\n\
if(curX < newX && isFinite(newX)){\n\
curEndX = newX;\n\
xScale.domain(curX, curEndX);\n\
}\n\
if(curY < newY && isFinite(newY)){\n\
curEndY = newY;\n\
yScale.domain(curY, curEndY);\n\
}\n\
rootPanel.render();\n\
}\n\
\n\
function zoomIn(){\n\
var newX = 0;\n\
var newY = 0;\n\
newX = curEndX  - curEndX*0.1;\n\
newY = curEndY  - curEndY*0.1;\n\
if(curX < newX && isFinite(newX)){\n\
curEndX = newX;\n\
xScale.domain(curX, curEndX);\n\
}\n\
if(curY < newY && isFinite(newY)){\n\
curEndY = newY;\n\
yScale.domain(curY, curEndY);\n\
}\n\
rootPanel.render();\n\
}\n\
\n\
function resetZooming(){\n\
curX  = 0;\n\
curY = 0;\n\
curEndX  = initMaxX;\n\
curEndY =  initMaxY;\n\
xScale.domain(curX, curEndX);\n\
yScale.domain(curY, curEndY);\n\
rootPanel.render();\n\
}\n"
		fh.write( action_content)
		fh.write( "\n")
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()



def create_header(workflow_stat):
	header_str = "<html>\n<head>\n<title>"+ workflow_stat.wf_uuid +"</title>\n<style type ='text/css'>\n\
#gantt_chart{\n\
border:1px solid red;\n\
}\n\
</style></head>\n<body>\n"
	return header_str
	
def create_include(workflow_stat):
	include_str = "<script type='text/javascript' src='js/protovis-r3.2.js'></script>\n\
<script type='text/javascript' src='action.js'></script>\n\
<script type='text/javascript' src='" + workflow_stat.wf_uuid  +"_data.js'></script>\n"
	return include_str
	
def create_variable(workflow_stat):
	# Adding  variables
	var_str = "<script type='text/javascript'>\nvar initMaxX = " + str(workflow_stat.workflow_run_time) + ";\n"
	var_str += "var initMaxY = "+str(workflow_stat.total_jobs*20) + ";\n"
	color_name_str = "var color =['darkblue','yellow','orange' ,'#00FFFF', 'purple'"
	desc_name_str = "var desc=['pre script','condor job','resource delay', 'job runtime as seen by dagman','post script '"
	for k,v in transformation_color_map.items():
		if workflow_stat.transformation_statistics_dict.has_key(k):
			color_name_str += ",'"+v +"'"
			desc_name_str +=",'"+k +"'"	
	color_name_str += "];\n"
	desc_name_str +="]\n;"
	var_str += color_name_str
	var_str += desc_name_str
	var_str +="var bar_width = 20;\n\
var bar_spacing = 20;\n\
var w = 1450;\n\
var h = 840;\n\
var toolbar_width = 550;\n\
var containerPanelPadding = 10;\n\
var chartPanelWidth = w+ containerPanelPadding*2;\n\
var chartPanelHeight  = h + containerPanelPadding*2;\n\
var curX  = 0;\n\
var curY = 0;\n\
var curEndX  = initMaxX;\n\
var curEndY =  initMaxY;\n\
var nameMargin  = 400;\n\
var scaleMargin = 15;\n\
var xScale = pv.Scale.linear(curX, curEndX).range(0, w-nameMargin);\n\
var yScale = pv.Scale.linear(curY, curEndY).range(0, h -scaleMargin);\n\
var xLabelPos = containerPanelPadding + nameMargin;\n\
var yLabelPos = 40;\n\
var panXFactor = 10;\n\
var panYFactor  = 10;\n\
var isNewWindow = false;\n\
var showName = false;\n\
var headerPanelWidth = w+ containerPanelPadding*2;\n\
var headerPanelHeight  = 100;\n\
var footerPanelWidth = w+ containerPanelPadding*2;\n\
var footerPanelHeight  = "+ str(50 + len(workflow_stat.transformation_statistics_dict)/4*10) + ";\n\
</script>\n"
	return var_str
	

def create_toolbar_panel(workflow_stat):
	panel_str = "<script type=\"text/javascript+protovis\">\n\
var headerPanel = new pv.Panel()\n\
.width(headerPanelWidth)\n\
.height(headerPanelHeight)\n\
.fillStyle('white');\n\n\
var panPanel  = headerPanel.add(pv.Panel)\n\
.left(w + containerPanelPadding -toolbar_width)\n\
.width(toolbar_width)\n\
.height(headerPanelHeight);\n\n\
panPanel.add(pv.Image)\n\
.left(10)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.title('Pan left')\n\
.url('images/left.png').event('click', panLeft);\n\n\
panPanel.add(pv.Image)\n\
.left(50)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(fadeRight)\n\
.title('Pan right')\n\
.event('click', panRight);\n\n\
panPanel.add(pv.Image)\n\
.left(90)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/up.png')\n\
.title('Pan up')\n\
.event('click', panUp);\n\
 panPanel.add(pv.Image)\n\
.left(140)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(fadeDown)\n\
.title('Pan down')\n\
.event('click', panDown);\n\n\
panPanel.add(pv.Image)\n\
.left(190)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-in.png')\n\
.title('Zoom in')\n\
.event('click', zoomIn);\n\n\
panPanel.add(pv.Image)\n\
.left(240)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-out.png')\n\
.title('Zoom out')\n\
.event('click', zoomOut);\n\n\
panPanel.add(pv.Image)\n\
.left(290)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-reset.png')\n\
.title('Zoom reset')\n\
.event('click', resetZooming);\n\n\
panPanel.add(pv.Image)\n\
.left(340)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(function() {if(isNewWindow){ return 'images/new-window-press.png';}else{ return 'images/new-window.png';}})\n\
.title('Open sub workflow in new window')\n\
.event('click', function(){ if(isNewWindow){ isNewWindow = false;headerPanel.render();}else{ isNewWindow = true;headerPanel.render();}});\n\n\
panPanel.def('active', false);\n\n\
panPanel.add(pv.Bar)\n\
.events('all')\n\
.left(390)\n\
.top(40)\n\
.width(100)\n\
.height(24)\n\
.event('click', setShowName)\n\
.fillStyle(function() this.parent.active() ? 'orange' : '#c5b0d5')\n\
.strokeStyle('black')\n\
.event('mouseover', function() this.parent.active(true))\n\
.event('mouseout', function() this.parent.active(false))\n\
.anchor('left').add(pv.Label)\n\
	.textAlign('left')\n\
	.textMargin(5)\n\
	.textStyle(function() this.parent.active() ? 'white' : 'black')\n\
	.textBaseline('middle')\n\
	.text(setShowLabel);\n\n"
	if workflow_stat.parent_wf_uuid is not None:
		panel_str += "panPanel.add(pv.Image)\n.left(500)\n.top(34)\n.width(32)\n.height(32)\n.url('images/return.png')\n.title('Return to parent workflow')\n.event('click', function(){\nself.location = '" +  workflow_stat.parent_wf_uuid+".html' ;\n});"
	panel_str += "headerPanel.add(pv.Label)\n\
.top(40)\n\
.left( containerPanelPadding + nameMargin)\n\
.font(function() {return 24 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('bottom')\n\
.text('Workflow execution Gantt chart');\n\n\
headerPanel.add(pv.Label)\n\
.top(80)\n\
.left(containerPanelPadding + nameMargin)\n\
.font(function() {return 16 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('bottom')\n\
.text('"+workflow_stat.dax_label +"');\n\
headerPanel.render();\n\n\
</script>\n"
	return panel_str

def create_chart_panel(workflow_stat):
	panel_str ="<script type=\"text/javascript+protovis\">\n\
var rootPanel = new pv.Panel()\n\
.width(chartPanelWidth)\n\
.height(chartPanelHeight)\n\
.fillStyle('white');\n\n\
var vis = rootPanel.add(pv.Panel)\n\
.bottom(containerPanelPadding)\n\
.top(containerPanelPadding)\n\
.left(containerPanelPadding)\n\
.width(w)\n\
.height(h)\n\
.fillStyle('white');\n\n\
var rulePanelH = vis.add(pv.Panel)\n\
.overflow('hidden')\n\
.bottom(scaleMargin);\n\n\
rulePanelH.add(pv.Rule)\n\
.left(nameMargin)\n\
.data(data)\n\
.strokeStyle('#F8F8F8')\n\
.width(w)\n\
.bottom( function(){\n\
return yScale(this.index * bar_spacing);\n\
})\n\
.anchor('left').add(pv.Label)\n\
.textBaseline('bottom')\n\
.text(function(d) {\n\
	if(showName){\n\
	return (this.index +1) +' ' + d.name;\n\
	}else{\n\
		return (this.index +1) ;\n\
	}\n\
	});\n\n\
var barPanel = vis.add(pv.Panel)\n\
.events('all')\n\
.left(nameMargin)\n\
.bottom(scaleMargin)\n\
.strokeStyle('black')\n\
.overflow('hidden');\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.preS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){\n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getPreTime(d);})\n\
.left(function(d){\n\
if(!d.preS){\n\
return 0;\n\
}\n\
return xScale(d.preS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
	printJobDetails(d);}\n\
})\n\
.fillStyle(function(d) {return color[0];})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.cS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){ \n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getCondorTime(d);})\n\
.left(function(d){\n\
if(!d.cS){\n\
return 0;\n\
}\n\
return xScale(d.cS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return color[1];})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.gS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){\n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getResourceDelay(d);})\n\
.left(function(d){\n\
if(!d.gS){\n\
return 0;\n\
}\n\
return xScale(d.gS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
	openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
	printJobDetails(d);}\n\
	})\n\
.fillStyle(function(d) {return color[2];})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.eS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){\n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getRunTime(d);})\n\
.left(function(d){\n\
if(!d.eS){\n\
return 0;\n\
}\n\
return xScale(d.eS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return color[3];})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.kS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){\n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getKickStartTime(d);})\n\
.left(function(d){\n\
if(!d.kS){\n\
return 0;\n\
}\n\
return xScale(d.kS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return d.color;})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
barPanel.add(pv.Bar)\n\
.data(data)\n\
.visible(function(d){return barvisibility(d.postS , this.index);})\n\
.height(function() {\n\
return bar_width;})\n\
.bottom(function(){\n\
return yScale(this.index * bar_spacing);})\n\
.width(function(d) {\n\
return getPostTime(d);})\n\
.left(function(d){\n\
if(!d.postS){\n\
return 0;\n\
}\n\
return xScale(d.postS);} )\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return color[4];})\n\
.strokeStyle(function(d) {return getJobBorder(d);});\n\n\
var rulePanelV = vis.add(pv.Panel)\n\
.overflow('hidden')\n\
.left(nameMargin)\n\
.bottom(0);\n\n\
rulePanelV.add(pv.Rule)\n\
.bottom(scaleMargin)\n\
.data(function() xScale.ticks())\n\
.strokeStyle('#F8F8F8')\n\
.left(xScale)\n\
.height(h )\n\
.anchor('bottom').add(pv.Label)\n\
.textAlign('left')\n\
.text(xScale.tickFormat);\n\n\
rootPanel.add(pv.Label)\n\
.left(10)\n\
.bottom(containerPanelPadding)\n\
.font(function() {return 20 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('middle')\n\
.text('Job count -->')\n\
.textAngle(-Math.PI / 2);\n\n\
rootPanel.render();\n\n\
</script>\n"
	return panel_str
	

def create_legend_panel(workflow_stat):
	panel_str ="<script type=\"text/javascript+protovis\">\n\
var footerPanel = new pv.Panel()\n\
.width(footerPanelWidth)\n\
.height(footerPanelHeight)\n\
.fillStyle('white');\n\
footerPanel.add(pv.Label)\n\
.top(0)\n\
.left(containerPanelPadding + nameMargin)\n\
.font(function() {return 20 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('top')\n\
.text('Timeline in seconds -->');\n\n\
footerPanel.add(pv.Dot)\n\
.data(desc)\n\
.left( function(d){\n\
if(this.index == 0){\n\
xLabelPos = containerPanelPadding + nameMargin;\n\
yLabelPos = 30;\n\
}else{\n\
if(xLabelPos + 180 > w){\n\
	xLabelPos =  containerPanelPadding + nameMargin;\n\
	yLabelPos -=10;\n\
}\n\
else{\n\
xLabelPos += 180;\n\
}\n\
}\n\
return xLabelPos;}\n\
)\n\
.bottom(function(d){\n\
return yLabelPos;})\n\
.fillStyle(function(d) color[this.index])\n\
.strokeStyle(null)\n\
.size(30)\n\
.anchor('right').add(pv.Label)\n\
.textMargin(6)\n\
.textAlign('left')\n\
.text(function(d) d);\n\n\
footerPanel.render();\n\n\
</script>\n"
	return panel_str

def create_gnatt_plot(workflow_stat_list):
	create_action_script()
	# print html file
	for workflow_stat in workflow_stat_list:
		data_file = os.path.join(output_dir,  workflow_stat.wf_uuid+".html")
		try:
			fh = open(data_file, "w")
			fh.write( "\n")
			str_list = []
			wf_header = create_header(workflow_stat)
			str_list.append(wf_header)
			wf_content = create_include(workflow_stat)
			str_list.append(wf_content)
			# Adding  variables
			wf_content =create_variable(workflow_stat)
			str_list.append(wf_content)
			# adding the tool bar panel
			wf_content = "<div id ='gantt_chart' style='width: 1500px; margin : 0 auto;' >\n"
			str_list.append(wf_content)
			wf_content =create_toolbar_panel(workflow_stat)
			str_list.append(wf_content)
			# Adding the chart panel
			wf_content =create_chart_panel(workflow_stat)
			str_list.append(wf_content)
			# Adding the legend panel
			wf_content =create_legend_panel(workflow_stat)
			str_list.append(wf_content)
			wf_content ="</div>\n<br/>" 
			str_list.append(wf_content)
			# printing the brain dump content
			if workflow_stat.submit_dir is None:
				logger.warning("Unable to display brain dump contents. Invalid submit directory for workflow  " + workflow_stat.wf_uuid)
			else:
				wf_content = print_braindump_file(os.path.join(rlb(workflow_stat.submit_dir)))
				str_list.append(wf_content)
			fh.write("".join(str_list))
			str_list.append(wf_content)
			wf_footer = "\n<div style='clear: left'>\n</div></body>\n</html>"
			fh.write(wf_footer)
		except IOError:
			logger.error("Unable to write to file " + data_file)
			sys.exit(1)
		else:
			fh.close()	
	return
def setup(output_dir):
	src_js_path = os.path.join(common.pegasus_home, "lib/javascript")
	src_img_path = os.path.join(common.pegasus_home, "share/protovis/images/")
	dest_js_path = os.path.join(output_dir, "js")
	dest_img_path = os.path.join(output_dir, "images/")
	if os.path.isdir(dest_js_path):
		logger.warning("Javascript directory exists. Deleting... ")
		try:
			shutil.rmtree(dest_js_path)
		except:
			logger.error("Unable to remove javascript directory."+dest_js_path)
			sys.exit(1)
	if os.path.isdir(dest_img_path):
		logger.warning("Image directory exists. Deleting... ")
		try:
			shutil.rmtree(dest_img_path)
		except:
			logger.error("Unable to remove image directory."+dest_img_path)
			sys.exit(1) 	 	
	shutil.copytree (src_js_path, dest_js_path)
	shutil.copytree (src_img_path, dest_img_path)


# ---------main----------------------------------------------------------------------------

# Configure command line option parser
prog_usage = prog_base +" [options] SUBMIT DIRECTORY" 
parser = optparse.OptionParser(usage=prog_usage)
parser.add_option("-o", "--output", action = "store", dest = "output_dir",
		help = "writes the output to given directory.")
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
sub_workflow_list = get_sub_workflows_uuid(w, output_db_url)
for sub_wf_uuid in sub_workflow_list:
	w = Workflow(output_db_url)
	w.initialize(sub_wf_uuid)
	workflow_stat = populate_workflow_details(w)
	workflow_stat_list.append(workflow_stat)

setup(output_dir)
print_workflow_details(workflow_stat_list)
create_gnatt_plot(workflow_stat_list)
sys.exit(0)

