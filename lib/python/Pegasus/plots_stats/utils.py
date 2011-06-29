#!/usr/bin/env python

import os
import re
import sys
import logging
import optparse
import math
import tempfile
import commands
import shutil
from Pegasus.tools import properties

# Initialize logging object
logger = logging.getLogger()

import common
from Pegasus.tools import utils


def isSubWfJob(job_name):
	if job_name.lstrip().startswith('subdax_') or job_name.lstrip().startswith('pegasus-plan_') or job_name.lstrip().startswith('subdag_'):
		return True;
	return False;
	
def rlb(file_path ,parent_wf_braindb_submit_dir , parent_submit_dir):
	"""
	This function converts the path relative to base path
	param file_path : file path for which the relative path needs to be calculated.
	param parent_wf_braindb_submit_dir : submit directory obtained from the braindump file
	param parent_submit_dir : submit directory given by the user
	Returns : path relative to the base 
	"""
	file_path = file_path.replace(parent_wf_braindb_submit_dir,parent_submit_dir)
	return file_path
	
def print_braindump_file(braindb_path):
	config = utils.slurp_braindb(braindb_path)
	brain_db_content =''
	if not config:
		logger.warning("could not process braindump.txt " + braindb_path)
		brain_db_content ="<div style='color:red;'>Unable to read braindump file </div>"
	else:
		brain_db_content = print_property_table(config , False ," : ")
	return brain_db_content
	
def parse_workflow_environment(wf_det):
	config = {}
	config["wf_uuid"] = wf_det.wf_uuid
	config["dag_file_name"] = wf_det.dag_file_name
	config["submit_hostname"] = wf_det.submit_hostname
	config["submit_dir"] = wf_det.submit_dir
	config["planner_arguments"] = wf_det.planner_arguments
	config["user"] = wf_det.user
	config["grid_dn"] = wf_det.grid_dn
	config["planner_version"] = wf_det.planner_version
	config["dax_label"] = wf_det.dax_label
	config["dax_version"] = wf_det.dax_version
	return config


def print_property_table(props , border= True , separator =""):
	html_content =''
	if border:
		html_content ="<table border = 1 style='color:#600000;'>"
	else:
		html_content ="<table style='color:#600000;'>"
	for key, value in props.items():
		if value is None:
			value ='-'
		html_content += "<tr><th align ='left' style ='color:#600000'>"+ key +"</th><td style ='color:#888888'>" + separator +str(value) +"</td></tr>"
	html_content +="</table>"
	return html_content

def print_grid_table(title , content_list):
	# find the pretty print length
	html_content = ''
	html_content +=( "\n<table border = 1 style='color:#600000;'>\n<tr>")
	# print title
	for index in range(len(title)):
		html_content +=  "\n<th style='color:#600000;'>" + title[index] +"</th>"
	html_content +="\n</tr>"
	for content in content_list:
		html_content += ( "\n")
		html_content +="<tr style ='color:#888888'>"
		for index in range(len(content)):
			html_content +="<td>"
			if isinstance(content[index],basestring):
				html_content += content[index]
			else:
				html_content += str(content[index])
			html_content +="</td>"
		html_content +="</tr>"
	html_content +=( "</table>\n")
	return html_content


def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)

def create_directory(dir_name , delete_if_exists = False ):
	if delete_if_exists:
		if os.path.isdir(dir_name):
			logger.warning("Deleting existing directory. Deleting... " + dir_name)
			try:
				shutil.rmtree(dir_name)
			except:
				logger.error("Unable to remove existing directory." + dir_name)
				sys.exit(1)
	if not os.path.isdir(dir_name):
		logger.info("Creating directory... " + dir_name)
		try:
			os.mkdir(dir_name)
		except:
			logger.error("Unable to create directory."+dir_name)
			sys.exit(1)

def copy_files(src, dest):
	for file in os.listdir(src):
		if os.path.isfile(os.path.join(src,file)):
			try:
				if not os.path.exists(os.path.join(dest,file)):
					shutil.copy(os.path.join(src,file), dest)
			except:
				logger.error("Unable to copy file "+ file +  " to "+ dest)
				sys.exit(1)
	
def parse_property_file(file_name, separator=" "):
	"""
	Reads a property file
	Param: file name
	Returns: Dictionary with the configuration, empty if error
	"""
	my_config = {}

	try:
		my_file = open(file_name, 'r')
	except:
	# Error opening file
		return my_config

	for line in my_file:
	# Remove \r and/or \n from the end of the line
		line = line.rstrip("\r\n")
		# Split the line into a key and a value
		k, v = line.split(separator, 1)
		v = v.strip()
		my_config[k] = v
	my_file.close()
	logger.debug("# parsed " + file_name)
	return my_config
"""
def format_seconds(sec):
	formatted_duration = ''
	days = sec / 86400
	sec -= 86400*days
	hrs = sec / 3600
	sec -= 3600*hrs
	mins = sec / 60
	sec -= 60*mins
	if days >= 1:
		if days == 1:
			formatted_duration  += str(days) + ' day, '
		else:
			formatted_duration  += str(days) + ' days, '
	if hrs >=1:
		if hrs == 1:
			formatted_duration  += str(hrs) + ' hr. ' 
		else:
			formatted_duration  += str(hrs) + ' hrs. '
	if mins >=1:
		if mins == 1:
			formatted_duration  += str(mins) + ' min. '
		else:
			formatted_duration  += str(mins) + ' mins. '
	if sec >=1:
		if sec ==1:
			formatted_duration  += str(sec) + " sec."
		else:
			formatted_duration  += str(sec) + " secs."
	
	return formatted_duration
"""

def format_seconds(duration):
	"""
	Utility for converting time to a readable format
	@param duration :  time in seconds and miliseconds
	@return time in format day,hour, min,sec
	"""
	if duration is None:
		return '-'
	milliseconds = math.modf(duration)[0]
	sec = int(duration)
	formatted_duration = ''
	days = sec / 86400
	sec -= 86400*days
	hrs = sec / 3600
	sec -= 3600*hrs
	mins = sec / 60
	sec -= 60*mins
	if days >= 1:
		if days == 1:
			formatted_duration  += str(days) + ' day, '
		else:
			formatted_duration  += str(days) + ' days, '
	if hrs >=1:
		if hrs == 1:
			formatted_duration  += str(hrs) + ' hr. ' 
		else:
			formatted_duration  += str(hrs) + ' hrs. '
	if mins >=1:
		if mins == 1:
			formatted_duration  += str(mins) + ' min. '
		else:
			formatted_duration  += str(mins) + ' mins. '
	if sec >=1:
		if sec ==1:
			formatted_duration  += str(sec) + " sec. "
		else:
			formatted_duration  += str(sec) + " secs. "
	milliseconds= round(milliseconds,3)*1000
	milliseconds= int(milliseconds)
	if milliseconds >=1:
		if milliseconds ==1:
			formatted_duration  += str(milliseconds) + " sec. "
		else:
			formatted_duration  += str(milliseconds)  + " millisecs."
	return formatted_duration
	

def get_workflow_wall_time(workflow_states_list):
	workflow_wall_time = None
	workflow_start_event_count  =0 
	workflow_end_event_count = 0
	is_end = False
	workflow_start_cum = 0
	workflow_end_cum =0
	for workflow_state in workflow_states_list:
		if workflow_state.state == 'WORKFLOW_STARTED':
			workflow_start_event_count +=1
			workflow_start_cum += workflow_state.timestamp
		else:
			workflow_end_event_count +=1
			workflow_end_cum += workflow_state.timestamp
	if workflow_start_event_count >0 and workflow_end_event_count > 0:
		if workflow_start_event_count == workflow_end_event_count:
			workflow_wall_time = workflow_end_cum - workflow_start_cum
	return workflow_wall_time
	
def get_db_url_wf_uuid(submit_dir , config_properties):
	#Getting values from braindump file
	top_level_wf_params = utils.slurp_braindb(submit_dir)
	top_level_prop_file = None
	if not top_level_wf_params:
		logger.error("Unable to process braindump.txt ")
		return None ,None
	wf_uuid = None
	if (top_level_wf_params.has_key('wf_uuid')):
		wf_uuid = top_level_wf_params['wf_uuid']
	else:
		logger.error("workflow id cannot be found in the braindump.txt ")
		return None ,None
	
	# Get the location of the properties file from braindump
	
	
	# Get properties tag from braindump
	if "properties" in top_level_wf_params:
	    top_level_prop_file = top_level_wf_params["properties"]
	    # Create the full path by using the submit_dir key from braindump
	    if "submit_dir" in top_level_wf_params:
	        top_level_prop_file = os.path.join(top_level_wf_params["submit_dir"], top_level_prop_file)
	
	# Parse, and process properties
	props = properties.Properties()
	props.new(config_file=config_properties, rundir_propfile=top_level_prop_file)
	
	output_db_url= None
	if props.property('pegasus.monitord.output') is not None:
		output_db_url = props.property('pegasus.monitord.output')
		if not (output_db_url.startswith("mysql:") or output_db_url.startswith("sqlite:")):
			logger.error("Unable to find database file from the properties file ")
			return None ,None
	else:
		dag_file_name =''
		if (top_level_wf_params.has_key('dag')):
			dag_file_name = top_level_wf_params['dag']
		else:
			logger.error("Dag file name cannot be found in the braindump.txt ")
			return None ,None
		# Create the sqllite db url
		output_db_file =submit_dir +"/"+ dag_file_name[:dag_file_name.find(".dag")] + ".stampede.db"
		output_db_url = "sqlite:///" + output_db_file
		if not os.path.isfile(output_db_file):
			logger.error("Unable to find database file in "+ submit_dir)
			return None , None
	return output_db_url , wf_uuid
	
