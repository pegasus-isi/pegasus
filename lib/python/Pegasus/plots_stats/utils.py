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
from datetime import datetime

from Pegasus.tools import properties

# Initialize logging object
logger = logging.getLogger()

import common
from Pegasus.tools import utils


def isSubWfJob(job_name):
	"""
		Returns whether the given job is a sub workflow or not
	"""
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
	
	
def parse_workflow_environment(wf_det):
	"""
	Parses the workflow detail to get the workflow environment details
	@param wf_det the workflow detail list.
	"""
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
	"""
	Utility method for printing a property table
	@props dictionary of the property key and value.
	@border boolean indication whether to draw a border or not.
	@separator string to separate key value pair.
	"""
	html_content =''
	if border:
		html_content ="\n<table border = 1 style='color:#600000;'>"
	else:
		html_content ="\n<table style='color:#600000;'>"
	for key, value in props.items():
		if value is None:
			value ='-'
		html_content += "\n<tr>\n<th align ='left' style ='color:#600000'>"+ key +"</th>\n<td style ='color:#888888'>" + separator +str(value) +"</td>\n</tr>"
	html_content +="\n</table>"
	return html_content
	

def print_sub_wf_links(wf_id_uuid_list ):
	"""
	Utility method for printing the link to sub workflow pages
	@param wf_id_uuid_list list of wf_id and wf_uuid
	"""
	html_content =''
	if len(wf_id_uuid_list) == 0:
		return  html_content
	
	html_content ="\n<div>"
	for wf_id_uuid in wf_id_uuid_list:
		html_content += "\n<a href ='"+ str(wf_id_uuid.wf_uuid) +".html'>"+ str(wf_id_uuid.wf_uuid) + "</a><br/>"
	html_content +="\n</div>"
	return html_content


def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)

def create_directory(dir_name , delete_if_exists = False ):
	"""
	Utility method for creating directory
	@param dir_name the directory path
	@param delete_if_exists boolean indication whether to delete the directory if it exists.
	"""
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
	"""
	Utility method for recursively copying from a directory to another
	@src source directory path
	@dst destination directory path
	"""
	for file in os.listdir(src):
		if os.path.isfile(os.path.join(src,file)):
			try:
				if not os.path.exists(os.path.join(dest,file)):
					shutil.copy(os.path.join(src,file), dest)
			except:
				logger.error("Unable to copy file "+ file +  " to "+ dest)
				sys.exit(1)
	
def format_seconds(duration, max_comp = 2):
	"""
	Utility for converting time to a readable format
	@param duration :  time in seconds and miliseconds
	@param max_comp :  number of components of the returned time
	@return time in n component format
	"""
	comp = 0
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

   	# days
	if comp < max_comp and (days >= 1 or comp > 0):
		comp += 1
		if days == 1:
			formatted_duration  += str(days) + ' day, '
		else:
			formatted_duration  += str(days) + ' days, '

	# hours
	if comp < max_comp and (hrs >=1 or comp > 0):
		comp += 1
		if hrs == 1:
			formatted_duration  += str(hrs) + ' hr, ' 
		else:
			formatted_duration  += str(hrs) + ' hrs, '
	
	# mins
	if comp < max_comp and (mins >=1 or comp > 0):
		comp += 1
		if mins == 1:
			formatted_duration  += str(mins) + ' min, '
		else:
			formatted_duration  += str(mins) + ' mins, '

	# seconds
	if comp < max_comp and (sec >=1 or comp > 0):
		comp += 1
		if sec ==1:
			formatted_duration  += str(sec) + " sec, "
		else:
			formatted_duration  += str(sec) + " secs, "

	return formatted_duration
	

def get_workflow_wall_time(workflow_states_list):
	"""
	Utility method for returning the workflow wall time given all the workflow states
	@worklow_states_list list of all workflow states.
	"""
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
	"""
	Utility method for returning the db_url and wf_uuid given the submit_dir and pegasus properties file.
	@submit_dir submit directory path
	@config_properties config properties file path
	"""
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
	
def get_date_multiplier(date_filter):
	"""
	Utility for returning the multiplier for a given date filter
	@param date filter :  the given date filter 
	@return multiplier for a given filter
	"""
	vals = {
	'month': 2629743,
	'week': 604800,
	'day': 86400,
	'hour': 3600
	}
	return vals[date_filter]
	
def get_date_format(date_filter):
	"""
	Utility for returning the date format for a given date filter
	@param date filter :  the given date filter 
	@return the date format for a given filter
	"""
	vals = {
	'month': '%Y-%m',
	'week': '%Y-%U',
	'day': '%Y-%m-%d',
	'hour': '%Y-%m-%d : %H'
	}
	return vals[date_filter]
	

def get_date_print_format(date_filter):
	"""
	Utility for returning the date format for a given date filter in human readable format
	@param date filter :  the given date filter 
	@return the date format for a given filter
	"""
	vals = {
	'month': '[YYYY-MM]',
	'week': '[YYYY-WW]',
	'day': '[YYYY-MM-DD]',
	'hour': '[YYYY-MM-DD : HH]'
	}
	return vals[date_filter]

def convert_to_date_format(value , date_time_filter):
	"""
Utility method for converting the value to date format
@param value :  value to format
@param date_time_filter :  the date time filter 
	"""
	multiplier = get_date_multiplier(date_time_filter)
	date_format = datetime.fromtimestamp(int(value*multiplier)).strftime(get_date_format(date_time_filter))
	return date_format
	
	

def round_decimal_to_str(value , to=3):
	"""
Utility method for rounding the decimal value to string to given digits
@param value :  value to round 
@param to    :  how many decimal points to round to
	"""
	rounded_value = '-'
	if value is None:
		return rounded_value
	rounded_value = str(round(float(value) , to))
	return rounded_value
	
