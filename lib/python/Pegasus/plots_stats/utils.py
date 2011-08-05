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
	file_path = os.path.abspath(file_path)
	file_path = file_path.replace(os.path.abspath(parent_wf_braindb_submit_dir),os.path.abspath(parent_submit_dir))
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
	


def print_property_table(props , border= True , separator =""):
	html_content =''
	if border:
		html_content ="<table border = 1 style='color:#600000;'>"
	else:
		html_content ="<table style='color:#600000;'>"
	for key, value in props.items():
		html_content += "<tr><th align ='left' style ='color:#600000'>"+ key +"</th><td style ='color:#888888'>" + separator +value +"</td></tr>"
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
	

def delete_directory(dir_path):
	"""
	Deletes a directory
	@param dir_path directory path
	@return Returns dir_path if deletion succeeds , None otherwise
	"""
	try:
		logger.warning("Deleting directory. Deleting... " + dir_path)
		shutil.rmtree(dir_path)
	except:
		logger.error("Unable to remove directory." + dir_path)
		return None
	return dir_path

def create_directory(dir_name , delete_if_exists = False ):
	if delete_if_exists:
		if os.path.isdir(dir_name):
			if delete_directory(dir_name) is None:
				logger.error("Failed to delete existing directory. " + dir_name)
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
	
