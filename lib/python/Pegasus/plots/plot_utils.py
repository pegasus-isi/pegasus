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

#TODO add subdag_ job also the sub workflow jobs
def isSubWfJob(job_name):
	if job_name.lstrip().startswith('subdax_'):
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
		brain_db_content ="<div style='width:1200;margin : 0 auto;'><table border = 1 style='color:#600000;'>"
		for key, value in config.items():
			brain_db_content += "<tr><th style ='color:#600000'>"+ key +"</th><td style ='color:#888888'>" +value +"</td></tr>"
		brain_db_content +="</table></div>"
	return brain_db_content


def convert_to_seconds(time):
	"""
	Converts the timedelta to seconds format 
	Param: time delta reference
	"""
	return (time.microseconds + (time.seconds + time.days * 24 * 3600) * pow(10,6)) / pow(10,6)

# ---------main----------------------------------------------------------------------------
def main():
	sys.exit(0)
	
	

if __name__ == '__main__':
	main()
