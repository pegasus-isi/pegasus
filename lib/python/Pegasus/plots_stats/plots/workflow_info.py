#!/usr/bin/env python
"""
Base class for storing workflow and job information


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
# Set default level to INFO
logger.setLevel(logging.INFO)

import common
from Pegasus.plots_stats import utils as plot_utils
from datetime import timedelta
from datetime import datetime

#---workflow information object

class WorkflowInfo:
	
	def __init__(self):
		self.submit_dir = None
		self.wf_uuid = None
		self.parent_wf_uuid = None
		self.dax_label = None
		self.dag_label = None
		self.dax_file_path = None
		self.dag_file_path = None
		self.dagman_start_time = None
		self.workflow_run_time = None
		self.total_jobs= None
		self.job_statistics_list =[]
		self.job_statistics_dict ={}
		self.transformation_statistics_dict ={}
		self.host_job_map={}
		self.transformation_color_map={}
		self.job_name_sub_wf_uuid_map ={}
				
	def get_formatted_host_data(self ):
		# find the pretty print length
		host_info =''
		for host_name , job_list in self.host_job_map.items():
			# Case where host_name is not calculated
			if host_name is None:
				host_info += ( " \n{ \"name\":"  + "\"Unknown\" , \"jobs\": [")
			else:
				host_info += ( " \n{ \"name\":"  + "\"" + host_name+ "\" , \"jobs\": [")
			job_info = ''
			for job_stat in job_list:
				job_stat_det = job_stat.getJobDetails(self.dagman_start_time)
				job_info +=("\n\t{")
				job_info +=( "\n\t")
				job_info += ( "\"name\":"  + "\"" + job_stat_det['name']+ "\" , ")
				job_info += ( "\"jobS\":" +  job_stat_det['jobExecS'] +" , ")
				job_info += ( "\"jobD\":" +  job_stat_det['jobExecD'] +" , ")
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
				if self.transformation_color_map.has_key(job_stat_det['transformation']):
					job_info += ( "\"color\": \"" +  self.transformation_color_map[job_stat_det['transformation']] +"\"  , ")
				else:
					# there is no compute task
					job_info += ( "\"color\": 'white' , ")
				"""
				if plot_utils.isSubWfJob(job_stat_det['name']):
					job_info += ( "\"sub_wf\":1 , "  )
					corresponding_dax =''
					if (self.job_name_sub_wf_uuid_map.has_key(job_stat_det['name'])): 
						corresponding_dax = self.job_name_sub_wf_uuid_map[job_stat_det['name']]
						job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ ".html\"")
					else:
						job_info += ( "\"sub_wf_name\":''")	
						
				else:
					job_info += ( "\"sub_wf\":0 , " )	
					job_info += ( "\"sub_wf_name\":''")
				"""
				if plot_utils.isSubWfJob(job_stat_det['name']):
					job_info += ( "\"sub_wf\":1 , "  )
					corresponding_dax =''
					print self.job_name_sub_wf_uuid_map.keys()
					print self.job_name_sub_wf_uuid_map.values()
					if (self.job_name_sub_wf_uuid_map.has_key(job_stat_det['instance_id'])): 
						corresponding_dax = self.job_name_sub_wf_uuid_map[job_stat_det['instance_id']]
						job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ ".html\"")
					else:
						job_info += ( "\"sub_wf_name\":''")	
						
				else:
					job_info += ( "\"sub_wf\":0 , " )	
					job_info += ( "\"sub_wf_name\":''")
				job_info +=( "\n\t},\n")
			host_info+= job_info
			host_info += "]},"	
		return host_info
		
	def get_formatted_job_data(self ):
		# find the pretty print length
		job_info = ''
		for job_stat in self.job_statistics_list:
			job_stat_det = job_stat.getJobDetails(self.dagman_start_time)
			job_info +=("{")
			job_info +=( "\n")
			job_info += ( "\"name\":"  + "\"" + job_stat_det['name']+ "\" , ")
			job_info += ( "\"jobS\":" +  job_stat_det['jobS'] +" , ")
			job_info += ( "\"jobD\":" +  job_stat_det['jobD'] +" , ")
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
			if self.transformation_color_map.has_key(job_stat_det['transformation']):
				job_info += ( "\"color\": \"" +  self.transformation_color_map[job_stat_det['transformation']] +"\"  , ")
			else:
				# there is no compute task
				job_info += ( "\"color\": 'white' , ")
			if plot_utils.isSubWfJob(job_stat_det['name']):
				job_info += ( "\"sub_wf\":1 , "  )
				corresponding_dax =''
				if (self.job_name_sub_wf_uuid_map.has_key(job_stat_det['instance_id'])): 
					corresponding_dax = self.job_name_sub_wf_uuid_map[job_stat_det['instance_id']]
					job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ ".html\"")
				else:
					job_info += ( "\"sub_wf_name\":''")	
					
			else:
				job_info += ( "\"sub_wf\":0 , " )	
				job_info += ( "\"sub_wf_name\":''")
			job_info +=( "},\n")
		return job_info		
						
	
#---job information ---

class JobInfo:
	def __init__(self):
		self.name = None
		self.instance_id = None
		self.site = None
		self.jobStart = None # This is timestamp of the first event in the jobstate.log this could be PRE_SCRIPT_STARTED
		self.jobDuration = None # This is duration till POST SCRIPT TERMINATED event or the last state of the job's run
		self.jobExecStart = None # This is timestamp of SUBMIT event   
		self.jobExecDuration = None # This is duration till JOB TERMINATED event or the last state of the job's run
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
		self.host_name = None
		
		
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
		job_details['instance_id']= self.instance_id
		
		if self.jobStart is not None and  self.jobDuration is not None:
			job_details['jobS'] = str(convert_to_base_time(self.jobStart , global_start_time))
			job_details['jobD'] = str(self.jobDuration)	  
		else:
			job_details['jobS'] = "''"
			job_details['jobD'] ="''"
			
		if self.jobExecStart is not None and  self.jobExecDuration is not None:
			job_details['jobExecS'] = str(convert_to_base_time(self.jobExecStart , global_start_time))
			job_details['jobExecD'] = str(self.jobExecDuration)	  
		else:
			job_details['jobExecS'] = "''"
			job_details['jobExecD'] ="''"
			
		if self.preStart is not None and  self.preDuration is not None:
			job_details['preS'] = str(convert_to_base_time(self.preStart , global_start_time))
			job_details['preD'] = str(self.preDuration)	  
		else:
			job_details['preS'] = "''"
			job_details['preD'] ="''"
			
		if self.condorStart is not None and  self.condorDuration is not None:
			job_details['cS'] = str(convert_to_base_time(self.condorStart , global_start_time))
			job_details['cD'] = str(self.condorDuration)	  
		else:
			job_details['cS'] = "''"
			job_details['cD'] ="''"
			
		if self.gridStart is not None and  self.gridDuration is not None:
			job_details['gS'] = str(convert_to_base_time(self.gridStart , global_start_time))
			job_details['gD'] = str(self.gridDuration)	  
		else:
			job_details['gS'] = "''"
			job_details['gD'] ="''"
		if self.executeStart is not None and  self.executeDuration is not None:
			job_details['eS'] = str(convert_to_base_time(self.executeStart , global_start_time))
			job_details['eD'] = str(self.executeDuration)	  
		else:
			job_details['eS'] = "''"
			job_details['eD'] ="''"
			
		if self.kickstartStart is not None and  self.kickstartDuration is not None:
			job_details['kS'] = str(convert_to_base_time(self.kickstartStart , global_start_time))
			job_details['kD'] = str(self.kickstartDuration)	  
		else:
			job_details['kS'] = "''"
			job_details['kD'] ="''"
			
		if self.postStart is not None and  self.postDuration is not None:
			job_details['postS'] = str(convert_to_base_time(self.postStart , global_start_time))
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
		
#-----date conversion----------
def convert_to_base_time(end_time , start_time):
	"""
	Converts the time to base time
	@param: end_time end time
	@param ; start_time start time
	"""
	return int(end_time)- int(start_time)

