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

logger = logging.getLogger(__name__)

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
		self.total_job_instances = None
		self.total_tasks = None
		self.job_statistics_list =[]
		self.transformation_statistics_dict ={}
		self.host_job_map={}
		self.transformation_color_map={}
		self.job_instance_id_sub_wf_uuid_map ={}
		self.sub_wf_id_uuids = []
		self.wf_env ={}
		self.wf_job_instances_over_time_statistics = {}
		self.wf_invocations_over_time_statistics = {}
				
	def get_formatted_host_data(self , extn = "html" ):
		"""
		Returns formatted host information data.
		"""
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
				if job_stat_det['transformation'] in self.transformation_color_map:
					job_info += ( "\"color\": \"" +  self.transformation_color_map[job_stat_det['transformation']] +"\"  , ")
				else:
					# there is no compute task
					job_info += ( "\"color\": 'white' , ")
				if plot_utils.isSubWfJob(job_stat_det['name']):
					job_info += ( "\"sub_wf\":1 , "  )
					corresponding_dax =''
					if (job_stat_det['instance_id'] in self.job_instance_id_sub_wf_uuid_map): 
						corresponding_dax = self.job_instance_id_sub_wf_uuid_map[job_stat_det['instance_id']]
						job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ "." + extn +"\"")
					else:
						job_info += ( "\"sub_wf_name\":''")	
						
				else:
					job_info += ( "\"sub_wf\":0 , " )	
					job_info += ( "\"sub_wf_name\":''")
				job_info +=( "\n\t},\n")
			host_info+= job_info
			host_info += "]},"	
		return host_info
		
	def get_formatted_transformation_data(self ):
		"""
		Returns formatted job information data.
		"""
		# find the pretty print length
		trans_info = ''
		for trans_stat in self.transformation_statistics_dict.values():
			trans_stat_det = trans_stat.getTransformationDetails()
			trans_info +=("{")
			trans_info +=( "\n")
			trans_info += ( "\"name\":"  + "\"" + trans_stat_det['name']+ "\" , ")
			trans_info += ( "\"count\":" +  trans_stat_det['count'] +" , ")
			trans_info += ( "\"success\":" +  trans_stat_det['success'] +" , ")
			trans_info += ( "\"failure\":" +  trans_stat_det['failure'] +" , ")
			trans_info += ( "\"min\":" +  trans_stat_det['min'] +" , ")
			trans_info += ( "\"max\":"  +  trans_stat_det['max'] +" , ")
			trans_info += ( "\"avg\":"    +  trans_stat_det['avg']  +" , ")
			trans_info += ( "\"total\":"    +  trans_stat_det['total']  +" , ")
			if trans_stat_det['name'] in self.transformation_color_map:
				trans_info += ( "\"color\": \"" +  self.transformation_color_map[trans_stat_det['name']] +"\"  ")
			else:
				# there is no compute task
				trans_info += ( "\"color\": 'white'  ")
			trans_info +=( "},\n")
		return trans_info
	
	def get_total_count_run_time(self):
		total_invoc_count =0
		total_runtime =0 
		for trans_stat in self.transformation_statistics_dict.values():
			total_invoc_count +=trans_stat.count
			total_runtime +=trans_stat.total_runtime
		return total_invoc_count, total_runtime
		
	def get_formatted_job_data(self ,extn ="html" ):
		"""
		Returns formatted job information data.
		"""
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
			if job_stat_det['transformation'] in self.transformation_color_map:
				job_info += ( "\"color\": \"" +  self.transformation_color_map[job_stat_det['transformation']] +"\"  , ")
			else:
				# there is no compute task
				job_info += ( "\"color\": 'white' , ")
			if plot_utils.isSubWfJob(job_stat_det['name']):
				job_info += ( "\"sub_wf\":1 , "  )
				corresponding_dax =''
				if (job_stat_det['instance_id'] in self.job_instance_id_sub_wf_uuid_map): 
					corresponding_dax = self.job_instance_id_sub_wf_uuid_map[job_stat_det['instance_id']]
					job_info += ( "\"sub_wf_name\":\""+ corresponding_dax+ "." + extn+"\"")
				else:
					job_info += ( "\"sub_wf_name\":''")	
					
			else:
				job_info += ( "\"sub_wf\":0 , " )	
				job_info += ( "\"sub_wf_name\":''")
			job_info +=( "},\n")
		return job_info
		
	def get_formatted_job_instances_over_time_data(self , date_time_filter):
		"""
		Returns formatted job instance over time data.
		@param date_time_filter date time filter
		"""
		job_instance_over_time_list = self.wf_job_instances_over_time_statistics[date_time_filter]
		job_info = ''
		for job_stat in job_instance_over_time_list:
			job_info +=("{")
			job_info +=( "\n")
			job_info += ( "\"datetime\":"  + "\"" + job_stat[0]+ "\" , ")
			job_info += ( "\"count\":" +  str(job_stat[1]) +" , ")
			job_info += ( "\"runtime\":" +  plot_utils.round_decimal_to_str(job_stat[2]) +"  ")
			job_info +=( "},\n")
		return job_info
		
	def get_formatted_invocations_over_time_data(self , date_time_filter):
		"""
		Returns formatted invocations over time  data.
		@param date_time_filter date time filter
		"""
		invs_over_time_list = self.wf_invocations_over_time_statistics[date_time_filter]
		inv_info = ''
		for inv_stat in invs_over_time_list:
			inv_info +=("{")
			inv_info +=( "\n")
			inv_info += ( "\"datetime\":"  + "\"" + inv_stat[0] + "\" , ")
			inv_info += ( "\"count\":" +  str(inv_stat[1]) +" , ")
			inv_info += ( "\"runtime\":" +  plot_utils.round_decimal_to_str(inv_stat[2]) +"  ")
			inv_info +=( "},\n")
		return inv_info
	
	def get_formatted_job_instances_over_time_metadata(self , date_time_filter):
		"""
		Returns formatted job instances over time metadata.
		@param date_time_filter date time filter
		"""
		job_instance_over_time_list = self.wf_job_instances_over_time_statistics[date_time_filter]
		max_count, max_runtime =  self.get_max_count_run_time(True, date_time_filter)
		job_info = ''
		job_info +=("{")
		job_info +=( "\n")
		job_info += ( "\"num\":"  + "\"" + str(len(job_instance_over_time_list))+ "\" , ")
		job_info += ( "\"max_count\":" +  str(max_count) +" , ")
		job_info += ( "\"max_runtime\":" +  plot_utils.round_decimal_to_str(max_runtime) +"  ")
		job_info +=( "},\n")
		return job_info
		
	def get_formatted_invocations_over_time_metadata(self , date_time_filter):
		"""
		Returns formatted invocations over time metadata.
		@param date_time_filter date time filter
		"""
		invs_over_time_list = self.wf_invocations_over_time_statistics[date_time_filter]
		max_count, max_runtime =  self.get_max_count_run_time(False, date_time_filter)
		inv_info = ''
		inv_info +=("{")
		inv_info +=( "\n")
		inv_info += ( "\"num\":"  + "\"" + str(len(invs_over_time_list))+ "\" , ")
		inv_info += ( "\"max_count\":" +  str(max_count) +" , ")
		inv_info += ( "\"max_runtime\":" +  plot_utils.round_decimal_to_str(max_runtime) +"  ")
		inv_info +=( "},\n")
		return inv_info
	
	def get_max_count_run_time(self, isJobInstance, date_time_filter):
		"""
		Returns the maximum count and runtime when filter by given filter.
		@parm isJobInstance true if it is calculated for job instances, false otherwise
		@param date_time_filter date time filter
		"""
		if isJobInstance:
			content_list = self.wf_job_instances_over_time_statistics[date_time_filter]
		else:
			content_list = self.wf_invocations_over_time_statistics[date_time_filter]
		
		if len(content_list) < 1:
			return None, None
		max_run_time = 0.0
		max_count =0
		for content in content_list:
			max_count = max(content[1],max_count )
			max_run_time = max(content[2],max_run_time )
		return max_count , max_run_time
						
	
#---job information ---

class JobInfo:
	def __init__(self):
		self.name = None
		self.instance_id = None
		self.retry_count = None
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
		"""
		Returns whether the job instance was successful or not
		"""
		if self.state =='SUCCESS':
			return True
		return False
	
	@property
	def is_failure(self):
		"""
		Returns whether the job instance was successful or not
		"""
		if self.state =='FAILED':
			return True
		return False
	
	def getJobDetails(self , global_start_time):
		"""
		Returns the job instance details
		@global_start_time the workflow start event.
		"""
		job_details ={}
		if self.retry_count > 0:
			job_details['name'] = self.name +"_retry_"+ str(self.retry_count)
		else:
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
		

class TransformationInfo:
	def __init__(self):
		self.name = None
		self.count = None
		self.succeeded_count = None
		self.failed_count = None
		self.total_runtime = None
		self.min = None
		self.max = None
		self.avg = None
	def getTransformationDetails(self ):
		"""
		Returns the transformation  details
		"""
		trans_details ={}
		trans_details['name'] = self.name
		trans_details['count'] = str(self.count)
		trans_details['success'] = str(self.succeeded_count)
		trans_details['failure'] = str(self.failed_count)
		trans_details['min'] = str(self.min)
		trans_details['max'] = str(self.max)
		trans_details['avg'] = str(self.avg)
		trans_details['total'] = str(self.total_runtime)
		return trans_details
		

		
#-----date conversion----------
def convert_to_base_time(end_time , start_time):
	"""
	Converts the time to base time
	@param: end_time end time
	@param ; start_time start time
	"""
	return int(end_time)- int(start_time)

