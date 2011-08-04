#!/usr/bin/env python

import os
import re
import sys
import logging
import optparse
import math
import tempfile

# Initialize logging object
logger = logging.getLogger()

import common
from Pegasus.plots_stats import utils as stats_utils
from netlogger.analysis.workflow.sql_alchemy import *
from datetime import timedelta

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program



job_statistics_size =[4,4,9,4,6,11,8,7,7,12]
transformation_statistics_size =[13,5,9,6,4,8,3,3,5]
job_detail_title =['#Job','Site','Kickstart','Post' ,'DAGMan','CondorQTime','Resource','Runtime','Seqexec','Seqexec-Delay']
transformation_statistics_title =['#Transformation','Count' ,'Succeeded', 'Failed','Mean' ,'Variance','Min' ,'Max' ,'Total']
job_detail_title_with_units =['Job','Site','Kickstart (sec.)','Post (sec.)' ,'DAGMan (sec.)','CondorQTime (sec.)','Resource (sec.)','Runtime (sec.)','Seqexec (sec.)','Seqexec-Delay (sec.)']
transformation_statistics_title_with_units =['Transformation','Count' ,'Succeeded', 'Failed','Mean (sec.)' ,'Variance (sec.)','Min (sec.)' ,'Max (sec.)' ,'Total (sec.)']

output_dir=''
cell_spacing =1
job_run_statistics_size = 50



#---workflow statistics

class WorkflowStatistics:
	def __init__(self):
		self.submit_dir ='-'
		self.parent_wf_uuid = None
		self.workflow_run_time = '-'
		self.workflow_cpu_time ='-'
		self.workflow_cpu_time_non_sub_wf ='-'
		self.total_jobs = '-'
		self.total_jobs_non_sub_wf = '-'
		self.succeeded_jobs ='-'
		self.succeeded_jobs_non_sub_wf ='-'
		self.failed_jobs ='-'
		self.failed_jobs_non_sub_wf ='-'
		self.unsubmitted_jobs ='-'
		self.unsubmitted_jobs_non_sub_wf ='-'
		self.unknown_jobs ='-'
		self.unknown_jobs_non_sub_wf ='-'
		self.total_succeeded_tasks ='-'
		self.total_failed_tasks ='-'
		self.job_statistics_dict ={}
		self.transformation_statistics_dict ={}
	
	
	def get_formatted_workflow_info(self):
		
		workflow_info = ''
		workflow_info +="#" + self.submit_dir
		workflow_info += "\n"
		if self.workflow_run_time is None or self.workflow_run_time == '-':
			workflow_info +="Workflow runtime                   :" + 	"-" 
		else:
			workflow_info +="Workflow runtime                   :" + stats_utils.format_seconds(self.workflow_run_time)
		workflow_info +="\n"
		if self.workflow_cpu_time is None or self.workflow_cpu_time =='-':
			workflow_info +="Cumulative workflow runtime        :" + "-"
		else:
			workflow_info +="Cumulative workflow runtime        :" + stats_utils.format_seconds(self.workflow_cpu_time)
		workflow_info +="\n"
		workflow_info +="Total jobs run                     :" + str(self.total_jobs)
		workflow_info +="\n"	
		workflow_info +="# jobs succeeded                   :" + str(self.succeeded_jobs)
		workflow_info +="\n"
		workflow_info +="# jobs failed                      :" + str(self.failed_jobs)
		workflow_info +="\n"
		workflow_info +="# jobs unsubmitted                 :" + str(self.unsubmitted_jobs)
		workflow_info +="\n"
		workflow_info +="# jobs unknown                     :" + str(self.unknown_jobs)
		workflow_info +="\n"
		workflow_info +="# Total tasks succeeded            :" + str(self.total_succeeded_tasks)
		workflow_info +="\n"
		workflow_info +="# Total tasks failed               :" + str(self.total_failed_tasks)
		workflow_info +="\n"
		return workflow_info
	
	def get_html_formatted_workflow_info(self):
		workflow_info = ''
		workflow_info += "<div> <table style='color:#600000;'>\n"
		if self.workflow_run_time is None or self.workflow_run_time == '-':
			workflow_info +="<tr><th style ='color:#600000'><pre>Workflow runtime                   :</pre></th><td style ='color:#888888'>" + "-" + "</td></tr>"
		else:
			workflow_info +="<tr><th style ='color:#600000'><pre>Workflow runtime                   :</pre></th><td style ='color:#888888'>" + stats_utils.format_seconds(self.workflow_run_time) + "</td></tr>"
		workflow_info +="\n"
		if self.workflow_cpu_time is None or self.workflow_cpu_time =='-':
			workflow_info +="<tr><th style ='color:#600000'><pre>Cumulative workflow runtime       :</pre></th><td style ='color:#888888'>" + "-"+ "</td></tr>"
		else:
			workflow_info +="<tr><th style ='color:#600000'><pre>Cumulative workflow runtime       :</pre></th><td style ='color:#888888'>" + stats_utils.format_seconds(self.workflow_cpu_time)+ "</td></tr>"
		workflow_info +="\n"
		workflow_info +="<tr><th style ='color:#600000'><pre>Total jobs run                     :</pre></th><td style ='color:#888888'>" + str(self.total_jobs)+ "</td></tr>"
		workflow_info +="\n"	
		workflow_info +="<tr><th style ='color:#600000'><pre># jobs succeeded                   :</pre></th><td style ='color:#888888'>" + str(self.succeeded_jobs)+ "</td></tr>"
		workflow_info +="\n"
		workflow_info +="<tr><th style ='color:#600000'><pre># jobs failed                      :</pre></th><td style ='color:#888888'>" + str(self.failed_jobs)+ "</td></tr>"
		workflow_info +="\n"
		workflow_info +="<tr><th style ='color:#600000'><pre># jobs unsubmitted                 :</pre></th><td style ='color:#888888'>" + str(self.unsubmitted_jobs)+ "</td></tr>"
		workflow_info +="\n"
		workflow_info +="<tr><th style ='color:#600000'><pre># jobs unknown                     :</pre></th><td style ='color:#888888'>" + str(self.unknown_jobs)+ "</td></tr>"
		workflow_info +="\n"
		workflow_info +="<tr><th style ='color:#600000'><pre># Total tasks succeeded            :</pre></th><td style ='color:#888888'>" + str(self.total_succeeded_tasks)+ "</td></tr>"
		workflow_info +="\n"	
		workflow_info +="<tr><th style ='color:#600000'><pre># Total tasks failed               :</pre></th><td style ='color:#888888'>" + str(self.total_failed_tasks)+ "</td></tr>"
		workflow_info +="</table></div>"	
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
			job_det =job_stat.getFormattedJobStatistics()
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
			job_det =job_stat.getFormattedJobStatistics()
			for index in range(len(job_det)):
				if isinstance(job_det[index],basestring):
					job_info += (job_det[index].ljust(job_statistics_size[index] + cell_spacing))
				else:
					job_info += (str(job_det[index]).ljust(job_statistics_size[index] + cell_spacing))
		job_info +=( "\n")
		return job_info
		
		
	def get_html_formatted_job_info(self):
		content_list = []
		# find the pretty print length
		#Add sort
		keys = self.job_statistics_dict.keys()
		keys.sort()	
		for key in keys:
			job_stat = self.job_statistics_dict[key]
			job_det =job_stat.getFormattedJobStatistics()
			content_list.append(job_det)
		job_info = stats_utils.print_grid_table(job_detail_title_with_units,content_list )
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
		
	def get_html_formatted_transformation_info(self):
		content_list =[]
		keys = 	self.transformation_statistics_dict.keys()
		keys.sort()
		# Sorting by name
		for k in keys:
			v = self.transformation_statistics_dict[k]
			trans_det = [k,v.count,v.successcount,v.failcount, v.mean, v.variance,v.min, v.max , v.total_runtime]
			content_list.append(trans_det)
		trans_info = stats_utils.print_grid_table(transformation_statistics_title_with_units,content_list )
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
	
	def getFormattedJobStatistics(self):
		formatted_job_stats = [self.name]
		if self.site is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.site)
		if self.kickstart is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.kickstart)
		if self.post is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.post)
		
		if self.dagman is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.dagman)
		
		if self.condor_delay is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.condor_delay)
				
		if self.resource is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.resource)
		
		if self.runtime is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.runtime)
		
		formatted_job_stats.append(self.seqexec)
		formatted_job_stats.append(self.seqexec_delay)
		return formatted_job_stats

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





