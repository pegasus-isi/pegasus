
import os
import re
import sys
import logging
import optparse
import math
import tempfile

logger = logging.getLogger(__name__)

from Pegasus.plots_stats import utils as stats_utils

transformation_stats_col_name =["Transformation","Count","Succeeded" , "Failed", "Min","Max","Mean","Total"]
job_stats_col_name =['Job','Try','Site','Kickstart','Post' ,'CondorQTime','Resource','Runtime','Seqexec','Seqexec-Delay']
worklow_summary_col_name =["Type" ,"Succeeded","Failed","Incomplete" ,"Total" , " " ,"Retries" , "Total Run (Retries Included)"]
worklow_status_col_name =["#","Type" ,"Succeeded","Failed","Incomplete" ,"Total" , " " ,"Retries" , "Total Run (Retries Included)" ,"Workflow Retries"]
time_stats_col_name =["Date","Count","Runtime"]
time_host_stats_col_name =["Date","Host", "Count","Runtime (Sec.)"]

NEW_LINE_STR ="\n"



class JobStatistics:
	def __init__(self):
		self.name = None
		self.site = None
		self.kickstart =None
		self.post = None
		self.condor_delay = None
		self.resource = None
		self.runtime = None
		self.condorQlen =None
		self.seqexec= None
		self.seqexec_delay = None
		self.retry_count = 0

	def getFormattedJobStatistics(self):
		"""
		Returns the formatted job statistics information
		@return:    formatted job statistics information
		"""
		formatted_job_stats = [self.name]
		formatted_job_stats.append(str(self.retry_count))
		if self.site is None:
			formatted_job_stats.append('-')
		else:
			formatted_job_stats.append(self.site)
		formatted_job_stats.append(round_to_str(self.kickstart))
		formatted_job_stats.append(round_to_str(self.post))
		formatted_job_stats.append(round_to_str(self.condor_delay))
		formatted_job_stats.append(round_to_str(self.resource))
		formatted_job_stats.append(round_to_str(self.runtime))
		formatted_job_stats.append(round_to_str(self.seqexec))
		formatted_job_stats.append(round_to_str(self.seqexec_delay))
		return formatted_job_stats

def convert_to_str(value):
	"""
	Utility for returning a str representation of the given value.
	Return '-' if value is None
	@parem value : the given value that need to be converted to string
	"""
	if value is None:
		return '-'
	return str(value)

def round_to_str(value , to=3):
	"""
Utility method for rounding the float value to rounded string
@param value :  value to round
@param to    :  how many decimal points to round to
	"""
	return stats_utils.round_decimal_to_str(value,to)

def format_seconds(duration):
	"""
	Utility for converting time to a readable format
	@param duration :  time in seconds and miliseconds
	@return time in format day,hour, min,sec
	"""
	return stats_utils.format_seconds(duration)


def print_row(content, isHeader= False):
	"""
	Utility method for generating one html row
	@param content :  list of column values
	@param format  :  column_size of each columns
	"""
	row_str =""
	row_str ="<tr>"
	for index in range(len(content)):
		if isHeader:
			row_str +="<th>"
		else:
			row_str +="<td>"
		row_str += str(content[index])
		if isHeader:
			row_str +="</th>"
		else:
			row_str +="</td>"
	row_str += "</tr>"
	row_str += NEW_LINE_STR
	return row_str


def print_workflow_summary(workflow_stats ):
	"""
	Prints the workflow statistics summary of an top level workflow
	@param workflow_stats :  workflow statistics object reference
	"""
	# status
	workflow_stats.set_job_filter('nonsub')
	# Tasks
	total_tasks = workflow_stats.get_total_tasks_status()
	total_succeeded_tasks = workflow_stats.get_total_succeeded_tasks_status()
	total_failed_tasks = workflow_stats.get_total_failed_tasks_status()
	total_unsubmitted_tasks = total_tasks -(total_succeeded_tasks + total_failed_tasks)
	total_task_retries =  workflow_stats.get_total_tasks_retries()
	total_invocations = total_succeeded_tasks + total_failed_tasks + total_task_retries
	# Jobs
	total_jobs = workflow_stats.get_total_jobs_status()
	total_succeeded_jobs = workflow_stats.get_total_succeeded_jobs_status()
	total_failed_jobs = workflow_stats.get_total_failed_jobs_status()
	total_unsubmitted_jobs = total_jobs - (total_succeeded_jobs + total_failed_jobs )
	total_job_retries = workflow_stats.get_total_jobs_retries()
	total_job_instance_retries =  total_succeeded_jobs + total_failed_jobs + total_job_retries
	# Sub workflows
	workflow_stats.set_job_filter('subwf')
	total_sub_wfs = workflow_stats.get_total_jobs_status()
	total_succeeded_sub_wfs = workflow_stats.get_total_succeeded_jobs_status()
	total_failed_sub_wfs = workflow_stats.get_total_failed_jobs_status()
	total_unsubmitted_sub_wfs = total_sub_wfs - (total_succeeded_sub_wfs + total_failed_sub_wfs)
	total_sub_wfs_retries = workflow_stats.get_total_jobs_retries()
	total_sub_wfs_tries =  total_succeeded_sub_wfs + total_failed_sub_wfs + total_sub_wfs_retries

	# tasks
	summary_str = ""
	summary_str += "total_succeeded_tasks: " + convert_to_str(total_succeeded_tasks)
	summary_str += NEW_LINE_STR
	summary_str += "total_failed_tasks: " + convert_to_str(total_failed_tasks)
	summary_str += NEW_LINE_STR
	summary_str += "total_unsubmitted_tasks: " + convert_to_str(total_unsubmitted_tasks)
	summary_str += NEW_LINE_STR
	summary_str += "total_tasks: " + convert_to_str(total_tasks)
	summary_str += NEW_LINE_STR
	summary_str += "total_task_retries: " + convert_to_str(total_task_retries)
	summary_str += NEW_LINE_STR
	summary_str += "total_invocations: " + convert_to_str(total_invocations)
	summary_str += NEW_LINE_STR


	summary_str += "total_succeeded_jobs: " + convert_to_str(total_succeeded_jobs)
	summary_str += NEW_LINE_STR
	summary_str += "total_failed_jobs: " + convert_to_str(total_failed_jobs)
	summary_str += NEW_LINE_STR
	summary_str += "total_unsubmitted_jobs: " + convert_to_str(total_unsubmitted_jobs)
	summary_str += NEW_LINE_STR
	summary_str += "total_jobs:" + convert_to_str(total_jobs)
	summary_str += NEW_LINE_STR
	summary_str += "total_job_retries: " + str(total_job_retries)
	summary_str += NEW_LINE_STR
	summary_str += "total_job_instance_retries:"  + convert_to_str(total_job_instance_retries)
	summary_str += NEW_LINE_STR


	summary_str += "total_succeeded_sub_wfs: " + convert_to_str(total_succeeded_sub_wfs)
	summary_str += NEW_LINE_STR
	summary_str += "total_failed_sub_wfs: " + convert_to_str(total_failed_sub_wfs)
	summary_str += NEW_LINE_STR
	summary_str += "total_unsubmitted_sub_wfs: " + convert_to_str(total_unsubmitted_sub_wfs)
	summary_str += NEW_LINE_STR
	summary_str += "total_sub_wfs: " + convert_to_str(total_sub_wfs)
	summary_str += NEW_LINE_STR
	summary_str += "total_sub_wfs_retries: " + str(total_sub_wfs_retries)
	summary_str += NEW_LINE_STR
	summary_str += "total_sub_wfs_tries: " + convert_to_str(total_sub_wfs_tries)
	summary_str += NEW_LINE_STR

	workflow_states_list = workflow_stats.get_workflow_states()
	workflow_wall_time = stats_utils.get_workflow_wall_time(workflow_states_list)

	if workflow_wall_time is None:
		summary_str += "workflow_runtime: -"
	else:
		summary_str += "workflow_runtime: %-20s (total %d seconds)" % \
				(format_seconds(workflow_wall_time), (workflow_wall_time))
	summary_str += NEW_LINE_STR
	workflow_cum_job_wall_time = workflow_stats.get_workflow_cum_job_wall_time()[0]
	if workflow_cum_job_wall_time is None:
		summary_str += "cumulative_workflow_runtime_kickstart: -"
	else:
		summary_str += "cumulative_workflow_runtime_kickstart: %-20s (total %d seconds)" % \
			(format_seconds(workflow_cum_job_wall_time),workflow_cum_job_wall_time)
	summary_str += NEW_LINE_STR
	submit_side_job_wall_time =  workflow_stats.get_submit_side_job_wall_time()[0]
	if submit_side_job_wall_time is None:
		summary_str += "cumulative_workflow_runtime_dagman: -"
	else:
		summary_str += "cumulative_workflow_runtime_dagman: %-20s (total %d seconds)" % \
			(format_seconds(submit_side_job_wall_time), submit_side_job_wall_time)
	return summary_str

def print_individual_workflow_stats(workflow_stats , title):
	"""
	Prints the workflow statistics of workflow
	@param workflow_stats :  workflow statistics object reference
	@param title  : title of the workflow table
	"""
	content_str ="<table class ='gallery_table'>"
	# individual workflow status

	# workflow status
	workflow_stats.set_job_filter('all')
	total_wf_retries = workflow_stats.get_workflow_retries()
	content = [title,convert_to_str(total_wf_retries) ]
	title_col_span =  len(worklow_status_col_name) -1
	content_str += print_row(worklow_status_col_name, True)
	wf_status_str = """<tr><td colspan ="""+ str(title_col_span) +  """ > """ + title + """</td><td>""" + convert_to_str(total_wf_retries) +"""</td></tr>"""

	#tasks
	workflow_stats.set_job_filter('nonsub')
	total_tasks = workflow_stats.get_total_tasks_status()
	total_succeeded_tasks = workflow_stats.get_total_succeeded_tasks_status()
	total_failed_tasks = workflow_stats.get_total_failed_tasks_status()
	total_unsubmitted_tasks = total_tasks -(total_succeeded_tasks + total_failed_tasks )
	total_task_retries =  workflow_stats.get_total_tasks_retries()
	total_task_invocations = total_succeeded_tasks + total_failed_tasks + total_task_retries
	content =["&nbsp;","Tasks",  convert_to_str(total_succeeded_tasks) , convert_to_str(total_failed_tasks), convert_to_str(total_unsubmitted_tasks) , convert_to_str(total_tasks) ,"&nbsp;",convert_to_str(total_task_retries), convert_to_str(total_task_invocations) ,"&nbsp;"]
	tasks_status_str =  print_row(content)

	# job status
	workflow_stats.set_job_filter('nonsub')
	total_jobs = workflow_stats.get_total_jobs_status()
	total_succeeded_jobs = workflow_stats.get_total_succeeded_jobs_status()
	total_failed_jobs = workflow_stats.get_total_failed_jobs_status()
	total_unsubmitted_jobs = total_jobs - (total_succeeded_jobs + total_failed_jobs )
	total_job_retries = workflow_stats.get_total_jobs_retries()
	total_job_invocations =  total_succeeded_jobs + total_failed_jobs + total_job_retries
	content = ["&nbsp;","Jobs",convert_to_str(total_succeeded_jobs), convert_to_str(total_failed_jobs) , convert_to_str(total_unsubmitted_jobs), convert_to_str(total_jobs) ,"&nbsp;",convert_to_str(total_job_retries), convert_to_str(total_job_invocations) ,"&nbsp;" ]
	jobs_status_str = print_row(content)

	# sub workflow
	workflow_stats.set_job_filter('subwf')
	total_sub_wfs = workflow_stats.get_total_jobs_status()
	total_succeeded_sub_wfs = workflow_stats.get_total_succeeded_jobs_status()
	total_failed_sub_wfs = workflow_stats.get_total_failed_jobs_status()
	total_unsubmitted_sub_wfs = total_sub_wfs - (total_succeeded_sub_wfs + total_failed_sub_wfs )
	total_sub_wfs_retries = workflow_stats.get_total_jobs_retries()
	total_sub_wfs_invocations =  total_succeeded_sub_wfs + total_failed_sub_wfs + total_sub_wfs_retries
	content = ["&nbsp;","Sub Workflows",convert_to_str(total_succeeded_sub_wfs), convert_to_str(total_failed_sub_wfs) , convert_to_str(total_unsubmitted_sub_wfs), convert_to_str(total_sub_wfs) ,"&nbsp;",convert_to_str(total_sub_wfs_retries), convert_to_str(total_sub_wfs_invocations) ,"&nbsp;" ]
	sub_wf_status_str = print_row(content)


	content_str += wf_status_str +"\n"
	content_str += tasks_status_str +"\n"
	content_str += jobs_status_str +"\n"
	content_str += sub_wf_status_str +"\n"
	content_str +="</table>"
	return content_str


def print_individual_wf_job_stats(workflow_stats , title):
	"""
	Prints the job statistics of workflow
	@param workflow_stats :  workflow statistics object reference
	@param title  : title for the table
	"""
	job_stats_dict={}
	job_stats_list=[]
	job_retry_count_dict={}
	job_status_str = "<div># " +  title +"</div>"
	job_status_str += "<table class ='gallery_table'>"
	job_status_str += print_row(job_stats_col_name, True)
	job_status_str +="\n"

	wf_job_stats_list =  workflow_stats.get_job_statistics()

	for job in wf_job_stats_list:
		job_stats = JobStatistics()
		job_stats.name = job.job_name
		job_stats.site = job.site
		job_stats.kickstart = job.kickstart
		job_stats.post = job.post_time
		job_stats.runtime = job.runtime
		job_stats.condor_delay = job.condor_q_time
		job_stats.resource = job.resource_delay
		job_stats.seqexec = job.seqexec
		if job_stats.seqexec is not None and job_stats.kickstart is not None:
			job_stats.seqexec_delay = (float(job_stats.seqexec) - float(job_stats.kickstart))
		if job.job_name in job_retry_count_dict:
			job_retry_count_dict[job.job_name] +=1
		else:
			job_retry_count_dict[job.job_name] = 1
		job_stats.retry_count = job_retry_count_dict[job.job_name]
		job_stats_list.append(job_stats)

	# printing
	content_list = []
	# find the pretty print length
	for job_stat in job_stats_list:
		job_det =job_stat.getFormattedJobStatistics()
		job_status_str +=print_row(job_det)
		job_status_str +=NEW_LINE_STR
	job_status_str += "</table>"
	return job_status_str


def round_to_str(value , to=3):
	"""
Utility method for rounding the float value to rounded string
@param value :  value to round
@param to    :  how many decimal points to round to
	"""
	return stats_utils.round_decimal_to_str(value,to)


def print_wf_transformation_stats(workflow_stats , title):
	"""
Prints the transformation statistics of workflow
@param workflow_stats :  workflow statistics object reference
@param title  : title of the transformation statistics
	"""
	transformation_status_str = "<div># " +  title +"</div>"
	transformation_status_str += "<table class ='gallery_table'>"
	transformation_status_str += print_row(transformation_stats_col_name, True)
	transformation_status_str += NEW_LINE_STR
	for transformation in workflow_stats.get_transformation_statistics():
		content = [transformation.transformation ,str(transformation.count),str(transformation.success) , str(transformation.failure), round_to_str(transformation.min),round_to_str(transformation.max),round_to_str(transformation.avg),round_to_str(transformation.sum)]
		transformation_status_str += print_row(content )
		transformation_status_str += NEW_LINE_STR
	transformation_status_str += "</table>"
	return transformation_status_str


def print_statistics_by_time_and_host(workflow_stats , time_filter):
	"""
	Prints the job instance and invocation statistics sorted by time
	@param workflow_stats :  workflow statistics object reference
	"""
	statistics_by_time_str = NEW_LINE_STR
	workflow_stats.set_job_filter('nonsub')
	workflow_stats.set_time_filter('hour')
	workflow_stats.set_transformation_filter(exclude=['condor::dagman'])

	statistics_by_time_str +="<div>#Job instances statistics per " + time_filter +"</div>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<table class ='gallery_table'>"
	statistics_by_time_str +=print_row(time_stats_col_name, True)
	statistics_by_time_str += NEW_LINE_STR
	stats_by_time = workflow_stats.get_jobs_run_by_time()
	formatted_stats_list = stats_utils.convert_stats_to_base_time(stats_by_time , time_filter)
	for stats in formatted_stats_list:
		content = [stats['date_format'] , str(stats['count']),round_to_str(stats['runtime'])]
		statistics_by_time_str += print_row(content )
		statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="</table>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<div>#Invocation statistics run per " + time_filter +"</div>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<table class ='gallery_table'>"
	statistics_by_time_str +=print_row(time_stats_col_name , True )
	statistics_by_time_str += NEW_LINE_STR
	stats_by_time = workflow_stats.get_invocation_by_time()
	formatted_stats_list = stats_utils.convert_stats_to_base_time(stats_by_time , time_filter)
	for stats in formatted_stats_list:
		content = [stats['date_format'] , str(stats['count']),round_to_str(stats['runtime'])]
		statistics_by_time_str += print_row(content )
		statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="</table>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<div>#Job instances statistics on host per " + time_filter +"</div>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<table class ='gallery_table'>"
	statistics_by_time_str +=print_row(time_host_stats_col_name , True )
	statistics_by_time_str += NEW_LINE_STR
	stats_by_time = workflow_stats.get_jobs_run_by_time_per_host()
	formatted_stats_list = stats_utils.convert_stats_to_base_time(stats_by_time , time_filter, True)
	for stats in formatted_stats_list:
		content = [stats['date_format'] ,str(stats['host']) , str(stats['count']),round_to_str(stats['runtime'])]
		statistics_by_time_str += print_row(content)
		statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="</table>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<div>#Invocation statistics on host per " + time_filter +"</div>"
	statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="<table class ='gallery_table'>"
	statistics_by_time_str +=print_row(time_host_stats_col_name , True )
	statistics_by_time_str += NEW_LINE_STR
	stats_by_time = workflow_stats.get_invocation_by_time_per_host()
	formatted_stats_list = stats_utils.convert_stats_to_base_time(stats_by_time , time_filter, True)
	for stats in formatted_stats_list:
		content = [stats['date_format'] ,str(stats['host']) , str(stats['count']),round_to_str(stats['runtime'])]
		statistics_by_time_str += print_row(content )
		statistics_by_time_str += NEW_LINE_STR
	statistics_by_time_str +="</table>"
	return statistics_by_time_str
