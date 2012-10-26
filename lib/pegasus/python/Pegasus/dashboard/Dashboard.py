##
#  Copyright 2007-2012 University Of Southern California
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

# Revision : $Revision: 2012 $

from netlogger.analysis.workflow import stampede_statistics

__author__ = "Rajiv Mayani"

#Python modules
import os
from time import localtime, strftime
from Pegasus.tools import utils
from Pegasus.plots_stats import utils as stats_utils

#Flask modules
from flask import url_for

#Dashboard modules
from Pegasus.dashboard import WorkflowInfo as queries

class NoWorkflowsFoundError (Exception):
    def __init__(self, **args):
        if 'count' in args:
            self.count = args ['count']
        else:
            self.count = 0
        
        if 'filtered' in args:
            self.filtered = args ['filtered']

class Utils(object):
    
    DAY = 86400
    HOUR = 3600
    MIN = 60

    @staticmethod
    def hour_multiplier (j):
        j.date_format *= Utils.HOUR
    
    @staticmethod
    def delete_pid_file (pid_filename):
        try:
            os.unlink (pid_filename)
        except OSError:
            print ('Cannot delete pid file %s' % pid_filename)

class Dashboard(object):
    
    def __init__(self, conn_url, root_wf_id=None, wf_id=None):
        if not conn_url:
            raise ValueError, 'A connection URL is required'
        
        self._master_db_url = conn_url
    
        """
        If the ID is specified, it means that the query is specific to a work-flow.
        So we will now query the master database to get the connection URL for the work-flow.
        """ 
        if root_wf_id or wf_id:
            self.initialize (root_wf_id, wf_id)
    
    def initialize (self, root_wf_id, wf_id):
 
        try:
            workflow = queries.MasterDatabase (self._master_db_url)
            self._db_id, self._root_wf_uuid, self._wf_db_url = workflow.get_wf_id_url (root_wf_id)
            self._wf_id = wf_id
        finally:
            Dashboard.close (workflow)
    
    @staticmethod
    def close (conn):
        if conn:
            conn.close ()
    
    def __get_wf_db_url (self):
        
        if not self._wf_db_url:
            raise ValueError, 'Work-flow database URL is not set'
        
        return self._wf_db_url

    def get_root_workflow_list (self, **table_args):
        """
        Get basic information about all work-flows running, on all databases. This is for the index page.
        Returns a list of work-flows.
        """
        self._workflows = []
        
        # Now, let's try to access the database
        try:
            all_workflows = queries.MasterDatabase (self._master_db_url)
            count, filtered, workflows = all_workflows.get_all_workflows (**table_args)
        
            if workflows:
                self._workflows.extend (workflows)
    
            if len (self._workflows) == 0:
                # Throw no work-flows found error.
                raise NoWorkflowsFoundError (count=count, filtered=filtered)
            
            self.__update_timestamp ()
            # Try removing Flask references here.
            self.__update_label_link ()
            
            counts = all_workflows.get_workflow_counts ()
            return (count, filtered, self._workflows, counts)
            
        finally:
            Dashboard.close (all_workflows)
         
    def __update_timestamp (self):
        for workflow in self._workflows:
            workflow.timestamp = strftime ("%a, %d %b %Y %H:%M:%S", localtime (workflow.timestamp))
    
    def __update_label_link (self):
        for workflow in self._workflows:
            workflow.dax_label = "<a href='" + url_for ('workflow', root_wf_id=workflow.wf_id, wf_uuid=workflow.wf_uuid) + "'>" + workflow.dax_label + "</a>"

    def workflow_stats (self):
        try:
            workflow = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), False)
            workflow.initialize (root_wf_id = self._wf_id)
            individual_stats = self._workflow_stats (workflow)
            
            workflow2 = stampede_statistics.StampedeStatistics (self.__get_wf_db_url ())
            workflow2.initialize (self._root_wf_uuid)
            
            all_stats = self._workflow_stats (workflow2)
            
            return { 'individual' : individual_stats, 'all' : all_stats }
        
        finally:
            Dashboard.close (workflow)
            Dashboard.close (workflow2)

    def _workflow_stats (self, workflow):
            # tasks
            tasks = {}
            workflow.set_job_filter('nonsub')
            tasks ['total_tasks'] = int (workflow.get_total_tasks_status())
            tasks ['total_succeeded_tasks'] = int (workflow.get_total_succeeded_tasks_status(False))
            tasks ['total_failed_tasks'] = int (workflow.get_total_failed_tasks_status())
            tasks ['total_unsubmitted_tasks'] = tasks ['total_tasks'] - (tasks ['total_succeeded_tasks'] + tasks ['total_failed_tasks'])
            tasks ['total_task_retries'] =  int (workflow.get_total_tasks_retries())
            tasks ['total_task_invocations'] = tasks ['total_succeeded_tasks'] + tasks ['total_failed_tasks'] + tasks ['total_task_retries']
            
            # job status
            jobs = {}
            workflow.set_job_filter('nonsub')
            jobs ['total_jobs'] = int (workflow.get_total_jobs_status())
            jobs ['total_succeeded_jobs'] = int (workflow.get_total_succeeded_jobs_status())
            jobs ['total_failed_jobs'] = int (workflow.get_total_failed_jobs_status())
            jobs ['total_unsubmitted_jobs'] = jobs ['total_jobs'] - (jobs ['total_succeeded_jobs'] + jobs ['total_failed_jobs'])
            jobs ['total_job_retries'] = int (workflow.get_total_jobs_retries())
            jobs ['total_job_invocations'] = jobs ['total_succeeded_jobs'] + jobs ['total_failed_jobs'] + jobs ['total_job_retries']
            
            # sub workflow
            wfs = {}
            workflow.set_job_filter('subwf')
            wfs ['total_sub_wfs'] = int (workflow.get_total_jobs_status())
            wfs ['total_succeeded_sub_wfs'] = int (workflow.get_total_succeeded_jobs_status())
            wfs ['total_failed_sub_wfs'] = int (workflow.get_total_failed_jobs_status())
            wfs ['total_unsubmitted_sub_wfs'] = wfs ['total_sub_wfs'] - (wfs ['total_succeeded_sub_wfs'] + wfs ['total_failed_sub_wfs'])
            wfs ['total_sub_wfs_retries'] = int (workflow.get_total_jobs_retries())
            wfs ['total_sub_wfs_invocations'] = wfs ['total_succeeded_sub_wfs'] + wfs ['total_failed_sub_wfs'] + wfs ['total_sub_wfs_retries']
            
            return [tasks, jobs, wfs]
        
    def job_breakdown_stats (self):
        try:
            workflow = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), True)
            workflow.initialize (root_wf_id = self._wf_id)
            content = []
            for t in workflow.get_transformation_statistics():
                content.append ([t.transformation, int(t.count), int(t.success), 
                           int(t.failure), float(t.min), float(t.max), float(t.avg), float(t.sum)])

            return content
        
        finally:
            Dashboard.close (workflow)
    
    def job_stats (self):
        try:
            workflow = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), False)
            workflow.initialize (root_wf_id = self._wf_id)
            workflow.set_job_filter('all')
            content = []
            
            for job in workflow.get_job_statistics ():
                
                kickstart = '0' if job.kickstart == None else float (job.kickstart)
                multiplier_factor = '0' if job.multiplier_factor == None else int (job.multiplier_factor)
                kickstart_multi = '0' if job.kickstart_multi == None else float (job.kickstart_multi)
                remote_cpu_time = '0' if job.remote_cpu_time == None else float (job.remote_cpu_time)
                post_time = '0' if job.post_time == None else float (job.post_time)
                condor_q_time = '0' if job.condor_q_time == None else float (job.condor_q_time)
                resource_delay = '0' if job.resource_delay == None else float (job.resource_delay)
                runtime = '0' if job.runtime == None else float (job.runtime)
                seqexec = '-' if job.seqexec == None else float (job.seqexec)

                seqexec_delay = '-'
                if job.seqexec is not None and job.kickstart is not None:
                    seqexec_delay = (float (job.seqexec) - float (job.kickstart))
                
                content.append ([job.job_name, 1, job.site, kickstart, multiplier_factor, kickstart_multi, 
                                 remote_cpu_time, post_time, condor_q_time, 
                                 resource_delay, runtime, seqexec, seqexec_delay, 
                                 utils.raw_to_regular (job.exit_code), job.host_name])

            return content
        
        finally:
            Dashboard.close (workflow)
            
    def plots_gantt_chart (self):
        try:
            #Expand has to be set to false. The method does not provide information when expand set to True.
            workflow = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), False)
            workflow.initialize (self._root_wf_uuid)
            gantt_chart = workflow.get_job_states ()

            return gantt_chart
        finally:
            Dashboard.close (workflow)
            
    def plots_time_chart (self, wf_id, time_filter='hour'):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            details = workflow.get_workflow_information ()
            
            workflow_plots = stampede_statistics.StampedeStatistics (self.__get_wf_db_url ())
            workflow_plots.initialize (details.wf_uuid)
            
            workflow_plots.set_job_filter ('nonsub')
            workflow_plots.set_time_filter (time_filter)
            workflow_plots.set_transformation_filter (exclude=['condor::dagman'])
            
            job, invocation = workflow_plots.get_jobs_run_by_time (), workflow_plots.get_invocation_by_time ()
            
            for j in job:
                Utils.hour_multiplier (j)
            
            for i in invocation:
                Utils.hour_multiplier (i)
            
            return job, invocation
        
        finally:
            Dashboard.close (workflow)
            Dashboard.close (workflow_plots)

    def plots_transformation_statistics (self, wf_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            details = workflow.get_workflow_information ()
            
            workflow_plots = stampede_statistics.StampedeStatistics (self.__get_wf_db_url ())
            workflow_plots.initialize (details.wf_uuid)
            
            workflow_plots.set_job_filter ('nonsub')
            workflow_plots.set_time_filter ('hour')
            workflow_plots.set_transformation_filter (exclude=['condor::dagman'])
            
            dist = workflow_plots.get_transformation_statistics ()
            
            return dist
        
        finally:
            Dashboard.close (workflow)
            Dashboard.close (workflow_plots)
    
    def get_workflow_information (self, wf_id=None, wf_uuid=None):
        """
        Get work-flow specific information. This is when user click on a work-flow link.
        Returns a work-flow object.
        """
        try:
            if not wf_id and not wf_uuid:
                raise ValueError, 'Workflow ID or Workflow UUID is required'
    
            workflow = None 
            workflow_statistics = None
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id=wf_id, wf_uuid=wf_uuid)

            details = self._get_workflow_details (workflow)
            job_counts = self._get_workflow_job_counts (workflow)
            
            #workflow_statistics = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), expand_workflow=(details.root_wf_id ==  details.wf_id))
            workflow_statistics = stampede_statistics.StampedeStatistics (self.__get_wf_db_url (), expand_workflow=True)
            workflow_statistics.initialize (details.wf_uuid)
            
            statistics = {}
            
            statistics.update (self._get_workflow_summary_times (workflow_statistics))
            
            #if details.root_wf_id ==  details.wf_id:
            statistics.update (self._get_workflow_summary_counts (workflow_statistics))
            
            return job_counts, details, statistics
        
        finally:
            Dashboard.close (workflow)
            Dashboard.close (workflow_statistics)

    def _get_workflow_details (self, workflow):
        return workflow.get_workflow_information ()
    
    def _get_workflow_job_counts (self, workflow):
        return workflow.get_workflow_job_counts ()

    def _get_workflow_summary_times (self, workflow):
        statistics = {}
        
        workflow_states_list = workflow.get_workflow_states ()
        statistics ['wall-time'] = float (stats_utils.get_workflow_wall_time (workflow_states_list))
        statistics ['cum-time'] = float (workflow.get_workflow_cum_job_wall_time ())
        statistics ['job-cum-time'] = float (workflow.get_submit_side_job_wall_time ())
        
        return statistics
    
    def _get_workflow_summary_counts (self, workflow):
        statistics = {}
        
        workflow.set_job_filter ('nonsub')
        statistics ['total-jobs'] = workflow.get_total_jobs_status()

        statistics ['successful-jobs'] = workflow.get_total_succeeded_jobs_status()
        statistics ['failed-jobs'] = workflow.get_total_failed_jobs_status()
        statistics ['unsubmitted-jobs'] = statistics ['total-jobs'] - (statistics ['successful-jobs'] + statistics ['failed-jobs'])
        statistics ['job-retries'] = workflow.get_total_jobs_retries()
        statistics ['job-instance-retries'] = statistics ['successful-jobs'] + statistics ['failed-jobs'] + statistics ['job-retries']
        
        return statistics
    
    def get_job_information (self, wf_id, job_id):
        """
        Get job specific information. This is when user click on a job link, on the work-flow details page.
        Returns a Job object.
        """
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            job_details = workflow.get_job_information (job_id)
            return job_details
        except NoResultFound:
            return None
        finally:
            Dashboard.close (workflow)
        
    def get_failed_jobs (self, wf_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id=wf_id)
            failed_jobs = workflow.get_failed_jobs ()
            return failed_jobs
        finally:
            Dashboard.close (workflow)
            
    def get_successful_jobs (self, wf_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id=wf_id)
            successful_jobs = workflow.get_successful_jobs ()
            return successful_jobs
        finally:
            Dashboard.close (workflow)
            
    def get_running_jobs (self, wf_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id=wf_id)
            running_jobs = workflow.get_other_jobs ()
            return running_jobs
        finally:
            Dashboard.close (workflow)

    def get_sub_workflows (self, wf_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id=wf_id)
            sub_workflows = workflow.get_sub_workflows ()
            return sub_workflows
        finally:
            Dashboard.close (workflow)
    
    def get_stdout (self, wf_id, job_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            stdout = workflow.get_stdout (job_id)
            return stdout
        finally:
            Dashboard.close (workflow)

    def get_successful_job_invocation (self, wf_id, job_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            successful_invocations = workflow.get_successful_job_invocations (job_id)
            return successful_invocations
        finally:
            Dashboard.close (workflow)

    def get_failed_job_invocation (self, wf_id, job_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            failed_invocations = workflow.get_failed_job_invocations (job_id)
            return failed_invocations
        finally:
            Dashboard.close (workflow)
    
    def get_stderr (self, wf_id, job_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            stderr = workflow.get_stderr (job_id)
            return stderr
        finally:
            Dashboard.close (workflow)
            
    def get_invocation_information (self, wf_id, job_id, task_id):
        try:
            workflow = queries.WorkflowInfo (self.__get_wf_db_url (), wf_id)
            invocation = workflow.get_invocation_information (job_id, task_id)
            invocation.start_time = strftime ("%a, %d %b %Y %H:%M:%S", localtime (invocation.start_time))
            return invocation
        finally:
            Dashboard.close (workflow)
