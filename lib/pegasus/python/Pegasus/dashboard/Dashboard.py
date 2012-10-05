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

            details = workflow.get_workflow_information ()
            
            workflow_statistics = stampede_statistics.StampedeStatistics (self.__get_wf_db_url ())
            workflow_statistics.initialize (details.wf_uuid)
            
            job_counts = workflow.get_workflow_job_counts ()
            
            statistics = {}
            
            workflow_states_list = workflow_statistics.get_workflow_states ()
            
            statistics ['wall-time'] = stats_utils.get_workflow_wall_time (workflow_states_list)
            statistics ['cum-time'] = workflow_statistics.get_workflow_cum_job_wall_time ()
            
            workflow_statistics.set_job_filter ('nonsub')
            statistics ['total-jobs'] = workflow_statistics.get_total_jobs_status()

            statistics ['successful-jobs'] = workflow_statistics.get_total_succeeded_jobs_status()
            statistics ['failed-jobs'] = workflow_statistics.get_total_failed_jobs_status()
            statistics ['unsubmitted-jobs'] = statistics ['total-jobs'] - (statistics ['successful-jobs'] + statistics ['failed-jobs'])
            statistics ['job-retries'] = workflow_statistics.get_total_jobs_retries()
            statistics ['job-instance-retries'] = statistics ['successful-jobs'] + statistics ['failed-jobs'] + statistics ['job-retries']
            
            return job_counts, details, statistics
        
        finally:
            Dashboard.close (workflow)
            Dashboard.close (workflow_statistics)

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
