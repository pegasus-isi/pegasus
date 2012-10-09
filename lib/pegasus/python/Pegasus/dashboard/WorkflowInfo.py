"""
"""

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

__author__ = "Rajiv Mayani"

from netlogger.analysis.modules._base import SQLAlchemyInit
from netlogger.analysis.schema.schema_check import ErrorStrings, SchemaCheck, SchemaVersionError
from netlogger.analysis.schema.stampede_dashboard_schema import DashboardWorkflow, DashboardWorkflowstate, initializeToDashboardDB
from netlogger.analysis.schema.stampede_schema import *
from netlogger.nllog import DoesLogging

class MasterDatabase (SQLAlchemyInit, DoesLogging):
    
    def __init__(self, connString=None):
        if connString is None:
            raise ValueError("Connection string is required")
        DoesLogging.__init__(self)
        
        try:
            SQLAlchemyInit.__init__(self, connString, initializeToDashboardDB)
        except exceptions.OperationalError, e:
            self.log.error('init', msg='%s' % ErrorStrings.get_init_error(e))
            raise RuntimeError
    
    def close (self):
        self.log.debug ('close')
        self.disconnect ()
    
    def get_wf_db_url (self, wf_id):
        """
        Given a work-flow UUID, query the master database to get the connection URL for the work-flow's STAMPEDE database. 
        """
        
        w = orm.aliased(DashboardWorkflow, name='w')
        
        q = self.session.query (w.db_url)
        q = q.filter (w.wf_id == wf_id)
        
        return q.one ().db_url
    
    def get_wf_id_url (self, root_wf_id):
        """
        Given a work-flow UUID, query the master database to get the connection URL for the work-flow's STAMPEDE database. 
        """
        
        w = orm.aliased(DashboardWorkflow, name='w')
        
        q = self.session.query (w.wf_id, w.wf_uuid, w.db_url)
        q = q.filter (w.wf_id == root_wf_id)
        
        q = q.one ()
        
        return q.wf_id, q.wf_uuid, q.db_url

    def get_all_workflows(self, **table_args):
        """
        SELECT w.*, ws.*
         FROM   workflow w 
                     JOIN workflowstate ws ON w.wf_id = ws.wf_id 
                JOIN (SELECT wf_id, max(timestamp) time 
                            FROM   workflowstate 
                            GROUP  BY wf_id) t ON ws.wf_id = t.wf_id 
                           AND ws.timestamp = t.time 
         WHERE  w.wf_id = ws.wf_id 
        AND ws.wf_id = t.wf_id 
        AND ws.timestamp = t.time; 
        """
        
        w = orm.aliased(DashboardWorkflow, name='w')
        ws = orm.aliased(DashboardWorkflowstate, name='ws')
        
        # Get last state change for each work-flow.
        qmax = self.session.query (DashboardWorkflowstate.wf_id, func.max (DashboardWorkflowstate.timestamp).label ('max_time'))
        qmax = qmax.group_by (DashboardWorkflowstate.wf_id)
        
        qmax = qmax.subquery ('max_timestamp')
        
        q = self.session.query (w.wf_id, w.wf_uuid, w.timestamp,
                                w.dag_file_name, w.submit_hostname,
                                w.submit_dir, w.planner_arguments,
                                w.user, w.grid_dn, w.planner_version,
                                w.dax_label, w.dax_version, w.db_url,
                                case([(ws.status == None, 'Running'),
                                      (ws.status == 0, 'Successful'),
                                      (ws.status != 0, 'Failed')], else_='Undefined').label ("state"))
        
        q = q.filter (w.wf_id == ws.wf_id)
        q = q.filter (ws.wf_id == qmax.c.wf_id)
        q = q.filter (ws.timestamp == qmax.c.max_time)
        
        # Get Total Count. Need this to pass to jQuery Datatable.
        count = q.count ()
        if count == 0:
            return (0, 0, [])
        
        if 'filter' in table_args:
            filter_text = '%' + table_args ['filter'] + '%'
            q = q.filter (or_ (w.dax_label.like (filter_text), w.submit_dir.like (filter_text), case([(ws.status == None, 'Running'), (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')], else_='Undefined').like (filter_text)))
        
        # Get Total Count. Need this to pass to jQuery Datatable.
        filtered = q.count ()
        
        if filtered == 0:
            return (count, 0, [])
        
        if 'sort-col-count' in table_args:
            for i in range (table_args ['sort-col-count']):
                
                if 'iSortCol_' + str(i) in table_args:
                    if 'sSortDir_' + str(i) in table_args and table_args ['sSortDir_' + str(i)] == 'asc':
                        i = table_args ['iSortCol_' + str(i)]

                        if i == 0:
                            q = q.order_by (w.dax_label)
                        elif i == 1:
                            q = q.order_by (w.submit_dir)
                        elif i == 2:
                            q = q.order_by (case([(ws.status == None, 'Running'), (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')], else_='Undefined'))
                        elif i == 3:
                            q = q.order_by (w.timestamp)
                        else:
                            raise ValueError, ('Invalid column (%s) in work-flow listing ' % i)
                    else:
                        i = table_args ['iSortCol_' + str(i)]

                        if i == 0:
                            q = q.order_by (desc (w.dax_label))
                        elif i == 1:
                            q = q.order_by (desc (w.submit_dir))
                        elif i == 2:
                            q = q.order_by (desc (case([(ws.status == None, 'Running'), (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')], else_='Undefined')))
                        elif i == 3:
                            q = q.order_by (desc (w.timestamp))
                        else:
                            raise ValueError, ('Invalid column (%s) in work-flow listing ' % i)

        else:
            # Default sorting order
            q = q.order_by (desc (w.timestamp))  
        
        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit (table_args ['limit'])
            q = q.offset (table_args ['offset'])

        return (count, filtered, q.all ())


    def get_workflow_counts (self):
        
        w = orm.aliased(DashboardWorkflow, name='w')
        ws = orm.aliased(DashboardWorkflowstate, name='ws')
        
        # Get last state change for each work-flow.
        qmax = self.session.query (DashboardWorkflowstate.wf_id, func.max (DashboardWorkflowstate.timestamp).label ('max_time'))
        qmax = qmax.group_by (DashboardWorkflowstate.wf_id)
        
        qmax = qmax.subquery ('max_timestamp')
        
        q = self.session.query (func.count (w.wf_id).label("total"),
                                func.sum (case([(ws.status == 0, 1)], else_=0)).label ("success"),
                                func.sum (case([(ws.status != 0, 1)], else_=0)).label ("fail"),
                                func.sum (case([(ws.status == None, 1)], else_=0)).label ("others")) 
        
        q = q.filter (w.wf_id == ws.wf_id)
        q = q.filter (ws.wf_id == qmax.c.wf_id)
        q = q.filter (ws.timestamp == qmax.c.max_time)
        
        tmp = q.one ()
        
        res = {}
        res ['success'] = tmp.success
        res ['total'] = tmp.total
        res ['fail'] = tmp.fail
        res ['others'] = tmp.others

        return res


class WorkflowInfo(SQLAlchemyInit, DoesLogging):

    def __init__(self, connString=None, wf_id=None, wf_uuid=None):
        if connString is None:
            raise ValueError("Connection string is required")
        DoesLogging.__init__(self)
        
        try:
            SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB)
        except exceptions.OperationalError, e:
            self.log.error('init', msg='%s' % ErrorStrings.get_init_error(e))
            raise RuntimeError
            
        # Check the schema version before proceeding.
        self.s_check = SchemaCheck(self.session)
        if not self.s_check.check_schema():
            raise SchemaVersionError

        self.initialize (wf_id, wf_uuid)
    
    def initialize (self, wf_id=None, wf_uuid=None):
       
        if not wf_id and not wf_uuid:
            raise ValueError, 'Workflow ID or Workflow UUID is required.'
        
        if wf_id:
            self._wf_id = wf_id
            return
        
        if wf_uuid:
            q = self.session.query (Workflow.wf_id)
            q = q.filter (Workflow.wf_uuid == wf_uuid)
            
            self._wf_id = q.one ().wf_id
            
    def get_workflow_information (self):
        
        qmax = self.session.query (func.max (Workflowstate.timestamp).label ('max_time'))
        qmax = qmax.filter (Workflowstate.wf_id == self._wf_id)
        
        qmax = qmax.subquery ('max_timestamp')
        
        ws = orm.aliased(Workflowstate, name='ws')
        w = orm.aliased(Workflow, name='w')
    
        q = self.session.query(w.wf_id, w.wf_uuid,
            w.parent_wf_id, w.root_wf_id, w.dag_file_name,
            w.submit_hostname, w.submit_dir, w.planner_arguments,
            w.user, w.grid_dn, w.planner_version,
            w.dax_label, w.dax_version, case([(ws.status == None, 'Running'),
                                      (ws.status == 0, 'Successful'),
                                      (ws.status != 0, 'Failed')], else_='Undefined').label ("state"), ws.timestamp)
        
        q = q.filter(w.wf_id == self._wf_id)
        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)
        
        return q.one ()

    def get_workflow_job_counts (self):
        
        qmax = self.__get_maxjss_subquery ()
        
        q = self.session.query (func.count (Job.wf_id).label("total"))
        q = q.add_column (func.sum (case([(JobInstance.exitcode == 0, 1)], else_=0)).label ("success"))
        q = q.add_column (func.sum (case([(JobInstance.exitcode == 0, case([(Job.type_desc == 'dag', 1), (Job.type_desc == 'dax', 1)], else_=0))], else_=0)).label ("success_workflow"))
        
        
        q = q.add_column (func.sum (case([(JobInstance.exitcode != 0, 1)], else_=0)).label ("fail"))
        q = q.add_column (func.sum (case([(JobInstance.exitcode != 0, case([(Job.type_desc == 'dag', 1), (Job.type_desc == 'dax', 1)], else_=0))], else_=0)).label ("fail_workflow"))
        
        q = q.add_column (func.sum (case([(JobInstance.exitcode == None, 1)], else_=0)).label ("others"))
        q = q.add_column (func.sum (case([(JobInstance.exitcode == None, case([(Job.type_desc == 'dag', 1), (Job.type_desc == 'dax', 1)], else_=0))], else_=0)).label ("others_workflow"))
        
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.job_id == JobInstance.job_id)
        q = q.filter (Job.job_id == qmax.c.job_id)
        q = q.filter (JobInstance.job_submit_seq == qmax.c.max_jss)
        
        tmp = q.one ()
        
        res = {}
        res ['success'] = tmp.success
        res ['success_workflow'] = tmp.success_workflow
        res ['total'] = tmp.total
        res ['fail'] = tmp.fail
        res ['fail_workflow'] = tmp.fail_workflow
        res ['others'] = tmp.others
        res ['others_workflow'] = tmp.others_workflow
        
        return res

    def get_job_information (self, job_id):

        qmax = self.__get_maxjss_subquery (job_id)
        
        q = self.session.query (Job.exec_job_id, Job.clustered, JobInstance.exitcode, JobInstance.stdout_file, JobInstance.stderr_file)
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.job_id == job_id)
        q = q.filter (Job.job_id == JobInstance.job_id)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        
        return q.one()
     
    def get_failed_jobs (self):

        qmax = self.__get_jobs_maxjss_sq ()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, Job.exec_job_id, JobInstance.exitcode)
        
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.type_desc != 'dax', Job.type_desc != 'dag')
        
        q = q.filter (Job.job_id == JobInstance.job_id)
        
        q = q.filter (Job.job_id == qmax.c.job_id)
        q = q.filter (JobInstance.job_submit_seq == qmax.c.max_jss)
        
        q = q.filter (JobInstance.exitcode != 0).filter (JobInstance.exitcode != None)
        
        q = q.group_by(JobInstance.job_id)

        return q.all()
    
    def get_successful_jobs (self):
        
        qmax = self.__get_jobs_maxjss_sq ()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, Job.exec_job_id, JobInstance.exitcode, JobInstance.local_duration, JobInstance.cluster_duration)
        q = q.add_column (case ([(Job.clustered == 1, JobInstance.cluster_duration)], else_=JobInstance.local_duration).label ("duration"))
        
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.type_desc != 'dax', Job.type_desc != 'dag')
        
        q = q.filter (Job.job_id == JobInstance.job_id)
        
        q = q.filter (Job.job_id == qmax.c.job_id)
        q = q.filter (JobInstance.job_submit_seq == qmax.c.max_jss)
        
        q = q.filter (JobInstance.exitcode == 0).filter (JobInstance.exitcode != None)
        
        q = q.group_by(JobInstance.job_id)

        return q.all()
    
    def get_other_jobs (self):
        
        qmax = self.__get_jobs_maxjss_sq ()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, Job.exec_job_id, JobInstance.exitcode, JobInstance.local_duration, JobInstance.cluster_duration)
        q = q.add_column (case ([(Job.clustered == 1, JobInstance.cluster_duration)], else_=JobInstance.local_duration).label ("duration"))
        
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.type_desc != 'dax', Job.type_desc != 'dag')
        
        q = q.filter (Job.job_id == JobInstance.job_id)
        
        q = q.filter (Job.job_id == qmax.c.job_id)
        q = q.filter (JobInstance.job_submit_seq == qmax.c.max_jss)
        
        q = q.filter (JobInstance.exitcode == None)
        
        q = q.group_by(JobInstance.job_id)

        return q.all()

    def __get_jobs_maxjss_sq (self):
        
        qmax = self.session.query(Job.job_id, func.max(JobInstance.job_submit_seq).label ('max_jss'))
        qmax = qmax.filter (Job.wf_id == self._wf_id)
        qmax = qmax.filter (Job.job_id == JobInstance.job_id)
        qmax = qmax.filter (Job.type_desc != 'dax', Job.type_desc != 'dag')
        qmax = qmax.group_by (Job.job_id)
        
        qmax = qmax.subquery ('allmaxjss')
        
        return qmax
    
    def get_sub_workflows (self):
        qmax = self.session.query (Workflowstate.wf_id, func.max (Workflowstate.timestamp).label ('max_time'))
        qmax = qmax.group_by (Workflowstate.wf_id)
        
        qmax = qmax.subquery ('max_timestamp')
        
        ws = orm.aliased (Workflowstate, name='ws')
        w = orm.aliased (Workflow, name='w')
    
        q = self.session.query (w.wf_id, w.wf_uuid, w.dax_label, case([(ws.status == None, 'Running'), (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')], else_='Undefined').label ("state"))
        
        q = q.filter (w.parent_wf_id == self._wf_id)
        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.wf_id == qmax.c.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)
        
        return q.all ()
    
    def get_stdout (self, job_id):
        jiq = orm.aliased(JobInstance, name='jii')
        qmax = self.session.query (JobInstance.job_instance_id, func.max (JobInstance.job_submit_seq))
        qmax = qmax.filter (Job.wf_id == self._wf_id)
        qmax = qmax.filter (Job.job_id == job_id)
        qmax = qmax.filter (Job.job_id == JobInstance.job_id).correlate(jiq)
        
        qmax = qmax.subquery ('maxjss')
        
        q = self.session.query (JobInstance.stdout_file, JobInstance.stdout_text)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        
        return q.one ()

    def get_stderr (self, job_id):
        jiq = orm.aliased(JobInstance, name='jii')
        qmax = self.session.query (JobInstance.job_instance_id, func.max (JobInstance.job_submit_seq))
        qmax = qmax.filter (Job.wf_id == self._wf_id)
        qmax = qmax.filter (Job.job_id == job_id)
        qmax = qmax.filter (Job.job_id == JobInstance.job_id).correlate(jiq)
        
        qmax = qmax.subquery ('maxjss')
        
        q = self.session.query (JobInstance.stderr_file, JobInstance.stderr_text)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        
        return q.one ()
    
    def get_successful_job_invocations (self, job_id):

        qmax = self.__get_maxjss_subquery (job_id)
        
        q = self.session.query (Job.exec_job_id, Invocation.abs_task_id, Invocation.exitcode, Invocation.remote_duration)
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.job_id == job_id)
        q = q.filter (Job.job_id == JobInstance.job_id)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        q = q.filter (JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter (Invocation.exitcode == 0)
        
        q = q.filter (or_(Invocation.abs_task_id != None, Invocation.task_submit_seq == 1))
        
        return q.all()

    def get_failed_job_invocations (self, job_id):

        qmax = self.__get_maxjss_subquery (job_id)
        
        q = self.session.query (Job.exec_job_id, Invocation.abs_task_id, Invocation.exitcode, Invocation.remote_duration)
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.job_id == job_id)
        q = q.filter (Job.job_id == JobInstance.job_id)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        q = q.filter (JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter (Invocation.exitcode != 0)
        
        q = q.filter (or_(Invocation.abs_task_id != None, Invocation.task_submit_seq == 1))
        
        return q.all()

    def __get_maxjss_subquery (self, job_id=None):
        
        jii = orm.aliased(JobInstance, name='jii')
        
        if job_id:
            
            qmax = self.session.query (JobInstance.job_instance_id, func.max (JobInstance.job_submit_seq))
            qmax = qmax.filter (Job.wf_id == self._wf_id)
            qmax = qmax.filter (Job.job_id == job_id)
            qmax = qmax.filter (Job.type_desc != 'dax', Job.type_desc != 'dag')
            qmax = qmax.filter (Job.job_id == JobInstance.job_id).correlate(jii)
            
            qmax = qmax.subquery ('maxjss')
        
        else:

            qmax = self.session.query (Job.job_id, func.max (JobInstance.job_submit_seq).label ('max_jss'))
            qmax = qmax.filter (Job.wf_id == self._wf_id)
            qmax = qmax.filter (Job.job_id == JobInstance.job_id).correlate(jii)
            
            qmax = qmax.group_by (Job.job_id)
            
            qmax = qmax.subquery ('maxjss')
            
        return qmax
    
    def get_invocation_information (self, job_id, task_id):

        qmax = self.__get_maxjss_subquery (job_id)
        
        q = self.session.query (Invocation.abs_task_id, Invocation.start_time, Invocation.remote_duration, Invocation.remote_cpu_time, Invocation.exitcode, Invocation.transformation, Invocation.executable, Invocation.argv)
        q = q.filter (Job.wf_id == self._wf_id)
        q = q.filter (Job.job_id == job_id)
        q = q.filter (Job.job_id == JobInstance.job_id)
        q = q.filter (JobInstance.job_instance_id == qmax.c.job_instance_id)
        q = q.filter (JobInstance.job_instance_id == Invocation.job_instance_id)
        
        if task_id == None:
            q = q.filter (Invocation.task_submit_seq == 1)
        else:
            q = q.filter (Invocation.abs_task_id == task_id)
        
        return q.one ()

    def close (self):
        self.log.debug ('close')
        self.disconnect ()
