"""
Library to generate statistics from the new Stampede 3.1 backend.

Usage::
 stats = StampedeStatistics(connString='sqlite:///montage.db')
 stats.initialize('unique_wf_uuid')
 stats.set_job_filter('dax')
 print stats.get_total_jobs_status()
 print stats.get_total_jobs_statistics()
 stats.set_job_filter('dag')
 print stats.get_total_jobs_status()
 print stats.get_total_jobs_statistics()
 etc.
 
Constructor and initialize methods:

The constructor takes a required sqlalchemy connection string
as the first argument.  The stats class will default to returning
data in the "expanded workflow" mode.  To change this behavior
and only analyize a single workflow set the optional arg:

expand_workflow = False

along with the connection string argument.

The initialize method is called with a single argument - the wf_uuid
of the desired "root workflow" whether returning data in expanded
mode or not.  The method will return True or False if a query
exception is raised so the programmer can test for success before
calling the subsequent query methods.  This method is intended
to be called once per object.

Job filtering:

Jobs can be filtered using any of the strings in the jobtype ENUM, 
with the addition of the values 'all' and 'nonsub' which will
return all jobs and non-subworkflow jobs respectively.  If the 
filter is not explicitly set, it will default to the 'all' mode.

The desired filter can be set with the set_job_filter() method. After
setting this method, all subsequent calls to the query methods will
return results according to the filter.  This can be set and reset
as many times as the user desires.  There is an example of re/setting 
the job filter in the usage section above.  The query methods
will return different values after the filter is re/set.

Return values from methods:

The return value types will vary from method to method.  Most of
the methods will return a single integer or floating point number.

Methods which return rows from the DB (rather than just a number) 
will return a list which can be interacted with in one of two 
ways - either by array index (list of tuples) or by a named attr
(list of objects).  The two following methods of interacting with 
the same query results will both produce the same output:

Example::
 for row in s.get_job_kickstart():
     print row[0], row[1], row[2]
     print row.job_id, row.job_name, row.kickstart

Either syntax will work.  When using the named attribute method, the
attributes are the names of the columns/aliases in the SELECT 
stanza of the query.  If the row returned by the method is printed, 
it will display as a tuple of results per row.

Methods::
 get_sub_workflow_ids
 get_descendant_workflow_ids
 get_total_jobs_status
 get_total_succeeded_jobs_status
 get_total_failed_jobs_status
 get_total_unknown_jobs_status
 get_total_tasks_status
 get_total_succeeded_tasks_status
 get_total_failed_tasks_status
 get_total_jobs_statistics
 get_total_succeeded_jobs_statistics
 get_total_failed_jobs_statistics
 get_total_tasks_statistics
 get_total_succeeded_tasks_statistics
 get_total_failed_tasks_statistics
 get_workflow_wall_time
 get_workflow_cum_job_wall_time
 get_submit_side_job_wall_time
 get_job_name
 get_job_site
 get_job_kickstart
 get_job_runtime
 get_job_seqexec
 get_job_seqexec_delay
 get_condor_q_time
 get_resource_delay
 get_dagman_delay
 get_post_time
 get_transformation_statistics
 
Methods listed in order of query list on wiki.

https://confluence.pegasus.isi.edu/display/pegasus/Pegasus+statistics+python+version
"""
__rcsid__ = "$Id: stampede_statistics.py 28031 2011-05-26 19:47:16Z mgoode $"
__author__ = "Monte Goode"

import decimal

from netlogger.analysis.modules._base import SQLAlchemyInit
from netlogger.analysis.schema.stampede_schema import *
from netlogger.nllog import DoesLogging, get_logger

class StampedeStatistics(SQLAlchemyInit, DoesLogging):
    def __init__(self, connString=None, expand_workflow=True):
        if connString is None:
            raise ValueError("connString is required")
        DoesLogging.__init__(self)
        SQLAlchemyInit.__init__(self, connString, initializeToPegasusDB)
        
        self._expand = expand_workflow
        
        self._root_wf_id = None
        self._root_wf_uuid = None
        self._filter_mode = None
        
        self._wfs = []
        pass
    
    def initialize(self, root_wf_uuid):
        self.log.debug('initialize')
        self._root_wf_uuid = root_wf_uuid
        q = self.session.query(Workflow.wf_id).filter(Workflow.wf_uuid == self._root_wf_uuid)      
        
        try:
            self._root_wf_id = q.one().wf_id
        except orm.exc.MultipleResultsFound, e:
            self.log.error('initialize',
                msg='Multiple results found for wf_uuid: %s' % root_wf_uuid)
            return False
        except orm.exc.NoResultFound, e:
            self.log.error('initialize',
                msg='No results found for wf_uuid: %s' % root_wf_uuid)
            return False
        
        if self._expand:
            q = self.session.query(Workflow.wf_id).filter(Workflow.root_wf_id == self._root_wf_id)
            for row in q.all():
                self._wfs.append(row.wf_id)
        else:
            self._wfs.append(self._root_wf_id)
        # Initialize filter with default value
        self.set_job_filter()
        return True
        
    def set_job_filter(self, filter='all'):
        modes = ['all', 'nonsub', 'dax', 'dag', 'compute', 'stage-in-tx', 
                'stage-out-tx', 'registration', 'inter-site-tx', 'create-dir', 
                'staged-compute', 'cleanup', 'chmod']
        try:
            modes.index(filter)
            self._filter_mode = filter
            self.log.debug('set_job_filter', msg='Setting filter to: %s' % filter)
        except:
            self._filter_mode = 'all'
            self.log.error('set_job_filter', msg='Unknown job filter %s - setting to any' % filter)
            
    #
    # Pulls information about sub workflows
    #
            
    def get_sub_workflow_ids(self):
        """
        Returns info on child workflows only.
        """
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid)
        q = q.filter(Workflow.parent_wf_id == self._root_wf_id)
        return q.all()
        
    def get_descendant_workflow_ids(self):
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid)
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Workflow.wf_id != self._root_wf_id)
        return q.all()
            
    #
    # Status of initially planned wf components.
    #
        
    def _get_job_filter(self):
        filters = {
            'all': None,
            'nonsub': not_(self._dax_or_dag_cond()),
            'dax': Job.type_desc == 'dax',
            'dag': Job.type_desc == 'dag',
            'compute': Job.type_desc == 'compute',
            'stage-in-tx': Job.type_desc == 'stage-in-tx',
            'stage-out-tx': Job.type_desc == 'stage-out-tx',
            'registration': Job.type_desc == 'registration',
            'inter-site-tx': Job.type_desc == 'inter-site-tx',
            'create-dir': Job.type_desc == 'create-dir',
            'staged-compute': Job.type_desc == 'staged-compute',
            'cleanup': Job.type_desc == 'cleanup',
            'chmod': Job.type_desc == 'chmod',
        }
        return filters[self._filter_mode]
        
    def _max_job_seq_subquery(self):
        """
        Creates the following subquery that is used in 
        several queries:
        and jb_inst.job_submit_seq  = (
            select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
            )
        """
        JobInstanceSub = orm.aliased(JobInstance)
        sub_q = self.session.query(func.max(JobInstanceSub.job_submit_seq).label('max_id'))
        sub_q = sub_q.filter(JobInstanceSub.job_id == JobInstance.job_id).correlate(JobInstance)
        sub_q = sub_q.group_by(JobInstanceSub.job_id).subquery()
        return sub_q
        
    def _dax_or_dag_cond(self):
        return or_(Job.type_desc == 'dax', Job.type_desc == 'dag')
        
    def get_total_jobs_status(self):
        """
        select
        (    
            select count(*)  from job as jb where jb.wf_id in (1,2,3) and not 
                        (jb.type_desc = 'dax'or jb.type_desc = 'dag')
        )
        +
        (
            select count(*) from
            (
                select jb_inst.job_id from job_instance as jb_inst , job as jb  
                where jb.wf_id in (1,2,3)
                and jb_inst.job_id = jb.job_id
                and (jb.type_desc ='dax' or jb.type_desc ='dag' )
                and jb_inst.subwf_id is  null
                and jb_inst.job_submit_seq  = (
                    select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
                    )
            )
        ) as total_jobs
        
        """
        if not self._expand:
            q = self.session.query(Job)
            q = q.filter(Job.wf_id.in_(self._wfs))
            if self._get_job_filter() is not None:
                q = q.filter(self._get_job_filter())
            return q.count()
        else:
            q = self.session.query(Job.job_id)
            q = q.filter(Job.wf_id.in_(self._wfs))
            q = q.filter(not_(self._dax_or_dag_cond()))
            job_count =  q.count()
        
            sub_q = self._max_job_seq_subquery()
        
            q = self.session.query(JobInstance.job_id)
            q = q.filter(Job.wf_id.in_(self._wfs))
            q = q.filter(JobInstance.job_id == Job.job_id)
            q = q.filter(JobInstance.subwf_id == None)
            q = q.filter(JobInstance.job_submit_seq == sub_q.as_scalar())
            q = q.filter(self._dax_or_dag_cond())
            job_instance_count = q.count()
            
            return job_count + job_instance_count
             
    def get_total_succeeded_jobs_status(self):
        """
        select DISTINCT count(jb.job_id)
            from
            job as jb,
            job_instance as jb_inst,
            jobstate as jb_state
            where  jb.wf_id in(
            1,2,3
            )
            and jb.job_id = jb_inst.job_id
            and not (jb.type_desc ='dax' or jb.type_desc ='dag')
            and jb_inst.job_instance_id = jb_state.job_instance_id
            and jb_state.state ='JOB_SUCCESS'
        """
        q = self.session.query(Job.job_id).distinct()
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.filter(Jobstate.state == 'JOB_SUCCESS')
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            q = q.filter(not_(self._dax_or_dag_cond()))
        
        return q.count()
        
    def get_total_failed_jobs_status(self):
        """
        select count(*) from
        (
            select jb_inst.job_instance_id
            from job as jb, job_instance as jb_inst , jobstate as jb_state
            where jb_inst.job_submit_seq  = (
                select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
            )
            and jb.wf_id in (1,2,3)    
            and jb.job_id = jb_inst.job_id
            and jb_inst.job_instance_id = jb_state.job_instance_id
            and (
                      (not (jb.type_desc ='dax' or jb.type_desc ='dag'))
                or
                      ((jb.type_desc ='dax' or jb.type_desc ='dag') and jb_inst.subwf_id is NULL)
                     )
            and jb_state.state in ('JOB_FAILURE')
        )
        """
        sub_q = self._max_job_seq_subquery()
        
        q = self.session.query(JobInstance.job_instance_id)
        q = q.filter(JobInstance.job_submit_seq == sub_q.as_scalar())
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.filter(Jobstate.state.in_(['JOB_FAILURE'])) # why in and not == ?
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        
        return q.count()
        
    def _query_jobstate_for_instance(self, states):
        """
        The states arg is a list of strings.
        Returns a list of job_instance_id(s).
        """
        q = self.session.query(Jobstate.job_instance_id, JobInstance.job_instance_id)
        q = q.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        q = q.filter(Jobstate.state.in_(states))
        
        instance_ids = []
        
        for row in q.all():
            instance_ids.append(row[0])
        return instance_ids
        
    def get_total_unknown_jobs_status(self):
        """
        select count(*) from job_instance jb_inst , job as jb
        where jb_inst.job_submit_seq  = (
                select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
            )
        and jb_inst.job_instance_id in
            (
            select js.job_instance_id from jobstate as js
            where js.job_instance_id = jb_inst.job_instance_id
            and js.state = 'SUBMIT'
            )
        and jb_inst.job_instance_id not in
            (
            select js.job_instance_id from jobstate as js
            where js.job_instance_id = jb_inst.job_instance_id
            and js.state in ( 'JOB_SUCCESS', 'JOB_FAILURE')
        )
        and jb_inst.job_id  = jb.job_id
        and jb.wf_id in (
                1,2,3
        )
        and not (jb.type_desc ='dax' or jb.type_desc ='dag' )
        """
        submits = self._query_jobstate_for_instance(['SUBMIT'])
        jobstops = self._query_jobstate_for_instance(['JOB_SUCCESS', 'JOB_FAILURE'])
        
        sub_q = self._max_job_seq_subquery()
        
        q = self.session.query(Job, JobInstance)
        q = q.filter(JobInstance.job_submit_seq == sub_q.as_scalar())
        q = q.filter(JobInstance.job_instance_id.in_(submits))
        q = q.filter(not_(JobInstance.job_instance_id.in_(jobstops)))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            q = q.filter(not_(self._dax_or_dag_cond()))
        
        return q.count()
        
    def get_total_tasks_status(self):
        """
        select count(*) from task where wf_id in (
            1,2,3
           )
        """
        return self.session.query(Task).filter(Task.wf_id.in_(self._wfs)).count()
        
    def _base_task_status_query(self):
        """
        select count(*) from
        task as tk,
        job_instance as jb_inst,
        job as jb,
        invocation as invoc
        where invoc.wf_id in (
            1,2,3
         )
        and jb_inst.job_submit_seq  = (
                select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
            )
        and tk.wf_id in (
             1,2,3
        )
        and  jb.job_id = jb_inst.job_id
        and jb_inst.job_instance_id = invoc.job_instance_id
        and tk.abs_task_id = invoc.abs_task_id
        and tk.wf_id = invoc.wf_id
        and invoc.exitcode = 0
        """
        sub_q = self._max_job_seq_subquery()
        
        q = self.session.query(Job, JobInstance, Task, Invocation)
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.filter(JobInstance.job_submit_seq == sub_q.as_scalar())
        q = q.filter(Task.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter(Task.abs_task_id == Invocation.abs_task_id)
        q = q.filter(Task.wf_id == Invocation.wf_id)
        return q
        
    def get_total_succeeded_tasks_status(self):
        q = self._base_task_status_query()
        q = q.filter(Invocation.exitcode == 0)
        return q.count()
        
    def get_total_failed_tasks_status(self):
        q = self._base_task_status_query()
        q = q.filter(Invocation.exitcode != 0)
        return q.count()
        
    #
    # Statistics of actually run wf components.
    #
    
    def get_total_jobs_statistics(self):
        """
        select count(*)  as total_jobs
        from
        job_instance as jb_inst ,
        job as jb
        where
        jb_inst.job_id  = jb.job_id
        and jb.wf_id in (
            1,2,3
        )  
        and (
              (not (jb.type_desc ='dax' or jb.type_desc ='dag'))
            or
              ((jb.type_desc ='dax' or jb.type_desc ='dag') and jb_inst.subwf_id is NULL)
           )
        """
        q = self.session.query(Job, JobInstance)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        
        return q.count()
        
    def get_total_succeeded_jobs_statistics(self):
        """
        select DISTINCT count(jb.job_id)
            from
            job as jb,
            job_instance as jb_inst,
            jobstate as jb_state
            where  jb.wf_id in(
            1,2,3
            )
            and jb.job_id = jb_inst.job_id
            and not (jb.type_desc ='dax' or jb.type_desc ='dag')
            and jb_inst.job_instance_id = jb_state.job_instance_id
            and jb_state.state ='JOB_SUCCESS'
        """
        q = self.session.query(Job.job_id).distinct()
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.filter(Jobstate.state == 'JOB_SUCCESS')
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            q = q.filter(not_(self._dax_or_dag_cond()))

        return q.count()
        
    def get_total_failed_jobs_statistics(self):
        """
        select count(*) as job_failure
        from
        jobstate as jb_state ,
        job_instance jb_inst,
        job as jb
        where  jb.wf_id in(
            1,2,3
              )
        and jb_state.job_instance_id = jb_inst.job_instance_id
        and jb.job_id = jb_inst.job_id
        and jb_state.state = 'JOB_FAILURE'
        and (
              (not (jb.type_desc ='dax' or jb.type_desc ='dag'))
            or
              ((jb.type_desc ='dax' or jb.type_desc ='dag') and jb_inst.subwf_id is NULL)
           )
        """
        q = self.session.query(Job, JobInstance, Jobstate)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Jobstate.state == 'JOB_FAILURE')
        # jobtype filtering
        if not self._expand and self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        else:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        
        return q.count()
        
        
    def _base_task_statistics_query(self):
        q = self.session.query(Invocation)
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.filter(Invocation.task_submit_seq >= 0)
        return q
        
    def get_total_tasks_statistics(self):
        """
        select count(*) from invocation as invoc where invoc.task_submit_seq >=0 and invoc.wf_id in  (
              1,2,3
         )
        """
        q = self._base_task_statistics_query()
        return q.count()
        
    def get_total_succeeded_tasks_statistics(self):
        """
        select count(*) as succeeded_tasks
        from
        invocation as invoc
        where
        invoc.wf_id in  (
            1,2,3
        )
        and invoc.exitcode = 0
        and invoc.task_submit_seq >=0
        """
        q = self._base_task_statistics_query()
        q = q.filter(Invocation.exitcode == 0)
        return q.count()
        
    def get_total_failed_tasks_statistics(self):
        """
        select count(*) as failed_tasks
        from
        invocation as invoc
        where
        invoc.wf_id in  (
            1,2,3
        )
        and invoc.exitcode <> 0
        and invoc.task_submit_seq >=0
        """
        q = self._base_task_statistics_query()
        q = q.filter(Invocation.exitcode != 0)
        return q.count()
        
    #
    # Run statistics
    #
    
    def get_workflow_wall_time(self):
        """
        select ws.wf_id,
        sum(case when (ws.state == 'WORKFLOW_TERMINATED') then ws.timestamp end)
        -
        sum (case when (ws.state == 'WORKFLOW_STARTED') then ws.timestamp end) as duration
        from workflowstate ws
        group by ws.wf_id
        """
        q = self.session.query(
            Workflowstate.wf_id,
            (
                func.sum(case([(Workflowstate.state == 'WORKFLOW_TERMINATED', Workflowstate.timestamp)]))
                -
                func.sum(case([(Workflowstate.state == 'WORKFLOW_STARTED', Workflowstate.timestamp)]))
            ).label('duration')
        ).filter(Workflowstate.wf_id.in_(self._wfs)).group_by(Workflowstate.wf_id)
        
        return q.all()
        
    def get_workflow_cum_job_wall_time(self):
        """
        select sum(remote_duration) from invocation as invoc 
           where  invoc.task_submit_seq >=0 and invoc.wf_id in(
              1,2,3
           )
        """
        q = self.session.query(func.sum(Invocation.remote_duration))
        q = q.filter(Invocation.task_submit_seq >= 0)
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        return q.first()[0]
        
    def get_submit_side_job_wall_time(self):
        """
        select sum(local_duration) from job_instance as jb_inst , job as jb where
        jb_inst.job_id  = jb.job_id
        and jb.wf_id in (
            1,2,3
        )
        and (
              (not (jb.type_desc ='dax' or jb.type_desc ='dag'))
            or
              ((jb.type_desc ='dax' or jb.type_desc ='dag') and jb_inst.subwf_id is NULL)
           )
           
        """
        q = self.session.query(func.sum(JobInstance.local_duration).label('wall_time'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        if self._expand:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        
        return q.first().wall_time
        
    #
    # Job Statistics
    #
    
    def get_job_name(self):
        """
        select jb.job_id, jb.exec_job_id as job_name
         from
         job as jb,
         job_instance as jb_inst
         where
         jb_inst.job_id = jb.job_id
         and jb.wf_id = 3
         group by jb.job_id
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, Job.exec_job_id.label('job_name'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs)).group_by(Job.job_id)
        return q.all()
        
    def get_job_site(self):
        """
        select job_id , group_concat(site) as sites from
        (
        select DISTINCT jb.job_id as job_id , jb_inst.site as site
        from
        job as jb,
        job_instance as jb_inst
        where
        jb.wf_id = 3
        and jb_inst.job_id = jb.job_id
        ) group by job_id
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, func.group_concat(JobInstance.site).label('sites'))
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id).group_by(Job.job_id)
        return q.all()
        
    def get_job_kickstart(self):
        """
        select jb.job_id,
        jb.exec_job_id as job_name ,
        sum(remote_duration) as kickstart
        from
        job as jb,
        invocation as invoc,
        job_instance as jb_inst
        where
        jb_inst.job_id = jb.job_id
        and jb.wf_id = 3
        and invoc.wf_id =3
        and invoc.task_submit_seq >=0
        and invoc.job_instance_id = jb_inst.job_instance_id
        group by jb.job_id
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, Job.exec_job_id.label('job_name'),
                    func.sum(Invocation.remote_duration).label('kickstart'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.filter(Invocation.task_submit_seq >= 0)
        q = q.filter(Invocation.job_instance_id == JobInstance.job_instance_id)
        q = q.group_by(Job.job_id, Job.exec_job_id).order_by(Job.job_id)
        
        return q.all()
        
    def get_job_runtime(self):
        """
        select jb.job_id,
        sum(jb_inst.local_duration) as runtime
        from
        job as jb,
        job_instance as jb_inst
        where
        jb_inst.job_id = jb.job_id
        and jb.wf_id = 3
        group by jb.job_id
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, func.sum(JobInstance.local_duration).label('runtime'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.group_by(Job.job_id).order_by(Job.job_id)
        
        return q.all()
        
    def get_job_seqexec(self):
        """
        select jb.job_id,
        sum(jb_inst.cluster_duration) as seqexec
        from
        job as jb,
        job_instance as jb_inst
        where
        jb_inst.job_id = jb.job_id
        and jb.wf_id = 3
        group by jb.job_id
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, func.sum(JobInstance.cluster_duration).label('seqexec'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.group_by(Job.job_id).order_by(Job.job_id)
        
        return q.all()
        
    def get_job_seqexec_delay(self):
        """
        Seqexec Delay is Seqexec - Kickstart calculated above.
        
        select jb.job_id,
        (
         (
         select sum(jb_inst.cluster_duration)
         from
         job_instance as jb_inst
         where
         jb_inst.job_id = jb.job_id
         group by jb_inst.job_id
         )
        -
         (
         select sum(remote_duration)
         from
         invocation as invoc,
         job_instance as jb_inst
         where
         jb_inst.job_id = jb.job_id
         and invoc.wf_id =jb.wf_id
         and invoc.task_submit_seq >=0
         and invoc.job_instance_id = jb_inst.job_instance_id
         group by jb_inst.job_id
         )
        ) as seqexec_delay
        from
        job as jb
        where jb.wf_id in (1,2,3)
        and jb.clustered <>0
        """
        if self._expand:
            return []
        
        sq_1 = self.session.query(func.sum(JobInstance.cluster_duration))
        sq_1 = sq_1.filter(JobInstance.job_id == Job.job_id).correlate(Job)
        sq_1 = sq_1.group_by(JobInstance.job_id).subquery().as_scalar()
        
        sq_2 = self.session.query(func.sum(Invocation.remote_duration))
        sq_2 = sq_2.filter(JobInstance.job_id == Job.job_id).correlate(Job)
        sq_2 = sq_2.filter(Invocation.wf_id == Job.wf_id)
        sq_2 = sq_2.filter(Invocation.task_submit_seq >= 0)
        sq_2 = sq_2.filter(Invocation.job_instance_id == JobInstance.job_instance_id)
        sq_2 = sq_2.group_by(JobInstance.job_id).subquery().as_scalar()
        
        q = self.session.query(Job.job_id, cast(sq_1 - sq_2, Float))
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.clustered != 0)
        q = q.order_by(Job.job_id)
        
        return q.all()
    
    def get_condor_q_time(self):
        """
        select job_id, job_name, sum(cQTime) as condorQTime from
        (
        select jb.exec_job_id as job_name,  jb.job_id as job_id, jb_inst.job_instance_id ,
            (
                (select min(timestamp) from jobstate where job_instance_id = jb_inst.job_instance_id and (state = 'GRID_SUBMIT' or state = 'GLOBUS_SUBMIT' or state = 'EXECUTE'))
                -
                (select timestamp from jobstate where job_instance_id = jb_inst.job_instance_id and state = 'SUBMIT' )
            )   as cQTime
        from
        job_instance as jb_inst,
        job as jb
        where jb_inst.job_id =jb.job_id
        and jb.wf_id = 2
        )   group by job_id
        """
        if self._expand:
            return []
        sq_1 = self.session.query(func.min(Jobstate.timestamp).label('ts'))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_1 = sq_1.filter(or_(Jobstate.state == 'GRID_SUBMIT', 
                    Jobstate.state == 'GLOBUS_SUBMIT', Jobstate.state == 'EXECUTE')).correlate(JobInstance)
        sq_1 = sq_1.subquery().as_scalar()
        
        sq_2 = self.session.query(Jobstate.timestamp.label('ts'))
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_2 = sq_2.filter(Jobstate.state == 'SUBMIT').correlate(JobInstance)
        sq_2 = sq_2.subquery().as_scalar()
        
        q = self.session.query(Job.exec_job_id.label('job_name'), Job.job_id, JobInstance.job_instance_id,
            cast(sq_1 - sq_2, Float).label('cQTime'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(Job.job_id).subquery()
        
        main = self.session.query(q.c.job_id, q.c.job_name, func.sum(q.c.cQTime).label('condorQTime'))
        main = main.group_by(q.c.job_id)
        
        return main.all()
        
    def get_resource_delay(self):
        """
        select job_id, job_name, sum(rTime) as resourceTime from
        (
        select jb.exec_job_id as job_name,  jb.job_id as job_id, jb_inst.job_instance_id ,
            (
                     (select timestamp from jobstate where job_instance_id = jb_inst.job_instance_id and state = 'EXECUTE' )  
                -
                    (select timestamp from jobstate where job_instance_id = jb_inst.job_instance_id and (state = 'GRID_SUBMIT' or state ='GLOBUS_SUBMIT'))
            )   as rTime
        from
        job_instance as jb_inst,
        job as jb
        where jb_inst.job_id =jb.job_id
        and jb.wf_id = 2
        )   group by job_id
        """
        if self._expand:
            return []
        sq_1 = self.session.query(Jobstate.timestamp.label('ts'))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_1 = sq_1.filter(Jobstate.state == 'EXECUTE').correlate(JobInstance)
        sq_1 = sq_1.subquery().as_scalar()
        
        sq_2 = self.session.query(Jobstate.timestamp.label('ts'))
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_2 = sq_2.filter(or_(Jobstate.state == 'GRID_SUBMIT', 
                    Jobstate.state == 'GLOBUS_SUBMIT')).correlate(JobInstance)
        sq_2 = sq_2.subquery().as_scalar()
        
        q = self.session.query(Job.exec_job_id.label('job_name'), Job.job_id, JobInstance.job_instance_id,
            cast(sq_1 - sq_2, Float).label('rTime'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(Job.job_id).subquery()
        
        main = self.session.query(q.c.job_id, q.c.job_name, func.sum(q.c.rTime).label('resourceTime'))
        main = main.group_by(q.c.job_id)
        
        return main.all()
        
    def get_dagman_delay(self):
        """
        select jb.exec_job_id as job_name,  
        (
          (
           select min(timestamp)
           from jobstate
           where job_instance_id in 
             (select job_instance_id from job_instance as jb_inst where jb_inst.job_id = jb.job_id )  
           and state ='SUBMIT'
          )
          -
          (
           select max(timestamp)
           from jobstate
           where job_instance_id in 
              (select job_instance_id from job_instance as jb_inst where jb_inst.job_id in
                (
                  select
                  parent.job_id as parent_job_id
                  from
                  job as parent,
                  job as child,
                  job_edge as edge
                  where
                  edge.wf_id = 2
                  and parent.wf_id = 2
                  and child.wf_id = 2
                  and child.job_id = jb.job_id
                  and edge.parent_exec_job_id like parent.exec_job_id
                  and edge.child_exec_job_id like child.exec_job_id
                 )
               )
           and (state = 'POST_SCRIPT_TERMINATED' or state ='JOB_TERMINATED')
          )
        )   as dagmanDelay
        from
        job as jb where
        jb.wf_id =2
        """
        if self._expand:
            return []
        # topmost nested queries
        sq_1 = self.session.query(JobInstance.job_instance_id)
        sq_1 = sq_1.filter(JobInstance.job_id == Job.job_id).correlate(Job)
        sq_1 = sq_1.subquery()
        
        sq_2 = self.session.query(func.min(Jobstate.timestamp))
        sq_2 = sq_2.filter(Jobstate.job_instance_id.in_(sq_1))
        sq_2 = sq_2.filter(Jobstate.state == 'SUBMIT').subquery().as_scalar()

        # lower nested queries
        Parent = orm.aliased(Job)
        Child  = orm.aliased(Job)
        
        sq_3 = self.session.query(Parent.job_id.label('parent_job_id'))
        sq_3 = sq_3.filter(JobEdge.wf_id.in_(self._wfs))
        sq_3 = sq_3.filter(Parent.wf_id.in_(self._wfs))
        sq_3 = sq_3.filter(Child.wf_id.in_(self._wfs))
        sq_3 = sq_3.filter(Child.job_id == Job.job_id).correlate(Job)
        sq_3 = sq_3.filter(JobEdge.parent_exec_job_id.like(Parent.exec_job_id))
        sq_3 = sq_3.filter(JobEdge.child_exec_job_id.like(Child.exec_job_id))
        sq_3 = sq_3.subquery()
        
        sq_4 = self.session.query(JobInstance.job_instance_id)
        sq_4 = sq_4.filter(JobInstance.job_id.in_(sq_3)).subquery()
        
        sq_5 = self.session.query(func.max(Jobstate.timestamp))
        sq_5 = sq_5.filter(Jobstate.job_instance_id.in_(sq_4))
        sq_5 = sq_5.filter(or_(Jobstate.state == 'POST_SCRIPT_TERMINATED', Jobstate.state == 'JOB_TERMINATED'))
        sq_5 = sq_5.subquery().as_scalar()
        
        q = self.session.query(Job.job_id, Job.exec_job_id.label('job_name'), 
                        cast(sq_2 - sq_5, Float).label('dagmanDelay'))
        
        q = q.filter(Job.wf_id.in_(self._wfs))
        
        return q.all()
        
    def get_post_time(self):
        """
        select job_id, job_name, sum(pTime) as postTime from
        (
        select jb.exec_job_id as job_name,  jb.job_id as job_id, jb_inst.job_instance_id ,
            (
                     (select timestamp from jobstate where job_instance_id = jb_inst.job_instance_id and state = 'POST_SCRIPT_TERMINATED')
                -
                    (select max(timestamp) from jobstate  where job_instance_id = jb_inst.job_instance_id  and (state ='POST_SCRIPT_STARTED' or state ='JOB_TERMINATED'))
            )   as pTime
        from
        job_instance as jb_inst,
        job as jb
        where jb_inst.job_id =jb.job_id
        and jb.wf_id = 2
        ) group by job_id
        """
        if self._expand:
            return []
        sq_1 = self.session.query(Jobstate.timestamp.label('ts'))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_1 = sq_1.filter(Jobstate.state == 'POST_SCRIPT_TERMINATED').correlate(JobInstance)
        sq_1 = sq_1.subquery().as_scalar()
        
        sq_2 = self.session.query(func.max(Jobstate.timestamp).label('ts'))
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        sq_2 = sq_2.filter(or_(Jobstate.state == 'POST_SCRIPT_STARTED', 
                    Jobstate.state == 'JOB_TERMINATED')).correlate(JobInstance)
        sq_2 = sq_2.subquery().as_scalar()
        
        q = self.session.query(Job.exec_job_id.label('job_name'), Job.job_id, JobInstance.job_instance_id,
            cast(sq_1 - sq_2, Float).label('pTime'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(Job.job_id).subquery()
        
        main = self.session.query(q.c.job_id, q.c.job_name, func.sum(q.c.pTime).label('postTime'))
        main = main.group_by(q.c.job_id)
        
        return main.all()
        
    def get_transformation_statistics(self):
        """
        select transformation, count(*), 
        min(remote_duration) , max(remote_duration) , 
        avg(remote_duration)  , sum(remote_duration) 
        from invocation as invoc where invoc.wf_id = 3 group by transformation
        """
        q = self.session.query(Invocation.transformation, 
                func.count(Invocation.invocation_id).label('count'),
                func.min(Invocation.remote_duration).label('min'),  
                func.max(Invocation.remote_duration).label('max'),
                func.avg(Invocation.remote_duration).label('avg'), 
                func.sum(Invocation.remote_duration).label('sum'))
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.group_by(Invocation.transformation)
        
        return q.all()
        
if __name__ == '__main__':
    pass