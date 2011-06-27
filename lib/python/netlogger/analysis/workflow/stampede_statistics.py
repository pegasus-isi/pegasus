"""
Library to generate statistics from the new Stampede 3.1 backend.

Usage::
 stats = StampedeStatistics(connString='sqlite:///montage.db')
 stats.initialize('unique_wf_uuid')
 stats.set_job_filter('dax')
 print stats.get_total_jobs_status()
 print stats.get_total_succeeded_jobs_status()
 stats.set_job_filter('dag')
 print stats.get_total_jobs_status()
 print stats.get_total_succeeded_jobs_status()
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
 get_total_jobs_retries
 get_total_tasks_status
 get_total_succeeded_tasks_status
 get_total_failed_tasks_status
 get_total_tasks_retries
 get_workflow_states
 get_workflow_cum_job_wall_time
 get_submit_side_job_wall_time
 get_workflow_details
 get_workflow_retries
 get_job_statistics
 get_job_states
 get_job_instance_sub_wf_map
 get_failed_job_instances
 get_job_name
 get_job_site
 get_job_kickstart
 get_job_runtime
 get_job_seqexec
 get_condor_q_time
 get_resource_delay
 get_post_time
 get_transformation_statistics
 
Methods listed in order of query list on wiki.

https://confluence.pegasus.isi.edu/display/pegasus/Pegasus+Statistics+Python+Version+Modified
"""
__rcsid__ = "$Id: stampede_statistics.py 28106 2011-06-23 22:15:49Z mgoode $"
__author__ = "Monte Goode"

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
        self._job_filter_mode = None
        self._task_filter_mode = None
        
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
            if not len(self._wfs):
                self.log.error('initialize',
                    msg='Unable to expand wf_uuid: %s - not a root_wf_id?' % root_wf_uuid)
                return False
        else:
            self._wfs.append(self._root_wf_id)
            
        if not len(self._wfs):
            self.log.error('initialize',
                msg='No results found for wf_uuid: %s' % root_wf_uuid)
            return False
        
        # Initialize filters with default value
        self.set_job_filter()
        return True
        
    def set_job_filter(self, filter='all'):
        modes = ['all', 'nonsub', 'subwf', 'dax', 'dag', 'compute', 'stage-in-tx', 
                'stage-out-tx', 'registration', 'inter-site-tx', 'create-dir', 
                'staged-compute', 'cleanup', 'chmod']
        try:
            modes.index(filter)
            self._job_filter_mode = filter
            self.log.debug('set_job_filter', msg='Setting filter to: %s' % filter)
        except:
            self._job_filter_mode = 'all'
            self.log.error('set_job_filter', msg='Unknown job filter %s - setting to all' % filter)
                
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
    
    #
    # The following block of queries are documented here:
    # https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary
    # and
    # https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file
    #
    
    def _dax_or_dag_cond(self, JobO=Job):
        return or_(JobO.type_desc == 'dax', JobO.type_desc == 'dag')
        
    def _get_job_filter(self, JobO=Job):
        filters = {
            'all': None,
            'nonsub': not_(self._dax_or_dag_cond(JobO)),
            'subwf': self._dax_or_dag_cond(JobO),
            'dax': JobO.type_desc == 'dax',
            'dag': JobO.type_desc == 'dag',
            'compute': JobO.type_desc == 'compute',
            'stage-in-tx': JobO.type_desc == 'stage-in-tx',
            'stage-out-tx': JobO.type_desc == 'stage-out-tx',
            'registration': JobO.type_desc == 'registration',
            'inter-site-tx': JobO.type_desc == 'inter-site-tx',
            'create-dir': JobO.type_desc == 'create-dir',
            'staged-compute': JobO.type_desc == 'staged-compute',
            'cleanup': JobO.type_desc == 'cleanup',
            'chmod': JobO.type_desc == 'chmod',
        }
        return filters[self._job_filter_mode]
        
    def _max_job_seq_subquery(self):
        """
        Creates the following subquery that is used in 
        several queries:
        and jb_inst.job_submit_seq  = (
            select max(job_submit_seq) from job_instance where job_id = jb_inst.job_id group by job_id
            )
        """
        JobInstanceSubMax = orm.aliased(JobInstance)
        sub_q = self.session.query(func.max(JobInstanceSubMax.job_submit_seq).label('max_id'))
        sub_q = sub_q.filter(JobInstanceSubMax.job_id == JobInstance.job_id).correlate(JobInstance)
        sub_q = sub_q.group_by(JobInstanceSubMax.job_id).subquery()
        return sub_q
        
    def get_total_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaljobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaljobs        
        """
        q = self.session.query(Job.job_id)
        if self._expand:
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            q = q.filter(Workflow.wf_id == self._wfs[0])
        q = q.filter(Job.wf_id == Workflow.wf_id)
        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
             
        return q.count()
        
    def get_total_succeeded_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalsucceededjobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalsucceededjobs
        """
        
        JobSub = orm.aliased(Job, name='JobSub')
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        WorkflowSub = orm.aliased(Workflow, name='WorkflowSub')
        
        sq_1 = self.session.query(JobInstanceSub.job_instance_id.label('jid'),
                func.max(JobInstanceSub.job_submit_seq).label('mjss'))
        
        if self._expand:
            sq_1 = sq_1.filter(WorkflowSub.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(WorkflowSub.wf_id == self._wfs[0])
        
        sq_1 = sq_1.filter(WorkflowSub.wf_id == JobSub.wf_id)
        sq_1 = sq_1.filter(JobSub.job_id == JobInstanceSub.job_id)
        if self._get_job_filter(JobSub) is not None:
            sq_1 = sq_1.filter(self._get_job_filter(JobSub))
        
        sq_1 = sq_1.group_by(JobSub.job_id).subquery()
        
        JobstateSub = orm.aliased(Jobstate, name='JobstateSub')
            
        sq_2 = self.session.query(sq_1.c.jid, func.max(JobstateSub.jobstate_submit_seq).label('sjss'))
        sq_2 = sq_2.filter(JobstateSub.job_instance_id == sq_1.c.jid)
        sq_2 = sq_2.group_by(sq_1.c.jid).subquery()
        
        JobstateOut = orm.aliased(Jobstate)
        
        q = self.session.query(JobstateOut.job_instance_id, JobstateOut.state, JobstateOut.timestamp, JobstateOut.jobstate_submit_seq, sq_2.c.jid, sq_2.c.sjss)
        q = q.filter(JobstateOut.job_instance_id == sq_2.c.jid)
        q = q.filter(JobstateOut.jobstate_submit_seq == sq_2.c.sjss)
        q = q.filter(JobstateOut.state.in_(['POST_SCRIPT_SUCCESS', 'JOB_SUCCESS']))

        return q.count()

    def get_total_failed_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalfailedjobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalfailedjobs
        """
        JobSub = orm.aliased(Job, name='JobSub')
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        WorkflowSub = orm.aliased(Workflow, name='WorkflowSub')
        
        sq_1 = self.session.query(JobInstanceSub.job_instance_id.label('jid'),
                func.max(JobInstanceSub.job_submit_seq).label('mjss'))
        if self._expand:
            sq_1 = sq_1.filter(WorkflowSub.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(WorkflowSub.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(JobSub.job_id == JobInstanceSub.job_id)
        if self._get_job_filter(JobSub) is not None:
            sq_1 = sq_1.filter(self._get_job_filter(JobSub))
        sq_1 = sq_1.group_by(JobSub.job_id)
        sq_1 = sq_1.subquery()
        
        q = self.session.query(sq_1.c.jid)
        q = q.filter(Jobstate.job_instance_id == sq_1.c.jid)
        q = q.filter(Jobstate.state.in_(['PRE_SCRIPT_FAILED','SUBMIT_FAILED','JOB_FAILURE' ,'POST_SCRIPT_FAILED']))
        return q.count()
        
    def _query_jobstate_for_instance(self, states):
        """
        The states arg is a list of strings.
        Returns an appropriate subquery.
        """
        q = self.session.query(Jobstate.job_instance_id)
        q = q.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        q = q.filter(Jobstate.state.in_(states)).subquery()
        return q
        
    def get_total_jobs_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-TotalJobRetries
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-TotalJobRetries
        """
        d_or_d = self._dax_or_dag_cond()
        
        sq_1 = self.session.query(func.count(Job.job_id))
        if self._expand:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Job.wf_id == Workflow.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())

        sq_1 = sq_1.subquery()

        
        sq_2 = self.session.query(func.count(distinct(JobInstance.job_id)))
        if self._expand:
            sq_2 = sq_2.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            sq_2 = sq_2.filter(Workflow.wf_id == self._wfs[0])
        sq_2 = sq_2.filter(Job.wf_id == Workflow.wf_id)
        sq_2 = sq_2.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_2 = sq_2.filter(self._get_job_filter())

        sq_2 = sq_2.subquery()
        
        q = self.session.query((sq_1.as_scalar() - sq_2.as_scalar()).label('total_job_retries'))
        
        return q.all()[0].total_job_retries
        
    def get_total_tasks_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaltask
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaltasks
        """
        q = self.session.query(Task.task_id)
        if self._expand:
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            q = q.filter(Workflow.wf_id == self._wfs[0])
        q = q.filter(Task.wf_id == Workflow.wf_id)
        q = q.filter(Task.job_id == Job.job_id)
        if self._get_job_filter(Task) is not None:
            q = q.filter(self._get_job_filter(Task))
        return q.count()
    
    def _base_task_status_query(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalsucceededtasks
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalsucceededtasks
        """
        sq_1 = self.session.query(Workflow.wf_id.label('wid'), JobInstance.job_instance_id.label('jiid'),
                func.max(JobInstance.job_submit_seq).label('mjss'))
        if self._expand:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        sq_1 = sq_1.group_by(JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.subquery()
        
        q = self.session.query(Invocation.invocation_id)
        q = q.filter(Invocation.abs_task_id != None)
        q = q.filter(Invocation.job_instance_id == sq_1.c.jiid)
        q = q.filter(Invocation.wf_id == sq_1.c.wid)
        
        return q
        
    def get_total_succeeded_tasks_status(self):
        q = self._base_task_status_query()
        q = q.filter(Invocation.exitcode == 0)
        return q.count()
        
    def get_total_failed_tasks_status(self):
        q = self._base_task_status_query()
        q = q.filter(Invocation.exitcode != 0)
        return q.count()
        
    def get_total_tasks_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaltaskretries
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaltaskretries
        """
        sq_1 = self.session.query(Workflow.wf_id.label('wid'), Invocation.abs_task_id.label('tid'))
        if self._expand:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Job.wf_id == Workflow.wf_id)
        sq_1 = sq_1.filter(Invocation.wf_id == Workflow.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.filter(JobInstance.job_instance_id == Invocation.job_instance_id)
        sq_1 = sq_1.filter(Invocation.abs_task_id != None)
        
        i = 0
        f = {}
        for row in sq_1.all():
            i += 1
            if not f.has_key(row):
                f[row] = True
                
        return i - len(f.keys())
                
    #
    # Run statistics
    #

    def get_workflow_states(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Workflowwalltime
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowwalltime
        """
        q = self.session.query(Workflowstate.wf_id, Workflowstate.state, Workflowstate.timestamp)
        q = q.filter(Workflowstate.wf_id.in_(self._wfs)).order_by(Workflowstate.restart_count)

        return q.all()

    def get_workflow_cum_job_wall_time(self):
        """
        select sum(remote_duration) from invocation as invoc 
           where  invoc.task_submit_seq >=0 and invoc.wf_id in(
              1,2,3
           )
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Workflowcumulativejobwalltime
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowcumulativejobwalltime
        """
        q = self.session.query(func.sum(Invocation.remote_duration))
        q = q.filter(Invocation.task_submit_seq >= 0)
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.filter(Invocation.transformation != 'condor::dagman')
        return q.first()[0]

    def get_submit_side_job_wall_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Cumulativejobwalltimeasseenfromsubmitside
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Cumulativejobwalltimeasseenfromsubmitside
        """
        q = self.session.query(func.sum(JobInstance.local_duration).label('wall_time'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        if self._expand:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))

        return q.first().wall_time
        
    def get_workflow_details(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowdetails
        """
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid,
            Workflow.parent_wf_id, Workflow.root_wf_id, Workflow.dag_file_name,
            Workflow.submit_hostname, Workflow.submit_dir, Workflow.planner_arguments,
            Workflow.user, Workflow.grid_dn, Workflow.planner_version,
            Workflow.dax_label, Workflow.dax_version)
        q = q.filter(Workflow.wf_id.in_(self._wfs))
        return q.all()
        
    def get_workflow_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowretries
        """
        sq_1 = self.session.query(func.max(Workflowstate.restart_count).label('retry'))
        if self._expand:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            sq_1 = sq_1.filter(Workflow.wf_id.in_(self._wfs))
        sq_1 = sq_1.filter(Workflowstate.wf_id == Workflow.wf_id)
        sq_1 = sq_1.group_by(Workflowstate.wf_id)
        sq_1 = sq_1.subquery()
        
        q = self.session.query(func.sum(sq_1.c.retry).label('total_retry'))
        return q.one().total_retry
    
    #
    # Job Statistics
    # These queries are documented:
    # https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file
    #
    
    def get_job_statistics(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-All
        """
        if self._expand:
            return []
        sq_1 = self.session.query(func.min(Jobstate.timestamp))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_1 = sq_1.filter(or_(Jobstate.state == 'GRID_SUBMIT', Jobstate.state == 'GLOBUS_SUBMIT',
                                Jobstate.state == 'EXECUTE'))
        sq_1 = sq_1.subquery()
        
        sq_2 = self.session.query(Jobstate.timestamp)
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_2 = sq_2.filter(Jobstate.state == 'SUBMIT')
        sq_2 = sq_2.subquery()
        
        sq_3 = self.session.query(func.min(Jobstate.timestamp))
        sq_3 = sq_3.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_3 = sq_3.filter(Jobstate.state == 'EXECUTE')
        sq_3 = sq_3.subquery()
        
        sq_4 = self.session.query(Jobstate.timestamp)
        sq_4 = sq_4.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_4 = sq_4.filter(or_(Jobstate.state == 'GRID_SUBMIT', Jobstate.state == 'GLOBUS_SUBMIT'))
        sq_4 = sq_4.subquery()
        
        sq_5 = self.session.query(func.sum(Invocation.remote_duration))
        sq_5 = sq_5.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_5 = sq_5.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_5 = sq_5.filter(Invocation.task_submit_seq >= 0)
        sq_5 = sq_5.group_by().subquery()
        
        sq_6 = self.session.query(Jobstate.timestamp)
        sq_6 = sq_6.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_6 = sq_6.filter(Jobstate.state == 'POST_SCRIPT_TERMINATED')
        sq_6 = sq_6.subquery()
        
        sq_7 = self.session.query(func.max(Jobstate.timestamp))
        sq_7 = sq_7.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_7 = sq_7.filter(or_(Jobstate.state == 'POST_SCRIPT_STARTED', Jobstate.state == 'JOB_TERMINATED'))
        sq_7 = sq_7.subquery()
        
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            Job.exec_job_id.label('job_name'), JobInstance.site,
            cast(sq_1.as_scalar() - sq_2.as_scalar(), Float).label('condor_q_time'),
            cast(sq_3.as_scalar() - sq_4.as_scalar(), Float).label('resource_delay'),
            cast(JobInstance.local_duration, Float).label('runtime'),
            cast(sq_5.as_scalar(), Float).label('kickstart'),
            cast(sq_6.as_scalar() - sq_7.as_scalar(), Float).label('post_time'),
            cast(JobInstance.cluster_duration, Float).label('seqexec'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        
        return q.all()
        
    def _state_sub_q(self, states, function=None):
        sq = None
        if not function:
            sq = self.session.query(Jobstate.timestamp)
        elif function == 'max':
            sq = self.session.query(func.max(Jobstate.timestamp))
        elif function == 'min':
            sq = self.session.query(func.min(Jobstate.timestamp))
        sq = sq.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq = sq.filter(Jobstate.state.in_(states)).subquery()
        return sq
        
    def get_job_states(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-JobStates
        """
        if self._expand:
            return []
        sq_1 = self.session.query(Host.hostname).filter(Host.host_id == JobInstance.host_id).correlate(JobInstance).subquery()
        #select min(timestamp) from jobstate where job_instance_id = jb_inst.job_instance_id
        # ) as jobS ,
        #(
        #select max(timestamp)-min(timestamp) from jobstate where job_instance_id = jb_inst.job_instance_id
        # ) as jobDuration,
        
        sq_jobS = self.session.query(func.min(Jobstate.timestamp))
        sq_jobS = sq_jobS.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance).subquery()
        
        sq_jobD = self.session.query(func.max(Jobstate.timestamp) - func.min(Jobstate.timestamp))
        sq_jobD = sq_jobD.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance).subquery()
        
        sq_2 = self._state_sub_q(['PRE_SCRIPT_STARTED'])
        sq_3 = self._state_sub_q(['PRE_SCRIPT_TERMINATED'])
        sq_4 = self._state_sub_q(['PRE_SCRIPT_STARTED'])
        sq_5 = self._state_sub_q(['SUBMIT'])
        sq_6 = self._state_sub_q(['JOB_TERMINATED'])
        sq_7 = self._state_sub_q(['GRID_SUBMIT', 'GLOBUS_SUBMIT'])
        sq_8 = self._state_sub_q(['EXECUTE'], 'min')
        sq_9 = self._state_sub_q(['EXECUTE', 'SUBMIT'], 'max')
        sq_10 = self._state_sub_q(['JOB_TERMINATED'])

        sq_11 = self.session.query(func.min(Invocation.start_time))
        sq_11 = sq_11.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_11 = sq_11.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_11 = sq_11.filter(Invocation.task_submit_seq >= 0)
        sq_11 = sq_11.group_by(Invocation.job_instance_id).subquery()

        sq_12 = self.session.query(func.sum(Invocation.remote_duration))
        sq_12 = sq_12.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_12 = sq_12.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_12 = sq_12.filter(Invocation.task_submit_seq >= 0)
        sq_12 = sq_12.group_by(Invocation.job_instance_id).subquery()
        
        sq_13 = self._state_sub_q(['POST_SCRIPT_STARTED', 'JOB_TERMINATED'], 'max')
        sq_14 = self._state_sub_q(['POST_SCRIPT_TERMINATED'])
        
        sq_15 = self.session.query(func.group_concat(func.distinct(Invocation.transformation)))
        sq_15 = sq_15.filter(Invocation.wf_id.in_(self._wfs))
        sq_15 = sq_15.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_15 = sq_15.filter(Invocation.transformation != 'dagman::post')
        sq_15 = sq_15.filter(Invocation.transformation != 'dagman::pre')
        sq_15 = sq_15.subquery()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
                Job.exec_job_id.label('job_name'), JobInstance.site,
                sq_1.as_scalar().label('host_name'),
                cast(sq_jobS.as_scalar(), Float).label('jobS'),
                cast(sq_jobD.as_scalar(), Float).label('jobDuration'),
                cast(sq_2.as_scalar(), Float).label('pre_start'),
                cast(sq_3.as_scalar() - sq_4.as_scalar(), Float).label('pre_duration'),
                cast(sq_5.as_scalar(), Float).label('condor_start'),
                cast(sq_6.as_scalar() - sq_5.as_scalar(), Float).label('condor_duration'),
                cast(sq_7.as_scalar(), Float).label('grid_start'),
                cast(sq_8.as_scalar() - sq_7.as_scalar(), Float).label('grid_duration'),
                cast(sq_9.as_scalar(), Float).label('exec_start'),
                cast(sq_10.as_scalar() - sq_9.as_scalar(), Float).label('exec_duration'),
                cast(sq_11.as_scalar(), Float).label('kickstart_start'),
                cast(sq_12.as_scalar(), Float).label('kickstart_duration'),
                cast(sq_13.as_scalar(), Float).label('post_start'),
                cast(sq_14.as_scalar() - sq_13.as_scalar(), Float).label('post_duration'),
                sq_15.as_scalar().label('transformation')
                )
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)
        
        return q.all()
        
    def get_job_instance_sub_wf_map(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Subworkflowjobinstancesmapping
        """
        if self._expand:
            return []
        q = self.session.query(JobInstance.job_instance_id, JobInstance.subwf_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(self._dax_or_dag_cond())
        return q.all()
        
    def get_failed_job_instances(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Failedjobinstances
        """
        if self._expand:
            return []
        d_or_d = self._dax_or_dag_cond()
        
        q = self.session.query(JobInstance.job_instance_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        q = q.filter(Jobstate.state.in_(['PRE_SCRIPT_FAILED','SUBMIT_FAILED','JOB_FAILURE' ,'POST_SCRIPT_FAILED']))
        
        return q.all()
    
    def get_job_name(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Name
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            Job.exec_job_id.label('job_name'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs)).order_by(JobInstance.job_submit_seq)
        return q.all()
        
    def get_job_site(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Site
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            JobInstance.site)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id).group_by(Job.job_id)
        q = q.order_by(JobInstance.job_submit_seq)
        return q.all()
        
    def get_job_kickstart(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Kickstart
        """
        if self._expand:
            return []
            
        sq_1 = self.session.query(func.sum(Invocation.remote_duration))
        sq_1 = sq_1.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_1 = sq_1.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_1 = sq_1.filter(Invocation.task_submit_seq >= 0)
        sq_1 = sq_1.group_by(Invocation.job_instance_id)
        sq_1 = sq_1.subquery()
            
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            cast(sq_1.as_scalar(), Float).label('kickstart'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)
        return q.all()
        
    def get_job_runtime(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Runtime
        """
        if self._expand:
            return []
            
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            JobInstance.local_duration.label('runtime'))
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.group_by(Job.job_id).order_by(JobInstance.job_submit_seq)
        
        return q.all()
        
    def get_job_seqexec(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Seqexec
        """
        if self._expand:
            return []
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            JobInstance.cluster_duration)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.clustered != 0)
        q = q.order_by(JobInstance.job_submit_seq)
        
        return q.all()
    
    def get_condor_q_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-CondorQTime
        """
        if self._expand:
            return []
            
        sq_1 = self.session.query(func.min(Jobstate.timestamp))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_1 = sq_1.filter(or_(Jobstate.state == 'GRID_SUBMIT', Jobstate.state == 'GLOBUS_SUBMIT',
                                Jobstate.state == 'EXECUTE'))
        sq_1 = sq_1.subquery()
        
        sq_2 = self.session.query(Jobstate.timestamp)
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_2 = sq_2.filter(Jobstate.state == 'SUBMIT')
        sq_2 = sq_2.subquery()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
                cast(sq_1.as_scalar() - sq_2.as_scalar(), Float).label('condor_q_time'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)

        return q.all()
        
    def get_resource_delay(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Resource
        """
        if self._expand:
            return []
        
        sq_1 = self.session.query(func.min(Jobstate.timestamp))
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_1 = sq_1.filter(Jobstate.state == 'EXECUTE')
        sq_1 = sq_1.subquery()

        sq_2 = self.session.query(Jobstate.timestamp)
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_2 = sq_2.filter(or_(Jobstate.state == 'GRID_SUBMIT', Jobstate.state == 'GLOBUS_SUBMIT'))
        sq_2 = sq_2.subquery()
        
        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
                cast(sq_1.as_scalar() - sq_2.as_scalar(), Float).label('resource_delay'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)
        
        return q.all()
        
    def get_post_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Post
        """
        if self._expand:
            return []
            
        sq_1 = self.session.query(Jobstate.timestamp)
        sq_1 = sq_1.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_1 = sq_1.filter(Jobstate.state == 'POST_SCRIPT_TERMINATED')
        sq_1 = sq_1.subquery()

        sq_2 = self.session.query(func.max(Jobstate.timestamp))
        sq_2 = sq_2.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_2 = sq_2.filter(or_(Jobstate.state == 'POST_SCRIPT_STARTED', Jobstate.state == 'JOB_TERMINATED'))
        sq_2 = sq_2.subquery()

        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
                cast(sq_1.as_scalar() - sq_2.as_scalar(), Float).label('post_time'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)
        
        return q.all()
        
        
    #
    # This query documented:
    # https://confluence.pegasus.isi.edu/display/pegasus/Transformation+Statistics+file
    #
        
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
                func.count(case([(Invocation.exitcode == 0, Invocation.exitcode)])).label('success'),
                func.count(case([(Invocation.exitcode != 0, Invocation.exitcode)])).label('failure'),
                func.max(Invocation.remote_duration).label('max'),
                func.avg(Invocation.remote_duration).label('avg'), 
                func.sum(Invocation.remote_duration).label('sum'))
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.group_by(Invocation.transformation)
        
        return q.all()
        
if __name__ == '__main__':
    pass