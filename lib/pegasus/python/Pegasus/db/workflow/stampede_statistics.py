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
 stats.close()

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

Time filtering:

This behaves much like job filtering.  For the runtime queries,
the time intervals 'month', 'week', 'day', and  'hour' can
be set using the set_time_filter() method.  If this method
is not set, it will default to the 'month' interval for filtering.

Hostname filtering:

For the runtime queries the method set_host_filter() can be used to
filter by various hosts.  This method differs from the job and time
filtering methods in that the argument can be either a string (for
a single hostname), or an array/list of hostnames for multiple
hostnames.

Example::
 s.set_host_filter('butterfly.isi.edu')
 or
 s.set_host_filter(['engage-submit3.renci.org', 'node0012.palmetto.clemson.edu'])

Either one of these variations will work.  The first variation will
only retrieve data for that one host, the second will return data
for both hosts.  If this method is not set, no hostname filtering
will be done and information for all hosts will be returned.

Transformation filtering:

Transformation filtering works similarly to hostname filtering in
that it can accept a single string value or a array/list of strings.
However the set_transformation_filter() method accepts two keyword
arguments - 'include' and 'exclude'.  Only one of these keywords can
be set per method call.

Example::
 s.set_transformation_filter(include='pegasus::dirmanager')
 s.set_transformation_filter(exclude=['dagman::post' , 'dagman::pre' ,'condor::dagman'])
 etc.

This example demonstrates the two proper keyword invocations and
that either a string or list may be used.  If this method is not
set, no filtering will be done and information for all transforms
will be returned.  Calling this method with no arguments will
reset any previously set filters.

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
 get_schema_version
 get_total_jobs_status
 get_total_succeeded_failed_jobs_status
 get_total_succeeded_jobs_status
 get_total_failed_jobs_status
 get_total_jobs_retries
 get_total_tasks_status
 get_total_succeeded_tasks_status
 get_total_failed_tasks_status
 get_task_success_report
 get_task_failure_report
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
 get_job_instance_info
 get_job_name
 get_job_site
 get_job_kickstart
 get_job_runtime
 get_job_seqexec
 get_condor_q_time
 get_resource_delay
 get_post_time
 get_transformation_statistics
 get_invocation_by_time
 get_jobs_run_by_time
 get_invocation_by_time_per_host
 get_jobs_run_by_time_per_host

Methods listed in order of query list on wiki.

https://confluence.pegasus.isi.edu/display/pegasus/Pegasus+Statistics+Python+Version+Modified
"""
__author__ = "Monte Goode"

from Pegasus.db import connection
from Pegasus.db.schema import *
from Pegasus.db.errors import StampedeDBNotFoundError

# Main stats class.

class StampedeStatistics(object):
    def __init__(self, connString, expand_workflow=True):
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))
        try:
            self.session = connection.connect(connString)
        except connection.ConnectionError as e:
            self.log.exception(e)
            raise StampedeDBNotFoundError

        self._expand = expand_workflow

        self._root_wf_id = None
        self._root_wf_uuid = None
        self._job_filter_mode = None
        self._time_filter_mode = None
        self._host_filter = None
        self._xform_filter = {'include':None, 'exclude':None}

        self._wfs = []

    def initialize(self, root_wf_uuid = None, root_wf_id = None):
        if root_wf_uuid == None and root_wf_id == None:
            self.log.error('Either root_wf_uuid or root_wf_id is required')
            raise ValueError('Either root_wf_uuid or root_wf_id is required')

        q = self.session.query(Workflow.root_wf_id, Workflow.wf_id, Workflow.wf_uuid)

        if root_wf_uuid:
            q = q.filter(Workflow.wf_uuid == root_wf_uuid)
        else:
            q = q.filter(Workflow.wf_id == root_wf_id)

        try:
            result = q.one ()
            self._root_wf_id = result.wf_id
            self._root_wf_uuid = result.wf_uuid
            self._is_root_wf = result.root_wf_id == result.wf_id
        except orm.exc.MultipleResultsFound as e:
            self.log.error('Multiple results found for wf_uuid: %s', root_wf_uuid)
            raise
        except orm.exc.NoResultFound as e:
            self.log.error('No results found for wf_uuid: %s', root_wf_uuid)
            raise

        self._wfs.insert(0, self._root_wf_id)

        if self._expand:
            '''
            select parent_wf_id, wf_id from workflow where root_wf_id =
            (select root_wf_id from workflow where wf_id=self._root_wf_id);
            '''
            sub_q = self.session.query(Workflow.root_wf_id).filter(Workflow.wf_id == self._root_wf_id).subquery('root_wf')

            q = self.session.query(Workflow.parent_wf_id, Workflow.wf_id).filter(Workflow.root_wf_id == sub_q.c.root_wf_id)

            # @tree will hold the entire sub-work-flow dependency structure.
            tree = {}

            for row in q.all():
                parent_node = row.parent_wf_id
                if parent_node in tree:
                    tree [parent_node].append (row.wf_id)
                else:
                    tree [parent_node] = [row.wf_id]

            self._get_descendants (tree, self._root_wf_id)

        self.log.debug('Descendant workflow ids %s', self._wfs)

        if not len(self._wfs):
            self.log.error('No results found for wf_uuid: %s', root_wf_uuid)
            raise ValueError('No results found for wf_uuid: %s', root_wf_uuid)

        # Initialize filters with default value
        self.set_job_filter()
        self.set_time_filter()
        self.set_host_filter()
        self.set_transformation_filter()
        return True

    def _get_descendants (self, tree, wf_node):
        '''
        If the root_wf_uuid given to initialize function is not the UUID of the root work-flow, and
        expand_workflow was set to True, then this recursive function determines all child work-flows.
        @tree A dictionary when key is the parent_wf_id and value is a list of its child wf_id's.
        @wf_node The node for which to determine descendants.
        '''

        if tree == None or wf_node == None:
            raise ValueError('Tree, or node cannot be None')

        if wf_node in tree:

            self._wfs.extend (tree [wf_node])

            for wf in tree [wf_node]:
                self._get_descendants (tree, wf)

    def close(self):
        self.log.debug('close')
        self.session.close()

    def set_job_filter(self, filter='all'):
        modes = ['all', 'nonsub', 'subwf', 'dax', 'dag', 'compute', 'stage-in-tx',
                'stage-out-tx', 'registration', 'inter-site-tx', 'create-dir',
                'staged-compute', 'cleanup', 'chmod']
        try:
            modes.index(filter)
            self._job_filter_mode = filter
            self.log.debug('Setting filter to: %s', filter)
        except:
            self._job_filter_mode = 'all'
            self.log.error('Unknown job filter %s - setting to all', filter)


    def set_time_filter(self, filter='month'):
        modes = ['month', 'week', 'day', 'hour']
        try:
            modes.index(filter)
            self._time_filter_mode = filter
            self.log.debug('Setting filter to: %s', filter)
        except:
            self._time_filter_mode = 'month'
            self.log.error('Unknown time filter %s - setting to month', filter)

    def set_host_filter(self, host=None):
        """
        The host argument can either be a string/single hostname or
        it can be a list/array of hostnames.
        """
        self._host_filter = host

    def set_transformation_filter(self, include=None, exclude=None):
        """
        Either of these args can either be a single string/xform type or
        it can be a list/array of xform types.

        Both arguments can not be set at the same time.  If they are,
        the program will log an error and not do any filtering.
        """
        self._xform_filter['include'] = include
        self._xform_filter['exclude'] = exclude

    #
    # Pulls information about sub workflows
    #

    def get_sub_workflow_ids(self):
        """
        Returns info on child workflows only.
        """
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid, Workflow.dax_label)
        q = q.filter(Workflow.parent_wf_id == self._root_wf_id)
        return q.all()

    def get_descendant_workflow_ids(self):
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid)
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Workflow.wf_id != self._root_wf_id)
        return q.all()

    def get_schema_version(self):
        return self.s_check.check_version()

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
        if self._expand and self._is_root_wf:
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            q = q.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            q = q.filter(Workflow.wf_id == self._wfs[0])
        q = q.filter(Job.wf_id == Workflow.wf_id)
        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())

        return q.count()

    def get_total_succeeded_failed_jobs_status(self, classify_error=False, tag=None):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalsucceeded_failed_jobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalsucceededfailedjobs
        """
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        sq_1 = self.session.query(func.max(JobInstanceSub.job_submit_seq).label('jss'), JobInstanceSub.job_id.label('jobid'))

        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstanceSub.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.group_by(JobInstanceSub.job_id).subquery()

        q = self.session.query(
            func.sum (case([(JobInstance.exitcode == 0, 1)], else_=0)).label ("succeeded"),
            func.sum (case([(JobInstance.exitcode != 0, 1)], else_=0)).label ("failed"))
        q = q.filter(JobInstance.job_id == sq_1.c.jobid)
        q = q.filter(JobInstance.job_submit_seq == sq_1.c.jss)

        if classify_error:
            if tag is None:
                self.log.error( "for error classification you need to specify tag")
                return None

            q = q.filter(JobInstance.job_instance_id == Tag.job_instance_id)
            q = q.filter(Tag.name == tag)
            q = q.filter(Tag.count > 0)

        return q.one()

    def get_total_held_jobs(self):
        """
        SELECT DISTINCT count(  job_instance_id) FROM
                                jobstate j JOIN   ( SELECT max(job_instance_id) as maxid FROM job_instance GROUP BY job_id) max_ji ON j.job_instance_id=max_ji.maxid
                                WHERE j.state = 'JOB_HELD';
        """

        sq_1 = self.session.query(func.max(JobInstance.job_instance_id).label('max_ji_id'), JobInstance.job_id.label('jobid'), Job.exec_job_id.label('jobname'))

        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])

        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        sq_1 = sq_1.group_by(JobInstance.job_id).subquery()

        q = self.session.query(distinct(Jobstate.job_instance_id.label('last_job_instance')), sq_1.c.jobid, sq_1.c.jobname, Jobstate.reason)

        q = q.filter( Jobstate.state == 'JOB_HELD')
        q = q.join( sq_1, Jobstate.job_instance_id == sq_1.c.max_ji_id)

        return q.all()

    def get_total_succeeded_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalsucceededjobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalsucceededjobs
        """
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        sq_1 = self.session.query(func.max(JobInstanceSub.job_submit_seq).label('jss'), JobInstanceSub.job_id.label('jobid'))
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstanceSub.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.group_by(JobInstanceSub.job_id).subquery()

        q = self.session.query(JobInstance.job_instance_id.label('last_job_instance'))
        q = q.filter(JobInstance.job_id == sq_1.c.jobid)
        q = q.filter(JobInstance.job_submit_seq == sq_1.c.jss)
        q = q.filter(JobInstance.exitcode == 0).filter(JobInstance.exitcode != None)
        return q.count()

    def _get_total_failed_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalfailedjobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalfailedjobs
        """
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        sq_1 = self.session.query(func.max(JobInstanceSub.job_submit_seq).label('jss'), JobInstanceSub.job_id.label('jobid'))
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstanceSub.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.group_by(JobInstanceSub.job_id).subquery()

        q = self.session.query(JobInstance.job_instance_id.label('last_job_instance'))
        q = q.filter(JobInstance.job_id == sq_1.c.jobid)
        q = q.filter(JobInstance.job_submit_seq == sq_1.c.jss)
        q = q.filter(JobInstance.exitcode != 0).filter(JobInstance.exitcode != None)

        return q

    def get_total_running_jobs_status(self):
        JobInstanceSub = orm.aliased(JobInstance, name='JobInstanceSub')
        sq_1 = self.session.query(func.max(JobInstanceSub.job_submit_seq).label('jss'), JobInstanceSub.job_id.label('jobid'))
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Workflow.wf_id == Job.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstanceSub.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())
        sq_1 = sq_1.group_by(JobInstanceSub.job_id).subquery()

        q = self.session.query(JobInstance.job_instance_id.label('last_job_instance'))
        q = q.filter(JobInstance.job_id == sq_1.c.jobid)
        q = q.filter(JobInstance.job_submit_seq == sq_1.c.jss)
        q = q.filter(JobInstance.exitcode == None)
        return q.count()

    def get_total_failed_jobs_status(self):

        q = self._get_total_failed_jobs_status()
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
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(Workflow.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(Job.wf_id == Workflow.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())

        sq_1 = sq_1.subquery()


        sq_2 = self.session.query(func.count(distinct(JobInstance.job_id)))
        if self._expand and self._is_root_wf:
            sq_2 = sq_2.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_2 = sq_2.filter(Workflow.wf_id.in_ (self._wfs))
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
        if self._expand and self._is_root_wf:
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            q = q.filter(Workflow.wf_id.in_ (self._wfs))
        else:
            q = q.filter(Workflow.wf_id == self._wfs[0])
        q = q.filter(Task.wf_id == Workflow.wf_id)
        q = q.filter(Task.job_id == Job.job_id)
        if self._get_job_filter(Task) is not None:
            q = q.filter(self._get_job_filter(Task))
        return q.count()


    def _base_task_status_query_old(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totalsucceededtasks
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totalsucceededtasks
        """
        # This query generation method is obsolete and is only being
        # kept for optimization reference.

        WorkflowSub1 = orm.aliased(Workflow, name='WorkflowSub1')
        JobInstanceSub1 = orm.aliased(JobInstance, name='JobInstanceSub1')
        JobSub1 = orm.aliased(Job, name='JobSub1')

        sq_1 = self.session.query(WorkflowSub1.wf_id.label('wid'),
                func.max(JobInstanceSub1.job_submit_seq).label('jss'),
                JobInstanceSub1.job_id.label('jobid')
        )
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(WorkflowSub1.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(WorkflowSub1.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(WorkflowSub1.wf_id == self._wfs[0])
        sq_1 = sq_1.filter(WorkflowSub1.wf_id == JobSub1.wf_id)
        sq_1 = sq_1.filter(JobSub1.job_id == JobInstanceSub1.job_id)
        sq_1 = sq_1.group_by(JobInstanceSub1.job_id)
        if self._get_job_filter(JobSub1) is not None:
            sq_1 = sq_1.filter(self._get_job_filter(JobSub1))
        sq_1 = sq_1.subquery()

        JobInstanceSub2 = orm.aliased(JobInstance, name='JobInstanceSub2')
        sq_2 = self.session.query(sq_1.c.wid.label('wf_id'), JobInstanceSub2.job_instance_id.label('last_job_instance_id'))
        sq_2 = sq_2.filter(JobInstanceSub2.job_id == sq_1.c.jobid)
        sq_2 = sq_2.filter(JobInstanceSub2.job_submit_seq == sq_1.c.jss)
        sq_2 = sq_2.subquery()

        q = self.session.query(Invocation.invocation_id)
        q = q.filter(Invocation.abs_task_id != None)
        q = q.filter(Invocation.job_instance_id == sq_2.c.last_job_instance_id)
        q = q.filter(Invocation.wf_id == sq_2.c.wf_id)

        # Calling wrapper methods would invoke like so:
        # q = self._base_task_status_query()
        # q = q.filter(Invocation.exitcode == 0)
        # return q.count()

        return q


    def _base_task_statistics_query(self, success=True, pmc=False):
        w = orm.aliased(Workflow, name='w')
        j = orm.aliased(Job, name='j')
        ji = orm.aliased(JobInstance, name='ji')

        sq_1 = self.session.query(w.wf_id,
                j.job_id,
                ji.job_instance_id.label('jiid'),
                ji.job_submit_seq.label('jss'),
                func.max(ji.job_submit_seq).label('maxjss'))
        if pmc :
            sq_1 = self.session.query(w.wf_id,
                j.job_id,
                ji.job_instance_id.label('jiid'),
                ji.job_submit_seq.label('jss'))

        sq_1 = sq_1.join(j, w.wf_id == j.wf_id)
        sq_1 = sq_1.join(ji, j.job_id == ji.job_id)
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(w.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(w.wf_id.in_ (self._wfs))
        else:
            sq_1 = sq_1.filter(w.wf_id == self._wfs[0])
        if not pmc:
            sq_1 = sq_1.group_by(j.job_id)
        if self._get_job_filter(j) is not None:
            sq_1 = sq_1.filter(self._get_job_filter(j))
        sq_1 = sq_1.subquery('t')

        # PM-713 - Change to func.count(distinct(Invocation.abs_task_id)) from func.count(Invocation.exitcode)
        sq_2 = self.session.query(sq_1.c.wf_id, func.count(distinct(Invocation.abs_task_id)).label('count'))
        sq_2 = sq_2.select_from(orm.join(sq_1, Invocation, sq_1.c.jiid == Invocation.job_instance_id))
        if not pmc:
           sq_2 = sq_2.filter(sq_1.c.jss == sq_1.c.maxjss)

        sq_2 = sq_2.filter(Invocation.abs_task_id != None)
        if success:
            sq_2 = sq_2.filter(Invocation.exitcode == 0)
        else:
            sq_2 = sq_2.filter(Invocation.exitcode != 0)
        sq_2 = sq_2.group_by(sq_1.c.wf_id)
        return sq_2

    def _task_statistics_query_sum(self, success=True, pmc=False):
        s = self._base_task_statistics_query(success,pmc).subquery('tt')
        q = self.session.query(func.sum(s.c.count).label('task_count'))
        return q.one()[0] or 0

    def get_total_succeeded_tasks_status(self,pmc=False):
        return self._task_statistics_query_sum(True,pmc)

    def get_total_failed_tasks_status(self):
        return self._task_statistics_query_sum(False,False)

    def get_task_success_report(self,pmc=False):
        return self._base_task_statistics_query(True,pmc).all()

    def get_task_failure_report(self):
        return self._base_task_statistics_query(False,False).all()

    def get_total_tasks_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaltaskretries
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaltaskretries
        """
        sq_1 = self.session.query(Workflow.wf_id.label('wid'), Invocation.abs_task_id.label('tid'))
        if self._expand and self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.root_wf_id == self._root_wf_id)
        elif self._expand and not self._is_root_wf:
            sq_1 = sq_1.filter(Workflow.wf_id.in_ (self._wfs))
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
            if row not in f:
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
        q = self.session.query(Workflowstate.wf_id, Workflowstate.state, Workflowstate.timestamp,
            Workflowstate.restart_count, Workflowstate.status)
        q = q.filter(Workflowstate.wf_id == self._root_wf_id).order_by(Workflowstate.restart_count)

        return q.all()

    def get_workflow_cum_job_wall_time(self):
        """
        select sum(remote_duration * multiplier_factor) FROM
        invocation as invoc, job_instance as ji WHERE
        invoc.task_submit_seq >= 0 and
        invoc.job_instance_id = ji.job_instance_id and
        invoc.wf_id in (1,2,3) and
        invoc.transformation <> 'condor::dagman'

        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Workflowcumulativejobwalltime
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowcumulativejobwalltime
        """
        q = self.session.query(cast(func.sum(Invocation.remote_duration * JobInstance.multiplier_factor), Float),
                               cast(func.sum(case([(
                                   Invocation.exitcode == 0, Invocation.remote_duration * JobInstance.multiplier_factor
                               )], else_=0)).label("goodput"), Float),
                               cast(func.sum(case([(
                                   Invocation.exitcode > 0, Invocation.remote_duration * JobInstance.multiplier_factor
                               )], else_=0)).label("badput"), Float))

        q = q.filter(Invocation.task_submit_seq >= 0)
        q = q.filter(Invocation.job_instance_id == JobInstance.job_instance_id)

        if self._expand:
            q = q.filter(Invocation.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)

        else:
            q = q.filter(Invocation.wf_id.in_(self._wfs))

        q = q.filter(Invocation.transformation != 'condor::dagman')

        return q.first()

    def get_summary_integrity_metrics(self):
        """

        :param type:    whether integrity type is check | compute
        :param file_type: file type input or output
        :return:
        """
        q = self.session.query(IntegrityMetrics.type,
                                func.sum(IntegrityMetrics.duration).label("duration"),
                                func.sum(IntegrityMetrics.count).label("count"))

        q = q.group_by(IntegrityMetrics.type)

        if self._expand:
            q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            q = q.filter(IntegrityMetrics.wf_id.in_(self._wfs))

        # at most two records grouped by type compute | check
        return q.all()

    def get_tag_metrics(self, name):
        """

        :param name:    what type of tag to aggregate on
        :return:
        """
        q = self.session.query(Tag.name,
                                func.sum(Tag.count).label("count"))

        q = q.group_by(Tag.name)
        q = q.filter(Tag.name == name )

        if self._expand:
            q = q.filter(Tag.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            q = q.filter(Tag.wf_id.in_(self._wfs))


        return q.all()

    def get_integrity_metrics(self):
        """

        :param type:    whether integrity type is check | compute
        :param file_type: file type input or output
        :return:
        """
        q = self.session.query( IntegrityMetrics.type,
                                IntegrityMetrics.file_type,
                                func.sum(IntegrityMetrics.duration).label("duration"),
                                func.sum(IntegrityMetrics.count).label("count"))


        q = q.group_by(IntegrityMetrics.type)
        q = q.group_by(IntegrityMetrics.file_type)


        if self._expand:
            q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        else:
            q = q.filter(IntegrityMetrics.wf_id.in_(self._wfs))

        """
        for result in q.all():
            print result
            print result.type
            print result.file_type
        """
        return q.all()

    def get_submit_side_job_wall_time(self):
        """
        select sum(local_duration * multiplier_factor) FROM
        job_instance as jb_inst, job as jb WHERE
        jb_inst.job_id  = jb.job_id and
        jb.wf_id in (1,2,3) and
        ((not (jb.type_desc ='dax' or jb.type_desc ='dag'))
          or
         ((jb.type_desc ='dax' or jb.type_desc ='dag') and jb_inst.subwf_id is NULL)
        )

        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Cumulativejobwalltimeasseenfromsubmitside
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Cumulativejobwalltimeasseenfromsubmitside
        """
        q = self.session.query(cast(func.sum(JobInstance.local_duration * JobInstance.multiplier_factor), Float).label('wall_time'),
                               cast(func.sum(case([(
                                   JobInstance.exitcode == 0, JobInstance.local_duration * JobInstance.multiplier_factor
                               )], else_=0)).label("goodput"), Float),
                               cast(func.sum(case([(
                                   JobInstance.exitcode > 0, JobInstance.local_duration * JobInstance.multiplier_factor
                               )], else_=0)).label("badput"), Float)
                               )

        q = q.filter(JobInstance.job_id == Job.job_id)

        if self._expand:
            q = q.filter(Job.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == self._root_wf_id)

        else:
            q = q.filter(Job.wf_id.in_(self._wfs))

        if self._expand:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))

        return q.first()

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
        if self._expand and self._is_root_wf:
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

        sq_4 = self.session.query(func.min(Jobstate.timestamp))
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

        sq_8 = self.session.query(func.max(Invocation.exitcode))
        sq_8 = sq_8.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_8 = sq_8.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        #PM-704 the task submit sequence needs to be >= -1 to include prescript status
        sq_8 = sq_8.filter(Invocation.task_submit_seq >= -1)
        sq_8 = sq_8.group_by().subquery()

        JobInstanceSub = orm.aliased(JobInstance)

        sq_9 = self.session.query(Host.hostname)
        sq_9 = sq_9.filter(JobInstanceSub.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_9 = sq_9.filter(Host.host_id == JobInstanceSub.host_id)
        sq_9 = sq_9.subquery()

        JI = orm.aliased(JobInstance)
        sq_10 = self.session.query(func.sum(Invocation.remote_duration * JI.multiplier_factor))
        sq_10 = sq_10.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_10 = sq_10.filter(Invocation.job_instance_id == JI.job_instance_id)
        sq_10 = sq_10.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_10 = sq_10.filter(Invocation.task_submit_seq >= 0)
        sq_10 = sq_10.group_by().subquery()

        sq_11 = self.session.query(func.sum(Invocation.remote_cpu_time))
        sq_11 = sq_11.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_11 = sq_11.filter(Invocation.wf_id == Job.wf_id).correlate(Job)
        sq_11 = sq_11.filter(Invocation.task_submit_seq >= 0)
        sq_11 = sq_11.group_by().subquery()


        q = self.session.query(Job.job_id, JobInstance.job_instance_id, JobInstance.job_submit_seq,
            Job.exec_job_id.label('job_name'), JobInstance.site,
            cast(sq_1.as_scalar() - sq_2.as_scalar(), Float).label('condor_q_time'),
            cast(sq_3.as_scalar() - sq_4.as_scalar(), Float).label('resource_delay'),
            cast(JobInstance.local_duration, Float).label('runtime'),
            cast(sq_5.as_scalar(), Float).label('kickstart'),
            cast(sq_6.as_scalar() - sq_7.as_scalar(), Float).label('post_time'),
            cast(JobInstance.cluster_duration, Float).label('seqexec'),
            sq_8.as_scalar().label('exit_code'),
            sq_9.as_scalar().label('host_name'),
            JobInstance.multiplier_factor,
            cast(sq_10.as_scalar(), Float).label('kickstart_multi'),
            sq_11.as_scalar().label('remote_cpu_time'))
        q = q.filter(JobInstance.job_id == Job.job_id)
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.order_by(JobInstance.job_submit_seq)

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
        sq_7 = self._state_sub_q(['GRID_SUBMIT', 'GLOBUS_SUBMIT'], 'max')
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

        # PM-752 we use the same query that we used to get the count of failed jobs
        q = self._get_total_failed_jobs_status()
        return q.all()

    def get_plots_failed_job_instances(self, final=False, all_jobs=False):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Job+Statistics+file#JobStatisticsfile-Failedjobinstances
        used in the pegasus plots code. is deprecated
        """
        if self._expand:
            return []
        d_or_d = self._dax_or_dag_cond()

        if not final:
            q = self.session.query(JobInstance.job_instance_id, JobInstance.job_submit_seq)
        else:
            q = self.session.query(JobInstance.job_instance_id, func.max(JobInstance.job_submit_seq))
        q = q.filter(Job.wf_id.in_(self._wfs))
        q = q.filter(Job.job_id == JobInstance.job_id)
        if not all_jobs:
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))
        q = q.filter(JobInstance.exitcode != 0).filter(JobInstance.exitcode != None)
        if final:
            q = q.group_by(JobInstance.job_id)
        q = q.order_by(JobInstance.job_submit_seq)

        return q.all()

    def get_job_instance_info(self, job_instance_id=None):
        """
        Job instance information.  Pulls all or for one instance.
        https://confluence.pegasus.isi.edu/pages/viewpage.action?pageId=14876831
        """
        if self._expand:
            return []

        sq_0 = self.session.query(Workflow.submit_dir)
        sq_0 = sq_0.filter(Workflow.wf_id == JobInstance.subwf_id).correlate(JobInstance)
        sq_0 = sq_0.subquery()

        sq_1 = self.session.query(Job.exec_job_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id).correlate(JobInstance)
        sq_1 = sq_1.subquery()

        sq_2 = self.session.query(Job.submit_file)
        sq_2 = sq_2.filter(Job.job_id == JobInstance.job_id).correlate(JobInstance)
        sq_2 = sq_2.subquery()

        sq_3 = self.session.query(Job.executable)
        sq_3 = sq_3.filter(Job.job_id == JobInstance.job_id).correlate(JobInstance)
        sq_3 = sq_3.subquery()

        sq_4 = self.session.query(Job.argv)
        sq_4 = sq_4.filter(Job.job_id == JobInstance.job_id).correlate(JobInstance)
        sq_4 = sq_4.subquery()

        sq_5 = self.session.query(Workflow.submit_dir)
        sq_5 = sq_5.filter(Workflow.wf_id == self._root_wf_id).subquery()

        sq_6 = self.session.query(func.max(Jobstate.jobstate_submit_seq).label('max_job_submit_seq'))
        sq_6 = sq_6.filter(Jobstate.job_instance_id == job_instance_id)
        sq_6 = sq_6.subquery()

        sq_7 = self.session.query(Jobstate.state)
        sq_7 = sq_7.filter(Jobstate.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_7 = sq_7.filter(Jobstate.jobstate_submit_seq == sq_6.as_scalar())
        sq_7 = sq_7.subquery()

        sq_8 = self.session.query(Invocation.executable)
        sq_8 = sq_8.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_8 = sq_8.filter(Invocation.task_submit_seq == -1)
        sq_8 = sq_8.subquery()

        sq_9 = self.session.query(Invocation.argv)
        sq_9 = sq_9.filter(Invocation.job_instance_id == JobInstance.job_instance_id).correlate(JobInstance)
        sq_9 = sq_9.filter(Invocation.task_submit_seq == -1)
        sq_9 = sq_9.subquery()

        sq_10 = self.session.query(Host.hostname)
        sq_10 = sq_10.filter(Host.host_id == JobInstance.host_id).correlate(JobInstance)
        sq_10 = sq_10.subquery()

        q = self.session.query(JobInstance.job_instance_id, JobInstance.site, JobInstance.stdout_file, JobInstance.stderr_file,
                JobInstance.stdout_text, JobInstance.stderr_text, JobInstance.work_dir,
                sq_0.as_scalar().label('subwf_dir'),
                sq_1.as_scalar().label('job_name'),
                sq_2.as_scalar().label('submit_file'),
                sq_3.as_scalar().label('executable'),
                sq_4.as_scalar().label('argv'),
                sq_5.as_scalar().label('submit_dir'),
                sq_7.as_scalar().label('state'),
                sq_8.as_scalar().label('pre_executable'),
                sq_9.as_scalar().label('pre_argv'),
                sq_10.as_scalar().label('hostname')
                )

        if job_instance_id:
            q = q.filter(JobInstance.job_instance_id == job_instance_id)

        return q.all()

    def get_invocation_info(self, ji_id=None):
        """
        SELECT task_submit_seq, exitcode, executable, argv, transformation, abs_task_id
        FROM invocation WHERE job_instance_id = 7 and wf_id = 1
        """
        if self._expand or not ji_id:
            return []

        q = self.session.query(Invocation.task_submit_seq, Invocation.exitcode,
                Invocation.executable, Invocation.argv, Invocation.transformation, Invocation.abs_task_id)
        q = q.filter(Invocation.job_instance_id == ji_id)
        q = q.filter(Invocation.wf_id.in_(self._wfs))

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


        sq_1 = self.session.query(func.sum(Invocation.remote_duration * JobInstance.multiplier_factor))
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
        SELECT transformation,
               count(invocation_id) as count,
               min(remote_duration * multiplier_factor) as min,
               count(CASE WHEN (invoc.exitcode = 0 and invoc.exitcode is NOT NULL) THEN invoc.exitcode END) AS success,
               count(CASE WHEN (invoc.exitcode != 0 and invoc.exitcode is NOT NULL) THEN invoc.exitcode END) AS failure,
               max(remote_duration * multiplier_factor) as max,
               avg(remote_duration * multiplier_factor) as avg,
               sum(remote_duration * multiplier_factor) as sum FROM
        invocation as invoc, job_instance as ji WHERE
        invoc.job_instance_id = ji.job_instance_id and
        invoc.wf_id IN (1,2,3) GROUP BY transformation
        """
        q = self.session.query(Invocation.transformation,
                func.count(Invocation.invocation_id).label('count'),
                cast(func.min(Invocation.remote_duration * JobInstance.multiplier_factor), Float).label('min'),
                func.count(case([(Invocation.exitcode == 0, Invocation.exitcode)])).label('success'),
                func.count(case([(Invocation.exitcode != 0, Invocation.exitcode)])).label('failure'),
                cast(func.max(Invocation.remote_duration * JobInstance.multiplier_factor), Float).label('max'),
                cast(func.avg(Invocation.remote_duration * JobInstance.multiplier_factor), Float).label('avg'),
                cast(func.sum(Invocation.remote_duration * JobInstance.multiplier_factor), Float).label('sum'))
        q = q.filter(Invocation.job_instance_id == JobInstance.job_instance_id)
        q = q.filter(Invocation.wf_id.in_(self._wfs))
        q = q.group_by(Invocation.transformation)

        return q.all()

    #
    # Runtime queries
    # https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
    #

    def _get_date_divisors(self):
        vals = {
        'month': 2629743,
        'week': 604800,
        'day': 86400,
        'hour': 3600
        }
        return vals[self._time_filter_mode]

    def _get_host_filter(self):
        if self._host_filter == None:
            return None
        elif isinstance(self._host_filter, type('str')):
            return Host.hostname == self._host_filter
        elif isinstance(self._host_filter, type([])):
            return Host.hostname.in_(self._host_filter)
        else:
            return None

    def _get_xform_filter(self):
        if self._xform_filter['include'] != None and \
            self._xform_filter['exclude'] != None:
            self.log.error('Can\'t set both transform include and exclude - reset s.set_transformation_filter()')
            return None
        elif self._xform_filter['include'] == None and \
            self._xform_filter['exclude'] == None:
            return None
        elif self._xform_filter['include'] != None:
            if isinstance(self._xform_filter['include'], type('str')):
                return Invocation.transformation == self._xform_filter['include']
            elif isinstance(self._xform_filter['include'], type([])):
                return Invocation.transformation.in_(self._xform_filter['include'])
            else:
                return None
        elif self._xform_filter['exclude'] != None:
            if isinstance(self._xform_filter['exclude'], type('str')):
                return Invocation.transformation != self._xform_filter['exclude']
            elif isinstance(self._xform_filter['exclude'], type([])):
                return not_(Invocation.transformation.in_(self._xform_filter['exclude']))
            else:
                return None
        else:
            return None

    def get_invocation_by_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
        """
        q = self.session.query(
                (cast(Invocation.start_time / self._get_date_divisors(), Integer)).label('date_format'),
                func.count(Invocation.invocation_id).label('count'),
                cast(func.sum(Invocation.remote_duration), Float).label('total_runtime')
        )
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Invocation.wf_id == Workflow.wf_id)
        if self._get_xform_filter() is not None:
            q = q.filter(self._get_xform_filter())
        q = q.group_by('date_format').order_by('date_format')

        return q.all()

    def get_jobs_run_by_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
        """
        q = self.session.query(
                (cast(Jobstate.timestamp / self._get_date_divisors(), Integer)).label('date_format'),
                func.count(JobInstance.job_instance_id).label('count'),
                cast(func.sum(JobInstance.local_duration), Float).label('total_runtime')
        )
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Workflow.wf_id == Job.wf_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        q = q.filter(Jobstate.state == 'EXECUTE')
        q = q.filter(JobInstance.local_duration != None)

        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        q = q.group_by('date_format').order_by('date_format')

        return q.all()


    def get_invocation_by_time_per_host(self, host=None):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
        """
        q = self.session.query(
            (cast(Invocation.start_time / self._get_date_divisors(), Integer)).label('date_format'),
            Host.hostname.label('host_name'),
            func.count(Invocation.invocation_id).label('count'),
            cast(func.sum(Invocation.remote_duration), Float).label('total_runtime')
        )
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Invocation.wf_id == Workflow.wf_id)
        q = q.filter(JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter(JobInstance.host_id == Host.host_id)
        if self._get_host_filter() is not None:
            q = q.filter(self._get_host_filter())
        if self._get_xform_filter() is not None:
            q = q.filter(self._get_xform_filter())
        q = q.group_by('date_format', 'host_name').order_by('date_format')

        return q.all()

    def get_jobs_run_by_time_per_host(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
        """
        q = self.session.query(
                (cast(Jobstate.timestamp / self._get_date_divisors(), Integer)).label('date_format'),
                Host.hostname.label('host_name'),
                func.count(JobInstance.job_instance_id).label('count'),
                cast(func.sum(JobInstance.local_duration), Float).label('total_runtime')
        )
        q = q.filter(Workflow.root_wf_id == self._root_wf_id)
        q = q.filter(Workflow.wf_id == Job.wf_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Jobstate.job_instance_id == JobInstance.job_instance_id)
        q = q.filter(Jobstate.state == 'EXECUTE')
        q = q.filter(JobInstance.host_id == Host.host_id)
        if self._get_host_filter() is not None:
            q = q.filter(self._get_host_filter())
        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        q = q.group_by('date_format', 'host_name').order_by('date_format')

        return q.all()

