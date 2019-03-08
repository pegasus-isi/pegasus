__author__ = "Rajiv Mayani"

import logging

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.schema import *
from Pegasus.db.errors import StampedeDBNotFoundError

# Main stats class.
class StampedeWorkflowStatistics(object):
    def __init__(self, connString=None, expand_workflow=True):
        self.log = logging.getLogger("%s.%s" % (self.__module__, self.__class__.__name__))
        try:
            self.session = connection.connect(connString)
        except (connection.ConnectionError, DBAdminError) as e:
            self.log.exception(e)
            raise StampedeDBNotFoundError

        self._expand = expand_workflow

        self._root_wf_id = []
        self._root_wf_uuid = []
        self.all_workflows = None
        self._job_filter_mode = None
        self._time_filter_mode = None
        self._host_filter = None
        self._xform_filter = {'include':None, 'exclude':None}

        self._wfs = []

    def initialize(self, root_wf_uuid = None, root_wf_id = None):
        if root_wf_uuid is None and root_wf_id is None:
            self.log.error('Either root_wf_uuid or root_wf_id is required')
            raise ValueError('Either root_wf_uuid or root_wf_id is required')

        if root_wf_uuid == '*' or root_wf_id == '*':
            self.all_workflows = True

        q = self.session.query(Workflow.root_wf_id, Workflow.wf_id, Workflow.wf_uuid)

        if root_wf_uuid:
            if root_wf_uuid == '*':
                q = q.filter(Workflow.root_wf_id == Workflow.wf_id)
            else:
                q = q.filter(Workflow.wf_uuid.in_(root_wf_uuid))
        else:
            if root_wf_id == '*':
                q = q.filter(Workflow.root_wf_id == Workflow.wf_id)
            else:
                q = q.filter(Workflow.root_wf_id.in_(root_wf_id))

        result = q.all()

        for workflow in result:
            if workflow.root_wf_id != workflow.wf_id:
                self.log.error('Only root level workflows are supported %s', workflow.wf_uuid)
                raise ValueError('Only root level workflows are supported %s', workflow.wf_uuid)

            self._root_wf_id.append(workflow.wf_id)
            self._root_wf_uuid.append(workflow.wf_uuid)

        if root_wf_uuid and root_wf_uuid != '*' and len(root_wf_uuid) != len(self._root_wf_uuid):
            self.log.error('Some workflows were not found')
            raise ValueError('Some workflows were not found')
        elif root_wf_id and root_wf_id != '*' and len(root_wf_id) != len(self._root_wf_id):
            self.log.error('Some workflows were not found')
            raise ValueError('Some workflows were not found')

        # Initialize filters with default value
        self.set_job_filter()
        self.set_time_filter()
        self.set_host_filter()
        self.set_transformation_filter()

        return True

    def __filter_all(self, q, w=None):

        if w is None:
            w = Workflow

        if not self.all_workflows:
            q = q.filter(w.root_wf_id.in_(self._root_wf_id))

        return q

    def __filter_roots_only(self, q, w=None):

        if w is None:
            w = Workflow

        if self.all_workflows:
            q = q.filter(w.root_wf_id == w.wf_id)
        else:
            q = q.filter(w.wf_id.in_(self._root_wf_id))

        return q

    def get_workflow_ids(self):
        q = self.session.query(Workflow.root_wf_id, Workflow.wf_id, Workflow.wf_uuid)
        q = self.__filter_all(q)
        q = q.order_by(Workflow.root_wf_id)
        return q.all()

    def get_workflow_details(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowdetails
        """
        q = self.session.query(Workflow.wf_id, Workflow.wf_uuid,
                               Workflow.parent_wf_id, Workflow.root_wf_id, Workflow.dag_file_name,
                               Workflow.submit_hostname, Workflow.submit_dir, Workflow.planner_arguments,
                               Workflow.user, Workflow.grid_dn, Workflow.planner_version,
                               Workflow.dax_label, Workflow.dax_version)

        q = self.__filter_roots_only(q)

        return q.all()

    def get_total_tasks_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaltask
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaltasks
        """
        q = self.session.query(Task.task_id)

        if self._expand:
            q = self.__filter_all(q)
        else:
            q = self.__filter_roots_only(q)

        q = q.filter(Task.wf_id == Workflow.wf_id)
        q = q.filter(Task.job_id == Job.job_id)

        if self._get_job_filter(Task) is not None:
            q = q.filter(self._get_job_filter(Task))

        return q.count()

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

        if self._expand:
            sq_1 = self.__filter_all(sq_1, w)
        else:
            sq_1 = self.__filter_roots_only(sq_1, w)

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

    def get_total_tasks_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaltaskretries
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaltaskretries
        """
        sq_1 = self.session.query(Workflow.wf_id.label('wid'), Invocation.abs_task_id.label('tid'))

        if self._expand:
            sq_1 = self.__filter_all(sq_1)
        else:
            sq_1 = self.__filter_roots_only(sq_1)

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

    def get_total_jobs_status(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Totaljobs
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Totaljobs
        """
        q = self.session.query(Job.job_id)

        if self._expand:
            q = self.__filter_all(q)
        else:
            q = self.__filter_roots_only(q)

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

        if self._expand:
            sq_1 = self.__filter_all(sq_1)
        else:
            sq_1 = self.__filter_roots_only(sq_1)

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

    def get_total_jobs_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-TotalJobRetries
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-TotalJobRetries
        """
        d_or_d = self._dax_or_dag_cond()

        sq_1 = self.session.query(func.count(Job.job_id))

        if self._expand:
            sq_1 = self.__filter_all(sq_1)
        else:
            sq_1 = self.__filter_roots_only(sq_1)

        sq_1 = sq_1.filter(Job.wf_id == Workflow.wf_id)
        sq_1 = sq_1.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_1 = sq_1.filter(self._get_job_filter())

        sq_1 = sq_1.subquery()


        sq_2 = self.session.query(func.count(distinct(JobInstance.job_id)))

        if self._expand:
            sq_2 = self.__filter_all(sq_2)
        else:
            sq_2 = self.__filter_roots_only(sq_2)

        sq_2 = sq_2.filter(Job.wf_id == Workflow.wf_id)
        sq_2 = sq_2.filter(Job.job_id == JobInstance.job_id)
        if self._get_job_filter() is not None:
            sq_2 = sq_2.filter(self._get_job_filter())

        sq_2 = sq_2.subquery()

        q = self.session.query((sq_1.as_scalar() - sq_2.as_scalar()).label('total_job_retries'))

        return q.all()[0].total_job_retries

    def get_workflow_states(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Summary#WorkflowSummary-Workflowwalltime
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowwalltime
        """
        q = self.session.query(Workflowstate.wf_id, Workflowstate.state, Workflowstate.timestamp,
                               Workflowstate.restart_count, Workflowstate.status)

        if self.all_workflows:
            q = q.filter(Workflowstate.wf_id == Workflow.wf_id)
            q = q.filter(Workflow.root_wf_id == Workflow.wf_id)
        else:
            q = q.filter(Workflowstate.wf_id.in_(self._root_wf_id))

        q = q.order_by(Workflowstate.wf_id, Workflowstate.restart_count)

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
            q = self.__filter_all(q)
            q = q.filter(Invocation.wf_id == Workflow.wf_id)
        else:
            if self.all_workflows:
                q = self.__filter_roots_only(q)
                q = q.filter(Invocation.wf_id == Workflow.wf_id)
            else:
                q = q.filter(Invocation.wf_id.in_(self._root_wf_id))

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
            q = self.__filter_all(q)
            q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
        else:
            if self.all_workflows:
                q = self.__filter_roots_only(q)
                q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
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
        q = q.filter(Tag.name == name)

        if self._expand:
            q = self.__filter_all(q)
            q = q.filter(Tag.wf_id == Workflow.wf_id)
        else:
            if self.all_workflows:
                q = self.__filter_roots_only(q)
                q = q.filter(Tag.wf_id == Workflow.wf_id)
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
            q = self.__filter_all(q)
            q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
        else:
            if self.all_workflows:
                q = self.__filter_roots_only(q)
                q = q.filter(IntegrityMetrics.wf_id == Workflow.wf_id)
            else:
                q = q.filter(IntegrityMetrics.wf_id.in_(self._wfs))

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
            q = self.__filter_all(q)
            q = q.filter(Job.wf_id == Workflow.wf_id)
        else:
            if self.all_workflows:
                q = self.__filter_roots_only(q)
                q = q.filter(Job.wf_id == Workflow.wf_id)
            else:
                q = q.filter(Job.wf_id.in_(self._root_wf_id))

        if self._expand:
            d_or_d = self._dax_or_dag_cond()
            q = q.filter(or_(not_(d_or_d), and_(d_or_d, JobInstance.subwf_id == None)))

        return q.first()

    def get_workflow_retries(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Workflow+Statistics+file#WorkflowStatisticsfile-Workflowretries
        """
        sq_1 = self.session.query(func.max(Workflowstate.restart_count).label('retry'))

        if self._expand:
            sq_1 = self.__filter_all(sq_1)
        else:
            sq_1 = self.__filter_roots_only(sq_1)

        sq_1 = sq_1.filter(Workflowstate.wf_id == Workflow.wf_id)
        sq_1 = sq_1.group_by(Workflowstate.wf_id)
        sq_1 = sq_1.subquery()

        q = self.session.query(func.sum(sq_1.c.retry).label('total_retry'))

        return q.one().total_retry

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
        q = q.filter(Workflow.wf_id == Invocation.wf_id)
        q = q.filter(Invocation.job_instance_id == JobInstance.job_instance_id)
        q = self.__filter_all(q)
        q = q.group_by(Invocation.transformation)

        return q.all()

    def get_invocation_by_time(self):
        """
        https://confluence.pegasus.isi.edu/display/pegasus/Additional+queries
        """
        q = self.session.query(
            (cast(Invocation.start_time / self._get_date_divisors(), Integer)).label('date_format'),
            func.count(Invocation.invocation_id).label('count'),
            cast(func.sum(Invocation.remote_duration), Float).label('total_runtime')
        )
        q = self.__filter_all(q)
        q = q.filter(Invocation.wf_id == Workflow.wf_id)
        if self._get_xform_filter() is not None:
            q = q.filter(self._get_xform_filter())
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
        q = self.__filter_all(q)
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
        q = self.__filter_all(q)
        q = q.filter(Workflow.wf_id == Job.wf_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.filter(JobInstance.host_id == Host.host_id)
        q = q.filter(Jobstate.state == 'EXECUTE')

        if self._get_host_filter() is not None:
            q = q.filter(self._get_host_filter())
        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())

        q = q.group_by('date_format', 'host_name').order_by('date_format')

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
        q = q.filter(Workflow.wf_id == Job.wf_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)

        q = self.__filter_all(q)
        q = q.filter(Jobstate.state == 'EXECUTE')
        q = q.filter(JobInstance.local_duration != None)

        if self._get_job_filter() is not None:
            q = q.filter(self._get_job_filter())
        q = q.group_by('date_format').order_by('date_format')

        return q.all()

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
        if self._host_filter is None:
            return None
        elif isinstance(self._host_filter, type('str')):
            return Host.hostname == self._host_filter
        elif isinstance(self._host_filter, type([])):
            return Host.hostname.in_(self._host_filter)
        else:
            return None

    def _get_xform_filter(self):
        if self._xform_filter['include'] is not None and \
                        self._xform_filter['exclude'] is not None:
            self.log.error('Can\'t set both transform include and exclude - reset s.set_transformation_filter()')
            return None
        elif self._xform_filter['include'] is None and \
                        self._xform_filter['exclude'] is None:
            return None
        elif self._xform_filter['include'] is not None:
            if isinstance(self._xform_filter['include'], type('str')):
                return Invocation.transformation == self._xform_filter['include']
            elif isinstance(self._xform_filter['include'], type([])):
                return Invocation.transformation.in_(self._xform_filter['include'])
            else:
                return None
        elif self._xform_filter['exclude'] is not None:
            if isinstance(self._xform_filter['exclude'], type('str')):
                return Invocation.transformation != self._xform_filter['exclude']
            elif isinstance(self._xform_filter['exclude'], type([])):
                return not_(Invocation.transformation.in_(self._xform_filter['exclude']))
            else:
                return None
        else:
            return None

