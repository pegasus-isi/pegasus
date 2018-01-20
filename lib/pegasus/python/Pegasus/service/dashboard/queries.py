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

__author__ = 'Rajiv Mayani'
import logging

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.db.schema import *
from sqlalchemy.orm.exc import *
from sqlalchemy.util._collections import KeyedTuple

log = logging.getLogger(__name__)


class MasterDBNotFoundError(Exception):
    pass


class MasterDatabase(object):
    def __init__(self, conn_string, debug=False):
        self._dbg = debug

        if conn_string is None:
            raise ValueError('Connection string is required')

        try:
            self.session = connection.connect(conn_string)
        except connection.ConnectionError as e:
            log.exception(e)
            message = e

            while isinstance(message, Exception):
                message = message.message

            if 'attempt to write a readonly database' in message:
                raise DBAdminError(message)

            raise MasterDBNotFoundError(e)

    def close(self):
        log.debug('close')
        self.session.close()

    def get_wf_db_url(self, wf_id):
        """
        Given a work-flow UUID, query the master database to get the connection URL for the work-flow's STAMPEDE database.
        """

        w = orm.aliased(DashboardWorkflow, name='w')

        q = self.session.query(w.db_url)
        q = q.filter(w.wf_id == wf_id)

        return q.one().db_url

    def get_wf_id_url(self, root_wf_id):
        """
        Given a work-flow UUID, query the master database to get the connection URL for the work-flow's STAMPEDE database.
        """
        q = self.session.query(DashboardWorkflow)

        if root_wf_id is None:
            raise ValueError('root_wf_id cannot be None')

        m_wf_id = str(root_wf_id)
        if m_wf_id.isdigit():
            q = q.filter(DashboardWorkflow.wf_id == root_wf_id)
        else:
            q = q.filter(DashboardWorkflow.wf_uuid == root_wf_id)

        q = q.one()

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
        qmax = self.session.query(
            DashboardWorkflowstate.wf_id,
            func.max(DashboardWorkflowstate.timestamp).label('max_time')
        )
        qmax = qmax.group_by(DashboardWorkflowstate.wf_id)

        qmax = qmax.subquery('max_timestamp')

        state = case(
            [
                (ws.status == None, 'Running'), (ws.status == 0, 'Successful'),
                (ws.status != 0, 'Failed')
            ],
            else_='Undefined'
        ).label('state')

        q = self.session.query(
            w.wf_id, w.wf_uuid, w.timestamp, w.dag_file_name,
            w.submit_hostname, w.submit_dir, w.planner_arguments, w.user,
            w.grid_dn, w.planner_version, w.dax_label, w.dax_version, w.db_url,
            w.archived, ws.reason, ws.status, state
        )

        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.wf_id == qmax.c.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)

        # Get Total Count. Need this to pass to jQuery Datatable.
        count = q.count()
        if count == 0:
            return 0, 0, []

        if 'filter' in table_args:
            filter_text = '%' + table_args['filter'] + '%'
            q = q.filter(
                or_(
                    w.dax_label.like(filter_text),
                    w.submit_hostname.like(filter_text),
                    w.submit_dir.like(filter_text), state.like(filter_text)
                )
            )

        if 'time_filter' in table_args:
            time_filter = table_args['time_filter']
            current_time = int(time.time())

            if time_filter == 'day':
                q = q.filter(
                    between(w.timestamp, current_time - 86400, current_time)
                )
            elif time_filter == 'week':
                q = q.filter(
                    between(w.timestamp, current_time - 604800, current_time)
                )
            elif time_filter == 'month':
                q = q.filter(
                    between(w.timestamp, current_time - 2620800, current_time)
                )
            elif time_filter == 'year':
                q = q.filter(
                    between(
                        w.timestamp, current_time - 31449600, current_time
                    )
                )

        # Get Total Count. Need this to pass to jQuery Datatable.
        filtered = q.count()

        if filtered == 0:
            return count, 0, []

        display_columns = [
            w.dax_label, w.submit_hostname, w.submit_dir, state, w.timestamp
        ]

        if 'sort-col-count' in table_args:
            for i in range(table_args['sort-col-count']):

                if 'iSortCol_' + str(i) in table_args:
                    if 'sSortDir_' + str(i) in table_args and table_args[
                        'sSortDir_' + str(i)
                    ] == 'asc':
                        i = table_args['iSortCol_' + str(i)]

                        if 0 <= i < len(display_columns):
                            q = q.order_by(display_columns[i])
                        else:
                            raise ValueError(
                                'Invalid column (%s) in work-flow listing ' % i
                            )
                    else:
                        i = table_args['iSortCol_' + str(i)]

                        if 0 <= i < len(display_columns):
                            q = q.order_by(desc(display_columns[i]))
                        else:
                            raise ValueError(
                                'Invalid column (%s) in work-flow listing ' % i
                            )

        else:
            # Default sorting order
            q = q.order_by(desc(w.timestamp))

        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit(table_args['limit'])
            q = q.offset(table_args['offset'])

        return count, filtered, q.all()

    def get_workflow_counts(self):

        w = orm.aliased(DashboardWorkflow, name='w')
        ws = orm.aliased(DashboardWorkflowstate, name='ws')

        # Get last state change for each work-flow.
        qmax = self.session.query(
            DashboardWorkflowstate.wf_id,
            func.max(DashboardWorkflowstate.timestamp).label('max_time')
        )
        qmax = qmax.group_by(DashboardWorkflowstate.wf_id)

        qmax = qmax.subquery('max_timestamp')

        q = self.session.query(
            func.count(w.wf_id).label('total'),
            func.sum(case([(ws.status == 0, 1)], else_=0)).label('success'),
            func.sum(case([(ws.status != 0, 1)], else_=0)).label('fail'),
            func.sum(case([(ws.status == None, 1)], else_=0)).label('others')
        )

        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.wf_id == qmax.c.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)

        return q.one()


class WorkflowInfo(object):
    def __init__(
        self, conn_string=None, wf_id=None, wf_uuid=None, debug=False
    ):
        self._dbg = debug

        if conn_string is None:
            raise ValueError('Connection string is required')

        try:
            self.session = connection.connect(conn_string)
        except connection.ConnectionError as e:
            log.exception(e)
            message = e

            while isinstance(message, Exception):
                message = message.message

            if 'attempt to write a readonly database' in message:
                raise DBAdminError(message)

            raise StampedeDBNotFoundError

        self.initialize(wf_id, wf_uuid)

    def initialize(self, wf_id=None, wf_uuid=None):

        if not wf_id and not wf_uuid:
            raise ValueError('Workflow ID or Workflow UUID is required.')

        if wf_id:
            self._wf_id = wf_id
            return

        if wf_uuid:
            q = self.session.query(Workflow.wf_id)
            q = q.filter(Workflow.wf_uuid == wf_uuid)

            self._wf_id = q.one().wf_id

    def get_workflow_information(self):

        qmax = self.session.query(
            func.max(Workflowstate.timestamp).label('max_time')
        )
        qmax = qmax.filter(Workflowstate.wf_id == self._wf_id)

        qmax = qmax.subquery('max_timestamp')

        ws = orm.aliased(Workflowstate, name='ws')
        w = orm.aliased(Workflow, name='w')

        q = self.session.query(
            w.wf_id, w.wf_uuid, w.parent_wf_id, w.root_wf_id, w.dag_file_name,
            w.submit_hostname, w.submit_dir, w.planner_arguments, w.user,
            w.grid_dn, w.planner_version, w.dax_label, w.dax_version,
            case(
                [
                    (ws.status == None, 'Running'),
                    (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')
                ],
                else_='Undefined'
            ).label('state'), ws.reason, ws.timestamp
        )

        q = q.filter(w.wf_id == self._wf_id)
        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)

        return q.one()

    def get_workflow_job_counts(self):

        qmax = self.__get_maxjss_subquery()

        q = self.session.query(func.count(Job.wf_id).label('total'))
        q = q.add_column(
            func.sum(
                case(
                    [(Job.type_desc == 'dag', 1), (Job.type_desc == 'dax', 1)],
                    else_=0
                )
            ).label('total_workflow')
        )
        q = q.filter(Job.wf_id == self._wf_id)

        totals = q.one()

        q = self.session.query(func.count(Job.wf_id).label('total'))
        q = q.add_column(
            func.sum(case([(JobInstance.exitcode == 0, 1)],
                          else_=0)).label('success')
        )
        q = q.add_column(
            func.sum(
                case(
                    [
                        (
                            JobInstance.exitcode == 0,
                            case(
                                [
                                    (Job.type_desc == 'dag', 1),
                                    (Job.type_desc == 'dax', 1)
                                ],
                                else_=0
                            )
                        )
                    ],
                    else_=0
                )
            ).label('success_workflow')
        )

        q = q.add_column(
            func.sum(case([(JobInstance.exitcode != 0, 1)],
                          else_=0)).label('fail')
        )
        q = q.add_column(
            func.sum(
                case(
                    [
                        (
                            JobInstance.exitcode != 0,
                            case(
                                [
                                    (Job.type_desc == 'dag', 1),
                                    (Job.type_desc == 'dax', 1)
                                ],
                                else_=0
                            )
                        )
                    ],
                    else_=0
                )
            ).label('fail_workflow')
        )

        q = q.add_column(
            func.sum(case([(JobInstance.exitcode == None, 1)],
                          else_=0)).label('running')
        )
        q = q.add_column(
            func.sum(
                case(
                    [
                        (
                            JobInstance.exitcode == None,
                            case(
                                [
                                    (Job.type_desc == 'dag', 1),
                                    (Job.type_desc == 'dax', 1)
                                ],
                                else_=0
                            )
                        )
                    ],
                    else_=0
                )
            ).label('running_workflow')
        )

        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(Job.job_id == qmax.c.job_id)
        q = q.filter(JobInstance.job_submit_seq == qmax.c.max_jss)

        counts = q.one()

        out = KeyedTuple(
            [
                totals.total, totals.total_workflow, totals.total -
                (counts.success + counts.fail + counts.running),
                totals.total_workflow - (
                    counts.success_workflow + counts.fail_workflow +
                    counts.running_workflow
                ), counts.success, counts.success_workflow, counts.fail,
                counts.fail_workflow, counts.running, counts.running_workflow
            ],
            labels=[
                "total", "total_workflow", "others", "others_workflow",
                "success", "success_workflow", "fail", "fail_workflow",
                "running", "running_workflow"
            ]
        )

        return out

    def get_job_information(self, job_id, job_instance_id):

        q = self.session.query(
            Job.exec_job_id, Job.clustered, JobInstance.job_instance_id,
            JobInstance.work_dir, JobInstance.exitcode,
            JobInstance.stdout_file, JobInstance.stderr_file, Host.site,
            Host.hostname, Host.ip
        )
        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.outerjoin(Host, JobInstance.host_id == Host.host_id)

        return q.one()

    def get_job_instances(self, job_id):

        q = self.session.query(
            Job.exec_job_id, JobInstance.job_instance_id, JobInstance.exitcode,
            JobInstance.job_submit_seq
        )
        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.order_by(desc(JobInstance.job_submit_seq))

        return q.all()

    def get_job_states(self, job_id, job_instance_id):

        q = self.session.query(
            Jobstate.state, Jobstate.reason, Jobstate.timestamp
        )
        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Jobstate.job_instance_id)

        q = q.order_by(asc(Jobstate.jobstate_submit_seq))

        return q.all()

    def _jobs_by_type(self):
        qmax = self.__get_jobs_maxjss_sq()

        q = self.session.query(
            Job.job_id, JobInstance.job_instance_id, Job.exec_job_id,
            JobInstance.exitcode
        )

        q = q.filter(Job.wf_id == self._wf_id)

        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.filter(Job.job_id == qmax.c.job_id)
        q = q.filter(JobInstance.job_submit_seq == qmax.c.max_jss)

        q = q.group_by(JobInstance.job_id)

        return q

    def get_failed_jobs(self, **table_args):

        q = self._jobs_by_type()
        q = q.filter(JobInstance.exitcode != 0
                     ).filter(JobInstance.exitcode != None)

        # Get Total Count. Need this to pass to jQuery Datatable.
        count = q.count()
        if count == 0:
            return (0, 0, [])

        filtered = count
        if 'filter' in table_args:
            filter_text = '%' + table_args['filter'] + '%'
            q = q.filter(
                or_(
                    Job.exec_job_id.like(filter_text),
                    JobInstance.exitcode.like(filter_text)
                )
            )

            # Get Total Count. Need this to pass to jQuery Datatable.
            filtered = q.count()

            if filtered == 0:
                return (count, 0, [])

        display_columns = [Job.exec_job_id, JobInstance.exitcode]

        if 'sort-col-count' in table_args:
            for i in range(table_args['sort-col-count']):

                if 'iSortCol_' + str(i) in table_args:
                    sort_order = desc

                    if 'sSortDir_' + str(i) in table_args and table_args[
                        'sSortDir_' + str(i)
                    ] == 'asc':
                        sort_order = asc

                    i = table_args['iSortCol_' + str(i)]

                    if i >= 0 and i < len(display_columns):
                        q = q.order_by(sort_order(display_columns[i]))
                    elif i >= len(display_columns) and i < 4:
                        pass
                    else:
                        raise ValueError(
                            'Invalid column(%s) in failed jobs listing ' % i
                        )

        else:
            # Default sorting order
            q = q.order_by(desc(Job.exec_job_id))

        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit(table_args['limit'])
            q = q.offset(table_args['offset'])

        return count, filtered, q.all()

    def get_successful_jobs(self, **table_args):

        q = self._jobs_by_type()

        q = q.add_column(JobInstance.local_duration)
        q = q.add_column(JobInstance.cluster_duration)
        duration = case(
            [(Job.clustered == 1, JobInstance.cluster_duration)],
            else_=JobInstance.local_duration
        ).label('duration')
        q = q.add_column(duration)

        q = q.filter(JobInstance.exitcode == 0
                     ).filter(JobInstance.exitcode != None)

        # Get Total Count. Need this to pass to jQuery Datatable.
        count = q.count()
        if count == 0:
            return (0, 0, [])

        filtered = count
        if 'filter' in table_args:
            filter_text = '%' + table_args['filter'] + '%'
            q = q.filter(or_(Job.exec_job_id.like(filter_text)))

            # Get Total Count. Need this to pass to jQuery Datatable.
            filtered = q.count()

            if filtered == 0:
                return count, 0, []

        display_columns = [Job.exec_job_id, duration]

        if 'sort-col-count' in table_args:
            for i in range(table_args['sort-col-count']):

                if 'iSortCol_' + str(i) in table_args:
                    sort_order = desc

                    if 'sSortDir_' + str(i) in table_args and table_args[
                        'sSortDir_' + str(i)
                    ] == 'asc':
                        sort_order = asc

                    i = table_args['iSortCol_' + str(i)]

                    if i >= 0 and i < len(display_columns):
                        q = q.order_by(sort_order(display_columns[i]))
                    else:
                        raise ValueError(
                            'Invalid column(%s) in successful jobs listing ' %
                            i
                        )

        else:
            # Default sorting order
            q = q.order_by(desc(Job.exec_job_id))

        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit(table_args['limit'])
            q = q.offset(table_args['offset'])

        return count, filtered, q.all()

    def get_other_jobs(self, **table_args):

        q = self._jobs_by_type()

        q = q.add_column(JobInstance.local_duration)
        q = q.add_column(JobInstance.cluster_duration)
        q = q.add_column(
            case(
                [(Job.clustered == 1, JobInstance.cluster_duration)],
                else_=JobInstance.local_duration
            ).label('duration')
        )

        q = q.filter(JobInstance.exitcode == None)

        # Get Total Count. Need this to pass to jQuery Datatable.
        count = q.count()
        if count == 0:
            return 0, 0, []

        filtered = count
        if 'filter' in table_args:
            filter_text = '%' + table_args['filter'] + '%'
            q = q.filter(or_(Job.exec_job_id.like(filter_text)))

            # Get Total Count. Need this to pass to jQuery Datatable.
            filtered = q.count()

            if filtered == 0:
                return count, 0, []

        display_columns = [Job.exec_job_id]

        if 'sort-col-count' in table_args:
            for i in range(table_args['sort-col-count']):

                if 'iSortCol_' + str(i) in table_args:
                    sort_order = desc

                    if 'sSortDir_' + str(i) in table_args and table_args[
                        'sSortDir_' + str(i)
                    ] == 'asc':
                        sort_order = asc

                    i = table_args['iSortCol_' + str(i)]

                    if i >= 0 and i < len(display_columns):
                        q = q.order_by(sort_order(display_columns[i]))
                    else:
                        raise ValueError(
                            'Invalid column(%s) in other jobs listing ' % i
                        )

        else:
            # Default sorting order
            q = q.order_by(desc(Job.exec_job_id))

        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit(table_args['limit'])
            q = q.offset(table_args['offset'])

        return count, filtered, q.all()

    def get_failing_jobs(self, **table_args):
        """
        SELECT job.job_id                   AS job_job_id,
               job_instance.job_instance_id AS job_instance_job_instance_id,
               job.exec_job_id              AS job_exec_job_id,
               job_instance.exitcode        AS job_instance_exitcode
        FROM   job,
               job_instance,
               (SELECT job.job_id                       AS job_id,
                       Max(job_instance.job_submit_seq) AS max_jss
                FROM   job,
                       job_instance
                WHERE  job.wf_id = 1
                       AND job.job_id = job_instance.job_id
                       AND job_instance.exitcode IS NOT NULL
                       AND job_instance.exitcode != 0
                GROUP  BY job.job_id) AS allmaxjss
        WHERE  job.wf_id = 1
               AND job.job_id = job_instance.job_id
               AND job_instance.exitcode != 0
               AND job_instance.exitcode IS NOT NULL
               AND job.job_id = allmaxjss.job_id
               AND job_instance.job_submit_seq = allmaxjss.max_jss
               AND job.job_id IN (SELECT DISTINCT j1.job_id AS anon_1
                                  FROM   job AS j1,
                                         job_instance AS ji1
                                  WHERE  j1.wf_id = 1
                                         AND j1.job_id = ji1.job_id
                                         AND ji1.exitcode IS NULL);
        """
        # Get a list of running jobs.
        j1 = orm.aliased(Job, name='j1')
        ji1 = orm.aliased(JobInstance, name='ji1')

        q_sub = self.session.query(distinct(j1.job_id))

        q_sub = q_sub.filter(j1.wf_id == self._wf_id)

        q_sub = q_sub.filter(j1.job_id == ji1.job_id)

        q_sub = q_sub.filter(ji1.exitcode == None)

        q_sub = q_sub.subquery()

        #
        # Get max(job_submit_seq) of all the failed job instances, for each job.
        #
        qmax = self.__get_jobs_maxjss_q()
        qmax = qmax.filter(JobInstance.exitcode != None
                           ).filter(JobInstance.exitcode != 0)
        qmax = qmax.subquery('allmaxjss')

        #
        # Get the latest failed job instances
        # whose job_id matches the job_ids of the currently running jobs.
        #

        q = self.session.query(
            Job.job_id, JobInstance.job_instance_id, Job.exec_job_id,
            JobInstance.exitcode
        )

        q = q.filter(Job.wf_id == self._wf_id)

        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.filter(JobInstance.exitcode != 0
                     ).filter(JobInstance.exitcode != None)

        q = q.filter(Job.job_id == qmax.c.job_id)
        q = q.filter(JobInstance.job_submit_seq == qmax.c.max_jss)

        q = q.filter(Job.job_id.in_(q_sub))

        #
        # Get Total Count. Need this to pass to jQuery Datatable.
        #
        count = q.count()
        if count == 0:
            return 0, 0, []

        filtered = count
        if 'filter' in table_args:
            filter_text = '%' + table_args['filter'] + '%'
            q = q.filter(
                or_(
                    Job.exec_job_id.like(filter_text),
                    JobInstance.exitcode.like(filter_text)
                )
            )

            # Get Total Count. Need this to pass to jQuery Datatable.
            filtered = q.count()

            if filtered == 0:
                return count, 0, []

        display_columns = [Job.exec_job_id, JobInstance.exitcode]

        if 'sort-col-count' in table_args:
            for i in range(table_args['sort-col-count']):

                if 'iSortCol_' + str(i) in table_args:
                    sort_order = desc

                    if 'sSortDir_' + str(i) in table_args and table_args[
                        'sSortDir_' + str(i)
                    ] == 'asc':
                        sort_order = asc

                    i = table_args['iSortCol_' + str(i)]

                    if i >= 0 and i < len(display_columns):
                        q = q.order_by(sort_order(display_columns[i]))
                    elif i >= len(display_columns) and i < 4:
                        pass
                    else:
                        raise ValueError(
                            'Invalid column(%s) in failed jobs listing ' % i
                        )

        else:
            # Default sorting order
            q = q.order_by(desc(Job.exec_job_id))

        if 'limit' in table_args and 'offset' in table_args:
            q = q.limit(table_args['limit'])
            q = q.offset(table_args['offset'])

        return count, filtered, q.all()

    def __get_jobs_maxjss_q(self):
        qmax = self.session.query(
            Job.job_id,
            func.max(JobInstance.job_submit_seq).label('max_jss')
        )
        qmax = qmax.filter(Job.wf_id == self._wf_id)
        qmax = qmax.filter(Job.job_id == JobInstance.job_id)
        qmax = qmax.group_by(Job.job_id)

        return qmax

    def __get_jobs_maxjss_sq(self):
        qmax = self.__get_jobs_maxjss_q()
        qmax = qmax.subquery('allmaxjss')
        return qmax

    def get_sub_workflows(self):
        qmax = self.session.query(
            Workflowstate.wf_id,
            func.max(Workflowstate.timestamp).label('max_time')
        )
        qmax = qmax.group_by(Workflowstate.wf_id)

        qmax = qmax.subquery('max_timestamp')

        ws = orm.aliased(Workflowstate, name='ws')
        w = orm.aliased(Workflow, name='w')

        q = self.session.query(
            w.wf_id, w.wf_uuid, w.dax_label,
            case(
                [
                    (ws.status == None, 'Running'),
                    (ws.status == 0, 'Successful'), (ws.status != 0, 'Failed')
                ],
                else_='Undefined'
            ).label('state')
        )

        q = q.filter(w.parent_wf_id == self._wf_id)
        q = q.filter(w.wf_id == ws.wf_id)
        q = q.filter(ws.wf_id == qmax.c.wf_id)
        q = q.filter(ws.timestamp == qmax.c.max_time)

        return q.all()

    def get_stdout(self, job_id, job_instance_id):
        q = self.session.query(
            JobInstance.stdout_file, JobInstance.stdout_text
        )
        q = q.filter(JobInstance.job_instance_id == job_instance_id)

        return q.one()

    def get_stderr(self, job_id, job_instance_id):
        q = self.session.query(
            JobInstance.stderr_file, JobInstance.stderr_text
        )
        q = q.filter(JobInstance.job_instance_id == job_instance_id)

        return q.one()

    def get_successful_job_invocations(self, job_id, job_instance_id):

        q = self.session.query(
            Job.exec_job_id, Invocation.invocation_id, Invocation.abs_task_id,
            Invocation.exitcode, Invocation.remote_duration
        )
        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter(Invocation.exitcode == 0)

        q = q.filter(
            or_(
                Invocation.abs_task_id != None, Invocation.task_submit_seq == 1
            )
        )

        return q.all()

    def get_failed_job_invocations(self, job_id, job_instance_id):

        q = self.session.query(
            Job.exec_job_id, Invocation.invocation_id, Invocation.abs_task_id,
            Invocation.exitcode, Invocation.remote_duration
        )
        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)
        q = q.filter(Job.job_id == JobInstance.job_id)
        q = q.filter(JobInstance.job_instance_id == Invocation.job_instance_id)
        q = q.filter(Invocation.exitcode != 0)

        q = q.filter(
            or_(
                Invocation.abs_task_id != None, Invocation.task_submit_seq == 1
            )
        )

        return q.all()

    def __get_maxjss_subquery(self, job_id=None):

        jii = orm.aliased(JobInstance, name='jii')

        if job_id:

            qmax = self.session.query(
                JobInstance.job_instance_id,
                func.max(JobInstance.job_submit_seq)
            )
            qmax = qmax.filter(Job.wf_id == self._wf_id)
            qmax = qmax.filter(Job.job_id == job_id)
            qmax = qmax.filter(Job.type_desc != 'dax', Job.type_desc != 'dag')
            qmax = qmax.filter(Job.job_id == JobInstance.job_id).correlate(jii)

            qmax = qmax.subquery('maxjss')

        else:

            qmax = self.session.query(
                Job.job_id,
                func.max(JobInstance.job_submit_seq).label('max_jss')
            )
            qmax = qmax.filter(Job.wf_id == self._wf_id)
            qmax = qmax.filter(Job.job_id == JobInstance.job_id).correlate(jii)

            qmax = qmax.group_by(Job.job_id)

            qmax = qmax.subquery('maxjss')

        return qmax

    def get_invocation_information(
        self, job_id, job_instance_id, invocation_id
    ):

        q = self.session.query(JobInstance.work_dir)

        q = q.join(Job, Job.job_id == JobInstance.job_id)
        q = q.outerjoin(Task, Job.job_id == Task.job_id)
        q = q.join(
            Invocation,
            and_(
                JobInstance.job_instance_id == Invocation.job_instance_id,
                and_(
                    or_(
                        Task.abs_task_id == None,
                        and_(
                            Task.abs_task_id != None,
                            Task.abs_task_id == Invocation.abs_task_id
                        )
                    )
                )
            )
        )

        q = q.filter(Job.wf_id == self._wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)

        q = q.add_columns(
            Task.task_id, Invocation.invocation_id, Invocation.abs_task_id,
            Invocation.start_time, Invocation.remote_duration,
            Invocation.remote_cpu_time, Invocation.exitcode,
            Invocation.transformation, Invocation.executable, Invocation.argv
        )

        if invocation_id is None:
            q = q.filter(Invocation.task_submit_seq == 1)
        else:
            q = q.filter(Invocation.invocation_id == invocation_id)

        return q.one()

    def close(self):
        log.debug('close')
        self.session.close()
