#  Copyright 2007-2014 University Of Southern California
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

import hashlib

from sqlalchemy import desc, distinct, func, and_, or_
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from Pegasus.db import connection
from Pegasus.db.modules import SQLAlchemyInit
from Pegasus.db.schema import *
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.db.admin.admin_loader import DBAdminError

from Pegasus.service import cache
from Pegasus.service.base import BaseQueryParser, BaseOrderParser, InvalidQueryError, InvalidOrderError
from Pegasus.service.monitoring.resources import RootWorkflowResource, RootWorkflowstateResource, CombinationResource
from Pegasus.service.monitoring.resources import JobInstanceResource, JobstateResource, TaskResource, InvocationResource
from Pegasus.service.monitoring.resources import WorkflowResource, WorkflowstateResource, JobResource, HostResource

log = logging.getLogger(__name__)


class WorkflowQueries(SQLAlchemyInit):
    def __init__(self, connection_string, use_cache=True):
        if connection_string is None:
            raise ValueError('Connection string is required')

        self._conn_string_csum = hashlib.md5(connection_string).hexdigest()

        try:
            SQLAlchemyInit.__init__(self, connection_string)
        except (connection.ConnectionError, DBAdminError) as e:
            log.exception(e)
            raise StampedeDBNotFoundError

        self._use_cache = True
        self.use_cache = use_cache

    @property
    def use_cache(self):
        return self._use_cache

    @use_cache.setter
    def use_cache(self, use_cache):
        if isinstance(use_cache, bool):
            self._use_cache = use_cache
        else:
            raise TypeError('Expecting boolean, found %s' % type(use_cache))

    def _cache_key_from_query(self, q):
        statement = q.with_labels().statement
        compiled = statement.compile()
        params = compiled.params

        cache_key = ' '.join([self._conn_string_csum, str(compiled)] + [str(params[k]) for k in sorted(params)])
        return hashlib.md5(cache_key).hexdigest()

    def _get_count(self, q, use_cache=True, timeout=60):
        cache_key = '%s.count' % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug('Cache Hit: %s' % cache_key)
            count = cache.get(cache_key)

        else:
            log.debug('Cache Miss: %s' % cache_key)
            count = q.count()
            t = timeout(count) if hasattr(timeout, '__call__') else timeout
            cache.set(cache_key, count, t)

        return count

    def _get_all(self, q, use_cache=True, timeout=60):
        cache_key = '%s.all' % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug('Cache Hit: %s' % cache_key)
            record = cache.get(cache_key)

        else:
            log.debug('Cache Miss: %s' % cache_key)
            record = q.all()
            t = timeout(record) if hasattr(timeout, '__call__') else timeout
            cache.set(cache_key, record, t)

        return record

    def _get_one(self, q, use_cache=True, timeout=60):
        cache_key = '%s.one' % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug('Cache Hit: %s' % cache_key)
            record = cache.get(cache_key)

        else:
            log.debug('Cache Miss: %s' % cache_key)
            record = q.one()
            t = timeout(record) if hasattr(timeout, '__call__') else timeout
            cache.set(cache_key, record, t)

        return record

    @staticmethod
    def _evaluate_query(q, query, resource):
        if not query:
            return q

        comparator = {
            '=': '__eq__',
            '!=': '__ne__',
            '<': '__lt__',
            '<=': '__le__',
            '>': '__gt__',
            '>=': '__ge__',
            'LIKE': 'like',
            'IN': 'in_'
        }

        operators = {
            'AND': and_,
            'OR': or_
        }

        operands = []

        def condition_expansion(expr, field):
            operands.append(getattr(field, comparator[expr[1]])(expr[2]))

        try:
            expression = BaseQueryParser(query).evaluate()

            for token in expression:
                if isinstance(token, tuple):
                    identifier = token[0]
                    condition_expansion(token, resource.get_mapped_field(identifier))

                elif isinstance(token, str) or isinstance(token, unicode):

                    operand_2 = operands.pop()
                    operand_1 = operands.pop()

                    if token in operators:
                        operands.append(operators[token](operand_1, operand_2))

            q = q.filter(operands.pop())

        except (KeyError, AttributeError):
            log.exception('Invalid field %s' % identifier)
            raise InvalidQueryError('Invalid field %s' % identifier)

        except IndexError:
            log.exception('Invalid expression %s' % query)
            raise InvalidQueryError('Invalid expression %s' % query)

        return q

    @staticmethod
    def _add_ordering(q, order, resource):
        if not q or not order or not resource:
            return q

        order_parser = BaseOrderParser(order)
        sort_order = order_parser.get_sort_order()

        for identifier, sort_dir in sort_order:
            try:
                field = resource.get_mapped_field(identifier)

                if sort_dir == 'ASC':
                    q = q.order_by(field)
                else:
                    q = q.order_by(desc(field))

            except (KeyError, AttributeError):
                log.exception('Invalid field %r' % identifier)
                raise InvalidOrderError('Invalid field %r' % identifier)

        return q

    @staticmethod
    def _add_pagination(q, start_index=None, max_results=None, total_records=None):
        """
        LIMIT <skip>, <count>       - Valid
        LIMIT <count> OFFSET <skip> - Valid
        OFFSET <skip>               - Invalid

        If only start_index is provided and total_records is known then we can compute both limit and offset to
        effectively support OFFSET <skip>
        """
        if start_index and max_results:
            q = q.offset(start_index)
            q = q.limit(max_results)

        elif not start_index and not max_results:
            return q

        else:
            if max_results:
                q = q.limit(max_results)

            elif total_records:
                q = q.offset(start_index)
                q = q.limit(total_records)

        return q


class MasterWorkflowQueries(WorkflowQueries):
    def get_root_workflows(self, start_index=None, max_results=None, query=None, order=None, use_cache=True, **kwargs):
        """
        Returns a collection of the Root Workflow objects.

        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of tuples (DashboardWorkflow, DashboardWorkflowstate)
        """

        #
        # Construct SQLAlchemy Query `q` to get total count.
        #
        q = self.session.query(DashboardWorkflow)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            log.debug('total_records 0')
            return [], 0, 0

        #
        # Finish Construction of Base SQLAlchemy Query `q`
        #
        qws = self._get_max_master_workflow_state()
        qws = qws.subquery('master_workflowstate')

        alias = aliased(DashboardWorkflowstate, qws)

        q = q.outerjoin(qws, DashboardWorkflow.wf_id == qws.c.wf_id)
        q = q.add_entity(alias)

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            resource = CombinationResource(RootWorkflowResource(), RootWorkflowstateResource(alias))

            q = self._evaluate_query(q, query, resource)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, RootWorkflowResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_root_workflow(self, m_wf_id, use_cache=True):
        """
        Returns a Root Workflow object identified by m_wf_id.

        :param m_wf_id: m_wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param use_cache: If available, use cached results

        :return: Root Workflow object
        """
        q = self.session.query(DashboardWorkflow)

        if m_wf_id is None:
            raise ValueError('m_wf_id cannot be None')

        m_wf_id = str(m_wf_id)
        if m_wf_id.isdigit():
            q = q.filter(DashboardWorkflow.wf_id == m_wf_id)
        else:
            q = q.filter(DashboardWorkflow.wf_uuid == m_wf_id)

        #
        # Finish Construction of Base SQLAlchemy Query `q`
        #
        qws = self._get_max_master_workflow_state()
        qws = qws.subquery('master_workflowstate')

        q = q.outerjoin(qws, DashboardWorkflow.wf_id == qws.c.wf_id)
        q = q.add_entity(aliased(DashboardWorkflowstate, qws))

        try:
            return self._get_one(q, use_cache)
        except NoResultFound as e:
            log.exception('Not Found: Root Workflow for given m_wf_id (%s)' % m_wf_id)
            raise e

    def _get_max_master_workflow_state(self, mws=DashboardWorkflowstate):
        qmax = self._get_recent_master_workflow_state()
        qmax = qmax.subquery('max_timestamp')

        q = self.session.query(mws)
        q = q.join(qmax, and_(mws.wf_id == qmax.c.wf_id,
                              mws.timestamp == qmax.c.max_time))

        return q

    def _get_recent_master_workflow_state(self, mws=DashboardWorkflowstate):
        q = self.session.query(mws.wf_id)
        q = q.add_column(func.max(mws.timestamp).label('max_time'))
        q = q.group_by(mws.wf_id)

        return q


class StampedeWorkflowQueries(WorkflowQueries):
    def wf_uuid_to_wf_id(self, wf_id):
        if wf_id is None:
            raise ValueError('wf_id cannot be None')

        wf_id = str(wf_id)
        if not wf_id.isdigit():
            q = self.session.query(Workflow.wf_id)
            q = q.filter(Workflow.wf_uuid == wf_id)

            try:
                q = self._get_one(q, True, timeout=600)
                wf_id = q.wf_id

            except NoResultFound, e:
                raise e

        return wf_id

    # Workflow

    def get_workflows(self, m_wf_id, start_index=None, max_results=None, query=None, order=None, use_cache=False,
                      **kwargs):
        """
        Returns a collection of the Workflow objects.

        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Workflow objects
        """
        m_wf_id = self.wf_uuid_to_wf_id(m_wf_id)

        #
        # Construct SQLAlchemy Query `q` to get total count.
        #
        q = self.session.query(Workflow)
        q = q.filter(Workflow.root_wf_id == m_wf_id)
        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, WorkflowResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, WorkflowResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_workflow(self, wf_id, use_cache=True):
        """
        Returns a Workflow object identified by wf_id.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid

        :return: Workflow object
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Workflow)
        q = q.filter(Workflow.wf_id == wf_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound, e:
            raise e

    # Workflow State

    def get_workflow_state(self, wf_id, start_index=None, max_results=None, query=None, order=None, recent=False,
                           use_cache=True, **kwargs):
        """
        Returns a collection of the Workflowstate objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results
        :param recent: Get the most recent results

        :return: Workflow States collection, total records count, total filtered records count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        # Use shorter caching timeout
        timeout = 5

        q = self.session.query(Workflowstate)
        q = q.filter(Workflowstate.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache, timeout=timeout)

        if total_records == 0:
            return [], 0, 0

        if recent:
            q = self._get_max_workflow_state()
            q = q.filter(Workflowstate.wf_id == wf_id)

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query or recent:
            q = self._evaluate_query(q, query, WorkflowstateResource())
            total_filtered = self._get_count(q, use_cache, timeout=timeout)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, WorkflowstateResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache, timeout=timeout)

        return records, total_records, total_filtered

    def _get_max_workflow_state(self, ws=Workflowstate):
        qmax = self._get_recent_workflow_state()
        qmax = qmax.subquery('max_timestamp')

        q = self.session.query(ws)
        q = q.join(qmax, and_(ws.wf_id == qmax.c.wf_id, ws.timestamp == qmax.c.max_time))

        return q

    def _get_recent_workflow_state(self, ws=Workflowstate):
        q = self.session.query(ws.wf_id)
        q = q.add_column(func.max(ws.timestamp).label('max_time'))
        q = q.group_by(ws.wf_id)

        return q

    # Job

    def get_workflow_jobs(self, wf_id, start_index=None, max_results=None, query=None, order=None, use_cache=True,
                          **kwargs):
        """
        Returns a collection of the Job objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Jobs collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Job)
        q = q.filter(Job.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, JobResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, JobResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_job(self, job_id, use_cache=True):
        """
        Returns a Job object identified by job_id.

        :param job_id: ID of the job
        :param use_cache: If available, use cached results

        :return: job record
        """
        if job_id is None:
            raise ValueError('job_id cannot be None')

        q = self.session.query(Job)
        q = q.filter(Job.job_id == job_id)

        try:
            return self._get_one(q, use_cache, timeout=600)
        except NoResultFound, e:
            raise e

    # Host

    def get_workflow_hosts(self, wf_id, start_index=None, max_results=None, query=None, order=None, use_cache=True,
                           **kwargs):
        """
        Returns a collection of the Host objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: hosts collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Host)
        q = q.filter(Host.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, HostResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, HostResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_host(self, host_id, use_cache=True):
            """

            :param host_id: Id of the host
            :param use_cache: If available, use cached results

            :return: host record
            """
            if host_id is None or not str(host_id).isdigit():
                raise ValueError('host_id cannot be None')

            q = self.session.query(Host)
            q = q.filter(Host.host_id == host_id)

            try:
                return self._get_one(q, use_cache)
            except NoResultFound, e:
                raise e

    # Job State

    def get_job_instance_states(self, wf_id, job_id, job_instance_id, start_index=None, max_results=None, query=None,
                                order=None, recent=False, use_cache=True, **kwargs):
        """

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param job_id: job_id associated with the job instance states
        :param job_instance_id: job_instance_id associated with the job instance states
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results
        :param recent: Get the most recent results

        :return: state record
        """
        q = self.session.query(Jobstate)

        if wf_id is None:
            raise ValueError('wf_id cannot be None')
        if job_id is None or not str(job_id).isdigit():
            raise ValueError('job_id must be an integer value')
        if job_instance_id is None or not str(job_instance_id).isdigit():
            raise ValueError('job_instance_id must be an integer value')

        q = q.filter(Jobstate.job_instance_id == job_instance_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, JobstateResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, JobstateResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    # Task

    def get_workflow_tasks(self, wf_id, start_index=None, max_results=None, query=None, order=None, use_cache=True,
                           **kwargs):
        """

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Task objects
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Task)
        q = q.filter(Task.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, TaskResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, TaskResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_job_tasks(self, job_id, start_index=None, max_results=None, query=None, order=None, use_cache=True,
                      **kwargs):
        """

        :param job_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Task objects
        """
        q = self.session.query(Task)
        q = q.filter(Task.job_id == job_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, TaskResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, TaskResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_task(self, task_id, use_cache=True):
        """

        :param task_id: Id of the task
        :param use_cache: If available, use cached results

        :return: task record
        """
        q = self.session.query(Task)

        if task_id is None:
            raise ValueError('task_id cannot be None')

        q = q.filter(Task.task_id == task_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound, e:
            raise e

    # Job Instance

    def get_job_instances(self, wf_id, job_id, start_index=None, max_results=None, query=None, order=None,
                          use_cache=True, **kwargs):
        """

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param job_id: job_id associated with the job instances
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: hosts collection, total jobs count, filtered jobs count
        """
        q = self.session.query(JobInstance)

        if job_id is None or not str(job_id).isdigit():
            raise ValueError('job__id must be an integer value')
        q = q.filter(JobInstance.job_id == job_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, JobInstanceResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, JobInstanceResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_job_instance(self, job_instance_id, use_cache=True):
        """

        :param job_instance_id: Id of the job instance
        :param use_cache: If available, use cached results

        :return: host record
        """
        if job_instance_id is None:
            raise ValueError('job_instance_id cannot be None')

        q = self.session.query(JobInstance)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound, e:
            raise e

    # Invocation

    def get_workflow_invocations(self, wf_id, start_index=None, max_results=None, query=None, order=None, recent=False,
                                 use_cache=True, **kwargs):
        """

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param recent: Get the most recent results
        :param use_cache: If available, use cached results

        :return: invocations record
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Invocation)
        q = q.filter(Invocation.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, InvocationResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, InvocationResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_job_instance_invocations(self, wf_id, job_id, job_instance_id, start_index=None, max_results=None,
                                     query=None, order=None, use_cache=True, **kwargs):
        """

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param job_id: Id of the job associated with the invocation
        :param job_instance_id: Id of the job instance associated with the invocation
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: invocations record
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        q = self.session.query(Invocation)

        if wf_id is None:
            raise ValueError('wf_id cannot be None')
        if job_id is None or not str(job_id).isdigit():
            raise ValueError('job_id must be an integer value')
        if job_instance_id is None or not str(job_instance_id).isdigit():
            raise ValueError('job_instance_id must be an integer value')

        q = q.filter(Invocation.wf_id == wf_id)
        q = q.filter(Invocation.job_instance_id == job_instance_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        if query:
            q = self._evaluate_query(q, query, InvocationResource())
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug('total_filtered is 0 or start_index >= total_filtered')
                return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, InvocationResource())

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return records, total_records, total_filtered

    def get_invocation(self, invocation_id, use_cache=True):
        """

        :param invocation_id: Id of the invocation
        :param use_cache: If available, use cached results

        :return: invocation record
        """
        if invocation_id is None or not str(invocation_id).isdigit():
            raise ValueError('invocation_id cannot be None')

        q = self.session.query(Invocation)
        q = q.filter(Invocation.invocation_id == invocation_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound, e:
            raise e
