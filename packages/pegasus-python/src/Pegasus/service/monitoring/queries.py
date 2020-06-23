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

__author__ = "Rajiv Mayani"

import hashlib
import logging

from sqlalchemy.orm import aliased, defer, joinedload
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import and_, desc, distinct, func

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.db.schema import (
    RCLFN,
    RCPFN,
    Host,
    Invocation,
    Job,
    JobInstance,
    Jobstate,
    MasterWorkflow,
    MasterWorkflowstate,
    RCMeta,
    Task,
    TaskMeta,
    Workflow,
    WorkflowFiles,
    WorkflowMeta,
    Workflowstate,
)
from Pegasus.service import cache
from Pegasus.service._query import InvalidQueryError, query_parse
from Pegasus.service._sort import InvalidSortError, sort_parse
from Pegasus.service.base import PagedResponse

log = logging.getLogger(__name__)


class WorkflowQueries:
    def __init__(self, connection_string, use_cache=True):
        if connection_string is None:
            raise ValueError("Connection string is required")

        self._conn_string_csum = hashlib.md5(
            connection_string.encode("utf-8")
        ).hexdigest()

        try:
            self.session = connection.connect(connection_string)
        except (connection.ConnectionError, DBAdminError) as e:
            log.exception(e)
            raise StampedeDBNotFoundError

        self._use_cache = True
        self.use_cache = use_cache

    def close(self):
        self.session.close()

    @property
    def use_cache(self):
        return self._use_cache

    @use_cache.setter
    def use_cache(self, use_cache):
        if isinstance(use_cache, bool):
            self._use_cache = use_cache
        else:
            raise TypeError("Expecting boolean, found %s" % type(use_cache))

    def _cache_key_from_query(self, q):
        statement = q.with_labels().statement
        compiled = statement.compile()
        params = compiled.params

        cache_key = " ".join(
            [self._conn_string_csum, str(compiled)]
            + [str(params[k]) for k in sorted(params)]
        )
        return hashlib.md5(cache_key.encode("utf-8")).hexdigest()

    def _get_count(self, q, use_cache=True, timeout=60):
        cache_key = "%s.count" % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug("Cache Hit: %s" % cache_key)
            count = cache.get(cache_key)

        else:
            log.debug("Cache Miss: %s" % cache_key)
            count = q.count()
            t = timeout(count) if callable(timeout) else timeout
            cache.set(cache_key, count, t)

        return count

    def _get_all(self, q, use_cache=True, timeout=60):
        cache_key = "%s.all" % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug("Cache Hit: %s" % cache_key)
            record = cache.get(cache_key)

        else:
            log.debug("Cache Miss: %s" % cache_key)
            record = q.all()
            t = timeout(record) if callable(timeout) else timeout
            cache.set(cache_key, record, t)

        return record

    def _get_one(self, q, use_cache=True, timeout=60):
        cache_key = "%s.one" % self._cache_key_from_query(q)
        if use_cache and cache.get(cache_key):
            log.debug("Cache Hit: %s" % cache_key)
            record = cache.get(cache_key)

        else:
            log.debug("Cache Miss: %s" % cache_key)
            record = q.one()
            t = timeout(record) if callable(timeout) else timeout
            cache.set(cache_key, record, t)

        return record

    @staticmethod
    def _evaluate_query(q, query, **resource):
        if not query:
            return q

        try:
            x = query_parse(query, **resource)
            q = q.filter(x[0])
        except (KeyError, AttributeError):
            log.exception("Invalid query %s" % query)
            raise InvalidQueryError("Invalid query %s" % query)
        except NameError as e:
            log.exception("Invalid field %s" % e)
            raise InvalidQueryError("Invalid field %s" % e)
        except Exception:
            log.exception("Invalid query %s" % query)
            raise InvalidQueryError("Invalid query %s" % query)

        return q

    @staticmethod
    def _add_ordering(q, order, **resource):
        if not q or not order or not resource:
            return q

        sort_order = sort_parse(order)

        for prefix, identifier, sort_dir in sort_order:
            try:
                field = getattr(resource[prefix], identifier)
            except (KeyError, AttributeError):
                log.exception("Invalid field {}.{}".format(prefix, identifier))
                raise InvalidSortError("Invalid field %r" % identifier)

            if sort_dir == "ASC":
                q = q.order_by(field)
            else:
                q = q.order_by(desc(field))

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
    def get_root_workflows(
        self,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Root Workflow objects.

        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of tuples (MasterWorkflow, MasterWorkflowstate)
        """

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(MasterWorkflow)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            log.debug("total_records 0")
            return PagedResponse([], 0, 0)

        #
        # Finish Construction of Base SQLAlchemy Query `q`
        #
        qws = self._get_max_master_workflow_state()
        qws = qws.subquery("master_workflowstate")

        alias = aliased(MasterWorkflowstate, qws)

        q = q.outerjoin(qws, MasterWorkflow.wf_id == qws.c.wf_id)
        q = q.add_entity(alias)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, r=MasterWorkflow, ws=alias)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, r=MasterWorkflow)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        for i in range(len(records)):
            new_record = records[i][0]
            new_record.workflow_state = records[i][1]
            new_record.__includes__ = ("workflow_state",)
            records[i] = new_record

        return PagedResponse(records, total_records, total_filtered)

    def get_root_workflow(self, m_wf_id, use_cache=True):
        """
        Returns a Root Workflow object identified by m_wf_id.

        :param m_wf_id: m_wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param use_cache: If available, use cached results

        :return: Root Workflow object
        """
        q = self.session.query(MasterWorkflow)

        if m_wf_id is None:
            raise ValueError("m_wf_id cannot be None")

        m_wf_id = str(m_wf_id)
        if m_wf_id.isdigit():
            q = q.filter(MasterWorkflow.wf_id == m_wf_id)
        else:
            q = q.filter(MasterWorkflow.wf_uuid == m_wf_id)

        #
        # Finish Construction of Base SQLAlchemy Query `q`
        #
        qws = self._get_max_master_workflow_state(m_wf_id=m_wf_id)
        qws = qws.subquery("master_workflowstate")

        q = q.outerjoin(qws, MasterWorkflow.wf_id == qws.c.wf_id)
        q = q.add_entity(aliased(MasterWorkflowstate, qws))

        try:
            record_tuple = self._get_one(q, use_cache)
            record = record_tuple[0]
            record.workflow_state = record_tuple[1]
            return record

        except NoResultFound as e:
            log.exception("Not Found: Root Workflow for given m_wf_id (%s)" % m_wf_id)
            raise e

    def _get_max_master_workflow_state(self, m_wf_id=None, mws=MasterWorkflowstate):
        qmax = self._get_recent_master_workflow_state(m_wf_id, mws)
        qmax = qmax.subquery("max_timestamp")

        q = self.session.query(mws)
        q = q.join(
            qmax, and_(mws.wf_id == qmax.c.wf_id, mws.timestamp == qmax.c.max_time)
        )

        return q

    def _get_recent_master_workflow_state(self, m_wf_id=None, mws=MasterWorkflowstate):
        q = self.session.query(mws.wf_id)
        q = q.add_columns(func.max(mws.timestamp).label("max_time"))

        if m_wf_id:
            log.debug("filter on m_wf_id")
            q = q.filter(mws.wf_id == m_wf_id)

        q = q.group_by(mws.wf_id)

        return q


class StampedeWorkflowQueries(WorkflowQueries):
    def wf_uuid_to_wf_id(self, wf_id):
        if wf_id is None:
            raise ValueError("wf_id cannot be None")

        wf_id = str(wf_id)
        if not wf_id.isdigit():
            q = self.session.query(Workflow.wf_id)
            q = q.filter(Workflow.wf_uuid == wf_id)

            try:
                q = self._get_one(q, True, timeout=600)
                wf_id = q.wf_id

            except NoResultFound as e:
                raise e

        return wf_id

    # Workflow

    def get_workflows(
        self,
        m_wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=False,
        **kwargs
    ):
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
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Workflow)
        q = q.filter(Workflow.root_wf_id == m_wf_id)
        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, w=Workflow)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, w=Workflow)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

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
        except NoResultFound as e:
            raise e

    # Workflow Meta

    def get_workflow_meta(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=False,
        **kwargs
    ):
        """
        Returns a collection of the Workflowstate objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Workflow Meta collection, total records count, total filtered records count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(WorkflowMeta)
        q = q.filter(WorkflowMeta.wf_id == wf_id)
        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, wm=WorkflowMeta)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, wm=WorkflowMeta)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    # Workflow Files

    def get_workflow_files(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=False,
        **kwargs
    ):
        """
        Returns a collection of all files associated with the Workflow.

        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Workflow Files
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(WorkflowFiles)
        q = q.filter(WorkflowFiles.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        q = (
            self.session.query(WorkflowFiles)
            .options(
                joinedload(WorkflowFiles.lfn).joinedload(RCLFN.pfns),
                joinedload(WorkflowFiles.lfn).joinedload(RCLFN.meta),
            )
            .filter(WorkflowFiles.wf_id == wf_id)
        )

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, l=RCLFN, p=RCPFN, rm=RCMeta)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, l=RCLFN, p=RCPFN, rm=RCMeta)

        records = self._get_all(q, use_cache)

        _records = []
        for r in records:
            o = {k: getattr(r, k) for k in r.__table__.columns.keys()}
            o["lfn"] = r.lfn.lfn
            o["pfns"] = r.lfn.pfns
            o["meta"] = r.lfn.meta
            _records.append(o)

        return PagedResponse(_records, total_records, total_filtered)

    # Workflow State

    def get_workflow_state(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Workflowstate objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Workflow States collection, total records count, total filtered records count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        # Use shorter caching timeout
        timeout = 5

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Workflowstate)
        q = q.filter(Workflowstate.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache, timeout=timeout)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, ws=Workflowstate)
            total_filtered = self._get_count(q, use_cache, timeout=timeout)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, ws=Workflowstate)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache, timeout=timeout)

        return PagedResponse(records, total_records, total_filtered)

    def _get_max_workflow_state(self, wf_id=None, ws=Workflowstate):
        qmax = self._get_recent_workflow_state(wf_id, ws)
        qmax = qmax.subquery("max_timestamp")

        q = self.session.query(ws)
        q = q.join(
            qmax, and_(ws.wf_id == qmax.c.wf_id, ws.timestamp == qmax.c.max_time)
        )

        return q

    def _get_recent_workflow_state(self, wf_id=None, ws=Workflowstate):
        q = self.session.query(ws.wf_id)
        q = q.add_columns(func.max(ws.timestamp).label("max_time"))

        if wf_id:
            log.debug("filter on wf_id")
            q = q.filter(ws.wf_id == wf_id)

        q = q.group_by(ws.wf_id)

        return q

    # Job

    def get_workflow_jobs(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
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

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Job)
        q = q.filter(Job.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, j=Job)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, j=Job)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_job(self, job_id, use_cache=True):
        """
        Returns a Job object identified by job_id.

        :param job_id: ID of the job
        :param use_cache: If available, use cached results

        :return: job record
        """
        if job_id is None:
            raise ValueError("job_id cannot be None")

        q = self.session.query(Job)
        q = q.filter(Job.job_id == job_id)

        try:
            return self._get_one(q, use_cache, timeout=600)
        except NoResultFound as e:
            raise e

    # Host

    def get_workflow_hosts(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
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

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Host)
        q = q.filter(Host.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, h=Host)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, h=Host)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_host(self, host_id, use_cache=True):
        """
        Returns a Host object identified by host_id.

        :param host_id: Id of the host
        :param use_cache: If available, use cached results

        :return: host record
        """
        if host_id is None or not str(host_id).isdigit():
            raise ValueError("host_id cannot be None")

        q = self.session.query(Host)
        q = q.filter(Host.host_id == host_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound as e:
            raise e

    # Job State

    def get_job_instance_states(
        self,
        wf_id,
        job_id,
        job_instance_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the JobInstanceState objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param job_id: job_id associated with the job instance states
        :param job_instance_id: job_instance_id associated with the job instance states
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: state record
        """
        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Jobstate)
        q = q.join(JobInstance, JobInstance.job_instance_id == Jobstate.job_instance_id)
        q = q.join(Job, Job.job_id == JobInstance.job_id)

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(Job.job_id == job_id)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)
        q = q.filter(Jobstate.job_instance_id == job_instance_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, js=Jobstate)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, js=Jobstate)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def _get_recent_job_state(self, job_instance_id=None, js=Jobstate):
        q = self.session.query(js.job_instance_id)
        q = q.add_columns(func.max(js.jobstate_submit_seq).label("max_jsss"))

        if job_instance_id:
            log.debug("filter on job_instance_id")
            q = q.filter(js.job_instance_id == job_instance_id)

        q = q.group_by(js.job_instance_id)

        return q

    # Task

    def get_workflow_tasks(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Task objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Task objects
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Task)
        q = q.filter(Task.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, t=Task)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, t=Task)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_job_tasks(
        self,
        wf_id,
        job_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Task objects.

        :param job_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Collection of Task objects
        """
        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Task)
        q = q.filter(Task.wf_id == wf_id)
        q = q.filter(Task.job_id == job_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, t=Task)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, t=Task)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_task(self, task_id, use_cache=True):
        """
        Returns a Task object identified by task_id.

        :param task_id: Id of the task
        :param use_cache: If available, use cached results

        :return: task record
        """
        q = self.session.query(Task)

        if task_id is None:
            raise ValueError("task_id cannot be None")

        q = q.filter(Task.task_id == task_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound as e:
            raise e

    # Task Meta

    def get_task_meta(
        self,
        task_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=False,
        **kwargs
    ):
        """
        Returns a collection of the TaskMeta objects.

        :param task_id: Id of the task
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Workflow Meta collection, total records count, total filtered records count
        """
        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(TaskMeta)
        q = q.filter(TaskMeta.task_id == task_id)
        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, tm=TaskMeta)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, tm=TaskMeta)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    # Job Instance

    def get_job_instances(
        self,
        wf_id,
        job_id,
        recent=False,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the JobInstance objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param job_id: job_id associated with the job instances
        :param recent: Get the most recent results
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: job-instance collection, total jobs count, filtered jobs count
        """
        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(JobInstance)
        q = q.join(Job, Job.job_id == JobInstance.job_id)

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(Job.job_id == job_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        if recent:
            qjss = self._get_recent_job_instance(job_id)
            qjss = qjss.subquery("max_jss")
            q = q.join(
                qjss,
                and_(
                    JobInstance.job_id == qjss.c.job_id,
                    JobInstance.job_submit_seq == qjss.c.max_jss,
                ),
            )

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query or recent:
            q = self._evaluate_query(q, query, ji=JobInstance)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, ji=JobInstance)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_job_instance(self, job_instance_id, use_cache=True, timeout=5):
        """
        Returns a JobInstance object identified by job_instance_id.

        :param job_instance_id: Id of the job instance
        :param use_cache: If available, use cached results
        :param timeout: Duration for which the job-instance should be cached, if exitcode is None i.e. Job is running

        :return: job-instance record
        """

        def timeout_duration(ji):
            return 300 if ji and ji.exitcode is not None else timeout

        if job_instance_id is None:
            raise ValueError("job_instance_id cannot be None")

        q = self.session.query(JobInstance)
        q = q.filter(JobInstance.job_instance_id == job_instance_id)

        try:
            return self._get_one(q, use_cache, timeout=timeout_duration)
        except NoResultFound as e:
            raise e

    def _get_recent_job_instance(self, job_id=None, ji=JobInstance):
        q = self.session.query(ji.job_id)
        q = q.add_columns(func.max(ji.job_submit_seq).label("max_jss"))

        if job_id:
            log.debug("filter on job_id")
            q = q.filter(ji.job_id == job_id)

        q = q.group_by(ji.job_id)

        return q

    # Invocation

    def get_workflow_invocations(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Invocation objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: invocations record
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Invocation)
        q = q.filter(Invocation.wf_id == wf_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, i=Invocation)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, i=Invocation)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_job_instance_invocations(
        self,
        wf_id,
        job_id,
        job_instance_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the Invocation objects.

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

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Invocation)
        q = q.filter(Invocation.wf_id == wf_id)
        q = q.filter(Invocation.job_instance_id == job_instance_id)

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, i=Invocation)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, i=Invocation)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)

        return PagedResponse(records, total_records, total_filtered)

    def get_invocation(self, invocation_id, use_cache=True):
        """
        Returns a Invocation object identified by invocation_id.

        :param invocation_id: Id of the invocation
        :param use_cache: If available, use cached results

        :return: invocation record
        """
        if invocation_id is None or not str(invocation_id).isdigit():
            raise ValueError("invocation_id cannot be None")

        q = self.session.query(Invocation)
        q = q.filter(Invocation.invocation_id == invocation_id)

        try:
            return self._get_one(q, use_cache)
        except NoResultFound as e:
            raise e

    # Views

    def get_running_jobs(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the running Job objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Jobs collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Job, JobInstance).options(
            defer(JobInstance.stdout_text), defer(JobInstance.stderr_text)
        )
        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(JobInstance.exitcode == None)  # noqa: E711

        # Recent
        qjss = self._get_recent_job_instance()
        qjss = qjss.filter(JobInstance.exitcode != None)  # noqa: E711
        qjss = qjss.subquery("max_jss")

        q = q.join(
            qjss,
            and_(
                JobInstance.job_id == qjss.c.job_id,
                JobInstance.job_submit_seq == qjss.c.max_jss,
            ),
        )

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, j=Job, ji=JobInstance)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, j=Job, ji=JobInstance)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)
        records = self._merge_job_instance(records)

        return PagedResponse(records, total_records, total_filtered)

    def get_successful_jobs(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the successful Job objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Jobs collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Job, JobInstance).options(
            defer(JobInstance.stdout_text), defer(JobInstance.stderr_text)
        )
        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode == 0
        )  # noqa: E711

        # Recent
        qjss = self._get_recent_job_instance()
        qjss = qjss.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode == 0
        )  # noqa: E711
        qjss = qjss.subquery("max_jss")

        q = q.join(
            qjss,
            and_(
                JobInstance.job_id == qjss.c.job_id,
                JobInstance.job_submit_seq == qjss.c.max_jss,
            ),
        )

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, j=Job, ji=JobInstance)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, j=Job, ji=JobInstance)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)
        records = self._merge_job_instance(records)

        return PagedResponse(records, total_records, total_filtered)

    def get_failed_jobs(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the failed Job objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Jobs collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Job, JobInstance).options(
            defer(JobInstance.stdout_text), defer(JobInstance.stderr_text)
        )
        q = q.filter(Job.job_id == JobInstance.job_id)

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode != 0
        )  # noqa: E711

        # Recent
        qjss = self._get_recent_job_instance()
        qjss = qjss.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode != 0
        )  # noqa: E711
        qjss = qjss.subquery("max_jss")

        q = q.join(
            qjss,
            and_(
                JobInstance.job_id == qjss.c.job_id,
                JobInstance.job_submit_seq == qjss.c.max_jss,
            ),
        )

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, j=Job, ji=JobInstance)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, j=Job, ji=JobInstance)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)
        records = self._merge_job_instance(records)

        return PagedResponse(records, total_records, total_filtered)

    def get_failing_jobs(
        self,
        wf_id,
        start_index=None,
        max_results=None,
        query=None,
        order=None,
        use_cache=True,
        **kwargs
    ):
        """
        Returns a collection of the failing Job objects.

        :param wf_id: wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid
        :param start_index: Return results starting from record `start_index`
        :param max_results: Return a maximum of `max_results` records
        :param query: Filtering criteria
        :param order: Sorting criteria
        :param use_cache: If available, use cached results

        :return: Jobs collection, total jobs count, filtered jobs count
        """
        wf_id = self.wf_uuid_to_wf_id(wf_id)

        #
        # Construct SQLAlchemy Query `q` to count.
        #
        q = self.session.query(Job, JobInstance).options(
            defer(JobInstance.stdout_text), defer(JobInstance.stderr_text)
        )

        q = q.filter(Job.wf_id == wf_id)
        q = q.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode != 0
        )  # noqa: E711

        q = q.filter(Job.job_id == JobInstance.job_id)

        # Running
        j = aliased(Job, name="j")
        ji = aliased(JobInstance, name="ji")

        qr = self.session.query(distinct(j.job_id))

        qr = qr.filter(j.wf_id == wf_id)
        qr = qr.filter(ji.exitcode == None)  # noqa: E711

        qr = qr.filter(j.job_id == ji.job_id)
        qr = qr.subquery()

        q = q.filter(Job.job_id.in_(qr))

        # Recent
        qjss = self._get_recent_job_instance()
        qjss = qjss.filter(Job.wf_id == wf_id)
        qjss = qjss.filter(JobInstance.exitcode != None).filter(
            JobInstance.exitcode != 0
        )  # noqa: E711

        qjss = qjss.filter(Job.job_id == JobInstance.job_id)
        qjss = qjss.subquery("allmaxjss")

        q = q.filter(
            and_(
                JobInstance.job_id == qjss.c.job_id,
                JobInstance.job_submit_seq == qjss.c.max_jss,
            )
        )

        total_records = total_filtered = self._get_count(q, use_cache)

        if total_records == 0:
            return PagedResponse([], 0, 0)

        #
        # Construct SQLAlchemy Query `q` to filter.
        #
        if query:
            q = self._evaluate_query(q, query, j=Job, ji=JobInstance)
            total_filtered = self._get_count(q, use_cache)

            if total_filtered == 0 or (start_index and start_index >= total_filtered):
                log.debug("total_filtered is 0 or start_index >= total_filtered")
                return PagedResponse([], total_records, total_filtered)

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        if order:
            q = self._add_ordering(q, order, j=Job, ji=JobInstance)

        #
        # Construct SQLAlchemy Query `q` to paginate.
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)

        records = self._get_all(q, use_cache)
        records = self._merge_job_instance(records)

        return PagedResponse(records, total_records, total_filtered)

    @staticmethod
    def _merge_job_instance(records):
        if records:
            for i in range(len(records)):
                new_record = records[i][0]
                new_record.job_instance = records[i][1]
                new_record.__includes__ = ("job_instance",)
                records[i] = new_record

        return records
