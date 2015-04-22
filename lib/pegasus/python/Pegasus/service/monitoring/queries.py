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

from sqlalchemy import desc
from sqlalchemy.orm.exc import NoResultFound

from Pegasus.db import connection
from Pegasus.db.modules import SQLAlchemyInit
from Pegasus.db.schema import DashboardWorkflow
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.db.admin.admin_loader import DBAdminError

from Pegasus.service import cache
from Pegasus.service.base import BaseOrderParser

log = logging.getLogger(__name__)


class WorkflowQueries(SQLAlchemyInit):
    def __init__(self, connection_string):
        if connection_string is None:
            raise ValueError('Connection string is required')

        self._conn_string_csum = hashlib.md5(connection_string).hexdigest()

        try:
            SQLAlchemyInit.__init__(self, connection_string)
        except (connection.ConnectionError, DBAdminError) as e:
            log.exception(e)
            raise StampedeDBNotFoundError

    @staticmethod
    def _get_count(q, cache_key, use_cache=True, timeout=60):
        if use_cache and cache.get(cache_key):
            log.debug('Cache hit for %s' % cache_key)
            count = cache.get(cache_key)
        else:
            count = q.count()
            cache.set(cache_key, count, timeout)

        return count

    @staticmethod
    def _add_ordering(q, order, fields):
        if not q or not order or not fields:
            return q

        order_parser = BaseOrderParser(order)
        sort_order = order_parser.get_sort_order()

        for identifier, sort_dir in sort_order:
            if identifier not in fields:
                log.error('Invalid field %r' % identifier)
                raise InvalidOrderError('Invalid field %r' % identifier)

            if sort_dir == 'ASC':
                q = q.order_by(fields[identifier])
            else:
                q = q.order_by(desc(fields[identifier]))

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
        :param use_cache:

        :return: Collection of DashboardWorklfow objects
        """

        #
        # Construct SQLAlchemy Query `q` to get total count.
        #
        q = self.session.query(DashboardWorkflow)
        total_records = MasterWorkflowQueries.get_total_root_workflows(q, use_cache)

        if total_records == 0:
            log.debug('total_records 0')
            return [], 0, 0

        #
        # Construct SQLAlchemy Query `q` to get filtered count.
        #
        # TODO: Validate `query`
        # TODO: Construct JOINS as per `query`
        q = q
        total_filtered = MasterWorkflowQueries.get_filtered_root_workflows(q, use_cache)

        if total_filtered == 0 or (start_index and start_index >= total_filtered):
            log.debug('total_filtered is 0 or start_index >= total_filtered')
            return [], total_records, total_filtered

        #
        # Construct SQLAlchemy Query `q` to sort
        #
        # TODO: Add support for sorting

        #
        # Construct SQLAlchemy Query `q` to add pagination
        #
        q = WorkflowQueries._add_pagination(q, start_index, max_results, total_filtered)
        records = q.all()

        return records, total_records, total_filtered

    def get_root_workflow(self, m_wf_id):
        """
        Returns a Root Workflow object identified by m_wf_id.

        :param m_wf_id: m_wf_id is wf_id iff it consists only of digits, otherwise it is wf_uuid

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

        try:
            return q.one()
        except NoResultFound as e:
            log.exception('Not Found: Root Workflow for given m_wf_id (%s)' % m_wf_id)
            raise e

    def get_wf_id_for_wf_uuid(self, wf_uuid):
        """
        Returns wf_id for a given wf_uuid

        :param wf_uuid: Workflow UUID

        :return: wf_id for given wf_uuid
        """
        q = self.session.query(DashboardWorkflow)

        q = q.filter(DashboardWorkflow.wf_uuid == wf_uuid)

        try:
            return q.one()
        except NoResultFound as e:
            log.exception('Not Found: wf_id for given the wf_uuid (%s)' % wf_uuid)
            raise e

    @staticmethod
    def get_total_root_workflows(q, use_cache=True):
        cache_key = ''
        return WorkflowQueries._get_count(q, cache_key, use_cache)

    @staticmethod
    def get_filtered_root_workflows(q, use_cache=True):
        cache_key = ''
        return WorkflowQueries._get_count(q, cache_key, use_cache)


class StampedeWorkflowQueries(WorkflowQueries):
    """
    TODO: Mimic code above for each remaining resources except for method get_wf_id_for_wf_uuid
    """
    pass
