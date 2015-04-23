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

import json

from collections import OrderedDict

from flask import url_for

from Pegasus.service.base import BaseSerializer


class RootWorkflowSerializer(BaseSerializer):
    """
    RootWorkflowSerializer is used to serialize root workflow resource instances into their JSON representation.
    """
    FIELDS = [
        'wf_id',
        'wf_uuid',
        'submit_hostname',
        'submit_dir',
        'planner_arguments',
        'planner_version',
        'user',
        'grid_dn',
        'dax_label',
        'dax_version',
        'dax_file',
        'dag_file_name',
        'timestamp'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(RootWorkflowSerializer, self).__init__(fields=RootWorkflowSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, root_workflows, records_total=None, records_filtered=None):
        """
        Encodes a collection of root-workflows into it's JSON representation.

        :param root_workflows: Collection of root workflow records to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of root workflow resource
        """
        if root_workflows is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(root_workflow) for root_workflow in root_workflows]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, root_workflow):
        """
        Encodes a single root-workflow into it's JSON representation.

        :param root_workflow: Single instance of root workflow resource

        :return: JSON representation of root workflow resource
        """

        return json.dumps(self._encode_record(root_workflow), **self._pretty_print_opts)

    def _encode_record(self, root_workflow):
        """
        Encodes a single root-workflow into it's JSON representation.

        :param record: Single instance of root workflow resource

        :return: JSON representation of root workflow resource
        """

        if root_workflow is None:
            return None

        root_workflow, root_workflow_state = root_workflow

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(root_workflow, field)

        #
        # Serialize the Workflow State Object
        #
        # TODO: Call WorkflowStateSerializer to encode workflow-state object
        json_record['workflow_state'] = None

        json_record['_links'] = self._links(root_workflow)

        return json_record

    @staticmethod
    def _links(root_workflow):
        """
        Generates JSON representation of the HATEOAS links to be attached to the root workflow resource.

        :param root_workflow: Root workflow resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for root workflow resource
        """

        links = OrderedDict([
            ('workflow', url_for('.get_workflows', m_wf_id=root_workflow.wf_id))
        ])

        return links


class WorkflowSerializer(BaseSerializer):
    FIELDS = [
        'wf_id',
        'wf_uuid',
        'dag_file_name',
        'timestamp',
        'submit_hostname',
        'submit_dir',
        'planner_arguments',
        'user',
        'grid_dn',
        'planner_version',
        'dax_label',
        'dax_version',
        'dax_file',
        'parent_wf_id',
        'root_wf_id'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowSerializer, self).__init__(fields=WorkflowSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, workflows, records_total=None, records_filtered=None):
        """
        Encodes a collection of workflows into it's JSON representation.

        :param workflows: Collection of root workflow records to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of workflow resource
        """
        if workflows is None:
            return None

        if not records_total or not records_filtered:
            pass

        print [(workflow) for workflow in workflows]
        records = [self._encode_record(workflow) for workflow in workflows]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, workflow):
        """
        Encodes a single workflow into it's JSON representation.

        :param workflow: Single instance of workflow resource

        :return: JSON representation of workflow resource
        """

        return json.dumps(self._encode_record(workflow), **self._pretty_print_opts)


    def _encode_record(self, workflow):
        """
        Encodes a single workflow into it's JSON representation.

        :param workflow: Single instance of workflow resource

        :return: JSON representation of workflow resource
        """

        if workflow is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(workflow, field)

        json_record['_links'] = self._links(workflow)

        return json_record


    @staticmethod
    def _links(workflow):
        """
        Generates JSON representation of the HATEOAS links to be attached to the workflow resource.

        :param workflow: Workflow resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for workflow resource
        """

        links = OrderedDict([
            #('workflow', url_for('.get_workflow',  wf_id=workflow.wf_id))
        ])

        return links
