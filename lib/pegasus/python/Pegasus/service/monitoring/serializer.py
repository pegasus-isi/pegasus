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

        records = [self._encode_record(root_workflow) for root_workflow in root_workflows]

        json_records = OrderedDict([
            ('records', records)
        ])

        if not records_total or not records_filtered:
            pass

        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records['_meta'] = records_meta

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

        records = [self._encode_record(workflow) for workflow in workflows]

        json_records = OrderedDict([
            ('records', records)
        ])

        if not records_total or not records_filtered:
            pass

        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records['_meta'] = records_meta

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
            ('workflow_state', url_for('.get_workflow_state', wf_id=workflow.wf_id)),
            ('job', url_for('.get_workflow_jobs', wf_id=workflow.wf_id)),
            ('task', url_for('.get_workflow_tasks', wf_id=workflow.wf_id)),
            ('host', url_for('.get_workflow_hosts', wf_id=workflow.wf_id)),
            ('invocation', url_for('.get_workflow_invocations', wf_id=workflow.wf_id))
        ])

        return links

class WorkflowStateSerializer(BaseSerializer):
    FIELDS = [
        'state',
        'timestamp',
        'restart_count',
        'status'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowStateSerializer, self).__init__(fields=WorkflowStateSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, states, records_total=None, records_filtered=None):
        """
        Encodes a collection of workflow states into it's JSON representation.

        :param states: Collection of workflow state records to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of workflow state resource
        """
        if states is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(state) for state in states]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, state):
        """
        Encodes a single workflow into it's JSON representation.

        :param workflow: Single instance of workflow resource

        :return: JSON representation of workflow resource
        """

        return json.dumps(self._encode_record(state), **self._pretty_print_opts)

    def _encode_record(self, state):
        """
        Encodes a single workflow state into it's JSON representation.

        :param state: Single instance of state resource

        :return: JSON representation of state resource
        """

        if state is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(state, field)

        json_record['_links'] = self._links(state)

        return json_record

    @staticmethod
    def _links(state):
        """
        Generates JSON representation of the HATEOAS links to be attached to the workflow state resource.

        :param state: Workflow state resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for state resource
        """

        links = OrderedDict([
            ('state', url_for('.get_workflow', wf_id=state.wf_id))
        ])

        return links

class WorkflowJobSerializer(BaseSerializer):
    FIELDS = [
        'job_id',
        'exec_job_id',
        'submit_file',
        'type_desc',
        'clustered',
        'max_retries',
        'executable',
        'argv',
        'task_count'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowJobSerializer, self).__init__(fields=WorkflowJobSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, jobs, records_total=None, records_filtered=None):
        """
        Encodes a collection of jobs into it's JSON representation.

        :param jobs: Collection of workflow jobs to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of jobs collection
        """
        if jobs is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(job) for job in jobs]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, job):
        """
        Encodes a single job into it's JSON representation.

        :param job: Single instance of job resource

        :return: JSON representation of job resource
        """

        return json.dumps(self._encode_record(job), **self._pretty_print_opts)

    def _encode_record(self, job):
        """
        Encodes a single job into it's JSON representation.

        :param job: Single instance of job resource

        :return: JSON representation of job resource
        """

        if job is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(job, field)

        json_record['_links'] = self._links(job)

        return json_record

    @staticmethod
    def _links(job):
        """
        Generates JSON representation of the HATEOAS links to be attached to the job resource.

        :param job: job resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for job resource
        """

        links = OrderedDict([
            ('workflow', url_for('.get_workflow', wf_id=job.wf_id)),
            ('task', url_for('.get_workflow_tasks', wf_id=job.wf_id)),
            ('job_instance', url_for('.get_workflow_job_instances', wf_id=job.wf_id, job_id=job.job_id))
        ])

        return links


class WorkflowHostSerializer(BaseSerializer):
    FIELDS = [
        'host_id',
        'site',
        'hostname',
        'ip',
        'uname',
        'total_memory'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowHostSerializer, self).__init__(fields=WorkflowHostSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, hosts, records_total=None, records_filtered=None):
        """
        Encodes a collection of hosts into it's JSON representation.

        :param hosts: Collection of workflow hosts to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of hosts collection
        """
        if hosts is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(host) for host in hosts]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, host):
        """
        Encodes a single host into it's JSON representation.

        :param host: Single instance of host resource

        :return: JSON representation of host resource
        """

        return json.dumps(self._encode_record(host), **self._pretty_print_opts)

    def _encode_record(self, host):
        """
        Encodes a single host into it's JSON representation.

        :param host: Single instance of host resource

        :return: JSON representation of host resource
        """

        if host is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(host, field)

        json_record['_links'] = self._links(host)

        return json_record

    @staticmethod
    def _links(host):
        """
        Generates JSON representation of the HATEOAS links to be attached to the host resource.

        :param host: host resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for host resource
        """

        links = OrderedDict([
            ('workflow', url_for('.get_workflow', wf_id=host.wf_id))
        ])

        return links

class WorkflowHostSerializer(BaseSerializer):
    FIELDS = [
        'host_id',
        'site',
        'hostname',
        'ip',
        'uname',
        'total_memory'
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowHostSerializer, self).__init__(fields=WorkflowHostSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, hosts, records_total=None, records_filtered=None):
        """
        Encodes a collection of hosts into it's JSON representation.

        :param hosts: Collection of workflow hosts to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of hosts collection
        """
        if hosts is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(host) for host in hosts]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, host):
        """
        Encodes a single host into it's JSON representation.

        :param host: Single instance of host resource

        :return: JSON representation of host resource
        """

        return json.dumps(self._encode_record(host), **self._pretty_print_opts)

    def _encode_record(self, host):
        """
        Encodes a single host into it's JSON representation.

        :param host: Single instance of host resource

        :return: JSON representation of host resource
        """

        if host is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(host, field)

        json_record['_links'] = self._links(host)

        return json_record

    @staticmethod
    def _links(host):
        """
        Generates JSON representation of the HATEOAS links to be attached to the host resource.

        :param host: host resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for host resource
        """

        links = OrderedDict([
            ('job_instance', url_for('.get_workflow_job_instances', wf_id=host.wf_id, job_id=host.host_id))
        ])

        return links

class WorkflowJobInstanceSerializer(BaseSerializer):
    FIELDS = [
        "job_instance_id",
        "host_id",
        "job_submit_seq",
        "sched_id",
        "site",
        "user",
        "work_dir",
        "cluster_start",
        "cluster_duration",
        "local_duration",
        "subwf_id",
        "stdout_text",
        "stderr_text",
        "stdin_file",
        "stdout_file",
        "stderr_file",
        "multiplier_factor"
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowJobInstanceSerializer, self).__init__(fields=WorkflowJobInstanceSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, job_instances, records_total=None, records_filtered=None):
        """
        Encodes a collection of job instances into it's JSON representation.

        :param job_instances: Collection of workflow job_instances to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of jobs collection
        """
        if job_instances is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(job_instance) for job_instance in job_instances]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, job_instance):
        """
        Encodes a single job_instance into it's JSON representation.

        :param job_instance: Single instance of job_instance resource

        :return: JSON representation of job_instance resource
        """

        return json.dumps(self._encode_record(job_instance), **self._pretty_print_opts)

    def _encode_record(self, job_instance):
        """
        Encodes a single job_instance into it's JSON representation.

        :param job_instance: Single instance of job_instance resource

        :return: JSON representation of job_instance resource
        """

        if job_instance is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(job_instance, field)

        json_record['_links'] = self._links(job_instance)

        return json_record

    @staticmethod
    def _links(job_instance):
        """
        Generates JSON representation of the HATEOAS links to be attached to the job_instance resource.

        :param job_instance: job_instance resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for job_instance resource
        """

        links = OrderedDict([
            #('state', url_for('.get_job_instance_states', wf_id=job_instance.wf_id, job_id=job_instance.job_id, job_instance_id= job_instance.job_instance_id)),
            #('host', url_for('.get_job_instance_host', wf_id=job_instance.wf_id, job_id=job_instance.job_id, job_instance_id=job_instance.job_instance_id)),
            #('invocation', url_for('.get_job_instance_invocations', wf_id=job_instance.wf_id, job_id=job_instance.job_id, job_instance_id=job_instance.job_instance_id)),
            #('job', url_for('.get_workflow_job', wf_id=job_instance.wf_id, job_id=job_instance.job_id))
        ])

        return links

class JobInstanceStateSerializer(BaseSerializer):
    FIELDS = [
        "job_instance_id",
        "state",
        "timestamp",
        "jobstate_submit_seq"
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(JobInstanceStateSerializer, self).__init__(fields=JobInstanceStateSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, states, records_total=None, records_filtered=None):
        """
        Encodes a collection of job instance states into it's JSON representation.

        :param states: Collection of workflow job_instance_states to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of states collection
        """
        if states is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(state) for state in states]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, state):
        """
        Encodes a single job_instance_state into it's JSON representation.

        :param state: Single instance of job_instance_state resource

        :return: JSON representation of job_instance_state resource
        """

        return json.dumps(self._encode_record(state), **self._pretty_print_opts)

    def _encode_record(self, state):
        """
        Encodes a single job_instance_state into it's JSON representation.

        :param state: Single instance of job_instance_state resource

        :return: JSON representation of job_instance_state resource
        """

        if state is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(state, field)

        json_record['_links'] = self._links(state)

        return json_record

    @staticmethod
    def _links(state):
        """
        Generates JSON representation of the HATEOAS links to be attached to the job_instance_state resource.

        :param state: job_instance_state resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for job_instance_state resource
        """

        links = OrderedDict([
            #('job_instance', url_for('.get_job_instance', wf_id=job_instance.wf_id, job_id=job_instance.job_id, job_instance_id=job_instance.job_instance_id)),
        ])

        return links

class WorkflowTaskSerializer(BaseSerializer):
    FIELDS = [
        "task_id",
        "abs_task_id",
        "type_desc",
        "transformation",
        "argv"
    ]

    def __init__(self, selected_fields=None, pretty_print=False, **kwargs):
        super(WorkflowTaskSerializer, self).__init__(fields=WorkflowTaskSerializer.FIELDS, pretty_print=pretty_print)
        self._selected_fields = selected_fields if selected_fields else self._fields

    def encode_collection(self, tasks, records_total=None, records_filtered=None):
        """
        Encodes a collection of tasks into it's JSON representation.

        :param tasks: Collection of workflow workflow_tasks to be encoded as JSON
        :param records_total: Number of records before applying the search criteria
        :param records_filtered: Number of records after applying the search criteria

        :return: JSON representation of tasks collection
        """
        if tasks is None:
            return None

        if not records_total or not records_filtered:
            pass

        records = [self._encode_record(task) for task in tasks]
        records_meta = OrderedDict([
            ('records_total', records_total),
            ('records_filtered', records_filtered)
        ])

        json_records = OrderedDict([
            ('records', records),
            ('_meta', records_meta)
        ])

        return json.dumps(json_records, **self._pretty_print_opts)

    def encode_record(self, task):
        """
        Encodes a single workflow_task into it's JSON representation.

        :param task: Single instance of workflow_task resource

        :return: JSON representation of workflow_task resource
        """

        return json.dumps(self._encode_record(task), **self._pretty_print_opts)

    def _encode_record(self, task):
        """
        Encodes a single workflow_task into it's JSON representation.

        :param task: Single instance of workflow_task resource

        :return: JSON representation of workflow_task resource
        """

        if task is None:
            return None

        json_record = OrderedDict()

        for field in self._selected_fields:
            json_record[field] = self._get_field_value(task, field)

        json_record['_links'] = self._links(task)

        return json_record

    @staticmethod
    def _links(task):
        """
        Generates JSON representation of the HATEOAS links to be attached to the workflow_task resource.

        :param task: workflow_task resource for which to generate HATEOAS links

        :return: JSON representation of the HATEOAS links for worklow_task resource
        """

        links = OrderedDict([
            ('workflow', url_for('.get_workflow', wf_id=task.wf_id))

        ])
        if task.job_id:
            links.update({'job': url_for('.get_workflow_job', wf_id=task.wf_id, job_id=task.job_id)})
        return links