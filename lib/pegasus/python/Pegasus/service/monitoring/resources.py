#  Copyright 2007-2014 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the 'License');
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__author__ = 'Rajiv Mayani'

import logging

from Pegasus.db.schema import (
    RCLFN, RCPFN, DashboardWorkflow, DashboardWorkflowstate, Host,
    Invocation, Job, JobInstance, Jobstate, RCMeta, Task, TaskMeta,
    Workflow, WorkflowFiles, WorkflowMeta, Workflowstate
)
from Pegasus.service.base import BaseResource

log = logging.getLogger(__name__)


class CombinationResource(BaseResource):
    """
    CombinationResource allows us to create a combined context when processing queries spanning across resources.

    Given:

    WorkflowResource Fields are [
        'wf_id',
        ..
        'timestamp'
    ]

    WorkflowstateResource Fields are [
        'state',
        ..
        'timestamp'
    ]

    Example:

    Trying to get Worklfow resource as /workflow?query=timestamp > x and timestamp < y

    Here there is no ambiguity as timestamp maps to WorkflowResource.

    Example:

    Trying to get Worklfow resource as /workflow?query=timestamp > x and restart_count < 2

    We are working across multiple resources. WorkflowResource and restart_count field which is from
    WorkflostateResource. Now we have Ambiguity

    As timestamp field here is ambiguous, we cannot know the resource that the query field refers to.
    CombinationResource allows us

    """

    def __init__(self, *resources):
        self._resources = resources
        self._resource_map = {}

        for resource in resources:
            self._resource_map[resource.prefix] = resource

        self._fields = None
        self._prefixed_fields = None
        self._field_prefix_map = None
        self._mapped_fields = None

    @property
    def fields(self):
        if self._fields is None:
            self._fields = []
            for resource in self._resources:
                self._fields.extend(resource.fields)

        return self._fields

    @property
    def prefixed_fields(self):
        if self._prefixed_fields is None:
            collisions = set([])
            self._prefixed_fields = set()
            self._field_prefix_map = {}

            for resource in self._resources:
                for field in resource.fields:
                    prefixed_field = '%s.%s' % (resource.prefix, field)
                    self._prefixed_fields.add(prefixed_field)
                    self._field_prefix_map[prefixed_field] = resource.prefix
                    if field in self._prefixed_fields:
                        # Collision
                        collisions.add(field)
                        del self._field_prefix_map[field]
                    else:
                        self._prefixed_fields.add(field)
                        self._field_prefix_map[field] = resource.prefix

            self._prefixed_fields -= collisions

        return self._prefixed_fields

    @property
    def field_prefix_map(self):
        if self._field_prefix_map is None:
            ignore = self.prefixed_fields

        return self._field_prefix_map

    def get_field_prefix(self, field):
        return self.field_prefix_map[
            field
        ] if field in self.field_prefix_map else None

    def mapped_fields(self, alias=None):
        if self._mapped_fields:
            return self._mapped_fields

        self._mapped_fields = mapped_fields = {}

        for resource in self._resources:
            for field in self.prefixed_fields:
                if field in resource.prefixed_fields:
                    mapped_fields[field] = resource.get_mapped_field(
                        field, alias
                    )

        return mapped_fields

    def get_mapped_field(self, field):
        mapped_fields = self.mapped_fields()
        return mapped_fields[field]

    def is_prefix_valid(self, field):
        splits = BaseResource._split_identifier(field)
        splits[0] in self._resource_map if len(splits) == 2 else True


class RootWorkflowResource(BaseResource):
    def __init__(self, alias=None):
        super(RootWorkflowResource,
              self).__init__(alias if alias else DashboardWorkflow)

        self._prefix = 'r'

        self._fields = [
            'wf_id', 'wf_uuid', 'submit_hostname', 'submit_dir',
            'planner_arguments', 'planner_version', 'user', 'grid_dn',
            'dax_label', 'dax_version', 'dax_file', 'dag_file_name',
            'timestamp'
        ]


class RootWorkflowstateResource(BaseResource):
    def __init__(self, alias=None):
        super(RootWorkflowstateResource,
              self).__init__(alias if alias else DashboardWorkflowstate)

        self._prefix = 'ws'

        self._fields = [
            'wf_id', 'state', 'status', 'restart_count', 'timestamp'
        ]


class WorkflowResource(BaseResource):
    def __init__(self, alias=None):
        super(WorkflowResource, self).__init__(alias if alias else Workflow)

        self._prefix = 'w'

        self._fields = [
            'wf_id', 'root_wf_id', 'parent_wf_id', 'wf_uuid',
            'submit_hostname', 'submit_dir', 'planner_arguments',
            'planner_version', 'user', 'grid_dn', 'dax_label', 'dax_version',
            'dax_file', 'dag_file_name', 'timestamp'
        ]


class WorkflowMetaResource(BaseResource):
    def __init__(self, alias=None):
        super(WorkflowMetaResource,
              self).__init__(alias if alias else WorkflowMeta)

        self._prefix = 'wm'

        self._fields = ['key', 'value']


class WorkflowFilesResource(BaseResource):
    def __init__(self, alias=None):
        super(WorkflowFilesResource,
              self).__init__(alias if alias else WorkflowFiles)

        self._prefix = 'wf'

        self._fields = ['wf_id', 'task_id']


class WorkflowstateResource(RootWorkflowstateResource):
    def __init__(self, alias=None):
        super(WorkflowstateResource,
              self).__init__(alias if alias else Workflowstate)


class JobResource(BaseResource):
    def __init__(self, alias=None):
        super(JobResource, self).__init__(alias if alias else Job)

        self._prefix = 'j'

        self._fields = [
            'job_id', 'exec_job_id', 'submit_file', 'type_desc', 'max_retries',
            'clustered', 'task_count', 'executable', 'argv', 'task_count'
        ]


class HostResource(BaseResource):
    def __init__(self, alias=None):
        super(HostResource, self).__init__(alias if alias else Host)

        self._prefix = 'h'

        self._fields = [
            'host_id', 'site', 'hostname', 'ip', 'uname', 'total_memory'
        ]


class JobstateResource(BaseResource):
    def __init__(self, alias=None):
        super(JobstateResource, self).__init__(alias if alias else Jobstate)

        self._prefix = 'js'

        self._fields = [
            'job_instance_id', 'state', 'jobstate_submit_seq', 'timestamp'
        ]


class TaskResource(BaseResource):
    def __init__(self, alias=None):
        super(TaskResource, self).__init__(alias if alias else Task)

        self._prefix = 't'

        self._fields = [
            'task_id', 'abs_task_id', 'type_desc', 'transformation', 'argv'
        ]


class TaskMetaResource(BaseResource):
    def __init__(self, alias=None):
        super(TaskMetaResource, self).__init__(alias if alias else TaskMeta)

        self._prefix = 'tm'

        self._fields = ['key', 'value']


class JobInstanceResource(BaseResource):
    def __init__(self, alias=None):
        super(JobInstanceResource,
              self).__init__(alias if alias else JobInstance)

        self._prefix = 'ji'

        self._fields = [
            'job_instance_id', 'host_id', 'job_submit_seq', 'sched_id', 'site',
            'user', 'work_dir', 'cluster_start', 'cluster_duration',
            'local_duration', 'subwf_id', 'stdout_text', 'stderr_text',
            'stdin_file', 'stdout_file', 'stderr_file', 'multiplier_factor',
            'exitcode'
        ]


class InvocationResource(BaseResource):
    def __init__(self, alias=None):
        super(InvocationResource,
              self).__init__(alias if alias else Invocation)

        self._prefix = 'i'

        self._fields = [
            'invocation_id', 'job_instance_id', 'abs_task_id',
            'task_submit_seq', 'start_time', 'remote_duration',
            'remote_cpu_time', 'exitcode', 'transformation', 'executable',
            'argv'
        ]


class RCLFNResource(BaseResource):
    def __init__(self, alias=None):
        super(RCLFNResource, self).__init__(alias if alias else RCLFN)

        self._prefix = 'l'

        self._fields = ['lfn_id', 'lfn']


class RCPFNResource(BaseResource):
    def __init__(self, alias=None):
        super(RCPFNResource, self).__init__(alias if alias else RCPFN)

        self._prefix = 'p'

        self._fields = ['pfn_id', 'pfn', 'site']


class RCMetaResource(BaseResource):
    def __init__(self, alias=None):
        super(RCMetaResource, self).__init__(alias if alias else RCMeta)

        self._prefix = 'rm'

        self._fields = ['key', 'value']
