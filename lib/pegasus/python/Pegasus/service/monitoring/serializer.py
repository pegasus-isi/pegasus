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

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from decimal import Decimal

from flask import url_for
from flask.json import JSONEncoder

from Pegasus.db.schema import *

from Pegasus.service.base import PagedResponse
from Pegasus.service.monitoring.resources import RootWorkflowResource, RootWorkflowstateResource
from Pegasus.service.monitoring.resources import JobInstanceResource, JobstateResource, TaskResource, InvocationResource
from Pegasus.service.monitoring.resources import WorkflowResource, WorkflowstateResource, JobResource, HostResource


class PegasusServiceJSONEncoder(JSONEncoder):
    """
    JSON Encoder for Pegasus Service API Resources
    """

    def default(self, obj):
        def obj_to_dict(resource):
            obj_dict = OrderedDict()

            for attribute in resource.fields:
                obj_dict[attribute] = getattr(obj, attribute)

            return obj_dict

        if isinstance(obj, PagedResponse):
            json_record = OrderedDict([
                ('records', obj.records)
            ])

            if obj.total_records or obj.total_filtered:
                meta = OrderedDict()

                if obj.total_records:
                    meta['records_total'] = obj.total_records

                if obj.total_filtered:
                    meta['records_filtered'] = obj.total_filtered

                json_record['_meta'] = meta

            return json_record

        elif isinstance(obj, DashboardWorkflow):
            json_record = obj_to_dict(RootWorkflowResource())
            json_record['workflow_state'] = obj.workflow_state

            json_record['_links'] = OrderedDict([
                ('workflow', url_for('.get_workflows', m_wf_id=obj.wf_id, _method='GET'))
            ])

            return json_record

        elif isinstance(obj, DashboardWorkflowstate):
            json_record = obj_to_dict(RootWorkflowstateResource())

            return json_record

        elif isinstance(obj, Workflow):
            json_record = obj_to_dict(WorkflowResource())
            json_record['_links'] = OrderedDict([
                ('workflow_state', url_for('.get_workflow_state', wf_id=obj.wf_id, _method='GET')),
                ('job', url_for('.get_workflow_jobs', wf_id=obj.wf_id, _method='GET')),
                ('task', url_for('.get_workflow_tasks', wf_id=obj.wf_id, _method='GET')),
                ('host', url_for('.get_workflow_hosts', wf_id=obj.wf_id, _method='GET')),
                ('invocation', url_for('.get_workflow_invocations', wf_id=obj.wf_id, _method='GET'))
            ])

            return json_record

        elif isinstance(obj, Workflowstate):
            json_record = obj_to_dict(WorkflowstateResource())
            json_record['_links'] = OrderedDict([
                ('workflow', url_for('.get_workflow', wf_id=obj.wf_id))
            ])

            return json_record

        elif isinstance(obj, Job):
            json_record = obj_to_dict(JobResource())
            json_record['_links'] = OrderedDict([
                ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)),
                ('task', url_for('.get_workflow_tasks', wf_id=obj.wf_id, _method='GET')),
                ('job_instance', url_for('.get_job_instances', wf_id=obj.wf_id, job_id=obj.job_id, _method='GET'))
            ])

            return json_record

        elif isinstance(obj, Host):
            json_record = obj_to_dict(HostResource())
            json_record['_links'] = OrderedDict([
                # TODO: Conditional handling of job_instance
                ('job_instance', url_for('.get_job_instance', wf_id=obj.wf_id, job_id=obj.host_id))
            ])

            return json_record

        elif isinstance(obj, Jobstate):
            json_record = obj_to_dict(JobstateResource())
            json_record['_links'] = OrderedDict([
                ('job_instance', url_for('.get_job_instance', job_instance_id=obj.job_instance_id))
            ])

            return json_record

        elif isinstance(obj, Task):
            json_record = obj_to_dict(TaskResource())
            json_record['_links'] = OrderedDict([
                ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)),
                ('job', url_for('.get_workflow_job', wf_id=obj.wf_id, job_id=obj.job_id))
            ])

            return json_record

        elif isinstance(obj, JobInstance):
            json_record = obj_to_dict(JobInstanceResource())
            json_record['_links'] = OrderedDict([
                ('job', url_for('.get_job', job_id=obj.job_id)),
                ('state', url_for('.get_job_instance_states', job_id=obj.job_id,
                                  job_instance_id=obj.job_instance_id, _method='GET')),
                ('host', url_for('.get_host', host_id=obj.host_id)),
                ('invocation', url_for('.get_job_instance_invocations', job_id=obj.job_id,
                                       job_instance_id=obj.job_instance_id, _method='GET'))
            ])

            return json_record

        elif isinstance(obj, Invocation):
            json_record = obj_to_dict(InvocationResource())
            json_record['_links'] = OrderedDict([
                ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)),
                ('job_instance', url_for('.get_job_instance', job_instance_id=obj.job_instance_id))
            ])

            return json_record

        elif isinstance(obj, Decimal):
            return float(obj)

        return JSONEncoder.default(self, obj)
