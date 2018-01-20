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

from decimal import Decimal

from flask import url_for
from flask.json import JSONEncoder
from Pegasus.db.schema import *
from Pegasus.service.base import (
    ErrorResponse, OrderedDict, OrderedSet, PagedResponse
)
from Pegasus.service.monitoring.resources import (
    HostResource, InvocationResource, JobInstanceResource,
    JobResource, JobstateResource, RCLFNResource, RCMetaResource,
    RCPFNResource, RootWorkflowResource, RootWorkflowstateResource,
    TaskMetaResource, TaskResource, WorkflowFilesResource,
    WorkflowMetaResource, WorkflowResource, WorkflowstateResource
)
from sqlalchemy.orm.attributes import instance_state

log = logging.getLogger(__name__)


class PegasusServiceJSONEncoder(JSONEncoder):
    """
    JSON Encoder for Pegasus Service API Resources
    """

    def default(self, obj):
        def obj_to_dict(
            resource, fields=None, ignore_unloaded=False, data=None
        ):
            data = data if data else obj
            obj_dict = OrderedDict()

            if ignore_unloaded:
                unloaded = instance_state(data).unloaded
                log.debug('ignore_unloaded is True, ignoring %s' % unloaded)

            for attribute in resource.fields:
                if not ignore_unloaded or (
                    ignore_unloaded and attribute not in unloaded
                ):
                    obj_dict[attribute] = getattr(data, attribute)

            if fields:
                for attribute in fields:
                    try:
                        if not ignore_unloaded or (
                            ignore_unloaded and attribute not in unloaded
                        ):
                            obj_dict[attribute] = getattr(data, attribute)
                    except AttributeError:
                        pass

            return obj_dict

        if isinstance(obj, PagedResponse):
            json_record = OrderedDict([('records', obj.records)])

            if obj.total_records or obj.total_filtered:
                meta = OrderedDict()

                if obj.total_records is not None:
                    meta['records_total'] = obj.total_records

                if obj.total_filtered is not None:
                    meta['records_filtered'] = obj.total_filtered

                json_record['_meta'] = meta

            return json_record

        elif isinstance(obj, ErrorResponse):
            json_record = OrderedDict(
                [('code', obj.code), ('message', obj.message)]
            )

            if obj.errors:
                json_record['errors'] = [
                    {
                        'field': f,
                        'errors': e
                    } for f, e in obj.errors
                ]

            return json_record

        elif isinstance(obj, DashboardWorkflow):
            json_record = obj_to_dict(RootWorkflowResource())
            json_record['workflow_state'] = obj.workflow_state

            json_record['_links'] = OrderedDict(
                [
                    (
                        'workflow',
                        url_for(
                            '.get_workflows', m_wf_id=obj.wf_id, _method='GET'
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, DashboardWorkflowstate):
            json_record = obj_to_dict(RootWorkflowstateResource())

            return json_record

        elif isinstance(obj, Workflow):
            json_record = obj_to_dict(WorkflowResource())
            json_record['_links'] = OrderedDict(
                [
                    (
                        'workflow_meta',
                        url_for(
                            '.get_workflow_meta',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'workflow_state',
                        url_for(
                            '.get_workflow_state',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'job',
                        url_for(
                            '.get_workflow_jobs',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'task',
                        url_for(
                            '.get_workflow_tasks',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'host',
                        url_for(
                            '.get_workflow_hosts',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'invocation',
                        url_for(
                            '.get_workflow_invocations',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, WorkflowMeta):
            json_record = obj_to_dict(WorkflowMetaResource())
            json_record['_links'] = OrderedDict(
                [('workflow', url_for('.get_workflow', wf_id=obj.wf_id))]
            )

            return json_record

        elif isinstance(obj, WorkflowFiles):
            json_record = obj_to_dict(WorkflowFilesResource())

            return json_record

        elif isinstance(obj, Workflowstate):
            json_record = obj_to_dict(WorkflowstateResource())
            json_record['_links'] = OrderedDict(
                [('workflow', url_for('.get_workflow', wf_id=obj.wf_id))]
            )

            return json_record

        elif isinstance(obj, Job):
            json_record = obj_to_dict(JobResource())

            if hasattr(obj, 'job_instance'):
                json_record['job_instance'] = obj.job_instance

            json_record['_links'] = OrderedDict(
                [
                    ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)), (
                        'task',
                        url_for(
                            '.get_workflow_tasks',
                            wf_id=obj.wf_id,
                            _method='GET'
                        )
                    ), (
                        'job_instance',
                        url_for(
                            '.get_job_instances',
                            wf_id=obj.wf_id,
                            job_id=obj.job_id,
                            _method='GET'
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, Host):
            json_record = obj_to_dict(HostResource())
            json_record['_links'] = OrderedDict(
                [
                    (
                        'workflow',
                        url_for(
                            '.get_workflows', m_wf_id=obj.wf_id, _method='GET'
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, Jobstate):
            json_record = obj_to_dict(JobstateResource())
            json_record['_links'] = OrderedDict(
                [
                    (
                        'job_instance',
                        url_for(
                            '.get_job_instance',
                            job_instance_id=obj.job_instance_id
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, Task):
            json_record = obj_to_dict(TaskResource())
            json_record['_links'] = OrderedDict(
                [
                    ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)), (
                        'job',
                        url_for(
                            '.get_job', wf_id=obj.wf_id, job_id=obj.job_id
                        )
                    ), (
                        'task_meta',
                        url_for(
                            '.get_task_meta',
                            wf_id=obj.wf_id,
                            job_id=obj.job_id,
                            task_id=obj.task_id
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, TaskMeta):
            json_record = obj_to_dict(TaskMetaResource())
            json_record['_links'] = OrderedDict(
                [('task', url_for('.get_task', task_id=obj.task_id))]
            )

            return json_record

        elif isinstance(obj, JobInstance):
            json_record = obj_to_dict(
                JobInstanceResource(), ignore_unloaded=True
            )
            json_record['_links'] = OrderedDict(
                [
                    ('job', url_for('.get_job', job_id=obj.job_id)), (
                        'state',
                        url_for(
                            '.get_job_instance_states',
                            job_id=obj.job_id,
                            job_instance_id=obj.job_instance_id,
                            _method='GET'
                        )
                    )
                ]
            )

            json_record['_links']['host'] = url_for(
                '.get_host', host_id=obj.host_id
            ) if obj.host_id else None
            json_record['_links']['invocation'] = url_for(
                '.get_job_instance_invocations',
                job_id=obj.job_id,
                job_instance_id=obj.job_instance_id,
                _method='GET'
            )

            return json_record

        elif isinstance(obj, Invocation):
            json_record = obj_to_dict(InvocationResource())
            json_record['_links'] = OrderedDict(
                [
                    ('workflow', url_for('.get_workflow', wf_id=obj.wf_id)), (
                        'job_instance',
                        url_for(
                            '.get_job_instance',
                            job_instance_id=obj.job_instance_id
                        )
                    )
                ]
            )

            return json_record

        elif isinstance(obj, RCLFN):
            json_record = obj_to_dict(RCLFNResource(), fields=['pfns', 'meta'])

            if hasattr(obj, 'extras'):
                json_record_2 = obj_to_dict(
                    WorkflowFilesResource(), data=obj.extras
                )
                json_record_2.update(json_record)
                json_record = json_record_2

                json_record['_links'] = OrderedDict(
                    [
                        (
                            'workflow',
                            url_for('.get_workflow', wf_id=obj.extras.wf_id)
                        ), (
                            'task',
                            url_for(
                                '.get_task',
                                wf_id=obj.extras.wf_id,
                                task_id=obj.extras.task_id
                            )
                        )
                    ]
                )

            return json_record

        elif isinstance(obj, RCPFN):
            json_record = obj_to_dict(RCPFNResource())

            return json_record

        elif isinstance(obj, RCMeta):
            json_record = obj_to_dict(RCMetaResource())

            return json_record

        elif isinstance(obj, OrderedSet):
            json_record = [item for item in obj]

            return json_record

        elif isinstance(obj, Decimal):
            return float(obj)

        return JSONEncoder.default(self, obj)
