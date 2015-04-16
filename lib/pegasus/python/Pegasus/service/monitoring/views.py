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

from flask import g, request

from Pegasus.service.monitoring import monitoring_routes


"""
Root Workflow

{
    "wf_id"             : int:wf_id,
    "wf_uuid"           : string:wf_uuid,
    "submit_hostname"   : string:submit_hostname,
    "submit_dir"        : string:submit_dir,
    "planner_arguments" : string:planner_arguments,
    "planner_version"   : string:planner_version,
    "user"              : string:user,
    "grid_dn"           : string:grid_dn,
    "dax_label"         : string:dax_label,
    "dax_version"       : string:dax_version,
    "dax_file"          : string:dax_file,
    "dag_file_name"     : string:dag_file_name,
    "timestamp"         : int:timestamp,
    "workflow_state"    : object:workflow_state,
    "_links"            : {
        "workflow" : href:workflow
    }
}
"""


@monitoring_routes.before_request
def get_query_args():
    g.query_args = {}
    query_args = ['start-index', 'max-results', 'query', 'recent']

    for arg in query_args:
        if arg in request.args:
            g.query_args[arg] = request.args.get(arg)


@monitoring_routes.route('/user/<string:username>/root')
@monitoring_routes.route('/user/<string:username>/root/query', methods=['POST'])
def get_root_workflows(username):
    """
    Returns a collection of root level workflows.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query boolean pretty-print: Return formatted JSON response.

    :statuscode 200: OK
    :statuscode 204: No content; when no workflows found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Root Workflow
    """
    pass


@monitoring_routes.route('/user/<string:username>/root/<string:m_wf_id>')
def get_root_workflow(username, m_wf_id):
    """
    Returns root level workflow identified by m_wf_id.

    :query boolean pretty-print: Return formatted JSON response.

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Root Workflow
    """
    pass


"""
Workflow

{
    "wf_id"             : int:wf_id,
    "root_wf_id"        : int:root_wf_id,
    "parent_wf_id"      : int:parent_wf_id,
    "wf_uuid"           : string:wf_uuid,
    "submit_hostname"   : string:submit_hostname,
    "submit_dir"        : string:submit_dir,
    "planner_arguments" : string:planner_arguments,
    "planner_version"   : string:planner_version,
    "user"              : string:user,
    "grid_dn"           : string:grid_dn,
    "dax_label"         : string:dax_label,
    "dax_version"       : string:dax_version,
    "dax_file"          : string:dax_file,
    "dag_file_name"     : string:dag_file_name,
    "timestamp"         : int:timestamp,
    "_links"            : {
        "workflow_state" : href:workflow_state,
        "job"            : href:job,
        "task"           : href:task,
        "host"           : href:host,
        "invocation"     : href:invocation
    }
}
"""


@monitoring_routes.route('/user/<string:username>/root/<string:m_wf_id>/workflow')
@monitoring_routes.route('/user/<string:username>/root//<string:m_wf_id>/workflow/query', methods=['POST'])
def get_workflows(username, m_wf_id):
    pass


@monitoring_routes.route('/user/<string:username>/root/<string:m_wf_id>/workflow/<string:wf_id>')
def get_workflow(username, m_wf_id, wf_id):
    pass


"""
{
    "state"         : string:state,
    "status"        : int:status,
    "restart_count" : int:restart_count,
    "timestamp"     : datetime:timestamp,
    "_links"          : {
        "workflow"     : "<href:workflow>"
    }
}
"""

"""
Job

{
    "job_id"      : int: job_id,
    "exec_job_id" : string: exec_job_id,
    "submit_file" : string: submit_file,
    "type_desc"   : string: type_desc,
    "max_retries" : int: max_retries,
    "clustered"   : bool: clustered,
    "task_count"  : int: task_count,
    "executable"  : string: executable,
    "argv"        : string: argv,
    "_links"      : {
        "workflow"     : href:workflow,
        "task"         : href:task,
        "job_instance" : href:job_instance
    }
}
"""

"""
Host

    {
    "host_id"      : int:host_id,
    "site_name"    : string:site_name,
    "hostname"     : string:hostname,
    "ip"           : string:ip,
    "uname"        : string:uname,
    "total_memory" : string:total_memory,
    "_links"       : {
        "workflow"     : href:workflow,
        "job_instance" : href:job_instance
    }
}
"""

"""
Job State

{
    "job_instance_id"     : "<int:job_instance_id>",
    "state"               : "<string:state>",
    "jobstate_submit_seq" : "<int:jobstate_submit_seq>",
    "timestamp"           : "<int:timestamp>",
    "_links"              : {
        "job_instance" : "href:job_instance"
    }
}
"""

"""
Task

{
    "task_id"        : int:task_id,
    "abs_task_id"    : string:abs_task_id,
    "type_desc"      : string: type_desc,
    "transformation" : string:transformation,
    "argv"           : string:argv,
    "_links"         : {
        "workflow" : href:workflow,
        "job"      : href:job
    }
}
"""

"""
Job Instance

{
    "job_instance_id"   : int:job_instance_id,
    "host_id"           : int:host_id,
    "job_submit_seq"    : int:job_submit_seq,
    "sched_id"          : string:sched_id,
    "site_name"         : string:site_name,
    "user"              : string:user,
    "work_dir"          : string:work_dir,
    "cluster_start"     : int:cluster_start,
    "cluster_duration"  : int:cluster_duration,
    "local_duration"    : int:local_duration,
    "subwf_id"          : int:subwf_id,
    "stdout_text"       : string:stdout_text,
    "stderr_text"       : string:stderr_text,
    "stdin_file"        : string:stdin_file,
    "stdout_file"       : string:stdout_file,
    "stderr_file"       : string:stderr_file,
    "multiplier_factor" : int:multiplier_factor,
    "_links"            : {
        "job_state"  : href:job_state,
        "host"       : href:host,
        "invocation" : href:invocation,
        "job"        : href:job
    }
}
"""

"""
Invocation

{
    "invocation_id"   : int:invocation_id,
    "job_instance_id" : int:job_instance_id,
    "abs_task_id"     : string:abs_task_id,
    "task_submit_seq" : int:task_submit_seq,
    "start_time"      : int:start_time,
    "remote_duration" : int:remote_duration,
    "remote_cpu_time" : int:remote_cpu_time,
    "exitcode"        : int:exitcode,
    "transformation"  : string:transformation,
    "executable"      : string:executable,
    "argv"            : string:argv,
    "_links"          : {
        "workflow"     : href:workflow,
        "job_instance" : href:job_instance
    }
}
"""


@monitoring_routes.errorhandler(404)
def page_not_found(error):
    pass


@monitoring_routes.errorhandler(BaseException)
def master_database_missing(error):
    pass
