import hashlib
import logging

from flask import g, make_response, request

from Pegasus.service import cache
from Pegasus.service._serialize import jsonify
from Pegasus.service.base import OrderedDict
from Pegasus.service.monitoring import monitoring as blueprint
from Pegasus.service.monitoring.queries import (
    MasterWorkflowQueries,
    StampedeWorkflowQueries,
)

log = logging.getLogger(__name__)

JSON_HEADER = {"Content-Type": "application/json"}


@blueprint.url_value_preprocessor
def pull_m_wf_id(endpoint, values):
    """
    If the requested endpoint contains a value for m_wf_id variable then extract it and set it in g.m_wf_id.
    """
    if values and "m_wf_id" in values:
        g.m_wf_id = values["m_wf_id"]


@blueprint.url_value_preprocessor
def pull_url_context(endpoint, values):
    """
    Create a context which can be used when generating url in link section of the responses.
    """
    url_context = {}
    keys = ["wf_id", "job_id", "task_id", "job_instance_id", "host_id", "instance_id"]

    if values:
        for key in keys:
            if key in values:
                url_context[key] = values[key]

        else:
            if url_context:
                g.url_context = url_context


@blueprint.before_request
def compute_stampede_db_url():
    """
    If the requested endpoint requires connecting to a STAMPEDE database, then determine STAMPEDE DB URL and store it
    in g.stampede_db_url. Also, set g.m_wf_id to be the root workflow's uuid
    """
    if "/workflow" not in request.path or "m_wf_id" not in g:
        return

    md5sum = hashlib.md5()
    md5sum.update(g.master_db_url.encode("utf-8"))
    m_wf_id = g.m_wf_id

    def _get_cache_key(key_suffix):
        return "{}.{}".format(md5sum.hexdigest(), key_suffix)

    cache_key = _get_cache_key(m_wf_id)

    if cache.get(cache_key):
        log.debug("Cache Hit: compute_stampede_db_url %s" % cache_key)
        root_workflow = cache.get(cache_key)

    else:
        log.debug("Cache Miss: compute_stampede_db_url %s" % cache_key)
        queries = MasterWorkflowQueries(g.master_db_url)
        root_workflow = queries.get_root_workflow(m_wf_id)
        queries.close()

        cache.set(_get_cache_key(root_workflow.wf_id), root_workflow, timeout=600)
        cache.set(_get_cache_key(root_workflow.wf_uuid), root_workflow, timeout=600)

    g.url_m_wf_id = root_workflow.wf_id
    g.m_wf_id = root_workflow.wf_uuid
    g.stampede_db_url = root_workflow.db_url


@blueprint.before_request
def get_query_args():
    g.query_args = {}

    def to_int(q_arg, value):
        try:
            return int(value)
        except ValueError as e:
            log.exception(
                "Query Argument {} = {} is not a valid int".format(q_arg, value)
            )
            e = ValueError(
                "Expecting integer for argument {}, found {!r}".format(
                    q_arg, str(value)
                )
            )
            e.codes = ("INVALID_QUERY_ARGUMENT", 400)
            raise e from None

    def to_str(q_arg, value):
        return value

    def to_bool(q_arg, value):
        value = value.strip().lower()

        if value in {"1", "true"}:
            return True

        elif value in {"0", "false"}:
            return False

        else:
            log.exception(
                "Query Argument {} = {} is not a valid boolean".format(q_arg, value)
            )
            e = ValueError(
                "Expecting boolean for argument {}, found {!r}".format(
                    q_arg, str(value)
                )
            )
            e.codes = ("INVALID_QUERY_ARGUMENT", 400)
            raise e

    query_args = OrderedDict(
        [
            ("pretty-print", to_bool),
            ("start-index", to_int),
            ("max-results", to_int),
            ("query", to_str),
            ("order", to_str),
        ]
    )

    for arg, cast in query_args.items():
        if arg in request.args:
            g.query_args[arg.replace("-", "_")] = cast(arg, request.args.get(arg))


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


@blueprint.route("/root")
def get_root_workflows(username):
    """
    Returns a collection of root level workflows.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response.

    :statuscode 200: OK
    :statuscode 204: No content; when no workflows found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Root Workflow
    """
    queries = MasterWorkflowQueries(g.master_db_url)
    paged_response = queries.get_root_workflows(**g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>")
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
    queries = MasterWorkflowQueries(g.master_db_url)
    record = queries.get_root_workflow(m_wf_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


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
        "workflow_meta"  : href:workflow_meta,
        "workflow_state" : href:workflow_state,
        "job"            : href:job,
        "task"           : href:task,
        "host"           : href:host,
        "invocation"     : href:invocation
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow")
def get_workflows(username, m_wf_id):
    """
    Returns a collection of workflows.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no workflows found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Workflow
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflows(g.m_wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>")
def get_workflow(username, m_wf_id, wf_id):
    """
    Returns workflow identified by m_wf_id, wf_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Workflow
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_workflow(wf_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


"""
Workflow Meta

{
    "key"         : string:key,
    "value"       : string:value,
    "_links"      : {
        "workflow" : <href:workflow>
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/meta")
def get_workflow_meta(username, m_wf_id, wf_id):
    """
    Returns a collection of workflow's metadata.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no workflow metadata found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: WorkflowMeta
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_meta(g.m_wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


"""
Workflow Files

{
    "wf_id"  : int:wf_id,
    "lfn_id" : string:lfn_id,
    "lfn"    : string:lfn,
    "pfns"   : [
         {
            "pfn_id" : <int:pfn_id>
            "pfn"    : <string:pfn>
            "site"   : <string:site>
         }
    ],
    "meta" : [
         {
            "meta_id" : <int:meta_id>
            "key"     : <string:key>
            "value"   : <string:value>
         }
    ],
    "_links"      : {
        "workflow" : <href:workflow>
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/files")
def get_workflow_files(username, m_wf_id, wf_id):
    """
    Returns a collection of workflows.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no workflows found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Workflow
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_files(g.m_wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


"""
Workflow State

{
    "wf_id"         : int:wf_id,
    "state"         : string:state,
    "status"        : int:status,
    "restart_count" : int:restart_count,
    "timestamp"     : datetime:timestamp,
    "_links"        : {
        "workflow" : <href:workflow>
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/state")
def get_workflow_state(username, m_wf_id, wf_id):
    """
    Returns a collection of Workflow States.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no workflowstates found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: WorkflowState
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_state(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


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
    "task_count"  : int:task_count,
    "_links"      : {
        "workflow"     : href:workflow,
        "task"         : href:task,
        "job_instance" : href:job_instance
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job")
def get_workflow_jobs(username, m_wf_id, wf_id):
    """
    Returns a collection of Jobs.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobs found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_jobs(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/<int:job_id>")
def get_job(username, m_wf_id, wf_id, job_id):
    """
    Returns job identified by m_wf_id, wf_id, job_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_job(job_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


"""
Host

{
    "host_id"      : int:host_id,
    "site"         : string:site,
    "hostname"     : string:hostname,
    "ip"           : string:ip,
    "uname"        : string:uname,
    "total_memory" : string:total_memory,
    "_links"       : {
        "workflow" : href:workflow
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/host")
def get_workflow_hosts(username, m_wf_id, wf_id):
    """
    Returns a collection of Hosts.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no hosts found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Host
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_hosts(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/host/<int:host_id>")
def get_host(username, m_wf_id, wf_id, host_id):
    """
    Returns host identified by m_wf_id, wf_id, host_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Host
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_host(host_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


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


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/job/<int:job_id>/job-instance/<int:job_instance_id>/state"
)
def get_job_instance_states(username, m_wf_id, wf_id, job_id, job_instance_id):
    """
    Returns a collection of Job States.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobstates found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: JobState
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_job_instance_states(
        wf_id, job_id, job_instance_id, **g.query_args
    )

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


"""
Task

{
    "task_id"        : int:task_id,
    "abs_task_id"    : string:abs_task_id,
    "type_desc"      : string: type_desc,
    "transformation" : string:transformation,
    "argv"           : string:argv,
    "task_count"     : int:task_count,
    "_links"         : {
        "workflow"  : href:workflow,
        "job"       : href:job,
        "task_meta" : href:task_meta
    }
}
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/task")
def get_workflow_tasks(username, m_wf_id, wf_id):
    """
    Returns a collection of Tasks.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no tasks found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Task
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_tasks(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/<int:job_id>/task")
def get_job_tasks(username, m_wf_id, wf_id, job_id):
    """
    Returns a collection of Tasks.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no tasks found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Task
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_job_tasks(wf_id, job_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/task/<int:task_id>")
def get_task(username, m_wf_id, wf_id, task_id):
    """
    Returns task identified by m_wf_id, wf_id, task_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Task
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_task(task_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


"""
Task Meta

{
    "key"         : string:key,
    "value"       : string:value,
    "_links"      : {
        "task" : "<href:task>"
    }
}
"""


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/task/<int:task_id>/meta"
)
def get_task_meta(username, m_wf_id, wf_id, task_id):
    """
    Returns a collection of task's metadata.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no workflow metadata found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: TaskMeta
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_task_meta(task_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


"""
Job Instance

{
    "job_instance_id"   : int:job_instance_id,
    "host_id"           : int:host_id,
    "job_submit_seq"    : int:job_submit_seq,
    "sched_id"          : string:sched_id,
    "site"              : string:site,
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
    "exitcode"          : int:exitcode,
    "_links"            : {
        "job_state"  : href:job_state,
        "host"       : href:host,
        "invocation" : href:invocation,
        "job"        : href:job
    }
}
"""


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/job/<int:job_id>/job-instance"
)
def get_job_instances(username, m_wf_id, wf_id, job_id):
    """
    Returns a collection of JobInstances.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no job instances found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: JobInstance
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_job_instances(
        wf_id,
        job_id,
        recent=request.args.get("recent", "false") == "true",
        **g.query_args,
    )

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/job-instance/<int:job_instance_id>"
)
def get_job_instance(username, m_wf_id, wf_id, job_instance_id):
    """
    Returns job instance identified by m_wf_id, wf_id, job_id, job_instance_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: JobInstance
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_job_instance(job_instance_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


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


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/invocation")
def get_workflow_invocations(username, m_wf_id, wf_id):
    """
    Returns a collection of Invocations.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no invocations found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Invocation
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_workflow_invocations(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/job/<int:job_id>/job-instance/<int:job_instance_id>/invocation"
)
def get_job_instance_invocations(username, m_wf_id, wf_id, job_id, job_instance_id):
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_job_instance_invocations(
        wf_id, job_id, job_instance_id, **g.query_args
    )

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route(
    "/root/<string:m_wf_id>/workflow/<string:wf_id>/invocation/<int:invocation_id>"
)
def get_invocation(username, m_wf_id, wf_id, invocation_id):
    """
    Returns invocation identified by m_wf_id, wf_id, invocation_id.

    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure
    :statuscode 404: Not found

    :return type: Record
    :return resource: Invocation
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    record = queries.get_invocation(invocation_id)

    #
    # Generate JSON Response
    #
    response_json = jsonify(record)

    return make_response(response_json, 200, JSON_HEADER)


"""
Utilities
"""


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/running")
def get_running_jobs(username, m_wf_id, wf_id):
    """
    Returns a collection of running Jobs.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobs found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_running_jobs(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/successful")
def get_successful_jobs(username, m_wf_id, wf_id):
    """
    Returns a collection of successful Jobs.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobs found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_successful_jobs(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/failed")
def get_failed_jobs(username, m_wf_id, wf_id):
    """
    Returns a collection of failed Jobs.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobs found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_failed_jobs(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)


@blueprint.route("/root/<string:m_wf_id>/workflow/<string:wf_id>/job/failing")
def get_failing_jobs(username, m_wf_id, wf_id):
    """
    Returns a collection of failing Jobs.

    :query int start-index: Return results starting from record <start-index> (0 indexed)
    :query int max-results: Return a maximum of <max-results> records
    :query string query: Search criteria
    :query string order: Sorting criteria
    :query boolean pretty-print: Return formatted JSON response

    :statuscode 200: OK
    :statuscode 204: No content; when no jobs found.
    :statuscode 400: Bad request
    :statuscode 401: Authentication failure
    :statuscode 403: Authorization failure

    :return type: Collection
    :return resource: Job
    """
    queries = StampedeWorkflowQueries(g.stampede_db_url)

    paged_response = queries.get_failing_jobs(wf_id, **g.query_args)

    if paged_response.total_records == 0:
        log.debug("Total records is 0; returning HTTP 204 No content")
        return make_response("", 204, JSON_HEADER)

    #
    # Generate JSON Response
    #
    response_json = jsonify(paged_response)

    return make_response(response_json, 200, JSON_HEADER)
