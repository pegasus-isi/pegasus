import logging
import os

from flask import g, redirect, render_template, request, send_from_directory, url_for
from sqlalchemy.orm.exc import NoResultFound

from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.service import filters
from Pegasus.service._serialize import serialize
from Pegasus.service.base import ErrorResponse, ServiceError
from Pegasus.service.dashboard import blueprint
from Pegasus.service.dashboard.dashboard import Dashboard, NoWorkflowsFoundError
from Pegasus.service.dashboard.queries import MasterDBNotFoundError
from Pegasus.tools import utils

log = logging.getLogger(__name__)


@blueprint.route("/")
def redirect_to_index():
    return redirect(url_for(".index"))


@blueprint.route("/u/<username>/")
def index(username):
    """
    List all workflows from the master database.
    """
    try:
        dashboard = Dashboard(g.master_db_url)
        args = __get_datatables_args()
        if request.is_xhr:
            count, filtered, workflows, totals = dashboard.get_root_workflow_list(
                **args
            )
        else:
            totals = dashboard.get_root_workflow_list(counts_only=True, **args)

    except NoWorkflowsFoundError as e:
        if request.is_xhr:
            workflows = []
            d = {
                "draw": args["sequence"] if args["sequence"] else 0,
                "recordsTotal": e.count if e.count is not None else len(workflows),
                "data": workflows,
            }
            if args["limit"]:
                d["recordsFiltered"] = e.filtered

            return serialize(d)

        return render_template("workflow.html", counts=(0, 0, 0, 0))

    if request.is_xhr:
        d = {
            "draw": args["sequence"] if args["sequence"] else 0,
            "recordsTotal": count if count is not None else len(workflows),
            "data": [w._asdict() for w in workflows],
        }
        if args["limit"]:
            d["recordsFiltered"] = filtered

        return serialize(d)

    return render_template("workflow.html", counts=totals)


@blueprint.route("/u/<username>/r/<root_wf_id>/w")
@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>")
def workflow(username, root_wf_id, wf_id=None):
    """
    Get details for a specific workflow.
    """
    wf_uuid = request.args.get("wf_uuid", None)

    if not wf_id and not wf_uuid:
        raise ValueError("Workflow ID or Workflow UUID is required")

    if wf_id:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
    else:
        dashboard = Dashboard(g.master_db_url, root_wf_id)

    try:
        counts, details, statistics = dashboard.get_workflow_information(wf_id, wf_uuid)
    except NoResultFound:
        return render_template("error/workflow/workflow_details_missing.html")

    return render_template(
        "workflow/workflow_details.html",
        root_wf_id=root_wf_id,
        wf_id=details.wf_id,
        workflow=details,
        counts=counts,
        statistics=statistics,
    )


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/sw/", methods=["GET"])
def sub_workflows(username, root_wf_id, wf_id):
    """
    Get a list of all sub-workflow of a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    sub_workflows = dashboard.get_sub_workflows(wf_id)

    for i in range(len(sub_workflows)):
        sub_workflows[i] = sub_workflows[i]._asdict()

    return serialize(sub_workflows)


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/j/failed/", methods=["GET"])
def failed_jobs(username, root_wf_id, wf_id):
    """
    Get a list of all failed jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, failed_jobs_list = dashboard.get_failed_jobs(
        wf_id, **args
    )

    for i in range(len(failed_jobs_list)):
        failed_jobs_list[i] = failed_jobs_list[i]._asdict()
        failed_jobs_list[i]["DT_RowClass"] = "failing"

    d = {
        "draw": args["sequence"] if args["sequence"] else 0,
        "recordsTotal": total_count
        if total_count is not None
        else len(failed_jobs_list),
        "data": failed_jobs_list,
    }
    if args["limit"]:
        d["recordsFiltered"] = filtered_count

    return serialize(d)


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/j/running/", methods=["GET"])
def running_jobs(username, root_wf_id, wf_id):
    """
    Get a list of all running jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, running_jobs_list = dashboard.get_running_jobs(
        wf_id, **args
    )

    for i in range(len(running_jobs_list)):
        running_jobs_list[i] = running_jobs_list[i]._asdict()
        running_jobs_list[i]["DT_RowClass"] = "running"

    d = {
        "draw": args["sequence"] if args["sequence"] else 0,
        "recordsTotal": total_count
        if total_count is not None
        else len(running_jobs_list),
        "data": running_jobs_list,
    }
    if args["limit"]:
        d["recordsFiltered"] = filtered_count

    return serialize(d)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/successful/", methods=["GET"]
)
def successful_jobs(username, root_wf_id, wf_id):
    """
    Get a list of all successful jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, successful_jobs_list = dashboard.get_successful_jobs(
        wf_id, **args
    )

    for i in range(len(successful_jobs_list)):
        successful_jobs_list[i] = successful_jobs_list[i]._asdict()
        successful_jobs_list[i]["DT_RowClass"] = "successful"
        successful_jobs_list[i]["root_wf_id"] = root_wf_id
        successful_jobs_list[i]["wf_id"] = wf_id
        successful_jobs_list[i]["duration_formatted"] = filters.time_to_str(
            successful_jobs_list[i]["duration"]
        )

    d = {
        "draw": args["sequence"] if args["sequence"] else 0,
        "recordsTotal": total_count
        if total_count is not None
        else len(successful_jobs_list),
        "data": successful_jobs_list,
    }
    if args["limit"]:
        d["recordsFiltered"] = filtered_count

    return serialize(d)


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/j/failing/", methods=["GET"])
def failing_jobs(username, root_wf_id, wf_id):
    """
    Get a list of failing jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, failing_jobs_list = dashboard.get_failing_jobs(
        wf_id, **args
    )

    for i in range(len(failing_jobs_list)):
        failing_jobs_list[i] = failing_jobs_list[i]._asdict()
        failing_jobs_list[i]["DT_RowClass"] = "failing"

    d = {
        "draw": args["sequence"] if args["sequence"] else 0,
        "recordsTotal": total_count
        if total_count is not None
        else len(failing_jobs_list),
        "data": failing_jobs_list,
    }
    if args["limit"]:
        d["recordsFiltered"] = filtered_count

    return serialize(d)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>",
    methods=["GET"],
)
def job(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get details of a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    job = dashboard.get_job_information(wf_id, job_id, job_instance_id)
    job_states = dashboard.get_job_states(wf_id, job_id, job_instance_id)
    job_instances = dashboard.get_job_instances(wf_id, job_id)

    if not job:
        return "Bad Request", 400

    return render_template(
        "workflow/job/job_details.html",
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        job_id=job_id,
        job=job,
        job_instances=job_instances,
        job_states=job_states,
    )


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/stdout",
    methods=["GET"],
)
def stdout(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get stdout contents for a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    text = dashboard.get_stdout(wf_id, job_id, job_instance_id)

    if text.stdout_text is None:
        return "No stdout for workflow " + wf_id + " job-id " + job_id
    else:
        return "<pre>%s</pre>" % utils.unquote(text.stdout_text)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/stderr",
    methods=["GET"],
)
def stderr(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get stderr contents for a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    text = dashboard.get_stderr(wf_id, job_id, job_instance_id)

    if text.stderr_text is None:
        return "No Standard error for workflow " + wf_id + " job-id " + job_id
    else:
        return "<pre>%s</pre>" % utils.unquote(text.stderr_text)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/successful",
    methods=["GET"],
)
def successful_invocations(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get list of successful invocations for a given job.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    successful_invocations_list = dashboard.get_successful_job_invocation(
        wf_id, job_id, job_instance_id
    )

    for i in range(len(successful_invocations_list)):
        successful_invocations_list[i] = successful_invocations_list[i]._asdict()
        successful_invocations_list[i]["remote_duration"] = filters.time_to_str(
            successful_invocations_list[i]["remote_duration"]
        )

    return serialize(successful_invocations_list)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/failed",
    methods=["GET"],
)
def failed_invocations(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get list of failed invocations for a given job.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    failed_invocations_list = dashboard.get_failed_job_invocation(
        wf_id, job_id, job_instance_id
    )

    for i in range(len(failed_invocations_list)):
        failed_invocations_list[i] = failed_invocations_list[i]._asdict()
        failed_invocations_list[i]["remote_duration"] = filters.time_to_str(
            failed_invocations_list[i]["remote_duration"]
        )

    return serialize(failed_invocations_list)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/",
    methods=["GET"],
)
@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/<invocation_id>",
    methods=["GET"],
)
def invocation(username, root_wf_id, wf_id, job_id, job_instance_id, invocation_id):
    """
    Get detailed invocation information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    invocation = dashboard.get_invocation_information(
        wf_id, job_id, job_instance_id, invocation_id
    )

    return render_template(
        "workflow/job/invocation/invocation_details.html",
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        job_id=job_id,
        job_instance_id=job_instance_id,
        invocation_id=invocation_id,
        invocation=invocation,
    )


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/charts", methods=["GET"])
def charts(username, root_wf_id, wf_id):
    """
    Get job-distribution information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    job_dist = dashboard.plots_transformation_statistics(wf_id)

    d = []
    for i in range(len(job_dist)):
        d.append(
            {
                "name": job_dist[i].transformation,
                "count": {
                    "total": job_dist[i].count,
                    "success": job_dist[i].success,
                    "failure": job_dist[i].failure,
                },
                "time": {
                    "total": job_dist[i].sum,
                    "min": job_dist[i].min,
                    "max": job_dist[i].max,
                    "avg": job_dist[i].avg,
                },
            }
        )

    return render_template(
        "workflow/charts.html", root_wf_id=root_wf_id, wf_id=wf_id, job_dist=d
    )


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/charts/time_chart", methods=["GET"]
)
def time_chart(username, root_wf_id, wf_id):
    """
    Get job-distribution information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    time_chart_job, time_chart_invocation = dashboard.plots_time_chart(wf_id)

    d = []
    for i in range(len(time_chart_job)):
        d.append(
            {
                "date_format": time_chart_job[i].date_format,
                "count": {
                    "job": time_chart_job[i].count,
                    "invocation": time_chart_invocation[i].count,
                },
                "total_runtime": {
                    "job": time_chart_job[i].total_runtime,
                    "invocation": time_chart_invocation[i].total_runtime,
                },
            }
        )

    return serialize(d)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/charts/gantt_chart", methods=["GET"]
)
def gantt_chart(username, root_wf_id, wf_id):
    """
    Get information required to generate a Gantt chart.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    gantt_chart = dashboard.plots_gantt_chart()

    d = []
    for i in range(len(gantt_chart)):
        d.append(
            {
                "job_id": gantt_chart[i].job_id,
                "job_instance_id": gantt_chart[i].job_instance_id,
                "job_submit_seq": gantt_chart[i].job_submit_seq,
                "job_name": gantt_chart[i].job_name,
                "transformation": gantt_chart[i].transformation,
                "jobS": gantt_chart[i].jobS,
                "jobDuration": gantt_chart[i].jobDuration,
                "pre_start": gantt_chart[i].pre_start,
                "pre_duration": gantt_chart[i].pre_duration,
                "condor_start": gantt_chart[i].condor_start,
                "condor_duration": gantt_chart[i].condor_duration,
                "grid_start": gantt_chart[i].grid_start,
                "grid_duration": gantt_chart[i].grid_duration,
                "exec_start": gantt_chart[i].exec_start,
                "exec_duration": gantt_chart[i].exec_duration,
                "kickstart_start": gantt_chart[i].kickstart_start,
                "kickstart_duration": gantt_chart[i].kickstart_duration,
                "post_start": gantt_chart[i].post_start,
                "post_duration": gantt_chart[i].post_duration,
            }
        )

    return serialize(d)


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics", methods=["GET"])
def statistics(username, root_wf_id, wf_id):
    """
    Get workflow statistics information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    summary_times = dashboard.workflow_summary_stats(wf_id)

    for key, value in summary_times.items():
        summary_times[key] = filters.time_to_str(value)

    workflow_stats = dashboard.workflow_stats()

    return render_template(
        "workflow/statistics.html",
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        summary_stats=summary_times,
        workflow_stats=workflow_stats,
    )


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/summary", methods=["GET"]
)
def workflow_summary_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    summary_times = dashboard.workflow_summary_stats(wf_id)

    for key, value in summary_times.items():
        summary_times[key] = filters.time_to_str(value)

    return serialize(summary_times)


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/workflow", methods=["GET"]
)
def workflow_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return serialize(dashboard.workflow_stats())


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/job_breakdown", methods=["GET"]
)
def job_breakdown_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return serialize(dashboard.job_breakdown_stats())


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/job", methods=["GET"]
)
def job_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return serialize(dashboard.job_stats())


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/integrity", methods=["GET"]
)
def integrity_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return serialize(dashboard.integrity_stats())


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/browser", methods=["GET"])
def file_browser(username, root_wf_id, wf_id):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        if os.path.isdir(submit_dir):
            init_file = request.args.get("init_file", None)
            return render_template(
                "file-browser.html",
                root_wf_id=root_wf_id,
                wf_id=wf_id,
                init_file=init_file,
            )
        else:
            raise ServiceError(
                ErrorResponse(
                    "SUBMIT_DIR_NOT_FOUND",
                    "%r is not a valid directory" % str(submit_dir),
                )
            )

    except NoResultFound:
        return render_template("error/workflow/workflow_details_missing.html")

    return "Error", 500


@blueprint.route("/u/<username>/r/<root_wf_id>/w/<wf_id>/files/", methods=["GET"])
@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/files/<path:path>", methods=["GET"]
)
def file_list(username, root_wf_id, wf_id, path=""):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        if os.path.isdir(submit_dir):
            dest = os.path.join(submit_dir, path)

            if os.path.isfile(dest):
                return "", 204

            folders = {"dirs": [], "files": []}

            for entry in os.listdir(dest):
                if os.path.isdir(os.path.join(dest, entry)):
                    folders["dirs"].append(os.path.normpath(os.path.join(path, entry)))
                else:
                    folders["files"].append(os.path.normpath(os.path.join(path, entry)))

            return serialize(folders), 200, {"Content-Type": "application/json"}

        else:
            raise ServiceError(
                ErrorResponse(
                    "SUBMIT_DIR_NOT_FOUND",
                    "%r is not a valid directory" % str(submit_dir),
                )
            )

    except NoResultFound:
        return render_template("error/workflow/workflow_details_missing.html")

    return "Error", 500


@blueprint.route(
    "/u/<username>/r/<root_wf_id>/w/<wf_id>/file/<path:path>", methods=["GET"]
)
def file_view(username, root_wf_id, wf_id, path):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        file_path = os.path.join(submit_dir, path)
        if not os.path.isfile(file_path):
            return "File not found", 404

        return send_from_directory(submit_dir, path)
    except NoResultFound:
        return render_template("error/workflow/workflow_details_missing.html")

    return "Error", 500


@blueprint.route("/u/<username>/info")
def info(username):
    return render_template("info.html")


def __get_datatables_args():
    """
    Extract list of arguments passed in the request
    """

    table_args = dict()

    #
    # Common Arguments
    #

    table_args["column-count"] = 0
    table_args["sort-col-count"] = 0

    if request.args.get("draw"):
        table_args["sequence"] = request.args.get("draw")

    if request.args.get("start"):
        table_args["offset"] = int(request.args.get("start"))

    if request.args.get("length"):
        table_args["limit"] = int(request.args.get("length"))

    if request.args.get("search[value]"):
        table_args["filter"] = request.args.get("search[value]")

    if request.args.get("search[regex]"):
        table_args["filter-regex"] = request.args.get("search[regex]")

    #
    # Custom Arguments
    #

    if request.args.get("time_filter"):
        table_args["time_filter"] = request.args.get("time_filter")

    i = 0
    while True:
        if request.args.get("columns[%d][data]" % i):
            table_args["column-count"] += 1
            table_args["mDataProp_%d" % i] = request.args.get("columns[%d][data]" % i)
        else:
            break

        #
        # Column Search
        #

        if request.args.get("columns[%d][searchable]" % i):
            table_args["bSearchable_%d" % i] = request.args.get(
                "columns[%d][searchable]" % i
            )

        if request.args.get("columns[%d][search][value]" % i):
            table_args["sSearch_%d" % i] = request.args.get(
                "columns[%d][search][value]" % i
            )

        if request.args.get("columns[%d][search][regex]" % i):
            table_args["bRegex_%d" % i] = request.args.get(
                "columns[%d][search][regex]" % i
            )

        #
        # Column Sort
        #

        if request.args.get("columns[%d][orderable]" % i):
            table_args["bSortable_%d" % i] = request.args.get(
                "columns[%d][orderable]" % i
            )

        if request.args.get("order[%d][column]" % i):
            table_args["sort-col-count"] += 1
            table_args["iSortCol_%d" % i] = int(
                request.args.get("order[%d][column]" % i)
            )

        if request.args.get("order[%d][dir]" % i):
            table_args["sSortDir_%d" % i] = request.args.get("order[%d][dir]" % i)

        i += 1

    return table_args


@blueprint.errorhandler(404)
def page_not_found(error):
    return render_template("error/404.html")


@blueprint.errorhandler(MasterDBNotFoundError)
def master_database_missing(error):
    log.exception(error)
    return render_template("error/master_database_missing.html")


@blueprint.errorhandler(StampedeDBNotFoundError)
def stampede_database_missing(error):
    log.exception(error)
    return render_template("error/stampede_database_missing.html")


@blueprint.errorhandler(DBAdminError)
def database_migration_error(error):
    log.exception(error)
    return render_template("error/database_migration_error.html", e=error)


@blueprint.errorhandler(ServiceError)
def error_response(error):
    log.exception(error)
    if request.is_xhr:
        return (
            serialize({"code": error.message.code, "message": error.message.message}),
            400,
            {"Content-Type": "application/json"},
        )
    else:
        return render_template("error/error_response.html", error=error.message)


@blueprint.errorhandler(Exception)
def catch_all(error):
    log.exception(error)
    return render_template("error/catch_all.html")
