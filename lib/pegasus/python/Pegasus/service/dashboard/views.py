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
import os
from datetime import datetime
from time import localtime, strftime

from flask import (
    g, json, redirect, render_template, request, send_from_directory, url_for
)
from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.errors import StampedeDBNotFoundError
from Pegasus.service import filters
from Pegasus.service.base import ErrorResponse, ServiceError
from Pegasus.service.dashboard import dashboard_routes
from Pegasus.service.dashboard.dashboard import (
    Dashboard, NoWorkflowsFoundError
)
from Pegasus.service.dashboard.queries import MasterDBNotFoundError
from Pegasus.tools import utils
from sqlalchemy.orm.exc import NoResultFound

log = logging.getLogger(__name__)


@dashboard_routes.route('/')
def redirect_to_index():
    return redirect(url_for('.index'))


@dashboard_routes.route('/u/<username>/')
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

            __update_label_link(workflows)
            __update_timestamp(workflows)

            for workflow in workflows:
                workflow.state = (
                    workflow.state + ' (%s)' % workflow.reason
                ) if workflow.status > 0 and workflow.reason else workflow.state
        else:
            totals = dashboard.get_root_workflow_list(counts_only=True, **args)

    except NoWorkflowsFoundError as e:
        if request.is_xhr:
            return render_template(
                'workflow.xhr.json',
                count=e.count,
                filtered=e.filtered,
                workflows=[],
                table_args=args
            )

        return render_template('workflow.html', counts=(0, 0, 0, 0))

    if request.is_xhr:
        return render_template(
            'workflow.xhr.json',
            count=count,
            filtered=filtered,
            workflows=workflows,
            table_args=args
        )

    return render_template('workflow.html', counts=totals)


@dashboard_routes.route('/u/<username>/r/<root_wf_id>/w')
@dashboard_routes.route('/u/<username>/r/<root_wf_id>/w/<wf_id>')
def workflow(username, root_wf_id, wf_id=None):
    """
    Get details for a specific workflow.
    """
    wf_uuid = request.args.get('wf_uuid', None)

    if not wf_id and not wf_uuid:
        raise ValueError('Workflow ID or Workflow UUID is required')

    if wf_id:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
    else:
        dashboard = Dashboard(g.master_db_url, root_wf_id)

    try:
        counts, details, statistics = dashboard.get_workflow_information(
            wf_id, wf_uuid
        )
    except NoResultFound:
        return render_template('error/workflow/workflow_details_missing.html')

    return render_template(
        'workflow/workflow_details.html',
        root_wf_id=root_wf_id,
        wf_id=details.wf_id,
        workflow=details,
        counts=counts,
        statistics=statistics
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/sw/', methods=['GET']
)
def sub_workflows(username, root_wf_id, wf_id):
    """
    Get a list of all sub-workflow of a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    sub_workflows = dashboard.get_sub_workflows(wf_id)

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(sub_workflows) > 0:
            return render_template(
                'workflow/sub_workflows.xhr.html',
                root_wf_id=root_wf_id,
                wf_id=wf_id,
                workflows=sub_workflows
            )
        else:
            return '', 204
    else:
        return render_template(
            'workflow/sub_workflows.html',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            workflows=sub_workflows
        )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/failed/', methods=['GET']
)
def failed_jobs(username, root_wf_id, wf_id):
    """
    Get a list of all failed jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, failed_jobs_list = dashboard.get_failed_jobs(
        wf_id, **args
    )

    for job in failed_jobs_list:
        job.exec_job_id = '<a href="' + url_for(
            '.job',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">' + job.exec_job_id + '</a>'
        job.stdout = '<a target="_blank" href="' + url_for(
            '.stdout',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">Application Stdout/Stderr</a>'
        job.stderr = '<a target="_blank" href="' + url_for(
            '.stderr',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">Condor Stderr/Pegasus Lite Log</a>'

    return render_template(
        'workflow/jobs_failed.xhr.json',
        count=total_count,
        filtered=filtered_count,
        jobs=failed_jobs_list,
        table_args=args
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/running/', methods=['GET']
)
def running_jobs(username, root_wf_id, wf_id):
    """
    Get a list of all running jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, running_jobs_list = dashboard.get_running_jobs(
        wf_id, **args
    )

    for job in running_jobs_list:
        job.exec_job_id = '<a href="' + url_for(
            '.job',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">' + job.exec_job_id + '</a>'

    return render_template(
        'workflow/jobs_running.xhr.json',
        count=total_count,
        filtered=filtered_count,
        jobs=running_jobs_list,
        table_args=args
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/successful/', methods=['GET']
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

    for job in successful_jobs_list:
        job.duration_formatted = filters.time_to_str(job.duration)
        job.exec_job_id = '<a href="' + url_for(
            '.job',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">' + job.exec_job_id + '</a>'

    return render_template(
        'workflow/jobs_successful.xhr.json',
        count=total_count,
        filtered=filtered_count,
        jobs=successful_jobs_list,
        table_args=args
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/failing/', methods=['GET']
)
def failing_jobs(username, root_wf_id, wf_id):
    """
    Get a list of failing jobs of the latest instance for a given workflow.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, failing_jobs_list = dashboard.get_failing_jobs(
        wf_id, **args
    )

    for job in failing_jobs_list:
        job.exec_job_id = '<a href="' + url_for(
            '.job',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">' + job.exec_job_id + '</a>'
        job.stdout = '<a target="_blank" href="' + url_for(
            '.stdout',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">Application Stdout/Stderr</a>'
        job.stderr = '<a target="_blank" href="' + url_for(
            '.stderr',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job.job_id,
            job_instance_id=job.job_instance_id
        ) + '">Condor Stderr/Pegasus Lite Log</a>'

    return render_template(
        'workflow/jobs_failing.xhr.json',
        count=total_count,
        filtered=filtered_count,
        jobs=failing_jobs_list,
        table_args=args
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>',
    methods=['GET']
)
def job(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get details of a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    job = dashboard.get_job_information(wf_id, job_id, job_instance_id)
    job_states = dashboard.get_job_states(wf_id, job_id, job_instance_id)
    job_instances = dashboard.get_job_instances(wf_id, job_id)

    previous = None

    for state in job_states:
        timestamp = state.timestamp
        state.timestamp = datetime.fromtimestamp(
            state.timestamp
        ).strftime('%a %b %d, %Y %I:%M:%S %p')

        if previous is None:
            state.interval = 0.0
        else:
            state.interval = timestamp - previous

        previous = timestamp

    if not job:
        return 'Bad Request', 400

    return render_template(
        'workflow/job/job_details.html',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        job_id=job_id,
        job=job,
        job_instances=job_instances,
        job_states=job_states
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/stdout',
    methods=['GET']
)
def stdout(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get stdout contents for a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    text = dashboard.get_stdout(wf_id, job_id, job_instance_id)

    if text.stdout_text == None:
        return 'No stdout for workflow ' + wf_id + ' job-id ' + job_id
    else:
        return '<pre>%s</pre>' % utils.unquote(text.stdout_text)


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/stderr',
    methods=['GET']
)
def stderr(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get stderr contents for a specific job instance.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    text = dashboard.get_stderr(wf_id, job_id, job_instance_id)

    if text.stderr_text == None:
        return 'No Standard error for workflow ' + wf_id + ' job-id ' + job_id
    else:
        return '<pre>%s</pre>' % utils.unquote(text.stderr_text)


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/successful',
    methods=['GET']
)
def successful_invocations(
    username, root_wf_id, wf_id, job_id, job_instance_id
):
    """
    Get list of successful invocations for a given job.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    successful_invocations_list = dashboard.get_successful_job_invocation(
        wf_id, job_id, job_instance_id
    )

    for item in successful_invocations_list:
        item.remote_duration_formatted = filters.time_to_str(
            item.remote_duration
        )

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(successful_invocations_list) > 0:
            return render_template(
                'workflow/job/invocations_successful.xhr.html',
                root_wf_id=root_wf_id,
                wf_id=wf_id,
                job_id=job_id,
                job_instance_id=job_instance_id,
                invocations=successful_invocations_list
            )
        else:
            return '', 204
    else:
        return render_template(
            'workflow/job/invocations_successful.html',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job_id,
            job_instance_id=job_instance_id,
            invocations=successful_invocations_list
        )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/failed',
    methods=['GET']
)
def failed_invocations(username, root_wf_id, wf_id, job_id, job_instance_id):
    """
    Get list of failed invocations for a given job.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    failed_invocations_list = dashboard.get_failed_job_invocation(
        wf_id, job_id, job_instance_id
    )

    for item in failed_invocations_list:
        item.remote_duration_formatted = filters.time_to_str(
            item.remote_duration
        )

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(failed_invocations_list) > 0:
            return render_template(
                'workflow/job/invocations_failed.xhr.html',
                root_wf_id=root_wf_id,
                wf_id=wf_id,
                job_id=job_id,
                job_instance_id=job_instance_id,
                invocations=failed_invocations_list
            )
        else:
            return '', 204
    else:
        return render_template(
            'workflow/job/invocations_failed.html',
            root_wf_id=root_wf_id,
            wf_id=wf_id,
            job_id=job_id,
            job_instance_id=job_instance_id,
            invocations=failed_invocations_list
        )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/',
    methods=['GET']
)
@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/j/<job_id>/ji/<job_instance_id>/i/<invocation_id>',
    methods=['GET']
)
def invocation(
    username, root_wf_id, wf_id, job_id, job_instance_id, invocation_id
):
    """
    Get detailed invocation information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    invocation = dashboard.get_invocation_information(
        wf_id, job_id, job_instance_id, invocation_id
    )

    return render_template(
        'workflow/job/invocation/invocation_details.html',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        job_id=job_id,
        job_instance_id=job_instance_id,
        invocation_id=invocation_id,
        invocation=invocation
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/charts', methods=['GET']
)
def charts(username, root_wf_id, wf_id):
    """
    Get job-distribution information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    job_dist = dashboard.plots_transformation_statistics(wf_id)

    return render_template(
        'workflow/charts.html',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        job_dist=job_dist
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/charts/time_chart',
    methods=['GET']
)
def time_chart(username, root_wf_id, wf_id):
    """
    Get job-distribution information
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    time_chart_job, time_chart_invocation = dashboard.plots_time_chart(wf_id)

    return render_template(
        'workflow/charts/time_chart.json',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        time_chart_job=time_chart_job,
        time_chart_invocation=time_chart_invocation
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/charts/gantt_chart',
    methods=['GET']
)
def gantt_chart(username, root_wf_id, wf_id):
    """
    Get information required to generate a Gantt chart.
    """
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    gantt_chart = dashboard.plots_gantt_chart()
    return render_template(
        'workflow/charts/gantt_chart.json',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        gantt_chart=gantt_chart
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics', methods=['GET']
)
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
        'workflow/statistics.html',
        root_wf_id=root_wf_id,
        wf_id=wf_id,
        summary_stats=summary_times,
        workflow_stats=workflow_stats
    )


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/summary',
    methods=['GET']
)
def workflow_summary_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    summary_times = dashboard.workflow_summary_stats(wf_id)

    for key, value in summary_times.items():
        summary_times[key] = filters.time_to_str(value)

    return json.dumps(summary_times)


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/workflow',
    methods=['GET']
)
def workflow_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return json.dumps(dashboard.workflow_stats())


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/job_breakdown',
    methods=['GET']
)
def job_breakdown_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return json.dumps(dashboard.job_breakdown_stats())


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/job', methods=['GET']
)
def job_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return json.dumps(dashboard.job_stats())


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/integrity', methods=['GET']
)
def integrity_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)
    return json.dumps(dashboard.integrity_stats())


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/statistics/time', methods=['GET']
)
def time_stats(username, root_wf_id, wf_id):
    dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id)

    return '{}'


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/browser', methods=['GET']
)
def file_browser(username, root_wf_id, wf_id):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        if os.path.isdir(submit_dir):
            init_file = request.args.get('init_file', None)
            return render_template(
                'file-browser.html',
                root_wf_id=root_wf_id,
                wf_id=wf_id,
                init_file=init_file
            )
        else:
            raise ServiceError(
                ErrorResponse(
                    'SUBMIT_DIR_NOT_FOUND',
                    '%r is not a valid directory' % str(submit_dir)
                )
            )

    except NoResultFound:
        return render_template('error/workflow/workflow_details_missing.html')

    return 'Error', 500


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/files/', methods=['GET']
)
@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/files/<path:path>',
    methods=['GET']
)
def file_list(username, root_wf_id, wf_id, path=''):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        if os.path.isdir(submit_dir):
            dest = os.path.join(submit_dir, path)

            if os.path.isfile(dest):
                return '', 204

            folders = {'dirs': [], 'files': []}

            for entry in os.listdir(dest):
                if os.path.isdir(os.path.join(dest, entry)):
                    folders['dirs'].append(
                        os.path.normpath(os.path.join(path, entry))
                    )
                else:
                    folders['files'].append(
                        os.path.normpath(os.path.join(path, entry))
                    )

            return json.dumps(folders), 200, {
                'Content-Type': 'application/json'
            }

        else:
            raise ServiceError(
                ErrorResponse(
                    'SUBMIT_DIR_NOT_FOUND',
                    '%r is not a valid directory' % str(submit_dir)
                )
            )

    except NoResultFound:
        return render_template('error/workflow/workflow_details_missing.html')

    return 'Error', 500


@dashboard_routes.route(
    '/u/<username>/r/<root_wf_id>/w/<wf_id>/file/<path:path>', methods=['GET']
)
def file_view(username, root_wf_id, wf_id, path):
    try:
        dashboard = Dashboard(g.master_db_url, root_wf_id, wf_id=wf_id)
        details = dashboard.get_workflow_details(wf_id)
        submit_dir = details.submit_dir

        file_path = os.path.join(submit_dir, path)
        if not os.path.isfile(file_path):
            return 'File not found', 404

        return send_from_directory(submit_dir, path)
    except NoResultFound:
        return render_template('error/workflow/workflow_details_missing.html')

    return 'Error', 500


@dashboard_routes.route('/u/<username>/info')
def info(username):
    return render_template('info.html')


def __update_timestamp(workflows):
    for workflow in workflows:
        workflow.timestamp = strftime(
            '%a, %d %b %Y %H:%M:%S', localtime(workflow.timestamp)
        )


def __update_label_link(workflows):
    for workflow in workflows:
        workflow.dax_label = '<a href="' + url_for(
            '.workflow', root_wf_id=workflow.wf_id, wf_uuid=workflow.wf_uuid
        ) + '">' + workflow.dax_label + '</a>'


def __get_datatables_args():
    """
    Extract list of arguments passed in the request
    """

    table_args = dict()

    #
    # Common Arguments
    #

    table_args['column-count'] = 0
    table_args['sort-col-count'] = 0

    if request.args.get('draw'):
        table_args['sequence'] = request.args.get('draw')

    if request.args.get('start'):
        table_args['offset'] = int(request.args.get('start'))

    if request.args.get('length'):
        table_args['limit'] = int(request.args.get('length'))

    if request.args.get('search[value]'):
        table_args['filter'] = request.args.get('search[value]')

    if request.args.get('search[regex]'):
        table_args['filter-regex'] = request.args.get('search[regex]')

    #
    # Custom Arguments
    #

    if request.args.get('time_filter'):
        table_args['time_filter'] = request.args.get('time_filter')

    i = 0
    while True:
        if request.args.get('columns[%d][data]' % i):
            table_args['column-count'] += 1
            table_args['mDataProp_%d' % i] = request.args.get(
                'columns[%d][data]' % i
            )
        else:
            break

        #
        # Column Search
        #

        if request.args.get('columns[%d][searchable]' % i):
            table_args['bSearchable_%d' % i] = request.args.get(
                'columns[%d][searchable]' % i
            )

        if request.args.get('columns[%d][search][value]' % i):
            table_args['sSearch_%d' % i] = request.args.get(
                'columns[%d][search][value]' % i
            )

        if request.args.get('columns[%d][search][regex]' % i):
            table_args['bRegex_%d' % i] = request.args.get(
                'columns[%d][search][regex]' % i
            )

        #
        # Column Sort
        #

        if request.args.get('columns[%d][orderable]' % i):
            table_args['bSortable_%d' % i] = request.args.get(
                'columns[%d][orderable]' % i
            )

        if request.args.get('order[%d][column]' % i):
            table_args['sort-col-count'] += 1
            table_args['iSortCol_%d' % i] = int(
                request.args.get('order[%d][column]' % i)
            )

        if request.args.get('order[%d][dir]' % i):
            table_args['sSortDir_%d' % i] = request.args.get(
                'order[%d][dir]' % i
            )

        i += 1

    return table_args


@dashboard_routes.errorhandler(404)
def page_not_found(error):
    return render_template('error/404.html')


@dashboard_routes.errorhandler(MasterDBNotFoundError)
def master_database_missing(error):
    log.exception(error)
    return render_template('error/master_database_missing.html')


@dashboard_routes.errorhandler(StampedeDBNotFoundError)
def stampede_database_missing(error):
    log.exception(error)
    return render_template('error/stampede_database_missing.html')


@dashboard_routes.errorhandler(DBAdminError)
def database_migration_error(error):
    log.exception(error)
    return render_template('error/database_migration_error.html', e=error)


@dashboard_routes.errorhandler(ServiceError)
def error_response(error):
    log.exception(error)
    if request.is_xhr:
        return json.dumps(
            {
                'code': error.message.code,
                'message': error.message.message
            }
        ), 400, {
            'Content-Type': 'application/json'
        }
    else:
        return render_template(
            'error/error_response.html', error=error.message
        )


@dashboard_routes.errorhandler(Exception)
def catch_all(error):
    log.exception(error)
    return render_template('error/catch_all.html')
