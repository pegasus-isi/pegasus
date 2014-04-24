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

import re

from time import localtime, strftime

from flask import request, render_template, url_for, json
from sqlalchemy.orm.exc import NoResultFound

from Pegasus.netlogger.analysis.error.Error import StampedeDBNotFoundError

from pegasus.service import app, filters
from pegasus.service.dashboard.dashboard import Dashboard, NoWorkflowsFoundError
from pegasus.service.dashboard.queries import MasterDBNotFoundError

# XXX This does not appear to be used!
class WorkflowsDetailsMissingError(Exception):
    pass

@app.route('/')
def index():
    '''
    List all workflows from the master database.
    '''
    try:
        dashboard = Dashboard()
        args = __get_datatables_args()
        count, filtered, workflows, totals = dashboard.get_root_workflow_list(**args)
        __update_label_link(workflows)
        __update_timestamp(workflows)
    except NoWorkflowsFoundError, e:
        if request.is_xhr:
            return render_template('workflow.xhr.json', count=e.count, filtered=e.filtered, workflows=[], table_args=args)

        return render_template('workflow.html', workflows=[], counts=(0, 0, 0, 0))

    if request.is_xhr:
        return render_template('workflow.xhr.json', count=count, filtered=filtered, workflows=workflows, table_args=args)

    return render_template('workflow.html', workflows=workflows, counts=totals)

@app.route('/root/<root_wf_id>/workflow')
@app.route('/root/<root_wf_id>/workflow/<wf_id>')
def workflow(root_wf_id, wf_id=None):
    '''
    Get details for a specific workflow.
    '''
    wf_uuid = request.args.get('wf_uuid', None)

    if not wf_id and not wf_uuid:
        raise ValueError, 'Workflow ID or Workflow UUID is required'

    if wf_id:
        dashboard = Dashboard(root_wf_id, wf_id=wf_id)
    else:
        dashboard = Dashboard(root_wf_id)

    try:
        counts, details, statistics = dashboard.get_workflow_information(wf_id, wf_uuid)
    except NoResultFound:
        return render_template('error/workflow/workflow_details_missing.html')

    return render_template('workflow/workflow_details.html', root_wf_id=root_wf_id, wf_id=details.wf_id, workflow=details, counts=counts, statistics=statistics);

@app.route('/root/<root_wf_id>/workflow/<wf_id>/sub_workflows/', methods=['GET'])
def sub_workflows(root_wf_id, wf_id):
    '''
    Get a list of all sub-workflow of a given workflow.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    sub_workflows = dashboard.get_sub_workflows(wf_id)

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(sub_workflows) > 0:
            return render_template('workflow/sub_workflows.xhr.html', root_wf_id=root_wf_id, wf_id=wf_id, workflows=sub_workflows)
        else:
            return '', 204
    else:
        return render_template('workflow/sub_workflows.html', root_wf_id=root_wf_id, wf_id=wf_id, workflows=sub_workflows)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/jobs/failed/', methods=['GET'])
def failed_jobs(root_wf_id, wf_id):
    '''
    Get a list of all failed jobs of the latest instance for a given workflow.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, failed_jobs_list = dashboard.get_failed_jobs(wf_id, **args)

    for job in failed_jobs_list:
        job.exec_job_id = '<a href="' + url_for('job', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job.job_id) + '">' + job.exec_job_id + '</a>'
        job.stdout = '<a href="' + url_for('stdout', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job.job_id) + '">stdout</a>'
        job.stderr = '<a href="' + url_for('stderr', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job.job_id) + '">stderr</a>'

    return render_template('workflow/jobs_failed.xhr.json', count=total_count, filtered=filtered_count, jobs=failed_jobs_list, table_args=args)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/jobs/running/', methods=['GET'])
def running_jobs(root_wf_id, wf_id):
    '''
    Get a list of all running jobs of the latest instance for a given workflow.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, running_jobs_list = dashboard.get_running_jobs(wf_id, **args)

    for job in running_jobs_list:
        job.exec_job_id = '<a href="' + url_for('job', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job.job_id) + '">' + job.exec_job_id + '</a>'

    return render_template('workflow/jobs_running.xhr.json', count=total_count, filtered=filtered_count, jobs=running_jobs_list, table_args=args)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/jobs/successful/', methods=['GET'])
def successful_jobs(root_wf_id, wf_id):
    '''
    Get a list of all successful jobs of the latest instance for a given workflow.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    args = __get_datatables_args()

    total_count, filtered_count, successful_jobs_list = dashboard.get_successful_jobs(wf_id, **args)

    for job in successful_jobs_list:
        job.duration_formatted = filters.time_to_str(job.duration)
        job.exec_job_id = '<a href="' + url_for('job', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job.job_id) + '">' + job.exec_job_id + '</a>'

    return render_template('workflow/jobs_successful.xhr.json', count=total_count, filtered=filtered_count, jobs=successful_jobs_list, table_args=args)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>', methods=['GET'])
def job(root_wf_id, wf_id, job_id):
    '''
    Get details of a specific job instance.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    job = dashboard.get_job_information(wf_id, job_id)

    if not job:
        return 'Bad Request', 400

    return render_template('workflow/job/job_details.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, job=job)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/stdout', methods=['GET'])
def stdout(root_wf_id, wf_id, job_id):
    '''
    Get stdout contents for a specific job instance.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    text = dashboard.get_stdout(wf_id, job_id)

    if text.stdout_text == None:
        return 'No stdout for workflow ' + wf_id + ' job-id ' + job_id
    else:
        pattern = re.compile('%0a', re.IGNORECASE)
        return '<pre>%s</pre>' % pattern.sub('\n', text.stdout_text)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/stderr', methods=['GET'])
def stderr(root_wf_id, wf_id, job_id):
    '''
    Get stderr contents for a specific job instance.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    text = dashboard.get_stderr(wf_id, job_id)

    if text.stderr_text == None:
        return 'No Standard error for workflow ' + wf_id + ' job-id ' + job_id;
    else:
        pattern = re.compile('%0a', re.IGNORECASE)
        return '<pre>%s</pre>' % pattern.sub('\n', text.stderr_text)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/invocations/successful', methods=['GET'])
def successful_invocations(root_wf_id, wf_id, job_id):
    '''
    Get list of successful invocations for a given job.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    successful_invocations_list = dashboard.get_successful_job_invocation(wf_id, job_id)

    for item in successful_invocations_list:
        item.remote_duration_formatted = filters.time_to_str(item.remote_duration)

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(successful_invocations_list) > 0:
            return render_template('workflow/job/invocations_successful.xhr.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, invocations=successful_invocations_list)
        else:
            return '', 204
    else:
        return render_template('workflow/job/invocations_successful.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, invocations=successful_invocations_list)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/invocations/failed', methods=['GET'])
def failed_invocations(root_wf_id, wf_id, job_id):
    '''
    Get list of failed invocations for a given job.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    failed_invocations_list = dashboard.get_failed_job_invocation(wf_id, job_id)

    # is_xhr = True if it is AJAX request.
    if request.is_xhr:
        if len(failed_invocations_list) > 0:
            return render_template('workflow/job/invocations_failed.xhr.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, invocations=failed_invocations_list)
        else:
            return '', 204
    else:
        return render_template('workflow/job/invocations_failed.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, invocations=failed_invocations_list)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/invocation/', methods=['GET'])
@app.route('/root/<root_wf_id>/workflow/<wf_id>/job/<job_id>/invocation/<task_id>', methods=['GET'])
def invocation(root_wf_id, wf_id, job_id, task_id=None):
    '''
    Get detailed invocation information
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    invocation = dashboard.get_invocation_information(wf_id, job_id, task_id)

    return render_template('workflow/job/invocation/invocation_details.html', root_wf_id=root_wf_id, wf_id=wf_id, job_id=job_id, task_id=task_id, invocation=invocation)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/charts', methods=['GET'])
def charts(root_wf_id, wf_id):
    '''
    Get job-distribution information
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    job_dist = dashboard.plots_transformation_statistics(wf_id)

    return render_template('workflow/charts.html', root_wf_id=root_wf_id, wf_id=wf_id, job_dist=job_dist)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/charts/time_chart', methods=['GET'])
def time_chart(root_wf_id, wf_id):
    '''
    Get job-distribution information
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    time_chart_job, time_chart_invocation = dashboard.plots_time_chart(wf_id)

    return render_template('workflow/charts/time_chart.json', root_wf_id=root_wf_id, wf_id=wf_id, time_chart_job=time_chart_job, time_chart_invocation=time_chart_invocation)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/charts/gantt_chart', methods=['GET'])
def gantt_chart(root_wf_id, wf_id):
    '''
    Get information required to generate a Gantt chart.
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    gantt_chart = dashboard.plots_gantt_chart()
    return render_template('workflow/charts/gantt_chart.json', root_wf_id=root_wf_id, wf_id=wf_id, gantt_chart=gantt_chart)

@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics', methods=['GET'])
def statistics(root_wf_id, wf_id):
    '''
    Get workflow statistics information
    '''
    dashboard = Dashboard(root_wf_id, wf_id)
    summary_times = dashboard.workflow_summary_stats(wf_id)

    for key, value in summary_times.items():
        summary_times[key] = filters.time_to_str(value)

    workflow_stats = dashboard.workflow_stats()

    return render_template('workflow/statistics.html', root_wf_id=root_wf_id, wf_id=wf_id, summary_stats=summary_times, workflow_stats=workflow_stats)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics/summary', methods=['GET'])
def workflow_summary_stats(root_wf_id, wf_id):
    dashboard = Dashboard(root_wf_id, wf_id)
    summary_times = dashboard.workflow_summary_stats(wf_id)

    for key, value in summary_times.items():
        summary_times[key] = filters.time_to_str(value)

    return json.dumps(summary_times)


@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics/workflow', methods=['GET'])
def workflow_stats(root_wf_id, wf_id):
    dashboard = Dashboard(root_wf_id, wf_id)
    return json.dumps(dashboard.workflow_stats())


@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics/job_breakdown', methods=['GET'])
def job_breakdown_stats(root_wf_id, wf_id):
    dashboard = Dashboard(root_wf_id, wf_id)
    return json.dumps(dashboard.job_breakdown_stats())


@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics/job', methods=['GET'])
def job_stats(root_wf_id, wf_id):
    dashboard = Dashboard(root_wf_id, wf_id)
    return json.dumps(dashboard.job_stats())


@app.route('/root/<root_wf_id>/workflow/<wf_id>/statistics/time', methods=['GET'])
def time_stats(root_wf_id, wf_id):
    dashboard = Dashboard(root_wf_id, wf_id)

    return '{}'

def __update_timestamp(workflows):
    for workflow in workflows:
        workflow.timestamp = strftime('%a, %d %b %Y %H:%M:%S', localtime(workflow.timestamp))

def __update_label_link(workflows):
    for workflow in workflows:
        workflow.dax_label = '<a href="' + url_for('workflow', root_wf_id=workflow.wf_id, wf_uuid=workflow.wf_uuid) + '">' + workflow.dax_label + '</a>'

def __get_datatables_args():
    '''
    Extract list of arguments passed in the request
    '''
    table_args = dict()

    if request.args.get('sEcho'):
        table_args['sequence'] = request.args.get('sEcho')

    if request.args.get('iColumns'):
        table_args['column-count'] = int(request.args.get('iColumns'))

    if request.args.get('sColumns'):
        table_args['columns'] = request.args.get('sColumns')

    if request.args.get('iDisplayStart'):
        table_args['offset'] = int(request.args.get('iDisplayStart'))

    if request.args.get('iDisplayLength'):
        table_args['limit'] = int(request.args.get('iDisplayLength'))

    if request.args.get('sSearch'):
        table_args['filter'] = request.args.get('sSearch')

    if request.args.get('bRegex'):
        table_args['filter-regex'] = request.args.get('bRegex')

    if request.args.get('iSortingCols'):
        table_args['sort-col-count'] = int(request.args.get('iSortingCols'))

    if request.args.get('time_filter'):
        table_args['time_filter'] = request.args.get('time_filter')

    if request.args.get('iColumns'):
        for i in range(int(request.args.get('iColumns'))):
            i = str(i)

            if request.args.get('mDataProp_' + i):
                table_args['mDataProp_' + i] = request.args.get('mDataProp_' + i)

            if request.args.get('sSearch_' + i):
                table_args['sSearch_' + i] = request.args.get('sSearch_' + i)

            if request.args.get('bRegex_' + i):
                table_args['bRegex_' + i] = request.args.get('bRegex_' + i)

            if request.args.get('bSearchable_' + i):
                table_args['bSearchable_' + i] = request.args.get('bSearchable_' + i)

            if request.args.get('iSortCol_' + i):
                table_args['iSortCol_' + i] = int(request.args.get('iSortCol_' + i))

            if request.args.get('bSortable_' + i):
                table_args['bSortable_' + i] = request.args.get('bSortable_' + i)

            if request.args.get('sSortDir_' + i):
                table_args['sSortDir_' + i] = request.args.get('sSortDir_' + i)

    return table_args

@app.errorhandler(404)
def page_not_found(error):
    return render_template('error/404.html')

# XXX This does not appear to be used!
@app.errorhandler(WorkflowsDetailsMissingError)
def workflow_details_missing(error):
    return render_template('error/workflow/workflow_details_missing.html');

@app.errorhandler(MasterDBNotFoundError)
def master_database_missing(error):
    return render_template('error/master_database_missing.html')

@app.errorhandler(StampedeDBNotFoundError)
def stampede_database_missing(error):
    return render_template('error/stampede_database_missing.html')

