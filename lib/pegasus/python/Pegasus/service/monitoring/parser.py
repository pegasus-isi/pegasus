#!/usr/bin/env python

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

from Pegasus.service.base import BaseQueryParser, InvalidQueryError


class RootWorkflowQueryParser(BaseQueryParser):
    FIELDS = set([
        'r.wf_id',
        'r.wf_uuid',
        'r.submit_hostname',
        'r.submit_dir',
        'r.planner_arguments',
        'r.planner_version',
        'r.user',
        'r.grid_dn',
        'r.dax_label',
        'r.dax_version',
        'r.dax_file',
        'r.dag_file_name',
        'r.timestamp'
    ])

    def __init__(self, expression):
        super(RootWorkflowQueryParser, self).__init__(expression)

    def identifier_handler(self, text):
        if text not in RootWorkflowQueryParser.FIELDS:
            raise InvalidQueryError('Invalid field %r' % text)

        super(RootWorkflowQueryParser, self).identifier_handler(text)

    #
    # Override Method Handler for Identifiers
    #
    BaseQueryParser.mapper[BaseQueryParser.IDENTIFIER] = identifier_handler

class StampedeWorkflowQueryParser(BaseQueryParser):
    FIELDS = set([
        'w.root_wf_id',
        'w.wf_id',
        'w.wf_uuid',
        'w.submit_hostname',
        'w.submit_dir',
        'w.planner_arguments',
        'w.planner_version',
        'w.user',
        'w.grid_dn',
        'w.dax_label',
        'w.dax_version',
        'w.dax_file',
        'w.dag_file_name',
        'w.timestamp',
        'ws.wf_id',
        'ws.state',
        'ws.timestamp',
        'ws.restart_count',
        'ws.status',
        'j.job_id',
        'j.wf_id',
        'j.exec_job_id',
        'j.submit_file',
        'j.type_desc',
        'j.clustered',
        'j.max_retries',
        'j.executable',
        'j.argv',
        'j.task_count',
        'h.host_id',
        'h.wf_id',
        'h.site',
        'h.hostname',
        'h.ip',
        'h.uname',
        'h.total_memory',
        'js.job_instance_id',
        'js.state',
        'js.timestamp',
        'js.jobstate_submit_seq',
        't.task_id',
        't.job_id',
        't.wf_id',
        't.abs_task_id',
        'transformation',
        't.argv',
        't.type_desc',
        'ji.job_instance_id',
        'ji.job_id',
        'ji.host_id',
        'ji.job_submit_seq',
        'ji.sched_id',
        'ji.site',
        'ji.user',
        'ji.work_dir',
        'ji.cluster_start',
        'ji.cluster_duration',
        'ji.local_duration',
        'ji.subwf_id',
        'ji.stdout_file',
        'ji.stdout_text',
        'ji.stderr_file',
        'ji.stderr_text',
        'ji.stderr_file',
        'ji.stderr_text',
        'i.invocation_id',
        'i.job_instance_id',
        'i.task_submit_seq',
        'i.start_time',
        'i.remote_duration',
        'i.remote_cpu_time',
        'i.exitcode',
        'i.raw_status',
        'i.transformation',
        'i.executable',
        'i.argv',
        'i.abs_task_id',
        'i.wf_id'
    ])

    def __init__(self, expression):
        super(StampedeWorkflowQueryParser, self).__init__(expression)

    def indentifier_handler(self, text):
        if text not in StampedeWorkflowQueryParser.FIELDS:
            raise InvalidQueryError('Invalid field %r' % text)


        super(RootWorkflowQueryParser, self).identifier_handler(text)

    #
    # Override Method Handler for Identifiers
    #
    BaseQueryParser.mapper[BaseQueryParser.IDENTIFIER] = identifier_handler

def main():
    constraint = RootWorkflowQueryParser("""
        id=1 OR lfn='a\\'b \\' d \\''
        AND ( pfn='b' or pfn ='cc' )
        AND site != NULL
        AND ( attr_name='x' AND attr_value likE 'y')
    """)

    print constraint


if __name__ == '__main__':
    main()
