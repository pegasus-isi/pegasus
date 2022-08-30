import logging
import time
import json
import yaml
import re
from pathlib import Path
from os import path
from typing import Dict, List, Union

from Pegasus import braindump
from Pegasus.client import condor

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))

class Status:
    def __init__(self):
        self._log = logging.getLogger(__name__)
        self._log.addHandler(console_handler)
        self._log.propagate = False

        """Keys are defined as follows
            * `unready`: Jobs blocked by dependencies
            * `ready`: Jobs ready for submission
            * `pre`: PRE-Scripts running
            * `queued`: Submitted jobs
            * `post`: POST-Scripts running
            * `succeeded`: Job completed with success
            * `failed`: Jobs completed with failure
            * `percent_done`: Success percentage
            * `state`: Workflow state
            * `dagname`: Name of workflow
        """
        self.K_UNREADY = "unready"
        self.K_READY = "ready"
        self.K_PRE = "pre"
        self.K_QUEUED = "queued"
        self.K_POST = "post"
        self.K_SUCCEEDED = "succeeded"
        self.K_FAILED = "failed"
        self.K_PERCENT_DONE = "percent_done"
        self.K_DAGNAME = "dagname"
        self.K_DAGSTATE = "state"
        self.K_TOTAL = "total"
        self.K_TOTALS = "totals"

        self.DAG_OK = "Running"
        self.DAG_DONE = "Success"
        self.DAG_FAIL = "Failure"

        self.JS_RUN = "Run"
        self.JS_IDLE = "Idle"
        self.JS_HELD = "Held"
        self.JS_REMOVING = "Del"

        self.root_wf_uuid = None
        self.pegasus_wf_name = None

        self._job_status_codes = {1:self.JS_IDLE,
                                  2:self.JS_RUN,
                                  3:self.JS_REMOVING,
                                  5:self.JS_HELD
                                 }

        self.status_output = {
                    self.K_TOTALS: {
                        self.K_UNREADY: 0,
                        self.K_READY: 0,
                        self.K_PRE: 0,
                        self.K_QUEUED: 0,
                        self.K_POST: 0.0,
                        self.K_SUCCEEDED: 0,
                        self.K_FAILED: 0,
                        self.K_PERCENT_DONE: 0.0,
                        self.K_TOTAL: 0
                    },
                    "dags": {
                        "root": {
                            self.K_UNREADY: 0,
                            self.K_READY: 0,
                            self.K_PRE: 0,
                            self.K_QUEUED: 0,
                            self.K_POST: 0,
                            self.K_SUCCEEDED: 0,
                            self.K_FAILED: 0,
                            self.K_PERCENT_DONE: 0.0,
                            self.K_TOTAL: 0,
                            self.K_DAGNAME: None,
                            self.K_DAGSTATE: None
                        }
                    }
                }


    def fetch_status(self, submit_dir: str, *, json : bool=False) -> Union[Dict, None]:
        """
        Shows the workflow status or returns a Dict containing status output
        :return: current status information
        :rtype: Union[dict, None]
        """

        rv_condor_q = self._get_q_values(submit_dir)
        rv_progress = self._get_progress(submit_dir)
        if json:
            return rv_progress
        if rv_condor_q :
            self._show_condor_jobs(rv_condor_q)
        else :
            self._log.info("(No matching jobs found in Condor Q)")
        self._show_job_progress(rv_progress)


    def _get_q_values(self, submit_dir: str):
        """ Internal method to retrieve Condor Q jobs
            Uses braindump to retrieve values of Attributes set by Pegasus
        """

        braindump = self._get_braindump(submit_dir)
        self.root_wf_uuid = braindump.root_wf_uuid
        self.pegasus_wf_name = braindump.pegasus_wf_name

        return self._get_condor_jobs(self.root_wf_uuid)


    def _show_condor_jobs(self, condor_jobs: list):
        """Internal method to display the Condor Q jobs and attributes"""

        self._log.info("{:5}{:^11}{:25}".format('STAT','IN_STATE','JOB'))
        _job_counts = {self.JS_IDLE:0,
                       self.JS_RUN:0,
                       self.JS_HELD:0,
                       self.JS_REMOVING:0
                      }

        for index,each_job in enumerate(condor_jobs):
            stat = self._job_status_codes[each_job['JobStatus']]
            _job_counts[stat] += 1
            in_state = time.strftime('%M:%S', time.gmtime(int(time.time() - each_job['EnteredCurrentStatus'])))
            if index == len(condor_jobs)-1:
                handle_bar_value = '\u2517'
            else:
                handle_bar_value = '\u2523'
            if (each_job['pegasus_wf_xformation'] == 'pegasus::dagman'):
                job_name = '{} ({})'.format(each_job['pegasus_wf_name'],each_job['Iwd'])
                self._log.info("{:5}{:^11}{:25}".format(stat,in_state,job_name))
            else:
                job_name = each_job['pegasus_wf_dag_job_id']
                self._log.info("{:5}{:^11} {}\u2501{:25}".format(stat,in_state,handle_bar_value,job_name))

        self._log.info('Summary : {} Condor jobs total (I:{} R:{})'.format(len(condor_jobs),_job_counts[self.JS_IDLE],_job_counts[self.JS_RUN]))


    def _get_progress(self, submit_dir: str) -> Dict:
        """Internal method to get workflow progress by parsing dagman.out file"""

        dagman_file = '{}/{}.dag.dagman.out'.format(submit_dir,self.pegasus_wf_name)

        with open(dagman_file,'r') as f:
            for line in f:
                dag_status_line = re.match(r'\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d DAG status',line)
                #MM/DD/YY HH:MM:SS DAG status: 0 (DAG_STATUS_OK)

                nodes_total_line = re.match(r'(?=.*(nodes total:))',line)
                #MM/DD/YY HH:MM:SS Of 10 nodes total:

                jobs_progress_line = re.match(r'\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d\ (\s*([0-9])){7}',line)
                #    0        1       2       3       4        5      6        7          8    <-- indices
                #MM/DD/YY hh:mm:ss  Done     Pre   Queued    Post   Ready   Un-Ready   Failed
                #MM/DD/YY hh:mm:ss   ===     ===      ===     ===     ===        ===      ===
                #MM/DD/YY hh:mm:ss    12       0       22       0       0         83        0

                if dag_status_line:
                    dag_status = int(line.split()[4])
                if nodes_total_line:
                    total_nodes = line.split()[3]
                if jobs_progress_line:
                    temp_values = line.split()
                    self.status_output["dags"]["root"] = {
                            self.K_UNREADY: int(temp_values[7]),
                            self.K_READY: int(temp_values[6]),
                            self.K_PRE: int(temp_values[3]),
                            self.K_QUEUED: int(temp_values[4]),
                            self.K_POST: int(temp_values[5]),
                            self.K_SUCCEEDED: int(temp_values[2]),
                            self.K_FAILED: int(temp_values[8]),
                            self.K_PERCENT_DONE: float("{:.2f}".format((int(temp_values[2])/int(total_nodes))*100)),
                            self.K_TOTAL: int(total_nodes),
                            self.K_DAGNAME: '{}.dag'.format(self.pegasus_wf_name),
                        }
                    # Non-zero exitcode shows Failure
                    if dag_status:
                        self.status_output["dags"]["root"][self.K_DAGSTATE] = self.DAG_FAIL

                    # Else Zero exitcode shows DAG_OK status
                    else:

                        #Check for Success DAG status
                        if self.status_output["dags"]["root"][self.K_PERCENT_DONE] == 100.0 :
                            self.status_output["dags"]["root"][self.K_DAGSTATE] = self.DAG_DONE

                        #Else it's Running DAG status
                        else:
                            self.status_output["dags"]["root"][self.K_DAGSTATE] = self.DAG_OK

        return self.status_output


    def _show_job_progress(self, values: dict):
        """Internal method to display workflow progress"""

        self._log.info("\n{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}".format('UNREADY',
                                                                'READY',
                                                                'PRE',
                                                                'QUEUED',
                                                                'POST',
                                                                'SUCCESS',
                                                                'FAILURE',
                                                                '%DONE'))

        dag_totals = values["dags"]["root"]
        self._log.info("{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}{:^8}".format(dag_totals["unready"],
                                                                dag_totals["ready"],
                                                                dag_totals["pre"],
                                                                dag_totals["queued"],
                                                                dag_totals["post"],
                                                                dag_totals["succeeded"],
                                                                dag_totals["failed"],
                                                                dag_totals["percent_done"]))
        dags_count = len(values['dags'])
        self._log.info("Summary: {} DAG total ({}:{})".format(dags_count,dag_totals["state"],dags_count))


    def _get_condor_jobs(self, root_wf_uuid):
        expression = r""'pegasus_wf_uuid == "{}"'"".format(root_wf_uuid)
        cmd = ['condor_q','-constraint',expression,'-json']
        return condor._q(cmd)


    @staticmethod
    def _get_braindump(submit_dir: str):
        try:
            with (Path(submit_dir) / "braindump.yml").open("r") as f:
                bd = braindump.load(f)

        except FileNotFoundError:
            raise WorkflowInstanceError(
                "Unable to load braindump file: {}".format(path)
            )
        return bd

class WorkflowInstanceError(Exception):
    pass
