import logging

from Pegasus.shadowq.dag import parse_dag
from Pegasus.shadowq.jobstate import JSLogReader, JSLogEvent

__all__ = ["WorkflowMonitor"]

log = logging.getLogger(__name__)

class WorkflowMonitor(object):
    def __init__(self, dag_file, jslog_file):
        self.dag_file = dag_file
        self.jslog_file = jslog_file
        self.dag = parse_dag(dag_file)

    def run(self):
        log.info("Monitoring workflow...")

        for r in JSLogReader(self.jslog_file):
            if r.event == JSLogEvent.MONITORD_STARTED:
                log.info("Monitord started")
            elif r.event == JSLogEvent.MONITORD_FINISHED:
                log.info("Monitord finished")
                # We are done when we get this event
                break
            elif r.event == JSLogEvent.DAGMAN_STARTED:
                log.info("DAGMan started")
            elif r.event == JSLogEvent.DAGMAN_FINISHED:
                log.info("DAGMan finished")
            else:
                job = self.dag.jobs[r.job_name]
                job.process_jslog_record(r)
                self.print_workflow()

        log.info("Workflow finished")

    def print_workflow(self):
        stats = {}
        for job_name in self.dag.jobs:
            job = self.dag.jobs[job_name]
            if job.state not in stats:
                stats[job.state] = 0
            stats[job.state] += 1

        log.info("Workflow State")
        for state in stats:
            log.info("%s: %d", state, stats[state])

