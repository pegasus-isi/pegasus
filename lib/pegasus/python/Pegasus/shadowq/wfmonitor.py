import logging

from Pegasus.shadowq.jobstate import JSLogEvent

__all__ = ["WorkflowMonitor"]

log = logging.getLogger(__name__)

class WorkflowMonitor(object):
    def __init__(self, dag, jslog):
        self.dag = dag
        self.jslog = jslog

    def run(self):
        log.info("Monitoring workflow...")

        for r in self.jslog:
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
                job = self.dag.get_job(r.job_name)
                job.process_jslog_record(r)
                self.dag.print_stats()

        log.info("Workflow finished")


