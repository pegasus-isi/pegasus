import threading
import logging

from Pegasus.shadowq.jobstate import JSLogEvent

__all__ = ["WorkflowMonitor"]

log = logging.getLogger(__name__)

class WorkflowMonitor(threading.Thread):
    def __init__(self, dag, jslog):
        threading.Thread.__init__(self)
        self.dag = dag
        self.jslog = jslog

    def run(self):
        log.info("Monitoring workflow...")

        for r in self.jslog:
            self.dag.process_jslog_record(r)

        log.info("Workflow finished")


