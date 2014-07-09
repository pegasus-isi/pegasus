import os
import sys
import logging
import time

from Pegasus.shadowq.dag import parse_dag
from Pegasus.shadowq.jobstate import JSLog
from Pegasus.shadowq.wfmonitor import WorkflowMonitor
from Pegasus.shadowq.provision import Provisioner

__all__ = ["main"]

log = logging.getLogger(__name__)

def main():
    # This configures logging
    import Pegasus.common

    logging.getLogger("Pegasus.shadowq").setLevel(logging.INFO)

    if len(sys.argv) != 2:
        print "Usage: %s DAGFILE" % sys.argv[0]
        exit(1)

    log.info("Shadow queue starting...")

    dag_file = sys.argv[1]
    dag_file = os.path.join(os.getcwd(), dag_file)
    dag_file = os.path.abspath(dag_file)

    wf_dir = os.path.dirname(dag_file)

    jslog_file = os.path.join(wf_dir, "jobstate.log")

    log.info("DAG: %s", dag_file)
    log.info("Workflow Dir: %s", wf_dir)
    log.info("Jobstate Log: %s", jslog_file)

    if not os.path.isfile(dag_file):
        log.error("DAG not found")
        exit(1)

    if not os.path.isdir(wf_dir):
        log.error("Workflow directory not found")
        exit(1)

    # Give it a few seconds for the jobstate.log file to be created
    for i in range(0, 15):
        if os.path.isfile(jslog_file):
            break
        time.sleep(1)

    if not os.path.isfile(jslog_file):
        log.error("Jobstate log not found")
        exit(1)

    # Change working directory
    os.chdir(wf_dir)

    dag = parse_dag(dag_file)
    jslog = JSLog(jslog_file)

    monitor = WorkflowMonitor(dag, jslog)
    monitor.start()

    slots = int(os.getenv("SHADOWQ_SLOTS", 1))
    estimates = os.getenv("SHADOWQ_ESTIMATES", None)
    interval = int(os.getenv("SHADOWQ_PROVISIONER_INTERVAL", 60))
    provisioner = Provisioner(dag, estimates, slots, interval)
    provisioner.start()

    monitor.join()

    log.info("Shadow queue exiting")

