import os
import logging
import time

from Pegasus.tools import utils
from Pegasus.shadowq.dag import parse_dag
from Pegasus.shadowq.jobstate import JSLog
from Pegasus.shadowq.wfmonitor import WorkflowMonitor
from Pegasus.shadowq.provision import Provisioner
from Pegasus.shadowq.messaging import ManifestListener, RequestPublisher

log = logging.getLogger(__name__)

def start(dag_file):
    log.info("Shadow queue starting...")

    dag_file = os.path.join(os.getcwd(), dag_file)
    dag_file = os.path.abspath(dag_file)

    wf_dir = os.path.dirname(dag_file)

    jslog_file = os.path.join(wf_dir, "jobstate.log")

    braindump = utils.slurp_braindb(wf_dir)

    wf_uuid = braindump["wf_uuid"]

    log.info("wf_uuid: %s", wf_uuid)
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

    estimates = os.getenv("SHADOWQ_ESTIMATES", None)
    interval = int(os.getenv("SHADOWQ_PROVISIONER_INTERVAL", 120))
    makespan = int(os.getenv("SHADOWQ_MAKESPAN", 0))
    sliceid = os.getenv("SHADOWQ_SLICEID")

    deadline = time.time() + makespan

    log.info("Interval: %d", interval)
    log.info("Makespan: %d", makespan)
    log.info("Deadline: %d", deadline)

    amqp_url = os.getenv("PEGASUS_AMQP_URL")
    if amqp_url is None:
        log.error("PEGASUS_AMQP_URL not specified in environment")
        exit(1)

    listener = ManifestListener(amqp_url, sliceid)
    listener.start()

    publisher = RequestPublisher(amqp_url, sliceid, wf_uuid)

    provisioner = Provisioner(dag, estimates, interval, deadline, listener, publisher)
    provisioner.start()

    monitor.join()

    publisher.send_workflow_finished()

    log.info("Shadow queue exiting")

