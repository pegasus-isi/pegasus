import logging
import time
import threading
from Pegasus.shadowq import sim
from Pegasus.shadowq.dag import JobState, JobType

log = logging.getLogger(__name__)

def uses_slot(jobtype):
    # TODO This is probably not 100% correct
    return jobtype in (
        JobType.COMPUTE,
        JobType.STAGE_IN,
        JobType.STAGE_OUT,
        JobType.INTER_POOL,
        JobType.CREATE_DIR,
        JobType.STAGE_IN_WORKER_PACKAGE,
        JobType.CLEANUP,
        JobType.CHMOD
    )

def simulate(dag):

    # TODO Figure out how many slots we really have available
    slots = 0
    for j in newdag.jobs:
        if j.state == JobState.RUNNING and uses_slot(j.jobtype):
            slots += 1

    s = sim.Simulation()
    wfe = sim.WorkflowEngine('Engine', dag.jobs, slots)
    s.add(wfe)
    s.simulate()
    return wfe.runtime

class Provisioner(threading.Thread):
    def __init__(self, dag, interval):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.dag = dag
        self.interval = interval

    def run(self):
        while True:
            log.info("Provisioning resources...")


            newdag = self.dag.clone()

            # Simulate the workflow to determine time remaining
            runtime = simulate(newdag)

            log.info("Time remaining: %s", runtime)
            log.info("Workflow should finish at: %s", (time.time() + runtime))

            time.sleep(self.interval)

