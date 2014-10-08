import logging
import time
import threading
from Pegasus.shadowq import sim
from Pegasus.shadowq.dag import JobState, JobType

log = logging.getLogger(__name__)

def uses_slot(jobtype):
    # FIXME This is probably not 100% correct
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

def read_estimates(filename):
    if filename is None:
        return {}

    estimates = {}

    # The estimates file is a two-column table with jobname and runtime
    f = open(filename, "r")
    for l in f:
        l = l.strip()
        rec = l.split()
        estimates[rec[0]] = float(rec[1])

    return estimates

class Provisioner(threading.Thread):
    def __init__(self, dag, estimates, interval, deadline, listener, publisher):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.dag = dag
        self.estimates = read_estimates(estimates)
        self.interval = interval
        self.deadline = deadline
        self.listener = listener
        self.publisher = publisher

    def simulate(self, start, slots):
        newdag = self.dag.clone()
        for j in newdag.jobs.values():

            # Update the runtime estimates
            if j.name in self.estimates:
                j.runtime = self.estimates[j.name]

        # Simulate the workflow to determine time remaining
        s = sim.Simulation(start=start)
        wfe = sim.WorkflowEngine('Engine', s, newdag, slots)
        s.simulate()

        return wfe.runtime

    def run(self):
        log.info("Provisioner starting...")

        while self.listener.status != "ready":
            log.info("Slice not ready. Waiting...")
            time.sleep(60)

        while True:
            log.info("Provisioning resources...")

            start = time.time()
            slots = self.listener.current
            runtime = self.simulate(start, slots)
            deadline_diff = (start + runtime) - self.deadline
            if start + runtime <= self.deadline:
                # We are going to meet the deadline
                while start + runtime <= self.deadline:
                    slots -= 1
                    runtime = self.simulate(start, slots)
                    if slots == 1:
                        log.info("Requesting minimum number of slots")
                        break
            else:
                # We are not going to meet the deadline
                while start + runtime >= self.deadline:
                    slots += 1
                    current_runtime = self.simulate(start, slots)
                    if runtime - current_runtime < 60:
                        # Runtime didn't significantly improve, so
                        # assume that we cannot meet the deadline
                        log.info("Cannot meet deadline")
                        break
                    runtime = current_runtime

            log.info("Requesting %d slots" % slots)

            self.publisher.send_modify_request(
                    self.deadline,
                    deadline_diff,
                    0, #util_max
                    self.listener.current,
                    slots)

            time.sleep(self.interval)

