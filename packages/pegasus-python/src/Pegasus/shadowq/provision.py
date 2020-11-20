import logging
import time
import threading
import subprocess
from Pegasus.shadowq import sim
from Pegasus.shadowq.dag import JobState, JobType

log = logging.getLogger(__name__)

def get_slots():
    try:
        proc = subprocess.Popen(["condor_status", "-total"], shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if proc.wait() != 0:
            raise Exception("Non-zero exitcode")
        totals = output.split("\n")[-2]
        totals = totals.split()
        return int(totals[1])
    except Exception, e:
        log.error("Unable to get total number of slots from condor_status")
        log.exception(e)
        return None

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

        while True:
            try:
                self.loop()
            except Exception, e:
                log.error("Unable to complete provisioning cycle...")
                log.exception(e)
                log.info("Trying again later...")
            time.sleep(self.interval)

    def loop(self):
        log.info("Provisioning resources...")

        current_slots = get_slots() or self.listener.slots() or 1

        start = time.time()
        slots = current_slots
        runtime = self.simulate(start, slots)
        finish = start + runtime
        deadline_diff = finish - self.deadline

        log.info("Slots: %d", slots)
        log.info("Estimated runtime: %f", runtime)
        log.info("Estimated finish: %f", finish)
        log.info("Deadline diff: %f", deadline_diff)
        log.info("Need runtime of: %f", self.deadline - start)

        if start > self.deadline:
            log.error("Exceeded deadline: Maintaining current slots")
        elif start + runtime <= self.deadline:
            log.info("Will meet deadline with current slots")
            # We are going to meet the deadline
            while start + runtime <= self.deadline:
                slots -= 1
                if slots == 0:
                    break
                log.info("Trying %d slots", slots)
                runtime = self.simulate(start, slots)
                log.info("Runtime: %f", runtime)
            slots += 1
            if slots == 1:
                log.info("Requesting minimum number of slots")
        else:
            log.info("Will NOT meet deadline with current slots")
            # We are not going to meet the deadline
            while start + runtime >= self.deadline:
                slots += 1
                log.info("Trying %d slots", slots)
                current_runtime = self.simulate(start, slots)
                log.info("Runtime: %f", current_runtime)
                if runtime - current_runtime < 60:
                    # Runtime didn't significantly improve, so
                    # assume that we cannot meet the deadline
                    log.info("Cannot meet deadline")
                    slots -= 1
                    break
                runtime = current_runtime

        log.info("Requesting %d slots" % slots)

        self.publisher.send_modify_request(
                self.deadline,
                deadline_diff,
                0, #util_max
                current_slots,
                slots)

