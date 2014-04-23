import os
import sys
import logging
import time

# This configures logging
import Pegasus.common

log = logging.getLogger("shadowq")

class State(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

JobState = State([
    "UNSUBMITTED",
    "QUEUED",
    "RUNNING",
    "POSTSCRIPT",
    "FINISHED",
    "SUCCESSFUL",
    "FAILED"
])

class Job(object):
    def __init__(self, name, submit_file):
        self.name = name
        self.submit_file = submit_file
        self.state = JobState.UNSUBMITTED
        self.parents = []
        self.children = []

class DAG(object):
    def __init__(self, jobs):
        self.jobs = jobs

class DAGException(Exception): pass

def parse_dag(dag_file):
    log.info("Parsing DAG...")

    jobs = {}

    with open(dag_file, "r") as f:
        for l in f:
            l = l.strip()
            rec = l.split()
            if l.startswith("JOB"):
                job_name = rec[1]
                job_submit_file = rec[2]
                j = Job(job_name, job_submit_file)
                jobs[job_name] = j
                log.debug("Parsed job: %s", job_name)
            elif l.startswith("PARENT"):
                # XXX This isn't strictly correct because DAGMan allows
                # multiple parents and children, but Pegasus doesn't use
                # that feature
                p = rec[1]
                c = rec[3]
                parent = jobs[p]
                child = jobs[c]
                parent.children.append(child)
                child.parents.append(parent)
                log.debug("Parsed edge: %s -> %s", p, c)
            elif l.startswith("SCRIPT"):
                # Shadow queue doesn't really care about pre and post scripts
                pass
            elif l.startswith("RETRY"):
                # Shadow queue doesn't care about RETRIES
                pass
            elif l.startswith("MAXJOBS"):
                pass
            elif len(l) == 0 or l[0] == "#":
                # Skip blank lines and comments
                pass
            else:
                raise DAGException("Unrecognized record", l)

    dag = DAG(jobs)

    log.info("Parsed DAG with %d jobs", len(jobs))

    return dag

class JobStateRecord(object):
    def ts_string(self):
        return time.strftime("%Y/%m/%d %H:%M:%S", self.ts)

    def __str__(self):
        if self.rtype == "wfstate":
            return "%s: %s" % (self.ts_string(), self.event)
        else:
            return "%s: %s %s" % (self.ts_string(), self.job, self.event)

class JobStateLogReader(object):
    def __init__(self, jslog_file):
        self.jslog = open(jslog_file, "r")

    def __iter__(self):
        return self

    def next(self):
        l = None
        while not l:
            l = self.jslog.readline()
            if not l:
                log.debug("No line available")
                time.sleep(1)
            else:
                log.debug("Got line: %s", l)
                return self.parse_record(l)

    def parse_record(self, l):
        rec = l.split()

        result = JobStateRecord()
        result.ts = time.localtime(float(rec[0]))

        if rec[1] == "INTERNAL":
            result.rtype = "wfstate"
            result.event = rec[3]
        else:
            result.rtype = "jobstate"
            result.job_name = rec[1]
            result.event = rec[2]
            result.job_id = rec[3]
            result.site = rec[4]

        return result


class ShadowQueue(object):
    def __init__(self, dag_file, jslog_file):
        self.dag_file = dag_file
        self.jslog_file = jslog_file
        self.dag = parse_dag(dag_file)

    def run(self):
        log.info("Shadow queue starting...")
        # TODO Maintain job queue

        for e in JobStateLogReader(self.jslog_file):
            if e.event == "MONITORD_STARTED":
                pass
            elif e.event == "MONITORD_FINISHED":
                # We are done when we get this event
                break
            elif e.event in ["DAGMAN_STARTED","DAGMAN_FINISHED"]:
                pass
            else:
                self.process_job_event(e)

        log.info("Shadow queue finished")

    def process_job_event(self, event):
        job = self.dag.jobs[event.job_name]

        if event.event == "SUBMIT":
            job.state = JobState.QUEUED
        elif event.event == "EXECUTE":
            job.state = JobState.RUNNING
        elif event.event == "JOB_TERMINATED":
            job.state = JobState.FINISHED
        elif event.event == "JOB_SUCCESS":
            job.state = JobState.SUCCESSFUL
        elif event.event == "JOB_FAILURE":
            job.state = JobState.FAILED
        elif event.event == "POST_SCRIPT_STARTED":
            job.state = JobState.POSTSCRIPT
        elif event.event == "POST_SCRIPT_TERMINATED":
            job.state = JobState.FINISHED
        elif event.event == "POST_SCRIPT_SUCCESS":
            job.state = JobState.SUCCESSFUL
        elif event.event == "POST_SCRIPT_FAILURE":
            job.state = JobState.FAILED
        else:
            raise Exception("Unknown job event", event.event)

        self.print_workflow()

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

def main():
    logging.getLogger().setLevel(logging.INFO)

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

    shadowq = ShadowQueue(dag_file, jslog_file)
    shadowq.run()

