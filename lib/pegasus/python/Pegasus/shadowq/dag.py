import logging

from Pegasus.shadowq.jobstate import JSLogEvent
from Pegasus.shadowq.util import Enum

__all__ = ["JobState","Job","DAG","DAGException","parse_dag"]

log = logging.getLogger(__name__)

JobState = Enum([
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

    def process_jslog_record(self, record):
        if record.event == JSLogEvent.SUBMIT:
            self.state = JobState.QUEUED
        elif record.event == JSLogEvent.EXECUTE:
            self.state = JobState.RUNNING
        elif record.event ==JSLogEvent.JOB_TERMINATED:
            self.state = JobState.FINISHED
        elif record.event ==JSLogEvent.JOB_SUCCESS:
            self.state = JobState.SUCCESSFUL
        elif record.event == JSLogEvent.JOB_FAILURE:
            self.state = JobState.FAILED
        elif record.event == JSLogEvent.POST_SCRIPT_STARTED:
            self.state = JobState.POSTSCRIPT
        elif record.event ==JSLogEvent.POST_SCRIPT_TERMINATED:
            self.state = JobState.FINISHED
        elif record.event == JSLogEvent.POST_SCRIPT_SUCCESS:
            self.state = JobState.SUCCESSFUL
        elif record.event == JSLogEvent.POST_SCRIPT_FAILURE:
            self.state = JobState.FAILED
        else:
            raise Exception("Unknown job state log event", record.event)

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

