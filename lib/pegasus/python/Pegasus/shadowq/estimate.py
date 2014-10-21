import sys
import time

from Pegasus.shadowq.jobstate import JSLog, JSLogEvent
from Pegasus.shadowq.joblog import JobLog, JobLogEvent

class Job(object):
    pass

def estimate_jobstate(jslog_file):
    jobs = {}

    for rec in JSLog(jslog_file):
        if rec.job_name is None:
            continue

        ts = time.mktime(rec.ts)

        if rec.job_name not in jobs:
            jobs[rec.job_name] = Job()

        j = jobs[rec.job_name]

        if rec.event == JSLogEvent.EXECUTE:
            j.start = ts
        elif rec.event == JSLogEvent.JOB_TERMINATED:
            j.finish = ts

    for name, j in jobs.items():
        if hasattr(j, "finish"):
            print "%s %f" % (name, j.finish - j.start)
        else:
            print "%s not finished" % (name)

def estimate_joblog(joblog_file):
    jobs = {}

    for r in JobLog(joblog_file):
        if r.job_name not in jobs:
            jobs[r.job_name] = Job()

        j = jobs[r.job_name]

        if r.event == JobLogEvent.EXECUTE:
            j.start = r.ts
        elif r.event == JobLogEvent.JOB_TERMINATED:
            j.finish = r.ts

    for name, j in jobs.items():
        if hasattr(j, "finish"):
            print name, (j.finish - j.start)
        else:
            print name, "not finished"

def main():
    if len(sys.argv) != 2:
        print "Usage: %s JOB_LOG" % sys.argv[0]
        exit(1)

    estimate_joblog(sys.argv[1])

