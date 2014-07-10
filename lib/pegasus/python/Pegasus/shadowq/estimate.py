import sys
import time

from Pegasus.shadowq.jobstate import JSLog, JSLogEvent

class Job(object):
    pass

def main():
    if len(sys.argv) != 2:
        print "Usage: %s jobstate.log" % sys.argv[0]
        exit(1)

    jslog_file = sys.argv[1]

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
        print "%s %f" % (name, j.finish - j.start)



