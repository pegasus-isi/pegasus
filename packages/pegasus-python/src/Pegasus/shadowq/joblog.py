import logging
import time
import datetime

from Pegasus.shadowq.util import Enum

log = logging.getLogger(__name__)

EVENTS = [
    "SUBMIT",                  # Job submitted
    "EXECUTE",                 # Job now running
    "EXECUTABLE_ERROR",        # Error in executable
    "CHECKPOINTED",            # Job was checkpointed
    "JOB_EVICTED",             # Job evicted from machine
    "JOB_TERMINATED",          # Job terminated
    "IMAGE_SIZE",              # Image size of job updated
    "SHADOW_EXCEPTION",        # Shadow threw an exception
    "GENERIC",                 # Generic log event
    "JOB_ABORTED",             # Job aborted
    "JOB_SUSPENDED",           # Job was suspended
    "JOB_UNSUSPENDED",         # Job was unsuspended
    "JOB_HELD",                # Job was held
    "JOB_RELEASED",            # Job was released
    "NODE_EXECUTE",            # MPI (or parallel) Node executing
    "NODE_TERMINATED",         # MPI (or parallel) Node terminated
    "POST_SCRIPT_TERMINATED",  # POST script terminated
    "GLOBUS_SUBMIT",           # Job Submitted to Globus
    "GLOBUS_SUBMIT_FAILED",    # Globus Submit Failed
    "GLOBUS_RESOURCE_UP",      # Globus Machine UP
    "GLOBUS_RESOURCE_DOWN",    # Globus Machine Down
    "REMOTE_ERROR",            # Remote Error
    "JOB_DISCONNECTED",        # RSC socket lost
    "JOB_RECONNECTED",         # RSC socket re-established
    "JOB_RECONNECT_FAILED",    # RSC reconnect failure
    "GRID_RESOURCE_UP",        # Grid machine UP
    "GRID_RESOURCE_DOWN",      # Grid machine Down
    "GRID_SUBMIT",             # Job submitted to grid resource
    "JOB_AD_INFORMATION",      # Job Ad information update
    "JOB_STATUS_UNKNOWN",      # Job status unknown
    "JOB_STATUS_KNOWN",        # Job status known
    "JOB_STAGE_IN",            # Job staging in input files
    "JOB_STAGE_OUT",           # Job staging out output files
    "ATTRIBUTE_UPDATE",        # Job attribute updated
    "PRESKIP"                  # PRE_SKIP event for DAGMan
]

EVENT_MAP = dict((code,event) for code,event in enumerate(EVENTS))

JobLogEvent = Enum(EVENTS)

class JobLogRecord(object):
    def __init__(self):
        self.ts = None
        self.event = None
        self.job_name = None
        self.job_id = None
        self.site = None

    def ts_string(self):
        return time.strftime("%Y/%m/%d %H:%M:%S", self.ts)

    def __str__(self):
        if self.job_name is not None:
            return "%s %s %s" % (self.ts_string(), self.job_name, self.event)
        else:
            return "%s %s" % (self.ts_string(), self.event)

class JobLog(object):
    def __init__(self, joblog_file):
        self.joblog_file = joblog_file
        self.finished = False
        self.jobid_map = {}

    def __iter__(self):
        self.joblog = open(self.joblog_file, "r")
        return self

    def next(self):
        current_event = []
        while True:
            l = self.joblog.readline()
            if not l:
                self.joblog.close()
                raise StopIteration()
            else:
                log.debug("Got line: %s", l)

                current_event.append(l)

                if l.startswith("..."):
                    if l[-1] != '\n':
                        # XXX Not sure if this can happen
                        raise Exception("JobLog got partial line:\n%s", l)

                    return self.parse_event("".join(current_event))

    def parse_jobid(self, idstr):
        idstr = idstr.strip('()')
        jobid, subjobid, subsubjobid = idstr.split(".")
        return "%d.%d" % (int(jobid), int(subjobid))

    def parse_timestamp(self, date, hms):
        year = datetime.datetime.now().year
        timestamp = "%s/%s %s" % (year, date, hms)
        return time.mktime(time.strptime(timestamp, "%Y/%m/%d %H:%M:%S"))

    def extract_dag_node(self, lines):
        for line in lines:
            if line.lstrip().startswith("DAG Node:"):
                return line.split(":")[1].strip()
        raise Exception("DAG Node not found for event:\n%s" % "".join(lines))

    def parse_event(self, e):
        lines = e.split('\n')

        rec = lines[0].split(" ", 4)

        record = JobLogRecord()
        record.event = EVENT_MAP[int(rec[0])]
        record.job_id = self.parse_jobid(rec[1])
        record.ts = self.parse_timestamp(rec[2], rec[3])

        if record.event == JobLogEvent.SUBMIT:
            record.job_name = self.extract_dag_node(lines)
            self.jobid_map[record.job_id] = record.job_name
        else:
            record.job_name = self.jobid_map[record.job_id]

        return record

