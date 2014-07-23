import logging
import time

from Pegasus.shadowq.util import Enum

__all__ = ["JSLogRecord", "JSLogEvent", "JSLogReader"]

log = logging.getLogger(__name__)

JSLogEvent = Enum([
    "MONITORD_STARTED",
    "MONITORD_FINISHED",
    "DAGMAN_STARTED",
    "DAGMAN_FINISHED",
    "SUBMIT",                  # Job submitted
    "EXECUTE",                 # Job now running
    "EXECUTABLE_ERROR",        # Error in executable
    "CHECKPOINTED",            # Job was checkpointed
    "JOB_EVICTED",             # Job evicted from machine
    "JOB_TERMINATED",          # Job terminated
    "IMAGE_SIZE",              # Image size of job updated
    "SHADOW_EXCEPTION",        # Shadow threw an exception
    "GENERIC",
    "JOB_ABORTED",             # Job aborted
    "JOB_SUSPENDED",           # Job was suspended
    "JOB_UNSUSPENDED",         # Job was unsuspended
    "JOB_HELD",                # Job was held
    "JOB_RELEASED",            # Job was released
    "NODE_EXECUTE",            # MPI (or parallel) Node executing
    "NODE_TERMINATED",         # MPI (or parallel) Node terminated
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
    "PRESKIP",                 # PRE_SKIP event for DAGMan
    "SUBMIT_FAILED",
    "GRID_SUBMIT_FAILED",
    "JOB_SUCCESS",
    "JOB_FAILURE",
    "POST_SCRIPT_STARTED",
    "POST_SCRIPT_TERMINATED",
    "POST_SCRIPT_SUCCESS",
    "POST_SCRIPT_FAILURE",
    "PRE_SCRIPT_STARTED",
    "PRE_SCRIPT_TERMINATED",
    "PRE_SCRIPT_SUCCESS",
    "PRE_SCRIPT_FAILURE"
])

class JSLogRecord(object):
    def __init__(self):
        self.ts = None
        self.event = None
        self.job_name = None
        self.job_id = None
        self.site = None

    def __str__(self):
        if self.job_name is not None:
            return "%s %s %s" % (self.ts, self.job_name, self.event)
        else:
            return "%s %s" % (self.ts, self.event)

class JSLog(object):
    def __init__(self, jslog_file):
        self.jslog_file = jslog_file
        self.finished = False

    def __iter__(self):
        self.jslog = open(self.jslog_file, "r")
        return self

    def next(self):
        if self.finished:
            self.jslog.close()
            raise StopIteration()

        while True:
            l = self.jslog.readline()
            if not l:
                log.debug("No line available")
                time.sleep(1)
            else:
                if l[-1] != '\n':
                    # XXX Not sure if this can happen
                    raise Exception("JSLog got partial line:\n%s", l)
                log.debug("Got line: %s", l)
                rec = self.parse_record(l)
                if rec.event == JSLogEvent.MONITORD_FINISHED:
                    self.finished = True
                return rec

    def parse_record(self, l):
        rec = l.split()

        record = JSLogRecord()
        record.ts = float(rec[0])

        if rec[1] == "INTERNAL":
            record.event = JSLogEvent[rec[3]]
        else:
            record.job_name = rec[1]
            record.event = JSLogEvent[rec[2]]
            record.job_id = rec[3]
            record.site = rec[4]

        return record

