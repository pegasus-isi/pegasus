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
    "SUBMIT",
    "EXECUTE",
    "JOB_TERMINATED",
    "JOB_SUCCESS",
    "JOB_FAILURE",
    "POST_SCRIPT_STARTED",
    "POST_SCRIPT_TERMINATED",
    "POST_SCRIPT_SUCCESS",
    "POST_SCRIPT_FAILURE"
])

class JSLogRecord(object):
    def ts_string(self):
        return time.strftime("%Y/%m/%d %H:%M:%S", self.ts)

    def __str__(self):
        if self.rtype == "wfstate":
            return "%s: %s" % (self.ts_string(), self.event)
        else:
            return "%s: %s %s" % (self.ts_string(), self.job, self.event)

class JSLogReader(object):
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

        record = JSLogRecord()
        record.ts = time.localtime(float(rec[0]))

        if rec[1] == "INTERNAL":
            record.event = JSLogEvent[rec[3]]
        else:
            record.job_name = rec[1]
            record.event = JSLogEvent[rec[2]]
            record.job_id = rec[3]
            record.site = rec[4]

        return record

