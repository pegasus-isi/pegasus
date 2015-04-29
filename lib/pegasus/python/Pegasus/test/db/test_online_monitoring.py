import errno
import os
import re
import unittest
import uuid
import shutil

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.modules.stampede_loader import Analyzer


class TestOnlineMonitoring(unittest.TestCase):
    def setUp(self):
        self.db_file = "monitoring-test.stampede.db"

        print "Loading test stampede db"
        shutil.copy("../" + self.db_file, self.db_file)

        dburi = "sqlite:///%s" % os.path.abspath(self.db_file)
        print "DB URI: %s" % dburi
        self.analyzer = Analyzer(dburi)
        self.db_session = self.analyzer.session

    def tearDown(self):
        print "Removing test stampede db"
        os.remove(self.db_file)

    def test_insert_measurement(self):
        print "Test test_insert_measurement"
        measurement = {
            "wf_uuid": "e168b2a3-c22f-4c03-a834-22afaa3b21b5",
            "dag_job_id": "sassena_ID0000006",
            "sched_id": "1037327.0",
            "hostname": "test-hostname",
            "exec_name": "/path/to/somewhere",
            "kickstart_pid": 12345,
            "ts": 1430206115,
            "stime": 7.280,
            "utime": 324.670,
            "iowait": 0.670,
            "vmsize": 3551292,
            "vmrss": 28540,
            "read_bytes": 0,
            "write_bytes": 0,
            "syscr": 0,
            "syscw": 0,
            "threads": 3
        }

        self.analyzer.online_monitoring_update(measurement)

        # query = st_job_metrics.select().where(st_job_metrics.c.dag_job_id == measurement["dag_job_id"])
        # result = self.analyzer.session.execute(query).fetchall()
        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))

    def test_update_measurement(self):
        print "Test test_update_measurement"
        measurement = {
            "wf_uuid": "e168b2a3-c22f-4c03-a834-22afaa3b21b5",
            "dag_job_id": "sassena_ID0000005",
            "sched_id": "1037329.0",
            "hostname": "test-hostname",
            "exec_name": "/path/to/somewhere",
            "kickstart_pid": 12345,

            "ts": 1430208115, # we want to update a measurement by changing ts
            "stime": 7.280,
            "utime": 324.670,
            "iowait": 0.670,
            "vmsize": 3551292,
            "vmrss": 28540,
            "read_bytes": 0,
            "write_bytes": 0,
            "syscr": 0,
            "syscw": 0,
            "threads": 3
        }

        # query = st_job_metrics.select().where(st_job_metrics.c.dag_job_id == measurement["dag_job_id"])
        # result = self.analyzer.session.execute(query).fetchall()
        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)

        # now we are updating an existing measurement
        self.analyzer.online_monitoring_update(measurement)

        # query = st_job_metrics.select().where(st_job_metrics.c.dag_job_id == measurement["dag_job_id"])
        # result = self.analyzer.session.execute(query).fetchall()
        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))


if __name__ == '__main__':
    unittest.main()
