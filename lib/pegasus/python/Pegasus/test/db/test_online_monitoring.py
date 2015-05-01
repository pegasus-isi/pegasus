import os
import unittest

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.modules.stampede_loader import Analyzer
from Pegasus.service.monitoring.online_monitoring import OnlineMonitord
from Pegasus.monitoring import event_output as eo


class TestOnlineMonitoring(unittest.TestCase):
    def setUp(self):
        logging.basicConfig()
        self.db_file = "monitoring-test.stampede.db"

        print "Loading test stampede db"
        shutil.copy("../" + self.db_file, self.db_file)

        dburi = "sqlite:///%s" % os.path.abspath(self.db_file)
        print "DB URI: %s" % dburi
        self.analyzer = Analyzer(dburi)
        self.db_session = self.analyzer.session

        self.wf_uuid = "e168b2a3-c22f-4c03-a834-22afaa3b21b5"
        self.online_monitord = None

        try:
            self.online_monitord = OnlineMonitord("testing", self.wf_uuid, dburi)
        except eo.SchemaVersionError:
            print "****************************************************"
            print "Detected database schema version mismatch!"
            print "cannot create events output... disabling event output!"
            print "****************************************************"
        except:
            print "cannot create events output... disabling event output!"

    def tearDown(self):
        print "Removing test stampede db"
        self.online_monitord.close()
        os.remove(self.db_file)

    def test_insert_measurement(self):
        print "Test test_insert_measurement"
        measurement = {
            "wf_uuid": self.wf_uuid,
            "dag_job_id": "sassena_ID0000006",
            "sched_id": "1037327.0",
            "hostname": "test-hostname",
            "exec_name": "/path/to/somewhere",
            "kickstart_pid": 12345,
            "ts": 1430206115,
            "stime": 7.280,
            "utime": 324.670,
            "iowait": 0.670,
            "vmSize": 3551292,
            "vmRSS": 28540,
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

            "ts": 1430208115,  # we want to update a measurement by changing ts
            "stime": 7.280,
            "utime": 324.670,
            "iowait": 0.670,
            "vmSize": 3551292,
            "vmRSS": 28540,
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

    def test_on_message_integration(self):
        self.assertIsNotNone(self.online_monitord, "online monitord wasn't initialized correctly")

        msg_body = "ts=1430205165 event=workflow_trace level=INFO status=0 job_id=1037283.0 kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 mpi_rank=0 utime=0.020 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 write_bytes=0 syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 dag_job_id=namd_ID0000002 condor_job_id=1037283.0"
        self.online_monitord.on_message(None, None, None, msg_body)
        self.online_monitord.on_message(None, None, None, msg_body)
        # self.online_monitord.event_sink.close()

        result = self.online_monitord.event_sink._db.session.query(JobMetrics).\
            filter(JobMetrics.dag_job_id == "namd_ID0000002", JobMetrics.ts == 1430205165).all()

        self.assertEquals(len(result), 1)


if __name__ == '__main__':
    unittest.main()
