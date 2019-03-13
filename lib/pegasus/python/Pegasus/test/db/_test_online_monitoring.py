import os
import unittest

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.workflow_loader import WorkflowLoader
from Pegasus.service.monitoring.online_monitoring import OnlineMonitord
from Pegasus.monitoring import event_output as eo
from Pegasus.tools import properties

import Pegasus.test.dax3


class TestOnlineMonitoring(unittest.TestCase):
    def setUp(self):
        logging.basicConfig()
        self.db_file = "monitoring-test.stampede.db"
        db_test_path = os.path.dirname(Pegasus.test.dax3.__file__)

        shutil.copy( ("%s/%s" % (db_test_path, self.db_file)) , self.db_file)

        dburi = "sqlite:///%s" % os.path.abspath(self.db_file)
        # print "DB URI: %s" % dburi
        self.analyzer = WorkflowLoader(dburi, props=properties.Properties())
        self.db_session = self.analyzer.session

        self.wf_uuid = "e168b2a3-c22f-4c03-a834-22afaa3b21b5"
        self.online_monitord = None

        try:
            self.online_monitord = OnlineMonitord("testing", self.wf_uuid, dburi)
#        except eo.SchemaVersionError:
#            print "****************************************************"
#            print "Detected database schema version mismatch!"
#            print "cannot create events output... disabling event output!"
#            print "****************************************************"
        except:
            print "cannot create events output... disabling event output!"

    def tearDown(self):
        # print "Removing test stampede db"
        self.db_session.close()
        self.online_monitord.close()
        for fl in glob.glob("monitoring-test.stampede.db*"):
            os.remove(fl)

    def test_insert_measurement(self):
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

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))

    def test_update_measurement(self):
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

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)

        # now we are updating an existing measurement
        self.analyzer.online_monitoring_update(measurement)

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))

    def test_on_message_integration(self):
        self.assertIsNotNone(self.online_monitord, "online monitord wasn't initialized correctly")

        msg_body = "ts=1430205165 event=workflow_trace level=INFO status=0 job_id=1037283.0 kickstart_pid=11471 " \
                   "executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=0 utime=0.020 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 " \
                   "write_bytes=0 syscr=0 syscw=0 PAPI_TOT_INS=1855598 PAPI_LD_INS=507197 PAPI_SR_INS=277589 " \
                   "PAPI_FP_OPS=795 PAPI_FP_INS=7  wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        msg_body = "ts=1430205166 event=workflow_trace level=INFO status=0 job_id=1037283.0 kickstart_pid=11471 " \
                   "executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=0 utime=0.020 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 " \
                   "write_bytes=0 syscr=0 syscw=0 PAPI_TOT_INS=163965282 PAPI_LD_INS=45781399 PAPI_SR_INS=23833112 " \
                   "PAPI_FP_OPS=194313 PAPI_FP_INS=1572  wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics).\
            filter(JobMetrics.dag_job_id == "namd_ID0000002", JobMetrics.ts == 1430205165).all()

        self.assertEquals(len(result), 1)

    def test_aggregation_logic_for_sequential_app(self):
        self.assertIsNotNone(self.online_monitord, "online monitord wasn't initialized correctly")

        msg_body = "ts=1430205165 utime=0.020 event=workflow_trace level=INFO status=0 " \
                   "job_id=1037283.0 kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun " \
                   "hostname=nid04942 mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 " \
                   "read_bytes=0 write_bytes=0 syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "wf_label=run-5 dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics).\
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 0)

        msg_body = "ts=1430205166 utime=0.060 event=workflow_trace level=INFO status=0 job_id=1037283.0 " \
                   "kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 write_bytes=0 " \
                   "syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics).\
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), 1430205165)
        self.assertEquals(result[0].utime, 0.02)

        msg_body = "ts=1430205167 utime=0.080 event=workflow_trace level=INFO status=0 job_id=1037283.0 " \
                   "kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 write_bytes=0 " \
                   "syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics).\
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), 1430205166)
        self.assertEquals(result[0].utime, 0.06)

    def test_aggregation_logic_for_mpi_app(self):
        """We are simulating messages from 2 mpi processes and we check if the app's utime is a sum of processes' utimes
        """
        self.assertIsNotNone(self.online_monitord, "online monitord wasn't initialized correctly")

        msg_body = "ts=1430205165 utime=0.020 event=workflow_trace level=INFO status=0 " \
                   "job_id=1037283.0 kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun " \
                   "hostname=nid04942 mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 " \
                   "read_bytes=0 write_bytes=0 syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "wf_label=run-5 dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 0)

        msg_body = "ts=1430205166 utime=0.060 event=workflow_trace level=INFO status=0 job_id=1037283.0 " \
                   "kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=1 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 write_bytes=0 " \
                   "syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()

        self.assertEquals(len(result), 0)

        msg_body = "ts=1430205167 utime=0.20 event=workflow_trace level=INFO status=0 " \
                   "job_id=1037283.0 kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun " \
                   "hostname=nid04942 mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 " \
                   "read_bytes=0 write_bytes=0 syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "wf_label=run-5 dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), 1430205166)
        self.assertEquals(result[0].utime, 0.08)

        msg_body = "ts=1430205168 utime=0.60 event=workflow_trace level=INFO status=0 job_id=1037283.0 " \
                   "kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun hostname=nid04942 " \
                   "mpi_rank=1 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 read_bytes=0 write_bytes=0 " \
                   "syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 wf_label=run-5 " \
                   "dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), 1430205166)
        self.assertEquals(result[0].utime, 0.08)

        msg_body = "ts=1430205169 utime=0.20 event=workflow_trace level=INFO status=0 " \
                   "job_id=1037283.0 kickstart_pid=11471 executable=/opt/cray/alps/5.2.1-2.0502.9072.13.1.gem/bin/aprun " \
                   "hostname=nid04942 mpi_rank=0 stime=0.010 iowait=0.000 vmSize=56552 vmRSS=2260 threads=3 " \
                   "read_bytes=0 write_bytes=0 syscr=0 syscw=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "wf_label=run-5 dag_job_id=namd_ID0000002 condor_job_id=1037283.0"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "namd_ID0000002").all()
        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), 1430205168)
        self.assertEquals(result[0].utime, 0.8)

    def test_insert_partial_message(self):
        measurement = {
            "wf_uuid": self.wf_uuid,
            "dag_job_id": "sassena_ID0000006",
            "sched_id": "1037327.0",
            # "hostname": "test-hostname",
            # "exec_name": "/path/to/somewhere",
            # "kickstart_pid": 12345,
            "ts": 1430206115,
            # "stime": 7.280,
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

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))
        self.assertIsNone(result[0].exec_name)
        self.assertIsNone(result[0].hostname)
        self.assertIsNone(result[0].kickstart_pid)
        self.assertIsNone(result[0].stime)

    def test_update_partial_message(self):
        measurement = {
            "wf_uuid": "e168b2a3-c22f-4c03-a834-22afaa3b21b5",
            "dag_job_id": "sassena_ID0000005",
            "sched_id": "1037329.0",
            # "hostname": "test-hostname",
            # "exec_name": "/path/to/somewhere",
            # "kickstart_pid": 12345,

            "ts": 1430208115,  # we want to update a measurement by changing ts
            # "stime": 7.280,
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

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)

        # now we are updating an existing measurement
        self.analyzer.online_monitoring_update(measurement)

        result = self.db_session.query(JobMetrics).filter(JobMetrics.dag_job_id == measurement["dag_job_id"]).all()

        self.assertEquals(len(result), 1)
        self.assertEquals(int(result[0].ts), int(measurement["ts"]))
        self.assertIsNone(result[0].exec_name)
        self.assertIsNone(result[0].hostname)
        self.assertIsNone(result[0].kickstart_pid)

    def test_insert_transfer_message(self):
        self.assertIsNotNone(self.online_monitord, "online monitord wasn't initialized correctly")

        msg_body = "ts=1431387401 event=data_transfer level=INFO status=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "dag_job_id=stage_in_local_hopper_2_0 hostname=obelix.isi.edu condor_job_id=1037277.0 " \
                   "src_url=file:///nfs/asd/darek/pegasus/SNS-Workflow/inputs/topfile_mock " \
                   "src_site_name=local " \
                   "dst_url=gsiftp://hoppergrid.nersc.gov/scratch/scratchdirs/darek/pegasus/run-sns-low-synth-1/run0001/topfile_mock " \
                   "dst_site_name=hopper transfer_start_ts=1431387400 transfer_duration=1 bytes_transferred=1024"

        self.online_monitord.on_message(msg_body)

        msg_body = "ts=1431387410 event=data_transfer level=INFO status=0 wf_uuid=e168b2a3-c22f-4c03-a834-22afaa3b21b5 " \
                   "dag_job_id=stage_in_local_hopper_2_0 hostname=obelix.isi.edu condor_job_id=1037277.0 " \
                   "src_url=file:///nfs/asd/darek/pegasus/SNS-Workflow/inputs/topfile_mock " \
                   "src_site_name=local " \
                   "dst_url=gsiftp://hoppergrid.nersc.gov/scratch/scratchdirs/darek/pegasus/run-sns-low-synth-1/run0001/topfile_mock " \
                   "dst_site_name=hopper transfer_start_ts=1431387400 transfer_duration=1 bytes_transferred=1024"

        self.online_monitord.on_message(msg_body)

        result = self.online_monitord.event_sink._db.session.query(JobMetrics). \
            filter(JobMetrics.dag_job_id == "stage_in_local_hopper_2_0", JobMetrics.bytes_transferred == 1024).all()

        self.assertEquals(len(result), 1)


if __name__ == '__main__':
    unittest.main()
