import logging

from Pegasus.db.admin.versions.base_version import BaseVersion
from sqlalchemy.exc import *

DB_VERSION = 4.1

log = logging.getLogger(__name__)

class Version(BaseVersion):

    def __init__(self, connection):
        super(Version, self).__init__(connection)


    def update(self, force=False):
        "Add archived field to master_workflow table"
        log.info("Updating to version %s" % DB_VERSION)

        try:
            self.db.execute("""
                CREATE TABLE job_metrics (
                  job_metrics_id	INTEGER	      NOT NULL,
                  job_instance_id	INTEGER	      NOT NULL,
                  dag_job_id	    VARCHAR(255)  NOT NULL,
                  hostname	        VARCHAR(255),
                  exec_name	        VARCHAR(255),
                  kickstart_pid	    INTEGER,
                  ts	            NUMERIC(16,6),
                  stime	            FLOAT,
                  utime	            FLOAT,
                  iowait	        FLOAT,
                  vmsize	        INTEGER,
                  vmrss	            INTEGER,
                  read_bytes	    INTEGER,
                  write_bytes	    INTEGER,
                  syscr	            INTEGER,
                  syscw	            INTEGER,
                  threads	        INTEGER,
                  bytes_transferred INTEGER,
                  transfer_duration INTEGER,
                  site              VARCHAR(255),
                  totins            BIGINT,
                  fpops             BIGINT,
                  fpins             BIGINT,
                  ldins             BIGINT,
                  srins             BIGINT,
                  l3misses          BIGINT,
                  l2misses          BIGINT,
                  l1misses          BIGINT,

                  PRIMARY 	KEY (job_metrics_id),
                  FOREIGN	KEY (job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE
                );
                """)

            self.db.execute("CREATE INDEX job_metrics_dag_job_id_idx ON job_metrics (dag_job_id);")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            log.error("Error adding 'job_metrics' table")
            log.exception(e)
            raise RuntimeError(e)


    def downgrade(self, force=False):
        try:
            self.db.execute("DROP INDEX job_metrics_dag_job_id_idx;")
            self.db.execute("DROP TABLE job_metrics;")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            log.error("Error removing 'job_metrics' table")
            log.exception(e)
            raise RuntimeError(e)
