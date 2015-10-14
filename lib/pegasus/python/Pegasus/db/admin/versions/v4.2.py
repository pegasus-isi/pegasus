import logging

from Pegasus.db.admin.versions.base_version import BaseVersion
from sqlalchemy.exc import *

DB_VERSION = 4.2

log = logging.getLogger(__name__)

class Version(BaseVersion):

    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        """Add archived field to master_workflow table"""
        log.info("Updating to version %s" % DB_VERSION)

        try:
            self.db.execute("""
                CREATE TABLE anomalies (
                  anomaly_id	    INTEGER	      NOT NULL,
                  wf_id             INTEGER	      NOT NULL,
                  ts	            NUMERIC(16,6) NOT NULL,
                  job_instance_id   INTEGER       NULL,
                  dag_job_id	    VARCHAR(255),
                  anomaly_type	    VARCHAR(255)  NOT NULL,
                  metrics	        VARCHAR(255),
                  message	        VARCHAR(255)  NOT NULL,
                  json  	        TEXT  NOT NULL,

                  PRIMARY   KEY (anomaly_id),
                  FOREIGN   KEY (wf_id) REFERENCES workflow (wf_id) ON DELETE CASCADE,
                  FOREIGN   KEY (job_instance_id) REFERENCES job_instance (job_instance_id) ON DELETE CASCADE
                );
                """)

            self.db.execute("CREATE INDEX anomalies_ts_idx ON anomalies (ts);")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            log.error("Error adding 'job_metrics' table")
            log.exception(e)
            raise RuntimeError(e)

    def downgrade(self, force=False):
        try:
            self.db.execute("DROP INDEX anomalies_ts_idx;")
            self.db.execute("DROP TABLE job_metrics;")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            log.error("Error removing 'job_metrics' table")
            log.exception(e)
            raise RuntimeError(e)
