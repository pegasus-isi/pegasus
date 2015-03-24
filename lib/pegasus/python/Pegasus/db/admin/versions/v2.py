__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 2

import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *
from sqlalchemy.exc import *

log = logging.getLogger(__name__)

class Version(BaseVersion):
    
    def __init__(self, connection):
        super(Version, self).__init__(connection)
    

    def update(self, force=False):
        try:
            self.db.execute("SELECT db_url FROM workflow")
            data = self.db.execute("SELECT COUNT(wf_id) FROM master_workflow").first()
            data2 = self.db.execute("SELECT COUNT(wf_id) FROM master_workflowstate").first()
            if data is not None or data2 is not None:
                if (data[0] > 0 or data2[0] > 0) and not force:
                    log.error("A possible data loss was detected: use '--force' to ignore this message.")
                    raise RuntimeError("A possible data loss was detected: use '--force' to ignore this message.")

            self.db.execute("DROP TABLE IF EXISTS master_workflow")
            self.db.execute("ALTER TABLE workflow RENAME TO master_workflow")
            self.db.execute("DROP INDEX IF EXISTS wf_id_KEY")
            self.db.execute("DROP INDEX IF EXISTS wf_uuid_UNIQUE")
            self.db.execute("CREATE INDEX IF NOT EXISTS KEY_MASTER_WF_ID ON master_workflow (wf_id)")
            self.db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WF_UUID ON master_workflow (wf_uuid)")
            self.db.execute("DROP TABLE IF EXISTS master_workflowstate")
            self.db.execute("ALTER TABLE workflowstate RENAME TO master_workflowstate")
            self.db.execute("DROP INDEX IF EXISTS UNIQUE_WORKFLOWSTATE")
            self.db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WORKFLOWSTATE ON master_workflowstate (wf_id)")

            self.db.execute("CREATE TABLE IF NOT EXISTS ensemble ("
                "id INTEGER PRIMARY KEY,"
                "name VARCHAR(100) NOT NULL,"
                "created DATETIME NOT NULL,"
                "updated DATETIME NOT NULL,"
                "state VARCHAR(6) NOT NULL,"
                "max_running INTEGER NOT NULL,"
                "max_planning INTEGER NOT NULL,"
                "username VARCHAR(100) NOT NULL,"
                "CHECK (state IN('ACTIVE', 'HELD', 'PAUSED'))"
                ")")
            self.db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE ON ensemble (username, name)")

            self.db.execute("CREATE TABLE IF NOT EXISTS ensemble_workflow ("
                "id INTEGER NOT NULL PRIMARY KEY,"
                "name VARCHAR(100) NOT NULL,"
                "basedir VARCHAR(512) NOT NULL,"
                "created DATETIME NOT NULL,"
                "updated DATETIME NOT NULL,"
                "state VARCHAR(11) NOT NULL,"
                "priority INTEGER NOT NULL,"
                "wf_uuid VARCHAR(36),"
                "submitdir VARCHAR(512),"
                "ensemble_id INTEGER NOT NULL,"            
                "CHECK (state IN('PLAN_FAILED', 'RUN_FAILED', 'FAILED', 'RUNNING', 'PLANNING', 'SUCCESSFUL', 'ABORTED', 'READY', 'QUEUED')),"
                "FOREIGN KEY(ensemble_id) REFERENCES ensemble(id)"
                ")")
            self.db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE_WORKFLOW ON ensemble_workflow (ensemble_id, name)")
            self.db.commit()
            
        except OperationalError:
            pass
        except Exception, e:
            self.db.rollback()
            log.exception(e)
            raise Exception(e)

        
    def downgrade(self, force=False):
        "Downgrade is not necessary as master tables have no conflict with previous versions."
        pass
