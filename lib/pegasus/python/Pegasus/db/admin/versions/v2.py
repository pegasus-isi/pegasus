__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 2

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *
from sqlalchemy.orm.exc import *

class Version(BaseVersion):
    
    def __init__(self, connections, database_name=None, verbose=False, debug=False):
        super(Version, self).__init__(connections, database_name, verbose, debug)
    

    def update(self, force):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            try:
                db.execute("SELECT db_url FROM workflow")
                data = db.execute("SELECT COUNT(wf_id) FROM master_workflow").first()
                data2 = db.execute("SELECT COUNT(wf_id) FROM master_workflowstate").first()
                if data is not None or data2 is not None:
                    if (data[0] > 0 or data2[0] > 0) and not force:
                        sys.stderr.write("ERROR: A possible data loss was detected: use '--force' to ignore this message.\n")
                        exit(1)
                db.execute("DROP TABLE IF EXISTS master_workflow")
                db.execute("ALTER TABLE workflow RENAME TO master_workflow")
                db.execute("DROP INDEX IF EXISTS wf_id_KEY")
                db.execute("DROP INDEX IF EXISTS wf_uuid_UNIQUE")
                db.execute("CREATE INDEX IF NOT EXISTS KEY_MASTER_WF_ID ON master_workflow (wf_id)")
                db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WF_UUID ON master_workflow (wf_uuid)")
                db.execute("DROP TABLE IF EXISTS master_workflowstate")
                db.execute("ALTER TABLE workflowstate RENAME TO master_workflowstate")
                db.execute("DROP INDEX IF EXISTS UNIQUE_WORKFLOWSTATE")
                db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WORKFLOWSTATE ON master_workflowstate (wf_id)")

                db.execute("CREATE TABLE IF NOT EXISTS ensemble ("
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
                db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE ON ensemble (username, name)")

                db.execute("CREATE TABLE IF NOT EXISTS ensemble_workflow ("
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
                db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE_WORKFLOW ON ensemble_workflow (ensemble_id, name)")
            except OperationalError:
                pass

        
    def downgrade(self, force):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].db
            data = db.execute("SELECT COUNT(wf_id) FROM master_workflow").first()
            if data is not None:
                if data[0] > 0 and not force:
                    sys.stderr.write("ERROR: A possible data loss was detected: use '--force' to ignore this message.\n")
                    exit(1)
            data = db.execute("SELECT COUNT(wf_id) FROM master_workflowstate").first()
            if data is not None:
                if data[0] > 0 and not force:
                    sys.stderr.write("ERROR: A possible data loss was detected: use '--force' to ignore this message.\n")
                    exit(1)
            db.execute("DROP TABLE master_workflowstate")
            db.execute("DROP TABLE master_workflow")


    def is_compatible(self):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            try:
                db.execute("SELECT wf_id FROM master_workflow")
            except:
                return False
            try:
                db.execute("DROP INDEX UNIQUE_MASTER_WF_UUID")
                db.execute("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WF_UUID ON master_workflow (wf_uuid)")
            except:
                return False
        return True
