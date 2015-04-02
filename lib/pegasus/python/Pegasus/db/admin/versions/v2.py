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
            data = None
            data2 = None
            try:
                data = self.db.execute("SELECT COUNT(wf_id) FROM master_workflow").first()
                data2 = self.db.execute("SELECT COUNT(wf_id) FROM master_workflowstate").first()
            except:
                pass
            
            if data is not None or data2 is not None:
                if (data[0] > 0 or data2[0] > 0) and not force:
                    raise DBAdminError("A possible data loss was detected: use '--force' to ignore this message.")
            
            if data:
                self.db.execute("DELETE FROM master_workflow")
                self.db.execute("INSERT INTO master_workflow(wf_id, wf_uuid, \
                    dax_label, dax_version, dax_file, dag_file_name, timestamp, \
                    submit_hostname, submit_dir, planner_arguments, user, \
                    grid_dn, planner_version) SELECT wf_id, wf_uuid, \
                    dax_label, dax_version, dax_file, dag_file_name, timestamp, \
                    submit_hostname, submit_dir, planner_arguments, user, \
                    grid_dn, planner_version FROM workflow")
            if data2:
                self.db.execute("DELETE FROM master_workflowstate")
                self.db.execute("INSERT INTO master_workflowstate(wf_id, state, \
                    timestamp, restart_count, status) SELECT wf_id, state, \
                    timestamp, restart_count, status FROM workflowstate")
            
            self.db.execute("ALTER TABLE workflow DROP COLUMN db_url")
            
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
            try :
                self.db.execute("CREATE INDEX UNIQUE_ENSEMBLE ON ensemble (username, name)")
            except:
                pass
            try:
                self.db.execute("CREATE INDEX UNIQUE_ENSEMBLE_WORKFLOW ON ensemble_workflow (ensemble_id, name)")
            except:
                pass
            
            self.db.commit()
            
        except OperationalError:
            pass
        except Exception, e:
            self.db.rollback()
            raise DBAdminError(e)

        
    def downgrade(self, force=False):
        
        self.db.execute("ALTER TABLE workflow ADD COLUMN db_url TEXT")
