__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 1.2

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *

class Version(BaseVersion):
    
    def __init__(self, config_properties, verbose=False, debug=False):
        super(Version, self).__init__(config_properties, verbose, debug)
        self.db = DashboardDB(config_properties, verbose, debug)
    
# -------------------------------------------------------------------
    def update(self, force):
        self.update_dashboard()
        
# -------------------------------------------------------------------
    def downgrade(self, force):
        self.downgrade_dashboard()

# -------------------------------------------------------------------
    def update_dashboard(self):
        self.rename_table("workflow", "master_workflow")
        self.db.execute_update("DROP INDEX IF EXISTS wf_id_KEY")
        self.db.execute_update("DROP INDEX IF EXISTS wf_uuid_UNIQUE")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS KEY_MASTER_WF_ID ON master_workflow (wf_id)")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WF_UUID ON master_workflow (wf_uuid)")
        
        self.rename_table("workflowstate", "master_workflowstate")
        self.db.execute_update("DROP INDEX IF EXISTS UNIQUE_WORKFLOWSTATE")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WORKFLOWSTATE ON master_workflowstate (wf_id)")
        
        self.db.execute_update("CREATE TABLE IF NOT EXISTS ensemble ("
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
        self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE ON ensemble (username, name)")
            
        self.db.execute_update("CREATE TABLE IF NOT EXISTS ensemble_workflow ("
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
        self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_ENSEMBLE_WORKFLOW ON ensemble_workflow (ensemble_id, name)")
        
# -------------------------------------------------------------------        
    def downgrade_dashboard(self):
        self.rename_table("master_workflow", "workflow")
        self.db.execute_update("DROP INDEX IF EXISTS KEY_MASTER_WF_ID")
        self.db.execute_update("DROP INDEX IF EXISTS UNIQUE_MASTER_WF_UUID")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS wf_id_KEY ON workflow (wf_id)")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS wf_uuid_UNIQUE ON workflow (wf_uuid)")
        
        self.rename_table("master_workflowstate", "workflowstate")
        self.db.execute_update("DROP INDEX IF EXISTS UNIQUE_MASTER_WORKFLOWSTATE")
        self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_WORKFLOWSTATE ON workflowstate (wf_id)")
        
# -------------------------------------------------------------------
    def rename_table(self, old_name, new_name):
        try:
            self.db.execute_update("ALTER TABLE %s RENAME TO %s" % (old_name, new_name,))
        except:
            pass

# -------------------------------------------------------------------
    def is_compatible(self):
        try:
            cur = self.db.get_connection().cursor()
            cur.execute("SELECT wf_id FROM master_workflow")
        except:
            return False
        try:
            self.db.execute_update("DROP INDEX UNIQUE_MASTER_WF_UUID")
            self.db.execute_update("CREATE INDEX IF NOT EXISTS UNIQUE_MASTER_WF_UUID ON master_workflow (wf_uuid)")
        except:
            return False
        return True

# -------------------------------------------------------------------
    def dispose(self):
        self.db.close()