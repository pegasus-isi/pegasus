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
            self.db.execute("SELECT parent_wf_id FROM workflow")
            try:
                self.db.execute("ALTER TABLE workflow ADD COLUMN db_url TEXT")
            except (OperationalError, ProgrammingError):
                pass
            except Exception, e:
                self.db.rollback()
                raise DBAdminError(e)
            return
        except Exception:
            try:
                self.db.execute("SELECT db_url FROM workflow")
            except (OperationalError, ProgrammingError):
                return
            
        data = None
        data2 = None
        try:
            data = self.db.execute("SELECT COUNT(wf_id) FROM master_workflow").first()
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            self.db.rollback()
            raise DBAdminError(e)
            
        try:
            data2 = self.db.execute("SELECT COUNT(wf_id) FROM master_workflowstate").first()
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            self.db.rollback()
            raise DBAdminError(e)
        
        if data2 is not None:
            if data2[0] > 0:
                raise DBAdminError("Table master_workflowstate already exists and is not empty.")
            else:
                self.db.execute("DROP TABLE master_workflowstate")
        if data is not None:
            if data[0] > 0:
                raise DBAdminError("Table master_workflow already exists and is not empty.")
            else:
                self.db.execute("DROP TABLE master_workflow")
               
        try:
            self.db.execute("ALTER TABLE workflow RENAME TO master_workflow")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            self.db.rollback()
            raise DBAdminError(e)
        
        try:
            self.db.execute("ALTER TABLE workflowstate RENAME TO master_workflowstate")
        except (OperationalError, ProgrammingError):
            pass
        except Exception, e:
            self.db.rollback()
            raise DBAdminError(e)
            
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

        
    def downgrade(self, force=False):
        pass