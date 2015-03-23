import logging

from Pegasus.db.admin.versions.base_version import BaseVersion

DB_VERSION = 4

log = logging.getLogger(__name__)

class Version(BaseVersion):

    def update(self, force):
        "Add archived field to master_workflow table"
        # TODO We might need to check to see if the field already exists first
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            try:
                db.execute("ALTER TABLE master_workflow ADD archived BOOLEAN NOT NULL default FALSE")
            except Exception, e:
                log.error("Error adding archived field to master_workflow table")
                log.exception(e)

    def downgrade(self, force):
        "Downgrade is not necessary as archived is added with a default that works for old versions"
        pass

    def is_compatible(self):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            try:
                db.execute("SELECT archived FROM master_workflow")
            except Exception, e:
                return False
        return True

