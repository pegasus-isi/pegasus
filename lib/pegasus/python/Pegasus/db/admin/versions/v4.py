import logging

from Pegasus.db.admin.versions.base_version import BaseVersion
from sqlalchemy.exc import *

DB_VERSION = 4

log = logging.getLogger(__name__)

class Version(BaseVersion):

    def __init__(self, connection):
        super(Version, self).__init__(connection)


    def update(self, force):
        "Add archived field to master_workflow table"
        # TODO We might need to check to see if the field already exists first
        try:
            self.db.execute("ALTER TABLE master_workflow ADD archived BOOLEAN NOT NULL default 0")
        except OperationalError:
            pass
        except Exception, e:
            log.error("Error adding archived field to master_workflow table")
            log.exception(e)
            raise RuntimeError(e)


    def downgrade(self, force):
        "Downgrade is not necessary as archived is added with a default that works for old versions"
        pass


    def is_compatible(self):
        try:
            self.db.execute("SELECT archived FROM master_workflow")
        except Exception, e:
            return False
        return True

