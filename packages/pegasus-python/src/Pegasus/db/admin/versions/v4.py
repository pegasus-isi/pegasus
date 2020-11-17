import logging

from sqlalchemy.exc import *

from Pegasus.db.admin.versions.base_version import BaseVersion

DB_VERSION = 4

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        "Add archived field to master_workflow table"
        log.info("Updating to version %s" % DB_VERSION)
        # TODO We might need to check to see if the field already exists first
        try:
            self.db.execute(
                "ALTER TABLE master_workflow ADD archived BOOLEAN NOT NULL default 0"
            )
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            log.error("Error adding archived field to master_workflow table")
            log.exception(e)
            raise RuntimeError(e)

    def downgrade(self, force=False):
        "Downgrade is not necessary as archived is added with a default that works for old versions"
