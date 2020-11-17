import logging

from Pegasus.db.admin.admin_loader import DBAdminError
from Pegasus.db.admin.versions.base_version import BaseVersion

DB_VERSION = 13

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super().__init__(connection)

    def update(self, force=False):
        """."""
        log.debug("Updating to version %s" % DB_VERSION)
        try:
            self.db.execute("ALTER TABLE invocation ADD COLUMN maxrss INTEGER")
            self.db.execute("ALTER TABLE invocation ADD COLUMN avg_cpu NUMERIC(16, 6)")
        except Exception as e:
            if "uplicate column name" not in str(
                e
            ) and "no such table: invocation" not in str(e):
                self.db.rollback()
                raise DBAdminError(e)

    def downgrade(self, force=False):
        """."""
        log.debug("Downgrading from version %s" % DB_VERSION)
        # no downgrade is necessary
