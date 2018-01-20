__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 1

import logging

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *
from sqlalchemy.exc import *

log = logging.getLogger(__name__)


class Version(BaseVersion):
    def __init__(self, connection):
        super(Version, self).__init__(connection)

    def update(self, force=False):
        log.info("Updating to version %s" % DB_VERSION)
        try:
            self.db.execute("SELECT site FROM rc_lfn LIMIT 0,1")
            return
        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)

        try:
            log.debug("  Updating database schema...")
            log.debug("    Creating new table...")
            self.db.execute("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
            self.db.execute(
                "ALTER TABLE rc_lfn_new ADD COLUMN site VARCHAR(245)"
            )
            log.debug("    Removing index...")
            self.db.execute("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
            log.debug("    Adding new constraint...")
            self.db.execute(
                "ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)"
            )
            log.debug("    Copying data...")
            self.db.execute(
                "INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT * FROM rc_lfn"
            )

            log.debug("    Renaming table...")
            if self.db.get_bind().driver == "mysqldb":
                self.db.execute(
                    "RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn"
                )
            else:
                self.db.execute("ALTER TABLE rc_lfn RENAME TO rc_lfn_old")
                self.db.execute("ALTER TABLE rc_lfn_new RENAME TO rc_lfn")

            log.debug("    Droping old table...")
            self.db.execute("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
            self.db.execute("DROP TABLE rc_lfn_old")
            log.debug("  Data schema successfully updated.")

            log.debug("  Migrating attribute data...")
            self.db.execute(
                "UPDATE rc_lfn l INNER JOIN rc_attr a ON (l.id=a.id AND a.name='pool') SET l.site=a.value"
            )
            log.debug("  Migration successfully completed.")

            log.debug("  Cleaning the database...")
            self.db.execute("DELETE FROM rc_attr WHERE name='pool'")
            log.debug("  Database successfully cleaned.")

            log.debug("  Adding new foreign constraint")
            self.db.execute(
                "ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)"
            )
            log.debug("  Foreign constraint successfully added.")

            log.debug("  Validating the update process...")
            data = self.db.execute(
                "SELECT COUNT(id) FROM rc_attr WHERE name='pool'"
            ).first()
            if data is not None:
                if data[0] > 0:
                    self.db.rollback()
                    raise DBAdminError(
                        "attribute pool failed to be removed. There are still %d entries."
                        % data[0]
                    )

            updated = self.db.execute(
                "SELECT COUNT(id) FROM rc_lfn WHERE site IS NOT NULL"
            ).first()
            log.debug("  Updated %d entries in the database." % updated)
            self.db.commit()

        except (OperationalError, ProgrammingError):
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)

    def downgrade(self, force=False):
        try:
            data = self.db.execute(
                "SELECT COUNT(id) AS c FROM rc_Lfn GROUP BY lfn, pfn ORDER BY c DESC LIMIT 1"
            ).first()
            if data is not None:
                count = int(data[0])
                if count > 1 and not force:
                    raise DBAdminError(
                        "A possible data loss was detected: use '--force' to ignore this message."
                    )

            log.debug("  Updating database schema...")
            log.debug("    Creating new table...")
            self.db.execute("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
            self.db.execute("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
            self.db.execute("ALTER TABLE rc_lfn_new DROP COLUMN site")
            log.debug("    Adding new constraint...")
            self.db.execute(
                "ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn)"
            )
            log.debug("    Copying data...")
            self.db.execute(
                "INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT MAX(id), lfn, pfn FROM rc_lfn GROUP BY lfn, pfn"
            )
            self.db.execute(
                "INSERT INTO rc_attr(id, name, value) SELECT n.id, 'pool', site FROM rc_lfn o INNER JOIN rc_lfn_new n ON (n.id = o.id)"
            )
            self.db.execute(
                "DELETE FROM rc_attr WHERE NOT EXISTS (SELECT id FROM rc_lfn_new WHERE rc_lfn_new.id = rc_attr.id)"
            )
            log.debug("    Renaming table...")
            self.db.execute(
                "RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn"
            )
            log.debug("    Droping old table...")
            self.db.execute("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
            self.db.execute("DROP TABLE rc_lfn_old")
            self.db.execute(
                "ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)"
            )
            log.debug("  Data schema successfully updated.")
            self.db.commit()
        except OperationalError:
            pass
        except Exception as e:
            self.db.rollback()
            raise DBAdminError(e)
