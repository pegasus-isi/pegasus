__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 1

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *


class Version(BaseVersion):
        
    def __init__(self, connections, database_name=None, verbose=False, debug=False):
        super(Version, self).__init__(connections, database_name, verbose, debug)

        
    def update(self, force):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            db.begin()
            self.verbose("  Updating database schema...\n")
            db.execute("UPDATE pegasus_schema SET version='1.3' WHERE name='JDBCRC' AND catalog='rc'")
            self.verbose("    Creating new table...\n")
            db.execute("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
            db.execute("ALTER TABLE rc_lfn_new ADD COLUMN site VARCHAR(245)")
            self.verbose("    Removing index...\n")
            db.execute("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
            self.verbose("    Adding new constraint...\n")
            db.execute("ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)")
            self.verbose("    Copying data...\n")
            db.execute("INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT * FROM rc_lfn")
            self.verbose("    Renaming table...\n")
            db.execute("RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn")
            self.verbose("    Droping old table...\n")
            db.execute("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
            db.execute("DROP TABLE rc_lfn_old")
            self.verbose("  Data schema successfully updated.\n")

            self.verbose("  Migrating attribute data...\n")
            db.execute("UPDATE rc_lfn l INNER JOIN rc_attr a ON (l.id=a.id AND a.name='pool') SET l.site=a.value")
            self.verbose("  Migration successfully completed.\n")

            self.verbose("  Cleaning the database...\n")  
            db.execute("DELETE FROM rc_attr WHERE name='pool'")
            self.verbose("  Database successfully cleaned.\n")

            self.verbose("  Adding new foreign constraint\n")
            db.execute("ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)")
            self.verbose("  Foreign constraint successfully added.\n")

            self.verbose("  Validating the update process...\n")
            data = db.execute("SELECT COUNT(id) FROM rc_attr WHERE name='pool'").first()
            if data is not None:
                if data[0] > 0:
                    sys.stderr.write("  Error: attribute pool failed to be removed. There are still %d entries." % data[0])
                    db.rollback()
                    sys.exit(1)

            updated = db.execute("SELECT COUNT(id) FROM rc_lfn WHERE site IS NOT NULL").first()
            self.verbose("  Updated %d entries in the database.\n" % updated)
            db.commit()


    def downgrade(self, force):
        if self.connections[self.database_name]:
            db = self.connections[self.database_name].session
            db.begin()

            data = db.execute("SELECT COUNT(id) AS c FROM rc_Lfn GROUP BY lfn, pfn ORDER BY c DESC LIMIT 1").first()
            if data is not None:
                count = int(data[0])
                if count > 1 and not force:
                    sys.stderr.write("ERROR: A possible data loss was detected: use '--force' to ignore this message.\n")
                    exit(1)

            self.verbose("  Updating database schema...\n")
            db.execute("UPDATE pegasus_schema SET version='1.2' WHERE name='JDBCRC' AND catalog='rc'")

            self.verbose("    Creating new table...\n")
            db.execute("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
            db.execute("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
            db.execute("ALTER TABLE rc_lfn_new DROP COLUMN site")
            self.verbose("    Adding new constraint...\n")
            db.execute("ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn)")
            self.verbose("    Copying data...\n")
            db.execute("INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT MAX(id), lfn, pfn FROM rc_lfn GROUP BY lfn, pfn")
            db.execute("INSERT INTO rc_attr(id, name, value) SELECT n.id, 'pool', site FROM rc_lfn o INNER JOIN rc_lfn_new n ON (n.id = o.id)")
            db.execute("DELETE FROM rc_attr WHERE NOT EXISTS (SELECT id FROM rc_lfn_new WHERE rc_lfn_new.id = rc_attr.id)")
            self.verbose("    Renaming table...\n")
            db.execute("RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn")
            self.verbose("    Droping old table...\n")
            db.execute("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
            db.execute("DROP TABLE rc_lfn_old")
            db.execute("ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)")
            self.verbose("  Data schema successfully updated.\n")


    def is_compatible(self):
        if self.connections[self.database_name]:  
            try:
                self.connections[self.database_name].db.execute("SELECT site FROM rc_lfn")
            except:
                return False
        return True
