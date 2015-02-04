__author__ = "Rafael Ferreira da Silva"

DB_VERSION = 1.1

from Pegasus.db.admin.admin_loader import *
from Pegasus.db.admin.versions.base_version import *


class Version(BaseVersion):
        
    def __init__(self, config_properties, verbose=False, debug=False):
        super(Version, self).__init__(config_properties, verbose, debug)
        
        self.db = JDBCRC(config_properties, verbose, debug)
                
# -------------------------------------------------------------------
    def update(self, force):
                
        replica_catalog = self.get_property('pegasus.catalog.replica')
        if replica_catalog != "JDBCRC":
            return
        
        self.verbose("  Updating database schema...\n")
        self.db.execute_update("UPDATE pegasus_schema SET version='1.3' WHERE name='JDBCRC' AND catalog='rc'")
        self.verbose("    Creating new table...\n")
        self.db.execute_update("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
        self.db.execute_update("ALTER TABLE rc_lfn_new ADD COLUMN site VARCHAR(245)")
        self.verbose("    Removing index...\n")
        self.db.execute_update("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
        self.verbose("    Adding new constraint...\n")
        self.db.execute_update("ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)")
        self.verbose("    Copying data...\n")
        self.db.execute_update("INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT * FROM rc_lfn")
        self.verbose("    Renaming table...\n")
        self.db.execute_update("RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn")
        self.verbose("    Droping old table...\n")
        self.db.execute_update("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
        self.db.execute_update("DROP TABLE rc_lfn_old")
        self.verbose("  Data schema successfully updated.\n")
        
        self.verbose("  Migrating attribute data...\n")
        self.db.execute_update("UPDATE rc_lfn l INNER JOIN rc_attr a ON (l.id=a.id AND a.name='pool') SET l.site=a.value")
        self.verbose("  Migration successfully completed.\n")
        
        self.verbose("  Cleaning the database...\n")  
        self.db.execute_update("DELETE FROM rc_attr WHERE name='pool'")
        self.verbose("  Database successfully cleaned.\n")

        self.verbose("  Adding new foreign constraint\n")
        self.db.execute_update("ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)")
        self.verbose("  Foreign constraint successfully added.\n")

        self.verbose("  Validating the update process...\n")
        cur = self.db.get_connection().cursor()
        cur.execute("SELECT COUNT(id) FROM rc_attr WHERE name='pool'")
        data = cur.fetchone()
        if data is not None:
            attrs = int(data[0])
            if attrs > 0:
                sys.stderr.write("  Error: attribute pool failed to be removed. There are still %d entries." % attrs)
                self.db.get_connection().rollback()
                sys.exit(1)

        cur.execute("SELECT COUNT(id) FROM rc_lfn WHERE site IS NOT NULL")
        updated = cur.fetchone()
        self.verbose("  Updated %d entries in the database.\n" % updated)

# -------------------------------------------------------------------
    def downgrade(self, force):
        
        replica_catalog = self.get_property('pegasus.catalog.replica')
        if replica_catalog != "JDBCRC":
            return
                
        cur = self.db.get_connection().cursor()
        cur.execute("SELECT COUNT(id) AS c FROM rc_Lfn GROUP BY lfn, pfn ORDER BY c DESC LIMIT 1")
        data = cur.fetchone()
        if data is not None:
            count = int(data[0])
            if count > 1 and not force:
                sys.stderr.write("ERROR: A possible data loss was detected: use '--force' to ignore this message.\n")
                exit(1)
        
        self.verbose("  Updating database schema...\n")
        self.db.execute_update("UPDATE pegasus_schema SET version='1.2' WHERE name='JDBCRC' AND catalog='rc'")
        
        self.verbose("    Creating new table...\n")
        self.db.execute_update("CREATE TABLE rc_lfn_new ( LIKE rc_lfn )")
        self.db.execute_update("ALTER TABLE rc_lfn_new DROP INDEX sk_rc_lfn")
        self.db.execute_update("ALTER TABLE rc_lfn_new DROP COLUMN site")
        self.verbose("    Adding new constraint...\n")
        self.db.execute_update("ALTER TABLE rc_lfn_new ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn)")
        self.verbose("    Copying data...\n")
        self.db.execute_update("INSERT INTO rc_lfn_new(id, lfn, pfn) SELECT MAX(id), lfn, pfn FROM rc_lfn GROUP BY lfn, pfn")
        self.db.execute_update("INSERT INTO rc_attr(id, name, value) SELECT n.id, 'pool', site FROM rc_lfn o INNER JOIN rc_lfn_new n ON (n.id = o.id)")
        self.db.execute_update("DELETE FROM rc_attr WHERE NOT EXISTS (SELECT id FROM rc_lfn_new WHERE rc_lfn_new.id = rc_attr.id)")
        self.verbose("    Renaming table...\n")
        self.db.execute_update("RENAME TABLE rc_lfn TO rc_lfn_old, rc_lfn_new TO rc_lfn")
        self.verbose("    Droping old table...\n")
        self.db.execute_update("ALTER TABLE rc_attr DROP FOREIGN KEY fk_rc_attr")
        self.db.execute_update("DROP TABLE rc_lfn_old")
        self.db.execute_update("ALTER TABLE rc_attr ADD CONSTRAINT fk_rc_attr FOREIGN KEY (id) REFERENCES rc_lfn(id)")
        self.verbose("  Data schema successfully updated.\n")

# -------------------------------------------------------------------
    def is_compatible(self):
        replica_catalog = self.get_property('pegasus.catalog.replica')
        if replica_catalog != "JDBCRC":
            return True
        try:
            cur = self.db.get_connection().cursor()
            cur.execute("SELECT site FROM rc_lfn")
        except:
            return False
        return True

# -------------------------------------------------------------------
    def dispose(self):
        self.db.close()
