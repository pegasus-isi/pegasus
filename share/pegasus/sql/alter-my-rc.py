#!/usr/bin/env python

import MySQLdb as mdb
import sys

mysqlHost = 'localhost'
mysqlUser = 'root' 
mysqlPassword = ''
mysqlDB = 'pegasus'

try:
    con = mdb.connect(mysqlHost, mysqlUser, mysqlPassword, mysqlDB);
    cur = con.cursor()
    
    print "Updating database schema..."
    cur.execute("UPDATE pegasus_schema SET version='1.3' WHERE name='JDBCRC' AND catalog='rc'")
    print "   Adding new column..."
    cur.execute("ALTER TABLE rc_lfn ADD COLUMN site VARCHAR(245)")
    
    print "   Removing index..."
    cur.execute("ALTER TABLE rc_lfn DROP INDEX sk_rc_lfn")
    
    print "   Adding new constraint..."
    cur.execute("ALTER TABLE rc_lfn ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)")
    con.commit()
    print "Data schema successfully updated."
    print ""
    
    print "Migrating attribute data..."
    cur = con.cursor()    
    cur.execute("UPDATE rc_lfn l INNER JOIN rc_attr a ON (l.id=a.id AND a.name='pool') SET l.site=a.value")
    con.commit()
    print "Migration successfully completed."
    print ""
        
    print "Cleaning the database..."
    cur = con.cursor()    
    cur.execute("DELETE FROM rc_attr WHERE name='pool'")
    con.commit()
    print "Database successfully cleaned."
    print ""    
    
    print "Validating the update process..."
    cur = con.cursor()
    cur.execute("SELECT COUNT(id) FROM rc_attr WHERE name='pool'")
    attrs = int( cur.fetchone()[0] )
    
    if attrs > 0:
        print "Error: attribute pool failed to be removed. There are still %d entries." % attrs
        sys.exit(1)
        
    cur.execute("SELECT COUNT(id) FROM rc_lfn WHERE site IS NOT NULL")
    updated = cur.fetchone()
    print "Updated %d entries in the database." % updated

    con.commit()



except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()