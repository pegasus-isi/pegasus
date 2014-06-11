#!/usr/bin/env python

import psycopg2
import sys

user = 'root' 
database = 'pegasus'

try:
    con = psycopg2.connect(database, user);
    cur = con.cursor()
    
    con.query("ALTER TABLE rc_lfn ADD COLUMN site VARCHAR(245) NOT NULL")
    cur.execute("ALTER TABLE rc_lfn DROP INDEX sk_rc_lfn")
    cur.execute("ALTER TABLE rc_lfn ADD CONSTRAINT sk_rc_lfn UNIQUE(lfn,pfn,site)")
    
    cur.execute("SELECT id, value FROM rc_attr WHERE name = 'pool'")
    rows = cur.fetchall()

    for row in rows:
        cur.execute("UPDATE rc_lfn SET site='%s' WHERE id=%d" % (row[1], row[0]))
        cur.execute("DELETE FROM rc_attr WHERE id=%d AND name='pool' AND value='%s'" % (row[0],row[1]))        
        
    con.commit()



except psycopg2.DatabaseError, e:
  
    print 'Error %s' % e
    sys.exit(1)
    
finally:    
        
    if con:    
        con.close()