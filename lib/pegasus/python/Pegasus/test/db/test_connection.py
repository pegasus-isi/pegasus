import errno
import os
import unittest

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *

class TestConnection(unittest.TestCase):

    def test_non_existent_url(self):
        dburi = "jdbc:mysql://localhost/unknown-db"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)
        
        dburi = "jdbc:mysql://root@localhost/unknown-db"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)
        
        dburi = "jdbc:mysql://localhost:1111/unknown-db"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)
                
        dburi = "sqlite:test-url.db"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)

        dburi = "test.db"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)
        
        dburi = "jdbc:invalid://localhost/testdb"
        self.assertRaises(connection.ConnectionError, connection.connect, dburi)

    def test_jdbc_sqlite(self):
        filename = "test-sqlite.db"
        self._silentremove(filename)
        dburi = "jdbc:sqlite:%s" % filename
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        os.remove(filename)
        
        filename = "/tmp/test-sqlite.db"
        self._silentremove(filename)
        dburi = "jdbc:sqlite:%s" % filename
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        os.remove(filename)
        
        self._silentremove(filename)
        dburi = "jdbc:sqlite:/%s" % filename
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        os.remove(filename)
    
    def test_connection_by_uri(self):
        filename = "/tmp/connect-1.db"
        self._silentremove(filename)
        dburi = "sqlite:///%s" % filename
        db = connection.connect(dburi, echo=False, schema_check=True, create=True)
        db.close()
        os.remove(filename)
        
    
    def _silentremove(self, filename):
        try:
            os.remove(filename)
        except OSError, e:
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occured

    
if __name__ == '__main__':
    unittest.main()
