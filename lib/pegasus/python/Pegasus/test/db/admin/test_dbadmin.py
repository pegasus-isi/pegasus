import os
import unittest

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *

class TestDBAdmin(unittest.TestCase):
    
    def test_create_database(self):
        dburi = db_get_uri(dburi="sqlite:////tmp/test.db")
        db = connection.connect(dburi, create=True)
        self.assertTrue(db_verify(db))
        os.remove("/tmp/test.db")
        
    def test_version_operations(self):
        dburi = db_get_uri(dburi="sqlite:////tmp/test2.db")
        db = connection.connect(dburi, create=True)

        db_downgrade(db, "4.4.2")
        self.assertEquals(db_current_version(db), 2)
        self.assertFalse(db_verify(db))
        
        db_downgrade(db)
        self.assertEquals(db_current_version(db), 1)
        self.assertFalse(db_verify(db))
                
        db_update(db, "4.4.0")
        self.assertEquals(db_current_version(db), 2)
        self.assertFalse(db_verify(db))
        
        db_update(db)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        self.assertTrue(db_verify(db))
        os.remove("/tmp/test2.db")
        
    def test_minimum_downgrade(self):
        dburi = db_get_uri(dburi="sqlite:////tmp/test3.db")
        db = connection.connect(dburi, create=True)

        db_downgrade(db, "4.3.0")
        self.assertEquals(db_current_version(db), 1)
        
        db_downgrade(db)
        self.assertEquals(db_current_version(db), 1)       
        os.remove("/tmp/test3.db")
        
    def test_all_downgrade_update(self):
        dburi = db_get_uri(dburi="sqlite:////tmp/test-all.db")
        db = connection.connect(dburi, create=True)

        db_downgrade(db, "4.3.0")
        self.assertEquals(db_current_version(db), 1)
        self.assertFalse(db_verify(db))
        
        db_update(db)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        os.remove("/tmp/test-all.db")
        self.assertTrue(db_verify(db))
        
    def test_jdbc_sqlite(self):
        dburi = db_get_uri(dburi="jdbc:sqlite:test-sqlite.db")
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        os.remove("test-sqlite.db")
        
        dburi = db_get_uri(dburi="jdbc:sqlite:/tmp/test-sqlite.db")
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        os.remove("/tmp/test-sqlite.db")
        
        dburi = db_get_uri(dburi="jdbc:sqlite://tmp/test-sqlite.db")
        db = connection.connect(dburi, create=True)
        self.assertEquals(db_current_version(db), CURRENT_DB_VERSION)
        os.remove("/tmp/test-sqlite.db")        

if __name__ == '__main__':
    unittest.main()
