import os
import unittest

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *

class TestDBAdmin(unittest.TestCase):
    
    def test_create_database(self):
        dburi = "sqlite:////tmp/test.db"
        db = connection.connect(dburi, create=True)
        self.assertTrue(db_verify(db))
        os.remove("/tmp/test.db")
        
    def test_version_operations(self):
        dburi = "sqlite:////tmp/test2.db"
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
        dburi = "sqlite:////tmp/test3.db"
        db = connection.connect(dburi, create=True)

        db_downgrade(db, "4.3.0")
        self.assertEquals(db_current_version(db), 1)
        
        db_downgrade(db)
        self.assertEquals(db_current_version(db), 1)       
        os.remove("/tmp/test3.db")

if __name__ == '__main__':
    unittest.main()
