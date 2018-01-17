import errno
import os
import re
import unittest
import uuid

from Pegasus.db import connection
from Pegasus.db.admin.admin_loader import *
from Pegasus.db.schema import *

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
        filename = str(uuid.uuid4())
        _silentremove(filename)
        dburi = "jdbc:sqlite:%s" % filename
        db = connection.connect(dburi, create=True, verbose=False)
        self.assertEqual(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        _remove(filename)
        
        filename = "/tmp/" + str(uuid.uuid4())
        _silentremove(filename)
        dburi = "jdbc:sqlite:%s" % filename
        db = connection.connect(dburi, create=True, verbose=False)
        self.assertEqual(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        _remove(filename)

        dburi = "jdbc:sqlite:/%s" % filename
        db = connection.connect(dburi, create=True, verbose=False)
        self.assertEqual(db_current_version(db), CURRENT_DB_VERSION)
        db.close()
        _remove(filename)

    def test_connection_by_uri(self):
        filename = str(uuid.uuid4())
        _silentremove(filename)
        dburi = "sqlite:///%s" % filename
        db = connection.connect(dburi, echo=False, schema_check=True, create=True, verbose=False)
        db.close()
        _remove(filename)


def _silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured


def _remove(filename):
    for f in os.listdir("."):
        if re.search(filename + ".*", f):
            os.remove(f)
    _silentremove(filename)


if __name__ == '__main__':
    unittest.main()
