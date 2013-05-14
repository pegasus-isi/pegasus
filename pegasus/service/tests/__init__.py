import unittest

from pegasus.service import app, db, config

class TestCase(unittest.TestCase):
    "This test case is for tests that just require the webapp"

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

class DBTestCase(TestCase):
    "This test case is for tests that require the database"

    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://' # in-memory database
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()

