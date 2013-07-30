import unittest

from pegasus.service import app, db, migrations

class TestCase(unittest.TestCase):
    "This test case is for tests that just require the webapp"

    def setUp(self):
        app.config.update(DEBUG=True)
        self.app = app.test_client()

class DBTestCase(TestCase):
    "This test case is for tests that require the database"

    def setUp(self):
        # Tests use an in-memory database
        app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://")
        migrations.create()

    def tearDown(self):
        db.session.remove()
        migrations.drop()

