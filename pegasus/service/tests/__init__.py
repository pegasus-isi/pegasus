import unittest
import tempfile
import shutil
import base64

from flask import json

from pegasus.service import app, db, migrations, users

class TestCase(unittest.TestCase):

    def setUp(self):
        app.config.update(DEBUG=True)

        # Create a temp dir to store data files
        self.tmpdir = tempfile.mkdtemp()
        app.config.update(STORAGE_DIR=self.tmpdir)

        self.app = app.test_client()

    def tearDown(self):
        # Remove the temp dir
        shutil.rmtree(self.tmpdir)

class DBTestCase(TestCase):
    "This test case is for tests that require the database"

    def setUp(self):
        TestCase.setUp(self)
        # Tests use an in-memory database
        app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://")
        migrations.create()

    def tearDown(self):
        db.session.remove()
        migrations.drop()
        TestCase.tearDown(self)

class UserTestCase(DBTestCase):
    "This test case has a user scott with password tiger"

    def setUp(self):
        DBTestCase.setUp(self)

        # Create a test user
        self.username = "scott"
        self.password = "tiger"
        self.email = "scott@isi.edu"
        users.create(username=self.username, password=self.password, email=self.email)
        db.session.commit()

        # Patch the Flask/Werkzeug open to support required features
        orig_open = self.app.open
        def myopen(*args, **kwargs):
            headers = kwargs.get("headers", [])

            # Support basic authentication
            if kwargs.get("auth", True):
                userpass = self.username + ":" + self.password
                uphash = base64.b64encode(userpass)
                headers.append(("Authorization", "Basic %s" % uphash))
                kwargs.update(headers=headers)

            if "auth" in kwargs:
                del kwargs["auth"]

            r = orig_open(*args, **kwargs)

            # If the response is json, parse it
            r.json = None
            if r.content_type == "application/json":
                r.json = json.loads(r.data)

            return r

        self.app.open = myopen

        self.get = self.app.get
        self.post = self.app.post
        self.delete = self.app.delete
        self.put = self.app.put

