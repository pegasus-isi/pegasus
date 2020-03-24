import base64
import logging
import os
import shutil
import sys
import tempfile
import threading
import unittest

from flask import json
from six import StringIO
from werkzeug.serving import make_server

from Pegasus.service import app


class TestCase(unittest.TestCase):
    def setUp(self):
        # We want our test cases quiet
        logging.basicConfig(level=logging.ERROR)

        app.config.update(DEBUG=True)

        # Create a temp dir to store data files
        self.tmpdir = tempfile.mkdtemp()
        app.config.update(STORAGE_DIR=self.tmpdir)

    def tearDown(self):
        # Remove the temp dir
        if os.path.isdir(self.tmpdir):
            shutil.rmtree(self.tmpdir)


class DBTestCase(TestCase):
    "This test case is for tests that require the database"

    def setUp(self):
        TestCase.setUp(self)
        self.dbfile = os.path.join(self.tmpdir, "workflow.db")
        self.dburi = "sqlite:///%s" % self.dbfile
        app.config.update(SQLALCHEMY_DATABASE_URI=self.dburi)
        migrations.create()

    def tearDown(self):
        db.session.remove()
        migrations.drop()
        os.remove(self.dbfile)
        TestCase.tearDown(self)


class UserTestCase(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        self.username = "scott"
        self.password = "tiger"

        app.config.update(AUTHENTICATION="NoAuthentication")


class APITestCase(UserTestCase):
    def setUp(self):
        UserTestCase.setUp(self)

        self.app = app.test_client()

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


class TestWSGIServer(threading.Thread):
    def __init__(self, *args, **kwargs):
        self.host = kwargs.pop("host")
        self.port = kwargs.pop("port")
        threading.Thread.__init__(self, *args, **kwargs)
        self.server = make_server(self.host, self.port, app=app)

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()
        self.server.server_close()
        self.join()


class ClientTestCase(APITestCase):
    def setUp(self):
        APITestCase.setUp(self)
        self.host = "127.0.0.1"
        self.port = 4999
        app.config["ENDPOINT"] = "http://{}:{}/".format(self.host, self.port)
        app.config["USERNAME"] = self.username
        app.config["PASSWORD"] = self.password
        self.server = TestWSGIServer(host=self.host, port=self.port)
        self.server.start()

        self.oldstdout = sys.stdout
        sys.stdout = StringIO()
        self.oldstderr = sys.stderr
        sys.stderr = StringIO()

    def stdio(self):
        stdout = sys.stdout.getvalue()
        sys.stdout.truncate(0)
        stderr = sys.stderr.getvalue()
        sys.stderr.truncate(0)
        return (stdout, stderr)

    def tearDown(self):
        sys.stdout = self.oldstdout
        sys.stderr = self.oldstderr
        self.server.shutdown()
        APITestCase.tearDown(self)


def IntegrationTest(f):
    def wrapper(*args, **kwargs):
        env = os.getenv("ENABLE_INTEGRATION_TESTS", None)
        if env is None:
            sys.stderr.write(" integration tests disabled ")
            return None

        return f(*args, **kwargs)

    return wrapper


def PerformanceTest(f):
    def wrapper(*args, **kwargs):
        env = os.getenv("ENABLE_PERFORMANCE_TESTS", None)
        if env is None:
            sys.stderr.write(" performance tests disabled ")
            return None

        return f(*args, **kwargs)

    return wrapper
