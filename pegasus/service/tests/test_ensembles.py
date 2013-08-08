from pegasus.service import tests, ensembles, api
from pegasus.service.ensembles import *

class TestEnsembles(tests.TestCase):
    def test_name(self):
        validate_ensemble_name("x"*99)
        validate_ensemble_name("ensemble12")
        validate_ensemble_name("ensemble.12")
        validate_ensemble_name("ensemble_12")
        validate_ensemble_name("ensemble-12")
        self.assertRaises(api.APIError, validate_ensemble_name, "x"*100)
        self.assertRaises(api.APIError, validate_ensemble_name, None)
        self.assertRaises(api.APIError, validate_ensemble_name, "foo/bar/baz")
        self.assertRaises(api.APIError, validate_ensemble_name, "../foo")
        self.assertRaises(api.APIError, validate_ensemble_name, "foo/../foo")

    def test_ensemble_states(self):
        self.assertEquals(EnsembleStates.ACTIVE, "ACTIVE")
        self.assertTrue("ACTIVE" in EnsembleStates)

    def test_priority(self):
        self.assertEquals(validate_priority(10), 10)
        self.assertEquals(validate_priority(10.1), 10)
        self.assertEquals(validate_priority(10.6), 10)
        self.assertEquals(validate_priority("10"), 10)
        self.assertRaises(api.APIError, validate_priority, "a")
        self.assertRaises(api.APIError, validate_priority, "10.1")

class TestEnsembleAPI(tests.APITestCase):
    def test_ensemble_api(self):
        r = self.get("/ensembles")
        self.assertEquals(r.status_code, 200)
        self.assertEquals(len(r.json), 0, "Should not be any ensembles")

        r = self.post("/ensembles")
        self.assertEquals(r.status_code, 400, "Should fail on missing ensemble params")

        r = self.post("/ensembles", data={"name":"myensemble", "priority":"10"})
        self.assertEquals(r.status_code, 201, "Should return created status")
        self.assertTrue("location" in r.headers, "Should have location header")

        r = self.get("/ensembles/myensemble")
        self.assertEquals(r.status_code, 200)
        self.assertEquals(r.json["name"], "myensemble", "Should be named myensemble")
        self.assertEquals(r.json["priority"], 10, "Should have a priority of 10")
        self.assertEquals(r.json["state"], EnsembleStates.ACTIVE, "Should be in active state")
        self.assertEquals(len(r.json["workflows"]), 0, "Should not have any workflows")

        # Need to sleep for one second so that updated gets a different value
        updated = r.json["updated"]
        import time
        time.sleep(1)

        r = self.get("/ensembles")
        self.assertEquals(r.status_code, 200, "Should return 200 OK")
        self.assertEquals(len(r.json), 1, "Should be one ensemble")

        update = {
            "state": EnsembleStates.HELD,
            "priority": "-10",
            "max_running": "10",
            "max_planning": "2"
        }
        r = self.post("/ensembles/myensemble", data=update)
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(r.json["state"], EnsembleStates.HELD, "Should be in held state")
        self.assertEquals(r.json["priority"], -10, "Should have priority -10")
        self.assertEquals(r.json["max_running"], 10, "max_running should be 10")
        self.assertEquals(r.json["max_planning"], 2, "max_planning should be 2")
        self.assertNotEquals(r.json["updated"], updated)

    def test_ensemble_workflow_api(self):
        r = self.post("/ensembles", data={"name": "myensemble"})
        self.assertEquals(r.status_code, 201, "Should return created status")

        r = self.get("/ensembles/myensemble/workflows")
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(len(r.json), 0, "Should have no workflows")

        # Create some test catalogs
        r = self.post("/catalogs/replica", data={"name": "rc", "format": "regex",
            "file": (StringIO("replicas"), "rc.txt")})
        self.assertEquals(r.status_code, 201)
        r = self.post("/catalogs/site", data={"name": "sc", "format": "xml4",
            "file": (StringIO("sites"), "sites.xml")})
        self.assertEquals(r.status_code, 201)
        r = self.post("/catalogs/transformation", data={"name": "tc",
            "format": "text", "file": (StringIO("transformations"), "tc.txt")})
        self.assertEquals(r.status_code, 201)

        # Create a test workflow
        req = {
            "name":"mywf",
            "priority":"10",
            "sites": "sc",
            "transformations": "tc",
            "replicas":"rc",
            "dax": (StringIO("my dax"), "my.dax"),
            "properties": (StringIO("my props"), "pegasus.props")
        }
        r = self.post("/ensembles/myensemble/workflows", data=req)
        self.assertEquals(r.status_code, 201, "Should return CREATED")
        self.assertTrue("location" in r.headers, "Should have location header")

        # Make sure all the files were created
        wfdir = os.path.join(self.tmpdir, "userdata/scott/ensembles/myensemble/workflows/mywf")
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "sites.xml")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "dax.xml")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "rc.txt")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "tc.txt")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "pegasus.properties")))

        r = self.get("/ensembles/myensemble")
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(len(r.json["workflows"]), 1, "Should have 1 workflow")

        r = self.get("/ensembles/myensemble/workflows")
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(len(r.json), 1, "Should have one workflow")

        r = self.get("/ensembles/myensemble/workflows/mywf")
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(r.json["name"], "mywf", "Name should be mywf")
        self.assertEquals(r.json["priority"], 10, "Should have priority 10")
        self.assertEquals(r.json["state"], EnsembleWorkflowStates.READY, "Should have state READY")
        self.assertTrue("dax" in r.json)
        self.assertTrue("properties" in r.json)
        self.assertTrue("sites" in r.json)
        self.assertTrue("replicas" in r.json)

        for f in ["dax","properties","sites","replicas","transformations"]:
            r = self.get("/ensembles/myensemble/workflows/mywf/%s" % f)
            self.assertEquals(r.status_code, 200, "Should return OK")
            self.assertTrue(len(r.data) > 0, "File should not be empty")

        r = self.post("/ensembles/myensemble/workflows/mywf", data={"priority":"100"})
        self.assertEquals(r.status_code, 200, "Should return OK")
        self.assertEquals(r.json["priority"], 100, "Should have priority 100")

class TestEnsembleClient(tests.ClientTestCase):

    def test_ensemble_client(self):
        cmd = ensembles.EnsembleCommand()

        cmd.main(["list"])
        self.assertTrue(len(self.stdout()) == 0, "Should be no stdout")

        cmd.main(["create","-n","foo","-p","10","-P","20","-R","30"])
        self.assertTrue(len(self.stdout()) == 0, "Should be no stdout")

        cmd.main(["list"])
        listing = self.stdout()
        self.assertEquals(len(listing.split("\n")), 3, "Should be two lines of stdout")

        cmd.main(["update","-n","foo","-p","40","-P","50","-R","60"])
        listing = self.stdout()
        self.assertTrue("Name: foo" in listing, "Name should be foo")
        self.assertTrue("Priority: 40" in listing, "Priority should be 40")
        self.assertTrue("Max Planning: 50" in listing, "Max Planning should be 50")
        self.assertTrue("Max Running: 60" in listing, "Max running should be 60")

        cmd.main(["pause","-n","foo"])
        listing = self.stdout()
        self.assertTrue("State: PAUSED" in listing, "State should be paused")

        cmd.main(["hold","-n","foo"])
        listing = self.stdout()
        self.assertTrue("State: HELD" in listing, "State should be held")

        cmd.main(["activate","-n","foo"])
        listing = self.stdout()
        self.assertTrue("State: ACTIVE" in listing, "State should be active")

