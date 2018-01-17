import time
from Pegasus.service import ensembles, api, db
from Pegasus.service.ensembles import *
from Pegasus.test.service import *

class TestEnsembles(TestCase):
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
        self.assertEqual(EnsembleStates.ACTIVE, "ACTIVE")
        self.assertTrue("ACTIVE" in EnsembleStates)

    def test_change_state(self):
        w = EnsembleWorkflow(1, "foo")

        # From PLAN_FAILED we can only go to READY, not QUEUED
        w.set_state(EnsembleWorkflowStates.PLAN_FAILED)
        self.assertRaises(api.APIError, w.change_state, EnsembleWorkflowStates.QUEUED)
        w.change_state(EnsembleWorkflowStates.READY)

        # From RUN_FAILED we can to to READY or QUEUED
        w.set_state(EnsembleWorkflowStates.RUN_FAILED)
        self.assertRaises(api.APIError, w.change_state, EnsembleWorkflowStates.PLANNING)
        w.change_state(EnsembleWorkflowStates.READY)
        w.set_state(EnsembleWorkflowStates.RUN_FAILED)
        w.change_state(EnsembleWorkflowStates.QUEUED)

        # From FAILED we can go to READY or QUEUED
        w.set_state(EnsembleWorkflowStates.FAILED)
        self.assertRaises(api.APIError, w.change_state, EnsembleWorkflowStates.PLANNING)
        w.change_state(EnsembleWorkflowStates.READY)
        w.set_state(EnsembleWorkflowStates.FAILED)
        w.change_state(EnsembleWorkflowStates.QUEUED)

    def test_priority(self):
        self.assertEqual(validate_priority(10), 10)
        self.assertEqual(validate_priority(10.1), 10)
        self.assertEqual(validate_priority(10.6), 10)
        self.assertEqual(validate_priority("10"), 10)
        self.assertRaises(api.APIError, validate_priority, "a")
        self.assertRaises(api.APIError, validate_priority, "10.1")

    def test_write_planning_script(self):
        f = StringIO()
        write_planning_script(f, tcformat="tc", rcformat="rc", scformat="sc",
                              sites=["local"], output_site="local",
                              staging_sites={"a":"b", "c":"d"},
                              clustering=["horizontal","vertical"],
                              force=True, cleanup=False)
        script = f.getvalue()
        self.assertTrue("#!/bin/bash" in script)
        self.assertTrue("pegasus-plan" in script)
        self.assertTrue("-Dpegasus.catalog.site=sc" in script)
        self.assertTrue("-Dpegasus.catalog.site.file=sites.xml" in script)
        self.assertTrue("-Dpegasus.catalog.transformation=tc" in script)
        self.assertTrue("-Dpegasus.catalog.transformation.file=tc.txt" in script)
        self.assertTrue("-Dpegasus.catalog.replica=rc" in script)
        self.assertTrue("-Dpegasus.catalog.replica.file=rc.txt" in script)
        self.assertTrue("--conf pegasus.properties" in script)
        self.assertTrue("--site local" in script)
        self.assertTrue("--output-site local" in script)
        self.assertTrue("--staging-site a=b,c=d" in script)
        self.assertTrue("--cluster horizontal,vertical" in script)
        self.assertTrue("--force" in script)
        self.assertTrue("--nocleanup" in script)
        self.assertTrue("--dir submit" in script)
        self.assertTrue("--dax dax.xml" in script)

        f = StringIO()
        write_planning_script(f, tcformat="tc", rcformat="rc", scformat="sc",
                              sites=["local"], output_site="local")
        script = f.getvalue()
        self.assertFalse("--staging-site" in script)
        self.assertFalse("--cluster" in script)
        self.assertFalse("--force" in script)
        self.assertFalse("--nocleanup" in script)

        f = StringIO()
        write_planning_script(f, tcformat="tc", rcformat="rc", scformat="sc",
                              sites=["local"], output_site="local",
                              staging_sites={"a":"b"},
                              clustering=["horiz"], force=False,
                              cleanup=True)
        script = f.getvalue()
        self.assertTrue("--staging-site a=b " in script)
        self.assertTrue("--cluster horiz" in script)
        self.assertFalse("--force" in script)
        self.assertFalse("--nocleanup" in script)

class TestEnsembleDB(UserTestCase):
    def test_ensemble_db(self):
        self.assertEqual(len(ensembles.list_ensembles(self.username)), 0, "Should be no ensembles")
        e = ensembles.create_ensemble(self.username, "foo", 1, 1)
        self.assertEqual(len(ensembles.list_ensembles(self.username)), 1, "Should be 1 ensemble")


        self.assertEqual(len(ensembles.list_actionable_ensembles()), 0, "Should be 0 actionable ensembles")

        w = ensembles.EnsembleWorkflow(e.id, "bar")
        db.session.add(w)
        db.session.flush()

        self.assertEqual(len(ensembles.list_actionable_ensembles()), 1, "Should be 1 actionable ensembles")

class TestEnsembleAPI(APITestCase):
    def test_ensemble_api(self):
        r = self.get("/ensembles")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json), 0, "Should not be any ensembles")

        r = self.post("/ensembles")
        self.assertEqual(r.status_code, 400, "Should fail on missing ensemble params")

        r = self.post("/ensembles", data={"name":"myensemble"})
        self.assertEqual(r.status_code, 201, "Should return created status")
        self.assertTrue("location" in r.headers, "Should have location header")

        r = self.get("/ensembles/myensemble")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json["name"], "myensemble", "Should be named myensemble")
        self.assertEqual(r.json["state"], EnsembleStates.ACTIVE, "Should be in active state")

        # Need to sleep for one second so that updated gets a different value
        updated = r.json["updated"]
        time.sleep(1)

        r = self.get("/ensembles")
        self.assertEqual(r.status_code, 200, "Should return 200 OK")
        self.assertEqual(len(r.json), 1, "Should be one ensemble")

        update = {
            "state": EnsembleStates.HELD,
            "max_running": "10",
            "max_planning": "2"
        }
        r = self.post("/ensembles/myensemble", data=update)
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["state"], EnsembleStates.HELD, "Should be in held state")
        self.assertEqual(r.json["max_running"], 10, "max_running should be 10")
        self.assertEqual(r.json["max_planning"], 2, "max_planning should be 2")
        self.assertNotEqual(r.json["updated"], updated)

    def test_ensemble_workflow_api(self):
        r = self.post("/ensembles", data={"name": "myensemble"})
        self.assertEqual(r.status_code, 201, "Should return created status")

        r = self.get("/ensembles/myensemble/workflows")
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(len(r.json), 0, "Should have no workflows")

        # Create some test catalogs
        catalogs.save_catalog("replica", self.username, "rc", "regex", StringIO("replicas"))
        catalogs.save_catalog("site", self.username, "sc", "xml", StringIO("sites"))
        catalogs.save_catalog("transformation", self.username, "tc", "text", StringIO("transformations"))
        db.session.commit()

        # Create a test workflow
        req = {
            "name":"mywf",
            "priority":"10",
            "site_catalog": "sc",
            "transformation_catalog": "tc",
            "replica_catalog":"rc",
            "dax": (StringIO("my dax"), "my.dax"),
            "conf": (StringIO("my props"), "pegasus.properties"),
            "sites": "local, remote, ",
            "output_site": "local",
            "clustering": "horizontal,  vertical,",
            "force": True,
            "cleanup": False,
            "staging_sites": "a=b, c=d,"
        }
        r = self.post("/ensembles/myensemble/workflows", data=req)
        self.assertEqual(r.status_code, 201, "Should return CREATED")
        self.assertTrue("location" in r.headers, "Should have location header")

        # Make sure all the files were created
        wfdir = os.path.join(self.tmpdir, "userdata/scott/ensembles/myensemble/workflows/mywf")
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "sites.xml")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "dax.xml")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "rc.txt")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "tc.txt")))
        self.assertTrue(os.path.isfile(os.path.join(wfdir, "pegasus.properties")))
        planfile = os.path.join(wfdir, "plan.sh")
        self.assertTrue(os.path.isfile(planfile))

        planscript = open(planfile).read()
        self.assertTrue("--nocleanup" in planscript)
        self.assertTrue("--force" in planscript)
        self.assertTrue("--cluster horizontal,vertical" in planscript)
        self.assertTrue("--output-site local" in planscript)
        self.assertTrue("--site local,remote" in planscript)
        self.assertTrue("--staging-site a=b,c=d" in planscript)

        r = self.get("/ensembles/myensemble/workflows")
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(len(r.json), 1, "Should have one workflow")

        r = self.get("/ensembles/myensemble/workflows/mywf")
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["name"], "mywf", "Name should be mywf")
        self.assertEqual(r.json["priority"], 10, "Should have priority 10")
        self.assertEqual(r.json["state"], EnsembleWorkflowStates.READY, "Should have state READY")
        self.assertTrue("wf_uuid" in r.json)
        self.assertTrue("dax" in r.json)
        self.assertTrue("conf" in r.json)
        self.assertTrue("sites" in r.json)
        self.assertTrue("replicas" in r.json)
        self.assertTrue("plan_script" in r.json)

        for f in ["dax.xml","pegasus.properties","sites.xml","rc.txt","tc.txt","plan.sh"]:
            r = self.get("/ensembles/myensemble/workflows/mywf/%s" % f)
            self.assertEqual(r.status_code, 200, "Should return OK")
            self.assertTrue(len(r.data) > 0, "File should not be empty")

        r = self.post("/ensembles/myensemble/workflows/mywf", data={"priority":"100"})
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["priority"], 100, "Should have priority 100")

        e = get_ensemble(self.username, "myensemble")

        ew = get_ensemble_workflow(e.id, "mywf")
        ew.set_state(EnsembleWorkflowStates.PLAN_FAILED)
        db.session.commit()

        r = self.post("/ensembles/myensemble/workflows/mywf", data={"state":EnsembleWorkflowStates.READY})
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["state"], EnsembleWorkflowStates.READY, "Should be in READY state")

        ew = get_ensemble_workflow(e.id, "mywf")
        ew.set_state(EnsembleWorkflowStates.RUN_FAILED)
        db.session.commit()

        r = self.post("/ensembles/myensemble/workflows/mywf", data={"state":EnsembleWorkflowStates.READY})
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["state"], EnsembleWorkflowStates.READY, "Should be in READY state")

        ew = get_ensemble_workflow(e.id, "mywf")
        ew.set_state(EnsembleWorkflowStates.RUN_FAILED)
        db.session.commit()

        r = self.post("/ensembles/myensemble/workflows/mywf", data={"state":EnsembleWorkflowStates.QUEUED})
        self.assertEqual(r.status_code, 200, "Should return OK")
        self.assertEqual(r.json["state"], EnsembleWorkflowStates.QUEUED, "Should be in QUEUED state")

class LargeDAXTest(ClientTestCase):

    @PerformanceTest
    def test_large_dax(self):
        r = self.post("/ensembles", data={"name": "myensemble"})
        self.assertEqual(r.status_code, 201, "Should return created status")

        # Create some test catalogs
        catalogs.save_catalog("replica", self.username, "rc", "regex", StringIO("replicas"))
        catalogs.save_catalog("site", self.username, "sc", "xml", StringIO("sites"))
        catalogs.save_catalog("transformation", self.username, "tc", "text", StringIO("transformations"))
        db.session.commit()

        # Create a 256MB file and submit it as the DAX
        daxfile = os.path.join(self.tmpdir, "large.dax")
        dax = open(daxfile, "w")
        for i in range(0, 256*1024):
            dax.write("x" * 1024)
        dax.close()

        cmd = ensembles.EnsembleCommand()

        start = time.time()
        cmd.main(["submit","-e","myensemble","-w","mywf","-R","rc","-S","sc","-T","tc","-d",daxfile,"-s","local","-o","local"])
        end = time.time()
        stdout, stderr = self.stdio()
        self.assertEqual(stdout, "", "Should be no stdout")
        elapsed = end-start
        self.assertTrue(elapsed < 10, "Should take less than 10 seconds")

class TestEnsembleClient(ClientTestCase):

    def test_ensemble_client(self):
        cmd = ensembles.EnsembleCommand()

        cmd.main(["ensembles"])
        stdout, stderr = self.stdio()
        self.assertEqual(stdout, "", "Should be no stdout")

        cmd.main(["create","-e","foo","-P","20","-R","30"])
        stdout, stderr = self.stdio()
        self.assertEqual(stdout, "", "Should be no stdout")

        cmd.main(["ensembles"])
        stdout, stderr = self.stdio()
        self.assertEqual(len(stdout.split("\n")), 3, "Should be two lines of stdout")

        cmd.main(["config","-e","foo","-P","50","-R","60"])
        stdout, stderr = self.stdio()
        self.assertTrue("Max Planning: 50" in stdout, "Max Planning should be 50")
        self.assertTrue("Max Running: 60" in stdout, "Max running should be 60")

        cmd.main(["pause","-e","foo"])
        stdout, stderr = self.stdio()
        self.assertTrue("State: PAUSED" in stdout, "State should be paused")

        #cmd.main(["hold","-e","foo"])
        #stdout, stderr = self.stdio()
        #self.assertTrue("State: HELD" in stdout, "State should be held")

        cmd.main(["activate","-e","foo"])
        stdout, stderr = self.stdio()
        self.assertTrue("State: ACTIVE" in stdout, "State should be active")

        # Create some test catalogs using the catalog API
        catalogs.save_catalog("replica", self.username, "rc", "regex", StringIO("replicas"))
        catalogs.save_catalog("site", self.username, "sc", "xml", StringIO("sites"))
        catalogs.save_catalog("transformation", self.username, "tc", "text", StringIO("transformations"))
        db.session.commit()

        cmd.main(["submit","-e","foo","-w","bar","-d","setup.py",
                  "-T","tc","-R","rc","-S","sc","-s","local",
                  "-o","local","--staging-site","ss=s,s2=s",
                  "-C","horiz,vert","-p","10","-c","setup.py"])
        stdout, stderr = self.stdio()
        self.assertEqual(stdout, "")

        cmd.main(["workflows","-e","foo"])
        stdout, stderr = self.stdio()
        self.assertTrue("bar" in stdout)

        cmd.main(["workflows","-e","foo","-l"])
        stdout, stderr = self.stdio()
        self.assertTrue("Name:     bar" in stdout)

        cmd.main(["priority","-e","foo","-w","bar","-p","100"])
        stdout, stderr = self.stdio()
        self.assertTrue("Priority: 100" in stdout)

        e = ensembles.get_ensemble(self.username, "foo")
        ew = ensembles.get_ensemble_workflow(e.id, "bar")
        ew.set_state(ensembles.EnsembleWorkflowStates.PLAN_FAILED)
        db.session.commit()

        cmd.main(["replan","-e","foo","-w","bar"])
        stdout, stderr = self.stdio()
        self.assertTrue("State: READY" in stdout)
        self.assertEqual(ew.state, ensembles.EnsembleWorkflowStates.READY)

        ew.set_state(ensembles.EnsembleWorkflowStates.RUN_FAILED)
        db.session.commit()

        cmd.main(["rerun","-e","foo","-w","bar"])
        stdout, stderr = self.stdio()
        self.assertTrue("State: QUEUED" in stdout)
        self.assertEqual(ew.state, ensembles.EnsembleWorkflowStates.QUEUED)

