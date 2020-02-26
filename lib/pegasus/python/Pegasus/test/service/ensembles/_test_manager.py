import os
import sys
import time
from StringIO import StringIO

from Pegasus.db import schema as dash
from Pegasus.service import catalogs, db, em, ensembles
from Pegasus.test.service import *


class TestWorkflowProcessor:
    def __init__(self, workflow):
        self.workflow = workflow

    def plan(self):
        pass

    def planning(self):
        return False

    def planning_successful(self):
        return True

    def get_wf_uuid(self):
        return "d8f8e15c-a55f-4ca0-8474-62bdb3310083"

    def get_submitdir(self):
        return "submitdir"

    def run(self):
        pass

    def pending(self):
        return False

    def running(self):
        return False

    def running_successful(self):
        return True


class EnsembleManagerTest(UserTestCase):
    def setUp(self):
        UserTestCase.setUp(self)
        em.EnsembleProcessor.Processor = TestWorkflowProcessor

    def tearDown(self):
        em.EnsembleProcessor.Processor = em.WorkflowProcessor
        UserTestCase.tearDown(self)

    def test_em(self):
        # Create an ensemble and a workflow
        e = ensembles.Ensemble(self.username, "foo")
        e.set_max_planning(1)
        e.set_max_running(1)
        db.session.add(e)
        db.session.flush()

        w = ensembles.EnsembleWorkflow(e.id, "bar")
        db.session.add(w)
        db.session.flush()

        w2 = ensembles.EnsembleWorkflow(e.id, "baz")
        db.session.add(w2)
        db.session.flush()

        mgr = em.EnsembleManager()

        e.set_state(ensembles.EnsembleStates.PAUSED)
        db.session.flush()

        mgr.loop_once()
        self.assertEqual(
            w.state,
            ensembles.EnsembleWorkflowStates.READY,
            "State should still be READY",
        )
        self.assertEqual(
            w2.state,
            ensembles.EnsembleWorkflowStates.READY,
            "State should still be READY",
        )

        e.set_state(ensembles.EnsembleStates.ACTIVE)
        db.session.flush()

        mgr.loop_once()
        self.assertEqual(
            w.state,
            ensembles.EnsembleWorkflowStates.PLANNING,
            "State should be PLANNING",
        )
        self.assertEqual(
            w2.state, ensembles.EnsembleWorkflowStates.READY, "State should be READY"
        )

        mgr.loop_once()
        self.assertEqual(
            w.state, ensembles.EnsembleWorkflowStates.RUNNING, "State should be RUNNING"
        )
        self.assertEqual(w.submitdir, "submitdir", "Submitdir should be set")
        self.assertEqual(
            w.wf_uuid, "d8f8e15c-a55f-4ca0-8474-62bdb3310083", "UUID should be set"
        )
        self.assertEqual(
            w2.state,
            ensembles.EnsembleWorkflowStates.PLANNING,
            "State should be PLANNING",
        )

        mgr.loop_once()
        self.assertEqual(
            w.state,
            ensembles.EnsembleWorkflowStates.SUCCESSFUL,
            "State should be SUCCESSFUL",
        )
        self.assertEqual(
            w2.state,
            ensembles.EnsembleWorkflowStates.RUNNING,
            "State should be RUNNING",
        )

        mgr.loop_once()
        self.assertEqual(
            w2.state,
            ensembles.EnsembleWorkflowStates.SUCCESSFUL,
            "State should be SUCCESSFUL",
        )


def RequiresPegasus(f):
    def wrapper(*args, **kwargs):
        try:
            em.get_pegasus_bin()
        except:
            sys.stderr.write(" test requires Pegasus ")
            return None

        return f(*args, **kwargs)

    return wrapper


def RequiresCondor(f):
    def wrapper(*args, **kwargs):
        try:
            em.get_condor_bin()
        except:
            sys.stderr.write(" test requires Condor ")
            return None

        return f(*args, **kwargs)

    return wrapper


class ScriptTest(TestCase):
    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def testGetEnv(self):
        PEGASUS_BIN = em.get_pegasus_bin()
        CONDOR_BIN = em.get_condor_bin()
        env = em.get_script_env()

        self.assertTrue(PEGASUS_BIN in env["PATH"])
        self.assertTrue(CONDOR_BIN in env["PATH"])

    def testForkScript(self):
        em.forkscript("true")

        cwdfile = "/tmp/forkscript.cwd"
        if os.path.isfile(cwdfile):
            os.remove(cwdfile)
        em.forkscript("echo $PWD > %s" % cwdfile, cwd="/")
        time.sleep(1)  # This just gives the script time to finish
        cwd = open(cwdfile, "r").read().strip()
        self.assertEqual(cwd, "/")
        os.remove(cwdfile)

        pidfile = "/tmp/forkscript.pid"
        if os.path.isfile(pidfile):
            os.remove(pidfile)
        em.forkscript("true", cwd="/tmp", pidfile="/tmp/forkscript.pid")
        self.assertTrue(os.path.isfile(pidfile))
        pid = int(open(pidfile, "r").read())
        self.assertTrue(pid > 0)
        os.remove(pidfile)

        self.assertRaises(
            em.EMException, em.forkscript, "true", cwd="/some/path/not/existing"
        )
        self.assertRaises(
            em.EMException, em.forkscript, "true", pidfile="/some/path/not/existing.pid"
        )

    def testRunScript(self):
        em.runscript("true")

        cwdfile = "/tmp/runscript.cwd"
        if os.path.isfile(cwdfile):
            os.remove(cwdfile)
        em.runscript("echo $PWD > %s" % cwdfile, cwd="/")
        cwd = open(cwdfile, "r").read().strip()
        self.assertEqual(cwd, "/")
        os.remove(cwdfile)

        self.assertRaises(
            em.EMException, em.runscript, "true", cwd="/some/path/not/existing"
        )


class WorkflowTest(UserTestCase):
    endstates = set(
        [
            ensembles.EnsembleWorkflowStates.SUCCESSFUL,
            ensembles.EnsembleWorkflowStates.PLAN_FAILED,
            ensembles.EnsembleWorkflowStates.RUN_FAILED,
            ensembles.EnsembleWorkflowStates.FAILED,
        ]
    )

    def test_workflow_processor(self):
        "Simple tests to make sure the WorkflowProcessor works"

        wf_uuid = "d8f8e15c-a55f-4ca0-8474-62bdb3310083"
        e = ensembles.Ensemble(self.username, "foo")
        db.session.add(e)
        db.session.flush()

        ew = ensembles.EnsembleWorkflow(e.id, "bar")
        ew.wf_uuid = wf_uuid
        db.session.add(ew)
        db.session.flush()

        p = em.WorkflowProcessor(ew)
        self.assertRaises(em.EMException, p.run)

        ew.submitdir = "/some/path/not/existing"
        db.session.flush()

        self.assertRaises(em.EMException, p.run)

        p = em.WorkflowProcessor(ew)
        self.assertTrue(p.pending())
        self.assertRaises(em.EMException, p.running)
        self.assertRaises(em.EMException, p.running_successful)

        dw = dash.DashboardWorkflow()
        dw.wf_uuid = wf_uuid
        db.session.add(dw)
        db.session.flush()

        ws = dash.DashboardWorkflowstate()
        ws.wf_id = dw.wf_id
        ws.state = "WORKFLOW_STARTED"
        ws.restart_count = 0
        ws.status = 0
        db.session.add(ws)
        db.session.flush()

        p = em.WorkflowProcessor(ew)
        self.assertTrue(p.running())
        self.assertRaises(em.EMException, p.running_successful)

        ws.state = "WORKFLOW_TERMINATED"
        ws.status = 0
        db.session.flush()

        self.assertFalse(p.running())
        self.assertTrue(p.running_successful())

        ws.status = 1
        db.session.flush()

        self.assertFalse(p.running_successful())

    def create_test_workflow(self, daxfile):
        # The replica catalog can be empty
        rcfile = StringIO("")

        # Just one transformation in the tc
        tcfile = StringIO(
            """
            tr ls {
              site local {
                pfn "/bin/ls"
                arch "x86_64"
                os "linux"
                type "INSTALLED"
              }
            }
        """
        )

        # Only the local site in the SC
        scfile = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                         version="4.0">
                <site  handle="local" arch="x86_64" os="LINUX">
                    <directory type="shared-scratch" path="%(tmpdir)s/scratch">
                        <file-server operation="all" url="file://%(tmpdir)s/scratch"/>
                    </directory>
                    <directory type="local-storage" path="%(tmpdir)s/storage">
                        <file-server operation="all" url="file://%(tmpdir)s/storage"/>
                    </directory>
                </site>
            </sitecatalog>
        """
            % {"tmpdir": self.tmpdir}
        )

        rc = catalogs.save_catalog("replica", self.username, "replica", "File", rcfile)
        sc = catalogs.save_catalog("site", self.username, "sites", "XML", scfile)
        tc = catalogs.save_catalog(
            "transformation", self.username, "transformations", "text", tcfile
        )

        conf = StringIO("pegasus.register=false")

        e = ensembles.create_ensemble(self.username, "process", 1, 1)
        ew = ensembles.create_ensemble_workflow(
            e.id,
            "process",
            0,
            rc,
            tc,
            sc,
            daxfile,
            conf,
            sites=["local"],
            output_site="local",
            force=True,
            cleanup=False,
        )

        return e, ew

    @IntegrationTest
    @RequiresPegasus
    def test_planner_fails(self):
        # This should fail to plan because the dax has an unknown transformation
        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="FROOB">
                    <argument>-l /</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """
        )

        e, ew = self.create_test_workflow(dax)

        p = em.WorkflowProcessor(ew)

        p.plan()
        while p.planning():
            time.sleep(1)

        self.assertFalse(p.planning_successful(), "Workflow should fail to plan")

    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def test_failed_workflow(self):
        # This workflow should fail because the argument to the ls job is invalid
        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="ls">
                    <argument>-l /path/that/does/not/exist</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """
        )

        e, ew = self.create_test_workflow(dax)

        p = em.WorkflowProcessor(ew)

        p.plan()
        while p.planning():
            time.sleep(1)

        self.assertTrue(p.planning_successful(), "Planning should succeed")

        submitdir = p.get_submitdir()
        self.assertTrue(os.path.isdir(submitdir), "Submit dir should exist")

        wf_uuid = p.get_wf_uuid()
        self.assertTrue(wf_uuid is not None, "wf_uuid should exist")

        # The ensemble processor normally does this
        ew.set_submitdir(submitdir)
        ew.set_wf_uuid(wf_uuid)
        db.session.flush()
        db.session.commit()

        p.run()

        while p.pending() or p.running():
            time.sleep(5)

        self.assertFalse(p.running_successful(), "The workflow should fail to run")

    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def test_successful_workflow(self):
        # This workflow should succeed
        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="ls">
                    <argument>-l /</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """
        )

        e, ew = self.create_test_workflow(dax)

        p = em.WorkflowProcessor(ew)

        p.plan()
        while p.planning():
            time.sleep(1)

        self.assertTrue(p.planning_successful())

        submitdir = p.get_submitdir()
        self.assertTrue(os.path.isdir(submitdir))

        wf_uuid = p.get_wf_uuid()
        self.assertTrue(wf_uuid is not None)

        ew.set_submitdir(submitdir)
        ew.set_wf_uuid(wf_uuid)
        db.session.flush()

        db.session.commit()

        p.run()

        while p.pending() or p.running():
            time.sleep(5)

        self.assertTrue(p.running_successful())

    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def test_ensemble_end_to_end(self):
        # This workflow should succeed
        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="ls">
                    <argument>-l /</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """
        )

        e, ew = self.create_test_workflow(dax)

        mgr = em.EnsembleManager()

        while ew.state not in self.endstates:
            mgr.loop_once()
            time.sleep(5)

        self.assertEqual(ew.state, ensembles.EnsembleWorkflowStates.SUCCESSFUL)

    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def test_ensemble_failure_end_to_end(self):
        # This workflow should fail because of the argument to ls
        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="ls">
                    <argument>-l /some/non/existent/directory</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """
        )

        e, ew = self.create_test_workflow(dax)

        mgr = em.EnsembleManager()

        while ew.state not in self.endstates:
            mgr.loop_once()
            time.sleep(5)

        self.assertEqual(ew.state, ensembles.EnsembleWorkflowStates.FAILED)

    @IntegrationTest
    @RequiresPegasus
    @RequiresCondor
    def test_hierarchical_workflow(self):
        subdax = """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <job id="ID0000001" name="ls">
                    <argument>-l /</argument>
                    <stdout name="listing.txt" link="output"/>
                    <uses name="listing.txt" link="output" register="false" transfer="true"/>
                </job>
            </adag>
        """

        subdaxfile = os.path.join(self.tmpdir, "subdax.xml")
        f = open(subdaxfile, "w")
        f.write(subdax)
        f.close()

        dax = StringIO(
            """<?xml version="1.0" encoding="UTF-8"?>
            <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.4.xsd"
                  version="3.4" name="process">
                <file name="subdax.xml">
                    <pfn url="file://%s" site="local"/>
                </file>
                <dax id="ID0000001" file="subdax.xml">
                </dax>
            </adag>
        """
            % subdaxfile
        )

        e, ew = self.create_test_workflow(dax)

        mgr = em.EnsembleManager()

        while ew.state not in self.endstates:
            mgr.loop_once()
            time.sleep(5)

        self.assertEqual(ew.state, ensembles.EnsembleWorkflowStates.SUCCESSFUL)
