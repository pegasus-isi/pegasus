import os
import sys
import subprocess
import logging
import threading
import time

from sqlalchemy.orm.exc import NoResultFound

from Pegasus.netlogger.analysis.schema import stampede_dashboard_schema as dash

from pegasus.service import app, db, ensembles
from pegasus.service.ensembles import EnsembleStates, EnsembleWorkflowStates

log = logging.getLogger("EnsembleManager")

class EMException(Exception): pass

def get_script_env():
    # Find the pegasus bin dir
    PEGASUS_HOME = app.config.get("PEGASUS_HOME", "/usr")
    PEGASUS_BIN = os.path.join(PEGASUS_HOME, "bin")

    if not os.path.isdir(PEGASUS_HOME):
        raise EMException("PEGASUS_HOME does not exist: %s" % PEGASUS_HOME)

    if not os.path.isdir(PEGASUS_BIN):
        raise EMException("PEGASUS_HOME/bin does not exist: %s" % PEGASUS_BIN)

    # Add pegasus bin dir to PATH
    env = dict(os.environ)
    PATH = env.get("PATH", "/bin:/usr/bin:/usr/local/bin")
    PATH = PEGASUS_BIN + ":" + PATH
    env["PATH"] = PATH
    env["PEGASUS_HOME"] = PEGASUS_HOME

    return env

def runscript(script, cwd=None):
    # Make sure the cwd is OK
    if cwd is not None and not os.path.isdir(cwd):
        raise EMException("Working directory does not exist: %s" % cwd)

    env = get_script_env()

    p = subprocess.Popen(script, shell=True, env=env, cwd=cwd)

    rc = p.wait()

    if rc != 0:
        raise EMException("Script failed with exitcode %d" % rc)

def forkscript(script, pidfile=None, cwd=None):
    # This does a double fork to detach the process from the python
    # interpreter so that we don't have to call wait() on it

    # Make sure the cwd is OK
    if cwd is not None and not os.path.isdir(cwd):
        raise EMException("Working directory does not exist: %s" % cwd)

    # This is just to ensure we get an exception if there is
    # something wrong with the pidfile
    if pidfile is not None:
        try:
            open(pidfile, "w").close()
        except:
            raise EMException("Unable to write pidfile: %s" % pidfile)

    env = get_script_env()

    pid1 = os.fork()
    if pid1 == 0:
        if cwd is not None:
            os.chdir(cwd)

        pid2 = os.fork()
        if pid2 == 0:
            os.execve("/bin/sh", ["/bin/sh","-c",script], env)
            os._exit(255)

        if pidfile is not None:
            f = open(pidfile, "w")
            f.write("%d\n" % pid2)
            f.close()

        os._exit(0)

    pid, exitcode = os.waitpid(pid1, 0)
    if exitcode != 0:
        raise EMException("Non-zero exitcode launching script: %d" % exitcode)

class WorkflowProcessor:
    def __init__(self, workflow):
        self.workflow = workflow

    def get_file(self, filename):
        dirname = self.workflow.get_dir()
        return os.path.join(dirname, filename)

    def get_pidfile(self):
        return self.get_file("planner.pid")

    def get_resultfile(self):
        return self.get_file("planner.result")

    def get_logfile(self):
        return self.get_file("workflow.log")

    def get_planfile(self):
        return self.get_file("plan.sh")

    def plan(self):
        "Launch the pegasus planner"
        workdir = self.workflow.get_dir()
        pidfile = self.get_pidfile()
        logfile = self.get_logfile()
        planfile = self.get_planfile()
        resultfile = self.get_resultfile()

        if os.path.isfile(pidfile) and self.running():
            raise EMException("Planner already running")

        # When we re-plan, we need to remove all the old
        # files so that the ensemble manager doesn't get
        # confused.
        files = [
            logfile,
            resultfile,
            pidfile
        ]
        for f in files:
            if os.path.isfile(f):
                os.remove(f)

        script = "%s >%s 2>&1; /bin/echo $? >%s" % (planfile, logfile, resultfile)
        forkscript(script, cwd=workdir, pidfile=pidfile)

    def planning(self):
        "Check pidfile to see if the planner is still running"
        pidfile = self.get_pidfile()

        if not os.path.exists(pidfile):
            raise EMException("pidfile missing")

        pid = int(open(pidfile,"r").read())

        try:
            os.kill(pid, 0)
            # If that succeeds, the process is still running
            return True
        except OSError, e:
            # errno 3 is No Such Process
            if e.errno != 3:
                raise

        return False

    def planning_successful(self):
        "Check to see if planning was successful"
        resultfile = self.get_resultfile()

        if not os.path.exists(resultfile):
            raise EMException("Result file not found: %s" % resultfile)

        exitcode = int(open(resultfile, "r").read())

        return exitcode == 0

    def get_submitdir(self):
        "Get the workflow submitdir from the workflow log"
        logfile = self.get_logfile()

        if not os.path.isfile(logfile):
            raise EMException("Workflow log file not found: %s" % logfile)

        submitdir = None

        f = open(logfile, "r")
        try:
            for l in f:
                if l.startswith("pegasus-run"):
                    submitdir = l.split()[1]
        finally:
            f.close()

        if submitdir is None:
            raise EMException("No pegasus-run found in the workflow log: %s" % logfile)

        return submitdir

    def get_wf_uuid(self):
        "Get the workflow UUID from the braindump file"
        submitdir = self.get_submitdir()

        braindump = os.path.join(submitdir, "braindump.txt")

        if not os.path.isfile(braindump):
            raise EMException("braindump.txt not found")

        wf_uuid = None

        f = open(braindump, "r")
        try:
            for l in f:
                if l.startswith("wf_uuid"):
                    wf_uuid = l.split()[1]
        finally:
            f.close()

        if wf_uuid is None:
            raise EMException("wf_uuid not found in braindump.txt")

        return wf_uuid

    def submit(self):
        "Submit the workflow using pegasus-run"
        submitdir = self.workflow.submitdir

        if submitdir is None:
            raise EMException("Workflow submitdir not set")

        if not os.path.isdir(submitdir):
            raise EMException("Workflow submit dir does not exist: %s" % submitdir)

        logfile = self.get_logfile()

        runscript("pegasus-run %s >>%s 2>&1" % (submitdir,logfile))

    def get_dashboard(self):
        "Get the dashboard record for the workflow"
        wf_uuid = self.workflow.wf_uuid
        if wf_uuid is None:
            raise EMException("wf_uuid is none")

        try:
            w = db.session.query(dash.DashboardWorkflow)\
                    .filter_by(wf_uuid=str(wf_uuid))\
                    .one()
            return w
        except NoResultFound:
            return None

    def get_dashboard_state(self):
        "Get the latest state of the workflow from the dashboard tables"
        w = self.get_dashboard()
        if w is None:
            raise EMException("Dashboard workflow not found")

        try:
            ws = db.session.query(dash.DashboardWorkflowstate)\
                    .filter_by(wf_id=w.wf_id)\
                    .order_by("timestamp desc")\
                    .first()
        except NoResultFound:
            raise EMException("Dashboard workflow state not found")

        return ws

    def pending(self):
        "The workflow is pending if there is no dashboard record"
        w = self.get_dashboard()
        return w is None

    def running(self):
        "Is the workflow running"
        ws = self.get_dashboard_state()
        return ws.state == "WORKFLOW_STARTED"

    def running_successful(self):
        "Assuming the workflow is done running, did it finish successfully?"
        ws = self.get_dashboard_state()
        if ws.state == "WORKFLOW_STARTED":
            raise EMException("Workflow is running")
        return ws.status == 0

class EnsembleProcessor:
    Processor = WorkflowProcessor

    def __init__(self, ensemble):
        self.ensemble = ensemble
        self.active = ensemble.state == EnsembleStates.ACTIVE
        self.max_running = ensemble.max_running
        self.max_planning = ensemble.max_planning
        self.running = 0
        self.planning = 0

        # We are only interested in workflows that are in one
        # of the following states. The EM doesn't handle other
        # states.
        active_states = set((
            EnsembleWorkflowStates.READY,
            EnsembleWorkflowStates.PLANNING,
            EnsembleWorkflowStates.QUEUED,
            EnsembleWorkflowStates.RUNNING
        ))

        # We need a copy of the list so we can sort it
        self.workflows = [w for w in ensemble.workflows if w.state in active_states]

        # Sort workflows by priority
        def get_priority(w):
            return w.priority

        self.workflows.sort(key=get_priority)

        # Count the number of planning and running workflows
        for w in self.workflows:
            if w.state == EnsembleWorkflowStates.PLANNING:
                self.planning += 1
            elif w.state == EnsembleWorkflowStates.RUNNING:
                self.running += 1

    def can_plan(self):
        return self.active and self.planning < self.max_planning

    def plan_workflow(self, workflow):
        log.info("Planning %s" % workflow.name)
        self.planning += 1
        workflow.set_state(EnsembleWorkflowStates.PLANNING)

        db.session.flush()

        # Fork planning task
        p = self.Processor(workflow)
        p.plan()

        db.session.commit()

    def handle_ready(self, workflow):
        if self.can_plan():
            self.plan_workflow(workflow)

    def handle_planning(self, workflow):
        p = self.Processor(workflow)

        if p.planning():
            return

        self.planning -= 1

        # Planning failed
        if not p.planning_successful():
            workflow.set_state(EnsembleWorkflowStates.FAILED)
            db.session.commit()
            return

        log.info("Queueing workflow %s" % workflow.name)

        # Planning succeeded, get uuid and queue workflow
        workflow.set_wf_uuid(p.get_wf_uuid())
        workflow.set_submitdir(p.get_submitdir())
        workflow.set_state(EnsembleWorkflowStates.QUEUED)
        db.session.commit()

        # Go ahead and handle the queued state now
        self.handle_queued(workflow)

    def can_submit(self):
        return self.active and self.running < self.max_running

    def submit_workflow(self, workflow):
        log.info("Submitting workflow %s" % workflow.name)

        self.running += 1
        workflow.set_state(EnsembleWorkflowStates.RUNNING)
        db.session.flush()

        p = self.Processor(workflow)
        p.submit()

        db.session.commit()

    def handle_queued(self, workflow):
        if self.can_submit():
            self.submit_workflow(workflow)

    def handle_running(self, workflow):
        p = self.Processor(workflow)

        if p.pending() or p.running():
            return

        self.running -= 1

        if p.running_successful():
            workflow.set_state(EnsembleWorkflowStates.SUCCESSFUL)
        else:
            workflow.set_state(EnsembleWorkflowStates.FAILED)

        db.session.commit()

    def handle_workflow(self, w):
        if w.state == EnsembleWorkflowStates.READY:
            self.handle_ready(w)
        elif w.state == EnsembleWorkflowStates.PLANNING:
            self.handle_planning(w)
        elif w.state == EnsembleWorkflowStates.QUEUED:
            self.handle_queued(w)
        elif w.state == EnsembleWorkflowStates.RUNNING:
            self.handle_running(w)

    def run(self):
        log.info("Processing %d ensemble workflows..." % len(self.workflows))
        for w in self.workflows:
            try:
                self.handle_workflow(w)
            except Exception, e:
                log.error("Processing workflow %s of ensemble %s" % (w.name, self.ensemble.name))
                log.exception(e)


class EnsembleManager(threading.Thread):
    Processor = EnsembleProcessor

    def __init__(self, interval=None):
        threading.Thread.__init__(self)
        self.daemon = True

        if interval is None:
            self.interval = float(app.config["EM_INTERVAL"])
        else:
            self.interval = interval

    def run(self):
        self.loop_forever()

    def loop_forever(self):
        while True:
            self.loop_once()
            time.sleep(self.interval)

    def loop_once(self):
        log.info("Processing ensembles...")
        for e in ensembles.list_actionable_ensembles():
            log.info("Processing ensemble %s" % e.name)
            p = self.Processor(e)
            p.run()
        log.info("Finished processing ensembles")

