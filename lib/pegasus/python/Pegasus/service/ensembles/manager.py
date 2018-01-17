import os
import sys
import subprocess
import logging
import time
import threading
import datetime
from sqlalchemy.orm.exc import NoResultFound

from Pegasus import user
from Pegasus.db import connection
from Pegasus.service import app
from Pegasus.db.ensembles import Ensembles, EnsembleStates, EnsembleWorkflowStates, EMError
from Pegasus.db.schema import DashboardWorkflow, DashboardWorkflowstate

log = logging.getLogger(__name__)

def pathfind(exe):
    PATH = os.getenv("PATH","/bin:/usr/bin:/usr/local/bin")
    PATH = PATH.split(":")
    for prefix in PATH:
        exepath = os.path.join(prefix, exe)
        if os.path.isfile(exepath):
            return exepath
    raise EMError("%s not found on PATH" % exe)

def get_bin(name, exe):
    # Try to find NAME/bin using 1) NAME env var, 2) NAME config
    # variable, 3) PATH env var
    exepath = None

    HOME = os.getenv(name, app.config.get(name, None))

    if HOME is not None:
        if not os.path.isdir(HOME):
            raise EMError("%s is not a directory: %s" % (name, HOME))
        BIN = os.path.join(HOME, "bin")
        if not os.path.isdir(BIN):
            raise EMError("%s/bin is not a directory: %s" % (name, BIN))
        exepath = os.path.join(BIN, exe)

    exepath = exepath or pathfind(exe)

    if not os.path.isfile(exepath):
        raise EMError("%s not found: %s" % (exe, exepath))

    BIN = os.path.dirname(exepath)

    return BIN

def get_pegasus_bin():
    return get_bin("PEGASUS_HOME", "pegasus-plan")

def get_condor_bin():
    return get_bin("CONDOR_HOME", "condor_submit_dag")

def check_environment():
    # Make sure Pegasus and Condor are on the PATH, or PEGASUS_HOME
    # and CONDOR_HOME are set in the environment or configuration
    get_pegasus_bin()
    get_condor_bin()

def get_script_env():
    PEGASUS_BIN = get_pegasus_bin()
    CONDOR_BIN = get_condor_bin()

    # Add pegasus bin dirs to PATH
    env = dict(os.environ)
    PATH = env.get("PATH", "/bin:/usr/bin:/usr/local/bin")
    PATH = PEGASUS_BIN + ":" + CONDOR_BIN + ":" + PATH
    env["PATH"] = PATH

    return env

def runscript(script, cwd=None, env=None):
    # Make sure the cwd is OK
    if cwd is not None and not os.path.isdir(cwd):
        raise EMError("Working directory does not exist: %s" % cwd)

    if env is None:
        env = dict(os.environ)

    p = subprocess.Popen(script, shell=True, env=env, cwd=cwd)

    rc = p.wait()

    if rc != 0:
        raise EMError("Script failed with exitcode %d" % rc)

def forkscript(script, pidfile=None, cwd=None, env=None):
    # This does a double fork to detach the process from the python
    # interpreter so that we don't have to call wait() on it

    # Make sure the cwd is OK
    if cwd is not None and not os.path.isdir(cwd):
        raise EMError("Working directory does not exist: %s" % cwd)

    if env is None:
        env = dict(os.environ)

    # This is just to ensure we get an exception if there is
    # something wrong with the pidfile
    if pidfile is not None:
        try:
            open(pidfile, "w").close()
        except:
            raise EMError("Unable to write pidfile: %s" % pidfile)

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
        raise EMError("Non-zero exitcode launching script: %d" % exitcode)

class WorkflowProcessor:
    def __init__(self, dao, workflow):
        self.dao = dao
        self.workflow = workflow

    def plan(self):
        "Launch the pegasus planner"
        w = self.workflow
        basedir = w.get_basedir()
        pidfile = w.get_pidfile()
        logfile = w.get_logfile()
        runfile = w.get_runfile()
        resultfile = w.get_resultfile()
        plan_command = w.get_plan_command()

        if os.path.isfile(pidfile) and self.planning():
            raise EMError("Planner already running")

        # When we re-plan, we need to remove all the old
        # files so that the ensemble manager doesn't get
        # confused.
        files = [
            runfile,
            resultfile,
            pidfile
        ]
        for f in files:
            if os.path.isfile(f):
                os.remove(f)

        script = "(%s) 2>&1 | tee -a %s | grep pegasus-run >%s ; /bin/echo $? >%s" % (plan_command, logfile, runfile, resultfile)
        forkscript(script, cwd=basedir, pidfile=pidfile, env=get_script_env())

    def planning(self):
        "Check pidfile to see if the planner is still running"
        pidfile = self.workflow.get_pidfile()

        if not os.path.exists(pidfile):
            raise EMError("pidfile missing")

        pid = int(open(pidfile,"r").read())

        try:
            os.kill(pid, 0)
            # If that succeeds, the process is still running
            return True
        except OSError as e:
            # errno 3 is No Such Process
            if e.errno != 3:
                raise

        return False

    def planning_successful(self):
        "Check to see if planning was successful"
        resultfile = self.workflow.get_resultfile()

        if not os.path.exists(resultfile):
            raise EMError("Result file not found: %s" % resultfile)

        exitcode = int(open(resultfile, "r").read())

        if exitcode != 0:
            return False

        try:
            self.find_submitdir()
        except Exception as e:
            log.exception(e)
            return False

        return True

    def find_submitdir(self):
        "Get the workflow submitdir from the workflow log"
        logfile = self.workflow.get_runfile()

        if not os.path.isfile(logfile):
            raise EMError("Workflow run file not found: %s" % logfile)

        submitdir = None

        f = open(logfile, "r")
        try:
            for l in f:
                if l.startswith("pegasus-run"):
                    submitdir = l.split()[1]
        finally:
            f.close()

        if submitdir is None:
            raise EMError("No pegasus-run found in the workflow run file: %s" % logfile)

        return submitdir

    def get_wf_uuid(self):
        "Get the workflow UUID from the braindump file"
        submitdir = self.find_submitdir()

        braindump = os.path.join(submitdir, "braindump.txt")

        if not os.path.isfile(braindump):
            raise EMError("braindump.txt not found")

        wf_uuid = None

        f = open(braindump, "r")
        try:
            for l in f:
                if l.startswith("wf_uuid"):
                    wf_uuid = l.split()[1]
        finally:
            f.close()

        if wf_uuid is None:
            raise EMError("wf_uuid not found in braindump.txt")

        return wf_uuid

    def run(self):
        "Run the workflow using pegasus-run"
        submitdir = self.workflow.submitdir

        if submitdir is None:
            raise EMError("Workflow submitdir not set")

        if not os.path.isdir(submitdir):
            raise EMError("Workflow submit dir does not exist: %s" % submitdir)

        logfile = self.workflow.get_logfile()

        runscript("pegasus-run %s >>%s 2>&1" % (submitdir, logfile), env=get_script_env())

    def get_dashboard(self):
        "Get the dashboard record for the workflow"
        wf_uuid = self.workflow.wf_uuid
        if wf_uuid is None:
            raise EMError("wf_uuid is none")

        try:
            w = self.dao.session.query(DashboardWorkflow)\
                    .filter_by(wf_uuid=str(wf_uuid))\
                    .one()
            return w
        except NoResultFound:
            name = self.workflow.name
            log.debug("No dashboard record for workflow %s" % name)
            return None

    def get_dashboard_state_for_running_workflow(self):
        """Get the latest state of the workflow from the dashboard
           tables where timestamp is > last updated of the ensemble workflow"""
        # We can only use this for running workflows because we are assuming
        # that the last state of the workflow should be after the updated
        # timestamp of the ensemble workflow. That might not be true for
        # workflows in states other than RUNNING.
        if self.workflow.state != EnsembleWorkflowStates.RUNNING:
            raise EMError("This method should only be called for running workflows")

        w = self.get_dashboard()
        if w is None:
            raise EMError("Dashboard workflow not found")

        # Need to compute the unix ts for updated in this ugly way
        updated = (self.workflow.updated - datetime.datetime(1970,1,1)).total_seconds()

        # Get the last event for the workflow where the event timestamp is
        # greater than the last updated ts for the ensemble workflow
        ws = self.dao.session.query(DashboardWorkflowstate)\
                .filter_by(wf_id=w.wf_id)\
                .filter(DashboardWorkflowstate.timestamp >= updated)\
                .order_by("timestamp desc")\
                .first()

        if ws is None:
            name = self.workflow.name
            log.info("No recent workflow state records for workflow %s" % name)

        return ws

    def pending(self):
        "The workflow is pending if there is no dashboard record"
        w = self.get_dashboard()
        return w is None

    def running(self):
        "Is the workflow running"
        ws = self.get_dashboard_state_for_running_workflow()
        if ws is None:
            return True
        return ws.state == "WORKFLOW_STARTED"

    def running_successful(self):
        "Assuming the workflow is done running, did it finish successfully?"
        ws = self.get_dashboard_state_for_running_workflow()
        if ws is None or ws.state == "WORKFLOW_STARTED":
            raise EMError("Workflow is running")
        return ws.status == 0

class EnsembleProcessor:
    Processor = WorkflowProcessor

    def __init__(self, dao, ensemble):
        self.dao = dao
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
        workflow.set_updated()

        self.dao.session.flush()

        # Fork planning task
        p = self.Processor(self.dao, workflow)
        try:
            p.plan()
        except Exception as e:
            log.error("Planning failed for workflow %s" % workflow.name)
            log.exception(e)
            workflow.set_state(EnsembleWorkflowStates.PLAN_FAILED)
            workflow.set_updated()

        self.dao.session.commit()

    def handle_ready(self, workflow):
        if self.can_plan():
            self.plan_workflow(workflow)
        else:
            log.debug("Delaying planning of workflow %s due to policy" % workflow.name)

    def handle_planning(self, workflow):
        p = self.Processor(self.dao, workflow)

        if p.planning():
            log.info("Workflow %s is still planning" % workflow.name)
            return

        log.info("Workflow %s is no longer planning" % workflow.name)
        self.planning -= 1

        # Planning failed
        if not p.planning_successful():
            log.error("Planning failed for workflow %s" % workflow.name)
            workflow.set_state(EnsembleWorkflowStates.PLAN_FAILED)
            workflow.set_updated()
            self.dao.session.commit()
            return

        log.info("Queueing workflow %s" % workflow.name)

        # Planning succeeded, get uuid and queue workflow
        workflow.set_wf_uuid(p.get_wf_uuid())
        workflow.set_submitdir(p.find_submitdir())
        workflow.set_state(EnsembleWorkflowStates.QUEUED)
        workflow.set_updated()
        self.dao.session.commit()

        # Go ahead and handle the queued state now
        self.handle_queued(workflow)

    def can_run(self):
        return self.active and self.running < self.max_running

    def run_workflow(self, workflow):
        log.info("Running workflow %s" % workflow.name)

        self.running += 1
        workflow.set_state(EnsembleWorkflowStates.RUNNING)
        workflow.set_updated()
        self.dao.session.flush()

        p = self.Processor(self.dao, workflow)
        try:
            p.run()
        except Exception as e:
            log.debug("Running of workflow %s failed" % workflow.name)
            log.exception(e)
            workflow.set_state(EnsembleWorkflowStates.RUN_FAILED)
            workflow.set_updated()

        self.dao.session.commit()

    def handle_queued(self, workflow):
        if self.can_run():
            self.run_workflow(workflow)
        else:
            log.debug("Delaying run of workflow %s due to policy" % workflow.name)

    def handle_running(self, workflow):
        p = self.Processor(self.dao, workflow)

        if p.pending():
            log.info("Workflow %s is pending" % workflow.name)
            return

        if p.running():
            log.info("Workflow %s is running" % workflow.name)
            return

        log.info("Workflow %s is no longer running" % workflow.name)
        self.running -= 1

        if p.running_successful():
            log.info("Workflow %s was successful" % workflow.name)
            workflow.set_state(EnsembleWorkflowStates.SUCCESSFUL)
        else:
            log.info("Workflow %s failed" % workflow.name)
            workflow.set_state(EnsembleWorkflowStates.FAILED)

        workflow.set_updated()

        self.dao.session.commit()

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
        edir = self.ensemble.get_localdir()
        if not os.path.isdir(edir):
            log.info("Creating ensemble directory: %s" % edir)
            os.makedirs(edir, 0o700)

        log.info("Processing %d ensemble workflows..." % len(self.workflows))
        for w in self.workflows:
            try:
                self.handle_workflow(w)
            except Exception as e:
                log.error("Processing workflow %s of ensemble %s" % (w.name, self.ensemble.name))
                log.exception(e)
                self.dao.session.rollback()


class EnsembleManager(threading.Thread):
    Processor = EnsembleProcessor

    def __init__(self, interval=None):
        threading.Thread.__init__(self)
        self.daemon = True
        if interval is None:
            interval = float(app.config["EM_INTERVAL"])
        self.interval = interval

    def run(self):
        log.info("Ensemble Manager starting")
        self.loop_forever()

    def loop_forever(self):
        while True:
            u = user.get_user_by_uid(os.getuid())
            session = connection.connect(u.get_master_db_url())
            try:
                dao = Ensembles(session)
                self.loop_once(dao)
            finally:
                session.close()
            time.sleep(self.interval)

    def loop_once(self, dao):
        actionable = dao.list_actionable_ensembles()
        if len(actionable) == 0:
            return

        log.info("Processing ensembles")
        for e in actionable:
            log.info("Processing ensemble %s", e.name)
            p = self.Processor(dao, e)
            p.run()

