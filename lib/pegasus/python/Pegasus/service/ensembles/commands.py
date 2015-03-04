import os
import sys
import urlparse
import requests
import logging
import zipfile
from optparse import OptionParser

from Pegasus.service.command import Command, CompoundCommand
from Pegasus.service.ensembles import emapp, manager
from Pegasus.service.ensembles.models import EnsembleStates, EnsembleWorkflowStates

log = logging.getLogger(__name__)

EM_PORT = os.getuid() + 7919

def add_ensemble_option(self):
    self.parser.add_option("-e", "--ensemble", action="store", dest="ensemble",
        default=None, help="Ensemble name")

def add_workflow_option(self):
    self.parser.add_option("-w", "--workflow", action="store", dest="workflow",
        default=None, help="Workflow name")

class EnsembleClientCommand(Command):
    def __init__(self):
        Command.__init__(self)
        self.endpoint = "http://127.0.0.1:%d/" % EM_PORT
        self.username = emapp.config["USERNAME"]
        if not self.username:
            raise Exception("Specify USERNAME in configuration")
        self.password = emapp.config["PASSWORD"]
        if not self.password:
            raise Exception("Specify PASSWORD in configuration")

    def _request(self, method, path, **kwargs):
        headers = {
            'accept': 'application/json'
        }
        defaults = {"auth": (self.username, self.password), "headers": headers}
        defaults.update(kwargs)
        url = urlparse.urljoin(self.endpoint, path)
        return requests.request(method, url, **defaults)

    def get(self, path, **kwargs):
        return self._request("get", path, **kwargs)

    def post(self, path, **kwargs):
        return self._request("post", path, **kwargs)

    def delete(self, path, **kwargs):
        return self._request("delete", path, **kwargs)

    def put(self, path, **kwargs):
        return self._request("put", path, **kwargs)

class ServerCommand(Command):
    description = "Start ensemble manager"
    usage = "%prog [options]"

    def __init__(self):
        Command.__init__(self)
        self.parser.add_option("-d", "--debug", action="store_true", dest="debug",
                               default=None, help="Enable debugging")

    def run(self):
        if self.options.debug:
            emapp.config.update(DEBUG=True)
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO

        logging.basicConfig(level=log_level)
        logging.getLogger().setLevel(log_level)

        # We only start the ensemble manager if we are not debugging
        # or if we are debugging and Werkzeug is restarting. This
        # prevents us from having two ensemble managers running in
        # the debug case.
        WERKZEUG_RUN_MAIN = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
        DEBUG = emapp.config.get("DEBUG", False)
        if (not DEBUG) or WERKZEUG_RUN_MAIN:
            # Make sure the environment is OK for the ensemble manager
            try:
                manager.check_environment()
            except manager.EMException, e:
                log.warning("%s: Ensemble manager disabled" % e.message)
            else:
                mgr = manager.EnsembleManager()
                mgr.start()

        if os.getuid() == 0:
            log.fatal("The ensemble manager should not be run as root")
            exit(1)

        emapp.run(port=EM_PORT, host="127.0.0.1")

        log.info("Exiting")

class EnsemblesCommand(EnsembleClientCommand):
    description = "List ensembles"
    usage = "Usage: %prog ensembles"

    def run(self):
        response = self.get("/ensembles")
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]
            exit(1)

        fmt = "%-20s %-8s %-30s %-30s %14s %12s"
        if len(result) > 0:
            print fmt % ("NAME","STATE","CREATED","UPDATED","MAX PLANNING","MAX RUNNING")
        for r in result:
            print fmt % (r["name"], r["state"], r["created"], r["updated"], r["max_planning"], r["max_running"])

class CreateCommand(EnsembleClientCommand):
    description = "Create ensemble"
    usage = "Usage: %prog create [options] -e ENSEMBLE"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=1, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=1, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {
            "name": self.options.ensemble,
            "max_planning": self.options.max_planning,
            "max_running": self.options.max_running
        }

        response = self.post("/ensembles", data=request)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:", result["message"]
            exit(1)

def pathfind(command):
    def isexe(fn):
        return os.path.isfile(fn) and os.access(fn, os.X_OK)

    path, exe = os.path.split(command)

    if path:
        if isexe(command):
            return os.path.abspath(command)
        return None

    # Search PATH
    for path in os.environ["PATH"].split(os.pathsep):
        fn = os.path.join(path, command)
        if isexe(fn):
            return fn

    return None

class SubmitCommand(EnsembleClientCommand):
    description = "Submit workflow"
    usage = "Usage: %prog submit [options] -e ENSEMBLE -w WORKFLOW plan_command [arg...]"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.disable_interspersed_args()
        self.parser.add_option("-p", "--priority", action="store", dest="priority",
            default=0, help="Workflow priority", metavar="NUMBER")

    def run(self):
        o = self.options
        p = self.parser

        if o.ensemble is None:
            p.error("Specify -e/--ensemble")
        if o.workflow is None:
            p.error("Specify -w/--workflow")

        if len(self.args) == 0:
            p.error("Specify planning command")

        command = self.args[0]
        args = self.args[1:]

        exe = pathfind(command)
        if exe is None:
            p.error("invalid planning command: %s" % command)

        args.insert(0, exe)

        command = '"%s"' % '" "'.join(args)

        data = {
            "name": o.workflow,
            "priority": o.priority,
            "basedir": os.getcwd(),
            "plan_command": command
        }

        response = self.post("/ensembles/%s/workflows" % o.ensemble, data=data)

        if response.status_code != 201:
            result = response.json()
            print "ERROR:",response.status_code,result["message"]

class WorkflowsCommand(EnsembleClientCommand):
    description = "List workflows in ensemble"
    usage = "Usage: %prog workflows [options] -e ENSEMBLE."

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-l", "--long", action="store_true", dest="long",
            default=False, help="Show detailed output")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        response = self.get("/ensembles/%s/workflows" % self.options.ensemble)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:",response.status_code,result["message"]
            exit(1)

        if len(result) == 0:
            return

        if self.options.long:
            for w in result:
                print "ID:      ",w["id"]
                print "Name:    ",w["name"]
                print "Created: ",w["created"]
                print "Updated: ",w["updated"]
                print "State:   ",w["state"]
                print "Priority:",w["priority"]
                print "UUID:    ",w["wf_uuid"]
                print "URL:     ",w["href"]
                print
        else:
            fmt = "%-20s %-15s %-8s %-30s %-30s"
            print fmt % ("NAME","STATE","PRIORITY","CREATED","UPDATED")
            for w in result:
                print fmt % (w["name"],w["state"],w["priority"],w["created"],w["updated"])

class StatusCommand(EnsembleClientCommand):
    description = "Check workflow status"
    usage = "Usage: %prog status [options] -e ENSEMBLE -w WORKFLOW"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)

    def run(self):
        o = self.options
        p = self.parser

        if o.ensemble is None:
            p.error("Specify -e/--ensemble")
        if o.workflow is None:
            p.error("Specify -w/--workflow")

        response = self.get("/ensembles/%s/workflows/%s" % (o.ensemble, o.workflow))

        result = response.json()

        if response.status_code != 200:
            print "ERROR:",response.status_code,result["message"]
            exit(1)

        print "ID:           %s" % result['id']
        print "Name:         %s" % result['name']
        print "Plan Command: %s" % result['plan_command']
        print "Created:      %s" % result['created']
        print "Updated:      %s" % result['updated']
        print "State:        %s" % result['state']
        print "UUID:         %s" % (result['wf_uuid'] or "")
        print "Priority:     %s" % result['priority']
        print "Base Dir:     %s" % result['basedir']
        print "Submit Dir:   %s" % (result['submitdir'] or "")
        print "Plan Log:     %s" % result['plan_log']

class StateChangeCommand(EnsembleClientCommand):
    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        response = self.post("/ensembles/%s" % self.options.ensemble, data={"state":self.newstate})
        result = response.json()

        if response.status_code != 200:
            print "ERROR:",result["message"]

        print "State:", result["state"]

class PauseCommand(StateChangeCommand):
    description = "Pause active ensemble"
    usage = "Usage: %prog pause -e ENSEMBLE"
    newstate = EnsembleStates.PAUSED

class ActivateCommand(StateChangeCommand):
    description = "Activate paused or held ensemble"
    usage = "Usage: %prog activate -e ENSEMBLE"
    newstate = EnsembleStates.ACTIVE

class HoldCommand(StateChangeCommand):
    description = "Hold active ensemble"
    usage = "Usage: %prog hold -e ENSEMBLE"
    newstate = EnsembleStates.HELD

class ConfigCommand(EnsembleClientCommand):
    description = "Change ensemble configuration"
    usage = "Usage: %prog config [options] -e ENSEMBLE"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        self.parser.add_option("-P", "--max-planning", action="store", dest="max_planning",
            default=None, type="int", help="Maximum number of workflows being planned at once")
        self.parser.add_option("-R", "--max-running", action="store", dest="max_running",
            default=None, type="int", help="Maximum number of workflows running at once")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")

        request = {}

        if self.options.max_planning:
            request["max_planning"] = self.options.max_planning
        if self.options.max_running:
            request["max_running"] = self.options.max_running

        if len(request) == 0:
            self.parser.error("Specify --max-planning or --max-running")

        response = self.post("/ensembles/%s" % self.options.ensemble, data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Max Planning:",result["max_planning"]
        print "Max Running:",result["max_running"]

class WorkflowStateChangeCommand(EnsembleClientCommand):
    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"state": self.newstate}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "State:", result["state"]

class ReplanCommand(WorkflowStateChangeCommand):
    description = "Replan failed workflow"
    usage = "Usage: %prog replan -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.READY

class RerunCommand(WorkflowStateChangeCommand):
    description = "Rerun failed workflow"
    usage = "Usage: %prog rerun -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.QUEUED

class AbortCommand(WorkflowStateChangeCommand):
    description = "Abort workflow"
    usage = "Usage: %prog abort -e ENSEMBLE -w WORKFLOW"
    newstate = EnsembleWorkflowStates.ABORTED

class PriorityCommand(EnsembleClientCommand):
    description = "Update workflow priority"
    usage = "Usage: %prog priority -e ENSEMBLE -w WORKFLOW -p PRIORITY"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        add_ensemble_option(self)
        add_workflow_option(self)
        self.parser.add_option("-p","--priority",action="store",dest="priority",
                default=None,type="int",help="New workflow priority")

    def run(self):
        if self.options.ensemble is None:
            self.parser.error("Specify -e/--ensemble")
        if self.options.workflow is None:
            self.parser.error("Specify -w/--workflow")
        if self.options.priority is None:
            self.parser.error("Specify -p/--priority")

        if len(self.args) > 0:
            self.parser.error("Invalid argument")

        request = {"priority": self.options.priority}

        response = self.post("/ensembles/%s/workflows/%s" % (self.options.ensemble, self.options.workflow), data=request)

        result = response.json()

        if response.status_code != 200:
            print "ERROR:", result["message"]
            exit(1)

        print "Priority:", result["priority"]

class EnsembleCommand(CompoundCommand):
    description = "Client for ensemble management"
    commands = [
        ("server", ServerCommand),
        ("ensembles", EnsemblesCommand),
        ("create", CreateCommand),
        ("pause", PauseCommand),
        ("activate", ActivateCommand),
        ("config", ConfigCommand),
        ("submit", SubmitCommand),
        ("workflows", WorkflowsCommand),
        ("status", StatusCommand),
        ("replan", ReplanCommand),
        ("rerun", RerunCommand),
        ("priority", PriorityCommand)
    ]

def main():
    "The entry point for pegasus-em"
    EnsembleCommand().main()

