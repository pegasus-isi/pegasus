import logging
import os
import sys
import time
import argparse 
import re
from urllib import parse as urlparse
from multiprocessing.connection import Client

import requests

from Pegasus.command import Command, CompoundCommand, LoggingCommand
from Pegasus.db.ensembles import EnsembleStates, EnsembleWorkflowStates
from Pegasus.service.ensembles import emapp, manager
from Pegasus.service.ensembles.trigger import TriggerManagerMessage, TriggerManager

log = logging.getLogger(__name__)

EM_PORT = os.getuid() + 7919


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
        headers = {"accept": "application/json"}
        defaults = {"auth": (self.username, self.password), "headers": headers}
        defaults.update(kwargs)
        url = urlparse.urljoin(self.endpoint, path)
        response = requests.request(method, url, **defaults)

        if 200 <= response.status_code < 300:
            return response

        try:
            result = response.json()
            print("ERROR:", result["message"])
        except Exception:
            print("ERROR:", response.text)

        exit(1)

    def get(self, path, **kwargs):
        return self._request("get", path, **kwargs)

    def post(self, path, **kwargs):
        return self._request("post", path, **kwargs)

    def delete(self, path, **kwargs):
        return self._request("delete", path, **kwargs)

    def put(self, path, **kwargs):
        return self._request("put", path, **kwargs)

    def splitew(self, ew):
        r = ew.split(".")
        if len(r) != 2:
            self.parser.error("Invalid ENSEMBLE.WORKFLOW: %s" % ew)
        return r


class ServerCommand(LoggingCommand):
    description = "Start ensemble manager"
    usage = "%prog [options]"

    def __init__(self):
        LoggingCommand.__init__(self)
        self.parser.add_option(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            default=None,
            help="Enable debugging",
        )

    def run(self):
        if self.options.debug:
            emapp.config.update(DEBUG=True)
            logging.getLogger().setLevel(logging.DEBUG)

        # We only start the ensemble manager if we are not debugging
        # or if we are debugging and Werkzeug is restarting. This
        # prevents us from having two ensemble managers running in
        # the debug case.
        WERKZEUG_RUN_MAIN = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
        DEBUG = emapp.config.get("DEBUG", False)
        if (not DEBUG) or WERKZEUG_RUN_MAIN:
            # Make sure the environment is OK for the ensemble manager
            try:
                manager.check_environment()
            except manager.EMError as e:
                log.warning("%s: Ensemble manager disabled" % e.message)
            else:
                em_mgr = manager.EnsembleManager()
                em_mgr.start()

                trigger_mgr = TriggerManager()
                trigger_mgr.start()

        if os.getuid() == 0:
            log.fatal("The ensemble manager should not be run as root")
            exit(1)

        emapp.run(port=EM_PORT, host="127.0.0.1")

        log.info("Exiting")


def formatts(ts):
    t = time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S %Z", t)


class EnsemblesCommand(EnsembleClientCommand):
    description = "List ensembles"
    usage = "Usage: %prog ensembles"

    def run(self):
        response = self.get("/ensembles")
        result = response.json()
        fmt = "%-20s %-8s %-24s %-24s %12s %12s"
        if len(result) > 0:
            print(
                fmt
                % ("NAME", "STATE", "CREATED", "UPDATED", "MAX PLANNING", "MAX RUNNING")
            )
        for r in result:
            print(
                fmt
                % (
                    r["name"],
                    r["state"],
                    formatts(r["created"]),
                    formatts(r["updated"]),
                    r["max_planning"],
                    r["max_running"],
                )
            )


class CreateCommand(EnsembleClientCommand):
    description = "Create ensemble"
    usage = "Usage: %prog create [options] ENSEMBLE"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        self.parser.add_option(
            "-P",
            "--max-planning",
            action="store",
            dest="max_planning",
            default=1,
            type="int",
            help="Maximum number of workflows being planned at once",
        )
        self.parser.add_option(
            "-R",
            "--max-running",
            action="store",
            dest="max_running",
            default=1,
            type="int",
            help="Maximum number of workflows running at once",
        )

    def run(self):
        if len(self.args) != 1:
            self.parser.error("Specify ENSEMBLE")

        name = self.args[0]

        request = {
            "name": name,
            "max_planning": self.options.max_planning,
            "max_running": self.options.max_running,
        }

        response = self.post("/ensembles", data=request)


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
    usage = "Usage: %prog submit [options] ENSEMBLE.WORKFLOW plan_command [arg...]"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        self.parser.disable_interspersed_args()
        self.parser.add_option(
            "-p",
            "--priority",
            action="store",
            dest="priority",
            default=0,
            help="Workflow priority",
            metavar="NUMBER",
        )

    def run(self):
        o = self.options
        p = self.parser

        if len(self.args) < 2:
            p.error("Specify ENSEMBLE.WORKFLOW and planning command")

        ew = self.args[0]
        command = self.args[1]
        args = self.args[2:]

        ensemble, workflow = self.splitew(ew)

        exe = pathfind(command)
        if exe is None:
            p.error("invalid planning command: %s" % command)

        args.insert(0, exe)

        command = '"%s"' % '" "'.join(args)

        data = {
            "name": workflow,
            "priority": o.priority,
            "basedir": os.getcwd(),
            "plan_command": command,
        }

        response = self.post("/ensembles/%s/workflows" % ensemble, data=data)


class WorkflowsCommand(EnsembleClientCommand):
    description = "List workflows in ensemble"
    usage = "Usage: %prog workflows [options] ENSEMBLE."

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        self.parser.add_option(
            "-l",
            "--long",
            action="store_true",
            dest="long",
            default=False,
            help="Show detailed output",
        )

    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        response = self.get("/ensembles/%s/workflows" % self.args[0])

        result = response.json()

        if len(result) == 0:
            return

        if self.options.long:
            for w in result:
                print("ID:      ", w["id"])
                print("Name:    ", w["name"])
                print("Created: ", formatts(w["created"]))
                print("Updated: ", formatts(w["updated"]))
                print("State:   ", w["state"])
                print("Priority:", w["priority"])
                print("UUID:    ", w["wf_uuid"])
                print("URL:     ", w["href"])
                print()
        else:
            fmt = "%-20s %-15s %-8s %-24s %-24s"
            print(fmt % ("NAME", "STATE", "PRIORITY", "CREATED", "UPDATED"))
            for w in result:
                print(
                    fmt
                    % (
                        w["name"],
                        w["state"],
                        w["priority"],
                        formatts(w["created"]),
                        formatts(w["updated"]),
                    )
                )


class StatusCommand(EnsembleClientCommand):
    description = "Check workflow status"
    usage = "Usage: %prog status ENSEMBLE.WORKFLOW"

    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE.WORKFLOW")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        ensemble, workflow = self.splitew(self.args[0])

        response = self.get("/ensembles/{}/workflows/{}".format(ensemble, workflow))

        result = response.json()

        print("ID:           %s" % result["id"])
        print("Name:         %s" % result["name"])
        print("Plan Command: %s" % result["plan_command"])
        print("Created:      %s" % formatts(result["created"]))
        print("Updated:      %s" % formatts(result["updated"]))
        print("State:        %s" % result["state"])
        print("UUID:         %s" % (result["wf_uuid"] or ""))
        print("Priority:     %s" % result["priority"])
        print("Base Dir:     %s" % result["basedir"])
        print("Submit Dir:   %s" % (result["submitdir"] or ""))
        print("Log:          %s" % result["log"])


class AnalyzeCommand(EnsembleClientCommand):
    description = "Analyze workflow status"
    usage = "Usage: %prog analyze ENSEMBLE.WORKFLOW"

    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE.WORKFLOW")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        ensemble, workflow = self.splitew(self.args[0])

        response = self.get(
            "/ensembles/{}/workflows/{}/analyze".format(ensemble, workflow)
        )

        sys.stdout.write(response.text)


class StateChangeCommand(EnsembleClientCommand):
    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        response = self.post(
            "/ensembles/%s" % self.args[0], data={"state": self.newstate}
        )
        result = response.json()

        print("State:", result["state"])


class PauseCommand(StateChangeCommand):
    description = "Pause active ensemble"
    usage = "Usage: %prog pause ENSEMBLE"
    newstate = EnsembleStates.PAUSED


class ActivateCommand(StateChangeCommand):
    description = "Activate paused or held ensemble"
    usage = "Usage: %prog activate ENSEMBLE"
    newstate = EnsembleStates.ACTIVE


class HoldCommand(StateChangeCommand):
    description = "Hold active ensemble"
    usage = "Usage: %prog hold ENSEMBLE"
    newstate = EnsembleStates.HELD


class ConfigCommand(EnsembleClientCommand):
    description = "Change ensemble configuration"
    usage = "Usage: %prog config [options] ENSEMBLE"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        self.parser.add_option(
            "-P",
            "--max-planning",
            action="store",
            dest="max_planning",
            default=None,
            type="int",
            help="Maximum number of workflows being planned at once",
        )
        self.parser.add_option(
            "-R",
            "--max-running",
            action="store",
            dest="max_running",
            default=None,
            type="int",
            help="Maximum number of workflows running at once",
        )

    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        ensemble = self.args[0]

        request = {}

        if self.options.max_planning:
            request["max_planning"] = self.options.max_planning
        if self.options.max_running:
            request["max_running"] = self.options.max_running

        if len(request) == 0:
            self.parser.error("Specify --max-planning or --max-running")

        response = self.post("/ensembles/%s" % ensemble, data=request)

        result = response.json()

        print("Max Planning:", result["max_planning"])
        print("Max Running:", result["max_running"])


class WorkflowStateChangeCommand(EnsembleClientCommand):
    def run(self):
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE.WORKFLOW")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        ensemble, workflow = self.splitew(self.args[0])

        request = {"state": self.newstate}

        response = self.post(
            "/ensembles/{}/workflows/{}".format(ensemble, workflow), data=request
        )

        result = response.json()

        print("State:", result["state"])


class ReplanCommand(WorkflowStateChangeCommand):
    description = "Replan failed workflow"
    usage = "Usage: %prog replan ENSEMBLE.WORKFLOW"
    newstate = EnsembleWorkflowStates.READY


class RerunCommand(WorkflowStateChangeCommand):
    description = "Rerun failed workflow"
    usage = "Usage: %prog rerun ENSEMBLE.WORKFLOW"
    newstate = EnsembleWorkflowStates.QUEUED


class AbortCommand(WorkflowStateChangeCommand):
    description = "Abort workflow"
    usage = "Usage: %prog abort ENSEMBLE.WORKFLOW"
    newstate = EnsembleWorkflowStates.ABORTED


class PriorityCommand(EnsembleClientCommand):
    description = "Update workflow priority"
    usage = "Usage: %prog priority ENSEMBLE.WORKFLOW -p PRIORITY"

    def __init__(self):
        EnsembleClientCommand.__init__(self)
        self.parser.add_option(
            "-p",
            "--priority",
            action="store",
            dest="priority",
            default=None,
            type="int",
            help="New workflow priority",
        )

    def run(self):
        if self.options.priority is None:
            self.parser.error("Specify -p/--priority")
        if len(self.args) == 0:
            self.parser.error("Specify ENSEMBLE.WORKFLOW")
        if len(self.args) > 1:
            self.parser.error("Invalid argument")

        ensemble, workflow = self.splitew(self.args[0])

        request = {"priority": self.options.priority}

        response = self.post(
            "/ensembles/{}/workflows/{}".format(ensemble, workflow), data=request
        )

        result = response.json()

        print("Priority:", result["priority"])

# --- Trigger Commands ---------------------------------------------------------

class TriggerCommand:
    """Base class for any Trigger related commands"""

    description = None
    usage = None

    def __init__(self):
        self.parser = argparse.ArgumentParser(
            usage=self.usage, description=self.description
        )

        # authentication
        self.endpoint = "http://127.0.0.1:%d/" % EM_PORT
        self.username = emapp.config["USERNAME"]
        if not self.username:
            raise Exception("Specify USERNAME in configuration")
        self.password = emapp.config["PASSWORD"]
        if not self.password:
            raise Exception("Specify PASSWORD in configuration")

    def run(self):
        """Command logic. To be overwritten in derived class"""
        raise NotImplementedError

    def main(self, args=None):
        self.args = self.parser.parse_args(args)
        self.run()
    
    def send_msg(self, msg: TriggerManagerMessage):
        """Send a TriggerManagerMessage to the TriggerManager

        :param msg: message to be sent
        :type msg: TriggerManagerMessage
        :raises ConnectionRefusedError: unable to connect to TriggerManager daemon
        """

        try:
            address = ('localhost', 3000)
            conn = Client(address, authkey=b"123")
            conn.send(msg)
            conn.close()
        except ConnectionRefusedError:
            print("Unable to connect to TriggerManager. Has the Ensemble Manager been started?")

class StopTriggerCommand(TriggerCommand):
    """Command to stop a given trigger"""

    description = "Stop a specific trigger"
    usage = "Usage: pegasus-em stop-trigger ENSEMBLE TRIGGER_NAME"

    def __init__(self):
        TriggerCommand.__init__(self)
        self.parser.add_argument(
            "ensemble",
            type=str,
            help="The ensemble that this trigger belongs to"
        )

        self.parser.add_argument(
            "trigger_name",
            type=str,
            help="The name of the trigger to stop"
        )
    
    def run(self):
        self.send_msg(        
                TriggerManagerMessage(
                    TriggerManagerMessage.STOP_TRIGGER,
                    **vars(self.args)
                )
            )

class StartPatternIntervalTriggerCommand(TriggerCommand):
    """Command to start a timed interval and file pattern based trigger"""

    description = "Start a timed, pattern, based trigger"
    usage = "pegasus-em trigger -e ENSEMBLE -t TRIGGER_NAME -p PREFIX -f FILE_PATTERN -w WORKFLOW_SCRIPT -i INTERVAL [-a 'ADDITIONAL_ARGS']"

    def __init__(self):
        TriggerCommand.__init__(self)
        # using required named args instead of positional args to avoid misordering
        self.parser.add_argument(
            "-e",
            "--ensemble",
            required=True,
            type=str,
            help="The name of the ensemble to which workflows started by this trigger will be added"
        )

        self.parser.add_argument(
            "-t",
            "--trigger-name",
            required=True,
            type=str,
            help="The a unique name that this trigger can be refferred by"
        )

        self.parser.add_argument(
            "-p",
            "--workflow-name-prefix",
            required=True,
            type=str,
            help="A prefix that will be attached to workflow names"
        )

        self.parser.add_argument(
            "-f",
            "--file-pattern",
            required=True,
            type=str,
            help="A file pattern that will be passed to glob.Glob to collect files"
        )

        self.parser.add_argument(
            "-w",
            "--workflow-script",
            required=True,
            type=str,
            help="The workflow script to be executed"
        )

        self.parser.add_argument(
            "-i",
            "--interval",
            required=True,
            type=str,
            help="Duration of each trigger interval. Must be given as '<int> <s|m|h|d>'"
        )

        self.parser.add_argument(
            "-a",
            "--additional-args",
            required=False,
            default=None,
            type=str,
            help="Additional args to be passed to the workflow script"
        )

    @staticmethod
    def to_seconds(value: str) -> int:
        """Convert time unit given as '<int> <s|m|h|d>` to seconds.

        :param value: input str
        :type value: str
        :raises ValueError: value must be given as '<int> <s|m|h|d>
        :raises ValueError: result must be > 0s
        :return: value given in seconds
        :rtype: int
        """

        value = value.strip()
        pattern = re.compile(r"\d+ *[sSmMhHdD]")
        if not pattern.fullmatch(value):
            raise ValueError("invalid interval: {}, interval must be given as '<int> <s|m|h|d>".format(self.args.interval))

        num = int(value[0:len(value)-1])
        unit = value[-1].lower()

        if unit not in "smhd":
            raise ValueError("invalid unit: {}, unit must be one of s, S, m, M, h, H, d, D")

        as_seconds = {
            "s": 1,
            "m": 60,
            "h": 60 * 60,
            "d": 60 * 60 * 24
        }

        return as_seconds[unit] * num

    def run(self):
        # get get interval as seconds
        interval = StartPatternIntervalTriggerCommand.to_seconds(self.args.interval)

        if interval <= 0:
            raise ValueError("invalid interval: {}, must be greater than 0 seconds".format(interval))

        kwargs = vars(self.args)
        # replace str interval with interval in seconds as int
        kwargs["interval"] = interval

        self.send_msg(
            TriggerManagerMessage(
                TriggerManagerMessage.START_PATTERN_INTERVAL_TRIGGER, 
                **kwargs
            )
        )

# TODO: implement
class TriggerStatusCommand(EnsembleClientCommand):
    pass

# ------------------------------------------------------------------------------

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
        ("analyze", AnalyzeCommand),
        ("replan", ReplanCommand),
        ("rerun", RerunCommand),
        ("priority", PriorityCommand),
        ("trigger", StartPatternIntervalTriggerCommand),
        ("stop-trigger", StopTriggerCommand)
    ]
    aliases = {
        "c": "create",
        "e": "ensembles",
        "w": "workflows",
        "sub": "submit",
        "an": "analyze",
        "st": "status",
    }


def main():
    "The entry point for pegasus-em"
    EnsembleCommand().main()
