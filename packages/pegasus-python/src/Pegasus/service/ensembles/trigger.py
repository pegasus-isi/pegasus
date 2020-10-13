import datetime
import os
import json
import threading
import time
import logging
import subprocess

from pathlib import Path
from typing import Optional, List

from Pegasus import user
from Pegasus.db import connection
from Pegasus.db.ensembles import Trigger, TriggerType

# --- setup dir for trigger related log files ----------------------------------
_TRIGGER_DIR = Path().home() / ".pegasus/triggers"

# --- manager ------------------------------------------------------------------
class TriggerManager(threading.Thread):
    def __init__(
        self,
    ):
        threading.Thread.__init__(self, daemon=True)

        self.log = logging.getLogger("trigger.manager")

        # interval in seconds at which the trigger manager will query the
        # database to see if there is work to do (start, stop, restart) triggers
        self.polling_rate = 15

        # key: (<ensenble_id>, <trigger_name>), value: handle to trigger thread
        self.running = dict()

        self.trigger_dao = None

    def run(self):
        self.log.info("trigger manager starting")

        while True:
            # TODO: user will always be the same.., keep this in the loop or move it out
            u = user.get_user_by_uid(os.getuid())
            session = connection.connect(
                u.get_master_db_url(), connect_args={"check_same_thread": False}
            )

            try:
                self.trigger_dao = Trigger(session)
                triggers = self.trigger_dao.list_triggers()
                self.log.info("processing {} triggers".format(len(triggers)))

                for t in triggers:
                    if t.state == "READY":
                        # self.start_trigger(t)
                        print("see trigger in ready state")
                    elif (
                        t.state == "RUNNING"
                        and TriggerManager.get_tname(t) not in self.running
                    ):
                        # restart
                        """
                        self.log.debug(
                            "{} not in memory, restarting it".format(
                                TriggerManager.get_tname(t)
                            )
                        )
                        self.start_trigger(t)
                        """
                        print("see trigger in RUNNING state, but not in memory")
                    elif t.state == "STOPPED":
                        # self.stop_trigger(t)
                        print("see trigger in stopped state")
            finally:
                session.close()

            time.sleep(self.polling_rate)

    def start_trigger(self, trigger: Trigger):
        trigger_name = TriggerManager.get_tname(trigger)
        self.log.debug("starting {}".format(trigger_name))
        kwargs = json.loads(trigger.args)

        # create trigger thread
        if trigger.type == TriggerType.CHRON:
            t = ChronTrigger(**kwargs)
        elif trigger.type == TriggerType.FILE_PATTERN:
            t = FilePatternTrigger(**kwargs)

        # keep ref to trigger thread
        self.running[trigger_name] = t

        # update state
        self.log.debug(
            "changing {name} state: {old_state} -> {new_state}".format(
                name=trigger_name,
                old_state=trigger.state,
                new_state="RUNNING",
            )
        )
        self.trigger_dao.update_state(
            ensemble_id=trigger.ensemble_id, trigger_id=trigger.id, new_state="RUNNING"
        )

    def stop_trigger(self, trigger: Trigger):
        # using reference to trigger, tell trigger to shutdown

        # remove entry from database
        pass

    @staticmethod
    def get_tname(trigger: Trigger) -> tuple:
        return (trigger.ensemble_id, trigger.name)


# trigger threads --------------------------------------------------------------
class TriggerThread(threading.Thread):
    def __init__(
        self,
        ensemble: str,
        trigger: str,
        workflow_script: str,
        workflow_args: List[str] = [],
    ):

        threading.Thread.__init__(self, name="::".join(ensemble, trigger))

        self.log = logging.getLogger("trigger.{}".format(self.name))
        self.ensemble = ensemble
        self.trigger = trigger
        self.workflow_cmd = [wf]
        self.workflow_cmd.extend(workflow_args)

        # thread stopping condition
        self.stop_event = threading.Event()

    def shutdown(self):
        """Gracefully shutdown this thread."""
        self.log.info("shutting down".format(self.name))
        self.stop_event.set()


class ChronTrigger(TriggerThread):
    def __init__(
        self,
        ensemble: str,
        trigger: str,
        interval: int,
        timeout: int,
        workflow_script: str,
        workflow_args: Optional[List[str]] = None,
    ):

        TriggerThread.__init__(
            self,
            ensemble=ensemble,
            trigger=trigger,
            workflow_script=workflow_script,
            workflow_args=workflow_args,
        )

        self.timeout = timeout
        self.interval = interval
        self.elapsed = 0

    def __repr__(self):
        return "<ChronTrigger {} interval={}s>".format(self.name, self.interval)

    def run(self):
        try:
            self.log.debug("starting")

            while not self.stop_even.isSet():
                cmd = [
                    "pegasus-em",
                    "submit",
                    self.ensemble,
                    "{}_{}".format(self.trigger, datetime.datetime.now().timestamp()),
                    self.workflow_cmd,
                ]

                cp = subprocess.run(cmd, stderr=subprocess.PIPE)

                if cp.returncode == 0:
                    self.log.info("executed cmd: {}".format(cmd))
                else:
                    self.log.error("encountered an error executing: {}".format(cmd))
                    raise RuntimeError(cp.stderr.decode())

                time.sleep(self.interval)
                self.elapsed += self.interval

                if self.elapsed >= self.timeout:
                    self.log.debug("timed out")
                    break

        except Exception as e:
            self.log.exception("error")
        finally:
            self.log.debug("exited")


class FilePatternTrigger(threading.Thread):
    pass