import datetime
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from glob import glob
from pathlib import Path
from typing import List, Optional

from Pegasus import user
from Pegasus.db import connection
from Pegasus.db.ensembles import Ensembles, Triggers, TriggerType
from Pegasus.db.schema import Trigger


# --- manager ------------------------------------------------------------------
class TriggerManager(threading.Thread):
    """
    Manages workflow triggers. Work is done based on the contents of the
    "trigger" table, and the state of each trigger in that table.
    """

    def __init__(self,):
        threading.Thread.__init__(self, daemon=True)

        self.log = logging.getLogger("trigger.manager")

        # interval in seconds at which the trigger manager will query the
        # database to see if there is work to do (start, stop, restart) triggers
        self.polling_rate = 15

        # references to currently running trigger threads
        # key: (<ensenble_id>, <trigger_name>), value: handle to trigger thread
        self.running = dict()

        self.trigger_dao = None
        self.ensemble_dao = None

    def run(self):
        """Trigger manager main loop."""

        self.log.info("trigger manager starting")

        while True:
            # TODO: user will always be the same.., keep this in the loop or move it out
            u = user.get_user_by_uid(os.getuid())
            session = connection.connect(
                u.get_master_db_url(), connect_args={"check_same_thread": False}
            )

            try:
                self.trigger_dao = Triggers(session)
                self.ensemble_dao = Ensembles(session)
                triggers = self.trigger_dao.list_triggers()
                self.log.info("processing {} triggers".format(len(triggers)))
                for t in triggers:
                    t_name = TriggerManager.get_tname(t)
                    if t.state == "READY":
                        self.start_trigger(t)
                    elif t.state == "RUNNING" and t_name not in self.running:
                        # restart
                        self.log.debug("{} not in memory, restarting it".format(t_name))
                        self.start_trigger(t)
                    elif t.state == "RUNNING" and not self.running[t_name].is_alive():
                        # exited
                        self.log.debug(
                            "{} exited, removing references to it".format(t_name)
                        )
                        self.stop_trigger(t)
                    elif t.state == "STOPPED":
                        self.stop_trigger(t)
            finally:
                session.close()

            time.sleep(self.polling_rate)

    def start_trigger(self, trigger: Trigger):
        """Given a trigger, start the appropriate trigger thread.

        :param trigger: the trigger to be started
        :type trigger: Trigger
        """

        trigger_name = TriggerManager.get_tname(trigger)
        self.log.debug("starting {}".format(trigger_name))

        workflow = json.loads(trigger.workflow)
        required_args = {
            "ensemble_id": trigger.ensemble_id,
            "ensemble": self.ensemble_dao.get_ensemble_name(trigger.ensemble_id),
            "trigger": trigger.name,
            "workflow_script": workflow["script"],
            "workflow_args": workflow["args"] if workflow["args"] else [],
        }
        trigger_specific_kwargs = json.loads(trigger.args)

        # create trigger thread
        if trigger._type == TriggerType.CRON.value:
            t = CronTrigger(**required_args, **trigger_specific_kwargs)

        elif trigger._type == TriggerType.FILE_PATTERN.value:
            t = FilePatternTrigger(**required_args, **trigger_specific_kwargs)

        else:
            raise NotImplementedError(
                "unsupported trigger type: {}".format(trigger.type)
            )

        # keep ref to trigger thread
        self.running[trigger_name] = t
        t.start()

        # update state
        self.log.debug(
            "changing {name} state: {old_state} -> {new_state}".format(
                name=trigger_name, old_state=trigger.state, new_state="RUNNING",
            )
        )
        self.trigger_dao.update_state(
            ensemble_id=trigger.ensemble_id, trigger_id=trigger._id, new_state="RUNNING"
        )

    def stop_trigger(self, trigger: Trigger):
        """Stop a trigger thread.

        :param trigger: the trigger to be stopped
        :type trigger: Trigger
        """
        # using reference to trigger, tell trigger to shutdown
        target_trigger = TriggerManager.get_tname(trigger)
        self.log.debug("stopping {}".format(target_trigger))
        self.running[target_trigger].shutdown()
        del self.running[target_trigger]

        # remove entry from database
        self.trigger_dao.delete_trigger(trigger.ensemble_id, trigger.name)

    @staticmethod
    def get_tname(trigger: Trigger) -> tuple:
        """Given a trigger object, get its name as a pair (<ensemble_id>, <trigger_name>)

        :param trigger: the trigger
        :type trigger: Trigger
        :return: pair (<ensemble_id>, <trigger_name>)
        :rtype: tuple
        """
        return (trigger.ensemble_id, trigger.name)


# trigger threads --------------------------------------------------------------
class TriggerThread(threading.Thread):
    """Base class for trigger thread implementations."""

    def __init__(
        self,
        ensemble_id: int,
        ensemble: str,
        trigger: str,
        workflow_script: str,
        workflow_args: List[str] = [],
    ):

        threading.Thread.__init__(self, name=(ensemble_id, trigger))

        self.log = logging.getLogger("trigger.{}::{}".format(ensemble_id, trigger))
        self.ensemble_id = ensemble_id
        self.ensemble = ensemble
        self.trigger = trigger
        self.workflow_cmd = [workflow_script]
        if workflow_args:
            self.workflow_cmd.extend(workflow_args)

        # thread stopping condition
        self.stop_event = threading.Event()

    def shutdown(self):
        """Gracefully shutdown this thread by setting the stop_event."""
        self.log.info("shutting down".format(self.name))
        self.stop_event.set()


class CronTrigger(TriggerThread):
    """Submits a workflow to the ensemble manager at a specified interval."""

    def __init__(
        self,
        ensemble_id: int,
        ensemble: str,
        trigger: str,
        interval: int,
        workflow_script: str,
        workflow_args: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        **kwargs
    ):

        TriggerThread.__init__(
            self,
            ensemble_id=ensemble_id,
            ensemble=ensemble,
            trigger=trigger,
            workflow_script=workflow_script,
            workflow_args=workflow_args,
        )

        self.timeout = int(timeout) if timeout else None
        self.interval = int(interval)
        self.elapsed = 0

    def __repr__(self):
        return "<CronTrigger {} interval={}s>".format(self.name, self.interval)

    def run(self):
        """CronTrigger main loop."""
        try:
            self.log.debug("starting")

            while not self.stop_event.isSet():
                cmd = [
                    "pegasus-em",
                    "submit",
                    "{ens}.{tr}_{ts}".format(
                        ens=self.ensemble,
                        tr=self.trigger,
                        ts=int(datetime.datetime.now().timestamp()),
                    ),
                ]
                cmd.extend(self.workflow_cmd)

                cp = subprocess.run(cmd, stderr=subprocess.PIPE)

                if cp.returncode == 0:
                    self.log.info("executed cmd: {}".format(cmd))
                else:
                    self.log.error("encountered an error executing cmd: {}".format(cmd))
                    stderr = (
                        cp.stderr.decode()
                        if isinstance(cp.stderr, bytes)
                        else cp.stderr
                    )
                    raise RuntimeError(stderr)

                time.sleep(self.interval)
                if self.timeout:
                    self.elapsed += self.interval

                    if self.elapsed >= self.timeout:
                        self.log.debug("timed out")
                        break

        except Exception:
            self.log.exception("error")
        finally:
            self.log.debug("exited")


class FilePatternTrigger(TriggerThread):
    """
    Collects files based on the given filepatterns and submits workflow to the
    ensemble manager at a specified interval.
    """

    def __init__(
        self,
        ensemble_id: str,
        ensemble: str,
        trigger: str,
        interval: int,
        workflow_script: str,
        workflow_args: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        **kwargs
    ):
        TriggerThread.__init__(
            self,
            ensemble_id=ensemble_id,
            ensemble=ensemble,
            trigger=trigger,
            workflow_script=workflow_script,
            workflow_args=workflow_args,
        )

        self.timeout = int(timeout) if timeout else None
        self.interval = int(interval)
        self.elapsed = 0
        self.file_patterns = kwargs["file_patterns"]

    def __repr__(self):
        return "<FilePatternTrigger {} interval={}s patterns={}>".format(
            self.name, self.interval, self.file_patterns
        )

    def run(self):
        """FilePatternTrigger main loop."""
        try:
            self.log.debug("starting")

            while not self.stop_event.isSet():

                files = self.collect_and_move_files()

                if len(files) > 0:
                    cmd = [
                        "pegasus-em",
                        "submit",
                        "{ens}.{tr}_{ts}".format(
                            ens=self.ensemble,
                            tr=self.trigger,
                            ts=int(datetime.datetime.now().timestamp()),
                        ),
                    ]
                    cmd.extend(self.workflow_cmd)
                    cmd.append("--inputs")
                    cmd.extend(files)

                    cp = subprocess.run(cmd, stderr=subprocess.PIPE)

                    if cp.returncode == 0:
                        self.log.info("executed cmd: {}".format(cmd))
                    else:
                        self.log.error(
                            "encountered error executing cmd: {}".format(cmd)
                        )
                        stderr = (
                            cp.stderr.decode()
                            if isinstance(cp.stderr, bytes)
                            else cp.stderr
                        )
                        raise RuntimeError(stderr)

                time.sleep(self.interval)
                if self.timeout:
                    self.elapsed += self.interval

                    if self.elapsed >= self.timeout:
                        self.log.debug("timed out")
                        break

        except Exception:
            self.log.exception("error")
        finally:
            self.log.debug("exited")

    def collect_and_move_files(self) -> List[str]:
        """
        Collect absolute paths of all files that match the given file
        patterns, then move those files into a newly created subdirectory
        meant for previously processed files. For example, if the patterns
        ["/inputs/*.txt", "/inputs2/*.txt] are given, and "/inputs/f1.txt" and
        "/inputs2/f2.txt" exist, the result will be that those two paths are returned
        AND those two files will be moved to "/inputs/processed" and "/inputs2/processed"
        respectively. This is to avoid looping over an increasingly larger set
        of files as more inputs arrive.

        :return: list of paths to matched files
        :rtype: List[str]
        """
        collected = []
        for pattern in self.file_patterns:
            # create dir to move files to if it doesn't already exist
            parent_dir = Path(pattern).parent.resolve()
            processed_dir = parent_dir / "processed"
            processed_dir.mkdir(parents=False, exist_ok=True)

            for match in glob(pattern):
                # move file into processed dir and add that path
                f = Path(match).resolve()
                dst = processed_dir / f.name

                shutil.move(str(f), str(dst))
                collected.append(str(dst))

        return collected
