import datetime
import fcntl
import logging
import math
import pickle
import queue
import subprocess
from enum import Enum
from glob import glob
from multiprocessing import Process
from multiprocessing.connection import Listener
from pathlib import Path
from threading import Event, Thread
from typing import List, Optional


class _TriggerManagerMessageType(Enum):
    """Message types to be handled by TriggerManager"""

    STOP_TRIGGER = 1
    START_PATTERN_INTERVAL_TRIGGER = 2
    STATUS = 3


class TriggerManagerMessage:
    """Messages to be handled by TriggerManager"""

    STOP_TRIGGER = _TriggerManagerMessageType.STOP_TRIGGER
    START_PATTERN_INTERVAL_TRIGGER = (
        _TriggerManagerMessageType.START_PATTERN_INTERVAL_TRIGGER
    )
    STATUS = _TriggerManagerMessageType.STATUS

    def __init__(self, _type: _TriggerManagerMessageType, **kwargs):
        self._type = _type
        self.kwargs = kwargs

    def __str__(self):
        return "<TriggerManagerMessage msg_type={}, kwargs={}>".format(
            self._type, self.kwargs
        )


class TriggerManager(Process):
    """
    Manager to be spawned by the pegasus-em server process. It will listen for 
    commands (in the form of TriggerManagerMessage's) by a peagsus-em client. 
    Any triggers created will be managed by this class. 
    """

    def __init__(self):
        Process.__init__(self, daemon=True)

        # TODO: add file handler so that logs can be written to ~/.pegasus/ensembles/triggers
        self._log = logging.getLogger("TriggerManager")

        # key="<ensemble>::<trigger_name>", value=trigger_thread
        self._running_triggers = dict()

        # triggers that can be removed from self._running_triggers
        self._to_del = queue.Queue()

        # make state visible to pegasus-em client via file
        trigger_dir = Path().home() / ".pegasus/triggers"
        trigger_dir.mkdir(parents=True, exist_ok=True)
        self._running_triggers_file = trigger_dir / "running.p"

        self._log.debug(
            "writing trigger state file to : {}".format(
                self._running_triggers_file.resolve()
            )
        )
        with self._running_triggers_file.open("wb") as f:
            pickle.dump(set(), f)

    def run(self):
        # TODO: make configurable
        address = ("localhost", 3000)
        self._log.info("starting at: {}".format(address))
        # TODO: change authkey
        with Listener(address, authkey=b"123") as listener:
            self._log.debug("listening for new connection")
            while True:
                # TODO: cleanup any threads that are no longer in running state
                with listener.accept() as conn:
                    self._log.debug(
                        "connection accepted from {}".format(listener.last_accepted)
                    )

                    msg = conn.recv()
                    self._log.debug("received message: {}".format(msg))

                    if msg._type == TriggerManagerMessage.STOP_TRIGGER:
                        self._stop_trigger_handler(**msg.kwargs)
                    elif (
                        msg._type
                        == TriggerManagerMessage.START_PATTERN_INTERVAL_TRIGGER
                    ):
                        self._start_pattern_interval_trigger_handler(**msg.kwargs)
                    elif msg._type == TriggerManagerMessage.STATUS:
                        self._status_handler(**msg.kwargs)
                    else:
                        self._log.error("invalid message: {}".format(msg))

    def _stop_trigger_handler(self, ensemble: str, trigger_name: str):
        """Handler for a STOP_TRIGGER message"""
        trigger = "::".join([ensemble, trigger_name])
        try:
            self._running_triggers[trigger].shutdown()
            del self._running_triggers[trigger]
            self._save_running_threads()

            self._log.info("stopped trigger {}".format(trigger))

        # case: trigger doesn't exist; should not happen as pegsus em client would
        # have thrown an exception after seeing that this trigger is not in running
        # triggers file
        except KeyError:
            self._log.error("TriggerManager does not contain: {}".format(trigger))

    def _start_pattern_interval_trigger_handler(
        self,
        ensemble: str,
        trigger_name: str,
        workflow_name_prefix: str,
        file_pattern: str,
        workflow_script: str,
        interval: int,
        additional_args: List[str],
    ):
        """Handler for a START_PATTERN_INTERVAL_TRIGGER message"""

        t = PatternIntervalTrigger(
            ensemble=ensemble,
            trigger_name=trigger_name,
            workflow_name_prefix=workflow_name_prefix,
            file_pattern=file_pattern,
            workflow_script=workflow_script,
            interval=interval,
            additional_args=additional_args,
        )

        if t.name not in self._running_triggers:
            self._running_triggers[t.name] = t
            self._save_running_threads()
            t.start()

        # case: trigger already exists, however this should never happen as pegasus em
        # client would have thrown an error after seeing it in the running
        # triggers file
        else:
            self._log.error(
                "Cannot overwrite existing trigger: {}::{}".format(
                    ensemble, trigger_name
                )
            )

    def _status_handler(self, ensemble: str, name: Optional[str] = None):
        """Handler for a STATUS message"""

        raise NotImplementedError("trigger status not yet implemented")

    def _save_running_threads(self):
        """
        Overwrite ~/.pegasus/triggers/running.p with the current set of
        running triggers.
        """
        running_triggers = {t for t in self._running_triggers}
        self._log.debug(
            "writing {} to {}".format(running_triggers, self._running_triggers_file)
        )

        with self._running_triggers_file.open("wb") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            pickle.dump(running_triggers, f)
            fcntl.flock(f, fcntl.LOCK_UN)


class PatternIntervalTrigger(Thread):
    """Time interval and file pattern based workflow trigger."""

    def __init__(
        self,
        *,
        ensemble: str,
        trigger_name: str,
        workflow_name_prefix: str,
        file_pattern: str,
        workflow_script: str,
        interval: int,
        additional_args: Optional[str] = None
    ):
        Thread.__init__(self, name="::".join([ensemble, trigger_name]), daemon=True)
        # TODO: add file handler so that logs can be written to ~/.pegasus/ensembles/triggers
        self._log = logging.getLogger("PatternIntervalTrigger")
        self._stop_event = Event()

        self.ensemble = ensemble
        self.trigger_name = trigger_name
        self.workflow_name_prefix = workflow_name_prefix
        self.file_pattern = file_pattern
        self.workflow_script = workflow_script
        self.interval = interval
        self.additional_args = additional_args
        self.last_ran = 0

        # TODO: add some accounting like list of workflows started, total time running, etc.

    def __str__(self):
        return "<PatternIntervalTrigger {}::{}>".format(
            self.ensemble, self.trigger_name
        )

    def run(self):
        self._log.info("{} starting".format(self))

        time_now = datetime.datetime.now().timestamp()
        while not self._stop_event.isSet():
            # get paths of all files that match pattern with mod date s.t.
            # self._last_ran <= mod date < time_now
            input_files = []
            for match in glob(self.file_pattern):
                p = Path(match).resolve()

                if self.last_ran <= p.stat().st_mtime and p.stat().st_mtime < time_now:
                    input_files.append(str(p))

            # early termination condition
            if len(input_files) == 0:
                self._log.info(
                    "{} encountered no new input files, shutting down".format(self)
                )
                break

            # build up peagsus-em submit command
            # example: pegasus-em submit myruns.run1 ./workflow.py --inputs /f1.txt f2.txt
            cmd = [
                "pegasus-em",
                "submit",
                "{}.{}_{}".format(
                    self.ensemble, self.workflow_name_prefix, math.floor(time_now)
                ),
                self.workflow_script,
                "--inputs",
            ]

            cmd.extend(input_files)

            if self.additional_args:
                cmd.extend(self.additional_args.split())

            self._log.debug("executing command: {}".format(" ".join(cmd)))

            # invoke pegasus-em submit
            cp = subprocess.run(cmd)

            if cp.returncode != 0:
                self._log.error(
                    "Encountered error submitting workflow with: {}, shutting down".format(
                        cmd
                    )
                )
                break

            # update last time ran to the time of current iteration
            self.last_ran = time_now

            # sleep for interval
            time.sleep(self.interval)

        self._log.info("{} done".format(self.name))

        # TODO: add myself to the q to be cleaned up

    def shutdown(self):
        """Gracefully shutdown this thread."""

        self._log.info("shutting down {}".format(self.trigger_name))
        self._stop_event.set()

        # TODO: add myself to the q to be cleaned  up
