import datetime
import fcntl
import logging
import math
import pickle
import queue
import subprocess
import time
from enum import Enum
from glob import glob
from multiprocessing.connection import Listener
from pathlib import Path
from threading import Event, Thread
from typing import List, Optional

# --- setup dir for trigger related files --------------------------------------
_TRIGGER_DIR = Path().home() / ".pegasus/triggers"

# --- messages -----------------------------------------------------------------
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


# --- manager ------------------------------------------------------------------
class TriggerManager(Thread):
    """
    Manager to be spawned by the pegasus-em server process. It will listen for
    commands (in the form of TriggerManagerMessage's) by a peagsus-em client.
    Any triggers created will be managed by this class.
    """

    def __init__(self):
        Thread.__init__(self, daemon=True)

        _TRIGGER_DIR.mkdir(parents=True, exist_ok=True)

        self.log = logging.getLogger("trigger.manager")

        # messages to be consumed by worker
        self.mailbox = queue.Queue()
        self.dispatcher = _TriggerDispatcher(self.mailbox)

    def run(self):
        # TODO: make configurable?
        address = ("localhost", 3000)
        self.log.info("starting and will listen on: {}".format(address))

        # start up trigger manager worker
        self.dispatcher.start()

        # TODO: change authkey?
        with Listener(address, authkey=b"123") as listener:
            while True:
                self.log.debug("listening for new connection")
                with listener.accept() as conn:
                    self.log.debug(
                        "connection accepted from {}".format(listener.last_accepted)
                    )

                    # handle message received from pegasus-em client
                    msg = conn.recv()
                    self.log.debug("received message: {}".format(msg))
                    self.mailbox.put(msg)


class _TriggerDispatcher(Thread):
    """Work thread of the TriggerManager"""

    def __init__(self, mailbox: queue.Queue):
        Thread.__init__(self, daemon=True)

        self.log = logging.getLogger("trigger.dispatcher")

        # make state visible to pegasus-em client via file
        self.running_triggers_file = _TRIGGER_DIR / "running.p"
        with self.running_triggers_file.open("wb") as f:
            pickle.dump(set(), f)

        # TODO: create file containing all workflows submitted to em by each trigger
        # self.submitted_workflows = _TRIGGER_DIR / "submitted.p"

        # work passed down by the TriggerManager
        self.mailbox = mailbox

        # key="<ensemble>::<trigger_name>", value=trigger_thread
        self.running = dict()

        # triggers that can be removed from self.running
        self.checkout = queue.Queue()

    def run(self):
        self.log.info("starting")

        work = {
            TriggerManagerMessage.START_PATTERN_INTERVAL_TRIGGER: self.start_pattern_interval_trigger_handler,
            TriggerManagerMessage.STOP_TRIGGER: self.stop_trigger_handler,
            TriggerManagerMessage.STATUS: self.status_handler,
        }

        while True:
            # handle accounting: remove references to any threads that have stopped
            has_changed = False
            while not self.checkout.empty():
                del self.running[self.checkout.get()]
                has_changed = True

            if has_changed:
                self.update_state_file()

            # handle work
            try:
                msg = self.mailbox.get(timeout=1)
                self.log.debug("received message: {}".format(msg))
                work[msg._type](**msg.kwargs)
            except queue.Empty:
                pass

    def stop_trigger_handler(self, ensemble: str, trigger_name: str):
        """Handler for a STOP_TRIGGER message"""
        trigger = "::".join([ensemble, trigger_name])
        try:
            self.running[trigger].shutdown()
            del self.running[trigger]
            self.update_state_file()

            self.log.info("stopped trigger {}".format(trigger))

        # case: trigger doesn't exist; should not happen as pegsus em client would
        # have thrown an exception after seeing that this trigger is not in running
        # triggers file
        except KeyError:
            self.log.error("TriggerManager does not contain: {}".format(trigger))

    def start_pattern_interval_trigger_handler(
        self,
        ensemble: str,
        trigger_name: str,
        workflow_name_prefix: str,
        file_patterns: List[str],
        workflow_script: str,
        interval: int,
        timeout: Optional[int] = None,
        additional_args: Optional[str] = None,
    ):
        """Handler for a START_PATTERN_INTERVAL_TRIGGER message"""

        t = _PatternIntervalTrigger(
            checkout=self.checkout,
            ensemble=ensemble,
            trigger_name=trigger_name,
            workflow_name_prefix=workflow_name_prefix,
            file_patterns=file_patterns,
            workflow_script=workflow_script,
            interval=interval,
            timeout=timeout,
            additional_args=additional_args,
        )

        if t.name not in self.running:
            self.running[t.name] = t
            self.update_state_file()
            t.start()

        # case: trigger already exists, however this should never happen as pegasus em
        # client would have thrown an error after seeing it in the running
        # triggers file
        else:
            self.log.error(
                "Cannot overwrite existing trigger: {}::{}".format(
                    ensemble, trigger_name
                )
            )

    def status_handler(self, ensemble: str, name: Optional[str] = None):
        """Handler for a STATUS message"""

        raise NotImplementedError("trigger status not yet implemented")

    def update_state_file(self):
        """
        Overwrite ~/.pegasus/triggers/running.p with the current set of
        running triggers.
        """
        running_triggers = {t for t in self.running}
        self.log.debug(
            "writing {} to {}".format(running_triggers, self.running_triggers_file)
        )

        with self.running_triggers_file.open("wb") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            pickle.dump(running_triggers, f)
            fcntl.flock(f, fcntl.LOCK_UN)


# --- trigger(s) ---------------------------------------------------------------
class _PatternIntervalTrigger(Thread):
    """Time interval and file pattern based workflow trigger."""

    def __init__(
        self,
        *,
        checkout: queue.Queue,
        ensemble: str,
        trigger_name: str,
        workflow_name_prefix: str,
        file_patterns: List[str],
        workflow_script: str,
        interval: int,
        timeout: Optional[str] = None,
        additional_args: Optional[str] = None
    ):
        Thread.__init__(self, name="::".join([ensemble, trigger_name]), daemon=True)

        self.log = logging.getLogger("trigger.trigger")
        self.stop_event = Event()
        self.checkout = checkout

        # workflow specific args
        self.ensemble = ensemble
        self.trigger_name = trigger_name
        self.workflow_name_prefix = workflow_name_prefix
        self.file_patterns = file_patterns
        self.workflow_script = workflow_script
        self.interval = interval
        self.additional_args = additional_args

        # state of trigger
        self.last_ran = 0
        self.start_time = 0
        self.timeout = timeout

    def __str__(self):
        return "<PatternIntervalTrigger {}>".format(self.name)

    def run(self):
        self.log.info("{} starting".format(self.name))
        self.start_time = datetime.datetime.now().timestamp()

        while not self.stop_event.isSet():
            time_now = datetime.datetime.now().timestamp()

            # check if timeout condition met if it is set
            if self.timeout and (time_now - self.start_time) >= self.timeout:
                self.log.info("{} timed out".format(self.name))
                break

            # get paths of all files that match pattern with mod date s.t.
            # self._last_ran <= mod date < time_now
            input_files = []
            for pattern in self.file_patterns:
                for match in glob(pattern):
                    p = Path(match)

                    # if a symlink matched, using the symlink's info and not
                    # the resolved file
                    if (
                        self.last_ran <= p.lstat().st_mtime
                        and p.lstat().st_mtime < time_now
                    ):
                        if p.exists():
                            input_files.append(str(p.resolve()))
                        else:
                            self.log.debug(
                                "File {} does not exist and will not be included in workflow submission".format(
                                    p
                                )
                            )

            if len(input_files) == 0:
                self.log.info(
                    "{} encountered no new input files for interval {}".format(
                        self.name,
                        "[{} - {})".format(
                            datetime.datetime.fromtimestamp(self.last_ran).isoformat(),
                            datetime.datetime.fromtimestamp(time_now).isoformat(),
                        ),
                    )
                )
            else:
                # build up peagsus-em submit command
                # example: pegasus-em submit myruns.run1 ./workflow.py --inputs /f1.txt f2.txt
                cmd = [
                    "pegasus-em",
                    "submit",
                    "{}.{}_{}".format(
                        self.ensemble, self.workflow_name_prefix, math.floor(time_now)
                    ),
                    self.workflow_script,
                ]

                # add any additional arguments passed (positional & named)
                if self.additional_args:
                    cmd.extend(self.additional_args.split())

                # add files passed as inputs
                cmd.append("--inputs")
                cmd.extend(input_files)

                self.log.debug(
                    "{} executing command: [{}] for interval {}".format(
                        self.name,
                        " ".join(cmd),
                        "[{} - {})".format(
                            datetime.datetime.fromtimestamp(self.last_ran).isoformat(),
                            datetime.datetime.fromtimestamp(time_now).isoformat(),
                        ),
                    )
                )

                # invoke pegasus-em submit
                cp = subprocess.run(cmd)

                if cp.returncode != 0:
                    self.log.error(
                        "{} encountered error submitting workflow with: {}, shutting down".format(
                            self.name, cmd
                        )
                    )
                    break

            # update last time ran to the time of current iteration
            self.last_ran = time_now

            # sleep for interval
            time.sleep(self.interval)

        # advertise that my work is complete
        self.checkout.put(self.name)
        self.log.info("{} done".format(self.name))

    def shutdown(self):
        """Gracefully shutdown this thread."""

        self.log.info("{} shutting down".format(self.name))
        self.stop_event.set()

        # advertise that my work is complete
        self.checkout.put(self.name)
