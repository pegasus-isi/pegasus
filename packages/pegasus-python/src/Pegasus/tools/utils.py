##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

"""Provides common functions used by all workflow programs."""


import calendar
import csv
import datetime
import errno
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import urllib
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from Pegasus import braindump

__all__ = ("quote", "unquote")

# Compile our regular expressions

# Used in epochdate
parse_iso8601 = re.compile(
    r"(\d{4})-?(\d{2})-?(\d{2})[ tT]?(\d{2}):?(\d{2}):?(\d{2})([.,]\d+)?([zZ]|[-+](\d{2}):?(\d{2}))"
)

# Used in out2log
re_remove_extensions = re.compile(r"(?:\.(?:rescue|dag))+$")

# Module variables
MAXLOGFILE = 1000  # For log rotation, check files from .000 to .999
jobbase = "jobstate.log"  # Default name for jobstate.log file
brainbase = "braindump.txt"  # Default name for workflow information file

logger = logging.getLogger(__name__)


class ConsoleHandler(logging.StreamHandler):
    """A handler that logs to console in the sensible way.

    StreamHandler can log to *one of* sys.stdout or sys.stderr.

    It is more sensible to log to sys.stdout by default with only error
    (logging.ERROR and above) messages going to sys.stderr. This is how
    ConsoleHandler behaves.
    """

    def __init__(self):
        logging.StreamHandler.__init__(self)
        self.stream = None  # reset it; we are not going to use it anyway

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.__emit(record, sys.stderr)
        else:
            self.__emit(record, sys.stdout)

    def __emit(self, record, strm):
        self.stream = strm
        logging.StreamHandler.emit(self, record)

    def flush(self):
        # Workaround a bug in logging module
        # See:
        #   http://bugs.python.org/issue6333
        if self.stream and hasattr(self.stream, "flush") and not self.stream.closed:
            logging.StreamHandler.flush(self)


def configure_logging(verbose: int = 0, quiet: int = 0):
    """
    Configure logging for command line tools.

    # --------------- # ------------- #
    # verbose - quiet | logging.level #
    # --------------- # ------------- #
    #           < 0   |  ERROR        #
    #             0   |  ERROR        #
    #             1   |  WARNING      #
    #             2   |  INFO         #
    #             3   |  DEBUG        #
    #             4   |  TRACE        #
    #           > 4   |  TRACE        #
    # --------------- # ------------- #

    :param verbose: Increase verbosity of logging, defaults to 0
    :type verbose: int, optional
    :param quiet: Decrease the verbosity of logging, defaults to 0
    :type quiet: int, optional
    """
    # Multiply by 10 because Python logging levels are 0, 10, 20, .., 50.
    level = (verbose - quiet) * 10

    # `verbose` - `quiet` can be < 0 or > 50, so normalize it between 0 and 40
    level = min(40, max(0, level))

    # Higher the `verbose` - `quiet` number greater the verbosity, but in
    # Python higher the level lower the verbosity, so subtract - 40 to
    # reverse the order
    level = 40 - level

    # Pegasus adds a custom level, TRACE, to be used as the most verbose level
    level = 9 if level == 0 else level

    configureLogging(level)


def configureLogging(level=logging.INFO):
    cls = logging.getLoggerClass()
    logging.TRACE = logging.DEBUG - 1

    def trace(self, message, *args, **kwargs):
        """Log a TRACE level message"""
        self.log(logging.TRACE, message, *args, **kwargs)

    logging.addLevelName(logging.TRACE, "TRACE")
    cls.trace = trace

    root = logging.getLogger()
    root.setLevel(level)
    cl = ConsoleHandler()
    formatter = logging.Formatter(
        "%(asctime)s:%(levelname)s:%(name)s(%(lineno)d): %(message)s"
    )
    cl.setFormatter(formatter)
    root.addHandler(cl)

    # logging configuration for the TriggerManager
    log_dir = Path().home() / ".pegasus/log"
    log_dir.mkdir(parents=True, exist_ok=True)

    trigger = logging.getLogger("trigger")

    # setup file handler
    log_file = str(
        log_dir
        / "{}-trigger_manager.log".format(
            datetime.datetime.now().strftime("%Y-%m-%d-T%H:%M:%S")
        )
    )
    fh = logging.FileHandler(log_file)

    # setup log format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)18s - %(levelname)6s - %(message)s"
    )
    fh.setFormatter(formatter)

    trigger.addHandler(fh)


def quote(s):
    """
    Encodes a byte string using URL encoding. This replaces bytes with their
    URL-encoded equivalent %XX where XX is the hexadecimal value of the byte.
    In particular, the bytes that are encoded include:

    1. Bytes < 0x20 or >= 0x7F (C0 and C1 control codes)
    2. Quote/apostrophe (0x27), double quote (0x22), and percent (0x25)

    This will always return a byte string. If the argument is a unicode string
    then it will be utf-8 encoded before being quoted.
    """

    if not isinstance(s, bytes):
        if isinstance(s, str):
            # We need to utf-8 encode unicode strings
            s = s.encode("utf-8")
        else:
            raise TypeError("Not a string: %s" % str(s))

    buf = []
    for byte in s:
        c = chr(byte)
        if byte < 0x20 or byte >= 0x7F:
            # Any byte less than 0x20 is a control character
            # any byte >= 0x7F is either a control character
            # or a multibyte unicode character
            buf.append("%%%02X" % byte)
        elif c == "%":
            buf.append("%25")
        elif c == "'":
            buf.append("%27")
        elif c == '"':
            buf.append("%22")
        else:
            # These are regular bytes
            buf.append(c)

    return "".join(buf)


def unquote(s):
    """
    Unquote a URL-quoted string.

    This will always return a byte string with an unknown encoding. If the
    argument is a unicode string then it will be encoded in latin-1 before being
    unquoted. Latin-1 is used because it doesn't modify bytes <= 0xFF, which
    is all we expect a quoted string to contain.

    Unfortunately, this cannot return a unicode string because we have no
    way of knowing what encoding was used for the original string that was
    passed to quote().
    """
    if not isinstance(s, str):
        raise TypeError("Not a string: %s" % str(s))

    if isinstance(s, str):
        # Technically, this should not happen because
        # quote always returns a byte string, but if it was
        # passed through a database or something then it might happen
        # Latin-1 is used because it has the nice property that every
        # unicode code point <= 0xFF has the same value in latin-1
        # We ignore anything else (i.e. >0xFF) because, tecnically, it
        # should have been removed by quote()
        s = s.encode("latin-1", "ignore")

    return urllib.parse.unquote_to_bytes(s)


def isodate(now=int(time.time()), utc=False, short=False):
    """
    This function converts seconds since epoch into ISO 8601 timestamp
    """
    if utc:
        my_time_u = time.gmtime(now)
        if short:
            return time.strftime("%Y%m%dT%H%M%SZ", my_time_u)
        else:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", my_time_u)
    else:
        my_time_l = time.localtime(now)
        my_offset = int(calendar.timegm(my_time_l) - time.mktime(my_time_l))
        offset = "%+03d%02d" % (my_offset / 3600, (abs(my_offset) % 3600) / 60)
        if short:
            return time.strftime("%Y%m%dT%H%M%S", my_time_l) + offset
        else:
            return time.strftime("%Y-%m-%dT%H:%M:%S", my_time_l) + offset


def epochdate(timestamp):
    """
    This function converts an ISO timestamp into seconds since epoch
    """

    try:
        # Split date/time and timezone information
        m = parse_iso8601.search(timestamp)
        if m is None:
            logger.warning('unable to match "%s" to ISO 8601' % timestamp)
            return None
        else:
            dt = "%04d-%02d-%02d %02d:%02d:%02d" % (
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
                int(m.group(6)),
            )
            tz = m.group(8)

        # my_time = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        my_time = datetime.datetime(*(time.strptime(dt, "%Y-%m-%d %H:%M:%S")[0:6]))

        if tz.upper() != "Z":
            # no zulu time, has zone offset
            my_offset = datetime.timedelta(
                hours=int(m.group(9)), minutes=int(m.group(10))
            )

            # adjust for time zone offset
            if tz[0] == "-":
                my_time = my_time + my_offset
            else:
                my_time = my_time - my_offset

        # Turn my_time into Epoch format
        return int(calendar.timegm(my_time.timetuple()))

    except Exception:
        logger.warning('unable to parse timestamp "%s"' % timestamp)
        return None


# TODO: Remove, only used in pegasus-statistics
def create_directory(dir_name, delete_if_exists=False):
    """
    Utility method for creating directory
    @param dir_name the directory path
    @param delete_if_exists specifies whether to delete the directory if it exists
    """
    if delete_if_exists:
        if os.path.isdir(dir_name):
            logger.warning("Deleting existing directory. Deleting... " + dir_name)
            try:
                shutil.rmtree(dir_name)
            except Exception:
                logger.error("Unable to remove existing directory." + dir_name)
                sys.exit(1)
    if not os.path.isdir(dir_name):
        logger.info("Creating directory... " + dir_name)
        try:
            os.mkdir(dir_name)
        except OSError:
            logger.error("Unable to create directory." + dir_name)
            sys.exit(1)


# TODO: Remove, only used in Pegasus/monitoring/workflow.py
def find_exec(program, curdir=False, otherdirs=[]):
    """
    Determine logical location of a given binary in PATH
    """
    # program is the executable basename to look for
    # When curdir is True we also check the current directory
    # Returns fully qualified path to binary, None if not found
    my_path = os.getenv("PATH", "/bin:/usr/bin")

    for my_dir in my_path.split(":") + otherdirs:
        my_file = os.path.join(os.path.expanduser(my_dir), program)
        # Test if file is 'executable'
        if os.access(my_file, os.X_OK):
            # Found it!
            return my_file

    if curdir:
        my_file = os.path.join(os.getcwd(), program)
        # Test if file is 'executable'
        if os.access(my_file, os.X_OK):
            # Yes!
            return my_file

    # File not found
    return None


# TODO: Remove, only used in pegasus-submitdir
def write_braindump(filename, items):
    "This simply writes a dict to the file specified in braindump format"
    f = open(filename, "w")
    for k in items:
        f.write(f"{k} {items[k]}\n")
    f.close()


# TODO: Remove, only used in pegasus-submitdir
def read_braindump(filename):
    "This simply reads a braindump dict from the file specified"
    items = {}
    f = open(filename)
    for l in f:
        k, v = l.strip().split(" ", 1)
        k = k.strip()
        v = v.strip()
        items[k] = v
    f.close()
    return items


def _slurp_braindb(run, brain_alternate=None):
    """
    Reads extra configuration from braindump database
    Param: run is the run directory
    Returns: Dictionary with the configuration, empty if error
    """
    my_config = {}

    if brain_alternate is None:
        my_braindb = os.path.join(run, "braindump.txt")
    else:
        my_braindb = os.path.join(run, brain_alternate)

    try:
        my_file = open(my_braindb)
    except OSError:
        # Error opening file
        return my_config

    for line in my_file:
        # Remove \r and/or \n from the end of the line
        line = line.rstrip("\r\n")
        # Split the line into a key and a value
        k, v = line.split(" ", 1)

        if k == "run" and v != run and run != ".":
            logger.warning("run directory mismatch, using %s" % (run))
            my_config[k] = run
        else:
            # Remove leading and trailing whitespaces from value
            v = v.strip()
            my_config[k] = v

    # Close file
    my_file.close()

    # Done!
    logger.debug("# slurped %s" % (my_braindb))
    return my_config


def slurp_braindb(run_dir, brain_alternate=None):
    """
    Consume braindump file and return it's content as a `dict`.

    :param run_dir: [description]
    :type run_dir: [type]
    :param brain_alternate: [description], defaults to None
    :type brain_alternate: [type], optional
    :return: [description]
    :rtype: [type]
    """
    if not run_dir:
        raise ValueError("run_dir is required")

    bdump = Path(run_dir) / (
        "braindump.yml" if brain_alternate is None else brain_alternate
    )

    if bdump.exists() is False:
        # TODO: Remove this except block one backwards compatibility for
        # 4.9 is not needed.
        cfg = _slurp_braindb(run_dir, brain_alternate)
    else:
        if bdump.is_file() is False:
            raise TypeError("%s is not a valid file" % bdump.resolve())

        with bdump.open("r") as fp:
            cfg = braindump.load(fp)
            cfg = {
                k: str(v) if isinstance(v, Path) else v for k, v in asdict(cfg).items()
            }

    return cfg


def raw_to_regular(exitcode):
    """
    This function decodes the raw exitcode into a plain format:
    For a regular exitcode, it returns a value between 0 and 127;
    For signals, it returns the negative signal number (-1 through -127)
    For failures (when exitcode < 0), it returns the special value -128
    """
    if not isinstance(exitcode, int):
        return exitcode
    if exitcode < 0:
        return -128
    if (exitcode & 127) > 0:
        # Signal
        return -(exitcode & 127)
    return exitcode >> 8


def regular_to_raw(exitcode):
    """
    This function encodes a regular exitcode into a raw exitcode.
    """
    if not isinstance(exitcode, int):
        logger.warning("exitcode not an integer!")
        return exitcode
    if exitcode == -128:
        return -1
    return exitcode << 8


def parse_exit(ec):
    """
    Parses an exit code any way possible
    Returns a string that shows what went wrong
    """
    if (ec & 127) > 0:
        my_signo = ec & 127
        my_core = ""
        if (ec & 128) == 128:
            my_core = " (core)"
        my_result = f"died on signal {my_signo}{my_core}"
    elif (ec >> 8) > 0:
        my_result = "exit code %d" % (ec >> 8)
    else:
        my_result = "OK"

    return my_result


def out2log(rundir, outfile):
    """
    purpose: infer output symlink for Condor common user log
    paramtr: rundir (IN): the run directory
    paramtr: outfile (IN): the name of the out file we use
    returns: the name of the log file to use
    """

    # Get the basename
    my_base = os.path.basename(outfile)
    # NEW: Account for rescue DAGs
    my_base = my_base[: my_base.find(".dagman.out")]
    my_base = re_remove_extensions.sub("", my_base)
    # Add .log extension
    my_base = my_base + ".log"
    # Create path
    my_log = os.path.join(rundir, my_base)

    return my_log, my_base


def write_pid_file(pid_filename, ts=int(time.time())):
    """
    This function writes a pid file with name 'filename' containing
    the current pid and timestamp.
    """
    try:
        PIDFILE = open(pid_filename, "w")
        PIDFILE.write("pid %s\n" % (os.getpid()))
        PIDFILE.write("timestamp %s\n" % (isodate(ts)))
    except OSError:
        logger.error("cannot write PID file %s" % (pid_filename))
    else:
        PIDFILE.close()


def pid_running(filename):
    """
    This function takes a file containing a single line in the format
    of pid 'xxxxxx'. If the file exists, it reads the line and checks if
    the process id 'xxxxxx' is still running. The function returns True
    if the process is still running, or False if not.
    """
    # First, we check if file exists
    if os.access(filename, os.F_OK):
        try:
            # Open pid file
            PIDFILE = open(filename)

            # Look for pid line
            for line in PIDFILE:
                line = line.strip()
                if line.startswith("pid"):
                    # Get pid
                    my_pid = int(line.split(" ")[1])
                    # We are done with this file, just close it...
                    PIDFILE.close()
                    # Now let's see if process still around...
                    try:
                        os.kill(my_pid, 0)
                    except OSError as err:
                        if err.errno == errno.ESRCH:
                            # pid is not found, monitoring cannot be running
                            logger.info("pid %d not running anymore..." % (my_pid))
                            return False
                        elif err.errno == errno.EPERM:
                            # pid cannot be killed because we don't have permission
                            logger.debug(
                                "no permission to talk to pid %d..." % (my_pid)
                            )
                            return True
                        else:
                            logger.warning(
                                "unknown error while sending signal to pid %d"
                                % (my_pid)
                            )
                            logger.warning(traceback.format_exc())
                            return True
                    except Exception:
                        logger.warning(
                            "unknown error while sending signal to pid %d" % (my_pid)
                        )
                        logger.warning(traceback.format_exc())
                        return True
                    else:
                        logger.debug("pid %d still running..." % (my_pid))
                        return True

            logger.warning(
                "could not find pid line in file %s. continuing..." % (filename)
            )

            # Don't forget to close file
            PIDFILE.close()
        except Exception:
            logger.warning("error processing file %s. continuing..." % (filename))
            logger.warning(traceback.format_exc())

        return True

    # PID file doesn't exist
    return False


# TODO: Remove, only used in pegasus-statistics
def monitoring_running(run_dir):
    """
    This function takes a run directory and returns true if it appears
    that pegasus-monitord is still running, or false if it has
    finished (or perhaps it was never started).
    """
    start_file = os.path.join(run_dir, "monitord.started")
    done_file = os.path.join(run_dir, "monitord.done")

    # If monitord finished, it is not running anymore
    if os.access(done_file, os.F_OK):
        return False

    # If monitord started, it is (possibly) still running
    if os.access(start_file, os.F_OK):
        # Let's check
        return pid_running(start_file)

    # Otherwise, monitord was never executed (so it is not running right now...)
    return False


# TODO: Remove, only used in pegasus-statistics
def loading_completed(run_dir):
    """
    This function examines a run directory and returns True if all
    events were successfully processed by pegasus-monitord.
    """
    # Loading is never completed if monitoring is still running
    if monitoring_running(run_dir) is True:
        return False

    start_file = os.path.join(run_dir, "monitord.started")
    done_file = os.path.join(run_dir, "monitord.done")
    log_file = os.path.join(run_dir, "monitord.log")

    # Both started and done files need to exist...
    if (not os.access(start_file, os.F_OK)) or (not os.access(done_file, os.F_OK)):
        return False

    # Check monitord.log for loading errors...
    if os.access(log_file, os.F_OK):
        try:
            LOG = open(log_file)
            for line in LOG:
                if line.find("NL-LOAD-ERROR -->") > 0:
                    # Found loading error... event processing was not completed
                    LOG.close()
                    return False
                if line.find("KICKSTART-PARSE-ERROR -->") > 0:
                    # Found kickstart parsing error... data not fully loaded
                    LOG.close()
                    return False
                if (
                    line.find("cannot create events output... disabling event output")
                    > 0
                ):
                    # Found loader initialization error... data not loaded
                    LOG.close()
                    return False
            LOG.close()
        except OSError:
            logger.warning("could not process log file: %s" % (log_file))

    # Otherwise, return true
    return True


def rotate_log_file(source_file):
    """
    This function rotates the specified logfile.
    """

    # First we check if we have the log file
    if not os.access(source_file, os.F_OK):
        # File doesn't exist, we don't have to rotate
        return

    # Now we need to find the latest log file

    # We start from .000
    sf = 0

    while sf < MAXLOGFILE:
        dest_file = source_file + ".%03d" % (sf)
        if os.access(dest_file, os.F_OK):
            # Continue to the next one
            sf = sf + 1
        else:
            break

    # Safety check to see if we have reached the maximum number of log files
    if sf >= MAXLOGFILE:
        logger.error("%s exists, cannot rotate log file anymore!" % (dest_file))
        sys.exit(1)

    # Now that we have source_file and dest_file, try to rotate the logs
    try:
        os.rename(source_file, dest_file)
    except OSError:
        logger.error(f"cannot rename {source_file} to {dest_file}")
        sys.exit(1)

    # Done!
    return


def make_boolean(value):
    # purpose: convert an input string into something boolean
    # paramtr: $x (IN): a property value
    # returns: 0 (false) or 1 (true)
    my_val = str(value)
    if (
        my_val.lower() == "true"
        or my_val.lower() == "on"
        or my_val.lower() == "yes"
        or my_val.isdigit()
        and int(value) > 0
    ):
        return 1

    return 0


@contextmanager
def write_table(f, fields, headers=None, widths=None, encoding=None):
    """."""
    with open(f, "w", encoding=encoding) as writer:
        if widths is None:
            widths = [len(_) for _ in fields]

        def writerow(row):
            writer.write(
                "".join(
                    str(value).ljust(writer.widths[i]) for i, value in enumerate(row)
                )
            )
            writer.write("\n")

        def writeheader():
            writer.writerow(writer.fields)

        def writetable(table):
            cols = writer.headers or writer.widths
            max_length = [_ for _ in cols]

            for row in table:
                for i, col in enumerate(writer.fields):
                    max_length[i] = max(max_length[i], len(str(row[col])))

            max_length = [i + 1 for i in max_length]
            for row in table:
                writer.writerow(row)

        writer.fields = fields
        writer.headers = headers
        writer.widths = widths
        writer.writerow = writerow
        writer.writeheader = writeheader
        writer.writetable = writetable

        yield writer


@contextmanager
def write_csv(f, fields, headers=None, dialect=csv.excel, encoding=None):
    """."""
    with open(f, "w", encoding=encoding) as csvfile:
        writer = csv.DictWriter(
            csvfile, dialect=dialect, fieldnames=fields, extrasaction="ignore"
        )
        _writerow = writer.writerow

        def writerow(row):
            _writerow({k: v for k, v in zip(fields, row)})

        def writeheader():
            writer.writerow(writer.headers or writer.fieldnames)

        writer.headers = headers
        writer.write = csvfile.write
        writer.writerow = writerow
        writer.writeheader = writeheader

        yield writer


def pegasus_version():
    """
    Returns the Pegasus version string.
    """

    # is calling out to pegasus-version really the best way to do this?
    pegasus_version = None
    if "PEGASUS_HOME" in os.environ:
        f = os.path.join(os.environ.get("PEGASUS_HOME"), "bin/pegasus-version")
        if os.path.isfile(f):
            pegasus_version = f

    if not pegasus_version:
        f = os.path.join(os.path.dirname(sys.argv[0]), "pegasus-version")
        if os.path.isfile(f):
            pegasus_version = f

    if pegasus_version:
        child = subprocess.Popen(
            pegasus_version,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=os.getcwd(),
        )
        out, err = child.communicate()
        if child.returncode != 0:
            raise Exception(err.decode("utf8").strip())
        return out.decode("utf8").strip()
