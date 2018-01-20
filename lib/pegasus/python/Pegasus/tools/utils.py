"""
utils.py: Provides common functions used by all workflow programs
"""
from __future__ import print_function

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

# Revision : $Revision: 2012 $

import re
import os
import sys
import time
import errno
import shutil
import logging
import calendar
import commands
import datetime
import traceback
import subprocess
import urllib

from builtins import int

__all__ = ['quote', 'unquote']

# Compile our regular expressions

# Used in epochdate
parse_iso8601 = re.compile(r'(\d{4})-?(\d{2})-?(\d{2})[ tT]?(\d{2}):?(\d{2}):?(\d{2})([.,]\d+)?([zZ]|[-+](\d{2}):?(\d{2}))')

# Used in out2log
re_remove_extensions = re.compile(r"(?:\.(?:rescue|dag))+$")

# Module variables
MAXLOGFILE = 1000                # For log rotation, check files from .000 to .999
jobbase = "jobstate.log"        # Default name for jobstate.log file
brainbase = "braindump.txt"        # Default name for workflow information file

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
        self.stream = None # reset it; we are not going to use it anyway

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
        if self.stream and hasattr(self.stream, 'flush') and not self.stream.closed:
            logging.StreamHandler.flush(self)

def configureLogging(level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(level)
    cl = ConsoleHandler()
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s(%(lineno)d): %(message)s")
    cl.setFormatter(formatter)
    root.addHandler(cl)

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
    if not isinstance(s, basestring):
        raise TypeError("Not a string: %s" % str(s))

    if isinstance(s, unicode):
        # We need to utf-8 encode unicode strings
        s = s.encode('utf-8')

    buf = []
    for c in s:
        i = ord(c)
        if i < 0x20 or i >= 0x7F:
            # Any byte less than 0x20 is a control character
            # any byte >= 0x7F is either a control character
            # or a multibyte unicode character
            buf.append("%%%02X" % i)
        elif c == '%':
            buf.append('%25')
        elif c == "'":
            buf.append('%27')
        elif c == '"':
            buf.append('%22')
        else:
            # These are regular bytes
            buf.append(c)

    return ''.join(buf)

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
    if not isinstance(s, basestring):
        raise TypeError("Not a string: %s" % str(s))

    if isinstance(s, unicode):
        # Technically, this should not happen because
        # quote always returns a byte string, but if it was
        # passed through a database or something then it might happen
        # Latin-1 is used because it has the nice property that every
        # unicode code point <= 0xFF has the same value in latin-1
        # We ignore anything else (i.e. >0xFF) because, tecnically, it
        # should have been removed by quote()
        s = s.encode('latin-1', 'ignore')

    return urllib.unquote(s)

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
        my_offset = int( calendar.timegm(my_time_l) - time.mktime(my_time_l) )
        offset = "%+03d%02d" % ( my_offset / 3600, (abs(my_offset) % 3600) / 60)
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
            logger.warn("unable to match \"%s\" to ISO 8601" % timestamp)
            return None
        else:
            dt = "%04d-%02d-%02d %02d:%02d:%02d" % (int(m.group(1)),
                                                    int(m.group(2)),
                                                    int(m.group(3)),
                                                    int(m.group(4)),
                                                    int(m.group(5)),
                                                    int(m.group(6)))
            tz = m.group(8)

        # my_time = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        my_time = datetime.datetime(*(time.strptime(dt, "%Y-%m-%d %H:%M:%S")[0:6]))

        if tz.upper() != 'Z':
            # no zulu time, has zone offset
            my_offset = datetime.timedelta(hours=int(m.group(9)), minutes=int(m.group(10)))

            # adjust for time zone offset
            if tz[0] == '-':
                my_time = my_time + my_offset
            else:
                my_time = my_time - my_offset
        
        # Turn my_time into Epoch format
        return int(calendar.timegm(my_time.timetuple()))

    except:
        logger.warn("unable to parse timestamp \"%s\"" % timestamp)
        return None

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
            except:
                logger.error("Unable to remove existing directory." + dir_name)
                sys.exit(1)
    if not os.path.isdir(dir_name):
        logger.info("Creating directory... " + dir_name)
        try:
            os.mkdir(dir_name)
        except OSError:
            logger.error("Unable to create directory." + dir_name)
            sys.exit(1)

def find_exec(program, curdir=False, otherdirs=[]):
    """
    Determine logical location of a given binary in PATH
    """
    # program is the executable basename to look for
    # When curdir is True we also check the current directory
    # Returns fully qualified path to binary, None if not found
    my_path = os.getenv("PATH","/bin:/usr/bin")

    for my_dir in my_path.split(':')+otherdirs:
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

def pipe_out_cmd(cmd_string):
    """
    Runs a command and captures stderr and stdout.
    Warning: Do not use shell meta characters
    Params: argument string, executable first
    Returns: All lines of output
    """
    my_result = []

    # Launch process using the subprocess module interface
    try:
        proc = subprocess.Popen(cmd_string.split(), shell=False,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                bufsize=1)
    except:
        # Error running command
        return None

    # Wait for it to finish, capturing output
    resp = proc.communicate()

    # Capture stdout
    for line in resp[0].split('\n'):
        if len(line):
            my_result.append(line)
        
    # Capture stderr
    for line in resp[1].split('\n'):
        if len(line):
            my_result.append(line)

    return my_result

def add_to_braindb(run, missing_keys, brain_alternate=None):
    """
    This function adds to the braindump database the missing
    keys from missing_keys.
    """
    my_config = {}

    if brain_alternate is None:
        my_braindb = os.path.join(run, brainbase)
    else:
        my_braindb = os.path.join(run, brain_alternate)

    try:
        my_file = open(my_braindb, 'a')
    except IOError:
        return

    try:
        for key in missing_keys:
            my_file.write("%s %s\n" % (str(key), str(missing_keys[key])))
    except IOError:
        # Nothing to do...
        pass

    try:
        my_file.close()
    except IOError:
        pass

def write_braindump(filename, items):
    "This simply writes a dict to the file specified in braindump format"
    f = open(filename, "w")
    for k in items:
        f.write("%s %s\n" % (k,items[k]))
    f.close()

def read_braindump(filename):
    "This simply reads a braindump dict from the file specified"
    items = {}
    f = open(filename, "r")
    for l in f:
        k,v = l.strip().split(" ", 1)
        k = k.strip()
        v = v.strip()
        items[k] = v
    f.close()
    return items

def slurp_braindb(run, brain_alternate=None):
    """
    Reads extra configuration from braindump database
    Param: run is the run directory
    Returns: Dictionary with the configuration, empty if error
    """
    my_config = {}

    if brain_alternate is None:
        my_braindb = os.path.join(run, brainbase)
    else:
        my_braindb = os.path.join(run, brain_alternate)

    try:
        my_file = open(my_braindb, 'r')
    except IOError:
        # Error opening file
        return my_config

    for line in my_file:
        # Remove \r and/or \n from the end of the line
        line = line.rstrip("\r\n")
        # Split the line into a key and a value
        k, v = line.split(" ", 1)
        
        if k == "run" and v != run and run != '.':
            logger.warn("run directory mismatch, using %s" % (run))
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

def version():
    """
    Obtains Pegasus version
    """
    my_output = commands.getstatusoutput("pegasus-version")

    return my_output[1]

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
    return (exitcode >> 8)

def regular_to_raw(exitcode):
    """
    This function encodes a regular exitcode into a raw exitcode.
    """
    if not isinstance(exitcode, int):
        logger.warning("exitcode not an integer!")
        return exitcode
    if exitcode == -128:
        return -1
    return (exitcode << 8)

def parse_exit(ec):
    """
    Parses an exit code any way possible
    Returns a string that shows what went wrong
    """
    if (ec & 127) > 0:
        my_signo = ec & 127
        my_core = ''
        if (ec & 128) == 128:
            my_core = " (core)"
        my_result = "died on signal %s%s" % (my_signo, my_core)
    elif (ec >> 8) > 0:
        my_result = "exit code %d" % ((ec >> 8))
    else:
        my_result = "OK"

    return my_result

def check_rescue(directory, dag):
    """
    Check for the existence of (multiple levels of) rescue DAGs
    Param: directory is the directory to check for the presence of rescue DAGs
    Param: dag is the filename of a regular DAG file
    Returns: List of rescue DAGs (which may be empty if none found)
    """
    my_base = os.path.basename(dag)
    my_result = []

    try:
        my_files = os.listdir(directory)
    except OSError:
        return my_result

    for my_file in my_files:
        # Add file to the list if pegasus-planned DAGs that have a rescue DAG
        if my_file.startswith(my_base) and my_file.endswith(".rescue"):
            my_result.append(os.path.join(directory, my_file))

    # Sort list
    my_result.sort()

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
    my_base = my_base[:my_base.find(".dagman.out")]
    my_base = re_remove_extensions.sub('', my_base)
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
    except IOError:
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
            PIDFILE = open(filename, 'r')

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
                            logger.debug("no permission to talk to pid %d..." % (my_pid))
                            return True
                        else:
                            logger.warning("unknown error while sending signal to pid %d" % (my_pid))
                            logger.warning(traceback.format_exc())
                            return True
                    except:
                        logger.warning("unknown error while sending signal to pid %d" % (my_pid))
                        logger.warning(traceback.format_exc())
                        return True
                    else:
                        logger.debug("pid %d still running..." % (my_pid))
                        return True

            logger.warning("could not find pid line in file %s. continuing..." % (filename))

            # Don't forget to close file
            PIDFILE.close()
        except:
            logger.warning("error processing file %s. continuing..." % (filename))
            logger.warning(traceback.format_exc())

        return True

    # PID file doesn't exist
    return False

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

def loading_completed(run_dir):
    """
    This function examines a run directory and returns True if all
    events were successfully processed by pegasus-monitord.
    """
    # Loading is never completed if monitoring is still running
    if monitoring_running(run_dir) == True:
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
            LOG = open(log_file, "r")
            for line in LOG:
                if line.find("NL-LOAD-ERROR -->") > 0:
                    # Found loading error... event processing was not completed
                    LOG.close()
                    return False
                if line.find("KICKSTART-PARSE-ERROR -->") > 0:
                    # Found kickstart parsing error... data not fully loaded
                    LOG.close()
                    return False
                if line.find("cannot create events output... disabling event output") > 0:
                    # Found loader initialization error... data not loaded
                    LOG.close()
                    return False
            LOG.close()
        except IOError:
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

    while (sf < MAXLOGFILE):
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
        logger.error("cannot rename %s to %s" % (source_file, dest_file))
        sys.exit(1)

    # Done!
    return

def log10(val):
    """
    Equivalent to ceil(log(val) / log(10))
    """
    result = 0
    while val > 1:
        result = result + 1
        val = val / 10

    if result:
        return result

    return 1

def make_boolean(value):
    # purpose: convert an input string into something boolean
    # paramtr: $x (IN): a property value
    # returns: 0 (false) or 1 (true)
    my_val = str(value)
    if (my_val.lower() == 'true' or
        my_val.lower() == 'on' or
        my_val.lower() == 'yes' or
        my_val.isdigit() and int(value) > 0):
        return 1

    return 0


if __name__ == "__main__":
    current_time = int(time.time())
    print("Testing isodate() function from now=%lu" % (current_time))
    print(" long local timestamp:", isodate(now=current_time))
    print("   long utc timestamp:", isodate(now=current_time, utc=True))
    print("short local timestamp:", isodate(now=current_time, short=True))
    print("  short utc timestamp:", isodate(now=current_time, utc=True, short=True))
    print()
    print("Testing epochdate() function from above ISO dates")
    print(" long local epochdate:", epochdate(isodate(now=current_time)))
    print("   long utc epochdate:", epochdate(isodate(now=current_time, utc=True)))
    print("short local timestamp:", epochdate(isodate(now=current_time, short=True)))
    print("  short utc timestamp:", epochdate(isodate(now=current_time, utc=True, short=True)))
    print()
    print("Testing find exec")
    print("Looking for ls...", find_exec('ls'))
    print("Looking for test.pl...", find_exec('test.pl', True))
    print("Monitord 1", find_exec("pegasus-mointord"))
    print("Monitord 2", find_exec(program="pegasus-monitord",otherdirs=["/usr/local/pegasus/src/4.0-branch/bin","/usr/local/pegasus"]))
    print()
    print("Testing parse_exit() function")
    print("ec = 5   ==> ", parse_exit(5))
    print("ec = 129 ==> ", parse_exit(129))
    print()
    print("Testing log10() function")
    print("log10(10):", log10(10))
    print("log10(100.2):", log10(100.2))
    print(version())
    print(slurp_braindb("."))
    print(pipe_out_cmd('ls -lR'))
    print()
    print("Testing quote/unquote functions...")
    print(repr(str(bytearray(xrange(256)))))
    print(quote(str(bytearray(xrange(256)))))
    print(unquote("carriage return: %0Apercent: %25%0Aquote: %27%0Adouble quote: %22"))
    print()
    print()

