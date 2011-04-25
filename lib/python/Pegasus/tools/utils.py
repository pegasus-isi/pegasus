"""
utils.py: Provides common functions used by all workflow programs
"""

##
#  Copyright 2007-2010 University Of Southern California
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
import logging
import calendar
import commands
import datetime
import subprocess

# Regular expressions
parse_iso8601 = re.compile(r'(\d{4})-?(\d{2})-?(\d{2})[ tT]?(\d{2}):?(\d{2}):?(\d{2})([.,]\d+)?([zZ]|[-+](\d{2}):?(\d{2}))')

# Module variables
MAXLOGFILE = 1000                # For log rotation, check files from .000 to .999
jobbase = "jobstate.log"        # Default name for jobstate.log file
brainbase = "braindump.txt"        # Default name for workflow information file

# Get logger object (initialized elsewhere)
logger = logging.getLogger()

def isodate(now=int(time.time()), utc=False, short=False):
    """
    This function converts seconds since epoch into ISO timestamp
    """

    if utc:
        my_time = time.gmtime(now)
    else:
        # FIXME: Zone offset is wrong on CentOS 5.5 with python 2.4
        my_time = time.localtime(now)
        
    if short:
        if utc:
            return time.strftime("%Y%m%dT%H%M%SZ", my_time)
        else:
            return time.strftime("%Y%m%dT%H%M%S%z", my_time)
    else:
        if utc:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", my_time)
        else:
            return time.strftime("%Y-%m-%dT%H:%M:%S%z", my_time)

def epochdate(timestamp, short=False):
    """
    This function converts an ISO timestamp into seconds since epoch
    Set short to False when the timestamp is in the YYYY-MM-DDTHH:MM:SSZZZ:ZZ format
    Set short to True when the timestamp is in the YYYYMMDDTHHMMSSZZZZZ format
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


def find_exec(program, curdir=False):
    """
    Determine logical location of a given binary in PATH
    """
    # program is the executable basename to look for
    # When curdir is True we also check the current directory
    # Returns fully qualified path to binary, None if not found
    my_path = os.getenv("PATH","/bin:/usr/bin")

    for my_dir in my_path.split(':'):
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
    except:
        return

    try:
        for key in missing_keys:
            my_file.write("%s %s\n" % (str(key), str(missing_keys[key])))
    except:
        # Nothing to do...
        pass

    try:
        my_file.close()
    except:
        pass

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
    except:
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

def check_rescue(dir, dag):
    """
    Check for the existence of (multiple levels of) rescue DAGs
    Param: dir is the directory to check for the presence of rescue DAGs
    Param: dag is the filename of a regular DAG file
    Returns: List of rescue DAGs (which may be empty if none found)
    """
    my_base = os.path.basename(dag)
    my_result = []

    try:
        my_files = os.listdir(dir)
    except:
        return my_result

    for file in my_files:
        # Add file to the list if pegasus-planned DAGs that have a rescue DAG
        if file.startswith(my_base) and file.endswith(".rescue"):
            my_result.append(os.path.join(dir, file))

    # Sort list
    my_result.sort()

    return my_result

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
    except:
        logger.error("cannot rename %s to %s" % (source_file, dest_file))
        sys.exit(1)

    # Done!
    return

def log10(x):
    """
    Equivalent to ceil(log(x) / log(10))
    """
    result = 0
    while x > 1:
        result = result + 1
        x = x / 10

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

def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    ''' Fork the current process as a daemon, redirecting standard file
        descriptors (by default, redirects them to /dev/null).

        This function was adapted from O'Reilly's Python Cookbook 2nd
        Edition.
    '''
    # Perform first fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # Exit first parent.
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Decouple from parent environment.
    os.chdir("/")
    os.umask(0002) # Be permisive
    os.setsid()

    # Perform second fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0) # Exit second parent.
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # The process is now daemonized, redirect standard file descriptors.
    for f in sys.stdout, sys.stderr: f.flush()

    si = file(stdin, 'r')
    so = file(stdout, 'w', 0)
    se = file(stderr, 'w', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

def keep_foreground():
    """
    This function turns the program into almost a daemon, but keep in
    foreground for Condor. It does not take any parameters and does
    not return anything.
    """

    # Go to a safe place that is not susceptible to sudden umounts
    # FIX THIS: It may break some things
    try:
        os.chdir('/')
    except:
        logger.critical("could not chdir!")
        sys.exit(1)
    
    # Although we cannot set sid, we can still become process group leader
    try:
        os.setpgid(0, 0)
    except:
        logger.critical("could not setpgid!")
        sys.exit(1)

if __name__ == "__main__":
    now = int(time.time())
    print "Testing isodate() function from now=%lu" % (now)
    print " long local timestamp:", isodate(now=now)
    print "   long utc timestamp:", isodate(now=now,utc=True)
    print "short local timestamp:", isodate(now=now,short=True)
    print "  short utc timestamp:", isodate(now=now,utc=True,short=True)
    print
    print "Testing epochdate() function from above ISO dates"
    print " long local epochdate:", epochdate(isodate(now=now))
    print "   long utc epochdate:", epochdate(isodate(now=now,utc=True))
    print "short local timestamp:", epochdate(isodate(now=now,short=True))
    print "  short utc timestamp:", epochdate(isodate(now=now,utc=True,short=True))
    print
    print "Looking for ls...", find_exec('ls')
    print "Looking for test.pl...", find_exec('test.pl', True)
    print
    print "Testing parse_exit() function"
    print "ec = 5   ==> ", parse_exit(5)
    print "ec = 129 ==> ", parse_exit(129)
    print
    print "Testing log10() function"
    print "log10(10):", log10(10)
    print "log10(100.2):", log10(100.2)
    print version()
    print slurp_braindb(".")
    print pipe_out_cmd('ls -lR')
