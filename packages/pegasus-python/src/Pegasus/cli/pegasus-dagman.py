#!/usr/bin/env python3

"""
pegasus-dagman

This program is to be run as a replacement for condor_dagman inside
of a submit file. The dag can be submitted by running the command
condor_submit_dag -dagman /path/to/pegasus-dagman my.dag

Usage: pegasus-dagman [options]
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
# Author : gmehta at isi dot edu
# Revision : $Revision$
__author__ = "Gaurang Mehta"
__author__ = "Mats Rynge"

import logging
import math
import os
import shutil
import signal
import subprocess
import sys
import time

from Pegasus.tools import utils


def find_prog(prog, dir=[]):
    def is_prog(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(prog)
    if fpath:
        if is_prog(prog):
            return prog
    else:
        for path in dir + os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, prog)
            if is_prog(exe_file):
                return exe_file
    return None


logger = logging.getLogger("pegasus-dagman")

# Use pegasus-config to find our lib path
print("Pegasus DAGMAN is %s" % sys.argv[0])

utils.configureLogging()

SLEEP_TIME = 15
DIED_TOO_QUICKLY_TIME = 300
MONITORD_KILL_TIME = 200  # the number of seconds after pegasus-remove is called, that monitord is sent the kill signal

dagman = None

monitord = None
monitord_last_start = 0
monitord_next_start = 0
monitord_current_restarts = 0
monitord_shutdown_mode = False
monitord_shutdown_time = 0


def dagman_launch(dagman_bin, arguments=[]):
    """Launches the condor_dagman program with all
    the arguments passed to pegasus-dagman"""
    if dagman_bin is not None:
        arguments.insert(0, "condor_scheduniv_exec." + os.getenv("CONDOR_ID"))
        try:
            dagman_proc = subprocess.Popen(
                arguments, stdout=sys.stdout, stderr=sys.stderr, executable=dagman_bin
            )
            logger.info("Launched Dagman with Pid %d" % dagman_proc.pid)
        except OSError as err:
            logger.error("Could not launch Dagman.", err)
            sys.exit(1)
    else:
        logger.error("Condor Dagman not found")
        sys.exit(127)

    return dagman_proc


def monitord_launch(monitord_bin, arguments=[]):
    """Launches Monitord in condor daemon mode"""
    if monitord_bin is not None:
        try:
            # Rotate log file, if it exists
            # PM-688
            logfile = "monitord.log"
            utils.rotate_log_file(logfile)
            # we have the right name of the log file
            log = open(logfile, "a", 1)
            monitord_proc = subprocess.Popen(
                [monitord_bin, "-N", os.getenv("_CONDOR_DAGMAN_LOG")],
                stdout=log.fileno(),
                stderr=subprocess.STDOUT,
            )
            log.close()
            logger.info("Launched Monitord with Pid %d" % monitord_proc.pid)
            return monitord_proc
        except OSError as err:
            logger.error("Could not launch Monitord.", err)
    else:
        logger.error("pegausus-monitord not found")
    return None


def is_dagman_copy_to_spool():
    """Checks using condor_config_val if dagman_copy_to_spool is set
    then copy condor_dagman to the current dir "bin_dir"
    """
    condor_config_val = find_prog("condor_config_val")
    copy_to_spool = subprocess.Popen(
        [condor_config_val, "DAGMAN_COPY_TO_SPOOL"], stdout=subprocess.PIPE, shell=False
    ).communicate()[0]
    logger.info("DAGMAN_COPY_TO_SPOOL is set to %s" % copy_to_spool)
    if copy_to_spool.lower().strip() == "true":
        return True
    else:
        return False


def sighandler(signum, frame):
    """ Signal handler to catch and pass SIGTERM, SIGABRT, SIGUSR1, SIGTERM """
    #   global dagman, monitord
    logger.info("pegasus-dagman caught SIGNAL %s" % signum)
    if dagman is not None:
        os.kill(dagman.pid, signum)

    if monitord is not None:
        # PM-767 when pegasus-remove is called, internally condor_rm is called
        # that sends a SIGUSR1 to pegasus-dagman.
        # we pass that signal to condor_dagman. But for monitord, we don't, as
        # we want monitord to gracefully exit after reaching the end of dagman
        # log file, so that the stampede database records workflow failed.
        if signum == signal.SIGUSR1:
            signum = signal.SIGINT
            global monitord_shutdown_mode, monitord_shutdown_time
            monitord_shutdown_mode = True
            monitord_shutdown_time = time.time()
        else:
            # All signals other than SIGUSR1 are passed as is
            logger.info("pegasus-dagman sent signal %s to monitord" % signum)
            os.kill(monitord.pid, signum)


# -- main--------------------------------------------------------------

if __name__ == "__main__":

    # Create a new process group. PM-972: A change in HTCondor 8.2.9
    # (https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=5173) means that
    # HTCondor now sets up the process group for the process, and setpgid()
    # will fail - but we can just ignore it as the process group is already set
    try:
        os.setpgid(0, 0)
    except Exception:
        pass

    signal.signal(signal.SIGTERM, sighandler)
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGABRT, sighandler)
    signal.signal(signal.SIGUSR1, sighandler)
    signal.signal(signal.SIGUSR2, sighandler)

    copy_to_spool = is_dagman_copy_to_spool()

    # Find dagman Binary
    dagman_bin = find_prog("condor_dagman")

    if dagman_bin is not None:
        # If copy_to_spool is set copy dagman binary to dag submit directory
        if copy_to_spool:
            old_dagman_bin = dagman_bin
            dagman_bin = os.path.join(
                os.getcwd(), "condor_scheduniv_exec." + os.getenv("CONDOR_ID")
            )
            shutil.copy2(old_dagman_bin, dagman_bin)
            logger.info(
                "Copied condor_dagman from {} to {}".format(old_dagman_bin, dagman_bin)
            )

    # Launch DAGMAN
    dagman = dagman_launch(dagman_bin, sys.argv[1:])

    # Find monitord Binary
    monitord_bin = find_prog("pegasus-monitord", [os.getenv("PEGASUS_HOME") + "/bin"])

    # Launch Monitord
    monitord = monitord_launch(monitord_bin)

    dagman.poll()
    monitord.poll()

    while monitord.returncode is None or dagman.returncode is None:
        if dagman.returncode is None and monitord.returncode is not None:
            # monitord is not running
            t = time.time()
            if monitord_next_start == 0:
                logger.error("monitord is not running")
                # did the process die too quickly?
                if t - monitord_last_start < DIED_TOO_QUICKLY_TIME:
                    monitord_current_restarts += 1
                else:
                    monitord_current_restarts = 0
                # backoff with upper limit
                backoff = min(math.exp(monitord_current_restarts + 3), 3600)
                logger.info(
                    "next monitord launch scheduled in about %d seconds" % (backoff)
                )
                monitord_next_start = t + backoff - 1
            # time to restart yet?
            if monitord_next_start <= t:
                monitord_next_start = 0
                monitord_last_start = t
                monitord = monitord_launch(monitord_bin)

        # PM-767 if in shutdown mode, check to see if we need to kill monitord
        if monitord_shutdown_mode:
            t = time.time()
            if t - monitord_shutdown_time > MONITORD_KILL_TIME:
                logger.info(
                    "monitord shudown time expired. Sending SIGINT to process %d"
                    % monitord.pid
                )
                os.kill(monitord.pid, signal.SIGINT)

        # sleep in between polls
        time.sleep(SLEEP_TIME)

        monitord.poll()
        dagman.poll()

    # Dagman and Monitord have exited. Lets exit pegasus-dagman with
    # a merged returncode
    logger.info("Dagman exited with code %d" % dagman.returncode)
    logger.info("Monitord exited with code %d" % monitord.returncode)
    if copy_to_spool:
        logger.info(
            "Removing copied condor_dagman from submit directory %s" % dagman_bin
        )
        os.remove(dagman_bin)
    sys.exit(dagman.returncode & monitord.returncode)
