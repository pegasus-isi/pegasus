##
#  Copyright 2007-2014 University Of Southern California
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

__author__ = "Mats Rynge <rynge@isi.edu>"

import os
import re
import signal
import subprocess
import sys
import threading
import logging


# --- global variables ----------------------------------------------------------------


logger = logging.getLogger("transfer_logger")


# --- classes -----------------------------------------------------------------


class Singleton(type):
    """Implementation of the singleton pattern"""
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                super(Singleton, cls).__call__(*args, **kwargs)
            cls.lock = threading.Lock()
        return cls._instances[cls]


class Tools(object):
    """Singleton for detecting and maintaining tools we depend on
    """
    
    __metaclass__ = Singleton
    
    _info = {}

    def find(self, executable, version_arg, version_regex):

        self.lock.acquire()
        try:
            if executable in self._info:
                if self._info[executable] is None:
                    return None
                return self._info[executable]
            
            logger.info("Trying to detect availability/location of tool: %s"
                        %(executable))
            
            # initialize the global tool info for this executable
            self._info[executable] = {}
            self._info[executable]['full_path'] = None
            self._info[executable]['version'] = None
            self._info[executable]['version_major'] = None
            self._info[executable]['version_minor'] = None
            self._info[executable]['version_patch'] = None
        
            # figure out the full path to the executable
            full_path = backticks("which " + executable + " 2>/dev/null") 
            full_path = full_path.rstrip('\n')
            if full_path == "":
                logger.info("Command '%s' not found in the current environment"
                            %(executable))
                self._info[executable] = None
                return self._info[executable]
            self._info[executable]['full_path'] = full_path
        
            # version
            if version_regex is None:
                version = "N/A"
            else:
                version = backticks(executable + " " + version_arg + " 2>&1")
                version = version.replace('\n', "")
                re_version = re.compile(version_regex)
                result = re_version.search(version)
                if result:
                    version = result.group(1)
                self._info[executable]['version'] = version
        
            # if possible, break up version into major, minor, patch
            re_version = re.compile("([0-9]+)\.([0-9]+)(\.([0-9]+)){0,1}")
            result = re_version.search(version)
            if result:
                self._info[executable]['version_major'] = int(result.group(1))
                self._info[executable]['version_minor'] = int(result.group(2))
                self._info[executable]['version_patch'] = result.group(4)
            if self._info[executable]['version_patch'] is None or \
                self._info[executable]['version_patch'] == "":
                self._info[executable]['version_patch'] = None
            else:
                self._info[executable]['version_patch'] = \
                    int(self._info[executable]['version_patch'])
        
            logger.info("Tool found: %s   Version: %s   Path: %s" 
                        % (executable, version, full_path))
            return self._info[executable]['full_path']
        finally:
            self.lock.release()


    def full_path(self, executable):
        """ Returns the full path to a given executable """
        self.lock.acquire()
        try:
            if executable in self._info and self._info[executable] is not None:
                return self._info[executable]['full_path']
            return None
        finally:
            self.lock.release()


    def major_version(self, executable):
        """ Returns the detected major version given executable """
        self.lock.acquire()
        try:
            if executable in self._info and self._info[executable] is not None:
                return self._info[executable]['version_major']
            return None
        finally:
            self.lock.release()


# --- functions ----------------------------------------------------------------
                

def setup_logger(debug_flag=False):

    global logger

    # log to the console
    console = logging.StreamHandler()
    
    # default log level - make logger/console match
    logger.setLevel(logging.INFO)
    console.setLevel(logging.INFO)

    # debug - from command line
    if debug_flag:
        logger.setLevel(logging.DEBUG)
        console.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("%(asctime)s %(levelname)7s:  %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)
    logger.debug("Logger has been configured")


def myexec(cmd_line, timeout_secs, should_log):
    """
    executes shell commands with the ability to time out if the command hangs
    """
    global delay_exit_code
    if should_log or logger.isEnabledFor(logging.DEBUG):
        logger.info(cmd_line)
    sys.stdout.flush()

    p = subprocess.Popen(cmd_line, shell=True)
    stdoutdata, stderrdata = p.communicate()
    rc = p.returncode
    if rc != 0:
        raise RuntimeError("Command '%s' failed with error code %s"
                           % (cmd_line, rc))


def backticks(cmd_line):
    """
    what would a python program be without some perl love?
    """
    return subprocess.Popen(cmd_line, shell=True,
                            stdout=subprocess.PIPE).communicate()[0]


def check_cred_fs_permissions(path):
    """
    Checks to make sure a given credential is protected by the file system
    permissions. If left too open (for example after a transfer over GASS,
    chmod it to be readable only by us.
    """
    if oct(os.stat(path).st_mode & 0777) != '0600':
        logger.warning("%s found to have weak permissions. chmod to 0600."
                       %(path))
        os.chmod(path, 0600)
