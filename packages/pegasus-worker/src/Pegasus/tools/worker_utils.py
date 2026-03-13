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

import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

from six.moves.builtins import int

# Module variables
logger = logging.getLogger("Pegasus")


def force_str(s):
    """Decode a bytes object to a str.

    :param s: the bytes object to decode
    :type s: bytes
    :return: decoded string
    :rtype: str
    """
    return s.decode()


def backticks(cmd_line):
    """Execute a shell command and return its stdout as a string.

    :param cmd_line: the shell command to execute
    :type cmd_line: str
    :return: stdout of the command
    :rtype: str
    """
    return force_str(
        subprocess.Popen(cmd_line, shell=True, stdout=subprocess.PIPE).communicate()[0]
    )


class TimedCommand:
    """Provides a shell callout with a configurable timeout (default 6 hours)."""

    def __init__(
        self,
        cmd,
        cmd_display=None,
        env_overrides={},
        timeout_secs=6 * 60 * 60,
        log_cmd=True,
        log_outerr=True,
        cwd=None,
    ):
        """
        :param cmd: the shell command to execute
        :type cmd: str
        :param cmd_display: an alternate command string used for logging (e.g. to hide credentials); if None, ``cmd`` is used
        :type cmd_display: str, optional
        :param env_overrides: environment variable overrides to apply in the subprocess environment
        :type env_overrides: dict, optional
        :param timeout_secs: maximum number of seconds to wait before killing the process; defaults to 6 hours
        :type timeout_secs: int, optional
        :param log_cmd: whether to log the command before executing it, defaults to True
        :type log_cmd: bool, optional
        :param log_outerr: whether to log the combined stdout/stderr after the command completes, defaults to True
        :type log_outerr: bool, optional
        :param cwd: working directory for the subprocess; if None, the current working directory is used
        :type cwd: str, optional
        """
        self._cmd = cmd
        if cmd_display is None:
            self._cmd_display = cmd
        else:
            self._cmd_display = cmd_display
        self._env_overrides = env_overrides.copy()
        self._timeout_secs = timeout_secs
        self._log_cmd = log_cmd
        self._log_outerr = log_outerr
        self._process = None
        self._out_file = None
        self._outerr = ""
        self._duration = 0.0
        self._cwd = cwd

    def run(self):
        """Execute the command, blocking until it completes or the timeout expires.

        :raises RuntimeError: if the command times out
        :raises RuntimeError: if the command exits with a non-zero exit code
        """

        def target():

            # custom environment for the subshell
            sub_env = os.environ.copy()
            for key, value in self._env_overrides.items():
                logger.debug(f"ENV override: {key} = {value}")
                sub_env[key] = value

            self._process = subprocess.Popen(
                self._cmd,
                shell=True,
                env=sub_env,
                stdout=self._out_file,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setpgrp,
                cwd=self._cwd,
            )
            self._process.communicate()

        if self._log_cmd or logger.isEnabledFor(logging.DEBUG):
            logger.info(self._cmd_display)

        sys.stdout.flush()

        # temp file for the stdout/stderr
        self._out_file = tempfile.TemporaryFile(prefix="pegasus-", suffix=".out")

        ts_start = time.time()

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(self._timeout_secs)
        if thread.is_alive():
            # do our best to kill the whole process group
            try:
                # os.killpg did not work in all environments
                # os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                kill_cmd = "kill -TERM -%d" % (os.getpgid(self._process.pid))
                kp = subprocess.Popen(kill_cmd, shell=True)
                kp.communicate()
                self._process.terminate()
            except Exception:
                pass
            thread.join()
            # log the output
            self._out_file.seek(0)
            stdout = str.strip(force_str(self._out_file.read()))
            if len(stdout) > 0:
                logger.info(stdout)
            self._out_file.close()
            raise RuntimeError(
                "Command timed out after %d seconds: %s"
                % (self._timeout_secs, self._cmd_display)
            )

        self._duration = time.time() - ts_start

        # log the output
        self._out_file.seek(0)
        self._outerr = str.strip(force_str(self._out_file.read()))
        if self._log_outerr and len(self._outerr) > 0:
            logger.info(self._outerr)
        self._out_file.close()

        if self._process.returncode != 0:
            print(self.get_outerr())
            raise RuntimeError(
                "Command exited with non-zero exit code (%d): %s"
                % (self._process.returncode, self._cmd_display)
            )

    def get_outerr(self):
        """Return the combined stdout and stderr captured from the command.

        :return: combined stdout and stderr output
        :rtype: str
        """
        return self._outerr

    def get_exit_code(self):
        """Return the exit code of the process.

        :return: process exit code
        :rtype: int
        """
        return self._process.returncode

    def get_duration(self):
        """Return the wall-clock duration of the command execution in seconds.

        :return: execution time in seconds
        :rtype: float
        """
        return self._duration


class Tools:
    """
    Singleton for detecting and maintaining tools we depend on
    """

    # singleton
    instance = None

    def __init__(self):
        if not Tools.instance:
            Tools.instance = Tools.__Tools()

    def __getattr__(self, name):
        return getattr(self.instance, name)

    class __Tools:
        _info = {}

        def __init__(self):
            self.lock = threading.Lock()

        def find(
            self, executable, version_arg=None, version_regex=None, path_prepend=None
        ):
            """Detect and cache the full path and version of an executable.

            Searches ``PATH`` (and optionally prepended directories) for the
            given executable, optionally running it with ``version_arg`` and
            extracting the version string via ``version_regex``.

            :param executable: name of the executable to find (e.g. ``"gsiftp"``)
            :type executable: str
            :param version_arg: argument to pass to the executable to obtain its version (e.g. ``"--version"``)
            :type version_arg: str, optional
            :param version_regex: regex with one capture group to extract the version number from the version output
            :type version_regex: str, optional
            :param path_prepend: list of additional directories to search before ``PATH``
            :type path_prepend: list, optional
            :return: full path to the executable, or None if not found
            :rtype: str or None
            """
            self.lock.acquire()
            try:
                if executable in self._info:
                    if self._info[executable] is None:
                        return None
                    return self._info[executable]["full_path"]

                logger.debug(
                    "Trying to detect availability/location of tool: %s" % (executable)
                )

                # initialize the global tool info for this executable
                self._info[executable] = {}
                self._info[executable]["full_path"] = None
                self._info[executable]["version"] = None
                self._info[executable]["version_major"] = None
                self._info[executable]["version_minor"] = None
                self._info[executable]["version_patch"] = None

                # figure out the full path to the executable
                path_entries = os.environ["PATH"].split(":")
                if "" in path_entries:
                    path_entries.remove("")
                # if we have PEGASUS_HOME set, and try to invoke a Pegasus tool, prepend
                if "PEGASUS_HOME" in os.environ and executable.find("pegasus-") == 0:
                    path_entries.insert(0, os.environ["PEGASUS_HOME"] + "/bin")
                if path_prepend is not None:
                    for entry in path_prepend:
                        path_entries.insert(0, entry)

                # now walk the path
                full_path = None
                for entry in path_entries:
                    full_path = entry + "/" + executable
                    if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
                        break
                    full_path = None

                if full_path is None:
                    logger.info(
                        "Command '%s' not found in the current environment"
                        % (executable)
                    )
                    self._info[executable] = None
                    return self._info[executable]
                self._info[executable]["full_path"] = full_path

                # version
                if version_regex is None:
                    version = "N/A"
                else:
                    version = backticks(executable + " " + version_arg + " 2>&1")
                    version = version.replace("\n", "")
                    re_version = re.compile(version_regex)
                    result = re_version.search(version)
                    if result:
                        version = result.group(1)
                    self._info[executable]["version"] = version

                # if possible, break up version into major, minor, patch
                re_version = re.compile(r"([0-9]+)\.([0-9]+)(\.([0-9]+)){0,1}")
                result = re_version.search(version)
                if result:
                    self._info[executable]["version_major"] = int(result.group(1))
                    self._info[executable]["version_minor"] = int(result.group(2))
                    self._info[executable]["version_patch"] = result.group(4)
                if (
                    self._info[executable]["version_patch"] is None
                    or self._info[executable]["version_patch"] == ""
                ):
                    self._info[executable]["version_patch"] = None
                else:
                    self._info[executable]["version_patch"] = int(
                        self._info[executable]["version_patch"]
                    )

                logger.info(
                    "Tool found: %s   Version: %s   Path: %s"
                    % (executable, version, full_path)
                )
                return self._info[executable]["full_path"]
            finally:
                self.lock.release()

        def full_path(self, executable):
            """Return the cached full path to the given executable.

            :param executable: name of the executable
            :type executable: str
            :return: full path, or None if not found or not yet detected
            :rtype: str or None
            """
            self.lock.acquire()
            try:
                if executable in self._info and self._info[executable] is not None:
                    return self._info[executable]["full_path"]
                return None
            finally:
                self.lock.release()

        def major_version(self, executable):
            """Return the cached major version number of the given executable.

            :param executable: name of the executable
            :type executable: str
            :return: major version number, or None if unknown
            :rtype: int or None
            """
            self.lock.acquire()
            try:
                if executable in self._info and self._info[executable] is not None:
                    return self._info[executable]["version_major"]
                return None
            finally:
                self.lock.release()

        def minor_version(self, executable):
            """Return the cached minor version number of the given executable.

            :param executable: name of the executable
            :type executable: str
            :return: minor version number, or None if unknown
            :rtype: int or None
            """
            self.lock.acquire()
            try:
                if executable in self._info and self._info[executable] is not None:
                    return self._info[executable]["version_minor"]
                return None
            finally:
                self.lock.release()

        def version_comparable(self, executable):
            """Return a zero-padded version string suitable for lexicographic comparison (e.g. ``"001002003"`` for v1.2.3).

            :param executable: name of the executable
            :type executable: str
            :return: comparable version string, or None if version is unknown
            :rtype: str or None
            """
            self.lock.acquire()
            try:
                if executable in self._info and self._info[executable] is not None:
                    return "%03d%03d%03d" % (
                        int(self._info[executable]["version_major"]),
                        int(self._info[executable]["version_minor"]),
                        int(self._info[executable]["version_patch"]),
                    )
                return None
            finally:
                self.lock.release()
