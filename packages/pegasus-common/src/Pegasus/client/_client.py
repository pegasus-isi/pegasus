import json
import logging
import re
import shutil
import subprocess
import time
from functools import partial
from os import path
from typing import Dict, List

from Pegasus import yaml

# Set log formatting s.t. only messages are shown. Output from pegasus
# commands will already contain log level categories so it isn't necessary
# in these logs.
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))


class PegasusClientError(Exception):
    """Exception raised when an invoked pegasus command line tool returns non 0"""

    def __init__(self, message, result):
        super().__init__(message)

        self.output = result.stdout + "\n" + result.stderr
        self.result = result


def from_env(pegasus_home: str = None):
    if not pegasus_home:
        pegasus_version_path = shutil.which("pegasus-version")

        if not pegasus_version_path:
            raise ValueError("PEGASUS_HOME not found")

        pegasus_home = path.dirname(path.dirname(pegasus_version_path))

    return Client(pegasus_home)


class Client:
    """
    Pegasus client.

    Pegasus workflow management client.
    """

    def __init__(self, pegasus_home: str):
        self._log = logging.getLogger("pegasus.client")
        self._log.addHandler(console_handler)
        self._log.propagate = False

        self._pegasus_home = pegasus_home

        base = path.normpath(path.join(pegasus_home, "bin"))

        self._plan = path.join(base, "pegasus-plan")
        self._run = path.join(base, "pegasus-run")
        self._status = path.join(base, "pegasus-status")
        self._remove = path.join(base, "pegasus-remove")
        self._analyzer = path.join(base, "pegasus-analyzer")
        self._statistics = path.join(base, "pegasus-statistics")

    def plan(
        self,
        dax: str,
        conf: str = None,
        sites: List[str] = None,
        output_sites: List[str] = ["local"],
        staging_sites: Dict[str, str] = None,
        input_dirs: List[str] = None,
        output_dir: str = None,
        dir: str = None,
        relative_dir: str = None,
        cleanup: str = "none",
        verbose: int = 0,
        force: bool = False,
        submit: bool = False,
        **kwargs
    ):
        cmd = [self._plan]

        for k, v in kwargs.items():
            cmd.append("-D{}={}".format(k, v))

        if conf:
            cmd.extend(("--conf", conf))

        if sites:
            if not isinstance(sites, list):
                raise TypeError(
                    "invalid sites: {}; list of str must be given".format(sites)
                )
            cmd.extend(("--sites", ",".join(sites)))

        if output_sites:
            if not isinstance(output_sites, list):
                raise TypeError(
                    "invalid output_sites: {}; list of str must be given".format(
                        output_sites
                    )
                )

            cmd.extend(("--output-sites", ",".join(output_sites)))

        if staging_sites:
            if not isinstance(staging_sites, dict):
                raise TypeError(
                    "invalid staging_sites: {}; dict<str, str> must be given".format(
                        staging_sites
                    )
                )

            cmd.extend(
                (
                    "--staging-site",
                    ",".join([s + "=" + ss for s, ss in staging_sites.items()]),
                )
            )

        if input_dirs:
            if not isinstance(input_dirs, list):
                raise TypeError(
                    "invalid input_dirs: {} list of str must be given".format(
                        input_dirs
                    )
                )

            cmd.extend(("--input-dir", ",".join(input_dirs)))

        if output_dir:
            cmd.extend(("--output-dir", output_dir))

        if dir:
            cmd.extend(("--dir", dir))

        if relative_dir:
            cmd.extend(("--relative-dir", relative_dir))

        if cleanup:
            cmd.extend(("--cleanup", cleanup))

        if verbose:
            cmd.append("-" + "v" * verbose)

        if force:
            cmd.append("--force")

        if submit:
            cmd.append("--submit")

        cmd.extend(("--dax", dax))

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Plan:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Plan:\n{} \n{}".format(rv.stdout, rv.stderr))

        submit_dir = self._get_submit_dir(rv.stdout)
        workflow = Workflow(submit_dir, self)
        return workflow

    def run(self, submit_dir: str, verbose: int = 0):
        cmd = [self._run]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Run:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Run:\n{} \n{}".format(rv.stdout, rv.stderr))

    def status(self, submit_dir: str, long: bool = False, verbose: int = 0):
        cmd = [self._status]

        if long:
            cmd.append("--long")

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Status:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Status:\n{} \n{}".format(rv.stdout, rv.stderr))

    def wait(self, submit_dir: str, delay: int = 2):
        """Prints progress bar and blocks until workflow completes or fails"""

        # match output from pegasus-status
        # for example, given the following output:
        #
        # UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
        #     0     0     0     0     0     8     0 100.0 Success *appends-0.dag
        #
        # the pattern would match the second line
        p = re.compile(r"\s*((\d+\s+){7})(\d+\.\d+\s+)(\w+\s+)(.*)")

        # indexes for info provided from status
        # UNRDY = 0
        READY = 1
        # PRE = 2
        IN_Q = 3
        # POST = 4
        DONE = 5
        FAIL = 6
        PCNT_DONE = 7
        STATE = 8

        # color strings for terminal output
        green = lambda s: "\x1b[1;32m" + s + "\x1b[0m"
        yellow = lambda s: "\x1b[1;33m" + s + "\x1b[0m"
        blue = lambda s: "\x1b[1;36m" + s + "\x1b[0m"
        red = lambda s: "\x1b[1;31m" + s + "\x1b[0m"

        can_continue = True
        while can_continue:
            rv = subprocess.run(
                ["pegasus-status", "-l", submit_dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if rv.returncode != 0:
                raise Exception(rv.stderr)

            for line in rv.stdout.decode("utf8").split("\n"):
                matched = p.match(line)

                if matched:
                    v = matched.group(1).split()
                    v.append(float(matched.group(3).strip()))
                    v.append(matched.group(4).strip())

                    completed = green("Completed: " + v[DONE])
                    queued = yellow("Queued: " + v[READY])
                    running = blue("Running: " + v[IN_Q])
                    fail = red("Failed: " + v[FAIL])

                    stats = (
                        "("
                        + completed
                        + ", "
                        + queued
                        + ", "
                        + running
                        + ", "
                        + fail
                        + ")"
                    )

                    # progress bar
                    bar_len = 50
                    filled_len = int(round(bar_len * (v[PCNT_DONE] * 0.01)))

                    bar = (
                        "\r["
                        + green("#" * filled_len)
                        + ("-" * (bar_len - filled_len))
                        + "] {percent:>5}% ..{state} {stats}".format(
                            percent=v[PCNT_DONE], state=v[STATE], stats=stats
                        )
                    )

                    if v[PCNT_DONE] < 100:
                        if v[STATE] != "Failure":
                            print(bar, end="")
                        else:
                            # failure
                            can_continue = False
                            print(bar, end="\n")
                    else:
                        # percent done >= 100 means STATE = success
                        can_continue = False
                        print(bar)

                    # skip the rest of the lines
                    break

            time.sleep(delay)

    def remove(self, submit_dir: str, verbose: int = 0):
        cmd = [self._remove]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Remove:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Remove:\n{} \n{}".format(rv.stdout, rv.stderr))

    def analyzer(self, submit_dir: str, verbose: int = 0):
        cmd = [self._analyzer]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Analyzer:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Analyzer:\n{} \n{}".format(rv.stdout, rv.stderr))

    def statistics(self, submit_dir: str, verbose: int = 0):
        cmd = [self._statistics]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = self._exec(cmd)

        if rv.exit_code:
            self._log.fatal("Statistics:\n{} \n{}".format(rv.stdout, rv.stderr))

        self._log.info("Statistics:\n{} \n{}".format(rv.stdout, rv.stderr))

    @staticmethod
    def _exec(cmd):
        if not cmd:
            raise ValueError("cmd is required")

        rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        r = Client._make_result(rv)

        if r.exit_code != 0:
            raise PegasusClientError("Pegasus command: {} FAILED".format(cmd), r)

        return r

    @staticmethod
    def _make_result(rv: subprocess.CompletedProcess):
        if not rv:
            raise ValueError("rv is required")

        r = Result(rv.args, rv.returncode, rv.stdout, rv.stderr)
        return r

    @staticmethod
    def _get_submit_dir(output: str):
        if not output:
            return

        # pegasus-plan produces slightly different output based on the presence
        # of the --submit flag, therefore we need to search for
        # pegasus-(run|remove) to get the submit directory
        pattern = re.compile(r"pegasus-(run|remove)\s*(.*)$")

        for line in output.splitlines():
            line = line.strip()
            match = pattern.search(line)

            if match:
                return match.group(2)


class Workflow:
    def __init__(self, submit_dir: str, client: Client = None):
        self._log = logging.getLogger("pegasus.client.workflow")
        self._log.addHandler(console_handler)
        self._log.propagate = False

        self._client = None
        self._submit_dir = submit_dir
        self.client = client or from_env()

        self.run = None
        self.status = None
        self.remove = None
        self.analyze = None
        self.statistics = None

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, client: Client):
        self._client = client

        self.run = partial(self._client.run, self._submit_dir)
        self.status = partial(self._client.status, self._submit_dir)
        self.remove = partial(self._client.remove, self._submit_dir)
        self.analyze = partial(self._client.analyzer, self._submit_dir)
        self.statistics = partial(self._client.statistics, self._submit_dir)


class Result:
    """An object to store outcome from the execution of a script."""

    def __init__(self, cmd, exit_code, stdout_bytes, stderr_bytes):
        self.cmd = cmd
        self.exit_code = exit_code
        self._stdout_bytes = stdout_bytes
        self._stderr_bytes = stderr_bytes
        self._json = None
        self._yaml = None

    def raise_exit_code(self):
        if self.exit_code == 0:
            return
        raise ValueError("Commad failed", self)

    @property
    def output(self):
        """Return standard output as str."""
        return self.stdout

    @property
    def stdout(self):
        """Return standard output as str."""
        if self._stdout_bytes is None:
            raise ValueError("stdout not captured")
        return self._stdout_bytes.decode().replace("\r\n", "\n")

    @property
    def stderr(self):
        """Return standard error as str."""
        if self._stderr_bytes is None:
            raise ValueError("stderr not captured")
        return self._stderr_bytes.decode().replace("\r\n", "\n")

    @property
    def json(self):
        """Return standard out as JSON."""
        if self._stdout_bytes:
            if not self._json:
                self._json = json.loads(self.output)
            return self._json
        else:
            return None

    # @property
    # def ndjson(self):
    #     """Return standard out as newline delimited JSON."""
    #     if self._stdout_bytes:
    #         if not self._json:
    #             self._json = json.load_all(self.output)
    #         return self._json
    #     else:
    #         return None

    @property
    def yaml(self):
        """Return standard out as YAML."""
        if self._stdout_bytes:
            if not self._yaml:
                self._yaml = yaml.load(self.output)
            return self._yaml
        else:
            return None

    @property
    def yaml_all(self):
        """Return standard out as YAML."""
        if self._stdout_bytes:
            if not self._yaml:
                self._yaml = yaml.load_all(self.output)
            return self._yaml
        else:
            return None
