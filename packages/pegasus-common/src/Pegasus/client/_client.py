import json
import logging
import re
import shutil
import subprocess
import threading
import time
from functools import partial
from os import path
from pathlib import Path
from typing import BinaryIO, Dict, List, Union

from Pegasus import braindump, yaml

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
        self._graph = path.join(base, "pegasus-graphviz")

    def plan(
        self,
        abstract_workflow: str = None,
        basename: str = None,
        job_prefix: str = None,
        conf: str = None,
        cluster: List[str] = None,
        sites: List[str] = None,
        output_sites: List[str] = ["local"],
        staging_sites: Dict[str, str] = None,
        cache: List[str] = None,
        input_dirs: List[str] = None,
        output_dir: str = None,
        dir: str = None,
        relative_dir: str = None,
        relative_submit_dir: str = None,
        random_dir: Union[bool, str] = False,
        inherited_rc_files: List[str] = None,
        cleanup: str = "none",
        reuse: List[str] = None,
        verbose: int = 0,
        quiet: int = 0,
        force: bool = False,
        force_replan: bool = False,
        forward: List[str] = None,
        submit: bool = False,
        java_options: List[str] = None,
        **kwargs
    ):
        cmd = [self._plan]

        for k, v in kwargs.items():
            cmd.append("-D{}={}".format(k, v))

        if basename:
            cmd.extend(("--basename", basename))

        if job_prefix:
            cmd.extend(("--job-prefix", job_prefix))

        if conf:
            cmd.extend(("--conf", conf))

        if cluster:
            if not isinstance(cluster, list):
                raise TypeError(
                    "invalid cluster: {}; list of str must be given".format(cluster)
                )

            cmd.extend(("--cluster", ",".join(cluster)))

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

        if cache:
            if not isinstance(cache, list):
                raise TypeError(
                    "invalid cache: {}; list of str must be given".format(cache)
                )

            cmd.extend(("--cache", ",".join(cache)))

        if input_dirs:
            if not isinstance(input_dirs, list):
                raise TypeError(
                    "invalid input_dirs: {}; list of str must be given".format(
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

        if relative_submit_dir:
            cmd.extend(("--relative-submit-dir", relative_submit_dir))

        if random_dir:
            if random_dir == True:
                cmd.append("--randomdir")
            else:
                cmd.append("--randomdir={}".format(random_dir))

        if inherited_rc_files:
            if not isinstance(inherited_rc_files, list):
                raise TypeError(
                    "invalid inherited_rc_files: {}; list of str must be given".format(
                        inherited_rc_files
                    )
                )

            cmd.extend(("--inherited-rc-files", ",".join(inherited_rc_files)))

        if cleanup:
            cmd.extend(("--cleanup", cleanup))

        if reuse:
            cmd.extend(("--reuse", ",".join(reuse)))

        if verbose > 0:
            cmd.append("-" + "v" * verbose)

        if quiet > 0:
            cmd.append("-" + "q" * quiet)

        if force:
            cmd.append("--force")

        if force_replan:
            cmd.append("--force-replan")

        if forward:
            if not isinstance(forward, list):
                raise TypeError(
                    "invalid forward: {}; list of str must be given".format(forward)
                )

            for opt in forward:
                cmd.extend(("--forward", opt))

        if submit:
            cmd.append("--submit")

        if java_options:
            if not isinstance(java_options, list):
                raise TypeError(
                    "invalid java_options: {}; list of str must be given".format(
                        java_options
                    )
                )

            for opt in java_options:
                cmd.append("-X{}".format(opt))

        # pegasus-plan will look for "workflow.yml" in cwd by default if
        # it is not given as last positional argument
        if abstract_workflow:
            cmd.append(abstract_workflow)

        # always use --json option
        cmd.append("--json")

        self._log.info("\n################\n# pegasus-plan #\n################")

        # don't stream stdout from planner, as this will be json output
        rv = self._exec(cmd, stream_stdout=False, stream_stderr=True)

        json_output = rv.json
        submit_dir = json_output["submit_dir"]

        # Some tools (launch-bamboo-script, ensemble-mananager) parse submit directory from
        # planner output. Therefore we need to log the following and retain the structure
        # of planner output.
        if submit:
            self._log.info(
                "\nYour workflow has been started and is running in the base directory:\n"
            )
            self._log.info(submit_dir)

            self._log.info("\n*** To monitor the workflow you can run ***\n")
            self._log.info("pegasus-status -l {}\n".format(submit_dir))

            self._log.info("\n*** To remove your workflow run ***\n")
            self._log.info("pegasus-remove {}\n".format(submit_dir))
        else:
            self._log.info("\n\n" + json_output["message"].strip() + "\n\n")
            self._log.info("pegasus-run {}".format(submit_dir))

        workflow = Workflow(submit_dir, self)
        return workflow

    def run(self, submit_dir: str, verbose: int = 0, grid: bool = False):
        cmd = [self._run]

        if verbose:
            cmd.append("-" + "v" * verbose)

        if grid:
            cmd.append("--grid")

        # always use --json option
        cmd.append("--json")

        cmd.append(submit_dir)

        self._log.info("\n###############\n# pegasus-run #\n###############")
        rv = self._exec(cmd, stream_stdout=False, stream_stderr=True)

        # As json output is printed to stdout and captured. Print out the
        # output that is normally emitted to stdout from pegasus-run when the
        # --json option is not given.
        self._log.info(
            "Your workflow has been started and is running in the base directory:\n"
        )
        self._log.info(submit_dir + "\n")
        self._log.info("*** To monitor the workflow you can run ***\n")
        self._log.info("pegasus-status -l {}\n".format(submit_dir))
        self._log.info("*** To remove your workflow run ***\n")
        self._log.info("pegasus-remove {}".format(submit_dir))

        return rv.json

    def status(self, submit_dir: str, long: bool = False, verbose: int = 0):
        cmd = [self._status]

        if long:
            cmd.append("--long")

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        self._log.info("\n##################\n# pegasus-status #\n##################")
        self._exec(cmd)

    def wait(self, root_wf_name: str, submit_dir: str, delay: int = 5):
        """Prints progress bar and blocks until workflow completes or fails"""

        # match output from pegasus-status
        # for example, given the following output:
        #
        # UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
        #     0     0     0     0     0     8     0 100.0 Success *appends-0.dag
        #
        # the pattern would match the second line
        p = re.compile(
            r"\s*(([\d,]+\s+){{7}})(\d+\.\d+\s+)(\w+\s+)(\*{wf_name}.*)".format(
                wf_name=root_wf_name
            )
        )

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

        # progress bar length
        bar_len = 36

        try:
            can_continue = True
            while can_continue:
                rv = subprocess.run(
                    ["pegasus-status", "-l", submit_dir],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                if rv.returncode != 0:
                    raise Exception(rv.stderr)

                found_match = False
                for line in rv.stdout.decode("utf8").split("\n"):
                    matched = p.match(line)

                    if matched:
                        found_match = True
                        v = matched.group(1).split()
                        v.append(float(matched.group(3).strip()))
                        v.append(matched.group(4).strip())

                        completed = green("Completed: " + str(v[DONE]).replace(",", ""))
                        queued = yellow("Queued: " + str(v[READY]).replace(",", ""))
                        running = blue("Running: " + str(v[IN_Q]).replace(",", ""))
                        fail = red("Failed: " + str(v[FAIL]).replace(",", ""))

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

                        filled_len = int(round(bar_len * (v[PCNT_DONE] * 0.01)))

                        bar = (
                            "\r["
                            + green("#" * filled_len)
                            + ("-" * (bar_len - filled_len))
                            + "] {percent:>5}% ..{state} {stats}".format(
                                percent=v[PCNT_DONE], state=v[STATE], stats=stats
                            )
                        )

                        if v[STATE] == "Running":
                            print(bar, end="")
                        elif v[STATE] in ["Failure", "Success"]:
                            can_continue = False
                            print(bar, end="\n")
                        else:
                            # unknown
                            print(bar, end="")

                        # skip the rest of the lines
                        break

                # When workflow is submitted, the pattern above may not be initially
                # present when the workflow job is idle or has just started running.
                if not found_match:
                    bar = (
                        "\r["
                        + ("-" * bar_len)
                        + "] {percent:>5}% ..".format(percent=0.0)
                    )

                    print(bar, end="")

                time.sleep(delay)
        except KeyboardInterrupt:
            print(
                "\nCancelling Client.wait(). Your workflow is still running and can be monitored with pegasus-status"
            )

    def remove(self, submit_dir: str, verbose: int = 0):
        cmd = [self._remove]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        self._log.info("\n##################\n# pegasus-remove #\n##################")
        self._exec(cmd)

    def analyzer(self, submit_dir: str, verbose: int = 0):
        cmd = [self._analyzer]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        self._log.info(
            "\n####################\n# pegasus-analyzer #\n####################"
        )

        self._exec(cmd)

    def statistics(self, submit_dir: str, verbose: int = 0):
        cmd = [self._statistics]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        self._log.info(
            "\n######################\n# pegasus-statistics #\n######################"
        )

        self._exec(cmd)

    def graph(
        self,
        workflow_file: str,
        include_files: bool = True,
        no_simplify: bool = True,
        label: str = "label",
        output: str = None,
        remove: List[str] = None,
        width: int = None,
        height: int = None,
    ):

        cmd = [self._graph]

        cmd.append(workflow_file)

        if include_files:
            cmd.append("--files")

        if not no_simplify:
            cmd.append("--nosimplify")

        cmd.append("--label={}".format(label))

        if output:
            cmd.append("--output={}".format(output))

        if remove:
            for item in remove:
                cmd.append("--remove={}".format(item))

        if width:
            cmd.append("--width={}".format(width))

        if height:
            cmd.append("--height={}".format(height))

        self._log.info(
            "\n####################\n# pegasus-graphviz #\n####################"
        )

        self._exec(cmd)

    @staticmethod
    def _handle_stream(
        proc: subprocess.Popen,
        stream: BinaryIO,
        dst: list,
        logger: logging.Logger = None,
        log_lvl: int = None,
    ):
        """Handler for processing and logging byte streams from subprocess.Popen.

        :param proc: subprocess.Popen object used to run a pegasus CLI tool
        :type proc: subprocess.Popen
        :param stream: either :code:`stdout` or :code:`stderr` of the given proc
        :type stream: BinaryIO
        :param dst: list where proc output from the given stream will be stored, line by line
        :type dst: list
        :param logger: the logger to use, defaults to None
        :type logger: logging.Logger, optional
        :param log_lvl: the log level to use (e.g. :code:`logging.INFO`, :code:`logging.ERROR`), defaults to None
        :type log_lvl: int, optional
        """

        def _log(logger: logging.Logger, log_lvl: int, msg: bytes):
            if logger:
                log_func = {
                    10: logger.debug,
                    20: logger.info,
                    30: logger.warning,
                    40: logger.error,
                    50: logger.critical,
                }
                try:
                    log_func[log_lvl](msg.decode().strip())
                except KeyError:
                    raise ValueError("invalid log_lvl: {}".format(log_lvl))

        log = partial(_log, logger, log_lvl)

        while True:
            line = stream.readline()

            if line:
                dst.append(line)
                log(line)

            # Has proc terminated? If so, collect remaining output and exit.
            if proc.poll() is not None:
                for l in stream.readlines():
                    dst.append(l)
                    log(l)

                break

    def _exec(self, cmd, stream_stdout=True, stream_stderr=False):
        if not cmd:
            raise ValueError("cmd is required")

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stream_handlers = []

        # out is not synchronized, don't access until after stdout_handler completes
        out = []
        stdout_handler = threading.Thread(
            target=Client._handle_stream,
            args=(
                proc,
                proc.stdout,
                out,
                self._log if stream_stdout else None,
                logging.INFO,
            ),
        )
        stream_handlers.append(stdout_handler)
        stdout_handler.start()

        # err is not synchronized, don't access until after stderr_handler completes
        err = []
        stderr_handler = threading.Thread(
            target=Client._handle_stream,
            args=(
                proc,
                proc.stderr,
                err,
                self._log if stream_stderr else None,
                logging.ERROR,
            ),
        )
        stream_handlers.append(stderr_handler)
        stderr_handler.start()

        for sh in stream_handlers:
            sh.join()

        exit_code = proc.returncode

        result = Result(cmd, exit_code, b"".join(out), b"".join(err))

        if exit_code != 0:
            raise PegasusClientError("Pegasus command: {} FAILED".format(cmd), result)

        return result


class WorkflowInstanceError(Exception):
    pass


class Workflow:
    def __init__(self, submit_dir: str, client: Client = None):
        self._log = logging.getLogger("pegasus.client.workflow")
        self._log.addHandler(console_handler)
        self._log.propagate = False

        self._client = None
        self._submit_dir = submit_dir
        self.braindump = self._get_braindump(self._submit_dir)

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

    @staticmethod
    def _get_braindump(submit_dir: str):
        try:
            with (Path(submit_dir) / "braindump.yml").open("r") as f:
                bd = braindump.load(f)
        except FileNotFoundError:
            raise WorkflowInstanceError(
                "Unable to load braindump file: {}".format(path)
            )

        return bd


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
