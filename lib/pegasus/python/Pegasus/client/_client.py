# -*- coding: utf-8 -*-

import logging
import re
import subprocess
from functools import partial
from os import path
from shutil import which

def from_env(pegasus_home=None):
    if not pegasus_home:
        pegasus_version_path = which("pegasus-version")

        if not pegasus_version_path:
            raise ValueError("PEGASUS_HOME not found")

        pegasus_home = path.dirname(path.dirname(pegasus_version_path))

    return Client(pegasus_home)


class Client(object):
    """
    Pegasus client.

    Pegasus workflow management client.
    """

    def __init__(self, pegasus_home):
        self._log = logging.getLogger(__name__)
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
        dax,
        conf=None,
        sites="local",
        output_site="local",
        input_dir=None,
        output_dir=None,
        dir=None,
        relative_dir=None,
        cleanup="none",
        verbose=None,
        force=False,
        submit=False,
        **kwargs
    ):
        cmd = [self._plan]

        system_properties = []
        for k, v in kwargs or {}:
            system_properties.append("-D%s=%s" % (k, v))
        else:
            cmd.append(system_properties)

        if conf:
            cmd.extend(("--conf", conf))

        if sites:
            cmd.extend(("--sites", sites))

        if output_site:
            cmd.extend(("--output-site", output_site))

        if input_dir:
            cmd.extend(("--input-dir", input_dir))

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

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Plan: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Plan: %s \n %s" % rv.stdout, rv.stderr)

        submit_dir = self._get_submit_dir(rv.stdout)
        workflow = Workflow(submit_dir, self)
        return workflow

    def run(self, submit_dir, verbose=None):
        cmd = [self._run]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Run: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Run: %s \n %s" % rv.stdout, rv.stderr)

    def status(self, submit_dir, long=False, verbose=None):
        cmd = [self._status]

        if long:
            cmd.append("--long")

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Status: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Status: %s \n %s" % rv.stdout, rv.stderr)

    def remove(self, submit_dir, verbose=None):
        cmd = [self._remove]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Remove: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Remove: %s \n %s" % rv.stdout, rv.stderr)

    def analyzer(self, submit_dir, verbose=None):
        cmd = [self._analyzer]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Analyzer: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Analyzer: %s \n %s" % rv.stdout, rv.stderr)

    def statistics(self, submit_dir, verbose=None):
        cmd = [self._statistics]

        if verbose:
            cmd.append("-" + "v" * verbose)

        cmd.append(submit_dir)

        rv = subprocess.run(cmd)

        if rv.returncode:
            self._log.fatal("Statistics: %s \n %s" % rv.stdout, rv.stderr)

        self._log.info("Statistics: %s \n %s" % rv.stdout, rv.stderr)

    @staticmethod
    def _get_submit_dir(output):
        if not output:
            return

        pattern = re.compile("pegasus-run\s*(.*)$")

        for line in output.splitlines():
            line = line.strip()
            match = pattern.match(line)

            if match:
                return match.group(1)


class Workflow(object):
    def __init__(self, submit_dir, client=None):
        self._log = logging.getLogger(__name__)
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
    def client(self, client):
        self._client = client

        self.run = partial(self._client.run, self._submit_dir)
        self.status = partial(self._client.status, self._submit_dir)
        self.remove = partial(self._client.remove, self._submit_dir)
        self.analyze = partial(self._client.analyzer, self._submit_dir)
        self.statistics = partial(self._client.statistics, self._submit_dir)
