import os 
import subprocess
import re
from collections import namedtuple
from textwrap import dedent

import pytest

from Pegasus.client._client import from_env
from Pegasus.client._client import Client

def test_from_env(pegasus_version_file):
    try:
        client = from_env()
    except ValueError as e:
        pytest.fail("should not have thrown {}".format(e))

def test_from_env_no_pegasus_home(monkeypatch):
    monkeypatch.setenv("PATH", "/tmp")
    with pytest.raises(ValueError) as e:
        from_env()
    
    assert "PEGASUS_HOME not found" in str(e)

@pytest.fixture(scope="function")
def mock_subprocess(mocker):
    CompletedProcess = namedtuple("CompletedProcess", ["returncode", "stdout", "stderr"])
    cp = CompletedProcess(
        returncode=0,
        stdout=" ",
        stderr=" "
    )
    mocker.patch("subprocess.run", return_value=cp)

@pytest.fixture(scope="function")
def client():
    return Client("/path")

class TestClient:
    def test_plan(self, mock_subprocess, client):
        client.plan(
            "dax.yml",
            conf="pegasus.conf",
            sites="local",
            output_site="local",
            input_dir="/input_dir",
            output_dir="/output_dir",
            dir="/dir",
            relative_dir="/relative_dir",
            cleanup="leaf",
            verbose=3,
            force=True,
            submit=True,
            env=123
        )
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-plan",
                "-Denv=123",
                "--conf", 
                "pegasus.conf",
                "--sites", 
                "local",
                "--output-site",
                "local",
                "--input-dir",
                "/input_dir",
                "--output-dir",
                "/output_dir",
                "--dir",
                "/dir",
                "--relative-dir",
                "/relative_dir",
                "--cleanup", 
                "leaf",
                "-vvv",
                "--force",
                "--submit",
                "--dax", 
                "dax.yml"
            ]
        )

    def test_run(self, mock_subprocess, client):
        client.run("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-run", 
                "-vvv", 
                "submit_dir"
            ]
        )

    def test_status(self, mock_subprocess, client):
        client.status("submit_dir", long=True, verbose=3)
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-status",
                "--long",
                "-vvv",
                "submit_dir"
            ]
        )

    def test_remove(self, mock_subprocess, client):
        client.remove("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-remove",
                "-vvv",
                "submit_dir"
            ]
        )

    def test_analyzer(self, mock_subprocess, client):
        client.analyzer("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-analyzer",
                "-vvv",
                "submit_dir"
            ]
        )

    def test_statistics(self, mock_subprocess, client):
        client.statistics("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            [
                "/path/bin/pegasus-statistics",
                "-vvv",
                "submit_dir"
            ]
        )

    def test__get_submit_dir(self):
        plan_output_with_direct_submit = dedent("""
            2020.02.11 15:39:42.958 PST:
            2020.02.11 15:39:42.963 PST:   -----------------------------------------------------------------------
            2020.02.11 15:39:42.969 PST:   File for submitting this DAG to HTCondor           : appends-0.dag.condor.sub
            2020.02.11 15:39:42.974 PST:   Log of DAGMan debugging messages                 : appends-0.dag.dagman.out
            2020.02.11 15:39:42.979 PST:   Log of HTCondor library output                     : appends-0.dag.lib.out
            2020.02.11 15:39:42.984 PST:   Log of HTCondor library error messages             : appends-0.dag.lib.err
            2020.02.11 15:39:42.990 PST:   Log of the life of condor_dagman itself          : appends-0.dag.dagman.log
            2020.02.11 15:39:42.995 PST:
            2020.02.11 15:39:43.000 PST:   -no_submit given, not submitting DAG to HTCondor.  You can do this with:
            2020.02.11 15:39:43.010 PST:   -----------------------------------------------------------------------
            2020.02.11 15:39:43.820 PST:   Your database is compatible with Pegasus version: 4.9.3
            2020.02.11 15:39:43.912 PST:   Submitting to condor appends-0.dag.condor.sub
            2020.02.11 15:39:43.940 PST:   Submitting job(s).
            2020.02.11 15:39:43.945 PST:   1 job(s) submitted to cluster 1533083.
            2020.02.11 15:39:43.950 PST:
            2020.02.11 15:39:43.956 PST:   Your workflow has been started and is running in the base directory:
            2020.02.11 15:39:43.961 PST:
            2020.02.11 15:39:43.966 PST:     /local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()
            2020.02.11 15:39:43.971 PST:
            2020.02.11 15:39:43.977 PST:   *** To monitor the workflow you can run ***
            2020.02.11 15:39:43.982 PST:
            2020.02.11 15:39:43.987 PST:     pegasus-status -l /local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()
            2020.02.11 15:39:43.992 PST:
            2020.02.11 15:39:43.998 PST:   *** To remove your workflow run ***
            2020.02.11 15:39:44.003 PST:
            2020.02.11 15:39:44.008 PST:     pegasus-remove /local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()
            2020.02.11 15:39:44.013 PST:
            2020.02.11 15:39:44.069 PST:   Time taken to execute is 2.117 seconds
        """)

        assert Client._get_submit_dir(plan_output_with_direct_submit) == "/local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()"
        
        plan_output_without_direct_submit = dedent("""
            2020.02.11 15:42:04.236 PST:
            2020.02.11 15:42:04.242 PST:   -----------------------------------------------------------------------
            2020.02.11 15:42:04.247 PST:   File for submitting this DAG to HTCondor           : appends-0.dag.condor.sub
            2020.02.11 15:42:04.252 PST:   Log of DAGMan debugging messages                 : appends-0.dag.dagman.out
            2020.02.11 15:42:04.258 PST:   Log of HTCondor library output                     : appends-0.dag.lib.out
            2020.02.11 15:42:04.263 PST:   Log of HTCondor library error messages             : appends-0.dag.lib.err
            2020.02.11 15:42:04.268 PST:   Log of the life of condor_dagman itself          : appends-0.dag.dagman.log
            2020.02.11 15:42:04.273 PST:
            2020.02.11 15:42:04.279 PST:   -no_submit given, not submitting DAG to HTCondor.  You can do this with:
            2020.02.11 15:42:04.289 PST:   -----------------------------------------------------------------------
            2020.02.11 15:42:05.120 PST:   Your database is compatible with Pegasus version: 4.9.3
            2020.02.11 15:42:05.126 PST:


            I have concretized your abstract workflow. The workflow has been entered
            into the workflow database with a state of "planned". The next step is
            to start or execute your workflow. The invocation required is


            pegasus-run  /local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()
        """
        )

        assert Client._get_submit_dir(plan_output_without_direct_submit) == "/local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()"
        