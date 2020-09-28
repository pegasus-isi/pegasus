import io
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest

from Pegasus import yaml
from Pegasus.braindump import Braindump
from Pegasus.client._client import (
    Client,
    PegasusClientError,
    Result,
    Workflow,
    WorkflowInstanceError,
    from_env,
)


def test_PegasusClientError():
    return_value = namedtuple("return_value", ["stdout", "stderr"])
    rv = return_value("stdout", "stderr")
    try:
        raise PegasusClientError("pegasus command failed", rv)
    except PegasusClientError as e:
        assert e.output == "stdout\nstderr"
        assert e.result == rv


def test_from_env(mocker):
    mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
    try:
        from_env()
        shutil.which.assert_called_once_with("pegasus-version")
    except ValueError as e:
        pytest.fail("should not have thrown {}".format(e))


def test_from_env_no_pegasus_home(monkeypatch):
    monkeypatch.setenv("PATH", "/tmp")
    with pytest.raises(ValueError) as e:
        from_env()

    assert "PEGASUS_HOME not found" in str(e)


@pytest.fixture(scope="function")
def mock_subprocess(mocker):
    class Popen:
        def __init__(self):
            self.stdout = io.BytesIO(b"some initial binary data: \x00\x01\n")
            self.stderr = io.BytesIO(b"some initial binary data: \x00\x01\n")
            self.returncode = 0

        def poll(self):
            return 0

        def __del__(self):
            self.stdout.close()
            self.stderr.close()

    mocker.patch("subprocess.Popen", return_value=Popen())


@pytest.fixture(scope="function")
def client():
    return Client("/path")


class TestClient:
    def test_plan(self, mocker, mock_subprocess, client):
        mocker.patch(
            "Pegasus.client._client.Workflow._get_braindump",
            return_value=Braindump(user="ryan"),
        )
        wf_instance = client.plan(
            abstract_workflow="wf.yml",
            conf="pegasus.conf",
            sites=["site1", "site2"],
            output_sites=["local", "other_site"],
            staging_sites={"es1": "ss1", "es2": "ss2"},
            input_dirs=["/input_dir1", "/input_dir2"],
            output_dir="/output_dir",
            dir="/dir",
            relative_dir="/relative_dir",
            random_dir="/random/dir",
            cleanup="leaf",
            reuse=["/submit_dir1", "/submit_dir2"],
            verbose=3,
            force=True,
            submit=True,
            env=123,
        )
        subprocess.Popen.assert_called_once_with(
            [
                "/path/bin/pegasus-plan",
                "-Denv=123",
                "--conf",
                "pegasus.conf",
                "--sites",
                "site1,site2",
                "--output-sites",
                "local,other_site",
                "--staging-site",
                "es1=ss1,es2=ss2",
                "--input-dir",
                "/input_dir1,/input_dir2",
                "--output-dir",
                "/output_dir",
                "--dir",
                "/dir",
                "--relative-dir",
                "/relative_dir",
                "--randomdir=/random/dir",
                "--cleanup",
                "leaf",
                "--reuse",
                "/submit_dir1,/submit_dir2",
                "-vvv",
                "--force",
                "--submit",
                "wf.yml",
            ],
            stderr=-1,
            stdout=-1,
        )

        assert wf_instance.braindump.user == "ryan"

    def test_plan_invalid_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", sites="local")

        assert "invalid sites: local" in str(e)

    def test_plan_invalid_staging_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", staging_sites="condorpool=origin")

        assert "invalid staging_sites: condorpool=origin" in str(e)

    def test_plan_invalid_output_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", output_sites="site1,site2")

        assert "invalid output_sites: site1,site2" in str(e)

    def test_plan_invalid_input_dirs(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", input_dirs="/input_dir")

        assert "invalid input_dirs: /input_dir" in str(e)

    def test_run(self, mock_subprocess, client):
        client.run("submit_dir", verbose=3, json=True)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-run", "-vvv", "-j", "submit_dir"], stderr=-1, stdout=-1
        )

    def test_status(self, mock_subprocess, client):
        client.status("submit_dir", long=True, verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-status", "--long", "-vvv", "submit_dir"],
            stderr=-1,
            stdout=-1,
        )

    @pytest.mark.parametrize(
        "pegasus_status_out, expected_wait_out",
        [
            (
                dedent(
                    """
                (no matching jobs found in Condor Q)
                UNRDY READY   PRE  IN_Q  POST  DONE   FAIL   %DONE STATE   DAGNAME
                    0     0     0     0     0  1,000  1,000  100.0 Success *wf-name-0.dag
                Summary: 1 DAG total (Success:1)
                """
                ).encode("utf8"),
                "\r[\x1b[1;32m##################################################\x1b[0m] 100.0% ..Success (\x1b[1;32mCompleted: 1000\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 1000\x1b[0m)\n",
            ),
            (
                dedent(
                    """
                STAT  IN_STATE  JOB
                Run      01:10  appends-0 ( /nas/home/tanaka/workflows/test-workflow-1583372721 )
                Summary: 1 Condor job total (R:1)

                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                    4     0     0     0     0     3     1  37.5 Failure *wf-name-0.dag
                Summary: 1 DAG total (Failure:1)
                """
                ).encode("utf8"),
                "\r[\x1b[1;32m###################\x1b[0m-------------------------------]  37.5% ..Failure (\x1b[1;32mCompleted: 3\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 1\x1b[0m)\n",
            ),
            (
                dedent(
                    """
                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                0     0     0     0     0     6     0 100.0 Success 00/00/analysis-wf_ID0000001/analysis-wf-0.dag
                0     0     0     0     0     3     0 100.0 Success 00/00/sleep-wf_ID0000002/sleep-wf-0.dag
                0     0     0     0     0    11     0 100.0 Success *wf-name-0.dag
                0     0     0     0     0    20     0 100.0         TOTALS (20 jobs)
                Summary: 3 DAGs total (Success:3)
                """
                ).encode("utf8"),
                "\r[\x1b[1;32m##################################################\x1b[0m] 100.0% ..Success (\x1b[1;32mCompleted: 11\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 0\x1b[0m)\n",
            ),
        ],
    )
    def test_wait(self, mocker, capsys, client, pegasus_status_out, expected_wait_out):
        mocker.patch(
            "subprocess.run",
            return_value=CompletedProcess(
                None, returncode=0, stdout=pegasus_status_out, stderr=""
            ),
        )
        client.wait(root_wf_name="wf-name", submit_dir="submit_dir")
        out, _ = capsys.readouterr()
        assert out == expected_wait_out

    def test_remove(self, mock_subprocess, client):
        client.remove("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-remove", "-vvv", "submit_dir"], stderr=-1, stdout=-1
        )

    def test_analyzer(self, mock_subprocess, client):
        client.analyzer("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-analyzer", "-vvv", "submit_dir"], stderr=-1, stdout=-1
        )

    def test_statistics(self, mock_subprocess, client):
        client.statistics("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-statistics", "-vvv", "submit_dir"], stderr=-1, stdout=-1
        )

    def test_graph(self, mock_subprocess, client):
        client.graph(
            workflow_file="workflow.yml",
            include_files=True,
            no_simplify=False,
            label="label",
            output="wf.dot",
            remove=["tr1", "tr2"],
            width=256,
            height=256,
        )
        subprocess.Popen.assert_called_once_with(
            [
                "/path/bin/pegasus-graphviz",
                "workflow.yml",
                "--files",
                "--nosimplify",
                "--label=label",
                "--output=wf.dot",
                "--remove=tr1",
                "--remove=tr2",
                "--width=256",
                "--height=256",
            ],
            stderr=-1,
            stdout=-1,
        )

    def test__exec(self, mock_subprocess, client):
        client._exec("ls")
        with pytest.raises(ValueError) as e:
            client._exec(None)

        assert str(e.value) == "cmd is required"

    def test__get_submit_dir(self):
        plan_output_with_direct_submit = dedent(
            """
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
        """
        )

        assert (
            Client._get_submit_dir(plan_output_with_direct_submit)
            == "/local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()"
        )

        plan_output_without_direct_submit = dedent(
            """
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

        assert (
            Client._get_submit_dir(plan_output_without_direct_submit)
            == "/local-scratch/tanaka/workflows/test-workflow-THIS-SHOULD-BE-FOUND-BY-Client._get_submit_dir()"
        )


@pytest.fixture(scope="function")
def make_result():
    def _make_result(cmd="command", exit_code=0, stdout=b"", stderr=b""):
        r = Result(cmd, exit_code, stdout, stderr)
        return r

    return _make_result


def test_raise_exit_code(make_result):
    r = make_result()
    assert r.raise_exit_code() is None

    with pytest.raises(ValueError) as e:
        r = make_result(exit_code=1)
        r.raise_exit_code()

    assert e.value.args[1] == r


def test_empty(make_result):
    r = make_result()
    assert r.output == ""
    assert r.stdout == ""
    assert r.stderr == ""
    assert r.json is None
    assert r.yaml is None
    assert r.yaml_all is None


def test_output(make_result):
    r = make_result(stdout=b"test")
    assert r.output == "test"


def test_output_fail(make_result):
    r = make_result(stdout=None)
    with pytest.raises(ValueError) as e:
        r.stdout
    assert str(e.value) == "stdout not captured"


def test_stdout(make_result):
    r = make_result(stdout=b"test")
    assert r.stdout == "test"


def test_stdout_fail(make_result):
    r = make_result(stdout=None)
    with pytest.raises(ValueError) as e:
        r.stdout
    assert str(e.value) == "stdout not captured"


def test_stderr(make_result):
    r = make_result(stderr=b"test")
    assert r.stderr == "test"


def test_stderr_fail(make_result):
    r = make_result(stderr=None)
    with pytest.raises(ValueError) as e:
        r.stderr
    assert str(e.value) == "stderr not captured"


def test_json(make_result):
    r = make_result(stdout=b'{"a": 1}')
    assert isinstance(r.json, dict)
    assert r.json["a"] == 1


def test_yaml(make_result):
    r = make_result(stdout=b"a: 1")
    assert isinstance(r.yaml, dict)
    assert r.yaml["a"] == 1


def test_yaml_all(make_result):
    r = make_result(
        stdout=b"""---
a: 1
---
b: 2
"""
    )

    d = [y for y in r.yaml_all]
    assert isinstance(d, list)
    assert len(d) == 2
    assert d[0]["a"] == 1
    assert d[1]["b"] == 2


class TestWorkflow:
    def test__get_braindump(self):
        # create a fake temporary submit dir and braindump.yml file
        with TemporaryDirectory() as td:
            bd_path = Path(td) / "braindump.yml"
            with bd_path.open("w+") as bd_file:
                yaml.dump({"user": "ryan", "submit_dir": "/submit_dir"}, bd_file)
                bd_file.seek(0)
                bd = Workflow._get_braindump(bd_path.parent)

                assert bd.user == "ryan"
                assert bd.submit_dir == Path("/submit_dir")

    def test_try_get_missing_braindump(self):
        with TemporaryDirectory() as td:
            with pytest.raises(WorkflowInstanceError) as e:
                Workflow._get_braindump(td)

            assert "Unable to load braindump file" in str(e)
