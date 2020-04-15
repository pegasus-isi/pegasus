import shutil
import subprocess
from subprocess import CompletedProcess
from textwrap import dedent

import pytest

from Pegasus.client._client import Client, Result, from_env


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
    cp = CompletedProcess(None, returncode=0, stdout=b" ", stderr=b" ")
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
            env=123,
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
                "dax.yml",
            ]
        )

    def test_run(self, mock_subprocess, client):
        client.run("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            ["/path/bin/pegasus-run", "-vvv", "submit_dir"]
        )

    def test_status(self, mock_subprocess, client):
        client.status("submit_dir", long=True, verbose=3)
        subprocess.run.assert_called_once_with(
            ["/path/bin/pegasus-status", "--long", "-vvv", "submit_dir"]
        )

    @pytest.mark.parametrize(
        "pegasus_status_out, expected_wait_out",
        [
            (
                dedent(
                    """
                (no matching jobs found in Condor Q)
                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                    0     0     0     0     0     8     0 100.0 Success *appends-0.dag
                Summary: 1 DAG total (Success:1)
                """
                ).encode("utf8"),
                "\r[\x1b[1;32m##################################################\x1b[0m] 100.0% ..Success (\x1b[1;32mCompleted: 8\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 0\x1b[0m)\n",
            ),
            (
                dedent(
                    """
                STAT  IN_STATE  JOB
                Run      01:10  appends-0 ( /nas/home/tanaka/workflows/test-workflow-1583372721 )
                Summary: 1 Condor job total (R:1)

                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                    4     0     0     0     0     3     1  37.5 Failure *appends-0.dag
                Summary: 1 DAG total (Failure:1)
                """
                ).encode("utf8"),
                "\r[\x1b[1;32m###################\x1b[0m-------------------------------]  37.5% ..Failure (\x1b[1;32mCompleted: 3\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 1\x1b[0m)\n",
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
        client.wait("submit_dir")
        out, _ = capsys.readouterr()
        assert out == expected_wait_out

    def test_remove(self, mock_subprocess, client):
        client.remove("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            ["/path/bin/pegasus-remove", "-vvv", "submit_dir"]
        )

    def test_analyzer(self, mock_subprocess, client):
        client.analyzer("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            ["/path/bin/pegasus-analyzer", "-vvv", "submit_dir"]
        )

    def test_statistics(self, mock_subprocess, client):
        client.statistics("submit_dir", verbose=3)
        subprocess.run.assert_called_once_with(
            ["/path/bin/pegasus-statistics", "-vvv", "submit_dir"]
        )

    def test__exec(self, mock_subprocess):
        Client._exec("ls")
        with pytest.raises(ValueError) as e:
            Client._exec(None)

        assert str(e.value) == "cmd is required"

    def test__make_result(self):
        with pytest.raises(ValueError) as e:
            Client._make_result(None)

        assert str(e.value) == "rv is required"

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
