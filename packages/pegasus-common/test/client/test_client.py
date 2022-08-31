import io
import logging
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path
from subprocess import Popen
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest

import Pegasus
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
            self.stdout = io.BytesIO(b'{"key":"value"}')
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
    @pytest.mark.parametrize("log_lvl", [(logging.INFO), (logging.ERROR)])
    def test__handle_stream(self, client, caplog, log_lvl):
        test_logger = logging.getLogger("handle_stream_test")
        caplog.set_level(log_lvl)

        # fork process to print 0\n1\n..4\n"
        proc = Popen(
            ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stuff = []

        # invoke stream handler
        Client._handle_stream(
            proc=proc,
            stream=proc.stdout,
            dst=stuff,
            logger=test_logger,
            log_lvl=log_lvl,
        )

        assert stuff == [b"0\n", b"1\n", b"2\n", b"3\n", b"4\n"]

        for t in caplog.record_tuples:
            if t[0] == "handle_stream_test":
                assert t[1] == log_lvl

    def test__handle_stream_no_logging(self, client, caplog):
        logging.getLogger("handle_stream_test")
        caplog.set_level(logging.DEBUG)

        # fork process to print 0\n1\n..4\n"
        proc = Popen(
            ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stuff = []

        # invoke stream handler
        Client._handle_stream(proc=proc, stream=proc.stdout, dst=stuff)

        assert stuff == [b"0\n", b"1\n", b"2\n", b"3\n", b"4\n"]

        for t in caplog.record_tuples:
            if t[0] == "handle_stream_test":
                pytest.fail(
                    "nothing should have been logged under logger: handle_stream_test"
                )

    def test__handle_stream_invalid_log_lvl(self, client):
        test_logger = logging.getLogger("handle_stream_test")

        # fork process to print 0\n1\n..4\n"
        proc = Popen(
            ["python3", "-c", 'exec("for i in range(5):\\n\\tprint(i)\\n")'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stuff = []

        # invoke stream handler
        with pytest.raises(ValueError) as e:
            Client._handle_stream(
                proc=proc,
                stream=proc.stdout,
                dst=stuff,
                logger=test_logger,
                log_lvl="INVALID_LOG_LEVEL",
            )

        assert "invalid log_lvl: INVALID_LOG_LEVEL" in str(e)

        # for good measure
        proc.kill()

    def test_plan(self, mocker, mock_subprocess, client):
        mocker.patch(
            "Pegasus.client._client.Result.json",
            return_value={"submit_dir": "/submit_dir"},
        )

        mocker.patch(
            "Pegasus.client._client.Workflow._get_braindump",
            return_value=Braindump(user="ryan"),
        )
        wf_instance = client.plan(
            abstract_workflow="wf.yml",
            basename="test_basename",
            cache=["/cache_file1", "/cache_file2"],
            cleanup="leaf",
            cluster=["horizontal", "label"],
            conf="pegasus.conf",
            dir="/dir",
            force=True,
            force_replan=True,
            forward=["arg1", "opt1=value"],
            inherited_rc_files=["f1", "f2"],
            input_dirs=["/input_dir1", "/input_dir2"],
            java_options=["mx1024m", "ms512m"],
            job_prefix="job_pref",
            output_dir="/output_dir",
            output_sites=["local", "other_site"],
            quiet=3,
            random_dir="/random/dir",
            relative_dir="/relative_dir",
            relative_submit_dir="rsd",
            reuse=["/submit_dir1", "/submit_dir2"],
            sites=["site1", "site2"],
            staging_sites={"es1": "ss1", "es2": "ss2"},
            submit=True,
            verbose=3,
            **{"pegasus.mode": "development"},
        )
        subprocess.Popen.assert_called_once_with(
            [
                "/path/bin/pegasus-plan",
                "-Dpegasus.mode=development",
                "--basename",
                "test_basename",
                "--job-prefix",
                "job_pref",
                "--conf",
                "pegasus.conf",
                "--cluster",
                "horizontal,label",
                "--sites",
                "site1,site2",
                "--output-sites",
                "local,other_site",
                "--staging-site",
                "es1=ss1,es2=ss2",
                "--cache",
                "/cache_file1,/cache_file2",
                "--input-dir",
                "/input_dir1,/input_dir2",
                "--output-dir",
                "/output_dir",
                "--dir",
                "/dir",
                "--relative-dir",
                "/relative_dir",
                "--relative-submit-dir",
                "rsd",
                "--randomdir=/random/dir",
                "--inherited-rc-files",
                "f1,f2",
                "--cleanup",
                "leaf",
                "--reuse",
                "/submit_dir1,/submit_dir2",
                "-vvv",
                "-qqq",
                "--force",
                "--force-replan",
                "--forward",
                "arg1",
                "--forward",
                "opt1=value",
                "--submit",
                "-Xmx1024m",
                "-Xms512m",
                "wf.yml",
                "--json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        assert wf_instance.braindump.user == "ryan"

    def test_plan_invalid_cluster(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", cluster="cluster")

        assert "invalid cluster: cluster" in str(e)

    def test_plan_invalid_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", sites="local")

        assert "invalid sites: local" in str(e)

    def test_plan_invalid_staging_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", staging_sites="condorpool=origin")

        assert "invalid staging_sites: condorpool=origin" in str(e)

    def test_plan_invalid_cache(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", cache="cache")

        assert "invalid cache: cache" in str(e)

    def test_plan_invalid_output_sites(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", output_sites="site1,site2")

        assert "invalid output_sites: site1,site2" in str(e)

    def test_plan_invalid_input_dirs(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", input_dirs="/input_dir")

        assert "invalid input_dirs: /input_dir" in str(e)

    def test_plan_invalid_inherited_rc_files(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", inherited_rc_files="files")

        assert "invalid inherited_rc_files: files" in str(e)

    def test_plan_invalid_forward(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", forward="forward")

        assert "invalid forward: forward" in str(e)

    def test_plan_invalid_java_options(self, client):
        with pytest.raises(TypeError) as e:
            client.plan("wf.yml", java_options="opts")

        assert "invalid java_options: opts" in str(e)

    def test_run(self, mock_subprocess, client):
        client.run("submit_dir", verbose=3, grid=True)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-run", "-vvv", "--grid", "--json", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_run_no_grid(self, mock_subprocess, client):
        client.run("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-run", "-vvv", "--json", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_status(self, mock_subprocess, client):
        client.status("submit_dir", long=True, verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-status", "--long", "-vvv", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    @pytest.mark.parametrize(
        "pegasus_status_out, expected_dict",
        [
            (
                dedent(
                    """
                (no matching jobs found in Condor Q)
                UNRDY READY   PRE  IN_Q  POST  DONE   FAIL   %DONE STATE   DAGNAME
                    1     2     3     4     5  1,000  1,000  100.0 Success *wf-name-0.dag
                Summary: 1 DAG total (Success:1)
                """
                ),
                {
                    "totals": {
                        "unready": 1,
                        "ready": 2,
                        "pre": 3,
                        "queued": 4,
                        "post": 5,
                        "succeeded": 1000,
                        "failed": 1000,
                        "percent_done": 100.0,
                        "total": 2015,
                    },
                    "dags": {
                        "root": {
                            "unready": 1,
                            "ready": 2,
                            "pre": 3,
                            "queued": 4,
                            "post": 5,
                            "succeeded": 1000,
                            "failed": 1000,
                            "percent_done": 100.0,
                            "state": "Success",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
            ),
            (
                dedent(
                    """
                STAT  IN_STATE  JOB
                Run      01:10  appends-0 ( /nas/home/tanaka/workflows/test-workflow-1583372721 )
                Summary: 1 Condor job total (R:1)

                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                    1     2     3     4     5     6     7  37.5 Failure *wf-name-0.dag
                Summary: 1 DAG total (Failure:1)
                """
                ),
                {
                    "totals": {
                        "unready": 1,
                        "ready": 2,
                        "pre": 3,
                        "queued": 4,
                        "post": 5,
                        "succeeded": 6,
                        "failed": 7,
                        "percent_done": 37.5,
                        "total": 28,
                    },
                    "dags": {
                        "root": {
                            "unready": 1,
                            "ready": 2,
                            "pre": 3,
                            "queued": 4,
                            "post": 5,
                            "succeeded": 6,
                            "failed": 7,
                            "percent_done": 37.5,
                            "state": "Failure",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
            ),
            (
                dedent(
                    """
                UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
                0     0     0     0     0     6     0 100.0 Success 00/00/analysis-wf_ID0000001/analysis-wf-0.dag
                0     0     0     0     0     3     0 100.0 Success 00/00/sleep-wf_ID0000002/sleep-wf-0.dag
                1     2     3     4     5     6     7 100.0 Success *wf-name-0.dag
                0     0     0     0     0    20     0 100.0         TOTALS (20 jobs)
                Summary: 3 DAGs total (Success:3)
                """
                ),
                {
                    "totals": {
                        "unready": 1,
                        "ready": 2,
                        "pre": 3,
                        "queued": 4,
                        "post": 5,
                        "succeeded": 6,
                        "failed": 7,
                        "percent_done": 100.0,
                        "total": 28,
                    },
                    "dags": {
                        "root": {
                            "unready": 1,
                            "ready": 2,
                            "pre": 3,
                            "queued": 4,
                            "post": 5,
                            "succeeded": 6,
                            "failed": 7,
                            "percent_done": 100.0,
                            "state": "Success",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
            ),
            (
                "get status should return None because this is invalid/unexpected status output",
                None,
            ),
        ],
    )
    def test__parse_status_output(self, pegasus_status_out, expected_dict):
        assert (
            Client._parse_status_output(pegasus_status_out, "wf-name") == expected_dict
        )

    @pytest.mark.parametrize(
        "status_output_str, expected_dict",
        [
            (
                dedent(
                    """
                (no matching jobs found in Condor Q)
                UNRDY READY   PRE  IN_Q  POST  DONE   FAIL   %DONE STATE   DAGNAME
                    1     2     3     4     5  1,000  1,000  100.0 Success *wf-name-0.dag
                Summary: 1 DAG total (Success:1)
                """
                ).encode("utf-8"),
                {
                    "totals": {
                        "unready": 1,
                        "ready": 2,
                        "pre": 3,
                        "queued": 4,
                        "post": 5,
                        "succeeded": 1000,
                        "failed": 1000,
                        "percent_done": 100.0,
                        "total": 2015,
                    },
                    "dags": {
                        "root": {
                            "unready": 1,
                            "ready": 2,
                            "pre": 3,
                            "queued": 4,
                            "post": 5,
                            "succeeded": 1000,
                            "failed": 1000,
                            "percent_done": 100.0,
                            "state": "Success",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
            ),
            (
                b"get status should return None because this is invalid/unexpected status output",
                None,
            ),
        ],
    )
    def test_get_status(self, mocker, client, status_output_str, expected_dict):
        '''mocker.patch(
            "Pegasus.client._client.Client._exec",
            return_value=Result(
                cmd=["/path/bin/pegasus-status", "--long", "submit_dir"],
                exit_code=0,
                stdout_bytes=status_output_str,
                stderr_bytes=b"",
            ),
        )'''
        mocker.patch("Pegasus.client.status.Status.fetch_status", return_value=expected_dict)
        assert client.get_status("wf-name", "submit_dir") == expected_dict
        Pegasus.client.status.Status.fetch_status.assert_called_once_with("submit_dir", json=True)

    @pytest.mark.parametrize(
        "status_output, expected_progress_bar",
        [
            (
                {
                    "totals": {
                        "unready": 0,
                        "ready": 0,
                        "pre": 0,
                        "queued": 0,
                        "post": 0,
                        "succeeded": 1000,
                        "failed": 1000,
                        "percent_done": 100.0,
                        "total": 2000,
                    },
                    "dags": {
                        "root": {
                            "unready": 0,
                            "ready": 0,
                            "pre": 0,
                            "queued": 0,
                            "post": 0,
                            "succeeded": 1000,
                            "failed": 1000,
                            "percent_done": 100.0,
                            "state": "Success",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
                "\r[\x1b[1;32m#########################\x1b[0m] 100.0% ..Success (\x1b[1;34mUnready: 0\x1b[0m, \x1b[1;32mCompleted: 1000\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 1000\x1b[0m)\n",
            ),
            (
                {
                    "totals": {
                        "unready": 4,
                        "ready": 0,
                        "pre": 0,
                        "queued": 0,
                        "post": 0,
                        "succeeded": 3,
                        "failed": 1,
                        "percent_done": 37.5,
                        "total": 8,
                    },
                    "dags": {
                        "root": {
                            "unready": 4,
                            "ready": 0,
                            "pre": 0,
                            "queued": 0,
                            "post": 0,
                            "succeeded": 3,
                            "failed": 1,
                            "percent_done": 37.5,
                            "state": "Failure",
                            "dagname": "wf-name-0.dag",
                        }
                    },
                },
                "\r[\x1b[1;32m#########\x1b[0m----------------]  37.5% ..Failure (\x1b[1;34mUnready: 4\x1b[0m, \x1b[1;32mCompleted: 3\x1b[0m, \x1b[1;33mQueued: 0\x1b[0m, \x1b[1;36mRunning: 0\x1b[0m, \x1b[1;31mFailed: 1\x1b[0m)\n",
            ),
        ],
    )
    def test_wait(self, mocker, capsys, client, status_output, expected_progress_bar):
        mocker.patch(
            "Pegasus.client._client.Client.get_status", return_value=status_output
        )

        client.wait(root_wf_name="wf-name", submit_dir="submit_dir")

        Pegasus.client._client.Client.get_status.assert_called_once_with(
            root_wf_name="wf-name", submit_dir="submit_dir"
        )

        out, _ = capsys.readouterr()
        assert out == expected_progress_bar

    def test_remove(self, mock_subprocess, client):
        client.remove("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-remove", "-vvv", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_analyzer(self, mock_subprocess, client):
        client.analyzer("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-analyzer", "-vvv", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_statistics(self, mock_subprocess, client):
        client.statistics("submit_dir", verbose=3)
        subprocess.Popen.assert_called_once_with(
            ["/path/bin/pegasus-statistics", "-vvv", "submit_dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test__exec(self, mock_subprocess, client):
        client._exec("ls")
        with pytest.raises(ValueError) as e:
            client._exec(None)

        assert str(e.value) == "cmd is required"


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
