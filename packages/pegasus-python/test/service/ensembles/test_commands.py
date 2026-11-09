import json
import os
import socket
from unittest.mock import MagicMock, patch

import pytest
import requests

import Pegasus
from Pegasus.db.ensembles import TriggerType
from Pegasus.service import _config
from Pegasus.service.ensembles import commands
from Pegasus.service.ensembles.commands import (
    EM_PORT_MAX,
    EM_PORT_MIN,
    CronTriggerCommand,
    EnsemblesCommand,
    FilePatternTriggerCommand,
    ListTriggersCommand,
    ServerCommand,
    _compute_em_port,
    emapp,
    resolve_client_port,
    resolve_server_port,
)


class TestEMPort:
    @pytest.mark.parametrize("uid", [0, 1000, 57616, 100_000_000])
    def test_em_port_is_within_valid_range(self, uid):
        port = _compute_em_port(uid)
        assert EM_PORT_MIN <= port < EM_PORT_MAX


@pytest.fixture
def pegasus_dir(tmp_path, monkeypatch):
    """Point ~/.pegasus at a temporary directory."""
    d = tmp_path / "dot-pegasus"
    d.mkdir()
    monkeypatch.setattr(_config, "DEFAULT_PEGASUS_DIR", str(d))
    return d


@pytest.fixture
def em_config(mocker):
    """Isolate emapp.config changes made by a test."""
    mocker.patch.dict(emapp.config, {})
    emapp.config.pop("EM_PORT", None)
    return emapp.config


class TestPortResolution:
    def test_server_default_is_uid_derived(self, em_config):
        assert resolve_server_port() == _compute_em_port(os.getuid())

    def test_server_config(self, em_config):
        em_config["EM_PORT"] = 18124
        assert resolve_server_port() == 18124

    def test_server_cli_overrides_config(self, em_config):
        em_config["EM_PORT"] = 18124
        assert resolve_server_port(18125) == 18125

    def test_server_invalid_cli(self, em_config):
        with pytest.raises(ValueError, match="--port"):
            resolve_server_port(80)

    def test_server_invalid_config(self, em_config):
        em_config["EM_PORT"] = "nope"
        with pytest.raises(ValueError, match="EM_PORT in ~/.pegasus/service.json"):
            resolve_server_port()

    def test_client_default_is_uid_derived(self, em_config):
        assert resolve_client_port(None) == _compute_em_port(os.getuid())

    def test_client_config(self, em_config):
        em_config["EM_PORT"] = 18124
        assert resolve_client_port(None) == 18124

    def test_client_runtime_overrides_config(self, em_config):
        em_config["EM_PORT"] = 18124
        assert resolve_client_port({"port": 18126}) == 18126

    def test_client_invalid_runtime_falls_back(self, em_config, pegasus_dir):
        em_config["EM_PORT"] = 18124
        assert resolve_client_port({"port": "bad"}) == 18124


class TestClientCredentials:
    def test_token_auth(self, em_config, pegasus_dir):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        _config.write_em_runtime(18127, "secret")

        cmd = EnsemblesCommand()
        cmd.connect()
        assert cmd.endpoint == "http://127.0.0.1:18127/"
        assert cmd.auth[1] == "secret"

    def test_token_auth_not_running(self, em_config, pegasus_dir):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        with pytest.raises(commands.EMClientError, match="not running"):
            EnsemblesCommand().connect()

    def test_construct_without_server(self, em_config, pegasus_dir):
        # --help / argument parsing must work without a running server
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        cmd = EnsemblesCommand()
        assert cmd.endpoint is None

    def test_request_without_server(self, em_config, pegasus_dir, mocker, capsys):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        request = mocker.patch("requests.request")

        with pytest.raises(SystemExit):
            EnsemblesCommand().get("/ensembles")
        assert "ERROR: Ensemble manager is not running" in capsys.readouterr().out
        request.assert_not_called()

    def test_request_invalid_config_port(self, em_config, pegasus_dir, capsys):
        em_config.update(AUTHENTICATION="NoAuthentication", EM_PORT=80)

        with pytest.raises(SystemExit):
            EnsemblesCommand().get("/ensembles")
        assert "ERROR: Invalid EM_PORT" in capsys.readouterr().out

    def test_token_auth_null_token(self, em_config, pegasus_dir):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        _config.write_em_runtime(18127, None)
        with pytest.raises(commands.EMClientError, match="not running"):
            EnsemblesCommand().connect()

    def test_no_auth(self, em_config, pegasus_dir):
        em_config.update(AUTHENTICATION="NoAuthentication", USERNAME="", PASSWORD="")
        _config.write_em_runtime(18127, None)

        cmd = EnsemblesCommand()
        cmd.connect()
        assert cmd.auth is None
        assert cmd.port == 18127

    def test_pam_auth(self, em_config, pegasus_dir):
        em_config.update(AUTHENTICATION="PAMAuthentication", USERNAME="u", PASSWORD="p")
        cmd = EnsemblesCommand()
        cmd.connect()
        assert cmd.auth == ("u", "p")

    @pytest.mark.parametrize(
        "username,password,missing",
        [("", "p", "USERNAME"), ("u", "", "PASSWORD")],
    )
    def test_pam_auth_missing(
        self, em_config, pegasus_dir, username, password, missing
    ):
        em_config.update(
            AUTHENTICATION="PAMAuthentication", USERNAME=username, PASSWORD=password
        )
        with pytest.raises(commands.EMClientError, match=f"Specify {missing} in"):
            EnsemblesCommand().connect()

    def test_request_sends_auth(self, em_config, pegasus_dir, mocker):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        _config.write_em_runtime(18127, "secret")
        response = MagicMock(status_code=200)
        request = mocker.patch("requests.request", return_value=response)

        cmd = EnsemblesCommand()
        assert cmd.get("/ensembles") is response
        assert request.call_args.kwargs["auth"][1] == "secret"
        assert request.call_args.args[1] == "http://127.0.0.1:18127/ensembles"

    def test_request_401_hint(self, em_config, pegasus_dir, mocker, capsys):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        _config.write_em_runtime(18127, "secret")
        mocker.patch("requests.request", return_value=MagicMock(status_code=401))

        with pytest.raises(SystemExit):
            EnsemblesCommand().get("/ensembles")
        assert "rejected the access token" in capsys.readouterr().out

    def test_request_connection_error(self, em_config, pegasus_dir, mocker, capsys):
        em_config["AUTHENTICATION"] = "LocalTokenAuthentication"
        _config.write_em_runtime(18127, "secret")
        mocker.patch(
            "requests.request", side_effect=requests.exceptions.ConnectionError()
        )

        with pytest.raises(SystemExit):
            EnsemblesCommand().get("/ensembles")
        assert "port 18127" in capsys.readouterr().out


class TestServerCommand:
    @pytest.fixture
    def server(self, em_config, pegasus_dir, mocker, monkeypatch):
        monkeypatch.delenv("WERKZEUG_RUN_MAIN", raising=False)
        em_config.update(AUTHENTICATION="LocalTokenAuthentication", DEBUG=False)
        mocker.patch("os.getuid", return_value=1000)
        mocker.patch.object(
            commands.manager,
            "check_environment",
            side_effect=commands.manager.EMError("no pegasus"),
        )
        mocker.patch.object(commands, "_port_available", return_value=True)

        seen = {}

        def fake_run(port, host):
            seen.update(port=port, host=host, runtime=_config.read_em_runtime())

        mocker.patch.object(emapp, "run", side_effect=fake_run)
        return seen

    def _run(self, *args):
        cmd = ServerCommand()
        cmd.parse(list(args))
        cmd.run()

    def test_parses_port(self):
        cmd = ServerCommand()
        cmd.parse(["--port", "18128"])
        assert cmd.options.port == 18128
        cmd.parse(["-p", "18129"])
        assert cmd.options.port == 18129

    def test_publishes_and_removes_runtime(self, server, em_config):
        self._run("--port", "18128")

        assert server["port"] == 18128
        assert server["host"] == "127.0.0.1"
        runtime = server["runtime"]
        assert runtime["port"] == 18128
        assert runtime["pid"] == os.getpid()
        assert runtime["token"] and runtime["token"] == em_config["EM_TOKEN"]
        # cleaned up on exit
        assert _config.read_em_runtime() is None

    def test_port_from_config(self, server, em_config):
        em_config["EM_PORT"] = 18130
        self._run()
        assert server["port"] == 18130

    def test_no_token_without_token_auth(self, server, em_config):
        em_config["AUTHENTICATION"] = "NoAuthentication"
        self._run("--port", "18128")
        assert server["runtime"]["token"] is None
        assert em_config["EM_TOKEN"] is None

    def test_new_token_each_start(self, server, em_config):
        self._run("--port", "18128")
        first = server["runtime"]["token"]
        self._run("--port", "18128")
        assert server["runtime"]["token"] != first

    def test_invalid_port(self, server):
        with pytest.raises(SystemExit):
            self._run("--port", "80")
        assert "port" not in server

    def test_port_in_use(self, server, mocker, caplog):
        mocker.patch.object(commands, "_port_available", return_value=False)
        with pytest.raises(SystemExit):
            self._run("--port", "18128")
        assert "Port 18128 is in use" in caplog.text
        assert _config.read_em_runtime() is None

    def test_already_running(self, server, mocker, caplog):
        _config.write_em_runtime(18131, "other", pid=os.getpid() + 1)
        mocker.patch.object(commands, "is_process_alive", return_value=True)

        with pytest.raises(SystemExit):
            self._run("--port", "18128")
        assert "already running" in caplog.text
        # the running server's runtime file is left untouched
        assert _config.read_em_runtime()["token"] == "other"

    def test_stale_runtime_is_replaced(self, server, mocker):
        _config.write_em_runtime(18131, "stale", pid=os.getpid() + 1)
        mocker.patch.object(commands, "is_process_alive", return_value=False)

        self._run("--port", "18128")
        assert server["runtime"]["token"] != "stale"

    def test_reloader_child_reuses_token(self, server, em_config, monkeypatch):
        _config.write_em_runtime(18128, "parent-token", pid=os.getpid() + 1)
        monkeypatch.setenv("WERKZEUG_RUN_MAIN", "true")

        self._run("--port", "18128")
        assert em_config["EM_TOKEN"] == "parent-token"
        # the child does not remove the parent's runtime file
        assert _config.read_em_runtime()["token"] == "parent-token"

    def test_refuses_root(self, server, mocker):
        mocker.patch("os.getuid", return_value=0)
        with pytest.raises(SystemExit):
            self._run("--port", "18128")
        assert _config.read_em_runtime() is None


def test_port_available():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        assert commands._port_available("127.0.0.1", port) is False


class TestCronTriggerCommand:
    @pytest.mark.parametrize(
        "args,expected_request_data",
        [
            (
                ["ensemble", "trigger", "10s", "/workflow.py"],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": None,
                    "type": TriggerType.CRON.value,
                },
            ),
            (
                ["ensemble", "trigger", "10s", "/workflow.py", "--timeout", "20s"],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
            ),
            (
                [
                    "ensemble",
                    "trigger",
                    "10s",
                    "/workflow.py",
                    "--timeout",
                    "20s",
                    "--args",
                    "arg1 arg2 --option1 --option2 x",
                ],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps(
                        ["arg1", "arg2", "--option1", "--option2", "x"]
                    ),
                    "interval": "10s",
                    "timeout": "20s",
                    "type": TriggerType.CRON.value,
                },
            ),
        ],
    )
    def test_run_cron_trigger_command(self, mocker, args, expected_request_data):

        mocker.patch("Pegasus.service.ensembles.commands.CronTriggerCommand.post")
        # need to patch EnsembleClientCommand so that checks in the constructor don't
        # cause the test to fail
        with patch("Pegasus.service.ensembles.commands.EnsembleClientCommand"):
            cmd = CronTriggerCommand()
            cmd.parse(args)
            cmd.run()
        Pegasus.service.ensembles.commands.CronTriggerCommand.post.assert_called_once_with(
            "/ensembles/ensemble/triggers/cron", data=expected_request_data
        )


class TestFilePatternTriggerCommand:
    @pytest.mark.parametrize(
        "args,expected_request_data",
        [
            (
                ["ensemble", "trigger", "10s", "/workflow.py", "/*.txt", "/*.png"],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "file_patterns": json.dumps(["/*.txt", "/*.png"]),
                    "timeout": None,
                    "type": TriggerType.FILE_PATTERN.value,
                },
            ),
            (
                [
                    "ensemble",
                    "trigger",
                    "10s",
                    "/workflow.py",
                    "/*.txt",
                    "/*.png",
                    "--timeout",
                    "20s",
                ],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps([]),
                    "interval": "10s",
                    "file_patterns": json.dumps(["/*.txt", "/*.png"]),
                    "timeout": "20s",
                    "type": TriggerType.FILE_PATTERN.value,
                },
            ),
            (
                [
                    "ensemble",
                    "trigger",
                    "10s",
                    "/workflow.py",
                    "/*.txt",
                    "/*.png",
                    "--timeout",
                    "20s",
                    "--args",
                    "arg1 arg2 --option1 --option2 x",
                ],
                {
                    "trigger": "trigger",
                    "workflow_script": "/workflow.py",
                    "workflow_args": json.dumps(
                        ["arg1", "arg2", "--option1", "--option2", "x"]
                    ),
                    "interval": "10s",
                    "file_patterns": json.dumps(["/*.txt", "/*.png"]),
                    "timeout": "20s",
                    "type": TriggerType.FILE_PATTERN.value,
                },
            ),
        ],
    )
    def test_run_file_pattern_trigger_command(
        self, mocker, args, expected_request_data
    ):
        mocker.patch(
            "Pegasus.service.ensembles.commands.FilePatternTriggerCommand.post"
        )
        # need to patch EnsembleClientCommand so that checks in the constructor don't
        # cause the test to fail
        with patch("Pegasus.service.ensembles.commands.EnsembleClientCommand"):
            cmd = FilePatternTriggerCommand()
            cmd.parse(args)
            cmd.run()
        Pegasus.service.ensembles.commands.FilePatternTriggerCommand.post.assert_called_once_with(
            "/ensembles/ensemble/triggers/file_pattern", data=expected_request_data
        )


class TestListTriggersCommand:
    @pytest.fixture(autouse=True)
    def configure_credentials(self, mocker):
        # ListTriggersCommand relies on EnsembleClientCommand's real __init__ to
        # build self.parser (optparse), so unlike the argparse-based trigger
        # creation commands, EnsembleClientCommand itself can't be mocked out here.
        mocker.patch.dict(
            emapp.config,
            {
                "AUTHENTICATION": "PAMAuthentication",
                "USERNAME": "user",
                "PASSWORD": "pass",
            },
        )
        mocker.patch(
            "Pegasus.service.ensembles.commands.read_em_runtime", return_value=None
        )

    def _mock_get(self, mocker, triggers):
        response = MagicMock()
        response.json.return_value = triggers
        return mocker.patch(
            "Pegasus.service.ensembles.commands.ListTriggersCommand.get",
            return_value=response,
        )

    def test_short_format(self, mocker, capsys):
        get = self._mock_get(
            mocker,
            [
                {
                    "name": "trigger1",
                    "type": TriggerType.CRON.value,
                    "state": "RUNNING",
                    "workflow": {"script": "/workflow.py", "args": []},
                    "args": {"interval": 10, "timeout": None},
                }
            ],
        )

        cmd = ListTriggersCommand()
        cmd.parse(["ensemble"])
        cmd.run()

        get.assert_called_once_with("/ensembles/ensemble/triggers")
        out = capsys.readouterr().out
        assert "trigger1" in out
        assert "RUNNING" in out
        assert "/workflow.py" in out

    def test_long_format(self, mocker, capsys):
        self._mock_get(
            mocker,
            [
                {
                    "name": "trigger1",
                    "type": TriggerType.CRON.value,
                    "state": "RUNNING",
                    "workflow": {"script": "/workflow.py", "args": ["arg1"]},
                    "args": {"interval": 10, "timeout": 20},
                }
            ],
        )

        cmd = ListTriggersCommand()
        cmd.parse(["ensemble", "-l"])
        cmd.run()

        out = capsys.readouterr().out
        assert "trigger1" in out
        assert "/workflow.py" in out
        assert "arg1" in out
        assert "10" in out
        assert "20" in out

    def test_long_format_with_null_workflow_args_and_args(self, mocker, capsys):
        self._mock_get(
            mocker,
            [
                {
                    "name": "trigger1",
                    "type": TriggerType.CRON.value,
                    "state": "READY",
                    "workflow": {"script": "/workflow.py", "args": None},
                    "args": None,
                }
            ],
        )

        cmd = ListTriggersCommand()
        cmd.parse(["ensemble", "-l"])
        cmd.run()

        out = capsys.readouterr().out
        assert "trigger1" in out
        assert "/workflow.py" in out

    def test_empty_result_prints_nothing(self, mocker, capsys):
        self._mock_get(mocker, [])

        cmd = ListTriggersCommand()
        cmd.parse(["ensemble"])
        cmd.run()

        assert capsys.readouterr().out == ""

    def test_requires_ensemble_argument(self):
        cmd = ListTriggersCommand()
        cmd.parse([])
        with pytest.raises(SystemExit):
            cmd.run()
