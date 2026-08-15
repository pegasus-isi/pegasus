import json
from unittest.mock import MagicMock, patch

import pytest

import Pegasus
from Pegasus.db.ensembles import TriggerType
from Pegasus.service.ensembles.commands import (
    EM_PORT_MAX,
    EM_PORT_MIN,
    CronTriggerCommand,
    FilePatternTriggerCommand,
    ListTriggersCommand,
    _compute_em_port,
    emapp,
)


class TestEMPort:
    @pytest.mark.parametrize("uid", [0, 1000, 57616, 100_000_000])
    def test_em_port_is_within_valid_range(self, uid):
        port = _compute_em_port(uid)
        assert EM_PORT_MIN <= port < EM_PORT_MAX


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
        mocker.patch.dict(emapp.config, {"USERNAME": "user", "PASSWORD": "pass"})

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
