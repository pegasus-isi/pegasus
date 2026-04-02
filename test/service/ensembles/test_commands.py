import json
from unittest.mock import patch

import pytest

import Pegasus
from Pegasus.db.ensembles import TriggerType
from Pegasus.service.ensembles.commands import (
    CronTriggerCommand,
    FilePatternTriggerCommand,
)


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
        with patch(
            "Pegasus.service.ensembles.commands.EnsembleClientCommand"
        ) as MockEnsembleClientCommand:
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
        with patch(
            "Pegasus.service.ensembles.commands.EnsembleClientCommand"
        ) as MockEnsembleClientCommand:
            cmd = FilePatternTriggerCommand()
            cmd.parse(args)
            cmd.run()
        Pegasus.service.ensembles.commands.FilePatternTriggerCommand.post.assert_called_once_with(
            "/ensembles/ensemble/triggers/file_pattern", data=expected_request_data
        )
