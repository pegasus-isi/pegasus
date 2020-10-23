import logging
import subprocess
import threading
import time
from subprocess import CompletedProcess

import pytest

import Pegasus
import Pegasus.db.schema
from Pegasus.db.ensembles import Ensembles, Triggers
from Pegasus.service.ensembles.trigger import (
    CronTrigger,
    FilePatternTrigger,
    TriggerManager,
    TriggerThread,
)


class TestTriggerManager:
    def test_run_and_start_trigger(self, mocker):
        mocker.patch("Pegasus.db.connection.connect", return_value=None)
        mocker.patch("Pegasus.service.ensembles.trigger.TriggerManager.start_trigger")
        mocker.patch(
            "Pegasus.db.ensembles.Triggers.list_triggers",
            return_value=[
                Pegasus.db.schema.Trigger(
                    _id=1,
                    ensemble_id=1,
                    name="test-trigger",
                    state="READY",
                    workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                    args=r'{"timeout":100, "interval":20}',
                    _type="CRON",
                )
            ],
        )

        mgr = TriggerManager()

        try:
            mgr.run()
        except AttributeError as e:
            assert "has no attribute 'close'" in str(e)

        Pegasus.service.ensembles.trigger.TriggerManager.start_trigger.assert_called_once()

    def test_run_and_restart_trigger(self, mocker, caplog):
        caplog.set_level(logging.DEBUG)
        mocker.patch("Pegasus.db.connection.connect", return_value=None)
        mocker.patch("Pegasus.service.ensembles.trigger.TriggerManager.start_trigger")
        mocker.patch(
            "Pegasus.db.ensembles.Triggers.list_triggers",
            return_value=[
                Pegasus.db.schema.Trigger(
                    _id=1,
                    ensemble_id=1,
                    name="test-trigger",
                    state="RUNNING",
                    workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                    args=r'{"timeout":100, "interval":20}',
                    _type="CRON",
                )
            ],
        )

        mgr = TriggerManager()

        try:
            mgr.run()
        except AttributeError as e:
            assert "has no attribute 'close'" in str(e)

        Pegasus.service.ensembles.trigger.TriggerManager.start_trigger.assert_called_once()

        if "(1, 'test-trigger') not in memory, restarting" not in caplog.text:
            pytest.fail(
                "TriggerManager.run did not get to the expected restart if statement..."
            )

    def test_run_and_cleanup_trigger(self, mocker, caplog):
        caplog.set_level(logging.DEBUG)
        mocker.patch("Pegasus.db.connection.connect", return_value=None)
        mocker.patch("Pegasus.service.ensembles.trigger.TriggerManager.stop_trigger")
        mocker.patch(
            "Pegasus.db.ensembles.Triggers.list_triggers",
            return_value=[
                Pegasus.db.schema.Trigger(
                    _id=1,
                    ensemble_id=1,
                    name="test-trigger",
                    state="RUNNING",
                    workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                    args=r'{"timeout":100, "interval":20}',
                    _type="CRON",
                )
            ],
        )

        mgr = TriggerManager()

        # give triggermanager a thread object where t.is_alive() will return False
        mgr.running[(1, "test-trigger")] = threading.Thread(name="test")

        try:
            mgr.run()
        except AttributeError as e:
            assert "has no attribute 'close'" in str(e)

        Pegasus.service.ensembles.trigger.TriggerManager.stop_trigger.assert_called_once()

        if "(1, 'test-trigger') exited, removing references" not in caplog.text:
            pytest.fail(
                "TriggerManager.run did not get to the expected stop trigger if statement..."
            )

    def test_run_and_stop_trigger(self, mocker, caplog):
        caplog.set_level(logging.DEBUG)
        mocker.patch("Pegasus.db.connection.connect", return_value=None)
        mocker.patch("Pegasus.service.ensembles.trigger.TriggerManager.stop_trigger")
        mocker.patch(
            "Pegasus.db.ensembles.Triggers.list_triggers",
            return_value=[
                Pegasus.db.schema.Trigger(
                    _id=1,
                    ensemble_id=1,
                    name="test-trigger",
                    state="STOPPED",
                    workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                    args=r'{"timeout":100, "interval":20}',
                    _type="CRON",
                )
            ],
        )

        mgr = TriggerManager()

        try:
            mgr.run()
        except AttributeError as e:
            assert "has no attribute 'close'" in str(e)

        Pegasus.service.ensembles.trigger.TriggerManager.stop_trigger.assert_called_once()

    def test_start_cron_trigger(self, mocker):
        mocker.patch(
            "Pegasus.service.ensembles.trigger.CronTrigger",
            return_value=threading.Thread(name="test"),
        )
        mocker.patch("Pegasus.db.ensembles.Triggers.update_state")
        mocker.patch(
            "Pegasus.db.ensembles.Ensembles.get_ensemble_name", return_value="test-ens"
        )
        mocker.patch("threading.Thread.start")

        mgr = TriggerManager()
        mgr.trigger_dao = Triggers(None)
        mgr.ensemble_dao = Ensembles(None)
        mgr.start_trigger(
            Pegasus.db.schema.Trigger(
                _id=1,
                ensemble_id=1,
                name="test-trigger",
                state="READY",
                workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                args=r'{"timeout":100, "interval":20}',
                _type="CRON",
            )
        )

        Pegasus.service.ensembles.trigger.CronTrigger.assert_called_once_with(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trigger",
            workflow_script="/wf.py",
            workflow_args=["arg1"],
            timeout=100,
            interval=20,
        )

        threading.Thread.start.assert_called_once_with()
        Pegasus.db.ensembles.Triggers.update_state.assert_called_once_with(
            ensemble_id=1, trigger_id=1, new_state="RUNNING"
        )

    def test_start_file_pattern_trigger(self, mocker):
        mocker.patch(
            "Pegasus.service.ensembles.trigger.FilePatternTrigger",
            return_value=threading.Thread(name="test"),
        )
        mocker.patch("Pegasus.db.ensembles.Triggers.update_state")
        mocker.patch(
            "Pegasus.db.ensembles.Ensembles.get_ensemble_name", return_value="test-ens"
        )
        mocker.patch("threading.Thread.start")

        mgr = TriggerManager()
        mgr.trigger_dao = Triggers(None)
        mgr.ensemble_dao = Ensembles(None)
        mgr.start_trigger(
            Pegasus.db.schema.Trigger(
                _id=1,
                ensemble_id=1,
                name="test-trigger",
                state="READY",
                workflow=r'{"script":"/wf.py", "args":["arg1"]}',
                args=r'{"timeout":100, "interval":20, "file_patterns":["/*.txt"]}',
                _type="FILE_PATTERN",
            )
        )

        Pegasus.service.ensembles.trigger.FilePatternTrigger.assert_called_once_with(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trigger",
            workflow_script="/wf.py",
            workflow_args=["arg1"],
            timeout=100,
            interval=20,
            file_patterns=["/*.txt"],
        )

        threading.Thread.start.assert_called_once_with()
        Pegasus.db.ensembles.Triggers.update_state.assert_called_once_with(
            ensemble_id=1, trigger_id=1, new_state="RUNNING"
        )

    def test_stop_trigger(self, mocker):
        mocker.patch("Pegasus.service.ensembles.trigger.TriggerThread.shutdown")
        mocker.patch("Pegasus.db.ensembles.Triggers.delete_trigger")

        # setup TriggerManager state
        mgr = TriggerManager()
        mgr.trigger_dao = Triggers(None)
        mgr.running[(1, "test-trigger")] = TriggerThread(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trigger",
            workflow_script="/wf.py",
        )

        # invoke stop trigger
        mgr.stop_trigger(
            Pegasus.db.schema.Trigger(
                _id=1,
                ensemble_id=1,
                name="test-trigger",
                state="RUNNING",
                workflow="json string",
                args=None,
                _type="CRON",
            )
        )

        # ensure shtudown was called on the target trigger
        Pegasus.service.ensembles.trigger.TriggerThread.shutdown.assert_called_once_with()

        # ensure the target trigger was removed from the database
        Pegasus.db.ensembles.Triggers.delete_trigger.assert_called_once_with(
            1, "test-trigger"
        )

    def test_get_tname(self):
        trigger = Pegasus.db.schema.Trigger(
            _id=1,
            ensemble_id=1,
            name="test-trigger",
            state="RUNNING",
            workflow="/wf.py",
            args=None,
            _type="CRON",
        )

        expected = (1, "test-trigger")
        result = TriggerManager.get_tname(trigger)

        assert result == expected


class TestTriggerThread:
    class DerivedTriggerThread(TriggerThread):
        def __init__(
            self,
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trigger",
            workflow_script="/wf.py",
            workflow_args=["arg1"],
        ):
            TriggerThread.__init__(
                self,
                ensemble_id=ensemble_id,
                ensemble=ensemble,
                trigger=trigger,
                workflow_script=workflow_script,
                workflow_args=workflow_args,
            )

        def run(self):
            while not self.stop_event.isSet():
                time.sleep(1)

    def test_constructor(self):
        trigger = TestTriggerThread.DerivedTriggerThread()

        assert trigger.name == "(1, 'test-trigger')"
        assert trigger.workflow_cmd == ["/wf.py", "arg1"]

    def test_shutdown(self):
        trigger = TestTriggerThread.DerivedTriggerThread()

        # start trigger thread
        trigger.start()

        # tell trigger to gracefully stop
        trigger.shutdown()

        # shutdown doesn't work if this waits forever
        trigger.join()


class TestCronTrigger:
    def test_run(self, mocker, caplog):
        # force return code of subprocess.run to be 1 so that TestFilePatternTrigger
        # main loop can exit
        mocker.patch(
            "subprocess.run",
            return_value=CompletedProcess(
                None, returncode=1, stdout=b"out", stderr=b"error we expected to get"
            ),
        )

        cron_trigger = CronTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval="10",
            workflow_script="/wf.py",
            workflow_args=["arg1", "arg2"],
        )

        # run main loop of trigger
        cron_trigger.run()

        # ensure epgasus-em submit command was properly built up
        # ensure pegasus-em submit command was properly built up
        args = subprocess.run.call_args[0][0]
        assert args[0:2] == ["pegasus-em", "submit"]
        assert "test-trgr" in args[2]
        assert args[3:] == ["/wf.py", "arg1", "arg2"]

        # ensure that the exception caught in the main loop of run is what we
        # expected for testing and not an actual error
        assert "error we expected to get" in caplog.text

    def test_timeout(self, mocker):
        # mock execution of pegasus-em submit command
        mocker.patch(
            "subprocess.run",
            return_value=CompletedProcess(
                None, returncode=0, stdout=b"out", stderr=b"err",
            ),
        )

        cron_trigger = CronTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval=1,
            timeout=2,
            workflow_script="/wf.py",
        )

        # main loop of trigger should run for two seconds, then exit
        cron_trigger.run()

        assert cron_trigger.elapsed == 2


class TestFilePatternTrigger:
    def test_run(self, mocker, tmp_path, caplog):
        # force return code of subprocess.run to be 1 so that TestFilePatternTrigger
        # main loop can exit
        mocker.patch(
            "subprocess.run",
            return_value=CompletedProcess(
                None, returncode=1, stdout=b"", stderr=b"error we expected to get"
            ),
        )

        # setup input dirs
        input_file = tmp_path / "input_file.txt"
        input_file.touch()

        # create fp trigger so we can use collect_and_move_files()
        fp_trigger = FilePatternTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval="10",
            workflow_script="/wf.py",
            workflow_args=["arg1", "arg2"],
            file_patterns=[str(tmp_path.resolve() / "*.txt")],
        )

        # run main loop of trigger
        fp_trigger.run()

        # ensure pegasus-em submit command was properly built up
        args = subprocess.run.call_args[0][0]
        assert args[0:2] == ["pegasus-em", "submit"]
        assert "test-trgr" in args[2]
        assert args[3:] == [
            "/wf.py",
            "arg1",
            "arg2",
            "--inputs",
            str(tmp_path.resolve() / "processed/input_file.txt"),
        ]

        # ensure that the exception caught in the main loop of run is what we
        # expected for testing and not an actual error
        assert "error we expected to get" in caplog.text

    def test_timeout(self, tmp_path):
        fp_trigger = FilePatternTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval=1,
            timeout=2,
            workflow_script="/wf.py",
            file_patterns=[str(tmp_path.resolve() / "file_that_doesnt_exist")],
        )

        # main loop of trigger should run for two seconds, then exit
        fp_trigger.run()

        assert fp_trigger.elapsed == 2

    def test_collect_and_move_files(self, tmp_path):
        # setup input dirs
        inputs_1 = tmp_path / "inputs_1"
        inputs_1.mkdir()

        f_1 = inputs_1 / "f1.txt"

        # this file is not expected to move
        f_2 = inputs_1 / "f2.jpg"

        inputs_2 = tmp_path / "inputs_2"
        inputs_2.mkdir()

        f_3 = inputs_2 / "f3.csv"

        # this file is not expected to move
        f_4 = inputs_2 / "f4.png"

        for f in [f_1, f_2, f_3, f_4]:
            f.touch()

        # create fp trigger so we can use collect_and_move_files()
        fp_trigger = FilePatternTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval="10",
            workflow_script="/wf.py",
            workflow_args=None,
            file_patterns=[
                str(inputs_1.resolve() / "*.txt"),
                str(inputs_2.resolve() / "*.csv"),
            ],
        )

        # call collect and move files func
        collected = set(fp_trigger.collect_and_move_files())

        # check that the correct files have been collected
        expected_dir_1 = inputs_1.resolve() / "processed"
        expected_dir_2 = inputs_2.resolve() / "processed"

        assert collected == {
            str(expected_dir_1 / "f1.txt"),
            str(expected_dir_2 / "f3.csv"),
        }

        # check that "processed" subdirs have been created
        assert expected_dir_1.is_dir()
        assert expected_dir_2.is_dir()

        # check that files have been moved
        assert not f_1.exists()
        assert not f_3.exists()
        assert (expected_dir_1 / "f1.txt").exists()
        assert (expected_dir_2 / "f3.csv").exists()

    def test_collect_and_move_files_multiple_times(self, tmp_path):
        # create fp trigger so we can use collect_and_move_files()
        fp_trigger = FilePatternTrigger(
            ensemble_id=1,
            ensemble="test-ens",
            trigger="test-trgr",
            interval="10",
            workflow_script="/wf.py",
            workflow_args=None,
            file_patterns=[str(tmp_path.resolve() / "*.txt")],
        )

        # add first file to input dir
        f_1 = tmp_path / "f1.txt"
        f_1.touch()

        # collect a single file
        collected = set(fp_trigger.collect_and_move_files())

        # check that f_1 moved and collected
        assert collected == {str(tmp_path.resolve() / "processed/f1.txt")}
        assert (tmp_path.resolve() / "processed/f1.txt").is_file()
        assert not f_1.exists()

        # add second file to input dir
        f_2 = tmp_path / "f2.txt"
        f_2.touch()

        # collect a single file again
        collected = set(fp_trigger.collect_and_move_files())

        # check that f_2 moved and collected
        assert collected == {str(tmp_path.resolve() / "processed/f2.txt")}
        assert (tmp_path.resolve() / "processed/f2.txt").is_file()
        assert not f_2.exists()
