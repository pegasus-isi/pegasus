import subprocess
from subprocess import CompletedProcess

from Pegasus.service.ensembles.trigger import FilePatternTrigger


class TestTriggerManager:
    pass


class TestTriggerThread:
    pass


class TestChronTrigger:
    pass


class TestFilePatternTrigger:
    def test_run(self, mocker, tmp_path):
        # force return code of subprocess.run to be 1 so that TestFilePatternTrigger
        # main loop can exit
        mocker.patch(
            "subprocess.run",
            return_value=CompletedProcess(None, returncode=1, stdout="", stderr=""),
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

    def test_timeout(self, tmp_path):
        # create fp trigger so we can use collect_and_move_files()
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
