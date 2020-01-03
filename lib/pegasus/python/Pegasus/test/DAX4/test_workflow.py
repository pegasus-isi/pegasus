import os
import json

import pytest

from Pegasus.DAX4.Workflow import AbstractJob, Job, JobInput, JobOutput, JobDependency
from Pegasus.DAX4.ReplicaCatalog import File
from Pegasus.DAX4.Errors import DuplicateError, NotFoundError


class TestJobInput:
    def test_eq(self):
        assert JobInput(File("a")) == JobInput(File("a"))
        assert JobInput(File("a")) != JobInput(File("b"))

    def test_tojson(self):
        assert JobInput(File("a")).__json__() == {"file": {"lfn": "a"}, "type": "input"}


class TestJobOutput:
    def test_eq(self):
        assert JobOutput(File("a")) == JobOutput(File("a"))
        assert JobOutput(File("a")) != JobOutput(File("b"))

    def test_tojson(self):
        assert JobOutput(
            File("a"), stage_out=False, register_replica=True
        ).__json__() == {
            "file": {"lfn": "a"},
            "type": "output",
            "stageOut": False,
            "registerReplica": True,
        }

        assert JobOutput(
            File("a"), stage_out=True, register_replica=True
        ).__json__() == {
            "file": {"lfn": "a"},
            "type": "output",
            "stageOut": True,
            "registerReplica": True,
        }


class TestAbstractJob:
    def test_get_inputs(self):
        job = AbstractJob()
        f1 = File("a")

        job.add_inputs(f1)

        assert job.get_inputs() == {f1}

    def test_add_inputs(self):
        job = AbstractJob()
        f1 = File("a")
        f2 = File("b")

        job.add_inputs(f1, f2)

        assert job.get_inputs() == {f1, f2}

    def test_add_duplicate_input(self):
        job = AbstractJob()
        job.add_inputs(File("a"))
        with pytest.raises(DuplicateError):
            job.add_inputs(File("a"))

    def test_get_outputs(self):
        job = AbstractJob()
        f1 = File("a")

        job.add_outputs(f1)

        assert job.get_outputs() == {f1}

    def test_add_outputs(self):
        job = AbstractJob()
        f1 = File("a")
        f2 = File("b")

        job.add_outputs(f1, f2)

        assert job.get_outputs() == {f1, f2}

    def test_add_duplicate_output(self):
        job = AbstractJob()
        job.add_outputs(File("a"))
        with pytest.raises(DuplicateError):
            job.add_outputs(File("a"))

    def test_add_args(self):
        job = AbstractJob()
        job.add_args("-i", File("f1"), "-o", File("f2"))

        assert job.args == ["-i", File("f1"), "-o", File("f2")]

    def test_extend_current_args(self):
        job = AbstractJob()
        job.add_args("-i", File("f1"), "-o", File("f2"))
        job.add_args("-v", "-n5")

        assert job.args == ["-i", File("f1"), "-o", File("f2"), "-v", "-n5"]

    def test_set_stdin(self):
        job = AbstractJob()
        job.set_stdin(File("a"))

        assert job.get_stdin() == File("a")
        assert job.get_inputs() == {File("a")}
        job.clear_stdin()

        job.set_stdin("a")
        assert job.get_stdin() == File("a")

    def test_stdin_already_set(self):
        job = AbstractJob()
        job.set_stdin(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stdin(File("b"))

    def test_set_duplicate_stdin(self):
        job = AbstractJob()
        job.add_inputs(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stdin(File("a"))

    def test_set_invalid_stdin(self):
        job = AbstractJob()
        with pytest.raises(ValueError):
            job.set_stdin(123)

    def test_get_stdin(self):
        job = AbstractJob()
        job.set_stdin("a")
        assert job.get_stdin() == File("a")

    def test_clear_stdin(self):
        job = AbstractJob()
        job.set_stdin(File("a"))

        assert job.get_inputs() == {File("a")}
        job.clear_stdin()

        assert job.get_stdin() == None
        assert job.get_inputs() == set()

    def test_set_stdout(self):
        job = AbstractJob()
        job.set_stdout(File("a"))

        assert job.get_stdout() == File("a")
        assert job.get_outputs() == {File("a")}
        job.clear_stdout()

        job.set_stdout("a")
        assert job.get_stdout() == File("a")

    def test_set_stdout_already_set(self):
        job = AbstractJob()
        job.set_stdout(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stdout(File("b"))

    def test_set_duplicate_stdout(self):
        job = AbstractJob()
        job.add_outputs(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stdout(File("a"))

    def test_set_invalid_stdout(self):
        job = AbstractJob()
        with pytest.raises(ValueError):
            job.set_stdout(123)

    def test_get_stdout(self):
        job = AbstractJob()
        job.set_stdout("a")
        assert job.get_stdout() == File("a")

    def test_clear_stdout(self):
        job = AbstractJob()
        job.set_stdout(File("a"))

        assert job.get_outputs() == {File("a")}
        job.clear_stdout()

        assert job.get_stdout() == None
        assert job.get_outputs() == set()

    def test_set_stderr(self):
        job = AbstractJob()
        job.set_stderr(File("a"))

        assert job.get_stderr() == File("a")
        assert job.get_outputs() == {File("a")}
        job.clear_stderr()

        job.set_stderr("a")
        assert job.get_stderr() == File("a")

    def test_set_stderr_already_set(self):
        job = AbstractJob()
        job.set_stderr(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stderr(File("b"))

    def test_set_duplicate_stderr(self):
        job = AbstractJob()
        job.add_outputs(File("a"))
        with pytest.raises(DuplicateError):
            job.set_stderr(File("a"))

    def test_set_invalid_stderr(self):
        job = AbstractJob()
        with pytest.raises(ValueError):
            job.set_stderr(123)

    def test_get_stderr(self):
        job = AbstractJob()
        job.set_stderr("a")
        assert job.get_stderr() == File("a")

    def test_clear_stderr(self):
        job = AbstractJob()
        job.set_stderr(File("a"))

        assert job.get_outputs() == {File("a")}
        job.clear_stderr()

        assert job.get_stderr() == None
        assert job.get_outputs() == set()


class TestJob:
    pass


class TestJobDependency:
    pass


class TestDAX:
    pass


class TestDAG:
    pass


class TestWorkflow:
    pass

