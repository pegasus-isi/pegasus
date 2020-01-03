import os
import json

import pytest

from Pegasus.DAX4.Workflow import (
    AbstractJob,
    Job,
    JobInput,
    JobOutput,
    JobDependency,
    Workflow,
    PEGASUS_VERSION,
)
from Pegasus.DAX4.ReplicaCatalog import File, ReplicaCatalog
from Pegasus.DAX4.Errors import DuplicateError, NotFoundError
from Pegasus.DAX4.TransformationCatalog import Transformation, TransformationCatalog
from Pegasus.DAX4.Mixins import (
    ProfileMixin,
    HookMixin,
    MetadataMixin,
    Namespace,
    EventType,
)
from Pegasus.DAX4.SiteCatalog import SiteCatalog
from Pegasus.DAX4.Writable import FileFormat


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

    def test_add_invalid_input(self):
        job = AbstractJob()
        with pytest.raises(ValueError):
            job.add_inputs(123, "abc")

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

    def test_add_invalid_output(self):
        job = AbstractJob()
        with pytest.raises(ValueError):
            job.add_outputs(123, "abc")

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
    @pytest.mark.parametrize("transformation", [(Transformation("t")), ("t")])
    def test_valid_job(self, transformation):
        Job(transformation)

    def test_invalid_job(self):
        with pytest.raises(ValueError):
            Job(123)

    def test_chaining(self):
        j = (
            Job("t1")
            .add_args("-n5")
            .add_inputs(File("if"))
            .add_outputs(File("of"))
            .set_stdin(File("stdin"))
            .set_stdout(File("stdout"))
            .set_stderr(File("stderr"))
        )

        assert j.transformation == "t1"
        assert j.args == ["-n5"]
        assert j.get_inputs() == {File("if"), File("stdin")}
        assert j.get_outputs() == {File("of"), File("stdout"), File("stderr")}

        (j.clear_stdin().clear_stdout().clear_stderr())

        assert j.get_inputs() == {File("if")}
        assert j.get_outputs() == {File("of")}

    def test_tojson_no_mixins(self):
        j = Job("t1", namespace="ns", node_label="label", _id="id", version="1")
        j.set_stdin("stdin")
        j.set_stdout("stdout")
        j.set_stderr("stderr")
        j.add_args("-i", File("f1"))
        j.add_inputs(File("if1"), File("if2"))
        j.add_outputs(File("of1"), File("of2"))

        result = j.__json__()
        result["uses"] = sorted(result["uses"], key=lambda use: use["file"]["lfn"])

        expected = {
            "name": "t1",
            "namespace": "ns",
            "id": "id",
            "nodeLabel": "label",
            "version": "1",
            "arguments": ["-i", {"lfn": "f1"}],
            "stdin": {"lfn": "stdin"},
            "stdout": {"lfn": "stdout"},
            "stderr": {"lfn": "stderr"},
            "uses": [
                {"file": {"lfn": "stdin"}, "type": "input"},
                {"file": {"lfn": "if1"}, "type": "input"},
                {"file": {"lfn": "if2"}, "type": "input"},
                {
                    "file": {"lfn": "stdout"},
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "stderr"},
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "of1"},
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "of2"},
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
            ],
        }

        expected["uses"] = sorted(expected["uses"], key=lambda use: use["file"]["lfn"])

        assert result == expected

    def test_tojson_with_mixins(self):
        j = Job("t1")
        j.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        j.add_shell_hook(EventType.START, "/bin/echo hi")
        j.add_metadata("key", "value")

        assert j.__json__() == {
            "name": "t1",
            "arguments": [],
            "uses": [],
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
            "metadata": {"key": "value"},
        }


class TestJobDependency:
    def test_tojson(self):
        jd = JobDependency("parent_id", {"child_id1"})
        assert jd.__json__() == {
            "id": "parent_id",
            "children": ["child_id1"],
        }


class TestDAX:
    pass


class TestDAG:
    pass


class TestWorkflow:
    def test_add_job(self):
        wf = Workflow("wf")
        j1 = Job("t1", _id="j1")
        j2 = Job("t2", _id="j2")
        wf.add_jobs(j1, j2)

        assert j1 == wf.get_job("j1")
        assert j2 == wf.get_job("j2")

    def test_add_duplicate_job(self):
        wf = Workflow("wf")
        with pytest.raises(DuplicateError):
            wf.add_jobs(Job("t1", _id="j1"), Job("t2", _id="j1"))

    def test_get_job(self):
        wf = Workflow("wf")
        j1 = Job("t1", _id="j1")
        wf.add_jobs(j1)

        assert j1 == wf.get_job("j1")

    def test_get_invalid_job(self):
        wf = Workflow("wf")
        with pytest.raises(NotFoundError):
            wf.get_job("abc123")

    def test_job_id_assignment_by_workflow(self):
        wf = Workflow("wf")
        j1 = Job("t1", _id="a")
        j2 = Job("t2")
        j3 = Job("t3", _id="b")
        j4 = Job("t4")
        j5 = Job("t5")
        wf.add_jobs(j1, j2, j3, j4, j5)

        assert j2._id == "ID0000001"
        assert j4._id == "ID0000002"
        assert j5._id == "ID0000003"

    def test_include_catalogs(self):
        tc = TransformationCatalog()
        rc = ReplicaCatalog()
        sc = SiteCatalog()

        wf = Workflow("wf")
        wf.include_catalog(tc)
        wf.include_catalog(rc)
        wf.include_catalog(sc)

        assert wf.transformation_catalog == tc
        assert wf.replica_catalog == rc
        assert wf.site_catalog == sc

    @pytest.mark.parametrize(
        "catalog", [(TransformationCatalog()), (ReplicaCatalog()), (SiteCatalog())]
    )
    def test_include_catalog_multiple_times(self, catalog):
        wf = Workflow("wf")
        wf.include_catalog(catalog)
        with pytest.raises(ValueError):
            wf.include_catalog(catalog)

    def test_include_invalid_catalog(self):
        wf = Workflow("wf")
        with pytest.raises(ValueError):
            wf.include_catalog(123)

    def test_add_dependency(self):
        wf = Workflow("wf")
        parent = Job("t1", _id="parent")
        child1 = Job("t1", _id="child1")

        wf.add_jobs(parent, child1)
        wf.add_dependency(parent, child1)

        assert wf.dependencies["parent"] == JobDependency(parent._id, {child1._id})

    def test_add_to_existing_dependencies(self):
        wf = Workflow("wf")
        parent = Job("t1", _id="parent")
        child1 = Job("t1", _id="child1")
        child2 = Job("t1", _id="child2")

        wf.add_jobs(parent, child1, child2)
        wf.add_dependency(parent, child1, child2)

        assert wf.dependencies["parent"] == JobDependency(
            parent._id, {child1._id, child2._id}
        )

    def test_add_duplicate_dependency(self):
        wf = Workflow("wf")
        parent = Job("t1", _id="parent")
        child1 = Job("t1", _id="child1")

        wf.add_jobs(parent, child1)
        wf.add_dependency(parent, child1)

        with pytest.raises(DuplicateError):
            wf.add_dependency(parent, child1)

    def test_infer_dependencies_fork_join_wf(self):
        wf = Workflow("wf", infer_dependencies=True)

        f1 = File("f1")
        f2 = File("f2")
        f3 = File("f3")
        f4 = File("f4")

        fork = Job("t1", _id="fork").add_outputs(f1, f2)
        work1 = Job("t1", _id="work1").add_inputs(f1).add_outputs(f3)
        work2 = Job("t1", _id="work2").add_inputs(f2).add_outputs(f4)
        join = Job("t1", _id="join").add_inputs(f3, f4)
        wf.add_jobs(fork, work1, work2, join)

        # manually call _infer_dependencies() as it is only called when
        # wf.write() is called
        wf._infer_dependencies()

        assert wf.dependencies["fork"] == JobDependency("fork", {"work1", "work2"})
        assert wf.dependencies["work1"] == JobDependency("work1", {"join"})
        assert wf.dependencies["work2"] == JobDependency("work2", {"join"})

    def test_infer_dependencies_when_job_uses_stdin_stdout_and_stderr(self):
        wf = Workflow("wf", infer_dependencies=True)
        j1 = Job("t1", _id="j1").add_outputs(File("f1"))
        j2 = Job("t1", _id="j2").set_stdin(*j1.get_outputs()).set_stdout(File("f2"))
        j3 = Job("t1", _id="j3").add_inputs(*j2.get_outputs())
        wf.add_jobs(j1, j2, j3)

        # manually call _infer_dependencies() as it is only called when
        # wf.write() is called
        wf._infer_dependencies()

        assert wf.dependencies["j1"] == JobDependency("j1", {"j2"})
        assert wf.dependencies["j2"] == JobDependency("j2", {"j3"})

    def test_tojson(self):
        tc = TransformationCatalog()
        tc.add_transformations(Transformation("t1"), Transformation("t2"))

        rc = ReplicaCatalog()
        rc.add_replica("lfn", "pfn", "site")

        wf = Workflow("wf", infer_dependencies=True)
        wf.include_catalog(tc)
        wf.include_catalog(rc)

        j1 = Job("t1", _id="j1").add_outputs(File("f1"), File("f2"))
        j2 = Job("t1").add_inputs(File("f1"), File("f2"))

        wf.add_jobs(j1, j2)

        wf._infer_dependencies()

        wf.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        wf.add_shell_hook(EventType.START, "/bin/echo hi")
        wf.add_metadata("key", "value")

        result = wf.__json__()

        rc_json = rc.__json__()
        del rc_json["pegasus"]

        tc_json = tc.__json__()
        del tc_json["pegasus"]

        expected = {
            "pegasus": PEGASUS_VERSION,
            "name": "wf",
            "replicaCatalog": rc_json,
            "transformationCatalog": tc_json,
            "jobs": [j1.__json__(), j2.__json__()],
            "jobDependencies": [JobDependency("j1", {"ID0000001"}).__json__()],
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
            "metadata": {"key": "value"},
        }

        assert result == expected

    def test_write(self):
        tc = TransformationCatalog()
        tc.add_transformations(Transformation("t1"), Transformation("t2"))

        rc = ReplicaCatalog()
        rc.add_replica("lfn", "pfn", "site")

        wf = Workflow("wf", infer_dependencies=True)
        wf.include_catalog(tc)
        wf.include_catalog(rc)

        j1 = Job("t1", _id="j1").add_outputs(File("f1"), File("f2"))
        j2 = Job("t1").add_inputs(File("f1"), File("f2"))

        wf.add_jobs(j1, j2)

        wf._infer_dependencies()

        wf.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        wf.add_shell_hook(EventType.START, "/bin/echo hi")
        wf.add_metadata("key", "value")

        rc_json = rc.__json__()
        del rc_json["pegasus"]

        tc_json = tc.__json__()
        del tc_json["pegasus"]

        expected = {
            "pegasus": PEGASUS_VERSION,
            "name": "wf",
            "replicaCatalog": rc_json,
            "transformationCatalog": tc_json,
            "jobs": [j1.__json__(), j2.__json__()],
            "jobDependencies": [JobDependency("j1", {"ID0000001"}).__json__()],
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
            "metadata": {"key": "value"},
        }

        test_output_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
        "WorkflowTestOutput.json")

        wf.write(non_default_filepath=test_output_filename, file_format=FileFormat.JSON)

        with open(test_output_filename, "r") as f:
            result = json.load(f)
        
        assert result == expected

        # cleanup
        os.remove(test_output_filename)
