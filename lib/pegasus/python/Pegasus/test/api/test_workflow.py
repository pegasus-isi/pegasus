import os
import json
import stat
import shutil
from tempfile import NamedTemporaryFile
from pathlib import Path


import pytest
import yaml
from jsonschema import validate

from Pegasus.api.workflow import AbstractJob
from Pegasus.api.workflow import Job
from Pegasus.api.workflow import SubWorkflow
from Pegasus.api.workflow import _LinkType
from Pegasus.api.workflow import _Use
from Pegasus.api.workflow import _JobDependency
from Pegasus.api.workflow import Workflow
from Pegasus.api.workflow import _needs_client
from Pegasus.api.workflow import _needs_submit_dir
from Pegasus.client._client import Client
from Pegasus.api.workflow import PEGASUS_VERSION
from Pegasus.api.replica_catalog import File
from Pegasus.api.replica_catalog import ReplicaCatalog
from Pegasus.api.errors import DuplicateError
from Pegasus.api.errors import NotFoundError
from Pegasus.api.transformation_catalog import Transformation
from Pegasus.api.transformation_catalog import TransformationSite
from Pegasus.api.transformation_catalog import TransformationCatalog
from Pegasus.api.mixins import ProfileMixin
from Pegasus.api.mixins import HookMixin
from Pegasus.api.mixins import MetadataMixin
from Pegasus.api.mixins import Namespace
from Pegasus.api.mixins import EventType
from Pegasus.api.site_catalog import SiteCatalog
from Pegasus.api.writable import _CustomEncoder


class Test_Use:
    def test_eq(self):
        assert _Use(File("a"), _LinkType.INPUT) == _Use(File("a"), _LinkType.OUTPUT)
        assert _Use(File("a"), _LinkType.INPUT) != _Use(File("b"), _LinkType.INPUT)

    @pytest.mark.parametrize(
        "use, expected",
        [
            (
                _Use(File("a"), _LinkType.INPUT, stage_out=True, register_replica=True),
                {
                    "file": {"lfn": "a"},
                    "type": "input",
                    "stageOut": True,
                    "registerReplica": True,
                },
            ),
            (
                _Use(
                    File("a"), _LinkType.OUTPUT, stage_out=False, register_replica=True
                ),
                {
                    "file": {"lfn": "a"},
                    "type": "output",
                    "stageOut": False,
                    "registerReplica": True,
                },
            ),
            (
                _Use(
                    File("a"),
                    _LinkType.CHECKPOINT,
                    stage_out=True,
                    register_replica=True,
                ),
                {
                    "file": {"lfn": "a"},
                    "type": "checkpoint",
                    "stageOut": True,
                    "registerReplica": True,
                },
            ),
        ],
    )
    def test_tojson(self, use, expected):
        result = json.loads(json.dumps(use, cls=_CustomEncoder))
        assert result == expected


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
        with pytest.raises(DuplicateError) as e:
            job.add_inputs(File("a"))

        assert "file: {file}".format(file=File("a")) in str(e)

    def test_add_invalid_input(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.add_inputs(123, "abc")

        assert "invalid input_file: 123" in str(e)

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
        with pytest.raises(DuplicateError) as e:
            job.add_outputs(File("a"))

        assert "file: {file}".format(file=File("a")) in str(e)

    def test_add_invalid_output(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.add_outputs(123, "abc")

        assert "invalid output_file: 123" in str(e)

    def test_add_inputs_and_outputs(self):
        job = AbstractJob()
        job.add_inputs(File("a"))
        job.add_outputs(File("b"))

        with pytest.raises(DuplicateError) as e:
            job.add_inputs(File("b"))

        assert "file: {file}".format(file=File("b")) in str(e)

    def test_add_checkpoint(self):
        job = AbstractJob()
        job.add_checkpoint(File("checkpoint"))

        assert _Use(File("checkpoint"), _LinkType.CHECKPOINT) in job.uses

    def test_add_invalid_checkpoint(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.add_checkpoint("badfile")

        assert "invalid checkpoint_file: badfile" in str(e)

    def test_add_duplicate_checkpoint(self):
        job = AbstractJob()
        job.add_inputs(File("abc"))
        with pytest.raises(DuplicateError) as e:
            job.add_checkpoint(File("abc"))

        assert "file: {file}".format(file=File("abc")) in str(e)

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

    def test_stdin_already_set(self):
        job = AbstractJob()
        job.set_stdin(File("a"))
        with pytest.raises(DuplicateError) as e:
            job.set_stdin(File("b"))

        assert "stdin has already" in str(e)

    def test_set_duplicate_stdin(self):
        job = AbstractJob()
        job.add_inputs(File("a"))
        with pytest.raises(DuplicateError) as e:
            job.set_stdin(File("a"))

        assert "file: {file}".format(file=File("a")) in str(e)

    def test_set_invalid_stdin(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.set_stdin(123)

        assert "invalid file: 123" in str(e)

    def test_get_stdin(self):
        job = AbstractJob()
        job.set_stdin("a")
        assert job.get_stdin() == File("a")

    def test_set_stdout(self):
        job = AbstractJob()
        job.set_stdout(File("a"))

        assert job.get_stdout() == File("a")
        assert job.get_outputs() == {File("a")}

    def test_set_stdout_already_set(self):
        job = AbstractJob()
        job.set_stdout(File("a"))
        with pytest.raises(DuplicateError) as e:
            job.set_stdout(File("b"))

        assert "stdout has already been set" in str(e)

    def test_set_duplicate_stdout(self):
        job = AbstractJob()
        job.add_outputs(File("a"))
        with pytest.raises(DuplicateError) as e:
            job.set_stdout(File("a"))

        assert "file: {file}".format(file=File("a")) in str(e)

    def test_set_invalid_stdout(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.set_stdout(123)

        assert "invalid file: 123" in str(e)

    def test_get_stdout(self):
        job = AbstractJob()
        job.set_stdout("a")
        assert job.get_stdout() == File("a")

    def test_set_stderr(self):
        job = AbstractJob()
        job.set_stderr(File("a"))

        assert job.get_stderr() == File("a")
        assert job.get_outputs() == {File("a")}

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
        with pytest.raises(TypeError) as e:
            job.set_stderr(123)

        assert "invalid file: 123" in str(e)

    def test_get_stderr(self):
        job = AbstractJob()
        job.set_stderr("a")
        assert job.get_stderr() == File("a")

    def test_tojson(self):
        j = AbstractJob(_id="aj", node_label="test")
        j.set_stdin("stdin")
        j.set_stdout("stdout")
        j.set_stderr("stderr")
        j.add_args("-i", File("f1"))
        j.add_inputs(File("if1"), File("if2"))
        j.add_outputs(File("of1"), File("of2"))
        j.add_checkpoint(File("cpf"))

        result = json.loads(json.dumps(j, cls=_CustomEncoder))
        result["uses"] = sorted(result["uses"], key=lambda use: use["file"]["lfn"])

        expected = {
            "id": "aj",
            "nodeLabel": "test",
            "arguments": ["-i", {"lfn": "f1"}],
            "stdin": {"lfn": "stdin"},
            "stdout": {"lfn": "stdout"},
            "stderr": {"lfn": "stderr"},
            "uses": [
                {
                    "file": {"lfn": "stdin"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "if1"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "if2"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
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
                {
                    "file": {"lfn": "cpf"},
                    "type": "checkpoint",
                    "stageOut": True,
                    "registerReplica": False,
                },
            ],
        }

        expected["uses"] = sorted(expected["uses"], key=lambda use: use["file"]["lfn"])

        assert result == expected


class TestJob:
    @pytest.mark.parametrize("transformation", [(Transformation("t")), ("t")])
    def test_valid_job(self, transformation):
        assert Job(transformation)

    def test_invalid_job(self):
        with pytest.raises(TypeError) as e:
            Job(123)

        assert "invalid transformation: 123" in str(e)

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

    def test_tojson_no_mixins(self):
        j = Job("t1", namespace="ns", node_label="label", _id="id", version="1")
        j.set_stdin("stdin")
        j.set_stdout("stdout")
        j.set_stderr("stderr")
        j.add_args("-i", File("f1"))
        j.add_inputs(File("if1"), File("if2"))
        j.add_outputs(File("of1"), File("of2"))

        result = json.loads(json.dumps(j, cls=_CustomEncoder))
        result["uses"] = sorted(result["uses"], key=lambda use: use["file"]["lfn"])

        expected = {
            "type": "job",
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
                {
                    "file": {"lfn": "stdin"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "if1"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
                {
                    "file": {"lfn": "if2"},
                    "type": "input",
                    "stageOut": False,
                    "registerReplica": False,
                },
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
        j.add_env(JAVA_HOME="/java/home")
        j.add_shell_hook(EventType.START, "/bin/echo hi")
        j.add_metadata(key="value")

        result = json.loads(json.dumps(j, cls=_CustomEncoder))
        expected = {
            "type": "job",
            "name": "t1",
            "arguments": [],
            "uses": [],
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
            "metadata": {"key": "value"},
        }

        assert result == expected


class Test_JobDependency:
    def test_tojson(self):
        jd = _JobDependency("parent_id", {"child_id1"})
        assert jd.__json__() == {
            "id": "parent_id",
            "children": ["child_id1"],
        }


class TestSubWorkflow:
    @pytest.mark.parametrize("file, is_planned", [(File("wf-file"), False), ("wf-file", True)])
    def test_valid_subworkflow(self, file, is_planned):
        assert SubWorkflow(file, is_planned)

    def test_invalid_subworkflow(self):
        with pytest.raises(TypeError) as e:
            SubWorkflow(123, False)

        assert "invalid file: 123" in str(e)

    @pytest.mark.parametrize(
        "subworkflow, expected",
        [
            ( 
                SubWorkflow("file", False, _id="test-subworkflow", node_label="label").add_args(
                "--sites", "condorpool"),
                {
                    "type": "dax",
                    "file": "file",
                    "id": "test-subworkflow",
                    "nodeLabel": "label",
                    "arguments": ["--sites", "condorpool"],
                    "uses": [
                        {
                            "file": {"lfn": "file"},
                            "type": "input",
                            "stageOut": False,
                            "registerReplica": False,
                        }
                    ],
                }
            ),
            (
                SubWorkflow("file", True, _id="test-subworkflow", node_label="label"),
                {
                    "type": "dag",
                    "file": "file",
                    "id": "test-subworkflow",
                    "nodeLabel": "label",
                    "arguments": [],
                    "uses": [
                        {
                            "file": {"lfn": "file"},
                            "type": "input",
                            "stageOut": False,
                            "registerReplica": False,
                        }
                    ],
                }
            )
        ]
    )
    def test_tojson(self, subworkflow, expected):
        result = json.loads(json.dumps(subworkflow, cls=_CustomEncoder))
        assert result == expected

@pytest.fixture
def expected_json():
    expected = {
        "pegasus": PEGASUS_VERSION,
        "name": "wf",
        "replicaCatalog": {
            "replicas": [{"lfn": "lfn", "pfn": "pfn", "site": "site", "regex": False}]
        },
        "transformationCatalog": {
            "transformations": [
                {
                    "name": "t1",
                    "sites": [{"name": "local", "pfn": "/pfn", "type": "installed"}],
                },
                {
                    "name": "t2",
                    "sites": [{"name": "local2", "pfn": "/pfn", "type": "stageable"}],
                },
            ]
        },
        "jobs": [
            {
                "type": "job",
                "id": "a",
                "name": "t1",
                "arguments": [],
                "uses": [
                    {
                        "file": {"lfn": "f1"},
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                    {
                        "file": {"lfn": "f2"},
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                ],
            },
            {
                "type": "job",
                "id": "b",
                "name": "t1",
                "arguments": [],
                "uses": [
                    {
                        "file": {"lfn": "f1"},
                        "type": "input",
                        "stageOut": False,
                        "registerReplica": False,
                    },
                    {
                        "file": {"lfn": "f2"},
                        "type": "input",
                        "stageOut": False,
                        "registerReplica": False,
                    },
                    {
                        "file": {"lfn": "checkpoint"},
                        "type": "checkpoint",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                ],
            },
            {
                "type": "dag",
                "file": "subworkflow.dag",
                "id": "c",
                "arguments": ["--sites", "condorpool"],
                "uses": [
                    {
                        "file": {"lfn": "subworkflow.dag"},
                        "type": "input",
                        "stageOut": False,
                        "registerReplica": False,
                    }
                ],
            },
            {
                "type": "dax",
                "file": "subworkflow.dax",
                "id": "d",
                "arguments": [],
                "uses": [
                    {
                        "file": {"lfn": "subworkflow.dax"},
                        "type": "input",
                        "stageOut": False,
                        "registerReplica": False,
                    }
                ],
            }
        ],
        "jobDependencies": [{"id": "a", "children": ["b"]}],
        "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
        "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
        "metadata": {"key": "value"},
    }

    expected["jobs"] = sorted(expected["jobs"], key=lambda j: j["id"])
    expected["jobs"][0]["uses"] = sorted(
        expected["jobs"][0]["uses"], key=lambda u: u["file"]["lfn"]
    )
    expected["jobs"][1]["uses"] = sorted(
        expected["jobs"][1]["uses"], key=lambda u: u["file"]["lfn"]
    )
    expected["transformationCatalog"]["transformations"] = sorted(expected["transformationCatalog"]["transformations"], key=lambda t: t["name"]) 

    return expected


@pytest.fixture(scope="function")
def wf():
    tc = TransformationCatalog()
    tc.add_transformation(
        Transformation("t1").add_site(TransformationSite("local", "/pfn", False)),
        Transformation("t2").add_site(TransformationSite("local2", "/pfn", True)),
    )

    rc = ReplicaCatalog()
    rc.add_replica("lfn", "pfn", "site")

    wf = Workflow("wf", infer_dependencies=True)
    wf.include_catalog(tc)
    wf.include_catalog(rc)

    j1 = Job("t1", _id="a").add_outputs(File("f1"), File("f2"))
    j2 = Job("t1", _id="b").add_inputs(File("f1"), File("f2")).add_checkpoint(File("checkpoint"))
    j3 = SubWorkflow("subworkflow.dag", True, _id="c").add_args("--sites", "condorpool")
    j4 = SubWorkflow(File("subworkflow.dax"), False, _id="d")

    wf.add_jobs(j1, j2, j3, j4)

    wf._infer_dependencies()

    wf.add_env(JAVA_HOME="/java/home")
    wf.add_shell_hook(EventType.START, "/bin/echo hi")
    wf.add_metadata(key="value")

    return wf


class TestWorkflow:
    @pytest.mark.parametrize(
        "job",
        [(Job("t1", _id="job")), (SubWorkflow(File("f1"), False, _id="job")), (SubWorkflow("f1", True, _id="job"))],
    )
    def test_add_job(self, job):
        wf = Workflow("wf")
        wf.add_jobs(job)

        assert job == wf.get_job("job")

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
        with pytest.raises(TypeError) as e:
            wf.include_catalog(123)

        assert "invalid catalog: 123" in str(e)

    def test_add_dependency(self):
        wf = Workflow("wf")
        parent = Job("t1", _id="parent")
        child1 = Job("t1", _id="child1")

        wf.add_jobs(parent, child1)
        wf.add_dependency(parent, child1)

        assert wf.dependencies["parent"] == _JobDependency(parent._id, {child1._id})

    def test_add_to_existing_dependencies(self):
        wf = Workflow("wf")
        parent = Job("t1", _id="parent")
        child1 = Job("t1", _id="child1")
        child2 = Job("t1", _id="child2")

        wf.add_jobs(parent, child1, child2)
        wf.add_dependency(parent, child1, child2)

        assert wf.dependencies["parent"] == _JobDependency(
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

        assert wf.dependencies["fork"] == _JobDependency("fork", {"work1", "work2"})
        assert wf.dependencies["work1"] == _JobDependency("work1", {"join"})
        assert wf.dependencies["work2"] == _JobDependency("work2", {"join"})

    def test_infer_dependencies_when_job_uses_stdin_stdout_and_stderr(self):
        wf = Workflow("wf", infer_dependencies=True)
        j1 = Job("t1", _id="j1").add_outputs(File("f1"))
        j2 = Job("t1", _id="j2").set_stdin(*j1.get_outputs()).set_stdout(File("f2"))
        j3 = Job("t1", _id="j3").add_inputs(*j2.get_outputs())
        wf.add_jobs(j1, j2, j3)

        # manually call _infer_dependencies() as it is only called when
        # wf.write() is called
        wf._infer_dependencies()

        assert wf.dependencies["j1"] == _JobDependency("j1", {"j2"})
        assert wf.dependencies["j2"] == _JobDependency("j2", {"j3"})

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema, wf, expected_json):
        result = json.loads(json.dumps(wf, cls=_CustomEncoder))

        workflow_schema = load_schema("wf-5.0.json")
        validate(instance=result, schema=workflow_schema)

        result["jobs"] = sorted(result["jobs"], key=lambda j: j["id"])
        result["jobs"][0]["uses"] = sorted(
            result["jobs"][0]["uses"], key=lambda u: u["file"]["lfn"]
        )
        result["jobs"][1]["uses"] = sorted(
            result["jobs"][1]["uses"], key=lambda u: u["file"]["lfn"]
        )

        result["transformationCatalog"]["transformations"] = sorted(result["transformationCatalog"]["transformations"], key=lambda t: t["name"]) 

        assert result == expected_json

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(
        self,
        convert_yaml_schemas_to_json,
        load_schema,
        wf,
        expected_json,
        _format,
        loader,
    ):
        with NamedTemporaryFile("r+") as f:
            wf.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

        workflow_schema = load_schema("wf-5.0.json")
        validate(instance=result, schema=workflow_schema)

        result["jobs"] = sorted(result["jobs"], key=lambda j: j["id"])
        result["jobs"][0]["uses"] = sorted(
            result["jobs"][0]["uses"], key=lambda u: u["file"]["lfn"]
        )
        result["jobs"][1]["uses"] = sorted(
            result["jobs"][1]["uses"], key=lambda u: u["file"]["lfn"]
        )

        result["transformationCatalog"]["transformations"] = sorted(result["transformationCatalog"]["transformations"], key=lambda t: t["name"]) 

        assert result == expected_json


@pytest.fixture(scope="function")
def obj():
    def _obj():
        class Obj:
            def __init__(self):
                self._client = None
                self._submit_dir = None
            
            @_needs_client
            def func_that_requires_client(self):
                ...
            @_needs_submit_dir
            def func_that_requires_submit_dir(self):
                ...
        
        return Obj()

    return _obj()

def test__needs_client(obj, mocker):
    mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
    obj.func_that_requires_client()
    shutil.which.assert_called_once_with("pegasus-version")
    assert isinstance(obj._client, Client)

def test__needs_submit_dir(obj):
    obj._submit_dir = "/path"
    try:
        obj.func_that_requires_submit_dir()
    except ValueError:
        pytest.fail("should not have thrown")

def test__needs_submit_dir_invalid(obj):
    with pytest.raises(ValueError):
        obj.func_that_requires_submit_dir()