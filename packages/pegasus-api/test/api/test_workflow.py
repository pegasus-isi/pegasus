import json
import os
import re
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from jsonschema import validate

import yaml

import Pegasus
from Pegasus.api.errors import DuplicateError, NotFoundError, PegasusError
from Pegasus.api.mixins import EventType, Namespace
from Pegasus.api.replica_catalog import File, ReplicaCatalog
from Pegasus.api.site_catalog import SiteCatalog
from Pegasus.api.transformation_catalog import (
    Transformation,
    TransformationCatalog,
    TransformationSite,
)
from Pegasus.api.workflow import (
    PEGASUS_VERSION,
    AbstractJob,
    Job,
    SubWorkflow,
    Workflow,
    _JobDependency,
    _LinkType,
    _needs_client,
    _needs_submit_dir,
    _Use,
)
from Pegasus.api.writable import _CustomEncoder
from Pegasus.client._client import Client


class Test_Use:
    def test_valid_use(self):
        assert _Use(File("a"), _LinkType.INPUT)

    def test_invalid_use_bad_file(self):
        with pytest.raises(TypeError) as e:
            _Use(123, _LinkType.INPUT)

        assert "invalid file: 123; file must be of type File" in str(e)

    def test_invalid_use_bad_link_type(self):
        with pytest.raises(TypeError) as e:
            _Use(File("a"), "link")

        assert "invalid link_type: link;" in str(e)

    def test_eq(self):
        assert _Use(File("a"), _LinkType.INPUT) == _Use(File("a"), _LinkType.OUTPUT)
        assert _Use(File("a"), _LinkType.INPUT) != _Use(File("b"), _LinkType.INPUT)

    def test_eq_invalid(self):
        with pytest.raises(ValueError) as e:
            _Use(File("a"), _LinkType.INPUT) == "use"

        assert "_Use cannot be compared with" in str(e)

    @pytest.mark.parametrize(
        "use, expected",
        [
            (
                _Use(
                    File("a"), _LinkType.INPUT, stage_out=None, register_replica=False
                ),
                {"lfn": "a", "type": "input", "registerReplica": False,},
            ),
            (
                _Use(File("a"), _LinkType.INPUT, stage_out=None, register_replica=None),
                {"lfn": "a", "type": "input",},
            ),
            (
                _Use(
                    File("a", size=2048).add_metadata(createdBy="ryan"),
                    _LinkType.OUTPUT,
                    stage_out=False,
                    register_replica=True,
                ),
                {
                    "lfn": "a",
                    "size": 2048,
                    "metadata": {"createdBy": "ryan"},
                    "type": "output",
                    "stageOut": False,
                    "registerReplica": True,
                },
            ),
            (
                _Use(
                    File("a", size=1024),
                    _LinkType.CHECKPOINT,
                    stage_out=True,
                    register_replica=True,
                ),
                {
                    "lfn": "a",
                    "size": 1024,
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
        result["uses"] = sorted(result["uses"], key=lambda use: use["lfn"])

        expected = {
            "id": "aj",
            "nodeLabel": "test",
            "arguments": ["-i", "f1"],
            "stdin": "stdin",
            "stdout": "stdout",
            "stderr": "stderr",
            "uses": [
                {"lfn": "stdin", "type": "input"},
                {"lfn": "if1", "type": "input"},
                {"lfn": "if2", "type": "input"},
                {
                    "lfn": "stdout",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "stderr",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "of1",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "of2",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "cpf",
                    "type": "checkpoint",
                    "stageOut": True,
                    "registerReplica": False,
                },
            ],
        }

        expected["uses"] = sorted(expected["uses"], key=lambda use: use["lfn"])

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
        j.add_args("-i", File("f1"), "-n", 1, 1.1)
        j.add_inputs(File("if1"), File("if2"))
        j.add_outputs(File("of1"), File("of2"))

        result = json.loads(json.dumps(j, cls=_CustomEncoder))
        result["uses"] = sorted(result["uses"], key=lambda use: use["lfn"])

        expected = {
            "type": "job",
            "name": "t1",
            "namespace": "ns",
            "id": "id",
            "nodeLabel": "label",
            "version": "1",
            "arguments": ["-i", "f1", "-n", 1, 1.1],
            "stdin": "stdin",
            "stdout": "stdout",
            "stderr": "stderr",
            "uses": [
                {"lfn": "stdin", "type": "input"},
                {"lfn": "if1", "type": "input"},
                {"lfn": "if2", "type": "input"},
                {
                    "lfn": "stdout",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "stderr",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "of1",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
                {
                    "lfn": "of2",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": False,
                },
            ],
        }

        expected["uses"] = sorted(expected["uses"], key=lambda use: use["lfn"])

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
    def test_eq(self):
        assert _JobDependency("1", {"2", "3"}) == _JobDependency("1", {"2", "3"})

    def test_eq_invalid(self):
        with pytest.raises(ValueError) as e:
            _JobDependency("1", {"2", "3"}) == 123

        assert "_JobDependency cannot be compared with" in str(e)

    def test_tojson(self):
        jd = _JobDependency("parent_id", {"child_id1"})
        assert jd.__json__() == {
            "id": "parent_id",
            "children": ["child_id1"],
        }


class TestSubWorkflow:
    @pytest.mark.parametrize(
        "file, is_planned", [(File("wf-file"), False), ("wf-file", True)]
    )
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
                SubWorkflow(
                    "file", False, _id="test-subworkflow", node_label="label"
                ).add_args("--sites", "condorpool"),
                {
                    "type": "pegasusWorkflow",
                    "file": "file",
                    "id": "test-subworkflow",
                    "nodeLabel": "label",
                    "arguments": ["--sites", "condorpool"],
                    "uses": [{"lfn": "file", "type": "input"}],
                },
            ),
            (
                SubWorkflow("file", True, _id="test-subworkflow", node_label="label"),
                {
                    "type": "condorWorkflow",
                    "file": "file",
                    "id": "test-subworkflow",
                    "nodeLabel": "label",
                    "arguments": [],
                    "uses": [{"lfn": "file", "type": "input"}],
                },
            ),
        ],
    )
    def test_tojson(self, subworkflow, expected):
        result = json.loads(json.dumps(subworkflow, cls=_CustomEncoder))
        assert result == expected


@pytest.fixture
def expected_json():
    expected = {
        "pegasus": PEGASUS_VERSION,
        "name": "wf",
        "jobs": [
            {
                "type": "job",
                "id": "a",
                "name": "t1",
                "stdin": "stdin",
                "stdout": "stdout",
                "stderr": "stderr",
                "arguments": ["do-nothing", "-n", 1, 1.1],
                "uses": [
                    {"lfn": "stdin", "type": "input"},
                    {
                        "lfn": "stdout",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                    {
                        "lfn": "stderr",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                    {
                        "lfn": "f1",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                    {
                        "lfn": "f2",
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
                    {"lfn": "f1", "type": "input"},
                    {"lfn": "f2", "type": "input"},
                    {
                        "lfn": "checkpoint",
                        "type": "checkpoint",
                        "stageOut": True,
                        "registerReplica": False,
                    },
                ],
            },
            {
                "type": "condorWorkflow",
                "file": "subworkflow.dag",
                "id": "c",
                "arguments": ["--sites", "condorpool"],
                "uses": [{"lfn": "subworkflow.dag", "type": "input"}],
            },
            {
                "type": "pegasusWorkflow",
                "file": "subworkflow.dax",
                "id": "d",
                "arguments": [],
                "uses": [{"lfn": "subworkflow.dax", "type": "input"}],
            },
        ],
        "jobDependencies": [{"id": "a", "children": ["b"]}],
        "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
        "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
        "metadata": {"key": "value"},
    }

    expected["jobs"] = sorted(expected["jobs"], key=lambda j: j["id"])
    expected["jobs"][0]["uses"] = sorted(
        expected["jobs"][0]["uses"], key=lambda u: u["lfn"]
    )
    expected["jobs"][1]["uses"] = sorted(
        expected["jobs"][1]["uses"], key=lambda u: u["lfn"]
    )

    return expected


@pytest.fixture(scope="function")
def wf():
    wf = Workflow("wf")

    j1 = (
        Job("t1", _id="a")
        .add_outputs(File("f1"), File("f2"))
        .add_args(File("do-nothing"), "-n", 1, 1.1)
        .set_stdin("stdin")
        .set_stdout("stdout")
        .set_stderr("stderr")
    )
    j2 = (
        Job("t1", _id="b")
        .add_inputs(File("f1"), File("f2"))
        .add_checkpoint(File("checkpoint"))
    )
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
        [
            (Job("t1", _id="job")),
            (SubWorkflow(File("f1"), False, _id="job")),
            (SubWorkflow("f1", True, _id="job")),
        ],
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

    def test_add_site_catalog(self):
        sc = SiteCatalog()
        wf = Workflow("wf")

        try:
            wf.add_site_catalog(sc)
        except:
            pytest.fail("should not have raised exception")

    def test_add_invalid_site_catalog(self):
        wf = Workflow("wf")
        with pytest.raises(TypeError) as e:
            wf.add_site_catalog(123)

        assert "invalid catalog: 123" in str(e)

    def test_add_duplicate_site_catalog(self):
        sc = SiteCatalog()
        wf = Workflow("wf")
        wf.add_site_catalog(sc)

        with pytest.raises(DuplicateError) as e:
            wf.add_site_catalog(sc)

        assert "a SiteCatalog has already" in str(e)

    def test_add_replica_catalog(self):
        rc = ReplicaCatalog()
        wf = Workflow("wf")

        try:
            wf.add_replica_catalog(rc)
        except:
            pytest.fail("should not have raised exception")

    def test_add_invalid_replica_catalog(self):
        wf = Workflow("wf")
        with pytest.raises(TypeError) as e:
            wf.add_replica_catalog(123)

        assert "invalid catalog: 123" in str(e)

    def test_add_duplicate_replica_catalog(self):
        rc = ReplicaCatalog()
        wf = Workflow("wf")
        wf.add_replica_catalog(rc)

        with pytest.raises(DuplicateError) as e:
            wf.add_replica_catalog(rc)

        assert "a ReplicaCatalog has already" in str(e)

    def test_add_transformation_catalog(self):
        tc = TransformationCatalog()
        wf = Workflow("wf")

        try:
            wf.add_transformation_catalog(tc)
        except:
            pytest.fail("should not have raised exception")

    def test_add_invalid_transformation_catalog(self):
        wf = Workflow("wf")
        with pytest.raises(TypeError) as e:
            wf.add_transformation_catalog(123)

        assert "invalid catalog: 123" in str(e)

    def test_add_duplicate_transformation_catalog(self):
        tc = TransformationCatalog()
        wf = Workflow("wf")
        wf.add_transformation_catalog(tc)

        with pytest.raises(DuplicateError) as e:
            wf.add_transformation_catalog(tc)

        assert "a TransformationCatalog has already" in str(e)

    def test_add_dependency_parents(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        parents = [
            Job("t", _id="parent1"),
            Job("t", _id="parent2"),
            Job("t", _id="parent3"),
        ]

        wf.add_jobs(job, *parents)

        wf.add_dependency(job, parents=[parents[0]])
        wf.add_dependency(job, parents=parents[1:])

        for parent in parents:
            assert wf.dependencies[parent._id] == _JobDependency(parent._id, {job._id})

    def test_add_dependency_children(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        children = [
            Job("t", _id="child1"),
            Job("t", _id="child2"),
            Job("t", _id="child3"),
        ]

        wf.add_jobs(job, *children)

        wf.add_dependency(job, children=[children[0]])
        assert wf.dependencies[job._id] == _JobDependency(job._id, {children[0]._id})

        wf.add_dependency(job, children=children[1:])
        assert wf.dependencies[job._id] == _JobDependency(
            job._id, {child._id for child in children}
        )

    def test_add_dependency_parents_and_children(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        parents = [Job("t", _id="parent1"), Job("t", _id="parent2")]

        children = [Job("t", _id="child1"), Job("t", _id="child2")]

        wf.add_jobs(*parents, *children)

        # add nothing
        wf.add_dependency(job)
        assert len(wf.dependencies) == 0

        wf.add_dependency(job, parents=parents, children=children)

        for parent in parents:
            assert wf.dependencies[parent._id] == _JobDependency(parent._id, {job._id})

        assert wf.dependencies[job._id] == _JobDependency(
            job._id, {child._id for child in children}
        )

    def test_add_duplicate_parent_dependency(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        parent = Job("t", _id="parent")

        wf.add_jobs(job, parent)

        with pytest.raises(DuplicateError) as e:
            wf.add_dependency(job, parents=[parent, parent])

        assert (
            "A dependency already exists between parent id: parent and job id: job"
            in str(e)
        )

    def test_add_duplicate_child_dependency(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        child = Job("t", _id="child")

        wf.add_jobs(job, child)

        with pytest.raises(DuplicateError) as e:
            wf.add_dependency(job, children=[child, child])

        assert (
            "A dependency already exists between job id: job and child id: child"
            in str(e)
        )

    def test_add_dependency_invalid_job(self):
        wf = Workflow("wf")
        job = Job("t")

        with pytest.raises(ValueError) as e:
            wf.add_dependency(job)

        assert "The given job does not have an id" in str(e)

    def test_add_dependency_invalid_parent(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        parent = Job("t")

        with pytest.raises(ValueError) as e:
            wf.add_dependency(job, parents=[parent])

        assert "One of the given parents does not have an id" in str(e)

    def test_add_dependency_invalid_child(self):
        wf = Workflow("wf")
        job = Job("t", _id="job")
        child = Job("t")

        with pytest.raises(ValueError) as e:
            wf.add_dependency(job, children=[child])

        assert "One of the given children does not have an id" in str(e)

    def test_infer_dependencies_fork_join_wf(self):
        wf = Workflow("wf")

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
        wf = Workflow("wf")
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
            result["jobs"][0]["uses"], key=lambda u: u["lfn"]
        )
        result["jobs"][1]["uses"] = sorted(
            result["jobs"][1]["uses"], key=lambda u: u["lfn"]
        )

        assert result == expected_json

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write_file_obj(
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

            # _path should be set by the call to write
            assert wf._path == f.name

            f.seek(0)
            result = loader(f)

        workflow_schema = load_schema("wf-5.0.json")
        validate(instance=result, schema=workflow_schema)

        result["jobs"] = sorted(result["jobs"], key=lambda j: j["id"])
        result["jobs"][0]["uses"] = sorted(
            result["jobs"][0]["uses"], key=lambda u: u["lfn"]
        )
        result["jobs"][1]["uses"] = sorted(
            result["jobs"][1]["uses"], key=lambda u: u["lfn"]
        )

        assert result == expected_json

    def test_write_str_filename(self, wf, load_schema, expected_json):
        path = "wf.yml"
        wf.write(path)

        # _path should be set by the call to write
        assert wf._path == path

        with open(path) as f:
            result = yaml.safe_load(f)

        workflow_schema = load_schema("wf-5.0.json")
        validate(instance=result, schema=workflow_schema)

        result["jobs"] = sorted(result["jobs"], key=lambda j: j["id"])
        result["jobs"][0]["uses"] = sorted(
            result["jobs"][0]["uses"], key=lambda u: u["lfn"]
        )
        result["jobs"][1]["uses"] = sorted(
            result["jobs"][1]["uses"], key=lambda u: u["lfn"]
        )

        assert result == expected_json

        os.remove(path)

    def test_write_default_filename(self, wf, expected_json):
        wf.write()
        EXPECTED_FILE = "workflow.yml"

        with open(EXPECTED_FILE) as f:
            result = yaml.safe_load(f)

        result["jobs"] = sorted(result["jobs"], key=lambda j: j["id"])

        for i in range(len(result["jobs"])):
            result["jobs"][i]["uses"] = sorted(
                result["jobs"][i]["uses"], key=lambda u: u["lfn"]
            )

        assert result == expected_json

        os.remove(EXPECTED_FILE)

    def test_write_wf_catalogs_included(self):
        wf = Workflow("test")
        wf.add_jobs(Job("ls"))

        wf.add_transformation_catalog(TransformationCatalog())
        wf.add_site_catalog(SiteCatalog())
        wf.add_replica_catalog(ReplicaCatalog())

        wf_path = Path("workflow.yml")
        with wf_path.open("w+") as f:
            wf.write(f)
            f.seek(0)
            result = yaml.load(f)

        expected = {
            "pegasus": "5.0",
            "name": "test",
            "siteCatalog": {"sites": []},
            "replicaCatalog": {"replicas": []},
            "transformationCatalog": {"transformations": []},
            "jobs": [
                {
                    "type": "job",
                    "name": "ls",
                    "id": "ID0000001",
                    "arguments": [],
                    "uses": [],
                }
            ],
            "jobDependencies": [],
        }

        assert expected == result

        wf_path.unlink()

    def test_write_valid_hierarchical_workflow(self, mocker):
        mocker.patch("Pegasus.api.workflow.Workflow.write")

        try:
            wf = Workflow("test")
            wf.add_jobs(SubWorkflow("file", False))
            wf.write(file="workflow.yml", _format="yml")
        except PegasusError:
            pytest.fail("shouldn't have thrown PegasusError")

        Pegasus.api.workflow.Workflow.write.assert_called_once_with(
            file="workflow.yml", _format="yml"
        )

    @pytest.mark.parametrize(
        "sc, tc",
        [
            (SiteCatalog(), None),
            (None, TransformationCatalog()),
            (SiteCatalog(), TransformationCatalog()),
        ],
    )
    def test_write_hierarchical_workflow_when_catalogs_are_inlined(self, sc, tc):
        wf = Workflow("test")
        wf.add_jobs(SubWorkflow("file", False))

        if sc:
            wf.add_site_catalog(sc)

        if tc:
            wf.add_transformation_catalog(tc)

        with pytest.raises(PegasusError) as e:
            wf.write()

        assert (
            "Site Catalog and Transformation Catalog must be written as a separate"
            in str(e)
        )

    def test_workflow_key_ordering_on_yml_write(self):
        tc = TransformationCatalog()
        rc = ReplicaCatalog()
        sc = SiteCatalog()

        wf = Workflow("wf")
        wf.add_transformation_catalog(tc)
        wf.add_replica_catalog(rc)
        wf.add_site_catalog(sc)

        wf.add_jobs(Job("t1", _id="a"))

        wf.add_env(JAVA_HOME="/java/home")
        wf.add_shell_hook(EventType.START, "/bin/echo hi")
        wf.add_metadata(key="value")

        wf.write()
        EXPECTED_FILE = Path("workflow.yml")

        with EXPECTED_FILE.open() as f:
            # reading in as str so ordering of keys is not disrupted
            # when loaded into a dict
            result = f.read()

        EXPECTED_FILE.unlink()

        """
        Check that wf keys have been ordered as follows (while ignoring nested keys):
        - pegasus,
        - name,
        - hooks,
        - profiles,
        - metadata,
        - siteCatalog,
        - replicaCatalog,
        - transformationCatalog,
        - jobs
        - jobDependencies
        """
        p = re.compile(
            r"pegasus: '5.0'[\w\W]+name:[\w\W]+hooks:[\w\W]+profiles:[\w\W]+metadata:[\w\W]+siteCatalog:[\w\W]+replicaCatalog:[\w\W]+transformationCatalog:[\w\W]+jobs:[\w\W]+jobDependencies:[\w\W]+"
        )
        assert p.match(result) is not None

    def test_plan_workflow_already_written(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        path = "wf.yml"
        wf.write(path).plan()

        assert wf._path == path

        Pegasus.client._client.Client.plan.assert_called_once_with(
            path,
            cleanup="none",
            conf=None,
            dir=None,
            force=False,
            input_dir=None,
            output_dir=None,
            output_site="local",
            relative_dir=None,
            sites=None,
            staging_sites=None,
            submit=False,
            verbose=0,
        )

        os.remove(path)

    def test_plan_workflow_not_written(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        DEFAULT_WF_PATH = "workflow.yml"
        wf.plan()

        assert wf._path == DEFAULT_WF_PATH

        Pegasus.client._client.Client.plan.assert_called_once_with(
            DEFAULT_WF_PATH,
            cleanup="none",
            conf=None,
            dir=None,
            force=False,
            input_dir=None,
            output_dir=None,
            output_site="local",
            relative_dir=None,
            sites=None,
            staging_sites=None,
            submit=False,
            verbose=0,
        )

        os.remove(DEFAULT_WF_PATH)

    def test_run(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.run")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf.run()

        Pegasus.client._client.Client.run.assert_called_once_with(None, verbose=0)

    def test_status(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.status")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.status()

        Pegasus.client._client.Client.status.assert_called_once_with(
            wf._submit_dir, long=0, verbose=0
        )

    def test_remove(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.remove")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.remove()

        Pegasus.client._client.Client.remove.assert_called_once_with(
            wf._submit_dir, verbose=0
        )

    def test_analyze(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.analyzer")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.analyze()

        Pegasus.client._client.Client.analyzer.assert_called_once_with(
            wf._submit_dir, verbose=0
        )

    def test_statistics(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.statistics")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.statistics()

        Pegasus.client._client.Client.statistics.assert_called_once_with(
            wf._submit_dir, verbose=0
        )


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
