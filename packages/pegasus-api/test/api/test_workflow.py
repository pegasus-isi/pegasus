import getpass
import json
import os
import re
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
import yaml
from jsonschema import validate

import Pegasus
from Pegasus.api.errors import DuplicateError, NotFoundError, PegasusError
from Pegasus.api.mixins import EventType, Namespace
from Pegasus.api.replica_catalog import File, ReplicaCatalog
from Pegasus.api.site_catalog import SiteCatalog
from Pegasus.api.transformation_catalog import Transformation, TransformationCatalog
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
from Pegasus.client._client import Workflow as WorkflowInstance

DEFAULT_WF_PATH = "workflow.yml"


class Test_Use:
    @pytest.mark.parametrize(
        "file, link, stage_out, register_replica, bypass_staging",
        [
            (File("a"), _LinkType.INPUT, True, True, True),
            (File("a"), _LinkType.INPUT, True, True, False),
            (File("a"), _LinkType.CHECKPOINT, False, False, False),
        ],
    )
    def test_valid_use(self, file, link, stage_out, register_replica, bypass_staging):
        assert _Use(file, link, stage_out, register_replica, bypass_staging)

    def test_invalid_use_bad_file(self):
        with pytest.raises(TypeError) as e:
            _Use(123, _LinkType.INPUT)

        assert "invalid file: 123; file must be of type File" in str(e)

    def test_invalid_use_bad_link_type(self):
        with pytest.raises(TypeError) as e:
            _Use(File("a"), "link")

        assert "invalid link_type: link;" in str(e)

    def test_invalid_use_bypass_set_and_type_neq_input(self):
        with pytest.raises(ValueError) as e:
            _Use(File("a"), _LinkType.OUTPUT, bypass_staging=True)

        assert "bypass can only be set to True when link type is INPUT" in str(e)

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
                    File("a"),
                    _LinkType.INPUT,
                    stage_out=None,
                    register_replica=None,
                    bypass_staging=True,
                ),
                {"lfn": "a", "type": "input", "bypass": True},
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
                    "metadata": {"createdBy": "ryan", "size": 2048},
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
                    "metadata": {"size": 1024},
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
        f2 = File("b")

        job.add_inputs(f1, "b")

        assert job.get_inputs() == {f1, f2}

    def test_add_inputs(self):
        job = AbstractJob()
        f1 = File("a")
        f2 = File("b")
        f3 = File("c")

        job.add_inputs(f1, f2, "c")

        assert job.get_inputs() == {f1, f2, f3}

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
        f2 = File("b")

        job.add_outputs(f1, "b")

        assert job.get_outputs() == {f1, f2}

    def test_add_outputs(self):
        job = AbstractJob()
        f1 = File("a")
        f2 = File("b")
        f3 = File("c")

        job.add_outputs(f1, f2, "c")

        assert job.get_outputs() == {f1, f2, f3}

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
        job.add_checkpoint("checkpoint2")

        assert _Use(File("checkpoint"), _LinkType.CHECKPOINT) in job.uses
        assert _Use(File("checkpoint2"), _LinkType.CHECKPOINT) in job.uses

    def test_add_invalid_checkpoint(self):
        job = AbstractJob()
        with pytest.raises(TypeError) as e:
            job.add_checkpoint(123)

        assert "invalid checkpoint_file: 123" in str(e)

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
        j.add_inputs(File("if3"), File("if4"), bypass_staging=True)
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
                {"lfn": "if3", "type": "input", "bypass": True},
                {"lfn": "if4", "type": "input", "bypass": True},
                {
                    "lfn": "stdout",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "stderr",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "of1",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "of2",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "cpf",
                    "type": "checkpoint",
                    "stageOut": True,
                    "registerReplica": True,
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
                    "registerReplica": True,
                },
                {
                    "lfn": "stderr",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "of1",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
                },
                {
                    "lfn": "of2",
                    "type": "output",
                    "stageOut": True,
                    "registerReplica": True,
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

    @pytest.mark.parametrize(
        "job, expected_repr_str",
        [
            (Job(Transformation(name="test")), "Job(transformation=test)"),
            (
                Job(Transformation(name="test", namespace="namespace")),
                "Job(namespace=namespace, transformation=test)",
            ),
            (
                Job(
                    Transformation(
                        name="test", namespace="namespace", version="version"
                    )
                ),
                "Job(namespace=namespace, transformation=test, version=version)",
            ),
            (Job(transformation="test"), "Job(transformation=test)"),
            (
                Job(transformation="test", namespace="namespace"),
                "Job(namespace=namespace, transformation=test)",
            ),
            (
                Job(transformation="test", namespace="namespace", version="version"),
                "Job(namespace=namespace, transformation=test, version=version)",
            ),
            (
                Job(transformation="test", _id="jid"),
                "Job(_id=jid, transformation=test)",
            ),
            (
                Job(transformation="test", node_label="label"),
                "Job(transformation=test, node_label=label)",
            ),
            (
                Job(transformation="test", version="version", node_label="label"),
                "Job(transformation=test, version=version, node_label=label)",
            ),
            (
                Job(Transformation(name="test"), _id="jid", node_label="label"),
                "Job(_id=jid, transformation=test, node_label=label)",
            ),
        ],
    )
    def test_repr(self, job, expected_repr_str):
        assert repr(job) == expected_repr_str


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
        "file, is_planned",
        [(File("wf-file"), False), ("wf-file", True), (Workflow("test"), False)],
    )
    def test_valid_subworkflow(self, file, is_planned):
        assert SubWorkflow(file, is_planned)

    def test_invalid_subworkflow(self):
        with pytest.raises(TypeError) as e:
            SubWorkflow(123, False)

        assert "invalid file: 123" in str(e)

    @pytest.mark.parametrize(
        "kwargs, expected_args_list",
        [
            ({}, []),
            (
                {
                    "conf": "conf",
                    "basename": "basename",
                    "job_prefix": "job_prefix",
                    "cluster": ["one", "two"],
                    "sites": ["local", "condorpool"],
                    "output_sites": ["local", "other"],
                    "staging_sites": {"condorpool": "staging"},
                    "cache": ["cache", Path("cache2")],
                    "input_dirs": ["input_dir1", Path("input_dir2")],
                    "output_dir": "output_dir",
                    "dir": "dir",
                    "relative_dir": "relative_dir",
                    "random_dir": True,
                    "relative_submit_dir": "relative_submit_dir",
                    "inherited_rc_files": ["f1", Path("f2")],
                    "cleanup": "inplace",
                    "reuse": ["reuse1", Path("reuse2")],
                    "verbose": 3,
                    "quiet": 3,
                    "force": True,
                    "force_replan": True,
                    "forward": ["forward"],
                    "submit": True,
                    "java_options": ["opt"],
                    "property.key": "property.value",
                },
                [
                    "-Dproperty.key=property.value",
                    "--basename",
                    "basename",
                    "--job-prefix",
                    "job_prefix",
                    "--conf",
                    "conf",
                    "--cluster",
                    "one,two",
                    "--sites",
                    "local,condorpool",
                    "--output-sites",
                    "local,other",
                    "--staging-site",
                    "condorpool=staging",
                    "--cache",
                    "cache,cache2",
                    "--input-dir",
                    "input_dir1,input_dir2",
                    "--output-dir",
                    "output_dir",
                    "--dir",
                    "dir",
                    "--relative-dir",
                    "relative_dir",
                    "--relative-submit-dir",
                    "relative_submit_dir",
                    "--randomdir",
                    "--inherited-rc-files",
                    "f1,f2",
                    "--cleanup",
                    "inplace",
                    "--reuse",
                    "reuse1,reuse2",
                    "-vvv",
                    "-qqq",
                    "--force",
                    "--force-replan",
                    "--forward",
                    "forward",
                    "--submit",
                    "-Xopt",
                ],
            ),
            (
                {
                    "conf": Path("conf"),
                    "basename": "basename",
                    "job_prefix": "job_prefix",
                    "cluster": ["one", "two"],
                    "sites": ["local", "condorpool"],
                    "output_sites": ["local", "other"],
                    "staging_sites": {"condorpool": "staging"},
                    "cache": ["cache", Path("cache2")],
                    "input_dirs": ["input_dir1", Path("input_dir2")],
                    "output_dir": "output_dir",
                    "dir": Path("dir"),
                    "relative_dir": Path("relative_dir"),
                    "random_dir": Path("random_dir"),
                    "relative_submit_dir": Path("relative_submit_dir"),
                    "inherited_rc_files": ["f1", Path("f2")],
                    "cleanup": "inplace",
                    "reuse": ["reuse1", Path("reuse2")],
                    "verbose": 3,
                    "quiet": 3,
                    "force": True,
                    "force_replan": True,
                    "forward": ["forward"],
                    "submit": False,
                    "java_options": ["opt"],
                    "other.property": "other.property.value",
                },
                [
                    "-Dother.property=other.property.value",
                    "--basename",
                    "basename",
                    "--job-prefix",
                    "job_prefix",
                    "--conf",
                    "conf",
                    "--cluster",
                    "one,two",
                    "--sites",
                    "local,condorpool",
                    "--output-sites",
                    "local,other",
                    "--staging-site",
                    "condorpool=staging",
                    "--cache",
                    "cache,cache2",
                    "--input-dir",
                    "input_dir1,input_dir2",
                    "--output-dir",
                    "output_dir",
                    "--dir",
                    "dir",
                    "--relative-dir",
                    "relative_dir",
                    "--relative-submit-dir",
                    "relative_submit_dir",
                    "--randomdir=random_dir",
                    "--inherited-rc-files",
                    "f1,f2",
                    "--cleanup",
                    "inplace",
                    "--reuse",
                    "reuse1,reuse2",
                    "-vvv",
                    "-qqq",
                    "--force",
                    "--force-replan",
                    "--forward",
                    "forward",
                    "-Xopt",
                ],
            ),
        ],
    )
    def test_add_planner_args(self, kwargs, expected_args_list):
        job = SubWorkflow("wf.yml", is_planned=False)
        job.add_planner_args(**kwargs)

        assert job.args == expected_args_list

    @pytest.mark.parametrize(
        "kwargs, expected_exception_msg",
        [
            ({"cluster": 1}, "invalid cluster"),
            ({"sites": 1}, "invalid sites"),
            ({"output_sites": 1}, "invalid output_sites"),
            ({"staging_sites": 1}, "invalid staging_sites"),
            ({"cache": 1}, "invalid cache"),
            ({"input_dirs": 1}, "invalid input_dirs"),
            ({"inherited_rc_files": 1}, "invalid inherited_rc_files"),
            ({"forward": 1}, "invalid forward"),
            ({"java_options": 1}, "invalid java_options"),
        ],
    )
    def test_invalid_planner_arg_type(self, kwargs, expected_exception_msg):
        with pytest.raises(TypeError) as e:
            SubWorkflow("wf_file").add_planner_args(**kwargs)

        assert expected_exception_msg in str(e)

    def test_add_planner_args_when_subworkflow_is_planned(self):
        with pytest.raises(PegasusError) as e:
            job = SubWorkflow("wf.yml", is_planned=True)
            job.add_planner_args()
            job.add_planner_args()

        assert (
            "SubWorkflow.add_planner_args() can only be called by SubWorkflows"
            in str(e)
        )

    def test_planner_args_already_set_error_thrown(self):
        job = SubWorkflow("wf.yml", is_planned=False)
        job.add_planner_args()

        with pytest.raises(PegasusError) as e:
            job.add_planner_args()

        assert "can only be invoked once" in str(e)

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

    def test_tojson_serialize_workflow_error(self):
        job = SubWorkflow(Workflow("test"))
        with pytest.raises(PegasusError) as e:
            job.__json__()

        assert "the given SubWorkflow file must be a File object" in str(e)

    @pytest.mark.parametrize(
        "subworkflow, expected_repr_str",
        [
            (
                SubWorkflow(file="file.yml"),
                "SubWorkflow(file=file.yml, is_planned=False)",
            ),
            (
                SubWorkflow(file="file.yml", is_planned=True),
                "SubWorkflow(file=file.yml, is_planned=True)",
            ),
            (
                SubWorkflow(file="file.yml", is_planned=False),
                "SubWorkflow(file=file.yml, is_planned=False)",
            ),
            (
                SubWorkflow(file="file.yml", _id="sid"),
                "SubWorkflow(_id=sid, file=file.yml, is_planned=False)",
            ),
            (
                SubWorkflow(file="file.yml", _id="sid", node_label="label"),
                "SubWorkflow(_id=sid, file=file.yml, is_planned=False, node_label=label)",
            ),
        ],
    )
    def test_repr(self, subworkflow, expected_repr_str):
        assert repr(subworkflow) == expected_repr_str


@pytest.fixture
def expected_json():
    expected = {
        "pegasus": PEGASUS_VERSION,
        "name": "wf㒀",
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
                        "registerReplica": True,
                    },
                    {
                        "lfn": "stderr",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
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
                        "registerReplica": True,
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
    wf = Workflow("wf㒀")

    j1 = (
        Job("t1", _id="a")
        .add_outputs(File("f1"), File("f2"), register_replica=False)
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
    @pytest.mark.parametrize("name", [("/bad/name1"), ("ba dname2")])
    def test_invalid_workflow_name(self, name):
        with pytest.raises(ValueError) as e:
            Workflow(name=name)

        assert "Invalid workflow name: {}".format(name) in str(e)

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

    def test_add_dependency_single_parent_add_child_multiple_times(self):
        wf = Workflow("wf")
        jobs = [Job("t", _id="job1"), Job("t", _id="job2")]
        parent = Job("t", _id="parent")

        for job in jobs:
            wf.add_dependency(job, parents=[parent])

        assert wf.dependencies[parent._id] == _JobDependency(
            parent._id, {job._id for job in jobs}
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

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
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

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
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

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
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
            result = yaml.safe_load(f)

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

        assert "createdOn" in result["x-pegasus"]
        assert result["x-pegasus"]["createdBy"] == getpass.getuser()
        assert result["x-pegasus"]["apiLang"] == "python"
        del result["x-pegasus"]
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
        "sc, tc, is_warning_msg_expected",
        [
            (None, None, False),
            (SiteCatalog(), None, True),
            (None, TransformationCatalog(), True),
            (SiteCatalog(), TransformationCatalog(), True),
        ],
    )
    def test_hierarchical_workflow_warning_message(
        self, caplog, sc, tc, is_warning_msg_expected
    ):
        wf = Workflow("test")
        wf.add_jobs(SubWorkflow("subwf.yml", is_planned=False))

        if sc:
            wf.add_site_catalog(sc)

        if tc:
            wf.add_transformation_catalog(tc)

        with NamedTemporaryFile(mode="w") as f:
            wf.write(f)

        expected_warning_msg = (
            "SiteCatalog and TransformationCatalog objects embedded into"
        )

        if is_warning_msg_expected:
            assert expected_warning_msg in str(caplog.records)
        else:
            assert expected_warning_msg not in str(caplog.records)

    @pytest.mark.parametrize(
        "workflow, expected_json",
        [
            (
                Workflow("abc")
                .add_jobs(
                    SubWorkflow(Workflow("subwf"), _id="testID")
                    .add_args("arg1")
                    .add_inputs(File("if.txt"))
                )
                .add_replica_catalog(
                    ReplicaCatalog().add_replica("local", "lfn", "pfn")
                ),
                {
                    "pegasus": "5.0",
                    "name": "abc",
                    "replicaCatalog": {
                        "replicas": [
                            {"lfn": "lfn", "pfns": [{"site": "local", "pfn": "pfn"}]},
                            {
                                "lfn": "subwf_testID.yml",
                                "pfns": [
                                    {
                                        "site": "local",
                                        "pfn": str(Path().cwd() / "subwf_testID.yml"),
                                    }
                                ],
                            },
                        ]
                    },
                    "jobs": [
                        {
                            "id": "testID",
                            "type": "pegasusWorkflow",
                            "file": "subwf_testID.yml",
                            "arguments": ["arg1"],
                            "uses": [
                                {"lfn": "if.txt", "type": "input",},
                                {"lfn": "subwf_testID.yml", "type": "input"},
                            ],
                        }
                    ],
                    "jobDependencies": [],
                },
            ),
            (
                Workflow("abc").add_jobs(
                    SubWorkflow(Workflow("subwf"), _id="testID")
                    .add_args("arg1")
                    .add_inputs(File("if.txt"))
                ),
                {
                    "pegasus": "5.0",
                    "name": "abc",
                    "replicaCatalog": {
                        "replicas": [
                            {
                                "lfn": "subwf_testID.yml",
                                "pfns": [
                                    {
                                        "site": "local",
                                        "pfn": str(Path().cwd() / "subwf_testID.yml"),
                                    }
                                ],
                            }
                        ]
                    },
                    "jobs": [
                        {
                            "id": "testID",
                            "type": "pegasusWorkflow",
                            "file": "subwf_testID.yml",
                            "arguments": ["arg1"],
                            "uses": [
                                {"lfn": "if.txt", "type": "input",},
                                {"lfn": "subwf_testID.yml", "type": "input"},
                            ],
                        }
                    ],
                    "jobDependencies": [],
                },
            ),
        ],
    )
    def test_workflow_to_subworkflow_conversion_in_write(
        self, load_schema, workflow, expected_json
    ):
        serialized_subworkflow_file = Path().cwd() / "subwf_testID.yml"

        with NamedTemporaryFile(mode="w+") as f:
            workflow.write(f)
            f.seek(0)
            result = yaml.safe_load(f)

        # validate written workflow yaml
        workflow_schema = load_schema("wf-5.0.json")
        validate(instance=result, schema=workflow_schema)

        del result["x-pegasus"]
        assert result == expected_json

        # cleanup
        serialized_subworkflow_file.unlink()

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
            r"x-pegasus:[\w\W]+pegasus: '5.0'[\w\W]+name:[\w\W]+hooks:[\w\W]+profiles:[\w\W]+metadata:[\w\W]+siteCatalog:[\w\W]+replicaCatalog:[\w\W]+transformationCatalog:[\w\W]+jobs:[\w\W]+jobDependencies:[\w\W]+"
        )
        assert p.match(result) is not None

    def test_plan_workflow_already_written(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        path = "wf.yml"
        wf.write(path).plan(**{"+property.key-_": "value"})

        assert wf._path == path

        Pegasus.client._client.Client.plan.assert_called_once_with(
            abstract_workflow=path,
            basename=None,
            cache=None,
            cleanup="inplace",
            cluster=None,
            conf=None,
            dir=None,
            force=False,
            force_replan=False,
            forward=None,
            inherited_rc_files=None,
            input_dirs=None,
            java_options=None,
            job_prefix=None,
            output_dir=None,
            output_sites=["local"],
            quiet=0,
            random_dir=False,
            relative_dir=None,
            relative_submit_dir=None,
            reuse=None,
            sites=None,
            staging_sites=None,
            submit=False,
            verbose=0,
            **{"+property.key-_": "value"},
        )

        os.remove(path)

    def test_Path_args_parsed_correctly_in_plan(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        path = "wf.yml"
        wf.write(path).plan(
            conf=Path("pegasus.properties"),
            input_dirs=["/path1", Path("/path2")],
            output_dir=Path("/output_dir"),
            relative_dir=Path("run1"),
            relative_submit_dir=Path("run1"),
            dir=Path("/dir"),
            reuse=["/submit_dir1", Path("/submit_dir2")],
            cache=["/cache", Path("/cache2")],
            inherited_rc_files=["/rc_file", Path("/rc_file2")],
            **{"pegasus.mode": "development"},
        )

        assert wf._path == path

        Pegasus.client._client.Client.plan.assert_called_once_with(
            abstract_workflow=path,
            basename=None,
            cache=["/cache", "/cache2"],
            cleanup="inplace",
            cluster=None,
            conf="pegasus.properties",
            dir="/dir",
            force=False,
            force_replan=False,
            forward=None,
            inherited_rc_files=["/rc_file", "/rc_file2"],
            input_dirs=["/path1", "/path2"],
            java_options=None,
            job_prefix=None,
            output_dir="/output_dir",
            output_sites=["local"],
            quiet=0,
            random_dir=False,
            relative_dir="run1",
            relative_submit_dir="run1",
            reuse=["/submit_dir1", "/submit_dir2"],
            sites=None,
            staging_sites=None,
            submit=False,
            verbose=0,
            **{"pegasus.mode": "development"},
        )

        os.remove(path)

    @pytest.mark.parametrize(
        "random_dir, expected_kwargs",
        [
            (
                True,
                {
                    "abstract_workflow": "wf.yml",
                    "basename": None,
                    "cache": None,
                    "cleanup": "inplace",
                    "cluster": None,
                    "conf": None,
                    "dir": None,
                    "force": False,
                    "force_replan": False,
                    "forward": None,
                    "inherited_rc_files": None,
                    "input_dirs": None,
                    "java_options": None,
                    "job_prefix": None,
                    "output_dir": None,
                    "output_sites": ["local"],
                    "quiet": 0,
                    "random_dir": True,
                    "relative_dir": None,
                    "relative_submit_dir": None,
                    "reuse": None,
                    "sites": None,
                    "staging_sites": None,
                    "submit": False,
                    "verbose": 0,
                },
            ),
            (
                "/path/to/dir",
                {
                    "abstract_workflow": "wf.yml",
                    "basename": None,
                    "cache": None,
                    "cleanup": "inplace",
                    "cluster": None,
                    "conf": None,
                    "dir": None,
                    "force": False,
                    "force_replan": False,
                    "forward": None,
                    "inherited_rc_files": None,
                    "input_dirs": None,
                    "java_options": None,
                    "job_prefix": None,
                    "output_dir": None,
                    "output_sites": ["local"],
                    "quiet": 0,
                    "random_dir": "/path/to/dir",
                    "relative_dir": None,
                    "relative_submit_dir": None,
                    "reuse": None,
                    "sites": None,
                    "staging_sites": None,
                    "submit": False,
                    "verbose": 0,
                },
            ),
            (
                Path("/path/to/dir"),
                {
                    "abstract_workflow": "wf.yml",
                    "basename": None,
                    "cache": None,
                    "cleanup": "inplace",
                    "cluster": None,
                    "conf": None,
                    "dir": None,
                    "force": False,
                    "force_replan": False,
                    "forward": None,
                    "inherited_rc_files": None,
                    "input_dirs": None,
                    "java_options": None,
                    "job_prefix": None,
                    "output_dir": None,
                    "output_sites": ["local"],
                    "quiet": 0,
                    "random_dir": "/path/to/dir",
                    "relative_dir": None,
                    "relative_submit_dir": None,
                    "reuse": None,
                    "sites": None,
                    "staging_sites": None,
                    "submit": False,
                    "verbose": 0,
                },
            ),
        ],
    )
    def test_random_dir_option_correctly_parsed_in_plan(
        self, wf, mocker, random_dir, expected_kwargs
    ):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        path = "wf.yml"
        wf.write(path).plan(random_dir=random_dir)

        assert wf._path == path

        Pegasus.client._client.Client.plan.assert_called_once_with(**expected_kwargs)

        os.remove(path)

    def test_plan_workflow_not_written(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")
        mocker.patch("Pegasus.client._client.Client.plan")

        wf.plan(
            dir="/dir",
            relative_dir="run1",
            input_dirs=["/dir1", "/dir2"],
            output_dir="/out",
        )

        assert wf._path == DEFAULT_WF_PATH

        Pegasus.client._client.Client.plan.assert_called_once_with(
            abstract_workflow=DEFAULT_WF_PATH,
            basename=None,
            cache=None,
            cleanup="inplace",
            cluster=None,
            conf=None,
            dir="/dir",
            force=False,
            force_replan=False,
            forward=None,
            inherited_rc_files=None,
            input_dirs=["/dir1", "/dir2"],
            java_options=None,
            job_prefix=None,
            output_dir="/out",
            output_sites=["local"],
            quiet=0,
            random_dir=False,
            relative_dir="run1",
            relative_submit_dir=None,
            reuse=None,
            sites=None,
            staging_sites=None,
            submit=False,
            verbose=0,
        )

        os.remove(DEFAULT_WF_PATH)

    def test_plan_workflow_and_access_braindump_file(self, wf, mocker):
        # shutil.which used in _client.from_env()
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        # create a fake temporary submit dir and braindump.yml file
        with TemporaryDirectory() as td, (Path(td) / "braindump.yml").open(
            "w+"
        ) as bd_file:
            yaml.dump({"user": "ryan", "submit_dir": "/submit_dir"}, bd_file)
            bd_file.seek(0)
            mocker.patch(
                "Pegasus.client._client.Client.plan", return_value=WorkflowInstance(td)
            )

            wf.plan()

            assert wf.braindump.user == "ryan"
            assert wf.braindump.submit_dir == Path("/submit_dir")

            os.remove(DEFAULT_WF_PATH)

    def test_access_braindump_file_before_workflow_planned(self):
        with pytest.raises(PegasusError) as e:
            wf = Workflow("test")
            wf.braindump

        assert (
            "requires a submit directory to be set; Workflow.plan() must be called prior"
            in str(e)
        )

    def test_run_and_access_run_output(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.run", return_value={"key": "value"})
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.run()

        assert wf.run_output == {"key": "value"}

        Pegasus.client._client.Client.run.assert_called_once_with(
            "submit_dir", verbose=0, grid=False
        )

    def test_wait(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.wait")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.wait()

        Pegasus.client._client.Client.wait.assert_called_once_with(
            "wf㒀", "submit_dir", delay=5
        )

    def test_run_with_grid_checking(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.run")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.run(grid=True)

        Pegasus.client._client.Client.run.assert_called_once_with(
            "submit_dir", verbose=0, grid=True
        )

    def test_access_run_output_before_workflow_run(self, wf):
        with pytest.raises(PegasusError) as e:
            wf.run_output

        assert "Workflow.run must be called before run_output can be accessed" in str(e)

    def test_status(self, wf, mocker):
        #mocker.patch("Pegasus.client._client.Client.status")
        mocker.patch("Pegasus.client.status.Status.fetch_status")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        wf.status()

        #Pegasus.client._client.Client.status.assert_called_once_with(
        #    wf._submit_dir, long=0, verbose=0
        #)
        Pegasus.client.status.Status.fetch_status.assert_called_once_with(wf._submit_dir, json=False, long=False)

    def test_get_status(self, wf, mocker):
        expected = {
            "totals": {
                "unready": 1,
                "ready": 1,
                "pre": 1,
                "queued": 1,
                "post": 1,
                "succeeded": 1,
                "failed": 1,
                "percent_done": 1.0,
                "total": 7,
            },
            "dags": {
                "root": {
                    "unready": 1,
                    "ready": 1,
                    "pre": 1,
                    "queued": 1,
                    "post": 1,
                    "succeeded": 1,
                    "failed": 1,
                    "percent_done": 1.0,
                    "state": "Running",
                    "dagname": "wf",
                }
            },
        }

        mocker.patch("Pegasus.client._client.Client.get_status", return_value=expected)

        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._submit_dir = "submit_dir"
        assert wf.get_status() == expected
        Pegasus.client._client.Client.get_status.assert_called_once_with(
            root_wf_name="wf㒀", submit_dir="submit_dir"
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

    def test_graph(self, wf, mocker):
        mocker.patch("Pegasus.client._client.Client.graph")
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        wf._path = "workflow.yml"
        wf.graph(
            include_files=True,
            no_simplify=True,
            label="label",
            output="wf.dot",
            remove=["tr1"],
            width=256,
            height=256,
        )

        Pegasus.client._client.Client.graph.assert_called_once_with(
            workflow_file="workflow.yml",
            include_files=True,
            no_simplify=True,
            label="label",
            output="wf.dot",
            remove=["tr1"],
            width=256,
            height=256,
        )

    def test_graph_workflow_not_yet_written(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        with pytest.raises(PegasusError) as e:
            wf.graph()

        assert "Workflow must be written" in str(e)

    def test_graph_invalid_label(self, wf, mocker):
        mocker.patch("shutil.which", return_value="/usr/bin/pegasus-version")

        # must be set so the label check can be reached
        wf._path = "workflow.yml"

        with pytest.raises(ValueError) as e:
            wf.graph(label="bad-label")

        assert "Invalid label: bad-label" in str(e)


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
    with pytest.raises(PegasusError) as e:
        obj.func_that_requires_submit_dir()
