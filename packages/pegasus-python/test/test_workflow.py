import json
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest

import Pegasus
from Pegasus import yaml
from Pegasus.api.mixins import EventType
from Pegasus.api.replica_catalog import File, ReplicaCatalog
from Pegasus.api.site_catalog import SiteCatalog
from Pegasus.api.transformation_catalog import TransformationCatalog
from Pegasus.api.workflow import Job, SubWorkflow, Workflow
from Pegasus.api.writable import _CustomEncoder
from Pegasus.workflow import _to_wf, dump, dumps, load, loads


def sort_parts(wf):
    """Sort jobs and job uses to make test deterministic"""
    for job in wf["jobs"]:
        job["uses"].sort(key=lambda u: u["lfn"])

    wf["jobs"].sort(key=lambda j: j["id"])

    return wf


@pytest.fixture(scope="module")
def wf1():
    _in = File("in", size=2048).add_metadata(createdBy="ryan")
    stdin = File("stdin").add_metadata(size=1024)
    stdout = File("stdout").add_metadata(size=1024)
    stderr = File("stderr").add_metadata(size=1024)
    out = File("out").add_metadata(size=1024)
    out2 = File("out2").add_metadata(size=1024)

    pegasus_workflow = File("pegasus_workflow").add_metadata(size=2048)
    condor_workflow = File("condor_workflow").add_metadata(size=2048)

    j1 = (
        Job("tr", _id="1", node_label="test")
        .add_args("-i", _in, "-o", out, "-n", 1, 1.1)
        .add_inputs(_in)
        .add_outputs(out)
        .set_stdin(stdin)
        .set_stdout(stdout)
        .set_stderr(stderr)
        .add_shell_hook(EventType.START, "/cmd2")
        .add_env(JAVA_HOME="/usr/bin/java")
        .add_metadata(xtra_info="123")
    )

    j2 = (
        Job("tr2", _id="2", node_label="test")
        .add_args("-i", out, "-o", out2)
        .add_inputs(out)
        .add_outputs(out2)
    )

    sbwf_pegasus = SubWorkflow(
        pegasus_workflow, False, _id="unplanned", node_label="test"
    ).add_args("-flag", "-flag2")

    sbwf_condor = SubWorkflow(condor_workflow, True, _id="planned", node_label="test")

    return (
        Workflow("test", infer_dependencies=False)
        .add_shell_hook(EventType.START, "/cmd")
        .add_dagman_profile(retry=1)
        .add_metadata(author="ryan")
        .add_jobs(j1, j2, sbwf_pegasus, sbwf_condor)
        .add_dependency(j1, children=[j2])
    )


@pytest.fixture(scope="module")
def wf2():
    _in = File("in")
    stdin = File("stdin")
    stdout = File("stdout")
    stderr = File("stderr")
    out = File("out")
    out2 = File("out2")

    pegasus_workflow = File("pegasus_workflow")
    condor_workflow = File("condor_workflow")

    j1 = (
        Job("tr", _id="1", node_label="test")
        .add_args("-i", _in, "-o", out)
        .add_inputs(_in)
        .add_outputs(out)
        .set_stdin(stdin)
        .set_stdout(stdout)
        .set_stderr(stderr)
    )

    j2 = (
        Job("tr2", _id="2", node_label="test")
        .add_args("-i", out, "-o", out2)
        .add_inputs(out)
        .add_outputs(out2)
    )

    sbwf_pegasus = SubWorkflow(
        pegasus_workflow, False, _id="unplanned", node_label="test"
    ).add_args("-flag", "-flag2")

    sbwf_condor = SubWorkflow(condor_workflow, True, _id="planned", node_label="test")

    return (
        Workflow("test", infer_dependencies=False)
        .add_jobs(j1, j2, sbwf_pegasus, sbwf_condor)
        .add_dependency(j1, children=[j2])
    )


@pytest.fixture(scope="module")
def wf3():
    wf = Workflow("test")
    wf.add_jobs(Job("ls"))
    wf.add_site_catalog(SiteCatalog())
    wf.add_transformation_catalog(TransformationCatalog())
    wf.add_replica_catalog(ReplicaCatalog())

    return wf


def test_to_wf_with_optional_args_set(wf1):
    expected = json.loads(json.dumps(wf1, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_wf(expected), cls=_CustomEncoder))

    assert sort_parts(result) == sort_parts(expected)


def test_to_wf_without_optional_args(wf2):
    expected = json.loads(json.dumps(wf2, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_wf(expected), cls=_CustomEncoder))

    assert sort_parts(result) == sort_parts(expected)


def test_to_wf_with_catalogs_included(wf3):
    expected = json.loads(json.dumps(wf3, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_wf(expected), cls=_CustomEncoder))

    assert result == expected


@pytest.mark.parametrize("_format", [("yml"), ("json")])
def test_load(wf1, _format):
    # write to tempfile as _format
    with TemporaryFile(mode="w+") as f:
        wf1.write(f, _format=_format)
        f.seek(0)

        # load into new wf object
        new_wf = load(f)

    # assert that what was loaded is equal to original
    result = json.loads(json.dumps(new_wf, cls=_CustomEncoder))
    expected = json.loads(json.dumps(wf1, cls=_CustomEncoder))

    assert sort_parts(result) == sort_parts(expected)


def test_loads_json(wf1):
    # dump wf1 to str, then load into new wf
    new_wf = loads(json.dumps(wf1, cls=_CustomEncoder))

    # assert that what was loaded is equal to the original
    result = json.loads(json.dumps(new_wf, cls=_CustomEncoder))
    expected = json.loads(json.dumps(wf1, cls=_CustomEncoder))

    assert sort_parts(result) == sort_parts(expected)


def test_loads_yaml(wf1):
    # dump wf1 to str, then load into new wf
    new_wf = loads(yaml.dump(json.loads(json.dumps(wf1, cls=_CustomEncoder))))

    # assert that what was loaded is equal to the original
    result = json.loads(json.dumps(new_wf, cls=_CustomEncoder))
    expected = json.loads(json.dumps(wf1, cls=_CustomEncoder))

    assert sort_parts(result) == sort_parts(expected)


def test_dump(mocker, wf1):
    mocker.patch("Pegasus.api.workflow.Workflow.write")
    with NamedTemporaryFile(mode="w") as f:
        dump(wf1, f, _format="yml")
        Pegasus.api.workflow.Workflow.write.assert_called_once_with(f, _format="yml")


def test_dumps(wf1):
    expected = json.loads(json.dumps(wf1, cls=_CustomEncoder))

    result = yaml.load(dumps(wf1))
    # file info not needed for test
    del result["x-pegasus"]

    assert result == expected
