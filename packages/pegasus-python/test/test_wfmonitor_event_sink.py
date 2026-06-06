"""Tests for WorkflowMonitorEventSink (live workflow-monitor JSONL output).

The sink translates the stampede event stream monitord dispatches into the
native record schema the workflow-monitor tool consumes, writing one JSON
object per line.  These tests drive the sink exactly as
``Workflow.output_to_db`` does — ``send(event, kwargs)`` with the same key
conventions (``__`` separators on dynamic events, ``.id``->``__id`` remapped
keys on static-bp events) — and assert the emitted records.
"""

import json
import logging

import pytest

from Pegasus.monitoring.event_output import WorkflowMonitorEventSink

WF = "ea17e8ac-0000-0000-0000-000000000001"


@pytest.fixture(autouse=True)
def _trace_level():
    """Install the custom TRACE log level the sinks use.

    In production this is done by ``Pegasus.tools.utils.configureLogging`` at
    monitord startup; replicate just the level install here (without touching
    root handlers) so ``self._log.trace(...)`` works under bare pytest.
    """
    cls = logging.getLoggerClass()
    if not hasattr(cls, "trace"):
        logging.TRACE = logging.DEBUG - 1
        logging.addLevelName(logging.TRACE, "TRACE")
        cls.trace = lambda self, msg, *a, **k: self.log(logging.TRACE, msg, *a, **k)
    yield


def _read(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def _drive(path):
    """Emit a small but representative workflow through the sink."""
    sink = WorkflowMonitorEventSink(str(path), restart=True)

    def send(event, **kw):
        kw.setdefault("xwf__id", WF)
        sink.send(event, kw)

    send(
        "wf.plan",
        ts=1000,
        dax_label="blackdiamond",
        user="bob",
        planner_version="5.2.0",
        submit_dir="/runs/run0001",
    )
    send("job.info", **{"job__id": "preprocess_ID0000001", "type_desc": "compute"})
    send(
        "job.info",
        **{"job__id": "create_dir_blackdiamond_0_local", "type_desc": "create-dir"},
    )
    send(
        "task.info",
        **{
            "task__id": "j1",
            "transformation": "pegasus::preprocess:4.0",
            "argv": " -a preprocess ",
        },
    )
    send("wf.map.task_job", **{"job__id": "preprocess_ID0000001", "task__id": "j1"})
    send("static.end")
    send("xwf.start", ts=1001, restart_count=0)

    # compute job lifecycle (with postscript): submit -> execute -> term ->
    # inv.end (maxrss) -> main.end (success)
    send("job_inst.submit.start", **{"job__id": "preprocess_ID0000001", "ts": 1002})
    send(
        "job_inst.submit.end",
        **{"job__id": "preprocess_ID0000001", "ts": 1002, "status": 0},
    )
    send(
        "job_inst.main.start",
        **{
            "job__id": "preprocess_ID0000001",
            "ts": 1003,
            "stdout__file": "00/00/pre.out.000",
            "stderr__file": "00/00/pre.err.000",
        },
    )
    send(
        "job_inst.main.term",
        **{"job__id": "preprocess_ID0000001", "ts": 1010, "status": 0},
    )
    send(
        "inv.end",
        **{
            "job__id": "preprocess_ID0000001",
            "ts": 1010,
            "inv__id": 1,
            "maxrss": 27792,
        },
    )
    send(
        "job_inst.main.end",
        **{
            "job__id": "preprocess_ID0000001",
            "ts": 1010,
            "status": 0,
            "exitcode": "0",
        },
    )
    # a failing submit on the aux job
    send(
        "job_inst.submit.end",
        **{"job__id": "create_dir_blackdiamond_0_local", "ts": 1004, "status": -1},
    )
    send("xwf.end", ts=1100, status=0, restart_count=0)
    sink.close()


def test_emits_native_workflow_monitor_records(tmp_path):
    path = tmp_path / "monitord-events.jsonl"
    _drive(path)
    recs = _read(path)

    # header first, with wf_uuid stamped on every record
    assert recs[0]["event_type"] == "workflow_start"
    assert recs[0]["dax_label"] == "blackdiamond"
    assert all(r["wf_uuid"] == WF for r in recs)

    jobs_init = [r for r in recs if r["event_type"] == "jobs_init"][0]
    assert jobs_init["total_jobs"] == 2
    pj = next(
        j for j in jobs_init["jobs"] if j["exec_job_id"] == "preprocess_ID0000001"
    )
    assert pj["type_desc"] == "compute"
    assert pj["transformation"] == "pegasus::preprocess:4.0"
    assert "task_argv" in pj
    aux = next(
        j
        for j in jobs_init["jobs"]
        if j["exec_job_id"] == "create_dir_blackdiamond_0_local"
    )
    assert "transformation" not in aux  # aux jobs have no mapped task


def test_workflow_state_transitions(tmp_path):
    path = tmp_path / "monitord-events.jsonl"
    _drive(path)
    recs = _read(path)
    states = [r for r in recs if r["event_type"] == "workflow_state"]
    assert [s["state"] for s in states] == ["WORKFLOW_STARTED", "WORKFLOW_TERMINATED"]
    assert states[0]["wf_start"] == 1001
    assert states[1]["wf_end"] == 1100
    assert states[1]["status"] == 0


@pytest.mark.parametrize(
    "exec_job_id, state",
    [
        ("preprocess_ID0000001", "SUBMIT"),
        ("preprocess_ID0000001", "EXECUTE"),
        ("preprocess_ID0000001", "JOB_TERMINATED"),
        ("preprocess_ID0000001", "JOB_SUCCESS"),
        ("create_dir_blackdiamond_0_local", "SUBMIT_FAILED"),
    ],
)
def test_job_state_strings_match_stampede(tmp_path, exec_job_id, state):
    """State strings must equal the stampede jobstate column values."""
    path = tmp_path / "monitord-events.jsonl"
    _drive(path)
    recs = _read(path)
    assert any(
        r["event_type"] == "job_state"
        and r["exec_job_id"] == exec_job_id
        and r["state"] == state
        for r in recs
    ), f"missing job_state {exec_job_id}/{state}"


def test_enrichment_carried_forward(tmp_path):
    """maxrss (from inv.end) and exitcode/stdout (from main.*) attach to the
    terminal job_state row — the row a consumer uses for final disposition."""
    path = tmp_path / "monitord-events.jsonl"
    _drive(path)
    recs = _read(path)
    success = [
        r
        for r in recs
        if r["event_type"] == "job_state"
        and r["exec_job_id"] == "preprocess_ID0000001"
        and r["state"] == "JOB_SUCCESS"
    ][0]
    assert success["exitcode"] == 0
    assert success["maxrss"] == 27792
    assert success["stdout_file"] == "00/00/pre.out.000"

    # synthetic integer job_id is stable across a job's transitions
    ids = {
        r["job_id"]
        for r in recs
        if r["event_type"] == "job_state" and r["exec_job_id"] == "preprocess_ID0000001"
    }
    assert len(ids) == 1


def test_restart_truncates(tmp_path):
    """restart=True (monitord --replay / recovery) must start a fresh file."""
    path = tmp_path / "monitord-events.jsonl"
    path.write_text('{"event_type": "stale"}\n')
    sink = WorkflowMonitorEventSink(str(path), restart=True)
    sink.send("wf.plan", {"xwf__id": WF, "ts": 1, "dax_label": "d"})
    sink.close()
    recs = _read(path)
    assert all(r.get("event_type") != "stale" for r in recs)
    assert recs[0]["event_type"] == "workflow_start"
