"""Unit tests for the AMQP event normaliser."""
from __future__ import annotations

import pytest
from app.events.schemas import normalize_event
from app.models.events import WorkflowEvent


def test_normalise_oom_failure():
    payload = {
        "wf_id": "wf-001",
        "job_id": "bowtie_0",
        "job_inst_id": 1,
        "sched_id": "12345.0",
        "exitcode": 137,
        "status": 1,
        "ts": 1724400000.0,
    }
    event = normalize_event("stampede.job_inst.main.failure", payload)
    assert event is not None
    assert event.event_type == "JOB_FAILED"
    assert event.workflow_id == "wf-001"
    assert event.job_id == "bowtie_0"
    assert event.job_instance_id == 1
    assert event.status == 1


def test_normalise_success():
    payload = {
        "wf_id": "wf-001",
        "job_id": "bowtie_0",
        "job_inst_id": 2,
        "exitcode": 0,
        "status": 0,
        "ts": 1724400600.0,
    }
    event = normalize_event("stampede.job_inst.main.success", payload)
    assert event is not None
    assert event.event_type == "JOB_SUCCEEDED"
    assert event.is_failure is False


def test_normalise_inv_nonzero_exit():
    """stampede.inv.end with non-zero exit → JOB_FAILED."""
    payload = {
        "wf_id": "wf-001",
        "job_id": "job_0",
        "job_inst_id": 1,
        "exitcode": 1,
        "ts": 1724400000.0,
    }
    event = normalize_event("stampede.inv.end", payload)
    assert event is not None
    assert event.event_type == "JOB_FAILED"


def test_normalise_unknown_routing_key_returns_none():
    event = normalize_event("some.unknown.key", {"wf_id": "x"})
    assert event is None


def test_event_id_is_stable():
    payload = {"wf_id": "wf-001", "job_inst_id": 1, "exitcode": 137, "ts": 100.0}
    id1 = WorkflowEvent.make_event_id(payload)
    id2 = WorkflowEvent.make_event_id(payload)
    assert id1 == id2


def test_event_id_differs_on_different_payload():
    p1 = {"wf_id": "wf-001", "job_inst_id": 1}
    p2 = {"wf_id": "wf-001", "job_inst_id": 2}
    assert WorkflowEvent.make_event_id(p1) != WorkflowEvent.make_event_id(p2)
