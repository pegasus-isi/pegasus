"""Unit tests for the deterministic rule classifier."""
from __future__ import annotations

import uuid
import pytest
from app.rules.classifier import classify
from app.models.context import (
    FailureContext, ResourceRequest, ResourceUsage, DataCheck
)
from app.models.diagnosis import FailureType


def _ctx(**kwargs) -> FailureContext:
    return FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id="wf-001",
        job_id="job_0",
        job_instance_id=1,
        **kwargs,
    )


# ── OUT_OF_MEMORY ─────────────────────────────────────────────────────────────

def test_oom_exit_137_and_signal_9():
    ctx = _ctx(exit_code=137, termination_signal=9)
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.OUT_OF_MEMORY
    assert diag.confidence >= 0.95
    assert diag.source == "RULE"


def test_oom_exit_137_and_memory_near_limit():
    ctx = _ctx(
        exit_code=137,
        requested_resources=ResourceRequest(memory_mb=8192),
        measured_resources=ResourceUsage(peak_memory_mb=7500),
    )
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.OUT_OF_MEMORY


def test_oom_scheduler_reason_and_near_limit():
    ctx = _ctx(
        scheduler_reason="Out of Memory: job removed",
        requested_resources=ResourceRequest(memory_mb=4096),
        measured_resources=ResourceUsage(peak_memory_mb=3900),
    )
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.OUT_OF_MEMORY


def test_single_oom_signal_no_mem_info_returns_none():
    """One OOM signal without supporting evidence → no rule fires."""
    ctx = _ctx(exit_code=137)
    diag = classify(ctx)
    assert diag is None


# ── DISK_EXCEEDED ─────────────────────────────────────────────────────────────

def test_disk_scheduler_reason():
    ctx = _ctx(scheduler_reason="Disk quota exceeded — job removed")
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.DISK_EXCEEDED


def test_disk_near_limit():
    ctx = _ctx(
        requested_resources=ResourceRequest(disk_mb=10240),
        measured_resources=ResourceUsage(disk_used_mb=9800),
    )
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.DISK_EXCEEDED


# ── WALLTIME_EXCEEDED ─────────────────────────────────────────────────────────

def test_walltime_scheduler_reason():
    ctx = _ctx(scheduler_reason="Job wall time limit exceeded")
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.WALLTIME_EXCEEDED


def test_walltime_near_limit():
    ctx = _ctx(
        requested_resources=ResourceRequest(runtime_seconds=3600),
        measured_resources=ResourceUsage(runtime_seconds=3500),
    )
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.WALLTIME_EXCEEDED


# ── TRANSIENT_INFRASTRUCTURE ──────────────────────────────────────────────────

def test_transient_node_failure():
    ctx = _ctx(scheduler_reason="Node failure detected — network disconnected")
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.TRANSIENT_INFRASTRUCTURE


def test_transient_eviction():
    ctx = _ctx(scheduler_reason="Job evicted by preemption")
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.TRANSIENT_INFRASTRUCTURE


# ── MISSING_INPUT ─────────────────────────────────────────────────────────────

def test_missing_input():
    ctx = _ctx(
        input_checks=[DataCheck(logical_filename="input.bam", exists=False, accessible=False)]
    )
    diag = classify(ctx)
    assert diag is not None
    assert diag.failure_type == FailureType.MISSING_INPUT
    assert "input:input.bam" in diag.evidence_ids


# ── No match ──────────────────────────────────────────────────────────────────

def test_no_match_returns_none():
    ctx = _ctx(exit_code=1, scheduler_reason="Unknown application error")
    diag = classify(ctx)
    assert diag is None
