"""Unit tests for the fix catalog."""
from __future__ import annotations

import uuid
import pytest
from app.fixes.catalog import lookup
from app.policies.loader import load_policy
from app.models.context import FailureContext, ResourceRequest
from app.models.diagnosis import Diagnosis, FailureType
from app.models.fixes import FixAction


def _ctx(memory_mb: int = 4096, disk_mb: int = 10240) -> FailureContext:
    return FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id="wf-001",
        job_id="job_0",
        job_instance_id=1,
        requested_resources=ResourceRequest(
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            runtime_seconds=3600,
        ),
    )


def _diag(ftype: FailureType) -> Diagnosis:
    return Diagnosis(
        failure_type=ftype,
        confidence=0.97,
        evidence_ids=["exit_code:137"],
        explanation="test",
        source="RULE",
    )


policy = load_policy("policies/remediation.yaml")


def test_oom_catalog_returns_increase_memory():
    ctx = _ctx(memory_mb=4096)
    proposal = lookup(_diag(FailureType.OUT_OF_MEMORY), ctx, policy)
    assert proposal is not None
    assert proposal.action == FixAction.INCREASE_MEMORY
    # 4096 × 1.5 = 6144
    assert proposal.proposed_configuration["memory_mb"] == 6144
    assert proposal.old_configuration["memory_mb"] == 4096
    assert proposal.requires_approval is False


def test_oom_catalog_respects_ceiling():
    ctx = _ctx(memory_mb=25000)  # 25 000 × 1.5 = 37 500 > 32 768 ceiling
    proposal = lookup(_diag(FailureType.OUT_OF_MEMORY), ctx, policy)
    assert proposal is not None
    assert proposal.proposed_configuration["memory_mb"] == 32768


def test_disk_catalog_returns_increase_disk():
    ctx = _ctx()
    proposal = lookup(_diag(FailureType.DISK_EXCEEDED), ctx, policy)
    assert proposal is not None
    assert proposal.action == FixAction.INCREASE_DISK


def test_transient_catalog_no_action_retry():
    ctx = _ctx()
    proposal = lookup(_diag(FailureType.TRANSIENT_INFRASTRUCTURE), ctx, policy)
    assert proposal is not None
    assert proposal.action == FixAction.NO_ACTION_TRANSIENT_RETRY
    assert proposal.old_configuration == {}
    assert proposal.proposed_configuration == {}


def test_application_error_returns_none():
    """APPLICATION_ERROR has no catalog entry → falls through to LLM planner."""
    ctx = _ctx()
    proposal = lookup(_diag(FailureType.APPLICATION_ERROR), ctx, policy)
    assert proposal is None


def test_overlay_hashes_differ():
    from app.fixes.overlay import build_overlay
    ctx = _ctx(memory_mb=4096)
    proposal = lookup(_diag(FailureType.OUT_OF_MEMORY), ctx, policy)
    assert proposal is not None
    overlay = build_overlay(proposal)
    assert overlay.hash_before != overlay.hash_after
    assert overlay.configs_differ()


def test_overlay_hashes_same_for_no_change():
    from app.fixes.overlay import build_overlay
    from app.models.fixes import FixProposal
    proposal = FixProposal(
        incident_id=uuid.uuid4(),
        action=FixAction.NO_ACTION_TRANSIENT_RETRY,
        old_configuration={},
        proposed_configuration={},
        justification="transient",
        confidence=0.90,
        source="RULE",
    )
    overlay = build_overlay(proposal)
    assert overlay.hash_before == overlay.hash_after
    assert not overlay.configs_differ()
