"""Unit tests for the deterministic policy engine."""
from __future__ import annotations

import uuid
import pytest
from app.policies.engine import PolicyEngine
from app.policies.loader import PolicyConfig, load_policy
from app.models.context import FailureContext, ResourceRequest, FixSummary
from app.models.diagnosis import Diagnosis, FailureType
from app.models.fixes import FixProposal, FixAction, PolicyDecision


def _policy() -> PolicyConfig:
    return load_policy("policies/remediation.yaml")


def _ctx(attempt: int = 1, previous_fixes: list[FixSummary] | None = None) -> FailureContext:
    return FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id="wf-001",
        job_id="job_0",
        job_instance_id=1,
        attempt_number=attempt,
        requested_resources=ResourceRequest(memory_mb=4096),
        previous_fixes=previous_fixes or [],
    )


def _oom_diagnosis(confidence: float = 0.97) -> Diagnosis:
    return Diagnosis(
        failure_type=FailureType.OUT_OF_MEMORY,
        confidence=confidence,
        evidence_ids=["exit_code:137"],
        explanation="OOM",
        source="RULE",
    )


def _oom_proposal(ctx: FailureContext) -> FixProposal:
    return FixProposal(
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_MEMORY,
        old_configuration={"memory_mb": 4096},
        proposed_configuration={"memory_mb": 6144},
        justification="OOM fix",
        confidence=0.97,
        source="RULE",
    )


# ── AUTO path ─────────────────────────────────────────────────────────────────

def test_oom_auto_decision():
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = _oom_diagnosis()
    proposal = _oom_proposal(ctx)
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.AUTO
    assert not record.checks_failed


# ── STOP cases ────────────────────────────────────────────────────────────────

def test_stop_on_low_confidence():
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = _oom_diagnosis(confidence=0.50)
    proposal = _oom_proposal(ctx)
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.ESCALATE


def test_stop_on_attempt_limit_exceeded():
    engine = PolicyEngine(_policy())
    ctx = _ctx(attempt=4)  # max is 3 for OOM
    diag = _oom_diagnosis()
    proposal = _oom_proposal(ctx)
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.STOP
    assert any("attempt_limit" in c for c in record.checks_failed)


def test_stop_on_ceiling_exceeded():
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = _oom_diagnosis()
    proposal = FixProposal(
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_MEMORY,
        old_configuration={"memory_mb": 4096},
        proposed_configuration={"memory_mb": 99999},  # exceeds 32768 ceiling
        justification="OOM",
        confidence=0.97,
        source="RULE",
    )
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.STOP
    assert any("memory_mb" in c for c in record.checks_failed)


def test_stop_on_repeated_ineffective_fix():
    engine = PolicyEngine(_policy())
    prev = FixSummary(fix_id="f-001", action="INCREASE_MEMORY", outcome="INEFFECTIVE", attempt_number=1)
    ctx = _ctx(attempt=2, previous_fixes=[prev])
    diag = _oom_diagnosis()
    proposal = _oom_proposal(ctx)
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.STOP
    assert any("repeated_ineffective_fix" in c for c in record.checks_failed)


def test_stop_on_no_configuration_change():
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = _oom_diagnosis()
    proposal = FixProposal(
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_MEMORY,
        old_configuration={"memory_mb": 4096},
        proposed_configuration={"memory_mb": 4096},  # identical — no change
        justification="same",
        confidence=0.97,
        source="RULE",
    )
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.STOP


def test_stop_on_requires_human_review():
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = Diagnosis(
        failure_type=FailureType.APPLICATION_ERROR,
        confidence=0.85,
        evidence_ids=[],
        explanation="app error",
        requires_human_review=True,
        source="LLM",
    )
    proposal = FixProposal(
        incident_id=ctx.incident_id,
        action=FixAction.HUMAN_REVIEW,
        justification="needs review",
        confidence=0.85,
        source="LLM",
    )
    record = engine.validate(proposal, diag, ctx)
    assert record.decision == PolicyDecision.ASK


# ── AC-06: unknown failure cannot trigger unsafe automatic action ──────────────

def test_unknown_failure_escalates():
    """AC-06: Unknown failures must not trigger AUTO."""
    engine = PolicyEngine(_policy())
    ctx = _ctx()
    diag = Diagnosis(
        failure_type=FailureType.UNKNOWN,
        confidence=0.90,
        evidence_ids=[],
        explanation="unknown",
        source="LLM",
    )
    proposal = FixProposal(
        incident_id=ctx.incident_id,
        action=FixAction.HUMAN_REVIEW,
        justification="unknown failure",
        confidence=0.90,
        source="LLM",
        requires_approval=True,
    )
    record = engine.validate(proposal, diag, ctx)
    assert record.decision in (PolicyDecision.ASK, PolicyDecision.ESCALATE, PolicyDecision.STOP)
    assert record.decision != PolicyDecision.AUTO
