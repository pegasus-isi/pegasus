"""Schema/contract tests — every Pydantic model round-trips cleanly."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
import pytest
from app.models.events import WorkflowEvent
from app.models.context import FailureContext, ResourceRequest, ResourceUsage
from app.models.diagnosis import Diagnosis, FailureType
from app.models.fixes import FixAction, FixProposal, FixOutcome, PolicyDecision


def test_workflow_event_round_trip():
    event = WorkflowEvent(
        event_id="abc123",
        event_type="JOB_FAILED",
        workflow_id="wf-001",
        job_id="job_0",
        job_instance_id=1,
        status=137,
        timestamp=datetime.now(timezone.utc),
    )
    data = event.model_dump(mode="json")
    reloaded = WorkflowEvent.model_validate(data)
    assert reloaded.event_id == event.event_id
    assert reloaded.event_type == event.event_type


def test_failure_context_round_trip():
    ctx = FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id="wf-001",
        job_id="job_0",
        job_instance_id=1,
        exit_code=137,
        termination_signal=9,
        requested_resources=ResourceRequest(memory_mb=4096, cpus=4),
        measured_resources=ResourceUsage(peak_memory_mb=4000),
    )
    data = ctx.model_dump(mode="json")
    reloaded = FailureContext.model_validate(data)
    assert reloaded.incident_id == ctx.incident_id
    assert reloaded.exit_code == 137


def test_diagnosis_round_trip():
    diag = Diagnosis(
        failure_type=FailureType.OUT_OF_MEMORY,
        confidence=0.97,
        evidence_ids=["exit_code:137", "signal:9"],
        explanation="OOM detected",
        source="RULE",
        rule_version="1.0.0",
    )
    data = diag.model_dump(mode="json")
    reloaded = Diagnosis.model_validate(data)
    assert reloaded.failure_type == FailureType.OUT_OF_MEMORY
    assert reloaded.confidence == 0.97


def test_fix_proposal_round_trip():
    proposal = FixProposal(
        incident_id=uuid.uuid4(),
        action=FixAction.INCREASE_MEMORY,
        old_configuration={"memory_mb": 4096},
        proposed_configuration={"memory_mb": 6144},
        justification="OOM fix",
        confidence=0.97,
        source="RULE",
    )
    data = proposal.model_dump(mode="json")
    reloaded = FixProposal.model_validate(data)
    assert reloaded.action == FixAction.INCREASE_MEMORY
    assert reloaded.proposed_configuration["memory_mb"] == 6144


def test_fix_outcome_values():
    for val in ("EFFECTIVE", "INEFFECTIVE", "PARTIALLY_EFFECTIVE", "NEW_FAILURE", "INCONCLUSIVE"):
        assert FixOutcome(val) is not None


def test_policy_decision_values():
    for val in ("AUTO", "ASK", "STOP", "ESCALATE"):
        assert PolicyDecision(val) is not None


def test_diagnosis_confidence_bounds():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Diagnosis(
            failure_type=FailureType.UNKNOWN,
            confidence=1.5,  # > 1.0 → invalid
            evidence_ids=[],
            explanation="bad",
            source="RULE",
        )
