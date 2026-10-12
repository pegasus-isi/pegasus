"""
Graph routing tests — verify the two-loop conditional edges without live services.

Uses FakeLLMProvider and FakePegasusRetryController so no external
dependencies are required.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from app.graph.graph import build_graph
from app.graph.state import RemediationState
from app.utils.llm.fake import FakeLLMProvider
from app.utils.models.diagnosis import Diagnosis, FailureType
from app.utils.models.fixes import FixAction, FixOutcome
from app.utils.pegasus.fake import FakePegasusRetryController
from app.utils.policies.engine import PolicyEngine
from app.utils.policies.loader import PolicyConfig, load_policy


def _make_oom_event() -> dict[str, Any]:
    from app.utils.models.events import WorkflowEvent
    event = WorkflowEvent(
        event_id="evt-001",
        event_type="JOB_FAILED",
        workflow_id="wf-001",
        job_id="bowtie_0",
        job_instance_id=1,
        status=137,
        timestamp=datetime.now(timezone.utc),
    )
    return event.model_dump(mode="json")


def _make_services() -> dict[str, Any]:
    policy_config = load_policy("policies/remediation.yaml")
    policy_engine = PolicyEngine(policy_config, confidence_threshold=0.80)
    return {
        "policy": policy_config,
        "policy_engine": policy_engine,
        "retry_controller": FakePegasusRetryController(),
        "llm": FakeLLMProvider(),
        "submit_dir": None,
        "condor_classads": {
            "ExitCode": 137,
            "ExitBySignal": True,
            "ExitSignal": 9,
            "RequestMemory": 4096,
            "MemoryUsage": 4000,
            "HoldReason": "Out of Memory",
        },
    }


@pytest.mark.asyncio
async def test_graph_routes_oom_to_auto_and_applies_fix():
    """
    End-to-end graph test for the OOM happy path:
    collect_context → rule_classifier → fix_catalog → policy(AUTO) → apply_fix → authorize_retry
    """
    graph = build_graph().compile()
    services = _make_services()

    initial: RemediationState = {
        "incident_id": str(uuid.uuid4()),
        "workflow_id": "wf-001",
        "job_id": "bowtie_0",
        "source_job_instance_id": 1,
        "failure_event": _make_oom_event(),
        "attempt": 1,
        "errors": [],
    }

    final_state = await graph.ainvoke(initial, **services)

    # Should have diagnosed OOM
    assert final_state.get("diagnosis", {}).get("failure_type") == "OUT_OF_MEMORY"
    # Policy should be AUTO
    assert final_state.get("policy_decision") == "AUTO"
    # A fix should be proposed
    assert final_state.get("proposed_fix", {}).get("action") == "INCREASE_MEMORY"
    # Overlay applied
    assert final_state.get("overlay") is not None
    # Retry authorized
    assert final_state.get("retry_job_instance_id") is not None


@pytest.mark.asyncio
async def test_graph_escalates_on_low_confidence():
    """
    When rule classifier returns no match and LLM returns low confidence,
    the graph should escalate.
    """
    from app.utils.models.diagnosis import AlternativeCause

    low_confidence_diagnosis = Diagnosis(
        failure_type=FailureType.UNKNOWN,
        confidence=0.40,   # below threshold
        evidence_ids=[],
        explanation="unclear",
        source="LLM",
    )

    fake_llm = FakeLLMProvider(responses={"Diagnosis": low_confidence_diagnosis})
    graph = build_graph().compile()
    services = _make_services()
    services["llm"] = fake_llm
    services["condor_classads"] = {
        "ExitCode": 1,
        "RequestMemory": 4096,
    }

    initial: RemediationState = {
        "incident_id": str(uuid.uuid4()),
        "workflow_id": "wf-001",
        "job_id": "job_0",
        "source_job_instance_id": 1,
        "failure_event": _make_oom_event(),
        "attempt": 1,
        "errors": [],
    }

    final_state = await graph.ainvoke(initial, **services)
    assert final_state.get("terminal") is True


@pytest.mark.asyncio
async def test_graph_stops_on_attempt_limit():
    """
    AC-07: Repeated ineffective fixes must converge to STOP (not loop forever).
    """
    from app.utils.models.context import FixSummary

    graph = build_graph().compile()
    services = _make_services()

    # Simulate being on attempt 4 (max for OOM is 3)
    initial: RemediationState = {
        "incident_id": str(uuid.uuid4()),
        "workflow_id": "wf-001",
        "job_id": "bowtie_0",
        "source_job_instance_id": 1,
        "failure_event": _make_oom_event(),
        "attempt": 4,   # exceeds max_attempts=3
        "errors": [],
        "context": {
            "incident_id": str(uuid.uuid4()),
            "workflow_id": "wf-001",
            "job_id": "bowtie_0",
            "job_instance_id": 1,
            "attempt_number": 4,
            "previous_fixes": [],
            "previous_diagnoses": [],
            "requested_resources": {"memory_mb": 4096},
            "measured_resources": {"peak_memory_mb": 4000},
            "input_checks": [],
            "output_checks": [],
            "missing_evidence": [],
        },
    }

    final_state = await graph.ainvoke(initial, **services)
    assert final_state.get("policy_decision") in ("STOP", "ESCALATE") or \
           final_state.get("terminal") is True
