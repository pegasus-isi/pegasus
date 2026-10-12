"""Unit tests for the FakePegasusRetryController."""
from __future__ import annotations

import uuid
import pytest
from app.pegasus.fake import FakePegasusRetryController
from app.pegasus.adapter import JobAttemptRef
from app.models.fixes import FixAction, FixProposal


def _proposal(incident_id: uuid.UUID) -> FixProposal:
    return FixProposal(
        incident_id=incident_id,
        action=FixAction.INCREASE_MEMORY,
        old_configuration={"memory_mb": 4096},
        proposed_configuration={"memory_mb": 6144},
        justification="test",
        confidence=0.97,
        source="RULE",
    )


@pytest.mark.asyncio
async def test_authorize_retry_increments_instance_id():
    ctrl = FakePegasusRetryController(next_instance_id=2)
    incident_id = uuid.uuid4()
    fix_id = uuid.uuid4()

    r1 = await ctrl.authorize_retry(incident_id, fix_id)
    r2 = await ctrl.authorize_retry(incident_id, fix_id)

    assert r1.retry_job_instance_id == 2
    assert r2.retry_job_instance_id == 3


@pytest.mark.asyncio
async def test_apply_overlay_stores_and_verifies():
    ctrl = FakePegasusRetryController()
    incident_id = uuid.uuid4()
    proposal = _proposal(incident_id)

    overlay = await ctrl.apply_overlay(proposal)
    assert overlay.configs_differ()

    receipt_fix_id = str(proposal.fix_id)
    # Fabricate a receipt
    from app.pegasus.adapter import RetryReceipt
    receipt = RetryReceipt(
        incident_id=str(incident_id),
        fix_id=receipt_fix_id,
        retry_job_instance_id=2,
        authorized_at="2026-08-23T00:00:00+00:00",
    )
    verification = await ctrl.verify_retry_configuration(receipt)
    assert verification.consumed_new_config is True
    assert verification.actual_configuration == {"memory_mb": 6144}


@pytest.mark.asyncio
async def test_verify_missing_overlay_returns_false():
    ctrl = FakePegasusRetryController()
    from app.pegasus.adapter import RetryReceipt
    receipt = RetryReceipt(
        incident_id="inc-001",
        fix_id="nonexistent-fix-id",
        retry_job_instance_id=2,
        authorized_at="2026-08-23T00:00:00+00:00",
    )
    verification = await ctrl.verify_retry_configuration(receipt)
    assert verification.consumed_new_config is False
