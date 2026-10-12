from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from Pegasus.healer.fixes.overlay import build_overlay
from Pegasus.healer.models.fixes import AppliedOverlay, FixProposal
from Pegasus.healer.pegasus.adapter import AttemptState, JobAttemptRef, RetryReceipt, Verification


class FakePegasusRetryController:
    """
    Deterministic fake for unit, graph, and integration tests.

    Documents the contract the real DAGManRetryController must fulfil.
    """

    def __init__(
        self,
        next_instance_id: int = 2,
        simulate_success: bool = True,
        exit_code_on_failure: int = 137,
    ) -> None:
        self._next_instance_id = next_instance_id
        self._simulate_success = simulate_success
        self._exit_code_on_failure = exit_code_on_failure
        self._applied_overlays: dict[str, AppliedOverlay] = {}
        self._receipts: list[RetryReceipt] = []
        self._cancelled: list[str] = []

    async def inspect_attempt(self, ref: JobAttemptRef) -> AttemptState:
        return AttemptState(
            is_terminal=True,
            is_failed=True,
            exit_code=self._exit_code_on_failure,
            scheduler_reason="memory" if self._exit_code_on_failure == 137 else None,
        )

    async def apply_overlay(self, fix: FixProposal) -> AppliedOverlay:
        overlay = build_overlay(fix)
        self._applied_overlays[str(fix.fix_id)] = overlay
        return overlay

    async def authorize_retry(self, incident_id: UUID, fix_id: UUID) -> RetryReceipt:
        receipt = RetryReceipt(
            incident_id=str(incident_id),
            fix_id=str(fix_id),
            retry_job_instance_id=self._next_instance_id,
            authorized_at=datetime.now(timezone.utc).isoformat(),
        )
        self._receipts.append(receipt)
        self._next_instance_id += 1
        return receipt

    async def verify_retry_configuration(self, receipt: RetryReceipt) -> Verification:
        overlay = self._applied_overlays.get(receipt.fix_id)
        if overlay is None:
            return Verification(
                consumed_new_config=False,
                actual_configuration={},
                mismatch_reason="No overlay found for fix_id",
            )
        return Verification(
            consumed_new_config=True,
            actual_configuration=overlay.new_config,
        )

    async def cancel_pending_retry(self, incident_id: UUID) -> None:
        self._cancelled.append(str(incident_id))
