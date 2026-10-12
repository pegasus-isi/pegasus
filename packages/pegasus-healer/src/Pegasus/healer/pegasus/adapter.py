from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable
from uuid import UUID

from Pegasus.healer.models.fixes import AppliedOverlay, FixProposal


@dataclass
class JobAttemptRef:
    workflow_id: str
    job_id: str
    job_instance_id: int


@dataclass
class AttemptState:
    is_terminal: bool
    is_failed: bool
    exit_code: int | None = None
    scheduler_reason: str | None = None
    scheduler_state: str | None = None


@dataclass
class RetryReceipt:
    """Proof that exactly one retry was authorized."""
    incident_id: str
    fix_id: str
    retry_job_instance_id: int
    authorized_at: str  # ISO-8601 UTC


@dataclass
class Verification:
    """Result of checking whether the retry consumed the applied overlay."""
    consumed_new_config: bool
    actual_configuration: dict = field(default_factory=dict)
    mismatch_reason: str | None = None


@runtime_checkable
class RetryController(Protocol):
    """
    Adapter interface for Pegasus/HTCondor retry control.

    The real implementation (DAGManRetryController) patches .sub files
    in-place before the POST script exits so DAGMan retries with new resources.

    The fake implementation (FakePegasusRetryController) is used in all tests.
    """

    async def inspect_attempt(self, ref: JobAttemptRef) -> AttemptState:
        """Return the current terminal state of a job attempt."""
        ...

    async def apply_overlay(self, fix: FixProposal) -> AppliedOverlay:
        """Write the execution overlay (resource profile change) for the next retry."""
        ...

    async def authorize_retry(self, incident_id: UUID, fix_id: UUID) -> RetryReceipt:
        """Release exactly ONE retry attempt and return its new job_instance_id."""
        ...

    async def verify_retry_configuration(self, receipt: RetryReceipt) -> Verification:
        """Confirm the retry actually consumed the new configuration."""
        ...

    async def cancel_pending_retry(self, incident_id: UUID) -> None:
        """Cancel a pending retry (e.g. when escalated or rejected)."""
        ...
