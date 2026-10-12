from __future__ import annotations

"""
Scheduler-agnostic data models and client Protocol.

Any scheduler backend (HTCondor, SLURM, ...) implements SchedulerClient.
The rest of the codebase talks only to this interface — it never imports
condor_* or slurm-specific symbols directly.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

JobState = Literal["idle", "running", "held", "completed", "removed", "unknown"]


@dataclass
class JobStatus:
    """Current state of a queued or running job."""
    state: JobState
    exit_code: int | None = None
    exit_signal: int | None = None
    memory_usage_mb: int | None = None
    disk_usage_mb: int | None = None
    wall_time_seconds: float | None = None
    hold_reason: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_idle(self) -> bool:
        return self.state == "idle"

    @property
    def is_running(self) -> bool:
        return self.state == "running"

    @property
    def is_active(self) -> bool:
        """True when the job is in the queue (idle or running)."""
        return self.state in ("idle", "running")


@dataclass
class JobHistory:
    """Terminal state of a completed/removed job, from scheduler history."""
    exit_code: int | None = None
    exit_signal: int | None = None
    memory_usage_mb: int | None = None
    disk_usage_mb: int | None = None
    wall_time_seconds: float | None = None
    hold_reason: str | None = None
    hold_reason_code: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionInfo:
    """
    Metadata about a SLURM partition (queue).

    Used by the healer to validate that a proposed memory increase fits
    within the current partition's ceiling, or to select a larger partition
    when it does not.
    """
    name: str
    max_mem_mb: int | None = None       # MaxMemPerNode in MB; None = unlimited
    max_time_seconds: int | None = None # MaxTime in seconds; None = unlimited
    accessible: bool = True             # State=UP and user has submission rights
    default_mem_mb: int | None = None   # DefMemPerNode in MB; None = unlimited
    raw: dict[str, Any] = field(default_factory=dict)

    def fits_memory(self, needed_mb: int) -> bool:
        """True when needed_mb is within this partition's ceiling."""
        if self.max_mem_mb is None:
            return True          # unlimited — always fits
        return needed_mb <= self.max_mem_mb


@runtime_checkable
class SchedulerClient(Protocol):
    """
    Scheduler-agnostic interface for job control.

    Implementations
    ───────────────
      HTCondorClient  →  condor_q / condor_qedit / condor_history / condor_rm
      SLURMClient     →  squeue / scontrol / sacct / scancel

    All methods are synchronous — they are called from the POST script
    (a short-lived subprocess) and from synchronous collectors.
    """

    def get_job_status(self, cluster_id: str) -> JobStatus | None:
        """
        Return the current status of a queued or running job.
        Returns None when the job is not found in the active queue.
        """
        ...

    def update_job_resources(self, cluster_id: str, resources: dict[str, Any]) -> bool:
        """
        Update resource requests for an IDLE job without cancelling it.

        resources keys (FixProposal.proposed_configuration):
          memory_mb, disk_mb, cpus, runtime_seconds

        HTCondor: condor_qedit <cluster_id> RequestMemory <mb>
        SLURM:    scontrol update JobId=<id> MinMemoryNode=<mb>M

        Returns True on success, False on partial/total failure.
        """
        ...

    def get_job_history(self, cluster_id: str) -> JobHistory:
        """
        Query the scheduler history for a completed or removed job.

        HTCondor: condor_history <cluster_id> -long
        SLURM:    sacct -j <cluster_id>

        Returns an empty JobHistory when the job is not found.
        """
        ...

    def cancel_job(self, cluster_id: str) -> bool:
        """
        Cancel a queued or running job.

        HTCondor: condor_rm <cluster_id>
        SLURM:    scancel <cluster_id>

        Returns True on success.
        """
        ...
