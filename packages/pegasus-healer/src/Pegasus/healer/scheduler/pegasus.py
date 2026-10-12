from __future__ import annotations

"""
PegasusClient — unified scheduler + workflow-level client.

Wraps a SchedulerClient (HTCondor or SLURM) for job-level operations and
adds Pegasus workflow-level CLI commands that operate on submit directories.
"""

import subprocess
from pathlib import Path
from typing import Any

import structlog

from Pegasus.healer.scheduler.base import JobHistory, JobStatus, PartitionInfo, SchedulerClient

log = structlog.get_logger(__name__)


def _run(cmd: list[str], timeout: int, log_key: str) -> subprocess.CompletedProcess | None:
    """Run a CLI command, returning None on missing binary or timeout."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        log.warning(f"{log_key}_not_found", cmd=cmd[0], note=f"{cmd[0]} not on PATH")
        return None
    except subprocess.TimeoutExpired:
        log.warning(f"{log_key}_timeout", timeout=timeout)
        return None


class PegasusClient:
    """
    Pegasus-aware scheduler client.

    Implements SchedulerClient by delegating to the wrapped backend
    (HTCondorClient or SLURMClient) and adds Pegasus workflow-level
    commands that operate on submit directories rather than cluster IDs.
    """

    def __init__(self, scheduler: SchedulerClient) -> None:
        self._scheduler = scheduler

    # ── SchedulerClient delegation ─────────────────────────────────────────

    def get_job_status(self, cluster_id: str) -> JobStatus | None:
        return self._scheduler.get_job_status(cluster_id)

    def update_job_resources(self, cluster_id: str, resources: dict[str, Any]) -> bool:
        return self._scheduler.update_job_resources(cluster_id, resources)

    def get_job_history(self, cluster_id: str) -> JobHistory:
        return self._scheduler.get_job_history(cluster_id)

    def cancel_job(self, cluster_id: str) -> bool:
        return self._scheduler.cancel_job(cluster_id)

    # ── Pegasus workflow-level commands ────────────────────────────────────

    def analyze_workflow(
        self,
        submit_dir: Path,
        job_id: str | None = None,
        *,
        timeout: int = 60,
    ) -> str | None:
        """
        Run pegasus-analyzer and return its stdout.

        With job_id: scoped to one job node (--job flag).
        Without:     full workflow analysis.

        Returns None when pegasus-analyzer is not on PATH or times out.
        """
        cmd = ["pegasus-analyzer", str(submit_dir)]
        if job_id:
            cmd += ["--job", job_id]
        result = _run(cmd, timeout, "pegasus_analyzer")
        if result is None:
            return None
        if result.returncode not in (0, 1):
            log.warning(
                "pegasus_analyzer_nonzero",
                rc=result.returncode,
                stderr=result.stderr[:300],
            )
        return result.stdout or None

    def get_workflow_status(
        self,
        submit_dir: Path,
        *,
        timeout: int = 30,
    ) -> str | None:
        """Run pegasus-status and return its stdout."""
        cmd = ["pegasus-status", "--long", str(submit_dir)]
        result = _run(cmd, timeout, "pegasus_status")
        if result is None:
            return None
        if result.returncode != 0:
            log.warning(
                "pegasus_status_nonzero",
                rc=result.returncode,
                stderr=result.stderr[:300],
            )
        return result.stdout or None

    def remove_workflow(self, submit_dir: Path, *, timeout: int = 30) -> bool:
        """Run pegasus-remove to cancel a running workflow."""
        cmd = ["pegasus-remove", str(submit_dir)]
        result = _run(cmd, timeout, "pegasus_remove")
        if result is None:
            return False
        success = result.returncode == 0
        if success:
            log.info("pegasus_remove_ok", submit_dir=str(submit_dir))
        else:
            log.warning("pegasus_remove_failed", rc=result.returncode,
                        stderr=result.stderr[:300])
        return success

    def run_workflow(self, submit_dir: Path, *, timeout: int = 30) -> bool:
        """Run pegasus-run to (re)submit a workflow."""
        cmd = ["pegasus-run", str(submit_dir)]
        result = _run(cmd, timeout, "pegasus_run")
        if result is None:
            return False
        success = result.returncode == 0
        if success:
            log.info("pegasus_run_ok", submit_dir=str(submit_dir))
        else:
            log.warning("pegasus_run_failed", rc=result.returncode,
                        stderr=result.stderr[:300])
        return success

    # ── SLURM partition / queue helpers ────────────────────────────────────────

    def get_partition_profile(self) -> list[PartitionInfo]:
        """
        Return resource limits for all visible SLURM partitions.

        Delegates to ``SLURMClient.list_partitions()`` when the underlying
        backend supports it (i.e. when running on a SLURM cluster).
        Returns an empty list on HTCondor-only clusters.

        All subprocess I/O is handled inside SLURMClient via its ``_run``
        helper — no direct subprocess calls here.
        """
        if hasattr(self._scheduler, "list_partitions"):
            return self._scheduler.list_partitions()
        return []

    def get_partition_info(self, partition_name: str) -> PartitionInfo | None:
        """
        Return limits for a single named SLURM partition.

        Delegates to ``SLURMClient.get_partition_info()``.
        Returns None on non-SLURM backends or when the partition is unknown.
        """
        if hasattr(self._scheduler, "get_partition_info"):
            return self._scheduler.get_partition_info(partition_name)
        return None

    def find_target_partition(
        self,
        needed_memory_mb: int,
        current_partition: str | None = None,
    ) -> PartitionInfo | None:
        """
        Find the smallest accessible SLURM partition that can hold ``needed_memory_mb``.

        Selection rules (all via PegasusClient — no direct scheduler calls):
          1. Partition must be accessible (State=UP, user has rights).
          2. Partition must satisfy ``fits_memory(needed_memory_mb)``.
          3. Current partition is excluded (it already proved insufficient).
          4. Among qualifying partitions, prefer the one with the smallest
             ceiling that still fits (avoids wasting high-memory resources).
          5. If only unlimited-memory partitions qualify, return the first.

        Returns None when no accessible partition can satisfy the request.
        This is an ESCALATE signal: the healer cannot fix the job automatically.
        """
        partitions = self.get_partition_profile()
        candidates = [
            p for p in partitions
            if p.accessible
            and p.fits_memory(needed_memory_mb)
            and p.name != (current_partition or "")
        ]
        if not candidates:
            log.warning(
                "no_partition_fits",
                needed_mb=needed_memory_mb,
                current=current_partition,
                checked=len(partitions),
            )
            return None

        # Prefer smallest ceiling that fits (cheapest resource tier)
        with_ceiling = [p for p in candidates if p.max_mem_mb is not None]
        unlimited = [p for p in candidates if p.max_mem_mb is None]
        if with_ceiling:
            target = min(with_ceiling, key=lambda p: p.max_mem_mb)  # type: ignore[arg-type]
        else:
            target = unlimited[0]

        log.info(
            "partition_migration_target",
            from_partition=current_partition,
            to_partition=target.name,
            max_mem_mb=target.max_mem_mb,
            needed_mb=needed_memory_mb,
        )
        return target


# ── Backwards-compatible standalone function ───────────────────────────────────

def run_pegasus_analyzer(
    submit_dir: Path,
    job_id: str | None = None,
    *,
    timeout: int = 60,
) -> str | None:
    """
    Standalone wrapper — called by collectors/submit_dir.py.
    """
    from Pegasus.healer.scheduler.factory import get_scheduler_client
    return PegasusClient(get_scheduler_client()).analyze_workflow(
        submit_dir, job_id, timeout=timeout
    )
