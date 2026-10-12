from __future__ import annotations

"""
ContextCollector — class-based evidence collector.

Replaces the standalone collect_failure_context() function with a proper
class that takes a SchedulerClient via constructor injection.

Usage:
    from Pegasus.healer.collectors.base import ContextCollector
    from Pegasus.healer.scheduler.factory import get_scheduler_client

    collector = ContextCollector(get_scheduler_client())
    ctx = await collector.collect(event, incident_id, ...)

Tests inject a fake:
    collector = ContextCollector(FakeSchedulerClient())
"""

import uuid
from typing import Any

import structlog

from Pegasus.healer.collectors import files as file_collector
from Pegasus.healer.models.context import (
    ArtifactRef,
    DiagnosisSummary,
    FailureContext,
    FixSummary,
    PolicySnapshot,
    ResourceRequest,
    ResourceUsage,
)
from Pegasus.healer.scheduler.base import JobHistory, SchedulerClient

log = structlog.get_logger(__name__)


class ContextCollector:
    """
    Collects all diagnostic evidence for a failed job.

    Combines:
      - Scheduler history  (exit code, resource usage, hold reason)
      - Submit-dir files   (stderr, stdout, kickstart, DAGMan logs)
      - Event fields       (workflow/job IDs, attempt number)

    into a FailureContext ready for the diagnostic agents.
    """

    def __init__(self, scheduler: SchedulerClient) -> None:
        self._scheduler = scheduler

    async def collect(
        self,
        job_id: str,
        workflow_id: str,
        job_instance_id: int,
        exit_code: int | None,
        scheduler_id: str | None,
        incident_id: uuid.UUID,
        attempt_number: int,
        previous_diagnoses: list[DiagnosisSummary],
        previous_fixes: list[FixSummary],
        policy_snapshot: PolicySnapshot | None,
        submit_dir: str | None = None,
    ) -> FailureContext:
        """
        Collect all evidence and return a FailureContext.

        All job identity fields are passed directly — no WorkflowEvent needed.
        submit_dir: Pegasus submit directory (.out/.err/.kickstart files).
        """
        missing_evidence: list[str] = []

        # ── Scheduler history ─────────────────────────────────────────────────
        history = self._fetch_history(scheduler_id, missing_evidence)

        # ── Exit code / termination signal ────────────────────────────────────
        resolved_exit_code: int | None = exit_code if exit_code is not None else history.exit_code
        termination_signal: int | None = history.exit_signal
        if resolved_exit_code is None:
            missing_evidence.append("mandatory:exit_code")

        # ── Hold state ────────────────────────────────────────────────────────
        scheduler_state = "Held" if history.hold_reason_code else None
        scheduler_reason = history.hold_reason

        # ── Requested resources (from raw scheduler data) ─────────────────────
        requested = self._extract_requested(history.raw, missing_evidence)

        # ── Measured resources (normalized JobHistory fields) ─────────────────
        measured = ResourceUsage(
            peak_memory_mb=history.memory_usage_mb,
            disk_used_mb=history.disk_usage_mb,
            runtime_seconds=_int(history.wall_time_seconds),
        )

        # ── Log files ─────────────────────────────────────────────────────────
        (
            stdout_ref, stderr_ref, kickstart_ref,
            condor_event_log_ref, submit_file_ref,
            dagman_out_ref, dagman_err_ref,
            workflow_log_ref, monitord_log_ref,
        ) = self._collect_files(job_id, workflow_id, job_instance_id, submit_dir, missing_evidence)

        ctx = FailureContext(
            incident_id=incident_id,
            workflow_id=workflow_id,
            job_id=job_id,
            job_instance_id=job_instance_id,
            scheduler_id=scheduler_id,
            exit_code=resolved_exit_code,
            termination_signal=termination_signal,
            scheduler_state=scheduler_state,
            scheduler_reason=scheduler_reason,
            requested_resources=requested,
            measured_resources=measured,
            stdout_ref=stdout_ref,
            stderr_ref=stderr_ref,
            kickstart_ref=kickstart_ref,
            condor_event_log_ref=condor_event_log_ref,
            submit_file_ref=submit_file_ref,
            dagman_out_ref=dagman_out_ref,
            dagman_err_ref=dagman_err_ref,
            workflow_log_ref=workflow_log_ref,
            monitord_log_ref=monitord_log_ref,
            input_checks=[],
            output_checks=[],
            attempt_number=attempt_number,
            previous_diagnoses=previous_diagnoses,
            previous_fixes=previous_fixes,
            policy_snapshot=policy_snapshot,
            missing_evidence=missing_evidence,
        )

        log.info(
            "context_collected",
            incident_id=str(incident_id),
            missing_fields=missing_evidence,
            has_stderr=stderr_ref is not None,
            has_kickstart=kickstart_ref is not None,
            has_condor_log=condor_event_log_ref is not None,
            has_submit_file=submit_file_ref is not None,
            has_dagman_out=dagman_out_ref is not None,
            has_workflow_log=workflow_log_ref is not None,
        )
        return ctx

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_history(
        self,
        scheduler_id: str | None,
        missing_evidence: list[str],
    ) -> JobHistory:
        if not scheduler_id:
            missing_evidence.append("mandatory:scheduler_history")
            return JobHistory()
        history = self._scheduler.get_job_history(scheduler_id)
        if history.exit_code is None and not history.raw:
            missing_evidence.append("mandatory:scheduler_history")
        return history

    def _extract_requested(
        self,
        raw: dict[str, Any],
        missing_evidence: list[str],
    ) -> ResourceRequest:
        """
        Extract requested resources from the scheduler's raw data.

        Both HTCondor (RequestMemory / RequestDisk / RequestCpus) and SLURM
        (mapped to the same keys by SLURMClient) store request values in raw.
        """
        req = ResourceRequest(
            memory_mb=_int(raw.get("RequestMemory")),
            disk_mb=_int(raw.get("RequestDisk")),
            cpus=_int(raw.get("RequestCpus")),
            runtime_seconds=None,
        )
        if req.memory_mb is None:
            missing_evidence.append("mandatory:requested_memory")
        return req

    def _collect_files(
        self,
        job_id: str,
        workflow_id: str,
        job_instance_id: int,
        submit_dir: str | None,
        missing_evidence: list[str],
    ) -> tuple[
        ArtifactRef | None, ArtifactRef | None, ArtifactRef | None,
        ArtifactRef | None, ArtifactRef | None, ArtifactRef | None,
        ArtifactRef | None, ArtifactRef | None, ArtifactRef | None,
    ]:
        if not submit_dir:
            missing_evidence.append("mandatory:submit_dir")
            return (None,) * 9  # type: ignore[return-value]

        return (
            file_collector.collect_stdout(submit_dir, workflow_id, job_id, job_instance_id),
            file_collector.collect_stderr(submit_dir, workflow_id, job_id, job_instance_id),
            file_collector.collect_kickstart(submit_dir, workflow_id, job_id, job_instance_id),
            file_collector.collect_condor_event_log(submit_dir, job_id, job_instance_id),
            file_collector.collect_submit_file(submit_dir, job_id, job_instance_id),
            file_collector.collect_dagman_out(submit_dir, workflow_id),
            file_collector.collect_dagman_err(submit_dir, workflow_id),
            file_collector.collect_workflow_log(submit_dir),
            file_collector.collect_monitord_log(submit_dir),
        )


# ── Module-level helper ───────────────────────────────────────────────────────

def _int(val: Any) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None
