"""Presentation-neutral statistics derived from immutable monitor snapshots."""

##
#  Copyright 2026 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

from __future__ import annotations

import dataclasses
import math
from dataclasses import dataclass
from statistics import median
from typing import TYPE_CHECKING, Any

from Pegasus.monitor.models import (
    EffectiveSnapshot,
    HealthState,
    JobSnapshot,
    Lifecycle,
    Provenance,
    SchedulerQueryKind,
    SchedulerQueryResult,
    normalize_workflow_state,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from Pegasus.monitor.coordinator import CoordinatorSnapshot


@dataclass(frozen=True, slots=True)
class SchedulerCoverage:
    """How much optional scheduler evidence contributed to a result."""

    kind: SchedulerQueryKind
    health: HealthState
    evidence_count: int
    matched_current_jobs: int
    eligible_current_jobs: int

    @property
    def complete(self) -> bool:
        return (
            self.health is HealthState.HEALTHY
            and self.matched_current_jobs == self.eligible_current_jobs
        )


@dataclass(frozen=True, slots=True)
class WorkflowStats:
    """Immutable statistics with explicit authority and source coverage."""

    total_jobs: int = 0
    compute_jobs: int = 0
    infra_jobs: int = 0
    succeeded: int = 0
    failed: int = 0
    held: int = 0
    unsubmitted: int = 0
    queued: int = 0
    running: int = 0
    wall_time: float | None = None
    total_compute_time: float | None = None
    parallelism: float | None = None
    dur_min: float | None = None
    dur_max: float | None = None
    dur_mean: float | None = None
    dur_median: float | None = None
    longest_job_name: str | None = None
    shortest_job_name: str | None = None
    peak_maxrss_kb: int | None = None
    peak_maxrss_job: str | None = None
    mean_maxrss_kb: float | None = None
    cpu_eff_min: float | None = None
    cpu_eff_max: float | None = None
    cpu_eff_mean: float | None = None
    mem_eff_min: float | None = None
    mem_eff_max: float | None = None
    mem_eff_mean: float | None = None
    wait_min: float | None = None
    wait_max: float | None = None
    wait_mean: float | None = None
    cpu_seconds: float | None = None
    transfer_bytes: int | None = None
    retry_count: int | None = None
    hosts: tuple[str, ...] | None = None
    pool_machines: int | None = None
    pool_total_cpus: int | None = None
    pool_total_gpus: int | None = None
    authoritative_final: bool = False
    workflow_provenance: Provenance = Provenance.DB_CONFIRMED
    db_confirmed_jobs: int = 0
    tail_overlay_jobs: int = 0
    tail_pending_jobs: int = 0
    provisional_jobs: int = 0
    pending_jobs: int = 0
    scheduler_coverage: tuple[SchedulerCoverage, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe mapping while retaining meaningful zeroes."""

        result: dict[str, Any] = {}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue
            if isinstance(value, Provenance):
                result[field.name] = value.value
            elif field.name == "scheduler_coverage":
                result[field.name] = [
                    {
                        "kind": item.kind.value,
                        "health": item.health.value,
                        "evidence_count": item.evidence_count,
                        "matched_current_jobs": item.matched_current_jobs,
                        "eligible_current_jobs": item.eligible_current_jobs,
                        "complete": item.complete,
                    }
                    for item in value
                ]
            elif isinstance(value, tuple):
                result[field.name] = list(value)
            else:
                result[field.name] = value
        return result


def _inputs(
    value: EffectiveSnapshot | CoordinatorSnapshot,
    scheduler_results: Sequence[SchedulerQueryResult] | None,
) -> tuple[EffectiveSnapshot, tuple[SchedulerQueryResult, ...], bool | None]:
    if isinstance(value, EffectiveSnapshot):
        return value, tuple(scheduler_results or ()), None
    effective = value.effective
    if effective is None:
        raise ValueError("statistics require an authoritative effective snapshot")
    return (
        effective,
        tuple(
            value.scheduler_results if scheduler_results is None else scheduler_results
        ),
        value.authoritative_complete,
    )


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _display_name(job: JobSnapshot) -> str:
    return job.transformations[0] if job.transformations else job.exec_job_id


def _current_attempt(job: JobSnapshot):
    if job.current_attempt is None:
        return None
    return next(
        (item for item in job.attempts if item.identity == job.current_attempt), None
    )


def _scheduler_by_kind(
    results: Iterable[SchedulerQueryResult],
) -> dict[SchedulerQueryKind, SchedulerQueryResult]:
    latest: dict[SchedulerQueryKind, SchedulerQueryResult] = {}
    for result in results:
        previous = latest.get(result.request.kind)
        if (
            previous is None
            or result.request.clock.monotonic >= previous.request.clock.monotonic
        ):
            latest[result.request.kind] = result
    return latest


def _history_rows(
    jobs: Sequence[JobSnapshot], result: SchedulerQueryResult | None
) -> tuple[list[Mapping[str, Any]], SchedulerCoverage]:
    eligible = [job for job in jobs if job.current_attempt is not None]
    if result is None:
        return [], SchedulerCoverage(
            SchedulerQueryKind.HISTORY, HealthState.UNAVAILABLE, 0, 0, len(eligible)
        )
    evidence = [item.payload.to_json_dict() for item in result.evidence]
    by_scheduler_id: dict[str, int | None] = {}
    by_job_name: dict[str, int | None] = {}
    for index, row in enumerate(evidence):
        if row.get("ClusterId") is not None:
            scheduler_id = f"{row.get('ClusterId')}.{row.get('ProcId', 0)}"
            by_scheduler_id[scheduler_id] = (
                index if scheduler_id not in by_scheduler_id else None
            )
        job_name = row.get("DAGNodeName")
        if isinstance(job_name, str) and job_name:
            by_job_name[job_name] = index if job_name not in by_job_name else None
    selected: list[Mapping[str, Any]] = []
    used: set[int] = set()
    for job in eligible:
        attempt = _current_attempt(job)
        scheduler_id = attempt.scheduler_id if attempt is not None else None
        match = (
            by_scheduler_id.get(scheduler_id)
            if scheduler_id is not None
            else by_job_name.get(job.exec_job_id)
        )
        if match is not None and match not in used:
            used.add(match)
            selected.append(evidence[match])
    return selected, SchedulerCoverage(
        SchedulerQueryKind.HISTORY,
        result.health.state,
        len(result.evidence),
        len(selected),
        len(eligible),
    )


def _ratio(values: list[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    return min(values), max(values), sum(values) / len(values)


def compute_workflow_stats(
    snapshot: EffectiveSnapshot | CoordinatorSnapshot,
    scheduler_results: Sequence[SchedulerQueryResult] | None = None,
) -> WorkflowStats:
    """Compute stats without acquiring data or counting historical attempts.

    Each :class:`JobSnapshot` contributes once.  Attempt-derived duration,
    memory, scheduler ID, and history matching always use its current attempt.
    """

    effective, results, coordinator_final = _inputs(snapshot, scheduler_results)
    jobs = list(effective.jobs)
    compute = [job for job in jobs if job.type_desc.lower() == "compute"]
    lifecycles = [job.lifecycle for job in jobs]
    workflow = effective.workflow
    wall_time: float | None = None
    if workflow.started_at is not None:
        end = workflow.ended_at
        if end is None:
            end = effective.published_at_epoch
        wall_time = max(0.0, float(end) - float(workflow.started_at))

    durations: list[tuple[float, str]] = []
    memory: list[tuple[int, str]] = []
    for job in compute:
        attempt = _current_attempt(job)
        if attempt is None:
            continue
        if attempt.start_time is not None and attempt.end_time is not None:
            duration = float(attempt.end_time - attempt.start_time)
            if duration > 0:
                durations.append((duration, _display_name(job)))
        if attempt.maxrss_kb is not None and attempt.maxrss_kb > 0:
            memory.append((attempt.maxrss_kb, _display_name(job)))

    by_kind = _scheduler_by_kind(results)
    history, history_coverage = _history_rows(
        jobs, by_kind.get(SchedulerQueryKind.HISTORY)
    )
    cpu_effs: list[float] = []
    mem_effs: list[float] = []
    waits: list[float] = []
    cpu_seconds = 0.0
    transfer_bytes = 0
    retries = 0
    hosts: dict[str, int] = {}
    for row in history:
        wall = _number(row.get("RemoteWallClockTime"))
        user_cpu = _number(row.get("RemoteUserCpu"))
        cpus = _number(row.get("RequestCpus", 1))
        if (
            wall is not None
            and user_cpu is not None
            and cpus is not None
            and wall > 0
            and cpus > 0
        ):
            cpu_effs.append(user_cpu / (wall * cpus))
        image_kb = _number(row.get("ImageSize"))
        request_memory = _number(row.get("RequestMemory"))
        if image_kb is not None and request_memory is not None and request_memory > 0:
            mem_effs.append(image_kb / (request_memory * 1024.0))
        qdate = _number(row.get("QDate"))
        started = _number(row.get("JobStartDate"))
        if qdate is not None and started is not None:
            waits.append(max(0.0, started - qdate))
        if user_cpu is not None:
            cpu_seconds += user_cpu
        transfer_bytes += max(0, _integer(row.get("BytesSent")) or 0)
        transfer_bytes += max(0, _integer(row.get("BytesRecvd")) or 0)
        retries += max(0, (_integer(row.get("NumJobStarts")) or 0) - 1)
        host = str(row.get("LastRemoteHost") or "")
        if host:
            short = host.split("@")[-1].split(".")[0]
            hosts[short] = hosts.get(short, 0) + 1

    pool = by_kind.get(SchedulerQueryKind.POOL)
    pool_values = pool.summary.to_json_dict() if pool is not None else {}
    pool_coverage = SchedulerCoverage(
        SchedulerQueryKind.POOL,
        HealthState.UNAVAILABLE if pool is None else pool.health.state,
        0 if pool is None else len(pool.evidence),
        0,
        0,
    )
    duration_values = [value for value, _ in durations]
    peak = max(memory, default=None, key=lambda item: item[0])
    longest = max(durations, default=None, key=lambda item: item[0])
    shortest = min(durations, default=None, key=lambda item: item[0])
    cpu_min, cpu_max, cpu_mean = _ratio(cpu_effs)
    mem_min, mem_max, mem_mean = _ratio(mem_effs)
    wait_min, wait_max, wait_mean = _ratio(waits)
    total_compute = sum(duration_values) if duration_values else None
    authoritative_final = (
        coordinator_final
        if coordinator_final is not None
        else workflow.provenance is Provenance.DB_CONFIRMED
        and normalize_workflow_state(workflow.state) == "WORKFLOW_TERMINATED"
        and workflow.status is not None
        and effective.pending_overlay_count == 0
    )

    return WorkflowStats(
        total_jobs=len(jobs),
        compute_jobs=len(compute),
        infra_jobs=len(jobs) - len(compute),
        succeeded=lifecycles.count(Lifecycle.SUCCEEDED),
        failed=lifecycles.count(Lifecycle.FAILED),
        held=lifecycles.count(Lifecycle.HELD),
        unsubmitted=lifecycles.count(Lifecycle.UNSUBMITTED),
        queued=lifecycles.count(Lifecycle.QUEUED),
        running=lifecycles.count(Lifecycle.RUNNING),
        wall_time=wall_time,
        total_compute_time=total_compute,
        parallelism=(
            total_compute / wall_time
            if total_compute is not None and wall_time is not None and wall_time > 0
            else None
        ),
        dur_min=min(duration_values) if duration_values else None,
        dur_max=max(duration_values) if duration_values else None,
        dur_mean=(
            sum(duration_values) / len(duration_values) if duration_values else None
        ),
        dur_median=median(duration_values) if duration_values else None,
        longest_job_name=None if longest is None else longest[1],
        shortest_job_name=None if shortest is None else shortest[1],
        peak_maxrss_kb=None if peak is None else peak[0],
        peak_maxrss_job=None if peak is None else peak[1],
        mean_maxrss_kb=(
            sum(value for value, _ in memory) / len(memory) if memory else None
        ),
        cpu_eff_min=cpu_min,
        cpu_eff_max=cpu_max,
        cpu_eff_mean=cpu_mean,
        mem_eff_min=mem_min,
        mem_eff_max=mem_max,
        mem_eff_mean=mem_mean,
        wait_min=wait_min,
        wait_max=wait_max,
        wait_mean=wait_mean,
        cpu_seconds=cpu_seconds if cpu_seconds > 0 else None,
        transfer_bytes=transfer_bytes if transfer_bytes > 0 else None,
        retry_count=retries if retries > 0 else None,
        hosts=(
            tuple(
                f"{name} ({count})"
                for name, count in sorted(
                    hosts.items(), key=lambda item: (-item[1], item[0])
                )
            )
            if hosts
            else None
        ),
        pool_machines=_integer(pool_values.get("machines")),
        pool_total_cpus=_integer(pool_values.get("total_cpus")),
        pool_total_gpus=(
            value
            if (value := _integer(pool_values.get("total_gpus"))) is not None
            and value > 0
            else None
        ),
        authoritative_final=authoritative_final,
        workflow_provenance=workflow.provenance,
        db_confirmed_jobs=sum(
            job.provenance is Provenance.DB_CONFIRMED for job in jobs
        ),
        tail_overlay_jobs=sum(
            job.provenance is Provenance.DB_WITH_TAIL_OVERLAY for job in jobs
        ),
        tail_pending_jobs=sum(
            job.provenance is Provenance.TAIL_PENDING for job in jobs
        ),
        provisional_jobs=sum(
            job.provenance is Provenance.PROVISIONAL_JOB for job in jobs
        ),
        pending_jobs=sum(bool(job.pending_tail) for job in jobs),
        scheduler_coverage=(history_coverage, pool_coverage),
    )
