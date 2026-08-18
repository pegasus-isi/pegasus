"""Pure why-idle analysis over already acquired monitor snapshots."""

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

import math
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from Pegasus.monitor.models import (
    EffectiveSnapshot,
    HealthState,
    JobSnapshot,
    Lifecycle,
    SchedulerQueryKind,
    SchedulerQueryResult,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from Pegasus.monitor.coordinator import CoordinatorSnapshot

MAX_RESOURCE_FIT_COMPARISONS = 250_000


class SourceDisposition(str, Enum):
    HEALTHY = "healthy"
    HEALTHY_EMPTY = "healthy_empty"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class SourceAssessment:
    kind: SchedulerQueryKind
    disposition: SourceDisposition
    evidence_count: int
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class IdleJob:
    exec_job_id: str
    state: Lifecycle
    type_desc: str
    provenance: str


@dataclass(frozen=True, slots=True)
class ResourceMismatch:
    exec_job_id: str
    requested_cpus: int | None
    requested_memory_mb: int | None
    requested_disk_kb: int | None
    requested_gpus: int | None
    reason: str
    requirements: str | None = None


@dataclass(frozen=True, slots=True)
class PrioritySummary:
    owner: str
    principal: str
    effective_priority: float | None
    real_priority: float | None
    resources_used: float | None
    better_priority_users: int


@dataclass(frozen=True, slots=True)
class IdleAnalysis:
    idle_jobs: tuple[IdleJob, ...]
    queued_jobs: int
    unsubmitted_jobs: int
    workflow_owner: str | None
    pool_total_cpus: int | None
    pool_idle_cpus: int | None
    pool_total_memory_mb: int | None
    pool_idle_memory_mb: int | None
    pool_total_gpus: int | None
    pool_idle_gpus: int | None
    pool_machines: int | None
    pool_total_slots: int | None
    pool_idle_slots: int | None
    priority: PrioritySummary | None
    negotiation_cycle_seconds: float | None
    negotiation_matches: int | None
    requirement_mismatches: tuple[ResourceMismatch, ...]
    findings: tuple[str, ...]
    suggestions: tuple[str, ...]
    sources: tuple[SourceAssessment, ...]


def _inputs(
    value: EffectiveSnapshot | CoordinatorSnapshot,
    results: Sequence[SchedulerQueryResult] | None,
) -> tuple[EffectiveSnapshot, tuple[SchedulerQueryResult, ...]]:
    if isinstance(value, EffectiveSnapshot):
        return value, tuple(results or ())
    if value.effective is None:
        raise ValueError("why-idle requires an authoritative effective snapshot")
    return value.effective, tuple(
        value.scheduler_results if results is None else results
    )


def _latest(
    results: Iterable[SchedulerQueryResult],
) -> dict[SchedulerQueryKind, SchedulerQueryResult]:
    selected: dict[SchedulerQueryKind, SchedulerQueryResult] = {}
    for result in results:
        old = selected.get(result.request.kind)
        if old is None or result.request.clock.monotonic >= old.request.clock.monotonic:
            selected[result.request.kind] = result
    return selected


def _disposition(result: SchedulerQueryResult | None) -> SourceDisposition:
    if result is None:
        return SourceDisposition.UNAVAILABLE
    state = result.health.state
    if state is HealthState.HEALTHY:
        return (
            SourceDisposition.HEALTHY
            if result.evidence
            else SourceDisposition.HEALTHY_EMPTY
        )
    if state is HealthState.STALE:
        return SourceDisposition.STALE
    if state is HealthState.DISABLED:
        return SourceDisposition.DISABLED
    if state is HealthState.UNAVAILABLE:
        return SourceDisposition.UNAVAILABLE
    return SourceDisposition.DEGRADED


def _assessment(
    kind: SchedulerQueryKind, result: SchedulerQueryResult | None
) -> SourceAssessment:
    return SourceAssessment(
        kind,
        _disposition(result),
        0 if result is None else len(result.evidence),
        None if result is None else result.health.error_code,
    )


def _effective_disposition(
    kind: SchedulerQueryKind,
    result: SchedulerQueryResult | None,
    disabled: frozenset[SchedulerQueryKind],
) -> SourceDisposition:
    if result is None and kind in disabled:
        return SourceDisposition.DISABLED
    return _disposition(result)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any, default: int = 0) -> int:
    number = _number(value)
    return default if number is None else max(0, int(number))


def _optional_integer(value: Any) -> int | None:
    number = _number(value)
    return None if number is None else max(0, int(number))


def _requested_integer(value: Any, default: int) -> int | None:
    return default if value is None else _optional_integer(value)


def _rows(result: SchedulerQueryResult | None) -> list[Mapping[str, Any]]:
    return (
        []
        if result is None
        else [item.payload.to_json_dict() for item in result.evidence]
    )


def _queue_owner(rows: Sequence[Mapping[str, Any]]) -> str | None:
    owners = {str(row.get("Owner", "")).strip() for row in rows}
    owners.discard("")
    return next(iter(owners)) if len(owners) == 1 else None


def _resource_tuple(
    row: Mapping[str, Any], *, total: bool
) -> tuple[int | None, int | None, int | None, int | None]:
    if total:
        return (
            _optional_integer(row.get("TotalSlotCpus") or row.get("Cpus")),
            _optional_integer(row.get("TotalSlotMemory") or row.get("Memory")),
            _optional_integer(row.get("TotalSlotDisk") or row.get("Disk")),
            _optional_integer(row.get("TotalSlotGPUs") or row.get("GPUs")),
        )
    return (
        _optional_integer(row.get("Cpus")),
        _optional_integer(row.get("Memory")),
        _optional_integer(row.get("Disk")),
        _optional_integer(row.get("GPUs")),
    )


def _available_slot(row: Mapping[str, Any]) -> bool:
    partitionable = str(row.get("SlotType", "")).lower() == "partitionable" or (
        _ad_bool(row.get("PartitionableSlot")) and not _ad_bool(row.get("DynamicSlot"))
    )
    return (
        partitionable
        or str(row.get("State", "")).lower() == "unclaimed"
        or str(row.get("Activity", "")).lower() == "idle"
    )


def _ad_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _fits(
    requested: tuple[int | None, int | None, int | None, int | None],
    capacity: tuple[int | None, int | None, int | None, int | None],
) -> bool | None:
    unknown = False
    for wanted, available in zip(requested, capacity):
        if wanted is None:
            unknown = True
            continue
        if wanted <= 0:
            continue
        if available is None:
            unknown = True
        elif wanted > available:
            return False
    return None if unknown else True


def _any_slot_fits(
    requested: tuple[int | None, int | None, int | None, int | None],
    slots: Sequence[tuple[int | None, int | None, int | None, int | None]],
    remaining: list[int],
) -> tuple[bool | None, bool]:
    unknown = False
    for slot in slots:
        if remaining[0] <= 0:
            return None, True
        remaining[0] -= 1
        result = _fits(requested, slot)
        if result is True:
            return True, False
        if result is None:
            unknown = True
    return (None if unknown else False), False


def _queue_rows_for_jobs(
    jobs: Sequence[JobSnapshot], rows: Sequence[Mapping[str, Any]]
) -> list[Mapping[str, Any]]:
    wanted = {job.exec_job_id for job in jobs if job.lifecycle is Lifecycle.QUEUED}
    return [
        row
        for row in rows
        if _integer(row.get("JobStatus"), -1) == 1
        and str(row.get("DAGNodeName", "")) in wanted
    ]


def analyze_why_idle(
    snapshot: EffectiveSnapshot | CoordinatorSnapshot,
    scheduler_results: Sequence[SchedulerQueryResult] | None = None,
    *,
    workflow_owner: str | None = None,
    disabled_scheduler_kinds: frozenset[SchedulerQueryKind] = frozenset(),
) -> IdleAnalysis:
    """Explain waiting jobs without performing any HTCondor or DB query."""

    effective, supplied = _inputs(snapshot, scheduler_results)
    results = _latest(supplied)
    queue = results.get(SchedulerQueryKind.QUEUE)
    pool = results.get(SchedulerQueryKind.POOL)
    priority = results.get(SchedulerQueryKind.PRIORITY)
    negotiator = results.get(SchedulerQueryKind.NEGOTIATOR)
    queue_rows = _rows(queue)
    pool_rows = _rows(pool)
    idle = tuple(
        IdleJob(job.exec_job_id, job.lifecycle, job.type_desc, job.provenance.value)
        for job in effective.jobs
        if job.lifecycle in {Lifecycle.QUEUED, Lifecycle.UNSUBMITTED}
    )
    queued = tuple(item for item in idle if item.state is Lifecycle.QUEUED)
    unsubmitted = tuple(item for item in idle if item.state is Lifecycle.UNSUBMITTED)
    findings: list[str] = []
    suggestions: list[str] = []
    mismatches: list[ResourceMismatch] = []

    if not idle:
        findings.append("No queued or unsubmitted jobs are present in this workflow.")
    if unsubmitted:
        findings.append(
            f"{len(unsubmitted)} job(s) are unsubmitted and waiting on DAG dependencies."
        )
    if queued:
        findings.append(f"{len(queued)} job(s) are queued for HTCondor matching.")
    elif unsubmitted:
        suggestions.append(
            "DAGMan will submit these jobs after their parent dependencies complete."
        )

    queue_state = _effective_disposition(
        SchedulerQueryKind.QUEUE, queue, disabled_scheduler_kinds
    )
    matched_queue_rows = _queue_rows_for_jobs(effective.jobs, queue_rows)
    if queued and queue_state is SourceDisposition.HEALTHY_EMPTY:
        findings.append(
            "The workflow queue query succeeded but returned no matching idle ClassAds."
        )
    elif queued and queue_state is SourceDisposition.STALE:
        findings.append(
            "Queue evidence is stale; conclusions use the last good result."
        )
    elif queued and queue_state is SourceDisposition.DISABLED:
        findings.append("Queue observation is disabled for this monitor run.")
    elif queued and queue_state in {
        SourceDisposition.UNAVAILABLE,
        SourceDisposition.DEGRADED,
    }:
        findings.append(
            "Queue evidence is unavailable; scheduler details are incomplete."
        )

    pool_summary = {} if pool is None else pool.summary.to_json_dict()
    pool_state = _effective_disposition(
        SchedulerQueryKind.POOL, pool, disabled_scheduler_kinds
    )
    if queued and pool_state is SourceDisposition.HEALTHY_EMPTY:
        findings.append("The pool query succeeded and reported no slots.")
    elif queued and pool_state is SourceDisposition.STALE:
        findings.append(
            "Pool capacity is stale and may no longer describe available slots."
        )
    elif queued and pool_state is SourceDisposition.DISABLED:
        findings.append("Pool observation is disabled for this monitor run.")
    elif queued and pool_state in {
        SourceDisposition.UNAVAILABLE,
        SourceDisposition.DEGRADED,
    }:
        findings.append(
            "Pool capacity is unavailable; resource fit cannot be confirmed."
        )

    available_slots = [
        _resource_tuple(row, total=False) for row in pool_rows if _available_slot(row)
    ]
    physical_slots = [_resource_tuple(row, total=True) for row in pool_rows]
    fit_cache: dict[
        tuple[int | None, int | None, int | None, int | None], str | None
    ] = {}
    fit_budget = [MAX_RESOURCE_FIT_COMPARISONS]
    fit_limit_reached = False
    if pool_state in {SourceDisposition.HEALTHY, SourceDisposition.STALE}:
        for row in matched_queue_rows:
            requested = (
                _requested_integer(row.get("RequestCpus"), 1),
                _requested_integer(row.get("RequestMemory"), 0),
                _requested_integer(row.get("RequestDisk"), 0),
                _requested_integer(row.get("RequestGpus"), 0),
            )
            if requested in fit_cache:
                reason = fit_cache[requested]
                if reason is None:
                    continue
            elif fit_budget[0] <= 0:
                reason = "analysis_limit"
                fit_limit_reached = True
                fit_cache[requested] = reason
            else:
                available_fit, exhausted = _any_slot_fits(
                    requested, available_slots, fit_budget
                )
                if available_fit is True:
                    fit_cache[requested] = None
                    continue
                physical_fit, physical_exhausted = _any_slot_fits(
                    requested, physical_slots, fit_budget
                )
                exhausted = exhausted or physical_exhausted
                if exhausted:
                    reason = "analysis_limit"
                    fit_limit_reached = True
                elif available_fit is None or physical_fit is None:
                    reason = "slot_capacity_unknown"
                elif physical_fit is True:
                    reason = "all_capable_slots_busy"
                else:
                    reason = "exceeds_single_slot_capacity"
                fit_cache[requested] = reason
            if reason is None:
                continue
            job_name = str(
                row.get("DAGNodeName") or f"cluster {row.get('ClusterId', '?')}"
            )
            mismatches.append(
                ResourceMismatch(
                    job_name,
                    requested[0],
                    requested[1],
                    requested[2],
                    requested[3],
                    reason,
                    str(row.get("Requirements"))[:512]
                    if row.get("Requirements")
                    else None,
                )
            )
        if mismatches:
            findings.append(
                f"{len(mismatches)} queued job(s) have no confirmed fit on an available single slot."
            )
            suggestions.append(
                "Check per-job resource requests and Requirements, or wait for a capable slot."
            )
        if fit_limit_reached:
            findings.append(
                "Resource-fit analysis reached its comparison limit; remaining unique request shapes are unknown."
            )

    owner = (
        workflow_owner.strip()
        if workflow_owner and workflow_owner.strip()
        else _queue_owner(queue_rows)
    )
    priority_summary: PrioritySummary | None = None
    priority_rows = _rows(priority)
    priority_state = _effective_disposition(
        SchedulerQueryKind.PRIORITY, priority, disabled_scheduler_kinds
    )
    if owner is None and queued:
        findings.append(
            "Workflow owner is ambiguous; priority evidence was not attributed to a user."
        )
    elif owner is not None:
        if priority_state in {SourceDisposition.HEALTHY, SourceDisposition.STALE}:
            candidates = [
                row
                for row in priority_rows
                if str(row.get("Name", "")) == owner
                or str(row.get("Name", "")).startswith(f"{owner}@")
            ]
            if len(candidates) == 1:
                mine = candidates[0]
                effective_priority = _number(
                    mine.get("EffectivePriority", mine.get("Priority"))
                )
                other_values = [
                    value
                    for row in priority_rows
                    if row is not mine
                    and (
                        value := _number(
                            row.get("EffectivePriority", row.get("Priority"))
                        )
                    )
                    is not None
                ]
                priority_summary = PrioritySummary(
                    owner,
                    str(mine.get("Name", owner)),
                    effective_priority,
                    _number(mine.get("RealPriority", mine.get("PriorityFactor"))),
                    _number(
                        mine.get("ResourcesUsed", mine.get("WeightedResourcesUsed"))
                    ),
                    sum(
                        effective_priority is not None and value < effective_priority
                        for value in other_values
                    ),
                )
                if priority_summary.better_priority_users:
                    findings.append(
                        f"{priority_summary.better_priority_users} user(s) have better fair-share priority."
                    )
            elif priority_state is SourceDisposition.HEALTHY:
                findings.append(
                    f"No unique priority record matched workflow owner '{owner}'."
                )
        elif queued and priority_state is SourceDisposition.HEALTHY_EMPTY:
            findings.append(
                "The user-priority query succeeded but returned no priority records."
            )
        elif queued and priority_state is SourceDisposition.DISABLED:
            findings.append("User-priority observation is disabled for this run.")
        elif queued:
            findings.append("User-priority evidence is unavailable or degraded.")

    negotiation_cycle: float | None = None
    negotiation_matches: int | None = None
    if negotiator is not None:
        summary = negotiator.summary.to_json_dict()
        negotiation_cycle = _number(summary.get("last_cycle_duration_seconds"))
        raw_matches = _number(summary.get("last_cycle_matches"))
        negotiation_matches = None if raw_matches is None else int(raw_matches)
        if negotiation_matches == 0 and queued:
            findings.append("The last negotiation cycle produced no matches.")
            suggestions.append(
                "Review slot Requirements, resource requests, and negotiator policy."
            )
        if negotiation_cycle is not None and negotiation_cycle > 60:
            suggestions.append(
                "A negotiation cycle over 60 seconds may indicate negotiator load."
            )

    if queued and not suggestions:
        suggestions.append(
            "If the jobs remain idle after another negotiation cycle, inspect requirements and fair-share priority."
        )

    assessments = tuple(
        (
            SourceAssessment(kind, SourceDisposition.DISABLED, 0, "disabled")
            if kind in disabled_scheduler_kinds and results.get(kind) is None
            else _assessment(kind, results.get(kind))
        )
        for kind in (
            SchedulerQueryKind.QUEUE,
            SchedulerQueryKind.POOL,
            SchedulerQueryKind.PRIORITY,
            SchedulerQueryKind.NEGOTIATOR,
        )
    )
    return IdleAnalysis(
        idle,
        len(queued),
        len(unsubmitted),
        owner,
        _optional_integer(pool_summary.get("total_cpus")),
        _optional_integer(pool_summary.get("idle_cpus")),
        _optional_integer(pool_summary.get("total_memory_mb")),
        _optional_integer(pool_summary.get("idle_memory_mb")),
        _optional_integer(pool_summary.get("total_gpus")),
        _optional_integer(pool_summary.get("idle_gpus")),
        _optional_integer(pool_summary.get("machines")),
        _optional_integer(pool_summary.get("total_slots")),
        _optional_integer(pool_summary.get("idle_slots")),
        priority_summary,
        negotiation_cycle,
        negotiation_matches,
        tuple(mismatches),
        tuple(findings),
        tuple(dict.fromkeys(suggestions)),
        assessments,
    )
