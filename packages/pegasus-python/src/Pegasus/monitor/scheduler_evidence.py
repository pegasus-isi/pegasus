"""Attempt-scoped scheduler observations for monitor explanations."""

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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from Pegasus.monitor.coordinator import CoordinatorSnapshot

from Pegasus.monitor.models import (
    HealthState,
    JobSnapshot,
    Lifecycle,
    SchedulerQueryKind,
    SchedulerQueryResult,
)


def scheduler_rows(
    jobs: Sequence[JobSnapshot],
    results: Sequence[SchedulerQueryResult],
    kind: SchedulerQueryKind,
) -> dict[str, dict[str, Any]]:
    """Index the latest healthy observation, joining by scheduler identity.

    A DAG node name alone cannot distinguish retries. Allow name matching only
    before any attempt has acquired a scheduler identity.
    """
    latest = max(
        (result for result in results if result.request.kind is kind),
        key=lambda result: result.request.clock.monotonic,
        default=None,
    )
    if latest is None or latest.health.state is not HealthState.HEALTHY:
        return {}
    by_id = {}
    by_name = {}
    for evidence in latest.evidence:
        row = {**evidence.target.to_json_dict(), **evidence.payload.to_json_dict()}
        cluster = row.get("ClusterId")
        if cluster is not None:
            by_id[f"{cluster}.{row.get('ProcId', 0)}"] = row
        name = row.get("DAGNodeName")
        if name:
            by_name.setdefault(name, []).append(row)
    selected = {}
    for job in jobs:
        if job.workflow != latest.request.workflow:
            continue
        attempt = next(
            (a for a in job.attempts if a.identity == job.current_attempt), None
        )
        scheduler_id = None if attempt is None else attempt.scheduler_id
        row = by_id.get(scheduler_id) if scheduler_id else None
        if not scheduler_id and not any(a.scheduler_id for a in job.attempts):
            candidates = by_name.get(job.exec_job_id, ())
            if len(candidates) == 1:
                row = candidates[0]
        if (
            row is not None
            and row.get("DAGNodeName", job.exec_job_id) == job.exec_job_id
        ):
            selected[job.exec_job_id] = row
    return selected


def observed_lifecycle(
    job: JobSnapshot, queue_rows: Mapping[str, Mapping[str, Any]]
) -> Lifecycle:
    """Use fresh scheduler observations for explanations, without changing history."""
    if job.lifecycle in {Lifecycle.FAILED, Lifecycle.SUCCEEDED}:
        return job.lifecycle
    row = queue_rows.get(job.exec_job_id)
    if row is not None:
        return {1: Lifecycle.QUEUED, 2: Lifecycle.RUNNING, 5: Lifecycle.HELD}.get(
            row.get("JobStatus"), job.lifecycle
        )
    if job.lifecycle is Lifecycle.UNSUBMITTED and any(
        a.identity == job.current_attempt and a.scheduler_id for a in job.attempts
    ):
        return Lifecycle.QUEUED
    return job.lifecycle


def is_held(job: JobSnapshot, queue_rows: Mapping[str, Mapping[str, Any]]) -> bool:
    return observed_lifecycle(job, queue_rows) is Lifecycle.HELD


def hold_reason_codes(
    jobs: Sequence[JobSnapshot], results: Sequence[SchedulerQueryResult]
) -> dict[str, int]:
    codes = {}
    for kind in (SchedulerQueryKind.HISTORY, SchedulerQueryKind.QUEUE):
        for name, row in scheduler_rows(jobs, results, kind).items():
            code = row.get("HoldReasonCode")
            if isinstance(code, int) and not isinstance(code, bool):
                codes[name] = code
    return codes


def hold_reasons(
    jobs: Sequence[JobSnapshot], results: Sequence[SchedulerQueryResult]
) -> dict[str, str]:
    reasons = {}
    for kind in (SchedulerQueryKind.HISTORY, SchedulerQueryKind.QUEUE):
        for name, row in scheduler_rows(jobs, results, kind).items():
            reason = row.get("HoldReason") or row.get("LastHoldReason")
            if reason:
                reasons[name] = str(reason)
    return reasons


def removed_jobs(
    jobs: Sequence[JobSnapshot], results: Sequence[SchedulerQueryResult]
) -> dict[str, dict[str, Any]]:
    """Return attempt-matched removal evidence; history closes stale queue ads."""
    rows = scheduler_rows(jobs, results, SchedulerQueryKind.QUEUE)
    rows.update(scheduler_rows(jobs, results, SchedulerQueryKind.HISTORY))
    return {name: row for name, row in rows.items() if row.get("JobStatus") == 3}


def workflow_removal_reason(snapshot: CoordinatorSnapshot) -> str | None:
    """Identify removal of DAGMan itself, not failure-triggered child cleanup."""
    if snapshot.effective is None or not snapshot.authoritative_complete:
        return None
    transition = snapshot.effective.workflow.transition
    reason = "" if transition is None else (transition.reason or "").lower()
    if "removed" in reason or "condor_rm" in reason:
        return transition.reason
    dag_rows = []
    for result in snapshot.scheduler_results:
        if (
            result.request.workflow != snapshot.effective.workflow.workflow
            or result.health.state is not HealthState.HEALTHY
            or result.request.kind
            not in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}
        ):
            continue
        for evidence in result.evidence:
            row = {**evidence.target.to_json_dict(), **evidence.payload.to_json_dict()}
            if row.get("JobUniverse") == 7:
                dag_rows.append(row)
    if not dag_rows:
        return None
    latest = max(
        dag_rows, key=lambda row: (row.get("QDate", 0), row.get("ClusterId", 0))
    )
    if snapshot.effective.workflow.restart_count:
        ended = latest.get("CompletionDate", latest.get("EnteredCurrentStatus", 0))
        started = snapshot.effective.workflow.started_at
        if started is None or ended < started:
            return None
    return (
        str(latest.get("RemoveReason") or "DAGMan was removed")
        if latest.get("JobStatus") == 3
        else None
    )
