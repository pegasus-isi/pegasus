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

import os
from dataclasses import dataclass
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


def recorded_hold_reason(job: JobSnapshot) -> str | None:
    """Hold reason Stampede recorded with the job's JOB_HELD transition.

    The queue is the fresher source, but this is the only one left when
    HTCondor is not queried.
    """

    transition = job.transition
    if transition is None or job.lifecycle is not Lifecycle.HELD:
        return None
    reason = (getattr(transition, "reason", None) or "").strip()
    return reason or None


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


_IN_SCHEDULER = frozenset(
    {
        Lifecycle.PRE,
        Lifecycle.QUEUED,
        Lifecycle.RUNNING,
        Lifecycle.HELD,
        Lifecycle.POST,
    }
)
REMOVED = "removed"


def observed_job_counts(snapshot: CoordinatorSnapshot) -> dict[str, int]:
    """Count jobs by lifecycle after applying what the scheduler shows.

    Stampede lags the queue by seconds to minutes and never records the end
    of a job that was removed with its workflow. A healthy queue observation
    replaces the recorded lifecycle of an unfinished job, and a job that
    HTCondor removed, or that was still unfinished when its workflow ended,
    is counted as ``removed`` instead of as queued, running or held.
    """

    counts = dict.fromkeys((item.value for item in Lifecycle), 0)
    counts[REMOVED] = 0
    if snapshot.effective is None:
        return counts
    jobs = snapshot.effective.jobs
    results = snapshot.scheduler_results
    queue_rows = scheduler_rows(jobs, results, SchedulerQueryKind.QUEUE)
    removals = removed_jobs(jobs, results)
    for job in jobs:
        lifecycle = observed_lifecycle(job, queue_rows)
        gone = job.exec_job_id in removals or snapshot.authoritative_complete
        if lifecycle in _IN_SCHEDULER and gone:
            counts[REMOVED] += 1
        else:
            counts[lifecycle.value] += 1
    return counts


_SCHEDULER_UNIVERSE = 7
_LEFT_QUEUE = frozenset({3, 4})
_DAGMAN_WRAPPER = "pegasus-dagman"
_RECORDED_IN_QUEUE = frozenset({Lifecycle.QUEUED, Lifecycle.RUNNING, Lifecycle.HELD})

DAGMAN_OUT = "dagman_out"
NODES_LOG = "nodes_log"


@dataclass(frozen=True, slots=True)
class LocalActivity:
    """What the workflow's submit directory shows, read once by the caller.

    ``epoch`` is the newest modification time of the files DAGMan and the
    schedd write as the workflow moves: ``<dag>.dagman.out`` and
    ``<dag>.nodes.log``. ``dagman_iwds`` holds the ``Iwd`` values, as the
    queue reported them, that resolve to the workflow's submit directory.
    """

    epoch: float | None = None
    source: str | None = None
    dagman_iwds: frozenset[str] = frozenset()


def _mtime(path: str) -> float | None:
    try:
        return os.stat(path).st_mtime
    except (OSError, ValueError):
        return None


def _same_directory(left: str, right: str) -> bool:
    try:
        return os.path.realpath(left) == os.path.realpath(right)
    except (OSError, ValueError):
        return False


def read_local_activity(
    snapshot: CoordinatorSnapshot,
    *,
    submit_dir: str | os.PathLike[str],
    recorded_submit_dir: str | os.PathLike[str] | None,
    dag_name: str,
) -> LocalActivity:
    """Stat the workflow's own DAGMan files and resolve queued DAGMan ``Iwd``s.

    A missing or unreadable file contributes nothing. HTCondor reports the
    directory a sub-workflow's DAGMan was started in, which Pegasus makes a
    symlink to the submit directory, so the two are compared resolved. A path
    recorded on another host is compared by its last component under the
    local copy of the same parent directory.
    """

    local = os.fspath(submit_dir)
    recorded = None if recorded_submit_dir is None else os.fspath(recorded_submit_dir)
    dag = os.path.basename(dag_name)
    stamps = [
        (stamp, source)
        for stamp, source in (
            (_mtime(os.path.join(local, f"{dag}.dagman.out")), DAGMAN_OUT),
            (_mtime(os.path.join(local, f"{dag}.nodes.log")), NODES_LOG),
        )
        if stamp is not None
    ]
    epoch, source = max(stamps, default=(None, None))

    iwds = set()
    latest = max(
        (
            result
            for result in getattr(snapshot, "scheduler_results", ())
            if result.request.kind is SchedulerQueryKind.QUEUE
        ),
        key=lambda result: result.request.clock.monotonic,
        default=None,
    )
    for evidence in () if latest is None else latest.related:
        iwd = evidence.payload.to_json_dict().get("Iwd")
        if not isinstance(iwd, str) or not iwd:
            continue
        candidates = [iwd]
        if recorded is not None and os.path.normpath(
            os.path.dirname(iwd)
        ) == os.path.normpath(os.path.dirname(recorded)):
            candidates.append(
                os.path.join(os.path.dirname(local), os.path.basename(iwd))
            )
        if any(_same_directory(candidate, local) for candidate in candidates) or (
            recorded is not None and os.path.normpath(iwd) == os.path.normpath(recorded)
        ):
            iwds.add(iwd)
    return LocalActivity(epoch, source, frozenset(iwds))


@dataclass(frozen=True, slots=True)
class QueueObservation:
    """What one healthy queue result says about the workflow's own DAGMan.

    ``complete`` is false when the result reached its row limit: rows may be
    missing, so the absence of a job proves nothing. ``dagman_is_wrapper``
    says the DAGMan row is the ``pegasus-dagman`` wrapper that runs
    ``condor_dagman`` as a child, which can outlive it.
    ``recorded_jobs_missing`` counts the jobs Stampede records as queued,
    running or held that a complete result does not show.
    """

    queried: bool = False
    complete: bool = False
    dagman_in_queue: bool | None = None
    jobs_in_queue: int | None = None
    dagman_cluster_id: int | None = None
    dagman_is_wrapper: bool | None = None
    recorded_jobs_missing: int | None = None


def _latest_queue(snapshot: CoordinatorSnapshot) -> SchedulerQueryResult | None:
    if snapshot.effective is None:
        return None
    workflow = snapshot.effective.workflow.workflow
    return max(
        (
            result
            for result in snapshot.scheduler_results
            if result.request.kind is SchedulerQueryKind.QUEUE
            and result.request.workflow == workflow
        ),
        key=lambda result: result.request.clock.monotonic,
        default=None,
    )


def _cluster(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def _in_queue(evidence: Sequence[Any]) -> list[dict[str, Any]]:
    rows = (item.payload.to_json_dict() for item in evidence)
    return [row for row in rows if row.get("JobStatus") not in _LEFT_QUEUE]


def observe_queue(
    snapshot: CoordinatorSnapshot, dagman_iwds: frozenset[str] = frozenset()
) -> QueueObservation:
    """Find the workflow's own DAGMan job and count its jobs in the queue.

    A root workflow's DAGMan is the scheduler-universe row of the workflow
    that no DAGMan submitted, so it has no ``DAGManJobId``. Every other
    scheduler-universe row of a workflow is a sub-workflow it is running, and
    counts as one of its jobs.

    A sub-workflow's DAGMan is a job of its parent and is not among the
    workflow's own rows. It is the row whose ``ClusterId`` the workflow's
    queued jobs name in ``DAGManJobId``, looked up among the DAGMan jobs of
    the tree; when the workflow has nothing queued, it is the DAGMan job whose
    ``Iwd`` is the submit directory. Neither test reads the row's workflow
    UUID, which is the parent's at any depth.
    """

    latest = _latest_queue(snapshot)
    if latest is None or latest.health.state is not HealthState.HEALTHY:
        return QueueObservation()
    assert snapshot.effective is not None
    identity = snapshot.effective.workflow.workflow
    own = _in_queue(latest.evidence)
    related = _in_queue(latest.related)

    def is_dagman(row: Mapping[str, Any]) -> bool:
        return row.get("JobUniverse") == _SCHEDULER_UNIVERSE

    if identity.wf_uuid == identity.root_wf_uuid:
        dagmans = [
            row
            for row in own
            if is_dagman(row) and _cluster(row.get("DAGManJobId")) is None
        ]
        jobs = len(own) - len(dagmans)
    else:
        jobs = len(own)
        by_cluster = {
            row.get("ClusterId"): row for row in (*related, *own) if is_dagman(row)
        }
        parents = {_cluster(row.get("DAGManJobId")) for row in own} - {None}
        if parents:
            dagmans = [by_cluster[c] for c in sorted(parents) if c in by_cluster]
        else:
            dagmans = [row for row in related if row.get("Iwd") in dagman_iwds]

    complete = len(latest.evidence) + len(latest.related) < latest.request.result_limit
    dagman = dagmans[0] if dagmans else None
    missing = None
    if complete:
        matched = scheduler_rows(
            snapshot.effective.jobs, (latest,), SchedulerQueryKind.QUEUE
        )
        missing = sum(
            job.lifecycle in _RECORDED_IN_QUEUE and job.exec_job_id not in matched
            for job in snapshot.effective.jobs
        )
    command = None if dagman is None else dagman.get("Cmd")
    return QueueObservation(
        queried=True,
        complete=complete,
        dagman_in_queue=True if dagman is not None else (False if complete else None),
        jobs_in_queue=jobs,
        dagman_cluster_id=None if dagman is None else _cluster(dagman.get("ClusterId")),
        dagman_is_wrapper=(
            None
            if dagman is None or not isinstance(command, str)
            else os.path.basename(command) == _DAGMAN_WRAPPER
        ),
        recorded_jobs_missing=missing,
    )


def queue_observation(
    snapshot: CoordinatorSnapshot,
) -> tuple[bool, bool | None, int | None]:
    """Report ``(queried, dagman_in_queue, jobs_in_queue)`` for the workflow.

    ``queried`` is true only for a healthy queue result. ``dagman_in_queue``
    is null when the queue was not queried, or when the result reached its
    row limit without showing DAGMan, which may then be among the omitted
    rows.
    """

    observed = observe_queue(snapshot)
    return observed.queried, observed.dagman_in_queue, observed.jobs_in_queue


def workflow_end_observed(snapshot: CoordinatorSnapshot) -> bool:
    """Require a healthy observation of this execution's terminal DAGMan ad.

    Empty queue results and child-only history can precede DAGMan's arrival in
    history. Their query timestamps alone cannot close the finalization race.
    """
    if snapshot.effective is None or not snapshot.authoritative_complete:
        return False
    workflow = snapshot.effective.workflow
    ended_at = workflow.ended_at
    if ended_at is None and workflow.transition is not None:
        ended_at = workflow.transition.identity.timestamp
    if ended_at is None:
        return False
    for result in snapshot.scheduler_results:
        if (
            result.request.workflow != workflow.workflow
            or result.health.state is not HealthState.HEALTHY
            or result.request.kind
            not in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}
            or result.health.checked_at_epoch < ended_at
        ):
            continue
        for evidence in result.evidence:
            row = {**evidence.target.to_json_dict(), **evidence.payload.to_json_dict()}
            # Removed jobs can have CompletionDate = 0, as in HTCondor 25.
            ended = row.get("CompletionDate") or row.get("EnteredCurrentStatus")
            if (
                row.get("JobUniverse") == 7
                and row.get("JobStatus") in {3, 4}
                and isinstance(ended, (int, float))
                and ended >= ended_at
            ):
                return True
    return False


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
        # A removed job may carry CompletionDate = 0; fall back to the removal time.
        ended = latest.get("CompletionDate") or latest.get("EnteredCurrentStatus", 0)
        started = snapshot.effective.workflow.started_at
        if started is None or ended < started:
            return None
    return (
        str(latest.get("RemoveReason") or "DAGMan was removed")
        if latest.get("JobStatus") == 3
        else None
    )
