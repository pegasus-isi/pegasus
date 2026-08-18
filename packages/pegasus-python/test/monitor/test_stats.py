"""Statistics over effective snapshots and optional scheduler evidence."""

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

from decimal import Decimal
from time import monotonic

import pytest

from Pegasus.monitor.coordinator import CoordinatorSnapshot
from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DBJobTransition,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    EffectiveSnapshot,
    FrozenPayload,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    Provenance,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    WorkflowIdentity,
    WorkflowSnapshot,
)
from Pegasus.monitor.stats import _history_rows, compute_workflow_stats

WORKFLOW = WorkflowIdentity("wf-stats", "wf-stats")


def _job(
    job_id: int,
    name: str,
    state: str | None,
    *,
    type_desc: str = "compute",
    attempts: tuple[JobAttempt, ...] | None = None,
    transformation: str | None = None,
) -> JobSnapshot:
    if attempts is None:
        identity = JobAttemptIdentity(job_id, job_id + 100, job_id)
        attempts = (JobAttempt(identity, scheduler_id=f"{job_id}.0"),)
    current = max(attempts, key=lambda item: item.identity.job_submit_seq).identity
    transition = (
        DBJobTransition(
            WORKFLOW,
            name,
            current.job_submit_seq,
            DBTransitionIdentity(
                current.job_instance_id, state, Decimal("100"), job_id
            ),
        )
        if state is not None
        else None
    )
    return JobSnapshot(
        WORKFLOW,
        job_id,
        name,
        type_desc,
        1,
        () if transformation is None else (transformation,),
        attempts,
        current,
        state,
        None if transition is None else transition.identity.timestamp,
        transition,
        Provenance.DB_CONFIRMED,
    )


def _snapshot(
    jobs: tuple[JobSnapshot, ...],
    *,
    terminated: bool = False,
    published: float = 200.0,
) -> EffectiveSnapshot:
    state = "WORKFLOW_TERMINATED" if terminated else "WORKFLOW_STARTED"
    timestamp = Decimal("190" if terminated else "10")
    transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, state, timestamp),
        0,
        0 if terminated else None,
    )
    workflow = WorkflowSnapshot(
        WORKFLOW,
        1,
        state,
        0 if terminated else None,
        0,
        Decimal("10"),
        Decimal("190") if terminated else None,
        transition,
    )
    return EffectiveSnapshot(
        SnapshotEpoch(1),
        workflow,
        jobs,
        DatabaseGeneration(1, 2, 3),
        None,
        published,
        published,
    )


def _result(
    kind: SchedulerQueryKind,
    rows: list[dict],
    *,
    health: HealthState = HealthState.HEALTHY,
    summary: dict | None = None,
) -> SchedulerQueryResult:
    request = SchedulerQueryRequest(
        WORKFLOW,
        kind,
        ClockSample(200.0, 100.0),
        5.0,
        max(100, len(rows)),
    )
    source = {
        SchedulerQueryKind.HISTORY: SourceName.CONDOR_HISTORY,
        SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
    }[kind]
    return SchedulerQueryResult(
        request,
        SourceHealth(source, health, 200.0),
        0.0,
        tuple(
            SchedulerEvidence(
                kind,
                FrozenPayload.from_mapping({"index": index}),
                FrozenPayload.from_mapping(row),
            )
            for index, row in enumerate(rows)
        ),
        FrozenPayload.from_mapping(summary or {}),
    )


def test_counts_each_effective_job_once() -> None:
    jobs = (
        _job(1, "ok", "JOB_SUCCESS"),
        _job(2, "bad", "JOB_FAILURE"),
        _job(3, "held", "JOB_HELD"),
        _job(4, "queued", "SUBMIT"),
        _job(5, "infra", "JOB_SUCCESS", type_desc="stage-in-tx"),
    )
    stats = compute_workflow_stats(_snapshot(jobs))
    assert (stats.total_jobs, stats.compute_jobs, stats.infra_jobs) == (5, 4, 1)
    assert (stats.succeeded, stats.failed, stats.held, stats.queued) == (2, 1, 1, 1)


def test_attempt_metrics_use_only_current_attempt() -> None:
    old_id = JobAttemptIdentity(1, 101, 1)
    current_id = JobAttemptIdentity(1, 102, 2)
    job = _job(
        1,
        "compute",
        "JOB_SUCCESS",
        attempts=(
            JobAttempt(
                old_id,
                scheduler_id="9.0",
                start_time=Decimal("0"),
                end_time=Decimal("1000"),
                maxrss_kb=999999,
            ),
            JobAttempt(
                current_id,
                scheduler_id="10.0",
                start_time=Decimal("100"),
                end_time=Decimal("130"),
                maxrss_kb=2048,
            ),
        ),
        transformation="example::compute",
    )
    stats = compute_workflow_stats(_snapshot((job,), terminated=True))
    assert stats.total_compute_time == 30.0
    assert stats.peak_maxrss_kb == 2048
    assert stats.longest_job_name == "example::compute"
    assert stats.wall_time == 180.0
    assert stats.parallelism == pytest.approx(1 / 6)


def test_history_matches_only_current_scheduler_attempt_and_reports_coverage() -> None:
    identity = JobAttemptIdentity(1, 101, 1)
    job = _job(
        1,
        "compute",
        "JOB_SUCCESS",
        attempts=(JobAttempt(identity, scheduler_id="10.0"),),
    )
    history = _result(
        SchedulerQueryKind.HISTORY,
        [
            {
                "ClusterId": 9,
                "ProcId": 0,
                "DAGNodeName": "compute",
                "RemoteWallClockTime": 100,
                "RemoteUserCpu": 1,
                "RequestCpus": 1,
            },
            {
                "ClusterId": 10,
                "ProcId": 0,
                "DAGNodeName": "compute",
                "RemoteWallClockTime": 20,
                "RemoteUserCpu": 10,
                "RequestCpus": 2,
                "RequestMemory": 100,
                "ImageSize": 51200,
                "QDate": 10,
                "JobStartDate": 15,
                "BytesSent": 2,
                "BytesRecvd": 3,
                "NumJobStarts": 2,
                "LastRemoteHost": "slot1@worker.example",
            },
        ],
    )
    stats = compute_workflow_stats(_snapshot((job,)), (history,))
    assert stats.cpu_eff_mean == pytest.approx(0.25)
    assert stats.mem_eff_mean == pytest.approx(0.5)
    assert stats.wait_mean == 5.0
    assert stats.transfer_bytes == 5
    assert stats.retry_count == 1
    assert stats.hosts == ("worker (1)",)
    coverage = stats.scheduler_coverage[0]
    assert coverage.evidence_count == 2
    assert coverage.matched_current_jobs == coverage.eligible_current_jobs == 1
    assert coverage.complete


def test_history_name_fallback_must_be_unique_for_current_attempt() -> None:
    identity = JobAttemptIdentity(1, 101, 1)
    job = _job(
        1,
        "compute",
        "JOB_SUCCESS",
        attempts=(JobAttempt(identity, scheduler_id=None),),
    )
    history = _result(
        SchedulerQueryKind.HISTORY,
        [
            {
                "ClusterId": 9,
                "DAGNodeName": "compute",
                "RemoteWallClockTime": 100,
                "RemoteUserCpu": 1,
            },
            {
                "ClusterId": 10,
                "DAGNodeName": "compute",
                "RemoteWallClockTime": 20,
                "RemoteUserCpu": 10,
            },
        ],
    )
    stats = compute_workflow_stats(_snapshot((job,)), (history,))
    assert stats.cpu_eff_mean is None
    assert stats.scheduler_coverage[0].matched_current_jobs == 0
    assert not stats.scheduler_coverage[0].complete


def test_history_matching_is_linear_at_scheduler_evidence_cap() -> None:
    jobs = tuple(_job(index, f"job{index}", "JOB_SUCCESS") for index in range(1, 5001))
    history = _result(
        SchedulerQueryKind.HISTORY,
        [
            {
                "ClusterId": 10000 + index,
                "ProcId": 0,
                "DAGNodeName": f"other{index}",
            }
            for index in range(4096)
        ],
    )
    started = monotonic()
    rows, coverage = _history_rows(jobs, history)
    elapsed = monotonic() - started
    assert rows == []
    assert coverage.matched_current_jobs == 0
    assert elapsed < 1.0


def test_optional_scheduler_absence_is_explicit() -> None:
    stats = compute_workflow_stats(_snapshot((_job(1, "x", "JOB_SUCCESS"),)))
    assert stats.cpu_eff_mean is None
    assert stats.pool_total_cpus is None
    assert [item.health for item in stats.scheduler_coverage] == [
        HealthState.UNAVAILABLE,
        HealthState.UNAVAILABLE,
    ]


def test_pool_summary_and_stale_coverage() -> None:
    pool = _result(
        SchedulerQueryKind.POOL,
        [],
        health=HealthState.STALE,
        summary={"machines": 2, "total_cpus": 8, "total_gpus": 1},
    )
    stats = compute_workflow_stats(_snapshot(()), (pool,))
    assert (stats.pool_machines, stats.pool_total_cpus, stats.pool_total_gpus) == (
        2,
        8,
        1,
    )
    assert stats.scheduler_coverage[1].health is HealthState.STALE


def test_finality_and_provenance_are_explicit() -> None:
    running = compute_workflow_stats(_snapshot((), terminated=False))
    final = compute_workflow_stats(_snapshot((), terminated=True))
    assert not running.authoritative_final
    assert final.authoritative_final
    assert final.workflow_provenance is Provenance.DB_CONFIRMED
    assert final.db_confirmed_jobs == 0
    assert final.to_dict()["workflow_provenance"] == "db_confirmed"


def test_coordinator_finality_is_not_reconstructed_by_analysis() -> None:
    effective = _snapshot((), terminated=True)
    publication = CoordinatorSnapshot(
        1,
        ClockSample(200.0, 100.0),
        effective,
        (),
        (),
        1,
        1,
        None,
        True,
        False,
    )
    assert not compute_workflow_stats(publication).authoritative_final
