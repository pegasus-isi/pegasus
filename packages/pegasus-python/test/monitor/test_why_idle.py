"""Pure why-idle analysis and scheduler availability distinctions."""

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
from Pegasus.monitor.why_idle import SourceDisposition, analyze_why_idle

WORKFLOW = WorkflowIdentity("wf-idle", "wf-idle")


def _job(index: int, state: str | None) -> JobSnapshot:
    identity = JobAttemptIdentity(index, index + 100, index)
    transition = (
        DBJobTransition(
            WORKFLOW,
            f"job{index}",
            index,
            DBTransitionIdentity(index + 100, state, Decimal("10"), index),
        )
        if state is not None
        else None
    )
    return JobSnapshot(
        WORKFLOW,
        index,
        f"job{index}",
        "compute",
        1,
        (),
        (JobAttempt(identity),),
        identity,
        state,
        None if transition is None else transition.identity.timestamp,
        transition,
        Provenance.DB_CONFIRMED,
    )


def _snapshot(*states: str | None) -> EffectiveSnapshot:
    workflow_transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", Decimal("1")),
        0,
    )
    return EffectiveSnapshot(
        SnapshotEpoch(1),
        WorkflowSnapshot(
            WORKFLOW,
            1,
            "WORKFLOW_STARTED",
            None,
            0,
            Decimal("1"),
            None,
            workflow_transition,
        ),
        tuple(_job(index, state) for index, state in enumerate(states, 1)),
        DatabaseGeneration(1, 2, 3),
        None,
        20.0,
        20.0,
    )


_SOURCE = {
    SchedulerQueryKind.QUEUE: SourceName.CONDOR_QUEUE,
    SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
    SchedulerQueryKind.PRIORITY: SourceName.CONDOR_PRIORITY,
    SchedulerQueryKind.NEGOTIATOR: SourceName.CONDOR_NEGOTIATOR,
}


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
        ClockSample(20.0, 20.0),
        5.0,
        max(100, len(rows)),
    )
    return SchedulerQueryResult(
        request,
        SourceHealth(_SOURCE[kind], health, 20.0),
        0.0,
        tuple(
            SchedulerEvidence(
                kind,
                FrozenPayload.from_mapping({"row": index}),
                FrozenPayload.from_mapping(row),
            )
            for index, row in enumerate(rows)
        ),
        FrozenPayload.from_mapping(summary or {}),
    )


def _queue(owner: str = "alice", **requests) -> SchedulerQueryResult:
    row = {
        "ClusterId": 10,
        "ProcId": 0,
        "DAGNodeName": "job1",
        "JobStatus": 1,
        "Owner": owner,
        "RequestCpus": 1,
        "RequestMemory": 100,
        "RequestDisk": 10,
        "RequestGpus": 0,
    }
    row.update(requests)
    return _result(SchedulerQueryKind.QUEUE, [row])


def test_no_waiting_jobs_returns_simple_finding() -> None:
    result = analyze_why_idle(_snapshot("JOB_SUCCESS"))
    assert result.idle_jobs == ()
    assert "No queued or unsubmitted jobs" in result.findings[0]


def test_unsubmitted_jobs_do_not_require_scheduler_sources() -> None:
    result = analyze_why_idle(_snapshot(None))
    assert result.unsubmitted_jobs == 1
    assert result.queued_jobs == 0
    assert any("DAGMan" in item for item in result.suggestions)


def test_healthy_empty_stale_and_unavailable_are_distinct() -> None:
    healthy_empty = _result(SchedulerQueryKind.QUEUE, [])
    stale_pool = _result(SchedulerQueryKind.POOL, [], health=HealthState.STALE)
    result = analyze_why_idle(_snapshot("SUBMIT"), (healthy_empty, stale_pool))
    assessments = {item.kind: item.disposition for item in result.sources}
    assert assessments[SchedulerQueryKind.QUEUE] is SourceDisposition.HEALTHY_EMPTY
    assert assessments[SchedulerQueryKind.POOL] is SourceDisposition.STALE
    assert assessments[SchedulerQueryKind.PRIORITY] is SourceDisposition.UNAVAILABLE


def test_unavailable_pool_without_cache_keeps_capacity_unknown() -> None:
    pool = _result(
        SchedulerQueryKind.POOL, [], health=HealthState.UNAVAILABLE, summary={}
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (_queue(), pool))
    assert result.pool_total_cpus is None
    assert result.pool_idle_cpus is None
    assert result.pool_machines is None
    assert result.pool_total_slots is None


def test_explicit_no_condor_is_disabled_not_unavailable() -> None:
    disabled = frozenset(SchedulerQueryKind)
    result = analyze_why_idle(_snapshot("SUBMIT"), disabled_scheduler_kinds=disabled)
    assert all(
        item.disposition is SourceDisposition.DISABLED for item in result.sources
    )
    assert any("disabled" in item for item in result.findings)


def test_healthy_empty_priority_is_not_reported_unavailable() -> None:
    priority = _result(SchedulerQueryKind.PRIORITY, [])
    result = analyze_why_idle(_snapshot("SUBMIT"), (_queue("alice"), priority))
    assert any("succeeded" in item for item in result.findings)
    assert not any("unavailable or degraded" in item for item in result.findings)


def test_resource_fit_is_per_slot_not_aggregate_pool_totals() -> None:
    queue = _queue(RequestCpus=8, RequestMemory=100)
    pool = _result(
        SchedulerQueryKind.POOL,
        [
            {"Name": "slot1@a", "State": "Unclaimed", "Cpus": 4, "Memory": 1000},
            {"Name": "slot1@b", "State": "Unclaimed", "Cpus": 4, "Memory": 1000},
        ],
        summary={"total_cpus": 8, "idle_cpus": 8},
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (queue, pool))
    assert len(result.requirement_mismatches) == 1
    assert result.requirement_mismatches[0].reason == "exceeds_single_slot_capacity"


def test_capable_but_claimed_slot_reports_busy_not_impossible() -> None:
    queue = _queue(RequestCpus=4)
    pool = _result(
        SchedulerQueryKind.POOL,
        [
            {
                "Name": "slot1@a",
                "State": "Claimed",
                "Cpus": 8,
                "Memory": 1000,
                "Disk": 100,
            }
        ],
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (queue, pool))
    assert result.requirement_mismatches[0].reason == "all_capable_slots_busy"


def test_partial_pool_ad_reports_unknown_instead_of_impossible() -> None:
    queue = _queue(RequestCpus=4, RequestMemory=100)
    pool = _result(
        SchedulerQueryKind.POOL,
        [{"Name": "slot1@a", "State": "Unclaimed", "Cpus": 8}],
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (queue, pool))
    assert result.requirement_mismatches[0].reason == "slot_capacity_unknown"


def test_expression_resource_request_is_unknown_not_zero() -> None:
    queue = _queue(RequestMemory="ifthenelse(MemoryUsage, 2048, 1024)")
    pool = _result(
        SchedulerQueryKind.POOL,
        [
            {
                "Name": "slot1@a",
                "State": "Unclaimed",
                "Cpus": 8,
                "Memory": 4096,
                "Disk": 100,
                "GPUs": 0,
            }
        ],
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (queue, pool))
    mismatch = result.requirement_mismatches[0]
    assert mismatch.requested_memory_mb is None
    assert mismatch.reason == "slot_capacity_unknown"


def test_resource_fit_analysis_is_bounded_at_source_caps() -> None:
    count = 4096
    snapshot = _snapshot(*(state for state in ("SUBMIT",) * count))
    queue = _result(
        SchedulerQueryKind.QUEUE,
        [
            {
                "ClusterId": index,
                "ProcId": 0,
                "DAGNodeName": f"job{index + 1}",
                "JobStatus": 1,
                "Owner": "alice",
                "RequestCpus": index + 2,
                "RequestMemory": 1024,
            }
            for index in range(count)
        ],
    )
    pool = _result(
        SchedulerQueryKind.POOL,
        [
            {
                "Name": f"slot{index}",
                "State": "Claimed",
                "Cpus": 1,
                "Memory": 1024,
            }
            for index in range(count)
        ],
    )
    started = monotonic()
    result = analyze_why_idle(snapshot, (queue, pool))
    elapsed = monotonic() - started
    assert any(
        item.reason == "analysis_limit" for item in result.requirement_mismatches
    )
    assert elapsed < 1.0


def test_unique_queue_owner_is_used_without_environment_fallback(monkeypatch) -> None:
    monkeypatch.setenv("USER", "wrong-user")
    priority = _result(
        SchedulerQueryKind.PRIORITY,
        [
            {"Name": "alice@example", "EffectivePriority": 10.0},
            {"Name": "bob@example", "EffectivePriority": 2.0},
        ],
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (_queue("alice"), priority))
    assert result.workflow_owner == "alice"
    assert result.priority is not None
    assert result.priority.principal == "alice@example"
    assert result.priority.better_priority_users == 1


def test_ambiguous_queue_owners_do_not_guess_priority_owner() -> None:
    queue = _result(
        SchedulerQueryKind.QUEUE,
        [
            {"DAGNodeName": "job1", "JobStatus": 1, "Owner": "alice"},
            {"DAGNodeName": "job1", "JobStatus": 1, "Owner": "bob"},
        ],
    )
    result = analyze_why_idle(_snapshot("SUBMIT"), (queue,))
    assert result.workflow_owner is None
    assert result.priority is None
    assert any("ambiguous" in item for item in result.findings)


def test_explicit_owner_takes_precedence_and_negotiator_is_optional() -> None:
    priority = _result(
        SchedulerQueryKind.PRIORITY,
        [{"Name": "planner@example", "EffectivePriority": 1.0}],
    )
    negotiator = _result(
        SchedulerQueryKind.NEGOTIATOR,
        [{"Name": "NEGOTIATOR"}],
        summary={"last_cycle_duration_seconds": 70.0, "last_cycle_matches": 0},
    )
    result = analyze_why_idle(
        _snapshot("SUBMIT"),
        (_queue("different"), priority, negotiator),
        workflow_owner="planner",
    )
    assert result.workflow_owner == "planner"
    assert result.negotiation_matches == 0
    assert any("over 60 seconds" in item for item in result.suggestions)
