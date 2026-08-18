"""Semantic-progress stall detection tests."""

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

from Pegasus.monitor.coordinator import CoordinatorSnapshot
from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DBJobTransition,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    EffectiveSnapshot,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    Provenance,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    WorkflowIdentity,
    WorkflowSnapshot,
)
from Pegasus.monitor.stall_detector import (
    StallDetector,
    StallDetectorConfig,
    StallEventKind,
    StallKind,
)

WORKFLOW = WorkflowIdentity("wf-stall", "wf-stall")


def _job(state: str) -> JobSnapshot:
    identity = JobAttemptIdentity(1, 2, 3)
    transition = DBJobTransition(
        WORKFLOW,
        "job",
        3,
        DBTransitionIdentity(2, state, Decimal("10"), 1),
    )
    return JobSnapshot(
        WORKFLOW,
        1,
        "job",
        "compute",
        1,
        (),
        (JobAttempt(identity),),
        identity,
        state,
        Decimal("10"),
        transition,
        Provenance.DB_CONFIRMED,
    )


def _publication(
    sequence: int,
    monotonic: float,
    semantic_progress: int,
    state: str,
    *,
    terminated: bool = False,
    pending: int = 0,
    source_health: tuple[SourceHealth, ...] = (),
) -> CoordinatorSnapshot:
    workflow_state = "WORKFLOW_TERMINATED" if terminated else "WORKFLOW_STARTED"
    workflow_transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(
            1, workflow_state, Decimal("20" if terminated else "1")
        ),
        0,
        0 if terminated else None,
    )
    workflow = WorkflowSnapshot(
        WORKFLOW,
        1,
        workflow_state,
        0 if terminated else None,
        0,
        Decimal("1"),
        Decimal("20") if terminated else None,
        workflow_transition,
    )
    effective = EffectiveSnapshot(
        SnapshotEpoch(sequence),
        workflow,
        (_job(state),),
        DatabaseGeneration(1, 2, 3),
        None,
        monotonic,
        monotonic,
        source_health,
    )
    return CoordinatorSnapshot(
        sequence=sequence,
        clock=ClockSample(monotonic, monotonic),
        effective=effective,
        source_health=source_health,
        scheduler_results=(),
        pending_tail_events=pending,
        unconfirmed_tail_events=(),
        last_tail_event_age=None,
        semantic_progress=semantic_progress,
        latest_effective_event=None,
        has_authoritative_base=True,
        authoritative_complete=terminated,
    )


def _detector() -> StallDetector:
    return StallDetector(
        StallDetectorConfig(
            cooldown_seconds=100,
            no_progress_seconds=5,
            running_plateau_seconds=5,
            idle_threshold_seconds=5,
            startup_grace_seconds=0,
        )
    )


def test_two_consecutive_strikes_required() -> None:
    detector = _detector()
    assert detector.check(_publication(1, 0, 1, "SUBMIT")) is None
    assert detector.check(_publication(2, 10, 1, "SUBMIT")) is None
    result = detector.check(_publication(3, 11, 1, "SUBMIT"))
    assert result is not None
    assert result.event is StallEventKind.DETECTED
    assert result.kind is StallKind.IDLE_TOO_LONG


def test_publication_and_pending_tail_changes_are_not_semantic_progress() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 4, "EXECUTE", pending=0))
    detector.check(_publication(2, 10, 4, "EXECUTE", pending=20))
    result = detector.check(_publication(3, 11, 4, "EXECUTE", pending=0))
    assert result is not None
    assert result.kind is StallKind.RUNNING_PLATEAU
    assert result.semantic_progress == 4


def test_semantic_progress_resolves_confirmed_stall() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    detector.check(_publication(2, 10, 1, "SUBMIT"))
    detected = detector.check(_publication(3, 11, 1, "SUBMIT"))
    assert detected is not None
    resolved = detector.check(_publication(4, 12, 2, "SUBMIT"))
    assert resolved is not None
    assert resolved.event is StallEventKind.RESOLVED
    assert not detector.is_stalled


def test_all_held_still_obeys_two_strike_rule() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "JOB_HELD"))
    assert detector.check(_publication(2, 1, 1, "JOB_HELD")) is None
    result = detector.check(_publication(3, 2, 1, "JOB_HELD"))
    assert result is not None and result.kind is StallKind.ALL_HELD


def test_duplicate_publication_does_not_supply_second_strike() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    first = _publication(2, 10, 1, "SUBMIT")
    assert detector.check(first) is None
    assert detector.check(first) is None
    assert not detector.is_stalled


def test_authoritative_completion_resolves_stall() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    detector.check(_publication(2, 10, 1, "SUBMIT"))
    detector.check(_publication(3, 11, 1, "SUBMIT"))
    result = detector.check(_publication(4, 12, 1, "JOB_SUCCESS", terminated=True))
    assert result is not None
    assert result.event is StallEventKind.RESOLVED
    assert result.summary == "Workflow completed"


def test_source_outage_time_is_not_counted_as_workflow_stall() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    unavailable = (
        SourceHealth(SourceName.STAMPEDE, HealthState.STALE, 10.0),
        SourceHealth(SourceName.LIVE_TAIL, HealthState.UNAVAILABLE, 10.0),
    )
    assert (
        detector.check(_publication(2, 100, 1, "SUBMIT", source_health=unavailable))
        is None
    )
    healthy = (SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, 101.0),)
    assert (
        detector.check(_publication(3, 101, 1, "SUBMIT", source_health=healthy)) is None
    )


def test_waiting_tail_does_not_make_stale_database_observable() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    health = (
        SourceHealth(SourceName.STAMPEDE, HealthState.STALE, 10.0),
        SourceHealth(SourceName.LIVE_TAIL, HealthState.WAITING, 10.0),
    )
    assert (
        detector.check(_publication(2, 100, 1, "SUBMIT", source_health=health)) is None
    )
    assert (
        detector.check(_publication(3, 200, 1, "SUBMIT", source_health=health)) is None
    )
    assert not detector.is_stalled


def test_stall_evidence_names_actual_authoritative_source() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT"))
    detector.check(_publication(2, 10, 1, "SUBMIT"))
    result = detector.check(_publication(3, 11, 1, "SUBMIT"))
    assert result is not None
    assert [(item.source, item.code) for item in result.evidence] == [
        (SourceName.STAMPEDE, "authoritative_base")
    ]


def test_stall_evidence_includes_live_pending_source() -> None:
    detector = _detector()
    detector.check(_publication(1, 0, 1, "SUBMIT", pending=1))
    detector.check(_publication(2, 10, 1, "SUBMIT", pending=1))
    result = detector.check(_publication(3, 11, 1, "SUBMIT", pending=1))
    assert result is not None
    assert [(item.source, item.code) for item in result.evidence] == [
        (SourceName.STAMPEDE, "authoritative_base"),
        (SourceName.LIVE_TAIL, "live_overlay"),
    ]
