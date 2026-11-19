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

import json
import os
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

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
from Pegasus.monitor.scheduler_evidence import (
    LocalActivity,
    observe_queue,
    observed_job_counts,
    read_local_activity,
)
from Pegasus.monitor.stall_detector import (
    StallAssessmentStatus,
    StallDetector,
    StallDetectorConfig,
    StallEventKind,
    StallKind,
    assess_stall_once,
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


# The helper stamps the job's last state change at epoch 10, the workflow start
# at epoch 1, and uses its ``monotonic`` argument as the wall clock too.


def test_one_shot_assessment_ages_the_last_recorded_state_change() -> None:
    assessment = assess_stall_once(_publication(1, 200, 1, "SUBMIT"))

    assert assessment.status is StallAssessmentStatus.STALLED
    assert assessment.kind is StallKind.IDLE_TOO_LONG
    assert assessment.last_progress_epoch == 10
    assert assessment.seconds_since_progress == 190
    document = assessment.to_json_dict()
    assert document["assessed"] is True
    assert document["stalled"] is True
    assert document["reason"] == "idle_too_long"
    assert document["method"] == "snapshot_timestamps"
    assert document["confirmed"] is False
    assert document["job_counts"]["queued"] == 1


def test_one_shot_assessment_applies_the_live_detector_idle_thresholds() -> None:
    quiet = assess_stall_once(_publication(1, 65, 1, "SUBMIT"))
    silent = assess_stall_once(_publication(1, 100, 1, "SUBMIT"))
    idle = assess_stall_once(_publication(1, 200, 1, "SUBMIT"))
    held = assess_stall_once(_publication(1, 100, 1, "JOB_HELD"))

    assert quiet.status is StallAssessmentStatus.NOT_STALLED
    assert quiet.reason == "within_thresholds"
    assert silent.kind is StallKind.NO_SEMANTIC_PROGRESS
    assert idle.kind is StallKind.IDLE_TOO_LONG
    assert held.kind is StallKind.ALL_HELD


def test_one_shot_assessment_honors_startup_grace_and_completion() -> None:
    starting = assess_stall_once(_publication(1, 30, 1, "JOB_HELD"))
    finished = assess_stall_once(
        _publication(1, 5000, 1, "JOB_SUCCESS", terminated=True)
    )

    assert starting.status is StallAssessmentStatus.NOT_STALLED
    assert starting.reason == "startup_grace"
    assert finished.status is StallAssessmentStatus.NOT_STALLED
    assert finished.reason == "workflow_complete"
    assert finished.to_json_dict()["stalled"] is False


def test_one_shot_assessment_separates_not_assessed_from_not_stalled() -> None:
    unhealthy = (SourceHealth(SourceName.STAMPEDE, HealthState.STALE, 200.0),)
    stale = assess_stall_once(
        _publication(1, 200, 1, "SUBMIT", source_health=unhealthy)
    )
    missing = assess_stall_once(
        replace(
            _publication(1, 200, 1, "SUBMIT"),
            effective=None,
            has_authoritative_base=False,
        )
    )

    for assessment, reason in (
        (stale, "stampede_not_healthy"),
        (missing, "authoritative_snapshot_unavailable"),
    ):
        assert assessment.status is StallAssessmentStatus.NOT_ASSESSED
        assert assessment.reason == reason
        document = assessment.to_json_dict()
        assert document["assessed"] is False
        assert document["stalled"] is None


def test_one_shot_assessment_leaves_the_live_detector_two_strike_rule() -> None:
    detector = _detector()
    publication = _publication(1, 0, 1, "SUBMIT")

    assert assess_stall_once(_publication(1, 200, 1, "SUBMIT")).kind is not None
    assert detector.check(publication) is None
    assert detector.check(_publication(2, 6, 1, "SUBMIT")) is None
    assert not detector.is_stalled


# The cases below replay the 2026-11-10 trial of the one-shot assessment
# against real workflows. Epochs are relative to the workflow start at 0.

_DAGMAN = {"ClusterId": 100, "ProcId": 0, "JobUniverse": 7, "JobStatus": 2}


def _trial_job(
    index: int,
    state: str | None,
    timestamp: float | None = None,
    *,
    workflow: WorkflowIdentity = WORKFLOW,
    name: str | None = None,
    scheduler_id: str | None = None,
) -> JobSnapshot:
    name = name or f"job{index}"
    identity = JobAttemptIdentity(index, index + 100, index)
    if state is None:
        return JobSnapshot(
            workflow,
            index,
            name,
            "compute",
            1,
            (),
            (),
            None,
            None,
            None,
            None,
            Provenance.DB_CONFIRMED,
        )
    stamp = Decimal(str(timestamp))
    transition = DBJobTransition(
        workflow,
        name,
        index,
        DBTransitionIdentity(index + 100, state, stamp, 1),
    )
    return JobSnapshot(
        workflow,
        index,
        name,
        "compute",
        1,
        (),
        (JobAttempt(identity, scheduler_id=scheduler_id or f"{200 + index}.0"),),
        identity,
        state,
        stamp,
        transition,
        Provenance.DB_CONFIRMED,
    )


def _queue_row(index: int, status: int) -> dict:
    return {
        "ClusterId": 200 + index,
        "ProcId": 0,
        "DAGNodeName": f"job{index}",
        "JobUniverse": 5,
        "JobStatus": status,
    }


def _evidence(rows) -> tuple[SchedulerEvidence, ...]:
    return tuple(
        SchedulerEvidence(
            SchedulerQueryKind.QUEUE,
            FrozenPayload.from_mapping(
                {key: row[key] for key in ("ClusterId", "ProcId") if key in row}
            ),
            FrozenPayload.from_mapping(row),
        )
        for row in rows
    )


def _queue(
    now: float,
    *rows: dict,
    limit: int = 100,
    workflow: WorkflowIdentity = WORKFLOW,
    related: tuple[dict, ...] = (),
    state: HealthState = HealthState.HEALTHY,
) -> SchedulerQueryResult:
    # ``related`` is passed only when a case has such rows, so the cases that
    # predate it still build their result the way they always did.
    extra = {"related": _evidence(related)} if related else {}
    return SchedulerQueryResult(
        SchedulerQueryRequest(
            workflow, SchedulerQueryKind.QUEUE, ClockSample(now, now), 5.0, limit
        ),
        SourceHealth(SourceName.CONDOR_QUEUE, state, now),
        0.0,
        _evidence(rows),
        **extra,
    )


def _trial(
    now: float,
    *jobs: JobSnapshot,
    queue: SchedulerQueryResult | None = None,
    workflow: WorkflowIdentity = WORKFLOW,
    started: float = 0,
) -> CoordinatorSnapshot:
    base = _publication(1, now, 1, "SUBMIT")
    began = Decimal(str(started))
    workflow = replace(
        base.effective.workflow,
        workflow=workflow,
        started_at=began,
        transition=replace(
            base.effective.workflow.transition,
            workflow=workflow,
            identity=DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", began),
        ),
    )
    return replace(
        base,
        effective=replace(base.effective, workflow=workflow, jobs=jobs),
        scheduler_results=() if queue is None else (queue,),
    )


def _done(count: int) -> tuple[JobSnapshot, ...]:
    return tuple(_trial_job(50 + index, "JOB_SUCCESS", 100) for index in range(count))


def _waiting(count: int) -> tuple[JobSnapshot, ...]:
    return tuple(_trial_job(70 + index, None) for index in range(count))


@pytest.mark.parametrize("elapsed", [28.62, 59.98, 60.92, 120.61, 169.0, 86400.0])
@pytest.mark.parametrize("with_queue", [False, True])
def test_one_shot_never_calls_a_running_job_a_stall(elapsed, with_queue) -> None:
    # once-running-30s.json, once-running-plateau.json and stall/: one job in
    # EXECUTE read as stalled at 60.92s and as running_plateau from 120.61s.
    now = 300.0 + elapsed
    queue = _queue(now, _DAGMAN, _queue_row(1, 2)) if with_queue else None

    assessment = assess_stall_once(
        _trial(now, _trial_job(1, "EXECUTE", 300), *_done(3), *_waiting(6), queue=queue)
    )

    document = assessment.to_json_dict()
    assert document["assessed"] is True
    assert document["stalled"] is False
    assert document["reason"] == "jobs_running"
    assert document["kind"] is None
    assert document["seconds_since_progress"] == pytest.approx(elapsed)
    assert document["scheduler"]["queue_queried"] is with_queue
    assert document["scheduler"]["dagman_in_queue"] is (True if with_queue else None)


@pytest.mark.parametrize("elapsed", [0.9, 36.8, 143.0, 5000.0])
def test_one_shot_all_held_ignores_jobs_unsubmitted_behind_the_hold(elapsed) -> None:
    # held3/: the only submitted job held with 5 unsubmitted behind it read as
    # within_thresholds indefinitely.
    now = 200.0 + elapsed
    assessment = assess_stall_once(
        _trial(now, _trial_job(1, "JOB_HELD", 200), *_done(3), *_waiting(5))
    )

    assert assessment.status is StallAssessmentStatus.STALLED
    assert assessment.kind is StallKind.ALL_HELD
    assert assessment.reason == "all_held"
    assert assessment.summary == (
        "All 1 submitted, unfinished job(s) are held; "
        "5 unsubmitted job(s) wait behind them"
    )


def test_one_shot_sees_a_hold_in_the_queue_before_stampede_records_it() -> None:
    # diagnose-held-before-db.json: the queue knew of the hold while Stampede
    # still recorded EXECUTE, for 35s to 600s in the trial.
    queue = _queue(230.0, _DAGMAN, _queue_row(1, 5))
    assessment = assess_stall_once(
        _trial(
            230.0, _trial_job(1, "EXECUTE", 200), *_done(3), *_waiting(5), queue=queue
        )
    )

    document = assessment.to_json_dict()
    assert document["kind"] == "all_held"
    assert document["job_counts"]["held"] == 0
    assert document["job_counts"]["running"] == 1
    assert document["observed_job_counts"]["held"] == 1
    assert document["observed_job_counts"]["running"] == 0
    assert document["observed_job_counts"]["total"] == 9


def test_one_shot_held_beside_an_idle_job_is_idle_not_all_held() -> None:
    # held/: one held job and one unmatchable idle job. Something can still
    # run, so the idle thresholds apply.
    jobs = (
        _trial_job(1, "JOB_HELD", 200),
        _trial_job(2, "SUBMIT", 150),
        *_done(3),
        *_waiting(5),
    )

    early = assess_stall_once(_trial(248.7, *jobs))
    late = assess_stall_once(_trial(334.0, *jobs))

    assert early.status is StallAssessmentStatus.NOT_STALLED
    assert early.reason == "within_thresholds"
    assert late.kind is StallKind.IDLE_TOO_LONG


def test_one_shot_flags_a_workflow_with_nothing_left_in_htcondor() -> None:
    # once-hier-root.json: WORKFLOW_STARTED, 13 of 16 jobs done, nothing
    # queued, running or held, 1,922s since progress, DAGMan crashed. It read
    # stalled:false, within_thresholds.
    jobs = (*_done(13), *_waiting(3))

    assessment = assess_stall_once(_trial(2022.0, *jobs, queue=_queue(2022.0)))

    document = assessment.to_json_dict()
    assert document["stalled"] is True
    assert document["reason"] == document["kind"] == "no_scheduler_activity"
    assert document["scheduler"] == {
        "queue_queried": True,
        "dagman_in_queue": False,
        "jobs_in_queue": 0,
        "queue_complete": True,
        "dagman_cluster_id": None,
        "dagman_is_wrapper": None,
        "recorded_jobs_missing": 0,
    }
    assert document["summary"] == (
        "HTCondor shows no DAGMan job and no job of this workflow in the queue, "
        "3 job(s) are unfinished, and no state change was recorded for 1922s"
    )


def test_one_shot_no_scheduler_activity_without_a_queue_says_so() -> None:
    jobs = (*_done(13), *_waiting(3))

    quiet = assess_stall_once(_trial(2022.0, *jobs))
    between_jobs = assess_stall_once(_trial(130.0, *jobs))

    assert quiet.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert quiet.dagman_in_queue is None
    assert quiet.summary.endswith("the HTCondor queue was not queried")
    assert between_jobs.status is StallAssessmentStatus.NOT_STALLED
    assert between_jobs.reason == "within_thresholds"


def test_one_shot_dagman_gone_overrides_stale_recorded_job_states() -> None:
    # DAGMan and monitord both died: Stampede still records a job in EXECUTE
    # that HTCondor no longer has.
    jobs = (_trial_job(1, "EXECUTE", 100), *_done(3), *_waiting(2))

    gone = assess_stall_once(_trial(400.0, *jobs, queue=_queue(400.0)))
    present = assess_stall_once(
        _trial(400.0, *jobs, queue=_queue(400.0, _DAGMAN, _queue_row(1, 2)))
    )
    just_left = assess_stall_once(_trial(130.0, *jobs, queue=_queue(130.0)))

    assert gone.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert present.reason == "jobs_running"
    assert just_left.status is StallAssessmentStatus.NOT_STALLED
    assert just_left.reason == "within_thresholds"


def test_one_shot_dagman_presence_is_unknown_when_the_queue_result_is_full() -> None:
    jobs = (*_done(2), *_waiting(2))
    rows = [_queue_row(index, 1) | {"DAGNodeName": f"other{index}"} for index in (8, 9)]

    assessment = assess_stall_once(
        _trial(2000.0, *jobs, queue=_queue(2000.0, *rows, limit=2))
    )

    assert assessment.dagman_in_queue is None
    assert assessment.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert assessment.summary.endswith("DAGMan was not confirmed in the HTCondor queue")


def test_one_shot_never_started_workflow_is_flagged_after_the_quiet_period() -> None:
    starting = assess_stall_once(
        _trial(59.0, *_waiting(4), queue=_queue(59.0, _DAGMAN))
    )
    silent = assess_stall_once(_trial(61.0, *_waiting(4), queue=_queue(61.0, _DAGMAN)))
    dead = assess_stall_once(_trial(900.0, *_waiting(4), queue=_queue(900.0)))

    assert starting.reason == "startup_grace"
    assert silent.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert silent.summary.endswith("DAGMan is still in the HTCondor queue")
    assert dead.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert dead.dagman_in_queue is False


def test_live_detector_never_reports_the_one_shot_only_kind() -> None:
    detector = _detector()
    base = _trial(0.0, *_done(13), *_waiting(3))
    results = [
        detector.check(replace(base, sequence=index, clock=ClockSample(now, now)))
        for index, now in enumerate((0.0, 100.0, 200.0, 300.0), 1)
    ]

    assert results == [None, None, None, None]


def test_summary_counts_name_their_source_and_carry_the_queue_view() -> None:
    # diagnose-held-before-db.json: job_counts.held was 0 beside a user_hold
    # finding, because counts came from Stampede and the finding from the queue.
    from Pegasus.monitor import json_output

    queue = _queue(230.0, _DAGMAN, _queue_row(1, 5))
    snapshot = _trial(
        230.0, _trial_job(1, "EXECUTE", 200), *_done(3), *_waiting(5), queue=queue
    )

    summary = json_output._summary(snapshot, None)

    assert summary["job_counts_source"] == "stampede"
    assert (summary["job_counts"]["held"], summary["job_counts"]["running"]) == (0, 1)
    observed = summary["observed_job_counts"]
    assert (observed["held"], observed["running"], observed["removed"]) == (1, 0, 0)
    assert observed["total"] == summary["job_counts"]["total"] == 9


def test_observed_counts_do_not_leave_jobs_queued_in_an_ended_workflow() -> None:
    # diagnose-removed.json: after pegasus-remove the terminated workflow still
    # counted queued 1, held 1, the last states Stampede recorded.
    from Pegasus.monitor import json_output

    jobs = (
        _trial_job(1, "JOB_HELD", 200),
        _trial_job(2, "SUBMIT", 150),
        *_done(3),
        *_waiting(5),
    )
    ended = replace(_trial(900.0, *jobs), authoritative_complete=True)

    summary = json_output._summary(ended, "via condor_rm (by user submit)")

    assert (summary["job_counts"]["queued"], summary["job_counts"]["held"]) == (1, 1)
    observed = summary["observed_job_counts"]
    assert (observed["queued"], observed["held"], observed["removed"]) == (0, 0, 2)
    assert (observed["succeeded"], observed["unsubmitted"]) == (3, 5)
    assert assess_stall_once(ended).reason == "workflow_complete"


def test_observed_counts_use_a_removal_seen_in_the_queue_of_a_running_workflow() -> (
    None
):
    queue = _queue(230.0, _DAGMAN, _queue_row(1, 3))
    snapshot = _trial(
        230.0, _trial_job(1, "JOB_HELD", 200), *_done(3), *_waiting(1), queue=queue
    )

    from Pegasus.monitor.scheduler_evidence import observed_job_counts

    observed = observed_job_counts(snapshot)

    assert (observed["held"], observed["removed"]) == (0, 1)


# The cases below replay the second trial of 2026-11-10 (monitor at
# 8663f4ac7): a sub-workflow's DAGMan was never found, Stampede trailing the
# queue read as a stall, and a killed DAGMan was never flagged.

ROOT_UUID = "wf-root"
SUB = WorkflowIdentity("wf-sub", ROOT_UUID)
INNER = WorkflowIdentity("wf-inner", ROOT_UUID)
STALL_FIXTURES = Path(__file__).parent / "fixtures" / "stall"

_ROOT_DAGMAN = {
    "ClusterId": 385,
    "ProcId": 0,
    "JobUniverse": 7,
    "JobStatus": 2,
    "Cmd": "/usr/lib/pegasus/venv/bin/pegasus-dagman",
    "Iwd": "/runs/run0001",
    "pegasus_wf_uuid": ROOT_UUID,
    "pegasus_root_wf_uuid": ROOT_UUID,
}
# A sub-workflow's DAGMan is a job of its parent and carries the parent's UUID.
_SUB_DAGMAN = {
    "ClusterId": 391,
    "ProcId": 0,
    "JobUniverse": 7,
    "JobStatus": 2,
    "Cmd": "/usr/bin/condor_dagman",
    "Iwd": "/runs/run0001/00/00/./inner_subwf",
    "DAGManJobId": 385,
    "DAGNodeName": "pegasus-plan_subwf",
    "pegasus_wf_uuid": ROOT_UUID,
    "pegasus_root_wf_uuid": ROOT_UUID,
}


def _sub_job_row(index: int, status: int, dagman: int = 391) -> dict:
    return _queue_row(index, status) | {"DAGManJobId": dagman}


def _fixture_snapshot(name: str) -> tuple[dict, CoordinatorSnapshot, LocalActivity]:
    data = json.loads((STALL_FIXTURES / name).read_text(encoding="utf-8"))
    workflow = WorkflowIdentity(**data["workflow"])
    jobs = [
        _trial_job(
            index,
            job["state"],
            job.get("timestamp"),
            workflow=workflow,
            name=job["exec_job_id"],
            scheduler_id=job.get("scheduler_id"),
        )
        for index, job in enumerate(data["jobs"], 1)
    ]
    jobs += [
        _trial_job(500 + index, None, workflow=workflow)
        for index in range(data.get("unsubmitted_jobs", 0))
    ]
    now = data["observed_at_epoch"]
    queue = _queue(
        now,
        *data["queue"]["own"],
        workflow=workflow,
        related=tuple(data["queue"]["related"]),
    )
    snapshot = _trial(
        now, *jobs, queue=queue, workflow=workflow, started=data["started_at"]
    )
    stamps = {
        "dagman_out": data["local"]["dagman_out_mtime"],
        "nodes_log": data["local"]["nodes_log_mtime"],
    }
    source = max(stamps, key=stamps.get)
    iwds = frozenset(
        row["Iwd"]
        for row in data["queue"]["related"]
        if os.path.normpath(row["Iwd"]) + ".000" == data.get("submit_dir")
    )
    return data, snapshot, LocalActivity(stamps[source], source, iwds)


def test_fixture_healthy_sub_workflow_is_not_stalled_as_its_job_leaves() -> None:
    data, snapshot, local = _fixture_snapshot("subworkflow-job-just-left-queue.json")
    assert data["captured_stall"]["stalled"] is True

    document = assess_stall_once(snapshot, local=local).to_json_dict()

    assert document["stalled"] is False
    assert document["reason"] == "within_thresholds"
    assert document["scheduler"] == {
        "queue_queried": True,
        "dagman_in_queue": True,
        "jobs_in_queue": 0,
        "queue_complete": True,
        "dagman_cluster_id": 391,
        "dagman_is_wrapper": False,
        "recorded_jobs_missing": 1,
    }
    assert document["seconds_since_progress"] == pytest.approx(
        data["captured_stall"]["seconds_since_progress"]
    )
    assert document["last_activity_epoch"] == 1794340775
    assert document["activity_source"] == "nodes_log"
    assert document["seconds_since_activity"] == pytest.approx(2.29, abs=0.01)


def test_fixture_sub_workflow_sample_needs_more_than_dagman_out() -> None:
    # DAGMan had not yet read the termination: its own file was 89 s old.
    # Found alone, the DAGMan job would have turned this sample into a stall
    # of the third kind; the schedd's write to the nodes log is what clears it.
    data, snapshot, local = _fixture_snapshot("subworkflow-job-just-left-queue.json")
    dagman_out_only = replace(
        local, epoch=data["local"]["dagman_out_mtime"], source="dagman_out"
    )

    assessment = assess_stall_once(snapshot, local=dagman_out_only)

    assert assessment.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert assessment.seconds_since_activity == pytest.approx(89.29, abs=0.01)


def test_fixture_killed_dagman_is_flagged_behind_its_wrapper() -> None:
    data, snapshot, local = _fixture_snapshot("dagman-killed-58min.json")
    assert data["captured_stall"]["reason"] == "jobs_running"

    document = assess_stall_once(snapshot, local=local).to_json_dict()

    assert document["stalled"] is True
    assert document["reason"] == document["kind"] == "no_scheduler_activity"
    assert document["scheduler"] == {
        "queue_queried": True,
        "dagman_in_queue": True,
        "jobs_in_queue": 0,
        "queue_complete": True,
        "dagman_cluster_id": 381,
        "dagman_is_wrapper": True,
        "recorded_jobs_missing": 1,
    }
    assert document["activity_source"] == "nodes_log"
    assert document["summary"] == (
        "The pegasus-dagman wrapper job (381) is in the HTCondor queue, which "
        "does not show that condor_dagman is alive, but none of the 1 job(s) "
        "recorded as queued, running or held is, and no state change was "
        "recorded for 3525s and DAGMan's files were last written 3479s ago"
    )


def test_killed_dagman_reads_running_while_the_orphan_is_in_the_queue() -> None:
    # +20 s of the trial: one sample cannot tell the orphan from a long job.
    # It is flagged no_progress_seconds after the orphan's last event.
    jobs = (_trial_job(1, "EXECUTE", 294), *_done(2), *_waiting(14))
    wrapper = _ROOT_DAGMAN | {"pegasus_wf_uuid": "wf-stall"}
    quiet = LocalActivity(297.0, "dagman_out")
    left = LocalActivity(340.0, "nodes_log")

    orphan = assess_stall_once(
        _trial(328.0, *jobs, queue=_queue(328.0, wrapper, _queue_row(1, 2))),
        local=quiet,
    )
    just_left = assess_stall_once(
        _trial(399.0, *jobs, queue=_queue(399.0, wrapper)), local=left
    )
    flagged = assess_stall_once(
        _trial(401.0, *jobs, queue=_queue(401.0, wrapper)), local=left
    )

    assert orphan.reason == "jobs_running"
    assert (just_left.status, just_left.reason) == (
        StallAssessmentStatus.NOT_STALLED,
        "within_thresholds",
    )
    assert flagged.kind is StallKind.NO_SCHEDULER_ACTIVITY


def test_one_shot_finds_a_sub_workflow_dagman_by_dagmanjobid() -> None:
    jobs = (
        _trial_job(1, "EXECUTE", 100, workflow=SUB),
        _trial_job(2, "JOB_SUCCESS", 90, workflow=SUB),
        _trial_job(3, None, workflow=SUB),
    )
    queue = _queue(
        400.0,
        _sub_job_row(1, 2),
        workflow=SUB,
        related=(_ROOT_DAGMAN, _SUB_DAGMAN),
    )
    snapshot = _trial(400.0, *jobs, queue=queue, workflow=SUB)

    observed = observe_queue(snapshot)
    assessment = assess_stall_once(snapshot)

    assert observed.dagman_in_queue is True
    assert observed.dagman_cluster_id == 391
    assert observed.dagman_is_wrapper is False
    assert assessment.reason == "jobs_running"
    # The DAGMan rows of the tree are not jobs of this workflow.
    assert observed.jobs_in_queue == 1
    counts = observed_job_counts(snapshot)
    assert (counts["running"], counts["succeeded"], counts["unsubmitted"]) == (1, 1, 1)
    assert sum(counts.values()) == 3


def test_one_shot_sub_workflow_dagman_gone_is_not_confused_with_the_roots() -> None:
    jobs = (_trial_job(1, "EXECUTE", 100, workflow=SUB), *_waiting(0))
    queue = _queue(400.0, workflow=SUB, related=(_ROOT_DAGMAN,))

    assessment = assess_stall_once(_trial(400.0, *jobs, queue=queue, workflow=SUB))

    assert assessment.dagman_in_queue is False
    assert assessment.kind is StallKind.NO_SCHEDULER_ACTIVITY


def test_one_shot_finds_a_sub_workflow_dagman_by_iwd_through_the_symlink(
    tmp_path: Path,
) -> None:
    # Pegasus plans the sub-workflow into <name>.000 and starts its DAGMan in
    # the symlink <name>, behind a "./" component, as the trial recorded.
    submit_dir = tmp_path / "00" / "00" / "inner_subwf.000"
    submit_dir.mkdir(parents=True)
    (tmp_path / "00" / "00" / "inner_subwf").symlink_to("inner_subwf.000")
    iwd = f"{tmp_path}/00/00/./inner_subwf"
    other = _SUB_DAGMAN | {"ClusterId": 392, "Iwd": f"{tmp_path}/00/00/./other_subwf"}
    jobs = (
        _trial_job(1, "JOB_SUCCESS", 100, workflow=SUB),
        _trial_job(2, None, workflow=SUB),
    )
    queue = _queue(
        130.0,
        workflow=SUB,
        related=(_ROOT_DAGMAN, other, _SUB_DAGMAN | {"Iwd": iwd}),
    )
    snapshot = _trial(130.0, *jobs, queue=queue, workflow=SUB)

    local = read_local_activity(
        snapshot,
        submit_dir=submit_dir,
        recorded_submit_dir=submit_dir,
        dag_name="inner-0.dag",
    )
    observed = observe_queue(snapshot, local.dagman_iwds)

    assert local.dagman_iwds == {iwd}
    assert observed.dagman_in_queue is True
    assert observed.dagman_cluster_id == 391
    assert observed.jobs_in_queue == 0
    # Without the resolved directory nothing identifies the row.
    assert observe_queue(snapshot).dagman_in_queue is False


def test_one_shot_matches_a_dagman_iwd_recorded_on_another_host(
    tmp_path: Path,
) -> None:
    # The run directory was copied: the queue reports the recorded path, the
    # symlink exists only under the local copy.
    submit_dir = tmp_path / "copy" / "inner_subwf.000"
    submit_dir.mkdir(parents=True)
    (tmp_path / "copy" / "inner_subwf").symlink_to("inner_subwf.000")
    queue = _queue(
        130.0,
        workflow=SUB,
        related=(_SUB_DAGMAN | {"Iwd": "/recorded/00/./inner_subwf"},),
    )
    snapshot = _trial(
        130.0, _trial_job(2, None, workflow=SUB), queue=queue, workflow=SUB
    )

    local = read_local_activity(
        snapshot,
        submit_dir=submit_dir,
        recorded_submit_dir="/recorded/00/inner_subwf.000",
        dag_name="inner-0.dag",
    )

    assert local.dagman_iwds == {"/recorded/00/./inner_subwf"}


def test_one_shot_follows_dagmanjobid_through_two_levels_of_nesting() -> None:
    # root (385) runs mid (391), which runs inner (397). Each DAGMan row
    # carries its parent's workflow UUID; neither lookup reads it.
    inner_dagman = {
        "ClusterId": 397,
        "ProcId": 0,
        "JobUniverse": 7,
        "JobStatus": 2,
        "Cmd": "/usr/bin/condor_dagman",
        "DAGManJobId": 391,
        "DAGNodeName": "job1",
        "pegasus_wf_uuid": SUB.wf_uuid,
        "pegasus_root_wf_uuid": ROOT_UUID,
    }
    inner = _trial(
        400.0,
        _trial_job(1, "EXECUTE", 100, workflow=INNER),
        queue=_queue(
            400.0,
            _sub_job_row(1, 2, dagman=397),
            workflow=INNER,
            related=(_ROOT_DAGMAN, _SUB_DAGMAN, inner_dagman),
        ),
        workflow=INNER,
    )
    # Seen from mid, inner's DAGMan is one of its own jobs, and the only one
    # queued: it names mid's DAGMan.
    mid = _trial(
        400.0,
        _trial_job(1, "EXECUTE", 100, workflow=SUB, scheduler_id="397.0"),
        queue=_queue(
            400.0, inner_dagman, workflow=SUB, related=(_ROOT_DAGMAN, _SUB_DAGMAN)
        ),
        workflow=SUB,
    )

    from_inner = observe_queue(inner)
    from_mid = observe_queue(mid)

    assert (from_inner.dagman_in_queue, from_inner.dagman_cluster_id) == (True, 397)
    assert from_inner.jobs_in_queue == 1
    assert (from_mid.dagman_in_queue, from_mid.dagman_cluster_id) == (True, 391)
    assert from_mid.jobs_in_queue == 1
    assert assess_stall_once(mid).reason == "jobs_running"


def test_one_shot_dead_root_dagman_is_not_masked_by_a_sub_workflow_dagman() -> None:
    # The root's DAGMan is gone; the sub-workflow it started is still queued
    # and is one of the root's jobs, not its DAGMan.
    sub_dagman = _SUB_DAGMAN | {
        "pegasus_wf_uuid": "wf-stall",
        "DAGNodeName": "job1",
        "ClusterId": 201,
    }
    jobs = (_trial_job(1, "EXECUTE", 100), *_done(2), *_waiting(2))
    snapshot = _trial(400.0, *jobs, queue=_queue(400.0, sub_dagman))

    observed = observe_queue(snapshot)

    assert observed.dagman_in_queue is False
    assert observed.dagman_cluster_id is None
    assert observed.jobs_in_queue == 1
    # The sub-workflow is still running, which one sample reads as a long job.
    assert assess_stall_once(snapshot).reason == "jobs_running"


def test_one_shot_root_running_a_sub_workflow_is_not_a_stall() -> None:
    # The root's own files are silent for as long as the sub-workflow runs.
    wrapper = _ROOT_DAGMAN | {"pegasus_wf_uuid": "wf-stall"}
    sub_dagman = _SUB_DAGMAN | {
        "pegasus_wf_uuid": "wf-stall",
        "DAGNodeName": "job1",
        "ClusterId": 201,
    }
    jobs = (_trial_job(1, "EXECUTE", 100), *_done(2), *_waiting(2))
    snapshot = _trial(7300.0, *jobs, queue=_queue(7300.0, wrapper, sub_dagman))

    assessment = assess_stall_once(snapshot, local=LocalActivity(105.0, "dagman_out"))

    assert assessment.reason == "jobs_running"
    assert (assessment.dagman_in_queue, assessment.jobs_in_queue) == (True, 1)
    assert assessment.queue.recorded_jobs_missing == 0


def test_one_shot_no_false_stall_right_after_pegasus_remove() -> None:
    # dead-rm/: for about 4 s the queue was empty and WORKFLOW_TERMINATED was
    # not yet recorded, 60 s or more after the last recorded change.
    jobs = (_trial_job(1, "EXECUTE", 100), *_done(2), *_waiting(14))
    snapshot = _trial(400.0, *jobs, queue=_queue(400.0))

    stampede_only = assess_stall_once(snapshot)
    with_files = assess_stall_once(snapshot, local=LocalActivity(397.0, "dagman_out"))

    assert stampede_only.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert with_files.status is StallAssessmentStatus.NOT_STALLED
    assert with_files.reason == "within_thresholds"
    assert with_files.last_activity_epoch == 397.0
    assert with_files.seconds_since_activity == pytest.approx(3.0)
    assert with_files.seconds_since_progress == pytest.approx(300.0)


@pytest.mark.parametrize("with_queue", [True, False])
def test_one_shot_no_false_idle_stall_as_a_job_leaves_the_queue(with_queue) -> None:
    # blocked/: Stampede still recorded SUBMIT for a job that had run and
    # left the queue; it read idle_too_long with jobs_in_queue 0.
    jobs = (_trial_job(1, "SUBMIT", 100), *_done(2), *_waiting(14))
    queue = _queue(400.0, _DAGMAN) if with_queue else None
    snapshot = _trial(400.0, *jobs, queue=queue)

    assessment = assess_stall_once(snapshot, local=LocalActivity(398.0, "nodes_log"))

    assert assessment.status is StallAssessmentStatus.NOT_STALLED
    assert assessment.reason == "within_thresholds"


def test_one_shot_stampede_lag_with_fresh_dagman_files_is_not_a_stall() -> None:
    # A busy workflow: monitord is minutes behind, so Stampede records jobs
    # in flight that have long left the queue. DAGMan is working, and writes.
    jobs = (
        _trial_job(1, "EXECUTE", 100),
        _trial_job(2, "EXECUTE", 100),
        _trial_job(3, "SUBMIT", 100),
        _trial_job(4, "JOB_HELD", 100),
        *_done(2),
        *_waiting(40),
    )
    snapshot = _trial(700.0, *jobs, queue=_queue(700.0, _DAGMAN))

    working = assess_stall_once(snapshot, local=LocalActivity(690.0, "dagman_out"))
    silent = assess_stall_once(snapshot, local=LocalActivity(630.0, "dagman_out"))

    assert working.queue.recorded_jobs_missing == 4
    assert working.status is StallAssessmentStatus.NOT_STALLED
    assert working.reason == "within_thresholds"
    assert silent.kind is StallKind.NO_SCHEDULER_ACTIVITY
    assert silent.summary.startswith("The DAGMan job (100) is in the HTCondor queue")


def test_one_shot_jobs_in_pre_or_post_scripts_are_not_missing_from_the_queue() -> None:
    jobs = (
        _trial_job(1, "POST_SCRIPT_STARTED", 100),
        _trial_job(2, "PRE_SCRIPT_STARTED", 100),
        *_done(2),
        *_waiting(3),
    )
    snapshot = _trial(4000.0, *jobs, queue=_queue(4000.0, _DAGMAN))

    assessment = assess_stall_once(snapshot)

    assert assessment.queue.recorded_jobs_missing == 0
    assert assessment.status is StallAssessmentStatus.NOT_STALLED
    assert assessment.reason == "within_thresholds"


@pytest.mark.parametrize(
    "queue",
    [
        pytest.param(
            lambda: _queue(4000.0, _DAGMAN, limit=1), id="reached-its-row-limit"
        ),
        pytest.param(
            lambda: _queue(4000.0, _DAGMAN, state=HealthState.STALE), id="stale"
        ),
        pytest.param(
            lambda: _queue(4000.0, _DAGMAN, state=HealthState.DEGRADED), id="degraded"
        ),
        pytest.param(
            lambda: _queue(4000.0, _DAGMAN, state=HealthState.UNAVAILABLE),
            id="unavailable",
        ),
        pytest.param(lambda: None, id="not-queried"),
    ],
)
def test_one_shot_an_incomplete_queue_result_never_shows_jobs_missing(queue) -> None:
    jobs = (_trial_job(1, "EXECUTE", 100), *_done(2), *_waiting(14))
    snapshot = _trial(4000.0, *jobs, queue=queue())

    assessment = assess_stall_once(snapshot, local=LocalActivity(110.0, "dagman_out"))

    assert assessment.queue.recorded_jobs_missing is None
    assert assessment.queue.complete is False
    assert assessment.status is StallAssessmentStatus.NOT_STALLED
    assert assessment.reason == "jobs_running"
    # The same sample from a complete result is the killed-DAGMan stall.
    complete = _trial(4000.0, *jobs, queue=_queue(4000.0, _DAGMAN))
    assert assess_stall_once(complete).kind is StallKind.NO_SCHEDULER_ACTIVITY


def test_local_activity_reads_the_workflows_own_dagman_files(tmp_path: Path) -> None:
    snapshot = _trial(400.0, *_done(1))
    (tmp_path / "wf-0.dag.dagman.out").write_text("x", encoding="utf-8")
    (tmp_path / "wf-0.dag.nodes.log").write_text("x", encoding="utf-8")
    os.utime(tmp_path / "wf-0.dag.dagman.out", (350.0, 350.0))
    os.utime(tmp_path / "wf-0.dag.nodes.log", (340.0, 340.0))
    # Another workflow's files in the same directory are not this one's.
    (tmp_path / "root-0.dag.dagman.out").write_text("x", encoding="utf-8")
    os.utime(tmp_path / "root-0.dag.dagman.out", (399.0, 399.0))

    def read() -> LocalActivity:
        return read_local_activity(
            snapshot,
            submit_dir=tmp_path,
            recorded_submit_dir=None,
            dag_name="wf-0.dag",
        )

    assert read() == LocalActivity(350.0, "dagman_out")
    os.utime(tmp_path / "wf-0.dag.nodes.log", (360.0, 360.0))
    assert read() == LocalActivity(360.0, "nodes_log")


def test_unreadable_dagman_files_fall_back_to_the_stampede_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jobs = (_trial_job(1, "SUBMIT", 100), *_done(2), *_waiting(3))
    snapshot = _trial(400.0, *jobs, queue=_queue(400.0, _DAGMAN, _queue_row(1, 1)))
    baseline = assess_stall_once(snapshot)

    missing = read_local_activity(
        snapshot, submit_dir=tmp_path, recorded_submit_dir=None, dag_name="wf-0.dag"
    )
    (tmp_path / "wf-0.dag.dagman.out").write_text("x", encoding="utf-8")

    def denied(path, *args, **kwargs):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(os, "stat", denied)
    unreadable = read_local_activity(
        snapshot, submit_dir=tmp_path, recorded_submit_dir=None, dag_name="wf-0.dag"
    )
    monkeypatch.undo()

    assert missing == unreadable == LocalActivity()
    for local in (missing, unreadable):
        assessment = assess_stall_once(snapshot, local=local)
        assert assessment == baseline
    assert baseline.kind is StallKind.IDLE_TOO_LONG
    assert baseline.last_activity_epoch == baseline.last_progress_epoch == 100.0
    assert baseline.activity_source == "stampede"
    assert baseline.seconds_since_activity == baseline.seconds_since_progress


def test_a_dagman_file_time_in_the_future_is_clamped_to_now() -> None:
    # The file server's clock runs an hour ahead of the reading host's.
    jobs = (_trial_job(1, "SUBMIT", 100), *_done(2), *_waiting(3))

    def assess(now: float):
        snapshot = _trial(now, *jobs, queue=_queue(now, _DAGMAN, _queue_row(1, 1)))
        return assess_stall_once(snapshot, local=LocalActivity(4000.0, "dagman_out"))

    ahead = assess(400.0)
    caught_up = assess(4121.0)

    assert ahead.last_activity_epoch == 400.0
    assert ahead.seconds_since_activity == 0.0
    assert ahead.status is StallAssessmentStatus.NOT_STALLED
    # Detection resumes once this host's clock passes the file's time: the
    # delay is bounded by the difference between the clocks.
    assert caught_up.seconds_since_activity == pytest.approx(121.0)
    assert caught_up.kind is StallKind.IDLE_TOO_LONG


def test_dagman_files_older_than_stampede_do_not_move_the_clock() -> None:
    jobs = (_trial_job(1, "SUBMIT", 100), *_done(2), *_waiting(3))
    snapshot = _trial(400.0, *jobs, queue=_queue(400.0, _DAGMAN, _queue_row(1, 1)))

    assessment = assess_stall_once(snapshot, local=LocalActivity(50.0, "dagman_out"))

    assert assessment.last_activity_epoch == 100.0
    assert assessment.activity_source == "stampede"
    assert assessment.kind is StallKind.IDLE_TOO_LONG
