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

from dataclasses import replace
from decimal import Decimal

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
    index: int, state: str | None, timestamp: float | None = None
) -> JobSnapshot:
    name = f"job{index}"
    identity = JobAttemptIdentity(index, index + 100, index)
    if state is None:
        return JobSnapshot(
            WORKFLOW,
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
        WORKFLOW,
        name,
        index,
        DBTransitionIdentity(index + 100, state, stamp, 1),
    )
    return JobSnapshot(
        WORKFLOW,
        index,
        name,
        "compute",
        1,
        (),
        (JobAttempt(identity, scheduler_id=f"{200 + index}.0"),),
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


def _queue(now: float, *rows: dict, limit: int = 100) -> SchedulerQueryResult:
    return SchedulerQueryResult(
        SchedulerQueryRequest(
            WORKFLOW, SchedulerQueryKind.QUEUE, ClockSample(now, now), 5.0, limit
        ),
        SourceHealth(SourceName.CONDOR_QUEUE, HealthState.HEALTHY, now),
        0.0,
        tuple(
            SchedulerEvidence(
                SchedulerQueryKind.QUEUE,
                FrozenPayload.from_mapping(
                    {key: row[key] for key in ("ClusterId", "ProcId") if key in row}
                ),
                FrozenPayload.from_mapping(row),
            )
            for row in rows
        ),
    )


def _trial(
    now: float, *jobs: JobSnapshot, queue: SchedulerQueryResult | None = None
) -> CoordinatorSnapshot:
    base = _publication(1, now, 1, "SUBMIT")
    workflow = replace(
        base.effective.workflow,
        started_at=Decimal("0"),
        transition=replace(
            base.effective.workflow.transition,
            identity=DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", Decimal("0")),
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
    present = assess_stall_once(_trial(400.0, *jobs, queue=_queue(400.0, _DAGMAN)))
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
