"""Deterministic reconciliation tests for the native monitor."""

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

from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBRefreshMode,
    DBRefreshRequest,
    DBRefreshResult,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    FrozenPayload,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    JobTransitionWatermark,
    Provenance,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    TailGap,
    TailGeneration,
    TailJobEvent,
    TailPollRequest,
    TailPollResult,
    TailTransitionIdentity,
    TailWorkflowEvent,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionWatermark,
)
from Pegasus.monitor.reconcile import Reconciler

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
GENERATION = DatabaseGeneration(1, 10, 20)
NEW_GENERATION = DatabaseGeneration(2, 10, 30)
TAIL_GENERATION = TailGeneration(1, 10, 40)
NEW_TAIL_GENERATION = TailGeneration(2, 10, 50)
CLOCK = ClockSample(200.0, 100.0)


def job_transition(
    state: str,
    timestamp: str,
    state_seq: int,
    *,
    submit_seq: int = 3,
    instance_id: int = 7,
    exec_job_id: str = "compute_ID0000001",
) -> DBJobTransition:
    return DBJobTransition(
        WORKFLOW,
        exec_job_id,
        submit_seq,
        DBTransitionIdentity(instance_id, state, Decimal(timestamp), state_seq),
    )


def workflow_transition(
    state: str,
    timestamp: str,
    *,
    restart: int = 0,
    status: int | None = None,
) -> DBWorkflowTransition:
    return DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(5, state, Decimal(timestamp)),
        restart,
        status,
    )


def database(
    transitions: tuple[DBJobTransition, ...] = (),
    *,
    workflow_transitions: tuple[DBWorkflowTransition, ...] | None = None,
    generation: DatabaseGeneration = GENERATION,
    epoch: int = 1,
) -> DatabaseSnapshot:
    if workflow_transitions is None:
        workflow_transitions = (workflow_transition("WORKFLOW_STARTED", "90"),)
    current_workflow = max(
        workflow_transitions, key=lambda item: item.authoritative_sort_key
    )
    current_restart = current_workflow.restart_count
    current_restart_rows = tuple(
        item.identity
        for item in workflow_transitions
        if item.restart_count == current_restart
    )
    workflow = WorkflowSnapshot(
        WORKFLOW,
        5,
        current_workflow.normalized_state,
        current_workflow.status,
        current_restart,
        next(
            (
                item.identity.timestamp
                for item in reversed(workflow_transitions)
                if item.restart_count == current_restart
                and item.normalized_state == "WORKFLOW_STARTED"
            ),
            None,
        ),
        (
            current_workflow.identity.timestamp
            if current_workflow.normalized_state == "WORKFLOW_TERMINATED"
            else None
        ),
        current_workflow,
    )

    jobs: tuple[JobSnapshot, ...] = ()
    watermarks: tuple[JobTransitionWatermark, ...] = ()
    if transitions:
        by_attempt: dict[tuple[str, int, int], list[DBJobTransition]] = {}
        for transition in transitions:
            key = (
                transition.exec_job_id,
                transition.job_submit_seq,
                transition.identity.job_instance_id,
            )
            by_attempt.setdefault(key, []).append(transition)
        built_jobs = []
        built_watermarks = []
        for job_id, (key, rows) in enumerate(by_attempt.items(), start=2):
            exec_job_id, submit_seq, instance_id = key
            current = max(rows, key=lambda item: item.authoritative_sort_key)
            attempt_id = JobAttemptIdentity(job_id, instance_id, submit_seq)
            built_jobs.append(
                JobSnapshot(
                    WORKFLOW,
                    job_id,
                    exec_job_id,
                    "compute",
                    1,
                    ("example::compute",),
                    (JobAttempt(attempt_id),),
                    attempt_id,
                    current.identity.state,
                    current.identity.timestamp,
                    current,
                    Provenance.DB_CONFIRMED,
                )
            )
            highest = max(row.identity.jobstate_submit_seq for row in rows)
            built_watermarks.append(
                JobTransitionWatermark(
                    instance_id,
                    highest,
                    tuple(
                        row.identity
                        for row in rows
                        if row.identity.jobstate_submit_seq == highest
                    ),
                )
            )
        jobs = tuple(built_jobs)
        watermarks = tuple(built_watermarks)

    return DatabaseSnapshot(
        SnapshotEpoch(epoch),
        generation,
        CLOCK.epoch,
        workflow,
        jobs,
        tuple(sorted(transitions, key=lambda item: item.recent_event_sort_key)),
        tuple(
            sorted(workflow_transitions, key=lambda item: item.authoritative_sort_key)
        ),
        watermarks,
        WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 5, current_restart),
            current_restart_rows,
        ),
    )


def retry_database(epoch: int = 2) -> DatabaseSnapshot:
    old = job_transition("JOB_SUCCESS", "100", 2)
    new = job_transition("SUBMIT", "101", 1, submit_seq=4, instance_id=8)
    base = database((old,), epoch=epoch)
    first_attempt = JobAttemptIdentity(2, 7, 3)
    second_attempt = JobAttemptIdentity(2, 8, 4)
    job = JobSnapshot(
        WORKFLOW,
        2,
        "compute_ID0000001",
        "compute",
        1,
        ("example::compute",),
        (JobAttempt(first_attempt), JobAttempt(second_attempt)),
        second_attempt,
        new.identity.state,
        new.identity.timestamp,
        new,
        Provenance.DB_CONFIRMED,
    )
    return replace(
        base,
        jobs=(job,),
        recent_transitions=tuple(
            sorted((old, new), key=lambda item: item.recent_event_sort_key)
        ),
        watermarks=(
            JobTransitionWatermark(7, 2, (old.identity,)),
            JobTransitionWatermark(8, 1, (new.identity,)),
        ),
    )


def db_result(
    snapshot: DatabaseSnapshot | None,
    *,
    mode: DBRefreshMode = DBRefreshMode.FULL_REBOOTSTRAP,
    health_state: HealthState = HealthState.HEALTHY,
    error_code: str | None = None,
    prior: DatabaseGeneration | None = None,
) -> DBRefreshResult:
    request = DBRefreshRequest(
        WORKFLOW,
        snapshot.epoch if snapshot is not None else SnapshotEpoch(1),
        mode,
        CLOCK,
        prior_generation=prior,
        pending_job_keys=() if mode is not DBRefreshMode.BOUNDED_SUFFIX else (),
    )
    health = SourceHealth(
        SourceName.STAMPEDE,
        health_state,
        CLOCK.epoch,
        error_code=error_code,
    )
    return DBRefreshResult(
        request,
        snapshot,
        health,
        snapshot.generation if snapshot is not None else prior,
    )


def tail_job(
    state: str,
    timestamp: str,
    offset: int,
    *,
    submit_seq: int = 3,
    generation: TailGeneration = TAIL_GENERATION,
    base: DatabaseGeneration | None = GENERATION,
    exec_job_id: str = "compute_ID0000001",
) -> TailJobEvent:
    return TailJobEvent(
        WORKFLOW,
        TailTransitionIdentity(generation, offset),
        base,
        offset + 10,
        CLOCK.monotonic,
        Decimal(timestamp),
        exec_job_id,
        state,
        submit_seq,
        "-",
        "local",
        "-",
        f"{timestamp} {exec_job_id} {state} - local - {submit_seq}",
    )


def tail_workflow(
    marker: str,
    timestamp: str,
    offset: int,
    *,
    status: int | None = None,
    generation: TailGeneration = TAIL_GENERATION,
    base: DatabaseGeneration | None = GENERATION,
) -> TailWorkflowEvent:
    return TailWorkflowEvent(
        WORKFLOW,
        TailTransitionIdentity(generation, offset),
        base,
        offset + 10,
        CLOCK.monotonic,
        Decimal(timestamp),
        marker,
        status,
        f"{timestamp} INTERNAL *** {marker} ***",
    )


def tail_result(
    *events: TailJobEvent | TailWorkflowEvent,
    generation: TailGeneration = TAIL_GENERATION,
    health_state: HealthState = HealthState.HEALTHY,
) -> TailPollResult:
    request = TailPollRequest(
        WORKFLOW,
        events[0].base_db_generation if events else GENERATION,
        CLOCK,
        100_000,
        100,
    )
    return TailPollResult(
        request,
        tuple(item for item in events if isinstance(item, TailJobEvent)),
        tuple(item for item in events if isinstance(item, TailWorkflowEvent)),
        (),
        (),
        SourceHealth(SourceName.LIVE_TAIL, health_state, CLOCK.epoch),
        generation,
        sum(len(item.original_line) for item in events),
        len(events),
    )


def install(reconciler: Reconciler, snapshot: DatabaseSnapshot) -> None:
    reconciler.apply_database(db_result(snapshot))


def effective(reconciler: Reconciler, epoch: int = 10):
    snapshot = reconciler.build_snapshot(SnapshotEpoch(epoch), CLOCK)
    assert snapshot is not None
    return snapshot


def scheduler_result(status: int) -> SchedulerQueryResult:
    kind = SchedulerQueryKind.QUEUE
    request = SchedulerQueryRequest(WORKFLOW, kind, CLOCK, 1.0, 10)
    return SchedulerQueryResult(
        request,
        SourceHealth(SourceName.CONDOR_QUEUE, HealthState.HEALTHY, CLOCK.epoch),
        0.0,
        (
            SchedulerEvidence(
                kind,
                FrozenPayload.from_mapping(
                    {
                        "DAGNodeName": "compute_ID0000001",
                        "ClusterId": 42,
                        "ProcId": 0,
                    }
                ),
                FrozenPayload.from_mapping(
                    {"ClusterId": 42, "ProcId": 0, "JobStatus": status}
                ),
            ),
        ),
    )


def test_initial_merge_suppresses_event_already_in_database() -> None:
    transition = job_transition("EXECUTE", "101", 2)
    reconciler = Reconciler(WORKFLOW)
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10, base=None)))

    install(reconciler, database((transition,)))

    snapshot = effective(reconciler)
    assert snapshot.pending_overlay_count == 0
    assert len(snapshot.jobs) == 1
    assert snapshot.jobs[0].provenance is Provenance.DB_CONFIRMED


def test_repeated_publication_reuses_job_roster_until_inputs_change() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))

    first = effective(reconciler, 10)
    second = effective(reconciler, 11)

    assert second.jobs is first.jobs

    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    changed = effective(reconciler, 12)

    assert changed.jobs is not second.jobs
    assert changed.jobs[0].state == "EXECUTE"


def test_database_object_replacement_invalidates_effective_base() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    first = effective(reconciler, 10)

    replacement = database((job_transition("EXECUTE", "101", 2),), epoch=2)
    reconciler.apply_database(
        db_result(
            replacement,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )
    second = effective(reconciler, 11)

    assert second.jobs is not first.jobs
    assert second.jobs[0].state == "EXECUTE"


def test_scheduler_revision_rebuilds_enriched_authoritative_base() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))

    reconciler.update_scheduler(scheduler_result(1))
    idle = effective(reconciler, 10)
    assert idle.jobs[0].scheduler.to_json_dict()["queue"]["JobStatus"] == 1

    reconciler.update_scheduler(scheduler_result(2))
    running = effective(reconciler, 11)
    assert running.jobs is not idle.jobs
    assert running.jobs[0].scheduler.to_json_dict()["queue"]["JobStatus"] == 2


def test_pre_database_preview_migrates_to_confirmed_event_at_stable_order() -> None:
    transition = job_transition("EXECUTE", "101", 2)
    event = tail_job("EXECUTE", "101", 10, base=None)
    reconciler = Reconciler(WORKFLOW)
    reconciler.ingest_tail(tail_result(event))

    first = reconciler.unconfirmed_tail_events()
    repeated = reconciler.unconfirmed_tail_events()

    assert first == repeated
    assert len(first) == 1
    assert first[0].event == event
    assert first[0].provenance is Provenance.TAIL_PENDING
    assert reconciler.semantic_progress == first[0].order

    install(reconciler, database((transition,)))
    snapshot = effective(reconciler)
    confirmed = next(item for item in snapshot.events if item.event == transition)

    assert reconciler.unconfirmed_tail_events() == ()
    assert reconciler.pending_count == 0
    assert confirmed.provenance is Provenance.DB_CONFIRMED
    assert confirmed.order == first[0].order
    assert sum(item.order == confirmed.order for item in snapshot.events) == 1


def test_pre_database_preview_withholds_gap_and_overflowed_streams() -> None:
    event = tail_job("EXECUTE", "101", 10, base=None)
    reconciler = Reconciler(WORKFLOW)
    reconciler.ingest_tail(tail_result(event))
    assert len(reconciler.unconfirmed_tail_events()) == 1

    gap = replace(
        tail_result(),
        gaps=(TailGap(TAIL_GENERATION, "truncated", dropped_lines=1),),
        health=SourceHealth(SourceName.LIVE_TAIL, HealthState.GAP, CLOCK.epoch),
    )
    reconciler.ingest_tail(gap)

    assert reconciler.unconfirmed_tail_events() == ()
    assert reconciler.buffered_count == 1

    bounded = Reconciler(WORKFLOW, max_pending_events=2)
    bounded.ingest_tail(
        tail_result(
            tail_job("SUBMIT", "100", 10, base=None),
            tail_job("EXECUTE", "101", 20, base=None),
        )
    )
    assert len(bounded.unconfirmed_tail_events()) == 2

    bounded.ingest_tail(tail_result(tail_job("JOB_SUCCESS", "102", 30, base=None)))
    assert bounded.unconfirmed_tail_events() == ()
    assert bounded.buffered_count == 0
    assert bounded.tail_rearm_required


def test_unmatched_initial_event_uses_zero_based_semantic_suffix_cursor() -> None:
    reconciler = Reconciler(WORKFLOW)
    event = tail_job("UNKNOWN_NEW_STATE", "101", 10, base=None)
    reconciler.ingest_tail(tail_result(event))

    install(reconciler, database((job_transition("SUBMIT", "100", 5),)))

    cursor = reconciler.reconciliation_cursor()
    assert cursor.job_watermarks == ()
    assert cursor.provisional_job_keys == (event.semantic_key,)
    snapshot = effective(reconciler)
    assert snapshot.pending_overlay_count == 1
    assert snapshot.events[-1].event.base_db_generation == GENERATION


def test_overlay_is_one_effective_job_and_is_replaced_on_confirmation() -> None:
    base = database((job_transition("SUBMIT", "100", 1),))
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, base)
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))

    pending = effective(reconciler)
    assert len(pending.jobs) == 1
    assert pending.jobs[0].state == "EXECUTE"
    assert pending.jobs[0].provenance is Provenance.DB_WITH_TAIL_OVERLAY
    assert pending.pending_overlay_count == 1

    confirmed = database(
        (
            job_transition("SUBMIT", "100", 1),
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(
            confirmed,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )
    final = effective(reconciler, 11)
    assert len(final.jobs) == 1
    assert final.pending_overlay_count == 0
    assert final.jobs[0].provenance is Provenance.DB_CONFIRMED


def test_repeated_same_second_events_reconcile_as_ordered_multiset() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "99", 1),)))
    first = tail_job("EXECUTE", "101", 10)
    second = tail_job("EXECUTE", "101", 20)
    reconciler.ingest_tail(tail_result(first, second))

    one_row = database(
        (
            job_transition("SUBMIT", "99", 1),
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(one_row, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(reconciler).pending_overlay_count == 1

    two_rows = database(
        (
            job_transition("SUBMIT", "99", 1),
            job_transition("EXECUTE", "101", 2),
            job_transition("EXECUTE", "101", 3),
        ),
        epoch=3,
    )
    reconciler.apply_database(
        db_result(two_rows, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(reconciler).pending_overlay_count == 0


def test_synthetic_terminal_group_retires_one_causal_tail_event() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("EXECUTE", "100", 1),)))
    reconciler.ingest_tail(tail_result(tail_job("JOB_ABORTED", "101", 10)))
    confirmed = database(
        (
            job_transition("EXECUTE", "100", 1),
            job_transition("JOB_ABORTED", "101", 2),
            job_transition("JOB_FAILURE", "101", 2),
        ),
        epoch=2,
    )

    reconciler.apply_database(
        db_result(confirmed, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )

    snapshot = effective(reconciler)
    assert snapshot.pending_overlay_count == 0
    assert snapshot.jobs[0].state == "JOB_FAILURE"


def test_one_db_group_can_supersede_intermediate_then_confirm_terminal() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("EXECUTE", "100", 1),)))
    reconciler.ingest_tail(
        tail_result(
            tail_job("JOB_TERMINATED", "101", 10),
            tail_job("JOB_SUCCESS", "101", 20),
        )
    )
    confirmed = database(
        (
            job_transition("EXECUTE", "100", 1),
            job_transition("JOB_SUCCESS", "101", 2),
        ),
        epoch=2,
    )

    reconciler.apply_database(
        db_result(confirmed, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )

    assert effective(reconciler).pending_overlay_count == 0


def test_held_pair_folds_but_plain_held_waits_for_reason_or_later_state() -> None:
    base = database((job_transition("EXECUTE", "100", 1),))
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, base)
    plain = tail_job("JOB_HELD", "101", 10)
    reconciler.ingest_tail(tail_result(plain))
    held_db = database(
        (
            job_transition("EXECUTE", "100", 1),
            job_transition("JOB_HELD", "102", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(held_db, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(reconciler).pending_overlay_count == 1

    reason = tail_job("JOB_HELD_REASON", "102", 20)
    reconciler.ingest_tail(tail_result(reason))
    assert effective(reconciler).pending_overlay_count == 0

    second = Reconciler(WORKFLOW)
    install(second, base)
    second.ingest_tail(tail_result(plain))
    released_db = database(
        (
            job_transition("EXECUTE", "100", 1),
            job_transition("JOB_RELEASED", "103", 2),
        ),
        epoch=2,
    )
    second.apply_database(
        db_result(released_db, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(second).pending_overlay_count == 0


def test_reconciliation_cursor_remains_at_original_database_cut() -> None:
    base_transition = job_transition("SUBMIT", "100", 1)
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((base_transition,)))
    reconciler.ingest_tail(tail_result(tail_job("UNKNOWN_NEW_STATE", "101", 10)))
    first = reconciler.reconciliation_cursor().job_watermarks[0]

    advanced = database(
        (
            base_transition,
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(advanced, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )

    assert reconciler.pending_count == 1
    assert reconciler.reconciliation_cursor().job_watermarks == (first,)
    assert first.highest_jobstate_submit_seq == 1


def test_reconciliation_cursor_advances_only_through_confirmed_prefix() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(
        tail_result(
            tail_job("EXECUTE", "101", 10),
            tail_job("UNKNOWN_NEW_STATE", "102", 20),
        )
    )
    first = reconciler.reconciliation_cursor().job_watermarks[0]
    assert first.highest_jobstate_submit_seq == 1

    advanced = database(
        (
            job_transition("SUBMIT", "100", 1),
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(advanced, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )

    assert reconciler.pending_count == 1
    cursor = reconciler.reconciliation_cursor().job_watermarks[0]
    assert cursor.highest_jobstate_submit_seq == 2


def test_provisional_job_is_replaced_without_duplicate_count() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database())
    event = tail_job(
        "SUBMIT",
        "101",
        10,
        submit_seq=8,
        exec_job_id="early_ID0000002",
    )
    reconciler.ingest_tail(tail_result(event))
    provisional = effective(reconciler)
    assert len(provisional.jobs) == 1
    assert provisional.jobs[0].job_id is None
    assert provisional.jobs[0].task_count == 0
    assert provisional.jobs[0].provenance is Provenance.PROVISIONAL_JOB
    assert reconciler.reconciliation_cursor().provisional_job_keys == (
        event.semantic_key,
    )

    row = job_transition(
        "SUBMIT",
        "101",
        1,
        submit_seq=8,
        instance_id=11,
        exec_job_id="early_ID0000002",
    )
    reconciler.apply_database(
        db_result(
            database((row,), epoch=2),
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )
    confirmed = effective(reconciler)
    assert len(confirmed.jobs) == 1
    assert confirmed.jobs[0].job_id is not None
    assert confirmed.pending_overlay_count == 0


def test_provisional_unknown_jobs_follow_known_jobs_in_stable_order() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(
        tail_result(
            tail_job("SUBMIT", "101", 10, exec_job_id="unknown_z"),
            tail_job("SUBMIT", "102", 20, exec_job_id="unknown_a"),
        )
    )

    snapshot = effective(reconciler)

    assert [job.exec_job_id for job in snapshot.jobs] == [
        "compute_ID0000001",
        "unknown_a",
        "unknown_z",
    ]
    assert all(job.job_id is None for job in snapshot.jobs[1:])


def test_retry_submit_sequence_overlays_and_replaces_one_database_job() -> None:
    old = job_transition("JOB_SUCCESS", "100", 2)
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((old,)))
    retry_submit = tail_job("SUBMIT", "101", 10, submit_seq=4)
    reconciler.ingest_tail(tail_result(retry_submit))

    pending = effective(reconciler)
    assert len(pending.jobs) == 1
    assert pending.jobs[0].state == "SUBMIT"
    assert pending.jobs[0].current_attempt is not None
    assert pending.jobs[0].current_attempt.job_submit_seq == 3

    reconciler.apply_database(
        db_result(
            retry_database(),
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )
    confirmed = effective(reconciler)
    assert len(confirmed.jobs) == 1
    assert len(confirmed.jobs[0].attempts) == 2
    assert confirmed.jobs[0].current_attempt is not None
    assert confirmed.jobs[0].current_attempt.job_submit_seq == 4
    assert confirmed.pending_overlay_count == 0


def test_workflow_restart_epochs_duplicate_markers_and_confirmation() -> None:
    terminated = (
        workflow_transition("WORKFLOW_STARTED", "90"),
        workflow_transition("WORKFLOW_TERMINATED", "100", status=1),
    )
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database(workflow_transitions=terminated))
    start = tail_workflow("DAGMAN_STARTED", "101", 10)
    duplicate = tail_workflow("DAGMAN_STARTED", "101", 20)
    finish = tail_workflow("DAGMAN_FINISHED", "102", 30, status=0)
    reconciler.ingest_tail(tail_result(start, duplicate, finish))
    pending = effective(reconciler)
    assert pending.workflow.restart_count == 1
    assert pending.workflow.state == "WORKFLOW_TERMINATED"
    assert pending.workflow.status == 0
    original_cursor = reconciler.reconciliation_cursor().workflow_watermark

    confirmed_rows = terminated + (
        workflow_transition("WORKFLOW_STARTED", "101", restart=1),
        workflow_transition("WORKFLOW_TERMINATED", "102", restart=1, status=0),
    )
    reconciler.apply_database(
        db_result(
            database(workflow_transitions=confirmed_rows, epoch=2),
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )
    final = effective(reconciler)
    assert final.pending_overlay_count == 0
    assert final.workflow.restart_count == 1
    assert original_cursor is not None
    assert original_cursor.restart.restart_count == 0


def test_unrepresentable_rapid_restart_remains_live_with_identity_conflict() -> None:
    terminated = (
        workflow_transition("WORKFLOW_STARTED", "90"),
        workflow_transition("WORKFLOW_TERMINATED", "100", status=1),
    )
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database(workflow_transitions=terminated))
    events = (
        tail_workflow("DAGMAN_STARTED", "101", 10),
        tail_workflow("DAGMAN_FINISHED", "102", 20, status=1),
        tail_workflow("DAGMAN_STARTED", "101", 30),
    )
    reconciler.ingest_tail(tail_result(*events))
    # workflowstate's three-column PK can only retain the first restart's row
    # at this identical state/timestamp.
    represented = terminated + (
        workflow_transition("WORKFLOW_STARTED", "101", restart=1),
        workflow_transition("WORKFLOW_TERMINATED", "102", restart=1, status=1),
    )
    reconciler.apply_database(
        db_result(
            database(workflow_transitions=represented, epoch=2),
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )

    snapshot = effective(reconciler)
    assert snapshot.pending_overlay_count == 1
    assert snapshot.workflow.restart_count == 2
    tail_health = next(
        item for item in snapshot.source_health if item.source is SourceName.LIVE_TAIL
    )
    assert tail_health.state is HealthState.DEGRADED
    assert tail_health.error_code == "workflow_db_identity_conflict"


def test_db_replacement_freezes_then_discards_unprovable_overlay() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(tail_result(tail_job("MYSTERY", "101", 10)))
    before = effective(reconciler)

    failure = db_result(
        None,
        mode=DBRefreshMode.CURRENT_SNAPSHOT,
        health_state=HealthState.RESYNC,
        error_code="database_generation_changed",
        prior=GENERATION,
    )
    reconciler.apply_database(failure)
    frozen = effective(reconciler, 11)
    assert frozen.jobs[0].state == before.jobs[0].state
    assert reconciler.refresh_mode() is DBRefreshMode.FULL_REBOOTSTRAP

    replacement = database(
        (job_transition("SUBMIT", "100", 1),),
        generation=NEW_GENERATION,
        epoch=12,
    )
    reconciler.apply_database(db_result(replacement))
    after = effective(reconciler, 12)
    assert after.db_generation == NEW_GENERATION
    assert after.pending_overlay_count == 0
    tail_health = next(
        item for item in after.source_health if item.source is SourceName.LIVE_TAIL
    )
    assert tail_health.state is HealthState.RESYNC


def test_tail_generation_change_freezes_until_full_db_refresh() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    before = effective(reconciler)
    replacement_event = tail_job(
        "JOB_SUCCESS",
        "102",
        10,
        generation=NEW_TAIL_GENERATION,
    )
    reconciler.ingest_tail(
        tail_result(replacement_event, generation=NEW_TAIL_GENERATION)
    )
    frozen = effective(reconciler, 11)
    assert frozen.tail_generation == before.tail_generation
    assert frozen.jobs[0].state == before.jobs[0].state

    refreshed = database(
        (
            job_transition("SUBMIT", "100", 1),
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=12,
    )
    reconciler.apply_database(db_result(refreshed))
    final = effective(reconciler, 12)
    assert final.tail_generation == NEW_TAIL_GENERATION
    assert final.jobs[0].state == "JOB_SUCCESS"
    assert final.pending_overlay_count == 1


def test_rotation_keeps_old_overlay_visible_until_lagging_database_catches_up() -> None:
    base = database((job_transition("SUBMIT", "100", 1),))
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, base)
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    before = effective(reconciler)

    reconciler.ingest_tail(
        tail_result(
            generation=NEW_TAIL_GENERATION, health_state=HealthState.REATTACHING
        )
    )
    reconciler.apply_database(db_result(replace(base, epoch=SnapshotEpoch(2))))
    lagging = effective(reconciler, 11)
    assert lagging.jobs[0].state == before.jobs[0].state == "EXECUTE"
    assert lagging.pending_overlay_count == 1
    assert reconciler.refresh_mode() is DBRefreshMode.FULL_REBOOTSTRAP

    caught_up = database(
        (
            job_transition("SUBMIT", "100", 1),
            job_transition("EXECUTE", "101", 2),
        ),
        epoch=3,
    )
    reconciler.apply_database(db_result(caught_up))
    final = effective(reconciler, 12)
    assert final.jobs[0].state == "EXECUTE"
    assert final.pending_overlay_count == 0


def test_pending_overlay_bound_requires_real_tail_rearm_and_full_refresh() -> None:
    reconciler = Reconciler(WORKFLOW, max_pending_events=1)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(
        tail_result(
            tail_job("EXECUTE", "101", 10),
            tail_job("JOB_SUCCESS", "102", 20),
        )
    )

    assert reconciler.tail_rearm_required
    assert reconciler.refresh_mode() is DBRefreshMode.FULL_REBOOTSTRAP
    assert reconciler.pending_count == 0


def test_quarantined_overlay_is_also_bounded_during_database_outage() -> None:
    reconciler = Reconciler(WORKFLOW, max_pending_events=1)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    effective(reconciler)
    reconciler.begin_database_rebootstrap()
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    assert reconciler.buffered_count == 1

    reconciler.ingest_tail(tail_result(tail_job("JOB_SUCCESS", "102", 20)))

    assert reconciler.buffered_count == 0
    assert reconciler.tail_rearm_required
    assert reconciler.refresh_mode() is DBRefreshMode.FULL_REBOOTSTRAP


def test_semantic_event_order_is_stable_across_refresh_only_publications() -> None:
    reconciler = Reconciler(WORKFLOW)
    base = database((job_transition("SUBMIT", "100", 1),))
    install(reconciler, base)
    first = effective(reconciler, 1)
    progress = reconciler.semantic_progress
    second = effective(reconciler, 2)

    assert progress > 0
    assert reconciler.semantic_progress == progress
    assert [item.order for item in second.events] == [
        item.order for item in first.events
    ]

    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    third = effective(reconciler, 3)
    assert reconciler.semantic_progress > progress
    assert third.events[-1].order == reconciler.semantic_progress


def test_event_order_survives_current_bounded_current_feed_shape() -> None:
    first_row = job_transition("SUBMIT", "100", 1)
    second_row = job_transition("EXECUTE", "101", 2)
    full = database((first_row, second_row))
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, full)
    first = effective(reconciler, 1)
    progress = reconciler.semantic_progress
    second_order = next(
        item.order
        for item in first.events
        if isinstance(item.event, DBJobTransition)
        and item.event.identity == second_row.identity
    )

    bounded = replace(full, recent_transitions=(first_row,))
    reconciler.apply_database(
        db_result(bounded, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    effective(reconciler, 2)
    reconciler.apply_database(
        db_result(full, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    restored = effective(reconciler, 3)

    assert reconciler.semantic_progress == progress
    assert (
        next(
            item.order
            for item in restored.events
            if isinstance(item.event, DBJobTransition)
            and item.event.identity == second_row.identity
        )
        == second_order
    )


def test_effective_event_feed_does_not_expand_with_current_job_heads() -> None:
    row = job_transition("EXECUTE", "101", 2)
    without_recent_job_feed = replace(database((row,)), recent_transitions=())
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, without_recent_job_feed)

    snapshot = effective(reconciler)

    assert len(snapshot.jobs) == 1
    assert all(not isinstance(item.event, DBJobTransition) for item in snapshot.events)
    assert len(snapshot.events) == len(
        without_recent_job_feed.recent_workflow_transitions
    )


def test_attempt_absent_from_installed_roster_stays_zero_based_after_racing_refresh() -> (
    None
):
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database())
    event = tail_job(
        "UNKNOWN_NEW_STATE",
        "101",
        10,
        submit_seq=8,
        exec_job_id="racing_ID0000002",
    )
    reconciler.ingest_tail(tail_result(event))
    discovered = job_transition(
        "SUBMIT",
        "101",
        5,
        submit_seq=8,
        instance_id=11,
        exec_job_id="racing_ID0000002",
    )
    reconciler.apply_database(
        db_result(
            database((discovered,), epoch=2),
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=GENERATION,
        )
    )

    cursor = reconciler.reconciliation_cursor()
    assert cursor.job_watermarks == ()
    assert cursor.provisional_job_keys == (event.semantic_key,)


def test_producer_rounding_not_truncation_controls_match() -> None:
    reconciler = Reconciler(WORKFLOW)
    install(reconciler, database((job_transition("SUBMIT", "100", 1),)))
    reconciler.ingest_tail(tail_result(tail_job("EXECUTE", "101", 10)))
    rounded = database(
        (
            job_transition("SUBMIT", "100", 1),
            job_transition("EXECUTE", "100.500001", 2),
        ),
        epoch=2,
    )
    reconciler.apply_database(
        db_result(rounded, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(reconciler).pending_overlay_count == 0

    control = Reconciler(WORKFLOW)
    install(control, database((job_transition("SUBMIT", "100", 1),)))
    control.ingest_tail(tail_result(tail_job("EXECUTE", "100", 10)))
    control.apply_database(
        db_result(rounded, mode=DBRefreshMode.CURRENT_SNAPSHOT, prior=GENERATION)
    )
    assert effective(control).pending_overlay_count == 1
