"""Contract tests for the native monitor's immutable shared models."""

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
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from Pegasus.monitor.models import (
    JSONL_V1_CONTRACT_STATUS,
    JSONL_V1_DEFERRED_FIELDS,
    BoundedAge,
    CheckpointRecord,
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
    DiagnosticEvidence,
    DiagnosticRecord,
    DiagnosticSeverity,
    EffectiveEvent,
    EffectiveSnapshot,
    EnrichmentRecord,
    FrozenPayload,
    GapReason,
    GapRecord,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSemanticKey,
    JobSnapshot,
    JobTransitionRecord,
    JobTransitionWatermark,
    Lifecycle,
    LiveTailProvider,
    Provenance,
    SchedulerEvidence,
    SchedulerProvider,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    StampedeSnapshotProvider,
    StreamHeader,
    TailGeneration,
    TailJobEvent,
    TailPollRequest,
    TailPollResult,
    TailTransitionIdentity,
    TailWorkflowEvent,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionRecord,
    WorkflowTransitionWatermark,
    job_lifecycle,
    normalize_job_state,
    producer_timestamp_key,
    state_precedence,
    transition_group_equivalent,
)

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
DB_GENERATION = DatabaseGeneration(3, 11, 101)
TAIL_GENERATION = TailGeneration(4, 11, 202)


def db_transition(
    state: str = "EXECUTE",
    timestamp: str = "100.500001",
    state_sequence: int = 9,
    job_submit_seq: int = 3,
) -> DBJobTransition:
    return DBJobTransition(
        workflow=WORKFLOW,
        exec_job_id="compute_ID0000001",
        job_submit_seq=job_submit_seq,
        identity=DBTransitionIdentity(
            job_instance_id=7,
            state=state,
            timestamp=Decimal(timestamp),
            jobstate_submit_seq=state_sequence,
        ),
    )


def workflow_transition(
    state: str = "WORKFLOW_STARTED",
    timestamp: str = "100.500001",
    restart_count: int = 0,
    status: int | None = None,
) -> DBWorkflowTransition:
    return DBWorkflowTransition(
        workflow=WORKFLOW,
        identity=DBWorkflowTransitionIdentity(5, state, Decimal(timestamp)),
        restart_count=restart_count,
        status=status,
    )


def confirmed_snapshot() -> EffectiveSnapshot:
    transition = db_transition()
    attempt_id = JobAttemptIdentity(2, 7, 3)
    attempt = JobAttempt(
        identity=attempt_id,
        scheduler_id="123.0",
        site="local",
        submit_time=Decimal("99.000000"),
        start_time=Decimal("100.500001"),
        raw_wait_status=0,
        exit_code=0,
    )
    job = JobSnapshot(
        workflow=WORKFLOW,
        job_id=2,
        exec_job_id="compute_ID0000001",
        type_desc="compute",
        task_count=1,
        transformations=("example::compute",),
        attempts=(attempt,),
        current_attempt=attempt_id,
        state=transition.identity.state,
        state_timestamp=transition.identity.timestamp,
        transition=transition,
        provenance=Provenance.DB_CONFIRMED,
    )
    workflow = WorkflowSnapshot(
        workflow=WORKFLOW,
        wf_id=5,
        state="WORKFLOW_STARTED",
        status=0,
        restart_count=0,
        started_at=workflow_transition(status=0).identity.timestamp,
        ended_at=None,
        transition=workflow_transition(status=0),
    )
    return EffectiveSnapshot(
        epoch=SnapshotEpoch(12),
        workflow=workflow,
        jobs=(job,),
        db_generation=DB_GENERATION,
        tail_generation=TAIL_GENERATION,
        published_at_epoch=101.0,
        published_at_monotonic=501.0,
        source_health=(
            SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, 101.0, 101.0),
        ),
        events=(EffectiveEvent(0, Provenance.DB_CONFIRMED, transition),),
    )


def confirmed_database_snapshot() -> DatabaseSnapshot:
    snapshot = confirmed_snapshot()
    transition = snapshot.jobs[0].transition
    assert transition is not None
    workflow_transition = snapshot.workflow.transition
    assert workflow_transition is not None
    return DatabaseSnapshot(
        epoch=snapshot.epoch,
        generation=DB_GENERATION,
        snapshot_at_epoch=snapshot.published_at_epoch,
        workflow=snapshot.workflow,
        jobs=snapshot.jobs,
        recent_transitions=(transition,),
        recent_workflow_transitions=(workflow_transition,),
        watermarks=(JobTransitionWatermark(7, 9, (transition.identity,)),),
        workflow_watermark=WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 5, 0),
            (workflow_transition.identity,),
        ),
    )


def test_models_are_deeply_immutable() -> None:
    source = {"nested": {"values": [1, 2]}, "path": Path("/tmp/example")}
    payload = FrozenPayload.from_mapping(source)
    snapshot = confirmed_snapshot()

    with pytest.raises(FrozenInstanceError):
        snapshot.epoch = SnapshotEpoch(13)  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        payload.fields = ()  # type: ignore[misc]

    source["nested"]["values"].append(3)  # type: ignore[index, union-attr]
    assert payload.to_json_dict() == {
        "nested": {"values": [1, 2]},
        "path": "/tmp/example",
    }
    assert isinstance(snapshot.jobs, tuple)
    assert isinstance(snapshot.jobs[0].attempts, tuple)


def test_exact_db_identity_hash_and_order_keep_full_precision() -> None:
    first = DBTransitionIdentity(7, "EXECUTE", Decimal("10.499999"), 4)
    second = DBTransitionIdentity(7, "EXECUTE", Decimal("10.500001"), 4)

    assert first != second
    assert len({first, second}) == 2
    assert sorted((second, first)) == [first, second]
    assert first.to_json_dict()["timestamp"] == "10.499999"


def test_tail_identity_is_generation_plus_start_offset() -> None:
    one = TailTransitionIdentity(TailGeneration(1, 8, 9), 100)
    same = TailTransitionIdentity(TailGeneration(1, 8, 9), 100)
    replaced = TailTransitionIdentity(TailGeneration(2, 8, 9), 100)

    assert one == same
    assert hash(one) == hash(same)
    assert one != replaced
    assert sorted((replaced, one)) == [one, replaced]


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("100.499999", "100"),
        ("100.500000", "100"),
        ("100.500001", "101"),
        ("101.500000", "102"),
        ("100.900000", "101"),
    ],
)
def test_producer_timestamp_rounding_matches_format_not_truncation(
    timestamp: str, expected: str
) -> None:
    assert producer_timestamp_key(Decimal(timestamp)) == expected


def test_job_and_workflow_semantic_keys_share_producer_rounding() -> None:
    db_job = db_transition(timestamp="100.500000")
    tail_job = TailJobEvent(
        workflow=WORKFLOW,
        identity=TailTransitionIdentity(TAIL_GENERATION, 30),
        base_db_generation=DB_GENERATION,
        end_offset=80,
        observed_at_monotonic=44.0,
        event_timestamp=Decimal("100.500000"),
        exec_job_id="compute_ID0000001",
        state="EXECUTE",
        job_submit_seq=3,
        raw_value="123.0",
        raw_site="local",
        raw_walltime="-",
        original_line="100 compute_ID0000001 EXECUTE 123.0 local - 3",
    )
    db_workflow = workflow_transition(timestamp="101.500000")
    tail_workflow = TailWorkflowEvent(
        workflow=WORKFLOW,
        identity=TailTransitionIdentity(TAIL_GENERATION, 81),
        base_db_generation=DB_GENERATION,
        end_offset=120,
        observed_at_monotonic=45.0,
        event_timestamp=Decimal("101.500000"),
        marker="DAGMAN_STARTED",
        status=None,
        original_line="102 INTERNAL *** DAGMAN_STARTED 123 ***",
    )

    assert db_job.semantic_key == tail_job.semantic_key
    assert db_workflow.semantic_key == tail_workflow.semantic_key_for(
        db_workflow.restart_count
    )
    assert db_job.identity.timestamp == Decimal("100.500000")
    assert db_workflow.identity.timestamp == Decimal("101.500000")


def test_semantic_keys_include_exact_workflow_scope() -> None:
    job = db_transition()
    foreign_workflow = WorkflowIdentity("wf-other", "wf-root")
    foreign_job = DBJobTransition(
        foreign_workflow,
        job.exec_job_id,
        job.job_submit_seq,
        job.identity,
    )
    workflow = workflow_transition()
    foreign_marker = DBWorkflowTransition(
        foreign_workflow,
        workflow.identity,
        workflow.restart_count,
        workflow.status,
    )

    assert job.semantic_key != foreign_job.semantic_key
    assert workflow.semantic_key != foreign_marker.semantic_key


def test_same_second_transitions_retain_order_and_multiplicity() -> None:
    events = (
        db_transition("EXECUTE", "100.100000", 8),
        db_transition("JOB_TERMINATED", "100.200000", 9),
        db_transition("EXECUTE", "100.300000", 10),
    )

    assert [event.identity.producer_timestamp for event in events] == ["100"] * 3
    assert len(events) == 3
    assert [event.identity.jobstate_submit_seq for event in events] == [8, 9, 10]


def test_known_normalization_and_unknown_states() -> None:
    assert normalize_job_state("JOB_HELD_REASON") == "JOB_HELD"
    assert normalize_job_state("PRE_SCRIPT_FAILURE") == "PRE_SCRIPT_FAILED"
    assert normalize_job_state("POST_SCRIPT_FAILURE") == "POST_SCRIPT_FAILED"
    assert normalize_job_state("TRANSFER_FAILURE") == "TRANSFER_FAILURE"
    assert normalize_job_state("JOB_FAILURE") == "JOB_FAILURE"
    assert normalize_job_state("future_state") == "FUTURE_STATE"
    assert job_lifecycle("future_state") is Lifecycle.OTHER
    assert job_lifecycle("JOB_EVICTED") is Lifecycle.HELD


def test_held_pair_and_synthetic_terminal_equivalence() -> None:
    assert transition_group_equivalent(("JOB_HELD", "JOB_HELD_REASON"), ("JOB_HELD",))
    assert transition_group_equivalent(
        ("PRE_SCRIPT_FAILURE",), ("PRE_SCRIPT_FAILED", "JOB_FAILURE")
    )
    assert transition_group_equivalent(("JOB_ABORTED",), ("JOB_ABORTED", "JOB_FAILURE"))
    assert not transition_group_equivalent(("JOB_HELD",), ("JOB_HELD",))
    assert transition_group_equivalent(("JOB_HELD_REASON",), ("JOB_HELD",))
    assert transition_group_equivalent(
        ("JOB_HELD",), ("JOB_HELD",), plain_held_is_confirming=True
    )
    assert not transition_group_equivalent(
        ("FUTURE_FAILURE",), ("FUTURE_FAILED", "JOB_FAILURE")
    )
    assert not transition_group_equivalent(
        ("JOB_HELD_REASON", "JOB_HELD"), ("JOB_HELD",)
    )
    assert not transition_group_equivalent(("JOB_HELD", "JOB_HELD"), ("JOB_HELD",))
    assert not transition_group_equivalent(("JOB_HELD",), ("JOB_HELD", "JOB_HELD"))


def test_terminal_state_precedence_is_deterministic() -> None:
    rows = [
        db_transition("JOB_FAILURE", "100.000000", 9),
        db_transition("JOB_ABORTED", "100.000000", 9),
    ]

    assert (
        max(rows, key=lambda row: row.authoritative_sort_key).identity.state
        == "JOB_FAILURE"
    )
    assert state_precedence("JOB_SUCCESS") > state_precedence("POST_SCRIPT_SUCCESS")
    assert state_precedence("unknown_state") == 0


def test_source_health_age_is_clamped_and_serializable() -> None:
    age = BoundedAge.between(1000.0, 100.0, 300.0)
    health = SourceHealth(
        source=SourceName.STAMPEDE,
        state=HealthState.STALE,
        checked_at_epoch=1000.0,
        last_success_epoch=100.0,
        last_good_age=age,
        stale_after_seconds=4.0,
        consecutive_failures=2,
        error_code="db_locked",
        detail="using last-good snapshot",
    )

    assert age.seconds == 300.0
    assert age.capped is True
    assert health.to_json_dict()["last_good_age"] == {
        "seconds": 300.0,
        "maximum_seconds": 300.0,
        "capped": True,
    }
    json.dumps(health.to_json_dict())


def test_source_health_has_explicit_recovery_and_unconfirmed_states() -> None:
    assert HealthState.REATTACHING.value == "reattaching"
    assert HealthState.RESYNC.value == "resync"
    assert HealthState.FAILED_UNCONFIRMED.value == "failed_unconfirmed"


def test_zero_status_and_exit_code_are_preserved() -> None:
    snapshot = confirmed_snapshot().to_json_dict()

    assert snapshot["workflow"]["status"] == 0  # type: ignore[index]
    assert snapshot["jobs"][0]["attempts"][0]["raw_wait_status"] == 0  # type: ignore[index]
    assert snapshot["jobs"][0]["attempts"][0]["exit_code"] == 0  # type: ignore[index]


def test_attempt_distinguishes_raw_wait_status_from_decoded_exit_code() -> None:
    attempt = JobAttempt(JobAttemptIdentity(1, 2, 3), raw_wait_status=256, exit_code=1)

    assert attempt.raw_wait_status == 256
    assert attempt.exit_code == 1
    assert attempt.to_json_dict()["raw_wait_status"] == 256
    assert attempt.to_json_dict()["exit_code"] == 1


def test_overlay_carries_tail_and_database_generations_without_db_row_sequence() -> (
    None
):
    event = TailJobEvent(
        workflow=WORKFLOW,
        identity=TailTransitionIdentity(TAIL_GENERATION, 1),
        base_db_generation=DB_GENERATION,
        end_offset=20,
        observed_at_monotonic=2.0,
        event_timestamp=Decimal("3.0"),
        exec_job_id="j",
        state="SUBMIT",
        job_submit_seq=8,
        raw_value="0",
        raw_site="-",
        raw_walltime="-",
        original_line="3 j SUBMIT 0 - - 8",
    )

    shape = event.to_json_dict()
    assert shape["identity"]["source_generation"] == TAIL_GENERATION.to_json_dict()  # type: ignore[index]
    assert shape["base_db_generation"] == DB_GENERATION.to_json_dict()
    assert "jobstate_submit_seq" not in shape
    assert "observed_at_monotonic" not in shape


def test_database_generation_detects_replacement_even_at_same_path_identity() -> None:
    original = DatabaseGeneration(1, 10, 20)
    replaced_inode = DatabaseGeneration(2, 10, 21)
    regrown_same_inode = DatabaseGeneration(2, 10, 20)

    assert len({original, replaced_inode, regrown_same_inode}) == 3


def test_database_snapshot_preserves_same_sequence_identity_group() -> None:
    snapshot = confirmed_snapshot()
    aborted = db_transition("JOB_ABORTED", "100.000000", 11)
    failure = db_transition("JOB_FAILURE", "100.000000", 11)
    watermark = JobTransitionWatermark(7, 11, (aborted.identity, failure.identity))
    current_job = replace(
        snapshot.jobs[0],
        state=failure.identity.state,
        state_timestamp=failure.identity.timestamp,
        transition=failure,
    )
    database = DatabaseSnapshot(
        epoch=snapshot.epoch,
        generation=DB_GENERATION,
        snapshot_at_epoch=101.0,
        workflow=snapshot.workflow,
        jobs=(current_job,),
        recent_transitions=(aborted, failure),
        recent_workflow_transitions=(snapshot.workflow.transition,),
        watermarks=(watermark,),
        workflow_watermark=WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 5, 0),
            (snapshot.workflow.transition.identity,),
        ),
    )

    assert len(database.watermarks[0].identities_at_highest_seq) == 2
    assert database.watermarks[0].to_json_dict()["highest_jobstate_submit_seq"] == 11
    assert "highest_submit_seq" not in database.watermarks[0].to_json_dict()
    assert [
        transition.identity.state for transition in database.recent_transitions
    ] == ["JOB_ABORTED", "JOB_FAILURE"]


def test_database_snapshot_rejects_workflow_overlay_scheduler_and_duplicate_jobs() -> (
    None
):
    database = confirmed_database_snapshot()
    pending_workflow = replace(
        database.workflow,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(TailTransitionIdentity(TAIL_GENERATION, 1),),
    )
    with pytest.raises(ValueError, match="workflow state must be DB-confirmed"):
        replace(database, workflow=pending_workflow)

    enriched = replace(
        database.jobs[0],
        scheduler=FrozenPayload.from_mapping(
            {"queue": {"JobStatus": 2, "reasons": ["waiting"]}}
        ),
    )
    with pytest.raises(ValueError, match="DB-confirmed jobs only"):
        replace(database, jobs=(enriched,))

    with pytest.raises(ValueError, match="unique job_id"):
        replace(database, jobs=(database.jobs[0], database.jobs[0]))

    duplicate_exec = JobSnapshot(
        WORKFLOW,
        3,
        database.jobs[0].exec_job_id,
        "compute",
        0,
        (),
        (),
        None,
        None,
        None,
        None,
        Provenance.DB_CONFIRMED,
    )
    with pytest.raises(ValueError, match="one job per exec_job_id"):
        replace(database, jobs=database.jobs + (duplicate_exec,))


def test_database_snapshot_rejects_duplicate_or_orphan_source_rows() -> None:
    database = confirmed_database_snapshot()
    transition = database.recent_transitions[0]
    with pytest.raises(ValueError, match="watermarks must be unique"):
        replace(database, watermarks=(database.watermarks[0], database.watermarks[0]))
    with pytest.raises(ValueError, match="transition identities must be unique"):
        replace(database, recent_transitions=(transition, transition))
    with pytest.raises(ValueError, match="no included job row"):
        replace(
            database,
            recent_transitions=(replace(transition, exec_job_id="orphan"),),
        )
    with pytest.raises(ValueError, match="no included attempt row"):
        replace(
            database,
            recent_transitions=(replace(transition, job_submit_seq=999),),
        )
    older = db_transition("SUBMIT", "99.0", 8)
    with pytest.raises(ValueError, match="stable feed order"):
        replace(database, recent_transitions=(transition, older))
    later = db_transition("JOB_HELD", "101.0", 10)
    with pytest.raises(ValueError, match="watermark is behind"):
        replace(database, recent_transitions=(later,))
    wrong_current_watermark = JobTransitionWatermark(
        7,
        9,
        (replace(transition.identity, state="JOB_HELD"),),
    )
    with pytest.raises(ValueError, match="contained in its watermark"):
        replace(
            database,
            recent_transitions=(),
            watermarks=(wrong_current_watermark,),
        )

    workflow_transition = database.recent_workflow_transitions[0]
    wrong_wf_id = replace(
        workflow_transition,
        identity=replace(workflow_transition.identity, wf_id=999),
    )
    with pytest.raises(ValueError, match="no included workflow row"):
        replace(database, recent_workflow_transitions=(wrong_wf_id,))
    terminated_identity = DBWorkflowTransitionIdentity(
        5, "WORKFLOW_TERMINATED", Decimal("101.0")
    )
    stale_workflow_watermark = WorkflowTransitionWatermark(
        database.workflow_watermark.restart,
        database.workflow_watermark.identities + (terminated_identity,),
    )
    with pytest.raises(ValueError, match="authoritative watermark row"):
        replace(database, workflow_watermark=stale_workflow_watermark)


def test_database_snapshot_enforces_workflow_global_attempt_identities() -> None:
    database = confirmed_database_snapshot()
    duplicate_seq_identity = JobAttemptIdentity(3, 8, 3)
    duplicate_seq_job = JobSnapshot(
        WORKFLOW,
        3,
        "other",
        "compute",
        1,
        (),
        (JobAttempt(duplicate_seq_identity),),
        duplicate_seq_identity,
        None,
        None,
        None,
        Provenance.DB_CONFIRMED,
    )
    with pytest.raises(ValueError, match="workflow-global unique"):
        replace(database, jobs=database.jobs + (duplicate_seq_job,))

    duplicate_instance_identity = JobAttemptIdentity(3, 7, 4)
    duplicate_instance_job = replace(
        duplicate_seq_job,
        attempts=(JobAttempt(duplicate_instance_identity),),
        current_attempt=duplicate_instance_identity,
    )
    with pytest.raises(ValueError, match="job_instance_id"):
        replace(database, jobs=database.jobs + (duplicate_instance_job,))


def test_job_snapshot_rejects_duplicate_attempts() -> None:
    job = confirmed_snapshot().jobs[0]

    with pytest.raises(ValueError, match="attempts must be unique"):
        replace(job, attempts=(job.attempts[0], job.attempts[0]))
    same_instance = JobAttempt(JobAttemptIdentity(2, 7, 4))
    with pytest.raises(ValueError, match="instance IDs must be unique"):
        replace(job, attempts=job.attempts + (same_instance,))
    same_submit_sequence = JobAttempt(JobAttemptIdentity(2, 8, 3))
    with pytest.raises(ValueError, match="submit sequences must be unique"):
        replace(job, attempts=job.attempts + (same_submit_sequence,))
    later_attempt = JobAttempt(JobAttemptIdentity(2, 8, 4))
    with pytest.raises(ValueError, match="highest job_submit_seq"):
        replace(job, attempts=job.attempts + (later_attempt,))


def test_workflow_restart_count_distinguishes_primary_key_collision_context() -> None:
    identity = DBWorkflowTransitionIdentity(5, "WORKFLOW_STARTED", Decimal("10.0"))
    first = DBWorkflowTransition(WORKFLOW, identity, restart_count=0, status=0)
    restarted = DBWorkflowTransition(WORKFLOW, identity, restart_count=1, status=0)

    assert first.identity == restarted.identity
    assert first != restarted
    assert restarted.to_json_dict()["restart_count"] == 1


def test_workflow_matching_includes_restart_epoch_and_terminal_status() -> None:
    success = workflow_transition(
        state="WORKFLOW_TERMINATED",
        timestamp="20.500000",
        restart_count=2,
        status=0,
    )
    failed = workflow_transition(
        state="WORKFLOW_TERMINATED",
        timestamp="20.500000",
        restart_count=2,
        status=1,
    )
    next_restart = workflow_transition(
        state="WORKFLOW_STARTED",
        timestamp="20.500000",
        restart_count=3,
    )

    assert success.semantic_key != failed.semantic_key
    assert success.semantic_key != next_restart.semantic_key
    assert success.authoritative_sort_key < next_restart.authoritative_sort_key


def test_authoritative_snapshot_consistency_allows_unsubmitted_and_overlay() -> None:
    snapshot = confirmed_snapshot()
    with pytest.raises(ValueError, match="authoritative workflow state"):
        replace(snapshot.workflow, state="WORKFLOW_TERMINATED")
    with pytest.raises(ValueError, match="authoritative workflow restart"):
        replace(snapshot.workflow, restart_count=1)
    with pytest.raises(ValueError, match="authoritative workflow start time"):
        replace(snapshot.workflow, started_at=Decimal("99.0"))
    with pytest.raises(ValueError, match="exact transition"):
        replace(snapshot.workflow, transition=None)
    workflow_overlay = replace(
        snapshot.workflow,
        state="WORKFLOW_TERMINATED",
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(TailTransitionIdentity(TAIL_GENERATION, 1),),
    )
    assert workflow_overlay.transition == snapshot.workflow.transition

    job = snapshot.jobs[0]
    with pytest.raises(ValueError, match="authoritative job state"):
        replace(job, state="JOB_HELD")
    with pytest.raises(ValueError, match="exact transition"):
        replace(job, transition=None)
    assert job.transition is not None
    with pytest.raises(ValueError, match="included attempt"):
        replace(
            job,
            transition=replace(job.transition, exec_job_id="other-job"),
        )
    with pytest.raises(ValueError, match="included attempt"):
        replace(
            job,
            transition=replace(job.transition, job_submit_seq=99),
        )
    job_overlay = replace(
        job,
        state="JOB_HELD",
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(TailTransitionIdentity(TAIL_GENERATION, 2),),
    )
    assert job_overlay.transition == job.transition

    unsubmitted = JobSnapshot(
        WORKFLOW,
        99,
        "not-yet-submitted",
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
    assert unsubmitted.lifecycle is Lifecycle.UNSUBMITTED


def test_effective_snapshot_rejects_cross_workflow_jobs() -> None:
    snapshot = confirmed_snapshot()
    foreign_job = JobSnapshot(
        workflow=WorkflowIdentity("other", "wf-root"),
        job_id=99,
        exec_job_id="foreign",
        type_desc="compute",
        task_count=0,
        transformations=(),
        attempts=(),
        current_attempt=None,
        state=None,
        state_timestamp=None,
        transition=None,
        provenance=Provenance.DB_CONFIRMED,
    )

    with pytest.raises(ValueError, match="exact selected wf_uuid"):
        EffectiveSnapshot(
            epoch=snapshot.epoch,
            workflow=snapshot.workflow,
            jobs=(foreign_job,),
            db_generation=snapshot.db_generation,
            tail_generation=snapshot.tail_generation,
            published_at_epoch=10.0,
            published_at_monotonic=20.0,
        )


def test_provisional_job_may_omit_db_id_but_confirmed_job_may_not() -> None:
    provisional = JobSnapshot(
        workflow=WORKFLOW,
        job_id=None,
        exec_job_id="early",
        type_desc="unknown",
        task_count=0,
        transformations=(),
        attempts=(),
        current_attempt=None,
        state="SUBMIT",
        state_timestamp=Decimal("1"),
        transition=None,
        provenance=Provenance.PROVISIONAL_JOB,
        pending_tail=(TailTransitionIdentity(TAIL_GENERATION, 1),),
    )

    assert provisional.job_id is None
    with pytest.raises(ValueError, match="only provisional jobs"):
        replace(provisional, provenance=Provenance.DB_CONFIRMED)
    with pytest.raises(ValueError, match="cannot fabricate"):
        replace(
            provisional,
            attempts=(JobAttempt(JobAttemptIdentity(1, 2, 3)),),
            current_attempt=JobAttemptIdentity(1, 2, 3),
        )


def test_effective_snapshot_has_ordered_mixed_event_feed_and_workflow_pending() -> None:
    snapshot = confirmed_snapshot()
    tail_identity = TailTransitionIdentity(TAIL_GENERATION, 500)
    tail_workflow = TailWorkflowEvent(
        workflow=WORKFLOW,
        identity=tail_identity,
        base_db_generation=DB_GENERATION,
        end_offset=550,
        observed_at_monotonic=5.0,
        event_timestamp=Decimal("105.0"),
        marker="DAGMAN_FINISHED",
        status=0,
        original_line="105 INTERNAL *** DAGMAN_FINISHED 0 ***",
    )
    workflow = replace(
        snapshot.workflow,
        state="WORKFLOW_TERMINATED",
        status=0,
        ended_at=tail_workflow.event_timestamp,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(tail_identity,),
    )
    db_job = snapshot.jobs[0].transition
    db_workflow = snapshot.workflow.transition
    assert db_job is not None
    assert db_workflow is not None
    effective = replace(
        snapshot,
        workflow=workflow,
        events=(
            EffectiveEvent(0, Provenance.DB_CONFIRMED, db_job),
            EffectiveEvent(1, Provenance.TAIL_PENDING, tail_workflow),
            EffectiveEvent(2, Provenance.DB_CONFIRMED, db_workflow),
        ),
    )

    assert effective.pending_overlay_count == 1
    assert [event.kind for event in effective.events] == [
        "db_job_transition",
        "tail_workflow_transition",
        "db_workflow_transition",
    ]
    assert effective.to_json_dict()["events"][1]["provenance"] == "tail_pending"  # type: ignore[index]
    assert effective.to_json_dict()["pending_overlay_count"] == 1
    with pytest.raises(ValueError, match="provenance"):
        EffectiveEvent(3, Provenance.DB_CONFIRMED, tail_workflow)
    with pytest.raises(ValueError, match="strictly increasing"):
        replace(effective, events=tuple(reversed(effective.events)))
    with pytest.raises(ValueError, match="DB event identities must be unique"):
        replace(
            snapshot,
            events=(
                EffectiveEvent(0, Provenance.DB_CONFIRMED, db_job),
                EffectiveEvent(1, Provenance.DB_CONFIRMED, db_job),
            ),
        )
    with pytest.raises(ValueError, match="file offset order"):
        replace(
            effective,
            events=(
                EffectiveEvent(0, Provenance.TAIL_PENDING, tail_workflow),
                EffectiveEvent(1, Provenance.TAIL_PENDING, tail_workflow),
            ),
        )
    with pytest.raises(ValueError, match="effective workflow state"):
        replace(effective, workflow=replace(workflow, state="WORKFLOW_STARTED"))
    with pytest.raises(ValueError, match="workflow pending identities"):
        replace(
            effective,
            jobs=(
                replace(
                    snapshot.jobs[0],
                    provenance=Provenance.DB_WITH_TAIL_OVERLAY,
                    pending_tail=(tail_identity,),
                ),
            ),
            workflow=snapshot.workflow,
        )


def test_effective_snapshot_rejects_duplicate_jobs_and_generation_conflicts() -> None:
    snapshot = confirmed_snapshot()
    with pytest.raises(ValueError, match="unique exec_job_id"):
        replace(snapshot, jobs=(snapshot.jobs[0], snapshot.jobs[0]))

    conflicting_instance = JobAttemptIdentity(99, 7, 4)
    second_job = JobSnapshot(
        WORKFLOW,
        99,
        "second-job",
        "compute",
        1,
        (),
        (JobAttempt(conflicting_instance),),
        conflicting_instance,
        None,
        None,
        None,
        Provenance.DB_CONFIRMED,
    )
    with pytest.raises(ValueError, match="workflow-unique job_instance_id"):
        replace(snapshot, jobs=snapshot.jobs + (second_job,))

    conflicting_sequence = JobAttemptIdentity(99, 8, 3)
    second_job = replace(
        second_job,
        attempts=(JobAttempt(conflicting_sequence),),
        current_attempt=conflicting_sequence,
    )
    with pytest.raises(ValueError, match="workflow-global job_submit_seq"):
        replace(snapshot, jobs=snapshot.jobs + (second_job,))

    other_tail = TailGeneration(9, TAIL_GENERATION.device, TAIL_GENERATION.inode)
    pending_job = replace(
        snapshot.jobs[0],
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(TailTransitionIdentity(other_tail, 10),),
    )
    with pytest.raises(ValueError, match="tail generation conflicts"):
        replace(snapshot, jobs=(pending_job,))

    tail_event = TailJobEvent(
        workflow=WORKFLOW,
        identity=TailTransitionIdentity(TAIL_GENERATION, 20),
        base_db_generation=DatabaseGeneration(99, 11, 101),
        end_offset=40,
        observed_at_monotonic=2.0,
        event_timestamp=Decimal("3.0"),
        exec_job_id="compute_ID0000001",
        state="EXECUTE",
        job_submit_seq=3,
        raw_value="123.0",
        raw_site="local",
        raw_walltime="-",
        original_line="3 compute_ID0000001 EXECUTE 123.0 local - 3",
    )
    with pytest.raises(ValueError, match="DB generation conflicts"):
        replace(
            snapshot,
            events=(EffectiveEvent(0, Provenance.TAIL_PENDING, tail_event),),
        )
    wrong_tail_event = replace(
        tail_event,
        identity=TailTransitionIdentity(other_tail, 20),
        base_db_generation=DB_GENERATION,
    )
    with pytest.raises(ValueError, match="event tail generation"):
        replace(
            snapshot,
            events=(EffectiveEvent(0, Provenance.TAIL_PENDING, wrong_tail_event),),
        )

    valid_tail_event = replace(
        tail_event,
        base_db_generation=DB_GENERATION,
        state="JOB_HELD",
    )
    pending_job = replace(
        snapshot.jobs[0],
        state="JOB_HELD",
        state_timestamp=valid_tail_event.event_timestamp,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(valid_tail_event.identity,),
    )
    effective = replace(
        snapshot,
        jobs=(pending_job,),
        events=(EffectiveEvent(0, Provenance.TAIL_PENDING, valid_tail_event),),
    )
    assert effective.jobs[0].state == "JOB_HELD"
    with pytest.raises(ValueError, match="effective job state"):
        replace(effective, jobs=(replace(pending_job, state="EXECUTE"),))
    with pytest.raises(ValueError, match="job pending identities"):
        replace(
            effective,
            events=(
                EffectiveEvent(
                    0,
                    Provenance.TAIL_PENDING,
                    replace(valid_tail_event, exec_job_id="other-job"),
                ),
            ),
        )
    with pytest.raises(ValueError, match="predate the current attempt"):
        replace(
            effective,
            events=(
                EffectiveEvent(
                    0,
                    Provenance.TAIL_PENDING,
                    replace(valid_tail_event, job_submit_seq=2),
                ),
            ),
        )
    earlier_offset_event = replace(
        valid_tail_event,
        identity=TailTransitionIdentity(TAIL_GENERATION, 10),
        end_offset=15,
        event_timestamp=Decimal("4.0"),
    )
    reversed_offset_job = replace(
        pending_job,
        state_timestamp=earlier_offset_event.event_timestamp,
        pending_tail=(valid_tail_event.identity, earlier_offset_event.identity),
    )
    with pytest.raises(ValueError, match="file offset order"):
        replace(
            effective,
            jobs=(reversed_offset_job,),
            events=(
                EffectiveEvent(0, Provenance.TAIL_PENDING, valid_tail_event),
                EffectiveEvent(1, Provenance.TAIL_PENDING, earlier_offset_event),
            ),
        )


def test_duplicate_workflow_markers_do_not_change_restart_timing() -> None:
    snapshot = confirmed_snapshot()
    duplicate_start = TailWorkflowEvent(
        WORKFLOW,
        TailTransitionIdentity(TAIL_GENERATION, 600),
        DB_GENERATION,
        650,
        6.0,
        Decimal("105"),
        "DAGMAN_STARTED",
        None,
        "105 INTERNAL *** DAGMAN_STARTED None ***",
    )
    started = replace(
        snapshot.workflow,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(duplicate_start.identity,),
    )
    effective = replace(
        snapshot,
        workflow=started,
        events=(EffectiveEvent(0, Provenance.TAIL_PENDING, duplicate_start),),
    )
    assert effective.workflow.started_at == snapshot.workflow.started_at
    assert effective.workflow.restart_count == 0

    first_finish = replace(
        duplicate_start,
        identity=TailTransitionIdentity(TAIL_GENERATION, 700),
        end_offset=750,
        event_timestamp=Decimal("106"),
        marker="DAGMAN_FINISHED",
        status=0,
    )
    duplicate_finish = replace(
        first_finish,
        identity=TailTransitionIdentity(TAIL_GENERATION, 800),
        end_offset=850,
        event_timestamp=Decimal("107"),
        status=1,
    )
    finished = replace(
        snapshot.workflow,
        state="WORKFLOW_TERMINATED",
        status=0,
        ended_at=first_finish.event_timestamp,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(first_finish.identity, duplicate_finish.identity),
    )
    effective = replace(
        snapshot,
        workflow=finished,
        events=(
            EffectiveEvent(0, Provenance.TAIL_PENDING, first_finish),
            EffectiveEvent(1, Provenance.TAIL_PENDING, duplicate_finish),
        ),
    )
    assert effective.workflow.ended_at == first_finish.event_timestamp
    assert effective.workflow.status == 0


def test_provider_protocols_are_structural_and_do_not_schedule() -> None:
    database = confirmed_database_snapshot()
    health = SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, 100.0, 100.0)
    tail_health = SourceHealth(SourceName.LIVE_TAIL, HealthState.HEALTHY, 100.0, 100.0)
    scheduler_health = SourceHealth(
        SourceName.CONDOR_QUEUE, HealthState.HEALTHY, 100.0, 100.0
    )
    clock = ClockSample(100.0, 50.0)

    class Reader:
        def refresh(self, request: DBRefreshRequest) -> DBRefreshResult:
            correlated = replace(
                database,
                epoch=request.next_epoch,
                snapshot_at_epoch=request.clock.epoch,
            )
            return DBRefreshResult(request, correlated, health, correlated.generation)

    class Tailer:
        def poll(self, request: TailPollRequest) -> TailPollResult:
            return TailPollResult(
                request, (), (), (), (), tail_health, TAIL_GENERATION, 0, 0
            )

    class Scheduler:
        def query(self, request: SchedulerQueryRequest) -> SchedulerQueryResult:
            return SchedulerQueryResult(
                request,
                scheduler_health,
                backoff_seconds=1.0,
            )

    assert isinstance(Reader(), StampedeSnapshotProvider)
    assert isinstance(Tailer(), LiveTailProvider)
    assert isinstance(Scheduler(), SchedulerProvider)
    request = TailPollRequest(
        WORKFLOW, DB_GENERATION, clock, max_bytes=4096, max_lines=20
    )
    assert request.max_lines == 20
    refresh = DBRefreshRequest(
        workflow=WORKFLOW,
        next_epoch=SnapshotEpoch(13),
        mode=DBRefreshMode.BOUNDED_SUFFIX,
        clock=clock,
        prior_generation=DB_GENERATION,
        pending_job_watermarks=database.watermarks,
        pending_workflow_watermark=database.workflow_watermark,
        recent_transition_limit=100,
        recent_workflow_transition_limit=20,
    )
    assert refresh.pending_workflow_watermark == database.workflow_watermark
    assert refresh.recent_workflow_transition_limit == 20
    assert isinstance(Reader().refresh(refresh), DBRefreshResult)
    assert isinstance(Tailer().poll(request), TailPollResult)
    scheduler_request = SchedulerQueryRequest(
        WORKFLOW, SchedulerQueryKind.QUEUE, clock, 2.0, 10
    )
    assert isinstance(Scheduler().query(scheduler_request), SchedulerQueryResult)


def test_source_requests_use_coordinator_clocks_and_explicit_refresh_modes() -> None:
    clock = ClockSample(100.0, 50.0)
    full = DBRefreshRequest(
        WORKFLOW,
        SnapshotEpoch(1),
        DBRefreshMode.FULL_REBOOTSTRAP,
        clock,
    )
    assert full.clock is clock

    current = DBRefreshRequest(
        WORKFLOW,
        SnapshotEpoch(2),
        DBRefreshMode.CURRENT_SNAPSHOT,
        clock,
        prior_generation=DB_GENERATION,
    )
    assert current.prior_generation == DB_GENERATION
    with pytest.raises(ValueError, match="requires a prior generation"):
        replace(current, prior_generation=None)

    with pytest.raises(ValueError, match="cannot carry suffix"):
        replace(
            full,
            pending_job_watermarks=confirmed_database_snapshot().watermarks,
        )
    with pytest.raises(ValueError, match="requires a prior base"):
        replace(full, mode=DBRefreshMode.BOUNDED_SUFFIX)
    provisional_key = JobSemanticKey(
        WORKFLOW.wf_uuid,
        "early",
        4,
        "101",
        "SUBMIT",
    )
    suffix = DBRefreshRequest(
        WORKFLOW,
        SnapshotEpoch(2),
        DBRefreshMode.BOUNDED_SUFFIX,
        clock,
        prior_generation=DB_GENERATION,
        pending_job_keys=(provisional_key,),
    )
    assert suffix.pending_job_keys == (provisional_key,)
    with pytest.raises(ValueError, match="cannot carry suffix cursors"):
        replace(current, pending_job_keys=(provisional_key,))
    with pytest.raises(ValueError, match="requested workflow"):
        replace(
            suffix,
            pending_job_keys=(replace(provisional_key, wf_uuid="other"),),
        )
    with pytest.raises(ValueError, match="finite"):
        ClockSample(float("nan"), 1.0)

    database = confirmed_database_snapshot()
    correlated = replace(
        database,
        epoch=full.next_epoch,
        snapshot_at_epoch=full.clock.epoch,
    )
    health = SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, 100.0)
    result = DBRefreshResult(full, correlated, health, correlated.generation)
    with pytest.raises(ValueError, match="request clock"):
        replace(result, health=replace(health, checked_at_epoch=99.0))
    with pytest.raises(ValueError, match="correlate"):
        replace(result, snapshot=replace(correlated, epoch=SnapshotEpoch(2)))
    changed_generation = DatabaseGeneration(4, 11, 102)
    current_snapshot = replace(
        database,
        epoch=current.next_epoch,
        snapshot_at_epoch=current.clock.epoch,
        generation=changed_generation,
    )
    current_health = SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, 100.0)
    with pytest.raises(ValueError, match="full database rebootstrap"):
        DBRefreshResult(
            current,
            current_snapshot,
            current_health,
            changed_generation,
        )

    older = db_transition("SUBMIT", "99.0", 8)
    limited_snapshot = replace(
        correlated,
        recent_transitions=(older, correlated.recent_transitions[0]),
    )
    with pytest.raises(ValueError, match="exceeds request limits"):
        DBRefreshResult(
            replace(full, recent_transition_limit=1),
            limited_snapshot,
            health,
            limited_snapshot.generation,
        )

    tail_request = TailPollRequest(
        WORKFLOW,
        DB_GENERATION,
        clock,
        max_bytes=10,
        max_lines=2,
    )
    tail_health = SourceHealth(SourceName.LIVE_TAIL, HealthState.HEALTHY, 100.0)
    tail_result = TailPollResult(
        tail_request,
        (),
        (),
        (),
        (),
        tail_health,
        TAIL_GENERATION,
        10,
        2,
    )
    with pytest.raises(ValueError, match="byte limit"):
        replace(tail_result, bytes_read=11)
    with pytest.raises(ValueError, match="line limit"):
        replace(tail_result, lines_read=3)


def test_scheduler_results_are_kind_correlated_bounded_and_backed_off() -> None:
    request = SchedulerQueryRequest(
        WORKFLOW,
        SchedulerQueryKind.PRIORITY,
        ClockSample(100.0, 50.0),
        timeout_seconds=2.0,
        result_limit=1,
    )
    health = SourceHealth(SourceName.CONDOR_PRIORITY, HealthState.HEALTHY, 100.0, 100.0)
    evidence = SchedulerEvidence(
        SchedulerQueryKind.PRIORITY,
        FrozenPayload.from_mapping({"user": "example"}),
        FrozenPayload.from_mapping({"priority": 10.0}),
    )
    result = SchedulerQueryResult(
        request,
        health,
        backoff_seconds=5.0,
        evidence=(evidence,),
    )

    assert result.request.kind is SchedulerQueryKind.PRIORITY
    assert result.backoff_seconds == 5.0
    with pytest.raises(ValueError, match="kind"):
        replace(
            result,
            evidence=(replace(evidence, kind=SchedulerQueryKind.NEGOTIATOR),),
        )
    with pytest.raises(ValueError, match="result limit"):
        replace(result, evidence=(evidence, evidence))
    with pytest.raises(ValueError, match="between 0 and 300"):
        replace(result, backoff_seconds=-1.0)
    with pytest.raises(ValueError, match="between 0 and 300"):
        replace(result, backoff_seconds=301.0)
    with pytest.raises(ValueError, match="health source"):
        replace(
            result,
            health=replace(health, source=SourceName.CONDOR_NEGOTIATOR),
        )
    with pytest.raises(ValueError, match="request clock"):
        replace(result, health=replace(health, checked_at_epoch=99.0))


def test_jsonl_v1_shapes_are_json_safe_and_db_confirmed() -> None:
    snapshot = confirmed_snapshot()
    database = confirmed_database_snapshot()
    transition = snapshot.jobs[0].transition
    assert transition is not None
    wf_transition = snapshot.workflow.transition
    assert wf_transition is not None
    records = (
        StreamHeader(
            0,
            "stream-1",
            WORKFLOW,
            100.0,
            "6.0.0-dev",
            FrozenPayload.from_mapping({"path": Path("/runs/001"), "id": UUID(int=0)}),
        ),
        CheckpointRecord(1, "stream-1", 101.0, database, "initial"),
        JobTransitionRecord(2, "stream-1", snapshot.epoch, 102.0, transition),
        WorkflowTransitionRecord(3, "stream-1", snapshot.epoch, 102.0, wf_transition),
        EnrichmentRecord(
            4,
            "stream-1",
            snapshot.epoch,
            102.5,
            SourceName.CONDOR_QUEUE,
            FrozenPayload.from_mapping({"exec_job_id": "compute_ID0000001"}),
            FrozenPayload.from_mapping({"JobStatus": 2}),
            112.5,
        ),
        DiagnosticRecord(
            5,
            "stream-1",
            snapshot.epoch,
            103.0,
            FrozenPayload.from_mapping({"exec_job_id": "compute_ID0000001"}),
            "held_job",
            DiagnosticSeverity.WARNING,
            "job is held",
            (
                DiagnosticEvidence(
                    SourceName.CONDOR_QUEUE,
                    "hold_reason",
                    FrozenPayload.from_mapping({"code": 34}),
                ),
            ),
        ),
        GapRecord(6, "stream-1", 104.0, GapReason.DISK_GUARD, 7),
    )

    shapes = [record.to_json_dict() for record in records]
    json.dumps(shapes)
    assert shapes[0]["contract_status"] == JSONL_V1_CONTRACT_STATUS
    checkpoint_job = shapes[1]["snapshot"]["jobs"][0]  # type: ignore[index]
    assert checkpoint_job["scheduler"] == {}
    assert checkpoint_job["pending_tail"] == []
    assert shapes[1]["snapshot"]["workflow"]["pending_tail"] == []  # type: ignore[index]
    assert shapes[2]["confirmed"] is True
    assert shapes[3]["confirmed"] is True
    assert shapes[4]["record_type"] == "enrichment"
    assert shapes[5]["changes_state"] is False
    assert shapes[6]["next_checkpoint_required"] is True
    assert JSONL_V1_DEFERRED_FIELDS


def test_checked_in_jsonl_contract_matches_record_shapes() -> None:
    contract_path = Path(__file__).parent / "contracts" / "jsonl-v1-records.json"
    contract = json.loads(contract_path.read_text())
    snapshot = confirmed_snapshot()
    database = confirmed_database_snapshot()
    transition = snapshot.jobs[0].transition
    assert transition is not None
    wf_transition = snapshot.workflow.transition
    assert wf_transition is not None
    records = (
        StreamHeader(0, "s", WORKFLOW, 1.0, "v"),
        CheckpointRecord(1, "s", 1.0, database, "initial"),
        JobTransitionRecord(2, "s", snapshot.epoch, 1.0, transition),
        WorkflowTransitionRecord(3, "s", snapshot.epoch, 1.0, wf_transition),
        EnrichmentRecord(
            4,
            "s",
            snapshot.epoch,
            1.0,
            SourceName.CONDOR_QUEUE,
            FrozenPayload(),
            FrozenPayload(),
            None,
        ),
        DiagnosticRecord(
            5,
            "s",
            snapshot.epoch,
            1.0,
            FrozenPayload(),
            "code",
            DiagnosticSeverity.INFO,
            "summary",
            (),
        ),
        GapRecord(6, "s", 1.0, GapReason.DISK_GUARD, 7),
    )

    for record in records:
        shape = record.to_json_dict()
        assert list(shape) == contract["record_types"][shape["record_type"]]


def test_checkpoint_rejects_provisional_overlay() -> None:
    snapshot = confirmed_snapshot()
    job = snapshot.jobs[0]
    pending_identity = TailTransitionIdentity(TAIL_GENERATION, 100)
    pending_job = JobSnapshot(
        workflow=job.workflow,
        job_id=job.job_id,
        exec_job_id=job.exec_job_id,
        type_desc=job.type_desc,
        task_count=job.task_count,
        transformations=job.transformations,
        attempts=job.attempts,
        current_attempt=job.current_attempt,
        state="JOB_HELD",
        state_timestamp=Decimal("102"),
        transition=job.transition,
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(pending_identity,),
    )
    pending_event = TailJobEvent(
        WORKFLOW,
        pending_identity,
        DB_GENERATION,
        150,
        502.0,
        Decimal("102"),
        job.exec_job_id,
        "JOB_HELD",
        job.current_attempt.job_submit_seq,  # type: ignore[union-attr]
        "-",
        "-",
        "-",
        "102 compute_ID0000001 JOB_HELD - - - 3",
    )
    pending = EffectiveSnapshot(
        epoch=SnapshotEpoch(13),
        workflow=snapshot.workflow,
        jobs=(pending_job,),
        db_generation=DB_GENERATION,
        tail_generation=TAIL_GENERATION,
        published_at_epoch=102.0,
        published_at_monotonic=502.0,
        events=(EffectiveEvent(0, Provenance.TAIL_PENDING, pending_event),),
    )

    with pytest.raises(TypeError, match="DatabaseSnapshot"):
        CheckpointRecord(1, "stream", 102.0, pending, "periodic")  # type: ignore[arg-type]


def test_checkpoint_rejects_effective_workflow_overlay() -> None:
    snapshot = confirmed_snapshot()
    pending_identity = TailTransitionIdentity(TAIL_GENERATION, 200)
    workflow = replace(
        snapshot.workflow,
        state="WORKFLOW_TERMINATED",
        status=0,
        ended_at=Decimal("102"),
        provenance=Provenance.DB_WITH_TAIL_OVERLAY,
        pending_tail=(pending_identity,),
    )
    pending_event = TailWorkflowEvent(
        WORKFLOW,
        pending_identity,
        DB_GENERATION,
        250,
        502.0,
        Decimal("102"),
        "DAGMAN_FINISHED",
        0,
        "102 INTERNAL *** DAGMAN_FINISHED 0 ***",
    )
    pending = replace(
        snapshot,
        workflow=workflow,
        events=(EffectiveEvent(0, Provenance.TAIL_PENDING, pending_event),),
    )

    with pytest.raises(TypeError, match="DatabaseSnapshot"):
        CheckpointRecord(1, "stream", 102.0, pending, "periodic")  # type: ignore[arg-type]


def test_monitor_package_import_has_no_source_or_rich_side_effects() -> None:
    package_src = Path(__file__).resolve().parents[2] / "src"
    script = """
import json
import sys
import Pegasus.monitor
print(json.dumps(sorted(name for name in sys.modules if name == 'rich' or name.startswith('rich.') or name.startswith('Pegasus.monitoring') or name.startswith('sqlalchemy'))))
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(package_src)
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert json.loads(result.stdout) == []
