"""Tests for deterministic, source-free monitor event-log replay."""

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

import builtins
from decimal import Decimal
from pathlib import Path

import pytest

from Pegasus.monitor.event_log import EventLogFormatError, encode_record
from Pegasus.monitor.models import (
    CheckpointRecord,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    DiagnosticEvidence,
    DiagnosticRecord,
    DiagnosticSeverity,
    EnrichmentRecord,
    FrozenPayload,
    GapReason,
    GapRecord,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    JobTransitionRecord,
    JobTransitionWatermark,
    Provenance,
    SnapshotEpoch,
    SourceName,
    StreamHeader,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionRecord,
    WorkflowTransitionWatermark,
)
from Pegasus.monitor.replay import ReplayEngine, ReplayStreamError, replay_records

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
GENERATION = DatabaseGeneration(1, 10, 20)


def _job_transition(
    state: str,
    *,
    timestamp: str,
    db_sequence: int,
) -> DBJobTransition:
    return DBJobTransition(
        WORKFLOW,
        "compute_ID0001",
        1,
        DBTransitionIdentity(10, state, Decimal(timestamp), db_sequence),
    )


def _snapshot(
    state: str = "SUBMIT",
    *,
    timestamp: str = "1",
    db_sequence: int = 1,
    epoch: int = 1,
) -> DatabaseSnapshot:
    transition = _job_transition(state, timestamp=timestamp, db_sequence=db_sequence)
    attempt_identity = JobAttemptIdentity(2, 10, 1)
    attempt = JobAttempt(
        attempt_identity,
        scheduler_id="100.0",
        site="local",
        submit_time=Decimal("1"),
    )
    job = JobSnapshot(
        WORKFLOW,
        2,
        "compute_ID0001",
        "compute",
        1,
        ("example::compute",),
        (attempt,),
        attempt_identity,
        state,
        Decimal(timestamp),
        transition,
        Provenance.DB_CONFIRMED,
    )
    workflow_transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", Decimal("1")),
        0,
        0,
    )
    workflow = WorkflowSnapshot(
        WORKFLOW,
        1,
        "WORKFLOW_STARTED",
        0,
        0,
        Decimal("1"),
        None,
        workflow_transition,
    )
    return DatabaseSnapshot(
        SnapshotEpoch(epoch),
        GENERATION,
        float(epoch),
        workflow,
        (job,),
        (transition,),
        (workflow_transition,),
        (JobTransitionWatermark(10, db_sequence, (transition.identity,)),),
        WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 1, 0),
            (workflow_transition.identity,),
        ),
    )


def _header(stream_id: str, timestamp: float = 1.0) -> StreamHeader:
    return StreamHeader(0, stream_id, WORKFLOW, timestamp, "6.0")


def _write(path: Path, *records: object, torn: bytes = b"") -> None:
    path.write_bytes(b"".join(encode_record(record) for record in records) + torn)


def test_normal_replay_reconstructs_db_state_and_preserves_evidence() -> None:
    initial = _snapshot()
    execute = _job_transition("EXECUTE", timestamp="2", db_sequence=2)
    diagnostic = DiagnosticRecord(
        3,
        "stream",
        SnapshotEpoch(2),
        2.1,
        FrozenPayload.from_mapping({"exec_job_id": "compute_ID0001"}),
        "running",
        DiagnosticSeverity.INFO,
        "job is running",
        (
            DiagnosticEvidence(
                SourceName.CONDOR_QUEUE,
                "matched",
                FrozenPayload.from_mapping({"slots": 1}),
            ),
        ),
    )
    enrichment = EnrichmentRecord(
        4,
        "stream",
        SnapshotEpoch(2),
        2.2,
        SourceName.CONDOR_QUEUE,
        FrozenPayload.from_mapping({"exec_job_id": "compute_ID0001"}),
        FrozenPayload.from_mapping({"job_status": 2}),
        100.0,
    )

    result = replay_records(
        (
            _header("stream"),
            CheckpointRecord(1, "stream", 1.0, initial, "initial"),
            JobTransitionRecord(2, "stream", SnapshotEpoch(2), 2.0, execute),
            diagnostic,
            enrichment,
        )
    )

    assert result.complete is True
    assert result.snapshot is not None
    assert result.snapshot.jobs[0].state == "EXECUTE"
    assert result.snapshot.jobs[0].provenance is Provenance.DB_CONFIRMED
    assert result.current_diagnostics == (diagnostic,)
    assert result.enrichments == (enrichment,)
    assert result.snapshot.jobs[0].scheduler.fields == ()


def test_timing_is_frame_based_and_scaled_by_speed(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    execute = _job_transition("EXECUTE", timestamp="2", db_sequence=2)
    _write(
        path,
        _header("stream", 10.1),
        CheckpointRecord(1, "stream", 10.1, _snapshot(), "initial"),
        JobTransitionRecord(2, "stream", SnapshotEpoch(2), 14.1, execute),
    )
    sleeps: list[float] = []
    seen: list[str] = []

    result = ReplayEngine(path, speed=2.0, sleep=sleeps.append).replay(
        lambda frame: seen.append(frame.snapshot.jobs[0].state or "")
    )

    assert sleeps == [2.0]
    assert seen == ["SUBMIT", "EXECUTE"]
    assert [frame.recorded_at_epoch for frame in result.frames] == [10.1, 14.1]

    sleeps.clear()
    ReplayEngine(path, speed=0, sleep=sleeps.append).replay()
    assert sleeps == []
    with pytest.raises(ValueError, match="speed"):
        ReplayEngine(path, speed=-1)


def test_gap_discards_incremental_records_until_recovery_checkpoint() -> None:
    ignored = _job_transition("EXECUTE", timestamp="2", db_sequence=2)
    recovered = _snapshot("JOB_HELD", timestamp="3", db_sequence=3, epoch=3)
    success = _job_transition("JOB_SUCCESS", timestamp="4", db_sequence=4)

    result = replay_records(
        (
            _header("stream"),
            CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial"),
            GapRecord(4, "stream", 2.0, GapReason.DISK_GUARD, 2, 3),
            JobTransitionRecord(5, "stream", SnapshotEpoch(2), 3.0, ignored),
            CheckpointRecord(6, "stream", 4.0, recovered, "recovery"),
            JobTransitionRecord(7, "stream", SnapshotEpoch(4), 5.0, success),
        )
    )

    assert result.complete is True
    assert result.ignored_records == 1
    assert result.snapshot is not None
    assert result.snapshot.jobs[0].state == "JOB_SUCCESS"
    assert ignored.identity not in {
        item.identity for item in result.snapshot.recent_transitions
    }


def test_replacement_header_resets_state_and_waits_for_its_checkpoint() -> None:
    old_execute = _job_transition("EXECUTE", timestamp="2", db_sequence=2)
    pre_checkpoint = _job_transition("JOB_FAILURE", timestamp="8", db_sequence=8)
    replacement = _snapshot("JOB_HELD", timestamp="9", db_sequence=9, epoch=1)

    result = replay_records(
        (
            _header("old"),
            CheckpointRecord(1, "old", 1.0, _snapshot(), "initial"),
            JobTransitionRecord(2, "old", SnapshotEpoch(2), 2.0, old_execute),
            _header("new", 10.0),
            JobTransitionRecord(1, "new", SnapshotEpoch(1), 10.1, pre_checkpoint),
            CheckpointRecord(2, "new", 10.2, replacement, "initial"),
        )
    )

    assert result.stream_replacements == 1
    assert result.ignored_records == 1
    assert result.header is not None and result.header.stream_id == "new"
    assert result.snapshot == replacement
    assert result.diagnostics == ()
    assert result.enrichments == ()


def test_same_sequence_transitions_have_deterministic_final_precedence() -> None:
    failure = _job_transition("JOB_FAILURE", timestamp="2", db_sequence=2)
    success = _job_transition("JOB_SUCCESS", timestamp="2", db_sequence=2)
    terminated = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, "WORKFLOW_TERMINATED", Decimal("3")),
        0,
        0,
    )
    records = (
        _header("stream"),
        CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial"),
        JobTransitionRecord(2, "stream", SnapshotEpoch(2), 2.0, failure),
        JobTransitionRecord(3, "stream", SnapshotEpoch(2), 2.0, success),
        WorkflowTransitionRecord(4, "stream", SnapshotEpoch(3), 3.0, terminated),
    )

    first = replay_records(records)
    second = replay_records(records)

    assert first.snapshot == second.snapshot
    assert first.snapshot is not None
    assert first.snapshot.jobs[0].state == "JOB_SUCCESS"
    assert first.snapshot.workflow.state == "WORKFLOW_TERMINATED"
    assert first.snapshot.workflow.ended_at == Decimal("3")
    assert first.snapshot.watermarks[0].identities_at_highest_seq == (
        failure.identity,
        success.identity,
    )


def test_torn_tail_is_retained_but_complete_malformed_record_fails(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    _write(
        path,
        _header("stream"),
        CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial"),
        torn=b'{"schema_version":1,"record_type":"job_transition"',
    )

    result = ReplayEngine(path, speed=0).replay()
    assert result.complete is True
    assert result.trailing_bytes.startswith(b'{"schema_version"')

    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(EventLogFormatError):
        ReplayEngine(path, speed=0).replay()

    path.write_bytes(b"")
    with pytest.raises(ReplayStreamError, match="empty"):
        ReplayEngine(path, speed=0).replay()


def test_replay_never_imports_or_calls_live_workflow_sources(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "events.jsonl"
    _write(
        path,
        _header("stream"),
        CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial"),
    )
    forbidden = {
        "Pegasus.monitor.stampede",
        "Pegasus.monitor.live_events",
        "Pegasus.monitor.condor",
        "Pegasus.monitor.diagnostics",
        "Pegasus.monitor.locator",
        "Pegasus.monitor.remote",
    }
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name in forbidden:
            raise AssertionError(f"live source imported during replay: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    result = ReplayEngine(path, speed=0).replay()
    assert result.complete is True


def test_engine_streams_without_read_bytes_and_can_discard_frames(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "events.jsonl"
    records: list[object] = [
        _header("stream"),
        CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial"),
    ]
    for sequence in range(2, 102):
        records.append(
            EnrichmentRecord(
                sequence,
                "stream",
                SnapshotEpoch(1),
                float(sequence),
                SourceName.CONDOR_QUEUE,
                FrozenPayload.from_mapping({"exec_job_id": "compute_ID0001"}),
                FrozenPayload.from_mapping({"sequence": sequence}),
                None,
            )
        )
    _write(path, *records)
    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _self: pytest.fail("ReplayEngine must stream instead of read_bytes"),
    )
    frames: list[int] = []

    result = ReplayEngine(path, speed=0, retain_frames=False).replay(
        lambda _frame: frames.append(1)
    )

    assert result.complete is True
    assert result.frames == ()
    assert len(frames) == 101
    assert len(result.enrichments) == 1
    assert result.enrichments[0].payload.to_json_dict()["sequence"] == 101


def test_streaming_replay_enforces_record_limit_and_tolerates_bounded_torn_tail(
    tmp_path,
) -> None:
    path = tmp_path / "events.jsonl"
    header = encode_record(_header("stream"))
    checkpoint = encode_record(
        CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial")
    )
    maximum = max(len(header), len(checkpoint))
    path.write_bytes(header + checkpoint + b"{" + b"x" * 20)

    result = ReplayEngine(path, speed=0, max_record_bytes=maximum).replay()
    assert result.complete is True
    assert result.trailing_bytes.startswith(b"{")

    path.write_bytes(header + checkpoint + b"{" + b"x" * (maximum + 1))
    with pytest.raises(EventLogFormatError, match="byte limit"):
        ReplayEngine(path, speed=0, max_record_bytes=maximum).replay()
