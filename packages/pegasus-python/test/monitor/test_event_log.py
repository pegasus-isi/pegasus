"""Tests for the canonical pegasus-monitor JSONL codec and writer."""

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
import types
from dataclasses import replace
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from Pegasus.monitor import event_log
from Pegasus.monitor.event_log import (
    EventLogFormatError,
    EventLogWriter,
    UnsafeEventLogPath,
    UnsupportedEventLogVersion,
    decode_json_line,
    decode_jsonl,
    decode_record,
    encode_record,
    read_jsonl,
)
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
    WorkflowTransitionWatermark,
)

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
GENERATION = DatabaseGeneration(1, 10, 20)


def _workflow_transition(
    state: str = "WORKFLOW_STARTED", timestamp: str = "1", status: int | None = 0
) -> DBWorkflowTransition:
    return DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, state, Decimal(timestamp)),
        0,
        status,
    )


def _job_transition(
    state: str = "SUBMIT", timestamp: str = "1", sequence: int = 1
) -> DBJobTransition:
    return DBJobTransition(
        WORKFLOW,
        "compute_ID0001",
        1,
        DBTransitionIdentity(10, state, Decimal(timestamp), sequence),
    )


def _snapshot(
    *,
    epoch: int = 1,
    state: str = "SUBMIT",
    timestamp: str = "1",
    sequence: int = 1,
    recent: tuple[DBJobTransition, ...] | None = None,
    generation: DatabaseGeneration = GENERATION,
) -> DatabaseSnapshot:
    transition = _job_transition(state, timestamp, sequence)
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
    workflow_transition = _workflow_transition()
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
    transitions = recent if recent is not None else (transition,)
    return DatabaseSnapshot(
        SnapshotEpoch(epoch),
        generation,
        float(epoch),
        workflow,
        (job,),
        transitions,
        (workflow_transition,),
        (JobTransitionWatermark(10, sequence, (transition.identity,)),),
        WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 1, 0),
            (workflow_transition.identity,),
        ),
    )


def _moderate_snapshot(job_count: int = 256) -> DatabaseSnapshot:
    base = _snapshot()
    jobs = []
    transitions = []
    watermarks = []
    for index in range(job_count):
        job_id = index + 1
        job_instance_id = 10_000 + index
        job_submit_seq = index + 1
        timestamp = Decimal(job_submit_seq)
        exec_job_id = f"compute_ID{job_id:07d}"
        transition = DBJobTransition(
            WORKFLOW,
            exec_job_id,
            job_submit_seq,
            DBTransitionIdentity(
                job_instance_id,
                "SUBMIT",
                timestamp,
                1,
            ),
        )
        attempt_identity = JobAttemptIdentity(
            job_id,
            job_instance_id,
            job_submit_seq,
        )
        jobs.append(
            JobSnapshot(
                WORKFLOW,
                job_id,
                exec_job_id,
                "compute",
                1,
                ("example::compute",),
                (
                    JobAttempt(
                        attempt_identity,
                        scheduler_id=f"{job_id}.0",
                        site="local",
                        submit_time=timestamp,
                    ),
                ),
                attempt_identity,
                "SUBMIT",
                timestamp,
                transition,
                Provenance.DB_CONFIRMED,
            )
        )
        transitions.append(transition)
        watermarks.append(
            JobTransitionWatermark(job_instance_id, 1, (transition.identity,))
        )
    return replace(
        base,
        jobs=tuple(jobs),
        recent_transitions=tuple(transitions),
        watermarks=tuple(watermarks),
    )


def _records(path: Path):
    return read_jsonl(path).records


def test_codec_round_trips_every_core_record() -> None:
    snapshot = _snapshot()
    transition = snapshot.jobs[0].transition
    assert transition is not None
    records = (
        StreamHeader(0, "stream", WORKFLOW, 1.0, "6.0"),
        CheckpointRecord(1, "stream", 1.0, snapshot, "initial"),
        JobTransitionRecord(2, "stream", snapshot.epoch, 2.0, transition),
        DiagnosticRecord(
            3,
            "stream",
            snapshot.epoch,
            2.0,
            FrozenPayload.from_mapping({"exec_job_id": "compute_ID0001"}),
            "held",
            DiagnosticSeverity.WARNING,
            "job held",
            (
                DiagnosticEvidence(
                    SourceName.CONDOR_QUEUE,
                    "hold_reason",
                    FrozenPayload.from_mapping({"code": 34}),
                ),
            ),
        ),
    )

    for record in records:
        assert decode_json_line(encode_record(record)) == record


def test_checkpoint_encoding_matches_legacy_canonical_bytes() -> None:
    record = CheckpointRecord(1, "stream", 1.0, _snapshot(), "initial")
    expected = (
        json.dumps(
            record.to_json_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )

    assert encode_record(record) == expected


def test_public_decode_record_does_not_mutate_parsed_payload() -> None:
    record = CheckpointRecord(1, "stream", 1.0, _moderate_snapshot(3), "initial")
    payload = record.to_json_dict()
    original = json.loads(json.dumps(payload))

    assert decode_record(payload) == record
    assert payload == original


def test_private_decode_progressively_releases_checkpoint_arrays(monkeypatch) -> None:
    record = CheckpointRecord(1, "stream", 1.0, _moderate_snapshot(3), "initial")
    payload = record.to_json_dict()
    snapshot_payload = payload["snapshot"]
    jobs = snapshot_payload["jobs"]
    watermarks = snapshot_payload["watermarks"]
    remaining_jobs = []
    remaining_watermarks = []
    decode_job = event_log._job_snapshot
    decode_watermark = event_log._job_watermark

    def observe_job(value):
        remaining_jobs.append(sum(item is not None for item in jobs))
        return decode_job(value)

    def observe_watermark(value):
        remaining_watermarks.append(sum(item is not None for item in watermarks))
        return decode_watermark(value)

    monkeypatch.setattr(event_log, "_job_snapshot", observe_job)
    monkeypatch.setattr(event_log, "_job_watermark", observe_watermark)

    assert event_log._decode_record(payload, destructive=True) == record
    assert remaining_jobs == [3, 2, 1]
    assert remaining_watermarks == [3, 2, 1]
    assert jobs == []
    assert watermarks == []
    assert "snapshot" not in payload
    assert "jobs" not in snapshot_payload
    assert "watermarks" not in snapshot_payload


def test_codec_tolerates_only_one_torn_trailing_line() -> None:
    header = encode_record(StreamHeader(0, "stream", WORKFLOW, 1.0, "6.0"))
    decoded = decode_jsonl(header + b'{"schema_version":1')

    assert len(decoded.records) == 1
    assert decoded.trailing_bytes == b'{"schema_version":1'
    with pytest.raises(EventLogFormatError, match="torn"):
        decode_jsonl(header + b"{", tolerate_torn_tail=False)


def test_codec_rejects_unsupported_versions_and_unconfirmed_records() -> None:
    unsupported = {
        "schema_version": 2,
        "record_type": "header",
        "sequence": 0,
        "stream_id": "s",
    }
    with pytest.raises(UnsupportedEventLogVersion):
        decode_json_line(json.dumps(unsupported) + "\n")

    header = encode_record(StreamHeader(0, "s", WORKFLOW, 1.0, "6.0"))
    with pytest.raises(EventLogFormatError, match="valid UTF-8"):
        decode_json_line(b"\xef\xbb\xbf" + header)

    draft_header = StreamHeader(0, "s", WORKFLOW, 1.0, "6.0").to_json_dict()
    draft_header["contract_status"] = "draft"
    with pytest.raises(EventLogFormatError, match="contract status"):
        decode_json_line(json.dumps(draft_header) + "\n")

    snapshot = _snapshot()
    transition = snapshot.jobs[0].transition
    assert transition is not None
    value = JobTransitionRecord(1, "s", snapshot.epoch, 1.0, transition).to_json_dict()
    value["confirmed"] = False
    with pytest.raises(EventLogFormatError, match="DB-confirmed"):
        decode_json_line(json.dumps(value) + "\n")


def test_writer_starts_with_header_checkpoint_and_secure_permissions(tmp_path) -> None:
    path = tmp_path / "workflow-events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        stream_id="stream",
    )

    writer.record_snapshot(_snapshot(), recorded_at_epoch=10.0)
    writer.close()

    records = _records(path)
    assert [record.sequence for record in records] == [0, 1]
    assert [record.to_json_dict()["record_type"] for record in records] == [
        "header",
        "checkpoint",
    ]
    assert records[1].reason == "initial"
    assert os.stat(path).st_mode & 0o777 == 0o600


def test_writer_streams_checkpoint_without_whole_snapshot_conversion(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "workflow-events.jsonl"
    snapshot = _moderate_snapshot()

    def fail_whole_record_conversion(_self):
        raise AssertionError("whole-checkpoint conversion must not be used")

    monkeypatch.setattr(DatabaseSnapshot, "to_json_dict", fail_whole_record_conversion)
    monkeypatch.setattr(CheckpointRecord, "to_json_dict", fail_whole_record_conversion)
    encoded = encode_record(CheckpointRecord(1, "stream", 10.0, snapshot, "initial"))
    assert decode_json_line(encoded).snapshot == snapshot

    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        stream_id="stream",
    )

    writer.record_snapshot(snapshot, recorded_at_epoch=10.0)
    writer.close()

    records = _records(path)
    assert len(records) == 2
    assert isinstance(records[1], CheckpointRecord)
    assert records[1].snapshot == snapshot


def test_new_writer_atomically_replaces_existing_torn_stream(tmp_path) -> None:
    path = tmp_path / "workflow-events.jsonl"
    path.write_bytes(b'{"torn":')
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        stream_id="fresh-stream",
    )

    writer.record_snapshot(_snapshot(), recorded_at_epoch=10.0)
    writer.close()

    records = _records(path)
    assert [record.stream_id for record in records] == [
        "fresh-stream",
        "fresh-stream",
    ]
    assert b'"torn"' not in path.read_bytes()


def test_writer_rejects_hardlinked_existing_output(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("existing")
    (tmp_path / "second-link").hardlink_to(path)
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0)

    with pytest.raises(UnsafeEventLogPath):
        writer.record_snapshot(_snapshot())


def test_writer_rejects_symlink_output(tmp_path) -> None:
    target = tmp_path / "target"
    target.write_text("")
    link = tmp_path / "events.jsonl"
    link.symlink_to(target)
    writer = EventLogWriter(link, WORKFLOW, "6.0", min_free_mb=0)

    with pytest.raises(UnsafeEventLogPath):
        writer.record_snapshot(_snapshot())


def test_writer_emits_new_transition_once_between_checkpoints(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        checkpoint_interval=1000,
        stream_id="stream",
    )
    first = _snapshot()
    first_transition = first.recent_transitions[0]
    second_transition = _job_transition("EXECUTE", "2", 2)
    second = _snapshot(
        epoch=2,
        state="EXECUTE",
        timestamp="2",
        sequence=2,
        recent=(first_transition, second_transition),
    )

    writer.record_snapshot(first, recorded_at_epoch=10.0)
    writer.record_snapshot(second, recorded_at_epoch=11.0)
    writer.record_snapshot(second, recorded_at_epoch=12.0)
    writer.close()

    records = _records(path)
    transitions = [
        record for record in records if isinstance(record, JobTransitionRecord)
    ]
    assert len(transitions) == 1
    assert transitions[0].transition.identity == second_transition.identity


def test_writer_emits_periodic_and_final_checkpoints(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        checkpoint_interval=5,
        stream_id="stream",
    )
    snapshot = _snapshot()

    writer.record_snapshot(snapshot, recorded_at_epoch=10.0)
    writer.record_snapshot(snapshot, recorded_at_epoch=15.0)
    writer.record_snapshot(snapshot, recorded_at_epoch=16.0, final=True)
    writer.close()

    reasons = [
        record.reason
        for record in _records(path)
        if isinstance(record, CheckpointRecord)
    ]
    assert reasons == ["initial", "periodic", "final"]


def test_attach_to_completed_is_explicit_and_repeated_final_is_idempotent(
    tmp_path,
) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0, stream_id="s")
    snapshot = _snapshot()

    writer.record_snapshot(snapshot, recorded_at_epoch=10.0, final=True)
    first_size = path.stat().st_size
    writer.record_snapshot(snapshot, recorded_at_epoch=11.0, final=True)
    writer.close()

    assert path.stat().st_size == first_size
    reasons = [
        record.reason
        for record in _records(path)
        if isinstance(record, CheckpointRecord)
    ]
    assert reasons == ["initial", "final"]


def test_final_checkpoint_takes_precedence_over_structure_change(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0)
    writer.record_snapshot(_snapshot(), recorded_at_epoch=1.0)
    changed = _snapshot(epoch=2)
    changed = replace(
        changed,
        jobs=(replace(changed.jobs[0], type_desc="compute-updated"),),
    )

    writer.record_snapshot(changed, recorded_at_epoch=2.0, final=True)
    writer.close()

    reasons = [
        record.reason
        for record in _records(path)
        if isinstance(record, CheckpointRecord)
    ]
    assert reasons == ["initial", "final"]


def test_disk_guard_reserves_gap_then_recovers_with_checkpoint(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    state = {"now": 0.0, "free": 1000 * 1024 * 1024}

    def clock() -> float:
        return state["now"]

    def usage(_path: Path):
        return types.SimpleNamespace(free=state["free"])

    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=100,
        checkpoint_interval=1000,
        stream_id="stream",
        clock=clock,
        disk_usage=usage,
    )
    snapshot = _snapshot()
    writer.record_snapshot(snapshot, recorded_at_epoch=0.0)

    state.update(now=10.0, free=10 * 1024 * 1024)
    writer.record_snapshot(snapshot, recorded_at_epoch=10.0)
    state["now"] = 20.0
    writer.record_snapshot(snapshot, recorded_at_epoch=20.0)
    assert writer.status.paused is True

    state.update(now=30.0, free=500 * 1024 * 1024)
    writer.record_snapshot(snapshot, recorded_at_epoch=30.0)
    writer.close()

    records = _records(path)
    assert [record.sequence for record in records] == [0, 1, 4, 5]
    gap = next(record for record in records if isinstance(record, GapRecord))
    assert (gap.first_missing_sequence, gap.last_missing_sequence) == (2, 3)
    checkpoint = records[-1]
    assert isinstance(checkpoint, CheckpointRecord)
    assert checkpoint.reason == "recovery"


def test_disk_guard_probe_failure_pauses_conservatively_then_recovers(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    state = {"fail": False}

    def usage(_path: Path):
        if state["fail"]:
            raise OSError("injected disk-usage failure")
        return types.SimpleNamespace(free=1000 * 1024 * 1024)

    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=100,
        disk_usage=usage,
        stream_id="s",
    )
    snapshot = _snapshot()
    writer.record_snapshot(snapshot, recorded_at_epoch=1.0)
    state["fail"] = True
    writer.record_snapshot(snapshot, recorded_at_epoch=2.0)
    assert writer.status.paused is True

    state["fail"] = False
    writer.record_snapshot(snapshot, recorded_at_epoch=3.0)
    writer.close()
    gap = next(record for record in _records(path) if isinstance(record, GapRecord))
    assert gap.reason is GapReason.DISK_GUARD


def test_diagnostic_records_are_redacted_and_do_not_change_state(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0, stream_id="s")
    snapshot = _snapshot()
    writer.record_snapshot(snapshot, recorded_at_epoch=1.0)
    finding = types.SimpleNamespace(
        exec_job_id="compute_ID0001",
        code="credential_failure",
        severity=DiagnosticSeverity.ERROR,
        summary="password=supersecret https://user:token@example.org/path",
        job_provenance="db_confirmed",
        evidence=(
            DiagnosticEvidence(
                SourceName.KICKSTART,
                "stderr",
                FrozenPayload.from_mapping(
                    {
                        "password": "bare-supersecret",
                        "Authorization": "Bearer raw-authorization-secret",
                        "privateKey": "raw-private-key",
                        "nested": {"url": "https://user:token@example.org/path"},
                        "items": list(range(140)),
                        "many": {f"field-{index}": index for index in range(140)},
                    }
                ),
            ),
        ),
    )
    batch = types.SimpleNamespace(new_findings=(finding,))

    writer.record_diagnostics(
        batch, snapshot_epoch=snapshot.epoch, recorded_at_epoch=2.0
    )
    writer.close()

    raw = path.read_text()
    assert "bare-supersecret" not in raw
    assert "raw-authorization-secret" not in raw
    assert "raw-private-key" not in raw
    assert "user:token" not in raw
    diagnostic = next(
        record for record in _records(path) if isinstance(record, DiagnosticRecord)
    )
    encoded = diagnostic.to_json_dict()
    assert encoded["changes_state"] is False
    payload = encoded["evidence"][0]["payload"]
    assert payload["password"] == "<redacted>"
    assert payload["Authorization"] == "<redacted>"
    assert payload["privateKey"] == "<redacted>"
    assert payload["items"][-1] == "<truncated>"
    assert payload["many"]["__truncated_entries__"] == 12


def test_diagnostic_epoch_must_match_current_authoritative_snapshot(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0)
    writer.record_snapshot(_snapshot(epoch=3), recorded_at_epoch=1.0)

    with pytest.raises(ValueError, match="current authoritative epoch"):
        writer.record_diagnostics(
            types.SimpleNamespace(new_findings=()),
            snapshot_epoch=SnapshotEpoch(2),
        )


def test_snapshot_workflow_must_match_writer_workflow(tmp_path) -> None:
    writer = EventLogWriter(
        tmp_path / "events.jsonl",
        WorkflowIdentity("other", "other-root"),
        "6.0",
        min_free_mb=0,
    )

    with pytest.raises(ValueError, match="snapshot workflow"):
        writer.record_snapshot(_snapshot())


def test_database_replacement_forces_checkpoint(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        checkpoint_interval=1000,
        stream_id="stream",
    )
    writer.record_snapshot(_snapshot(), recorded_at_epoch=1.0)
    replacement = replace(_snapshot(epoch=2), generation=DatabaseGeneration(2, 10, 21))
    writer.record_snapshot(replacement, recorded_at_epoch=2.0)
    writer.close()

    reasons = [
        record.reason
        for record in _records(path)
        if isinstance(record, CheckpointRecord)
    ]
    assert reasons == ["initial", "database_replacement"]


def test_path_replacement_starts_fresh_stream_with_replacement_checkpoint(
    tmp_path,
) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        stream_id="original-stream",
    )
    writer.record_snapshot(_snapshot(), recorded_at_epoch=1.0)

    replacement = tmp_path / "replacement"
    replacement.write_text("external replacement")
    os.replace(replacement, path)
    writer.record_snapshot(_snapshot(epoch=2), recorded_at_epoch=2.0)
    writer.close()

    records = _records(path)
    assert [record.sequence for record in records] == [0, 1]
    assert records[0].stream_id != "original-stream"
    assert isinstance(records[1], CheckpointRecord)
    assert records[1].reason == "stream_replacement"


def test_replacement_checkpoint_reason_survives_guarded_start_retry(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    state = {"free": 1000 * 1024 * 1024}

    def usage(_path: Path):
        return types.SimpleNamespace(free=state["free"])

    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=100,
        stream_id="original-stream",
        disk_usage=usage,
    )
    writer.record_snapshot(_snapshot(), recorded_at_epoch=1.0)
    replacement = tmp_path / "replacement"
    replacement.write_text("external replacement")
    os.replace(replacement, path)

    state["free"] = 1
    writer.record_snapshot(_snapshot(epoch=2), recorded_at_epoch=2.0)
    assert writer.status.started is False
    assert path.read_text() == "external replacement"

    state["free"] = 1000 * 1024 * 1024
    writer.record_snapshot(_snapshot(epoch=2), recorded_at_epoch=3.0)
    writer.close()
    records = _records(path)
    assert isinstance(records[1], CheckpointRecord)
    assert records[1].reason == "stream_replacement"


def test_hard_size_cap_preflights_checkpoint_and_recovers_with_gap(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=0,
        checkpoint_interval=1,
        stream_id="stream",
    )
    snapshot = _snapshot()
    writer.record_snapshot(snapshot, recorded_at_epoch=1.0)
    initial_size = path.stat().st_size
    writer.max_log_bytes = initial_size + 1

    writer.record_snapshot(snapshot, recorded_at_epoch=2.0)

    assert writer.status.paused is True
    assert path.stat().st_size == initial_size
    assert path.stat().st_size <= writer.max_log_bytes

    writer.max_log_bytes = initial_size + 1024 * 1024
    writer.record_snapshot(snapshot, recorded_at_epoch=3.0)
    writer.close()

    records = _records(path)
    gap = next(record for record in records if isinstance(record, GapRecord))
    assert gap.reason is GapReason.SIZE_LIMIT
    assert (gap.first_missing_sequence, gap.last_missing_sequence) == (2, 2)
    assert isinstance(records[-1], CheckpointRecord)
    assert records[-1].reason == "recovery"


def test_initial_write_failure_preserves_target_and_retry_starts_clean_stream(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("preserve until complete")
    writer = EventLogWriter(path, WORKFLOW, "6.0", min_free_mb=0, stream_id="s")
    real_write = os.write
    state = {"fail": True}

    def fail_once(fd: int, data: bytes) -> int:
        if state["fail"]:
            state["fail"] = False
            raise OSError("injected write failure")
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", fail_once)
    writer.record_snapshot(_snapshot(), recorded_at_epoch=1.0)
    assert path.read_text() == "preserve until complete"
    assert writer.status.started is False

    writer.record_snapshot(_snapshot(), recorded_at_epoch=2.0)
    writer.close()
    assert [record.sequence for record in _records(path)] == [0, 1]


def test_recovery_batch_write_failure_extends_gap_and_retries_atomically(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "events.jsonl"
    state = {"free": 1000 * 1024 * 1024}

    def usage(_path: Path):
        return types.SimpleNamespace(free=state["free"])

    writer = EventLogWriter(
        path,
        WORKFLOW,
        "6.0",
        min_free_mb=100,
        stream_id="s",
        disk_usage=usage,
    )
    snapshot = _snapshot()
    writer.record_snapshot(snapshot, recorded_at_epoch=1.0)
    state["free"] = 1
    writer.record_snapshot(snapshot, recorded_at_epoch=2.0)

    real_write = os.write
    state["free"] = 1000 * 1024 * 1024
    state["fail"] = True

    def fail_once(fd: int, data: bytes) -> int:
        if state.pop("fail", False):
            raise OSError("injected recovery failure")
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", fail_once)
    writer.record_snapshot(snapshot, recorded_at_epoch=3.0)
    assert writer.status.paused is True
    writer.record_snapshot(snapshot, recorded_at_epoch=4.0)
    writer.close()

    records = _records(path)
    assert [record.sequence for record in records] == [0, 1, 5, 6]
    gap = records[2]
    assert isinstance(gap, GapRecord)
    assert (gap.first_missing_sequence, gap.last_missing_sequence) == (2, 4)
    assert isinstance(records[-1], CheckpointRecord)
    assert records[-1].reason == "recovery"
