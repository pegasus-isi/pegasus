"""Versioned, DB-confirmed JSONL recording for :mod:`Pegasus.monitor`.

The writer is deliberately source-neutral.  It accepts immutable coordinator
publications plus the reconciler's authoritative :class:`DatabaseSnapshot` and
never opens Stampede, tails ``jobstate.log``, or invokes HTCondor itself.
"""

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
import math
import os
import re
import shutil
import stat
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias
from uuid import uuid4

from Pegasus.monitor.models import (
    JSONL_V1_CONTRACT_STATUS,
    JSONL_V1_SCHEMA_VERSION,
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

EventRecord: TypeAlias = (
    StreamHeader
    | CheckpointRecord
    | JobTransitionRecord
    | WorkflowTransitionRecord
    | EnrichmentRecord
    | DiagnosticRecord
    | GapRecord
)

DEFAULT_CHECKPOINT_INTERVAL = 300.0
DEFAULT_MIN_FREE_MB = 200.0
MAX_DIAGNOSTIC_PAYLOAD_DEPTH = 8
MAX_DIAGNOSTIC_MAPPING_ENTRIES = 128
MAX_DIAGNOSTIC_SEQUENCE_ENTRIES = 128
_SECRET_KEY_PARTS = frozenset(
    {
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "key",
        "passwd",
        "passphrase",
        "password",
        "secret",
        "token",
    }
)
_JSON_ENCODER = json.JSONEncoder(
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=False,
)
_STREAM_BUFFER_BYTES = 64 * 1024


class EventLogError(RuntimeError):
    """Base error for the canonical event stream."""


class EventLogFormatError(EventLogError, ValueError):
    """A complete record violates the schema-v1 contract."""


class UnsupportedEventLogVersion(EventLogFormatError):
    """The stream uses a schema version this reader cannot consume."""


class UnsafeEventLogPath(EventLogError):
    """The requested output path cannot be opened without following a link."""


@dataclass(frozen=True, slots=True)
class _PathIdentity:
    device: int
    inode: int
    size: int


@dataclass(frozen=True, slots=True)
class DecodedJSONL:
    """Complete decoded records plus a tolerated trailing partial line."""

    records: tuple[EventRecord, ...]
    trailing_bytes: bytes = b""


@dataclass(frozen=True, slots=True)
class EventLogStatus:
    """Small immutable status surface for display/server health reporting."""

    path: Path
    stream_id: str
    sequence: int
    started: bool
    paused: bool
    gap_first_sequence: int | None
    gap_last_sequence: int | None
    last_checkpoint_epoch: float | None


def encode_record(record: EventRecord) -> bytes:
    """Encode one canonical record as one compact UTF-8 JSONL line."""

    encoded = bytearray()
    for chunk in _iter_record_bytes(record):
        encoded.extend(chunk)
    return bytes(encoded)


def _encode_small_record(record: EventRecord) -> bytes:
    try:
        payload = json.dumps(
            record.to_json_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise EventLogFormatError(str(error)) from error
    return payload.encode("utf-8") + b"\n"


def _iter_json_bytes(value: object) -> Iterator[bytes]:
    for chunk in _JSON_ENCODER.iterencode(value):
        yield chunk.encode("utf-8")


def _iter_json_array(values: Iterable[object]) -> Iterator[bytes]:
    yield b"["
    separator = b""
    for value in values:
        yield separator
        yield from _iter_json_bytes(value)
        separator = b","
    yield b"]"


def _iter_snapshot_bytes(snapshot: DatabaseSnapshot) -> Iterator[bytes]:
    """Yield canonical snapshot JSON while materializing one array item at a time."""

    yield b'{"epoch":'
    yield from _iter_json_bytes(snapshot.epoch.value)
    yield b',"generation":'
    yield from _iter_json_bytes(snapshot.generation.to_json_dict())
    yield b',"snapshot_at_epoch":'
    yield from _iter_json_bytes(snapshot.snapshot_at_epoch)
    yield b',"workflow":'
    yield from _iter_json_bytes(snapshot.workflow.to_json_dict())
    yield b',"jobs":'
    yield from _iter_json_array(job.to_json_dict() for job in snapshot.jobs)
    yield b',"recent_transitions":'
    yield from _iter_json_array(
        transition.to_json_dict() for transition in snapshot.recent_transitions
    )
    yield b',"recent_workflow_transitions":'
    yield from _iter_json_array(
        transition.to_json_dict() for transition in snapshot.recent_workflow_transitions
    )
    yield b',"watermarks":'
    yield from _iter_json_array(
        watermark.to_json_dict() for watermark in snapshot.watermarks
    )
    yield b',"workflow_watermark":'
    yield from _iter_json_bytes(snapshot.workflow_watermark.to_json_dict())
    yield b"}"


def _iter_checkpoint_bytes(record: CheckpointRecord) -> Iterator[bytes]:
    yield b'{"schema_version":'
    yield from _iter_json_bytes(JSONL_V1_SCHEMA_VERSION)
    yield b',"record_type":"checkpoint","sequence":'
    yield from _iter_json_bytes(record.sequence)
    yield b',"stream_id":'
    yield from _iter_json_bytes(record.stream_id)
    yield b',"recorded_at_epoch":'
    yield from _iter_json_bytes(record.recorded_at_epoch)
    yield b',"snapshot":'
    yield from _iter_snapshot_bytes(record.snapshot)
    yield b',"reason":'
    yield from _iter_json_bytes(record.reason)
    yield b"}\n"


def _iter_record_bytes(record: EventRecord) -> Iterator[bytes]:
    try:
        if isinstance(record, CheckpointRecord):
            yield from _iter_checkpoint_bytes(record)
        else:
            yield _encode_small_record(record)
    except (TypeError, ValueError) as error:
        raise EventLogFormatError(str(error)) from error


def _encoded_records_size(records: Sequence[EventRecord]) -> int:
    return sum(len(chunk) for record in records for chunk in _iter_record_bytes(record))


def decode_json_line(line: bytes | str) -> EventRecord:
    """Decode one complete JSON line into the shared immutable contracts."""

    if isinstance(line, bytes):
        terminated = line.endswith(b"\n")
    else:
        terminated = line.endswith("\n")
    if not terminated:
        raise EventLogFormatError("record is not newline terminated")
    if isinstance(line, bytes) and json.detect_encoding(line) != "utf-8":
        raise EventLogFormatError("record is not valid UTF-8")
    try:
        payload = json.loads(line)
    except UnicodeDecodeError as error:
        raise EventLogFormatError("record is not valid UTF-8") from error
    except json.JSONDecodeError as error:
        raise EventLogFormatError(f"invalid JSON record: {error.msg}") from error
    return _decode_record(payload, destructive=True)


def decode_jsonl(data: bytes, *, tolerate_torn_tail: bool = True) -> DecodedJSONL:
    """Decode complete lines and optionally retain one torn trailing record."""

    if not data:
        return DecodedJSONL(())
    complete = data
    trailing = b""
    if not data.endswith(b"\n"):
        boundary = data.rfind(b"\n")
        if boundary < 0:
            if tolerate_torn_tail:
                return DecodedJSONL((), data)
            raise EventLogFormatError("stream ends with a torn record")
        complete = data[: boundary + 1]
        trailing = data[boundary + 1 :]
        if not tolerate_torn_tail:
            raise EventLogFormatError("stream ends with a torn record")
    records = tuple(
        decode_json_line(line) for line in complete.splitlines(keepends=True)
    )
    return DecodedJSONL(records, trailing)


def read_jsonl(path: Path, *, tolerate_torn_tail: bool = True) -> DecodedJSONL:
    """Read a local event stream without invoking any workflow data source."""

    return decode_jsonl(path.read_bytes(), tolerate_torn_tail=tolerate_torn_tail)


def decode_record(value: object) -> EventRecord:
    """Validate and decode one already-parsed JSON object."""

    return _decode_record(value, destructive=False)


def _decode_record(value: object, *, destructive: bool) -> EventRecord:
    """Decode parsed JSON, optionally releasing private parser containers."""

    payload = _mapping(value, "record")
    version = _integer(payload.get("schema_version"), "schema_version")
    if version != JSONL_V1_SCHEMA_VERSION:
        raise UnsupportedEventLogVersion(f"unsupported schema version: {version}")
    record_type = _string(payload.get("record_type"), "record_type")
    sequence = _integer(payload.get("sequence"), "sequence")
    stream_id = _string(payload.get("stream_id"), "stream_id")

    try:
        if record_type == "header":
            if payload.get("contract_status") != JSONL_V1_CONTRACT_STATUS:
                raise EventLogFormatError("unsupported schema-v1 contract status")
            return StreamHeader(
                sequence,
                stream_id,
                _workflow_identity(payload.get("workflow")),
                _finite_float(payload.get("created_at_epoch"), "created_at_epoch"),
                _string(payload.get("monitor_version"), "monitor_version"),
                _payload(payload.get("source_metadata", {}), "source_metadata"),
            )
        recorded = _finite_float(payload.get("recorded_at_epoch"), "recorded_at_epoch")
        if record_type == "checkpoint":
            return CheckpointRecord(
                sequence,
                stream_id,
                recorded,
                _database_snapshot(
                    _take(payload, "snapshot", destructive=destructive),
                    destructive=destructive,
                ),
                _string(payload.get("reason"), "reason"),
            )
        if record_type == "gap":
            if payload.get("next_checkpoint_required") is not True:
                raise EventLogFormatError("gap must require a checkpoint")
            last = payload.get("last_missing_sequence")
            return GapRecord(
                sequence,
                stream_id,
                recorded,
                GapReason(_string(payload.get("reason"), "reason")),
                _integer(
                    payload.get("first_missing_sequence"), "first_missing_sequence"
                ),
                None if last is None else _integer(last, "last_missing_sequence"),
            )
        epoch = SnapshotEpoch(_integer(payload.get("snapshot_epoch"), "snapshot_epoch"))
        if record_type == "job_transition":
            if payload.get("confirmed") is not True:
                raise EventLogFormatError("job transition is not DB-confirmed")
            return JobTransitionRecord(
                sequence,
                stream_id,
                epoch,
                recorded,
                _job_transition(payload.get("transition")),
            )
        if record_type == "workflow_transition":
            if payload.get("confirmed") is not True:
                raise EventLogFormatError("workflow transition is not DB-confirmed")
            return WorkflowTransitionRecord(
                sequence,
                stream_id,
                epoch,
                recorded,
                _workflow_transition(payload.get("transition")),
            )
        if record_type == "enrichment":
            expires = payload.get("expires_at_epoch")
            return EnrichmentRecord(
                sequence,
                stream_id,
                epoch,
                recorded,
                SourceName(_string(payload.get("source"), "source")),
                _payload(payload.get("target", {}), "target"),
                _payload(payload.get("payload", {}), "payload"),
                None if expires is None else _finite_float(expires, "expires_at_epoch"),
            )
        if record_type == "diagnostic_result":
            if payload.get("changes_state") is not False:
                raise EventLogFormatError("diagnostic_result must not change state")
            evidence = tuple(
                _diagnostic_evidence(item)
                for item in _sequence(payload.get("evidence", []), "evidence")
            )
            return DiagnosticRecord(
                sequence,
                stream_id,
                epoch,
                recorded,
                _payload(payload.get("target", {}), "target"),
                _string(payload.get("code"), "code"),
                DiagnosticSeverity(_string(payload.get("severity"), "severity")),
                _string(payload.get("summary"), "summary"),
                evidence,
            )
    except (TypeError, ValueError) as error:
        if isinstance(error, EventLogFormatError):
            raise
        raise EventLogFormatError(str(error)) from error
    raise EventLogFormatError(f"unknown record type: {record_type}")


class EventLogWriter:
    """Write one canonical DB-confirmed stream without blocking the workflow."""

    def __init__(
        self,
        path: Path,
        workflow: WorkflowIdentity,
        monitor_version: str,
        *,
        source_metadata: Mapping[str, object] | None = None,
        min_free_mb: float = DEFAULT_MIN_FREE_MB,
        max_log_mb: float | None = None,
        checkpoint_interval: float = DEFAULT_CHECKPOINT_INTERVAL,
        stream_id: str | None = None,
        clock: Callable[[], float] = time.time,
        disk_usage: Callable[[Path], object] = shutil.disk_usage,
    ) -> None:
        if min_free_mb < 0:
            raise ValueError("min_free_mb must be non-negative")
        if max_log_mb is not None and max_log_mb <= 0:
            raise ValueError("max_log_mb must be positive")
        if checkpoint_interval <= 0 or not math.isfinite(checkpoint_interval):
            raise ValueError("checkpoint_interval must be finite and positive")
        self.path = Path(path)
        self.workflow = workflow
        self.monitor_version = monitor_version
        self.source_metadata = FrozenPayload.from_mapping(source_metadata or {})
        self.min_free_bytes = int(min_free_mb * 1024 * 1024)
        self.max_log_bytes = (
            None if max_log_mb is None else int(max_log_mb * 1024 * 1024)
        )
        self.checkpoint_interval = checkpoint_interval
        self.stream_id = stream_id or str(uuid4())
        self._clock = clock
        self._disk_usage = disk_usage
        self._fd: int | None = None
        self._path_identity: _PathIdentity | None = None
        self._sequence = 0
        self._started = False
        self._closed = False
        self._paused = False
        self._pause_reason = GapReason.DISK_GUARD
        self._gap_first: int | None = None
        self._gap_last: int | None = None
        self._last_checkpoint_epoch: float | None = None
        self._last_generation: DatabaseGeneration | None = None
        self._last_structure: tuple[object, ...] | None = None
        self._known_job_transitions: set[DBTransitionIdentity] = set()
        self._known_workflow_transitions: set[DBWorkflowTransitionIdentity] = set()
        self._diagnostic_keys: set[tuple[object, ...]] = set()
        self._current_snapshot_epoch: SnapshotEpoch | None = None
        self._last_final_snapshot: DatabaseSnapshot | None = None
        self._pending_checkpoint_reason = "initial"

    @property
    def status(self) -> EventLogStatus:
        return EventLogStatus(
            self.path,
            self.stream_id,
            self._sequence,
            self._started,
            self._paused,
            self._gap_first,
            self._gap_last,
            self._last_checkpoint_epoch,
        )

    def record_publication(
        self,
        publication: object,
        database: DatabaseSnapshot | None,
    ) -> None:
        """Record one coordinator publication using only its authoritative base."""

        if database is None:
            return
        clock = getattr(publication, "clock", None)
        publication_epoch = getattr(clock, "epoch", None)
        recorded_at = float(
            self._clock() if publication_epoch is None else publication_epoch
        )
        final = bool(getattr(publication, "authoritative_complete", False))
        self.record_snapshot(database, recorded_at_epoch=recorded_at, final=final)

    def record_snapshot(
        self,
        snapshot: DatabaseSnapshot,
        *,
        recorded_at_epoch: float | None = None,
        final: bool = False,
    ) -> None:
        """Emit transitions or a checkpoint for one authoritative DB snapshot."""

        self._ensure_open_state()
        self._validate_snapshot(snapshot)
        now = self._finite_now(recorded_at_epoch)
        if not self._started:
            self._start_stream(
                snapshot, now, self._pending_checkpoint_reason, final=final
            )
            return
        if not self._active_path_matches():
            self._restart_after_replacement(snapshot, now, final=final)
            return
        capacity_reason = self._capacity_failure(0, recovering=self._paused)
        if capacity_reason is not None:
            self._reserve_gap(capacity_reason)
            return
        if self._paused:
            self._recover(snapshot, now, final=final)
            return
        if final and snapshot == self._last_final_snapshot:
            self._current_snapshot_epoch = snapshot.epoch
            return

        reason: str | None = None
        structure = _database_structure(snapshot)
        if final:
            reason = "final"
        elif snapshot.generation != self._last_generation:
            reason = "database_replacement"
        elif structure != self._last_structure:
            reason = "structure_change"
        elif (
            self._last_checkpoint_epoch is None
            or now - self._last_checkpoint_epoch >= self.checkpoint_interval
        ):
            reason = "periodic"
        if reason is not None:
            self._write_checkpoint(snapshot, now, reason)
            return

        new_events: list[DBJobTransition | DBWorkflowTransition] = []
        for transition in snapshot.recent_transitions:
            if transition.identity not in self._known_job_transitions:
                new_events.append(transition)
        for transition in snapshot.recent_workflow_transitions:
            if transition.identity not in self._known_workflow_transitions:
                new_events.append(transition)
        new_events.sort(key=_transition_order)
        for transition in new_events:
            sequence = self._next_sequence()
            if isinstance(transition, DBJobTransition):
                record: EventRecord = JobTransitionRecord(
                    sequence,
                    self.stream_id,
                    snapshot.epoch,
                    now,
                    transition,
                )
                self._known_job_transitions.add(transition.identity)
            else:
                record = WorkflowTransitionRecord(
                    sequence,
                    self.stream_id,
                    snapshot.epoch,
                    now,
                    transition,
                )
                self._known_workflow_transitions.add(transition.identity)
            if not self._write_record(record):
                return
        self._current_snapshot_epoch = snapshot.epoch

    def record_diagnostics(
        self,
        diagnostics: object,
        *,
        snapshot_epoch: SnapshotEpoch,
        recorded_at_epoch: float | None = None,
    ) -> None:
        """Persist newly emitted diagnostics after recursive bounded redaction."""

        if not self._started or self._closed:
            return
        if snapshot_epoch != self._current_snapshot_epoch:
            raise ValueError(
                "diagnostic snapshot_epoch must match the current authoritative epoch"
            )
        if not self._active_path_matches():
            self._abandon_replaced_stream()
            return
        now = self._finite_now(recorded_at_epoch)
        findings = tuple(getattr(diagnostics, "new_findings", ()))
        for finding in findings:
            key = (
                snapshot_epoch.value,
                getattr(finding, "exec_job_id", None),
                getattr(finding, "code", None),
                getattr(finding, "summary", None),
            )
            if key in self._diagnostic_keys:
                continue
            if self._paused:
                # Only an authoritative snapshot may close a gap and establish
                # the required recovery checkpoint.
                return
            evidence = tuple(
                DiagnosticEvidence(
                    item.source,
                    str(item.code),
                    FrozenPayload.from_mapping(
                        _redact_payload(item.payload.to_json_dict())
                    ),
                )
                for item in getattr(finding, "evidence", ())
            )
            target = FrozenPayload.from_mapping(
                {
                    "workflow": self.workflow.to_json_dict(),
                    "exec_job_id": str(getattr(finding, "exec_job_id", "")),
                    "job_provenance": str(
                        getattr(finding, "job_provenance", "unknown")
                    ),
                }
            )
            severity = getattr(finding, "severity", DiagnosticSeverity.INFO)
            if not isinstance(severity, DiagnosticSeverity):
                severity = DiagnosticSeverity(str(severity))
            record = DiagnosticRecord(
                self._next_sequence(),
                self.stream_id,
                snapshot_epoch,
                now,
                target,
                str(getattr(finding, "code", "diagnostic")),
                severity,
                _redact_text(str(getattr(finding, "summary", "")), 2048),
                evidence,
            )
            if not self._write_record(record):
                return
            self._diagnostic_keys.add(key)

    def close(self) -> None:
        """Close the stream descriptor; closing never mutates workflow state."""

        if self._closed:
            return
        self._closed = True
        if self._fd is not None:
            try:
                os.close(self._fd)
            finally:
                self._fd = None
                self._path_identity = None

    def _start_stream(
        self,
        snapshot: DatabaseSnapshot,
        recorded_at: float,
        checkpoint_reason: str,
        *,
        final: bool,
    ) -> None:
        """Atomically replace the target with a complete fresh stream prefix."""

        header = StreamHeader(
            0,
            self.stream_id,
            self.workflow,
            recorded_at,
            self.monitor_version,
            self.source_metadata,
        )
        checkpoint = CheckpointRecord(
            1, self.stream_id, recorded_at, snapshot, checkpoint_reason
        )
        records: list[EventRecord] = [header, checkpoint]
        if final:
            records.append(
                CheckpointRecord(2, self.stream_id, recorded_at, snapshot, "final")
            )
        if not self._atomic_replace_stream(records):
            return
        self._sequence = len(records) - 1
        self._started = True
        self._paused = False
        self._gap_first = None
        self._gap_last = None
        self._apply_checkpoint(snapshot, recorded_at)
        self._last_final_snapshot = snapshot if final else None
        self._pending_checkpoint_reason = "initial"

    def _restart_after_replacement(
        self, snapshot: DatabaseSnapshot, recorded_at: float, *, final: bool
    ) -> None:
        self._abandon_replaced_stream()
        self.stream_id = str(uuid4())
        self._reset_stream_state()
        self._pending_checkpoint_reason = "stream_replacement"
        self._start_stream(snapshot, recorded_at, "stream_replacement", final=final)

    def _recover(
        self, snapshot: DatabaseSnapshot, recorded_at: float, *, final: bool
    ) -> None:
        if self._gap_first is None or self._gap_last is None:
            raise EventLogError("paused stream is missing its gap range")
        gap = GapRecord(
            self._sequence + 1,
            self.stream_id,
            recorded_at,
            self._pause_reason,
            self._gap_first,
            self._gap_last,
        )
        checkpoint = CheckpointRecord(
            self._sequence + 2,
            self.stream_id,
            recorded_at,
            snapshot,
            "recovery",
        )
        records: list[EventRecord] = [gap, checkpoint]
        if final:
            records.append(
                CheckpointRecord(
                    self._sequence + 3,
                    self.stream_id,
                    recorded_at,
                    snapshot,
                    "final",
                )
            )
        if not self._write_recovery_batch(records):
            return
        self._paused = False
        self._gap_first = None
        self._gap_last = None
        self._apply_checkpoint(snapshot, recorded_at)
        self._last_final_snapshot = snapshot if final else self._last_final_snapshot

    def _write_checkpoint(
        self, snapshot: DatabaseSnapshot, recorded_at: float, reason: str
    ) -> None:
        record = CheckpointRecord(
            self._next_sequence(), self.stream_id, recorded_at, snapshot, reason
        )
        if not self._write_record(record):
            return
        self._apply_checkpoint(snapshot, recorded_at)
        if reason == "final":
            self._last_final_snapshot = snapshot

    def _apply_checkpoint(self, snapshot: DatabaseSnapshot, recorded_at: float) -> None:
        self._last_checkpoint_epoch = recorded_at
        self._last_generation = snapshot.generation
        self._last_structure = _database_structure(snapshot)
        self._known_job_transitions = _job_transition_identities(snapshot)
        self._known_workflow_transitions = _workflow_transition_identities(snapshot)
        self._current_snapshot_epoch = snapshot.epoch

    def _reserve_gap(self, reason: GapReason) -> None:
        self._paused = True
        self._pause_reason = reason
        missing = self._next_sequence()
        if self._gap_first is None:
            self._gap_first = missing
        self._gap_last = missing

    def _write_record(
        self, record: EventRecord, *, expected_sequence: int | None = None
    ) -> bool:
        fd = self._active_fd()
        sequence = record.sequence
        if expected_sequence is not None:
            if sequence != expected_sequence:
                raise EventLogError("unexpected stream-header sequence")
        elif sequence != self._sequence:
            raise EventLogError("record sequence was not reserved")
        if not self._active_path_matches():
            if sequence > 0:
                self._mark_missing(sequence, GapReason.STREAM_REPLACED)
            self._abandon_replaced_stream()
            return False
        encoded_size = _encoded_records_size((record,))
        capacity_reason = self._capacity_failure(encoded_size)
        if capacity_reason is not None:
            if sequence > 0:
                self._mark_missing(sequence, capacity_reason)
            return False
        start = self._expected_size()
        try:
            self._write_records(fd, (record,))
        except OSError:
            self._rollback_partial_write(fd, start)
            if sequence > 0:
                self._mark_missing(sequence, GapReason.WRITER_ERROR)
            return False
        self._set_expected_size(start + encoded_size)
        return True

    def _atomic_replace_stream(self, records: Sequence[EventRecord]) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        original = self._safe_target_status()
        temp_path = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(temp_path, flags, 0o600)
        except OSError as error:
            raise UnsafeEventLogPath(
                f"cannot create secure event-log temporary file: {temp_path}"
            ) from error
        try:
            self._validate_open_fd(fd, temp_path)
            os.fchmod(fd, 0o600)
            encoded_size = _encoded_records_size(records)
            reason = self._capacity_failure_for(fd, encoded_size, recovering=False)
            if reason is not None:
                return False
            self._write_records(fd, records)
            os.fsync(fd)
            self._require_unchanged_target(original)
            os.replace(temp_path, self.path)
            self._fsync_parent()
            final_status = os.fstat(fd)
            self._fd = fd
            self._path_identity = _PathIdentity(
                final_status.st_dev, final_status.st_ino, final_status.st_size
            )
            fd = -1
            return True
        except OSError:
            # The old target is preserved until replace.  If the replace itself
            # succeeded, the complete fsynced prefix remains a valid stream and
            # the next publication can safely replace it again.
            return False
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    def _write_recovery_batch(self, records: Sequence[EventRecord]) -> bool:
        fd = self._active_fd()
        if not self._active_path_matches():
            self._abandon_replaced_stream()
            return False
        encoded_size = _encoded_records_size(records)
        capacity_reason = self._capacity_failure(encoded_size, recovering=True)
        if capacity_reason is not None:
            self._reserve_gap(capacity_reason)
            return False
        start = self._expected_size()
        final_sequence = records[-1].sequence
        try:
            self._write_records(fd, records)
        except OSError:
            self._rollback_partial_write(fd, start)
            self._sequence = final_sequence
            self._mark_missing(final_sequence, GapReason.WRITER_ERROR)
            return False
        self._sequence = final_sequence
        self._set_expected_size(start + encoded_size)
        return True

    def _capacity_failure(
        self, encoded_size: int, *, recovering: bool = False
    ) -> GapReason | None:
        return self._capacity_failure_for(
            self._active_fd(), encoded_size, recovering=recovering
        )

    def _capacity_failure_for(
        self, fd: int, encoded_size: int, *, recovering: bool
    ) -> GapReason | None:
        try:
            opened = os.fstat(fd)
        except OSError:
            return GapReason.DISK_GUARD
        if self.max_log_bytes is not None:
            if opened.st_size + encoded_size > self.max_log_bytes:
                return GapReason.SIZE_LIMIT
        if self.min_free_bytes <= 0:
            return None
        try:
            free = int(getattr(self._disk_usage(self.path.parent), "free"))
        except (OSError, TypeError, ValueError, AttributeError):
            return GapReason.DISK_GUARD
        minimum = self.min_free_bytes
        if recovering:
            minimum = int(minimum * 1.5)
        if free - encoded_size < minimum:
            return GapReason.DISK_GUARD
        return None

    def _active_fd(self) -> int:
        if self._fd is None or self._path_identity is None:
            raise EventLogError("event-log stream is not open")
        return self._fd

    def _active_path_matches(self) -> bool:
        if self._fd is None or self._path_identity is None:
            return False
        try:
            path_status = self.path.lstat()
            self._validate_target_status(path_status)
            opened = os.fstat(self._fd)
        except (OSError, UnsafeEventLogPath):
            return False
        expected = self._path_identity
        return (
            path_status.st_dev == expected.device
            and path_status.st_ino == expected.inode
            and opened.st_dev == expected.device
            and opened.st_ino == expected.inode
            and opened.st_size == expected.size
            and path_status.st_size == expected.size
        )

    def _safe_target_status(self) -> os.stat_result | None:
        try:
            status = self.path.lstat()
        except FileNotFoundError:
            return None
        self._validate_target_status(status)
        return status

    def _validate_target_status(self, status: os.stat_result) -> None:
        if (
            stat.S_ISLNK(status.st_mode)
            or not stat.S_ISREG(status.st_mode)
            or status.st_nlink != 1
            or status.st_uid != os.geteuid()
        ):
            raise UnsafeEventLogPath(f"unsafe event-log path: {self.path}")

    def _require_unchanged_target(self, original: os.stat_result | None) -> None:
        current = self._safe_target_status()
        if original is None:
            unchanged = current is None
        else:
            unchanged = current is not None and (
                current.st_dev,
                current.st_ino,
            ) == (original.st_dev, original.st_ino)
        if not unchanged:
            raise UnsafeEventLogPath(
                f"event-log target changed while opening: {self.path}"
            )

    def _validate_open_fd(self, fd: int, path: Path) -> os.stat_result:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != os.geteuid()
        ):
            raise UnsafeEventLogPath(f"unsafe open event-log file: {path}")
        return opened

    def _fsync_parent(self) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        try:
            directory_fd = os.open(self.path.parent, flags)
        except OSError:
            return
        try:
            try:
                os.fsync(directory_fd)
            except OSError:
                # Some otherwise-safe filesystems do not permit directory
                # fsync.  The file itself was fsynced before atomic replace.
                pass
        finally:
            os.close(directory_fd)

    @staticmethod
    def _write_all(fd: int, encoded: bytes) -> None:
        written = 0
        while written < len(encoded):
            count = os.write(fd, encoded[written:])
            if count <= 0:
                raise OSError("short event-log write")
            written += count

    @classmethod
    def _write_records(cls, fd: int, records: Sequence[EventRecord]) -> None:
        buffer = bytearray()
        for record in records:
            for chunk in _iter_record_bytes(record):
                if not chunk:
                    continue
                if len(buffer) + len(chunk) > _STREAM_BUFFER_BYTES:
                    if buffer:
                        cls._write_all(fd, bytes(buffer))
                        buffer.clear()
                    if len(chunk) >= _STREAM_BUFFER_BYTES:
                        cls._write_all(fd, chunk)
                        continue
                buffer.extend(chunk)
        if buffer:
            cls._write_all(fd, bytes(buffer))

    def _rollback_partial_write(self, fd: int, start: int) -> None:
        try:
            os.ftruncate(fd, start)
            os.lseek(fd, 0, os.SEEK_END)
        except OSError as truncate_error:
            raise EventLogError(
                "event log contains an unrecoverable partial record"
            ) from truncate_error

    def _expected_size(self) -> int:
        if self._path_identity is None:
            raise EventLogError("event-log path identity is unavailable")
        return self._path_identity.size

    def _set_expected_size(self, size: int) -> None:
        if self._path_identity is None:
            raise EventLogError("event-log path identity is unavailable")
        self._path_identity = _PathIdentity(
            self._path_identity.device, self._path_identity.inode, size
        )

    def _mark_missing(self, sequence: int, reason: GapReason) -> None:
        self._paused = True
        self._pause_reason = reason
        if self._gap_first is None:
            self._gap_first = sequence
        self._gap_last = sequence

    def _abandon_replaced_stream(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
        self._fd = None
        self._path_identity = None

    def _reset_stream_state(self) -> None:
        self._sequence = 0
        self._started = False
        self._paused = False
        self._pause_reason = GapReason.STREAM_REPLACED
        self._gap_first = None
        self._gap_last = None
        self._last_checkpoint_epoch = None
        self._last_generation = None
        self._last_structure = None
        self._known_job_transitions.clear()
        self._known_workflow_transitions.clear()
        self._diagnostic_keys.clear()
        self._current_snapshot_epoch = None
        self._last_final_snapshot = None

    def _validate_snapshot(self, snapshot: DatabaseSnapshot) -> None:
        if snapshot.workflow.workflow != self.workflow:
            raise ValueError("snapshot workflow does not match event-log workflow")

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def _finite_now(self, value: float | None) -> float:
        result = self._clock() if value is None else float(value)
        if not math.isfinite(result):
            raise ValueError("recorded_at_epoch must be finite")
        return result

    def _ensure_open_state(self) -> None:
        if self._closed:
            raise EventLogError("event log is closed")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EventLogFormatError(f"{name} must be an object")
    return value


def _take(
    value: Mapping[str, object],
    key: str,
    *,
    destructive: bool,
    default: object = None,
) -> object:
    if destructive and isinstance(value, dict):
        return value.pop(key, default)
    return value.get(key, default)


def _sequence(value: object, name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise EventLogFormatError(f"{name} must be an array")
    return value


def _decode_items(
    value: object,
    name: str,
    decoder: Callable[[object], object],
    *,
    destructive: bool,
) -> tuple[object, ...]:
    source = _sequence(value, name)
    if not destructive or not isinstance(source, list):
        return tuple(decoder(item) for item in source)

    result: list[object] = []
    try:
        for index in range(len(source)):
            result.append(decoder(source[index]))
            source[index] = None
    finally:
        source.clear()
    return tuple(result)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise EventLogFormatError(f"{name} must be a non-empty string")
    return value


def _optional_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise EventLogFormatError(f"{name} must be a string or null")
    return value


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EventLogFormatError(f"{name} must be an integer")
    return value


def _optional_integer(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _integer(value, name)


def _finite_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventLogFormatError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise EventLogFormatError(f"{name} must be finite")
    return result


def _payload(value: object, name: str) -> FrozenPayload:
    return FrozenPayload.from_mapping(_mapping(value, name))


def _workflow_identity(value: object) -> WorkflowIdentity:
    item = _mapping(value, "workflow identity")
    return WorkflowIdentity(
        _string(item.get("wf_uuid"), "wf_uuid"),
        _string(item.get("root_wf_uuid"), "root_wf_uuid"),
    )


def _generation(value: object) -> DatabaseGeneration:
    item = _mapping(value, "database generation")
    return DatabaseGeneration(
        _integer(item.get("generation"), "generation"),
        _integer(item.get("device"), "device"),
        _integer(item.get("inode"), "inode"),
    )


def _job_identity(value: object) -> JobAttemptIdentity:
    item = _mapping(value, "job attempt identity")
    return JobAttemptIdentity(
        _integer(item.get("job_id"), "job_id"),
        _integer(item.get("job_instance_id"), "job_instance_id"),
        _integer(item.get("job_submit_seq"), "job_submit_seq"),
    )


def _job_attempt(value: object) -> JobAttempt:
    item = _mapping(value, "job attempt")
    return JobAttempt(
        _job_identity(item.get("identity")),
        _optional_string(item.get("scheduler_id"), "scheduler_id"),
        _optional_string(item.get("site"), "site"),
        item.get("submit_time"),
        item.get("start_time"),
        item.get("end_time"),
        _optional_integer(item.get("raw_wait_status"), "raw_wait_status"),
        _optional_integer(item.get("exit_code"), "exit_code"),
        _optional_string(item.get("stdout_path"), "stdout_path"),
        _optional_string(item.get("stderr_path"), "stderr_path"),
        _optional_integer(item.get("maxrss_kb"), "maxrss_kb"),
    )


def _db_transition_identity(value: object) -> DBTransitionIdentity:
    item = _mapping(value, "job transition identity")
    return DBTransitionIdentity(
        _integer(item.get("job_instance_id"), "job_instance_id"),
        _string(item.get("state"), "state"),
        item.get("timestamp"),
        _integer(item.get("jobstate_submit_seq"), "jobstate_submit_seq"),
    )


def _job_transition(value: object) -> DBJobTransition:
    item = _mapping(value, "job transition")
    return DBJobTransition(
        _workflow_identity(item.get("workflow")),
        _string(item.get("exec_job_id"), "exec_job_id"),
        _integer(item.get("job_submit_seq"), "job_submit_seq"),
        _db_transition_identity(item.get("identity")),
        _optional_string(item.get("reason"), "reason"),
    )


def _workflow_transition_identity(value: object) -> DBWorkflowTransitionIdentity:
    item = _mapping(value, "workflow transition identity")
    return DBWorkflowTransitionIdentity(
        _integer(item.get("wf_id"), "wf_id"),
        _string(item.get("state"), "state"),
        item.get("timestamp"),
    )


def _workflow_transition(value: object) -> DBWorkflowTransition:
    item = _mapping(value, "workflow transition")
    return DBWorkflowTransition(
        _workflow_identity(item.get("workflow")),
        _workflow_transition_identity(item.get("identity")),
        _integer(item.get("restart_count"), "restart_count"),
        _optional_integer(item.get("status"), "status"),
        _optional_string(item.get("reason"), "reason"),
    )


def _workflow_snapshot(value: object) -> WorkflowSnapshot:
    item = _mapping(value, "workflow snapshot")
    transition_value = item.get("transition")
    return WorkflowSnapshot(
        _workflow_identity(item.get("workflow")),
        _integer(item.get("wf_id"), "wf_id"),
        _string(item.get("state"), "state"),
        _optional_integer(item.get("status"), "status"),
        _integer(item.get("restart_count"), "restart_count"),
        item.get("started_at"),
        item.get("ended_at"),
        None if transition_value is None else _workflow_transition(transition_value),
        Provenance(_string(item.get("provenance"), "provenance")),
        (),
    )


def _job_snapshot(value: object) -> JobSnapshot:
    item = _mapping(value, "job snapshot")
    transition_value = item.get("transition")
    current_value = item.get("current_attempt")
    return JobSnapshot(
        _workflow_identity(item.get("workflow")),
        _optional_integer(item.get("job_id"), "job_id"),
        _string(item.get("exec_job_id"), "exec_job_id"),
        _string(item.get("type_desc"), "type_desc"),
        _integer(item.get("task_count"), "task_count"),
        tuple(
            _string(entry, "transformation")
            for entry in _sequence(item.get("transformations", []), "transformations")
        ),
        tuple(
            _job_attempt(entry)
            for entry in _sequence(item.get("attempts", []), "attempts")
        ),
        None if current_value is None else _job_identity(current_value),
        _optional_string(item.get("state"), "state"),
        item.get("state_timestamp"),
        None if transition_value is None else _job_transition(transition_value),
        Provenance(_string(item.get("provenance"), "provenance")),
        (),
        _payload(item.get("scheduler", {}), "scheduler"),
    )


def _job_watermark(value: object) -> JobTransitionWatermark:
    item = _mapping(value, "job watermark")
    return JobTransitionWatermark(
        _integer(item.get("job_instance_id"), "job_instance_id"),
        _integer(
            item.get("highest_jobstate_submit_seq"), "highest_jobstate_submit_seq"
        ),
        tuple(
            _db_transition_identity(entry)
            for entry in _sequence(
                item.get("identities_at_highest_seq", []),
                "identities_at_highest_seq",
            )
        ),
    )


def _workflow_watermark(value: object) -> WorkflowTransitionWatermark:
    item = _mapping(value, "workflow watermark")
    restart_value = _mapping(item.get("restart"), "workflow restart")
    restart = WorkflowRestartIdentity(
        _workflow_identity(restart_value.get("workflow")),
        _integer(restart_value.get("wf_id"), "wf_id"),
        _integer(restart_value.get("restart_count"), "restart_count"),
    )
    return WorkflowTransitionWatermark(
        restart,
        tuple(
            _workflow_transition_identity(entry)
            for entry in _sequence(item.get("identities", []), "identities")
        ),
    )


def _database_snapshot(value: object, *, destructive: bool = False) -> DatabaseSnapshot:
    item = _mapping(value, "database snapshot")
    jobs = _decode_items(
        _take(item, "jobs", destructive=destructive, default=[]),
        "jobs",
        _job_snapshot,
        destructive=destructive,
    )
    recent_transitions = _decode_items(
        _take(item, "recent_transitions", destructive=destructive, default=[]),
        "recent_transitions",
        _job_transition,
        destructive=destructive,
    )
    recent_workflow_transitions = _decode_items(
        _take(
            item,
            "recent_workflow_transitions",
            destructive=destructive,
            default=[],
        ),
        "recent_workflow_transitions",
        _workflow_transition,
        destructive=destructive,
    )
    watermarks = _decode_items(
        _take(item, "watermarks", destructive=destructive, default=[]),
        "watermarks",
        _job_watermark,
        destructive=destructive,
    )
    return DatabaseSnapshot(
        SnapshotEpoch(_integer(item.get("epoch"), "epoch")),
        _generation(item.get("generation")),
        _finite_float(item.get("snapshot_at_epoch"), "snapshot_at_epoch"),
        _workflow_snapshot(item.get("workflow")),
        jobs,
        recent_transitions,
        recent_workflow_transitions,
        watermarks,
        _workflow_watermark(item.get("workflow_watermark")),
    )


def _diagnostic_evidence(value: object) -> DiagnosticEvidence:
    item = _mapping(value, "diagnostic evidence")
    return DiagnosticEvidence(
        SourceName(_string(item.get("source"), "source")),
        _string(item.get("code"), "code"),
        _payload(item.get("payload", {}), "payload"),
    )


def _transition_order(
    transition: DBJobTransition | DBWorkflowTransition,
) -> tuple[object, ...]:
    if isinstance(transition, DBJobTransition):
        return (
            transition.identity.timestamp,
            1,
            transition.exec_job_id,
            transition.identity.job_instance_id,
            transition.identity.jobstate_submit_seq,
            transition.identity.state,
        )
    return (
        transition.identity.timestamp,
        0,
        transition.restart_count,
        transition.identity.state,
    )


def _job_transition_identities(snapshot: DatabaseSnapshot) -> set[DBTransitionIdentity]:
    identities = {item.identity for item in snapshot.recent_transitions}
    identities.update(
        identity
        for watermark in snapshot.watermarks
        for identity in watermark.identities_at_highest_seq
    )
    identities.update(
        job.transition.identity for job in snapshot.jobs if job.transition is not None
    )
    return identities


def _workflow_transition_identities(
    snapshot: DatabaseSnapshot,
) -> set[DBWorkflowTransitionIdentity]:
    identities = {item.identity for item in snapshot.recent_workflow_transitions}
    identities.update(snapshot.workflow_watermark.identities)
    if snapshot.workflow.transition is not None:
        identities.add(snapshot.workflow.transition.identity)
    return identities


def _database_structure(snapshot: DatabaseSnapshot) -> tuple[object, ...]:
    """Fingerprint non-transition data that requires a fresh checkpoint."""

    jobs: list[object] = []
    for job in snapshot.jobs:
        jobs.append(
            (
                job.job_id,
                job.exec_job_id,
                job.type_desc,
                job.task_count,
                job.transformations,
                tuple(
                    json.dumps(attempt.to_json_dict(), sort_keys=True)
                    for attempt in job.attempts
                ),
                job.current_attempt,
            )
        )
    return (snapshot.workflow.wf_id, tuple(jobs))


def _redact_text(value: str, maximum: int) -> str:
    from Pegasus.monitor.diagnostics import redact_excerpt

    return redact_excerpt(value, maximum)


def _redact_payload(value: object, depth: int = 0) -> dict[str, object]:
    if depth > MAX_DIAGNOSTIC_PAYLOAD_DEPTH:
        return {"truncated": True}
    source = _mapping(value, "diagnostic payload")
    result: dict[str, object] = {}
    for index, (key, item) in enumerate(source.items()):
        if index >= MAX_DIAGNOSTIC_MAPPING_ENTRIES:
            result["__truncated_entries__"] = len(source) - index
            break
        text_key = str(key)
        result[text_key] = _redact_value(
            item, depth + 1, secret=_is_secret_key(text_key)
        )
    return result


def _redact_value(value: object, depth: int, *, secret: bool = False) -> object:
    if secret:
        return "<redacted>"
    if depth > MAX_DIAGNOSTIC_PAYLOAD_DEPTH:
        return "<truncated>"
    if isinstance(value, str):
        return _redact_text(value, 1024)
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_DIAGNOSTIC_MAPPING_ENTRIES:
                result["__truncated_entries__"] = len(value) - index
                break
            text_key = str(key)
            result[text_key] = _redact_value(
                item, depth + 1, secret=_is_secret_key(text_key)
            )
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        result: list[object] = []
        for index, item in enumerate(value):
            if index >= MAX_DIAGNOSTIC_SEQUENCE_ENTRIES:
                result.append("<truncated>")
                break
            result.append(_redact_value(item, depth + 1))
        return result
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _redact_text(str(value), 1024)


def _is_secret_key(value: str) -> bool:
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    parts = {part for part in re.split(r"[^a-z0-9]+", separated.lower()) if part}
    compact = "".join(parts)
    return bool(parts & _SECRET_KEY_PARTS) or compact in {
        "accesskey",
        "apikey",
        "clientsecret",
        "privatekey",
        "secretkey",
        "signingkey",
        "sshkey",
    }
