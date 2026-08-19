"""Deterministic, source-free replay of canonical monitor JSONL streams.

Replay consumes only the typed schema-v1 records decoded by
``Pegasus.monitor.event_log``.  Checkpoints are complete authoritative state;
transition records may update only transition-derived fields.  No workflow
locator, Stampede reader, live tail, kickstart parser, HTCondor provider, or
network transport is imported or invoked here.
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

import math
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from Pegasus.monitor.event_log import EventLogFormatError, EventRecord, decode_json_line
from Pegasus.monitor.models import (
    CheckpointRecord,
    DatabaseSnapshot,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    DiagnosticRecord,
    EnrichmentRecord,
    GapRecord,
    JobSnapshot,
    JobTransitionRecord,
    JobTransitionWatermark,
    StreamHeader,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionRecord,
    WorkflowTransitionWatermark,
    normalize_workflow_state,
    state_precedence,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class ReplayError(RuntimeError):
    """Base error for a semantically invalid replay stream."""


class ReplayStreamError(ReplayError, ValueError):
    """The typed records do not form one valid canonical stream."""


@dataclass(frozen=True, slots=True)
class ReplayFrame:
    """One frame after applying all contiguous records in a wall-clock second."""

    recorded_at_epoch: float
    snapshot: DatabaseSnapshot
    diagnostics: tuple[DiagnosticRecord, ...]
    enrichments: tuple[EnrichmentRecord, ...]
    record_types: tuple[str, ...]
    awaiting_checkpoint: bool


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Final replay state and the frames suitable for presentation."""

    header: StreamHeader | None
    snapshot: DatabaseSnapshot | None
    diagnostics: tuple[DiagnosticRecord, ...]
    enrichments: tuple[EnrichmentRecord, ...]
    frames: tuple[ReplayFrame, ...]
    awaiting_checkpoint: bool
    ignored_records: int
    stream_replacements: int
    trailing_bytes: bytes = b""

    @property
    def complete(self) -> bool:
        """Whether a usable checkpoint has established complete current state."""

        return self.snapshot is not None and not self.awaiting_checkpoint

    @property
    def current_diagnostics(self) -> tuple[DiagnosticRecord, ...]:
        """Diagnostic results associated with the final snapshot epoch."""

        if self.snapshot is None:
            return ()
        return tuple(
            item
            for item in self.diagnostics
            if item.snapshot_epoch == self.snapshot.epoch
        )


class ReplayAccumulator:
    """Incrementally reconstruct DB-confirmed state from typed stream records."""

    def __init__(self) -> None:
        self.header: StreamHeader | None = None
        self.snapshot: DatabaseSnapshot | None = None
        self.awaiting_checkpoint = True
        self.ignored_records = 0
        self.stream_replacements = 0
        self._last_sequence: int | None = None
        self._diagnostics: list[DiagnosticRecord] = []
        self._enrichments: list[EnrichmentRecord] = []

    @property
    def diagnostics(self) -> tuple[DiagnosticRecord, ...]:
        return tuple(self._diagnostics)

    @property
    def enrichments(self) -> tuple[EnrichmentRecord, ...]:
        return tuple(self._enrichments)

    def consume(self, record: EventRecord) -> bool:
        """Apply one record and return whether it contributes a replay frame.

        Invalid incremental state is never partially applied.  A sequence hole,
        explicit gap, stream header, or structurally inapplicable transition
        places the accumulator in checkpoint-only recovery mode.
        """

        if isinstance(record, StreamHeader):
            self._consume_header(record)
            return False
        if self.header is None:
            raise ReplayStreamError("event stream does not begin with a header")
        if record.stream_id != self.header.stream_id:
            raise ReplayStreamError("record stream_id does not match its header")

        sequence_is_contiguous = self._sequence_is_contiguous(record)
        if not sequence_is_contiguous and not isinstance(
            record, (CheckpointRecord, GapRecord)
        ):
            self.awaiting_checkpoint = True
            self.ignored_records += 1
            self._last_sequence = record.sequence
            return False
        self._last_sequence = record.sequence

        if isinstance(record, GapRecord):
            self.awaiting_checkpoint = True
            return self.snapshot is not None
        if isinstance(record, CheckpointRecord):
            self._consume_checkpoint(record)
            return True
        if self.awaiting_checkpoint or self.snapshot is None:
            self.ignored_records += 1
            return False

        self._expire_enrichments(record.recorded_at_epoch)
        if isinstance(record, JobTransitionRecord):
            updated = _apply_job_transition(self.snapshot, record)
            return self._accept_incremental_snapshot(updated)
        if isinstance(record, WorkflowTransitionRecord):
            updated = _apply_workflow_transition(self.snapshot, record)
            return self._accept_incremental_snapshot(updated)
        if isinstance(record, DiagnosticRecord):
            self._diagnostics.append(record)
            return True
        if isinstance(record, EnrichmentRecord):
            if (
                record.expires_at_epoch is None
                or record.expires_at_epoch > record.recorded_at_epoch
            ):
                self._enrichments = [
                    item
                    for item in self._enrichments
                    if not (
                        item.source is record.source and item.target == record.target
                    )
                ]
                self._enrichments.append(record)
            return True
        raise ReplayStreamError(f"unsupported replay record: {type(record).__name__}")

    def _consume_header(self, header: StreamHeader) -> None:
        if self.header is not None:
            self.stream_replacements += 1
        self.header = header
        self.snapshot = None
        self.awaiting_checkpoint = True
        self._last_sequence = 0
        self._diagnostics.clear()
        self._enrichments.clear()

    def _sequence_is_contiguous(self, record: EventRecord) -> bool:
        if self._last_sequence is None:
            return False
        expected = self._last_sequence + 1
        if record.sequence < expected:
            raise ReplayStreamError("record sequences must increase monotonically")
        if record.sequence == expected:
            return True
        if not isinstance(record, GapRecord):
            return False
        return (
            record.first_missing_sequence <= expected
            and record.last_missing_sequence == record.sequence - 1
        )

    def _consume_checkpoint(self, record: CheckpointRecord) -> None:
        assert self.header is not None
        if record.snapshot.workflow.workflow != self.header.workflow:
            raise ReplayStreamError("checkpoint workflow does not match stream header")
        self.snapshot = record.snapshot
        self.awaiting_checkpoint = False
        self._diagnostics = [
            item
            for item in self._diagnostics
            if item.snapshot_epoch == record.snapshot.epoch
        ]
        self._expire_enrichments(record.recorded_at_epoch)

    def _accept_incremental_snapshot(self, updated: DatabaseSnapshot | None) -> bool:
        if updated is None:
            self.awaiting_checkpoint = True
            self.ignored_records += 1
            return False
        self.snapshot = updated
        self._diagnostics = [
            item for item in self._diagnostics if item.snapshot_epoch == updated.epoch
        ]
        return True

    def _expire_enrichments(self, at_epoch: float) -> None:
        self._enrichments = [
            item
            for item in self._enrichments
            if item.expires_at_epoch is None or item.expires_at_epoch > at_epoch
        ]


class ReplayEngine:
    """Load and replay a local canonical JSONL file without live source calls."""

    def __init__(
        self,
        path: Path,
        *,
        speed: float = 1.0,
        max_record_bytes: int = 256 * 1024 * 1024,
        retain_frames: bool = True,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(speed) or speed < 0:
            raise ValueError("replay speed must be finite and non-negative")
        if max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        self.path = Path(path)
        self.speed = speed
        self.max_record_bytes = max_record_bytes
        self.retain_frames = retain_frames
        self._sleep = sleep

    def replay(
        self, on_frame: Callable[[ReplayFrame], None] | None = None
    ) -> ReplayResult:
        """Replay the file, honoring original frame timing divided by ``speed``.

        A speed of zero is an explicit no-delay mode.  The first frame is
        delivered immediately; negative timestamp movement never sleeps.
        """

        stream_state = _StreamState()
        records = _iter_jsonl_records(self.path, self.max_record_bytes, stream_state)
        return _process_records(
            records,
            trailing_bytes=lambda: stream_state.trailing_bytes,
            retain_frames=self.retain_frames,
            on_frame=on_frame,
            speed=self.speed,
            sleep=self._sleep,
        )


def replay_records(
    records: Iterable[EventRecord], *, trailing_bytes: bytes = b""
) -> ReplayResult:
    """Replay typed records immediately and return deterministic final state."""

    return _process_records(
        records,
        trailing_bytes=lambda: trailing_bytes,
        retain_frames=True,
    )


@dataclass(slots=True)
class _StreamState:
    trailing_bytes: bytes = b""


def _iter_jsonl_records(
    path: Path, max_record_bytes: int, state: _StreamState
) -> Iterable[EventRecord]:
    """Yield complete records with one bounded allocation per JSONL line."""

    with path.open("rb") as stream:
        while True:
            line = stream.readline(max_record_bytes + 1)
            if not line:
                return
            if len(line) > max_record_bytes:
                raise EventLogFormatError(
                    "event-log record exceeds the configured replay byte limit"
                )
            if not line.endswith(b"\n"):
                state.trailing_bytes = line
                return
            yield decode_json_line(line)


def _process_records(
    records: Iterable[EventRecord],
    *,
    trailing_bytes: Callable[[], bytes],
    retain_frames: bool,
    on_frame: Callable[[ReplayFrame], None] | None = None,
    speed: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayResult:
    """Consume a possibly streaming record iterable and emit bounded frames."""

    accumulator = ReplayAccumulator()
    frames: list[ReplayFrame] = []
    frame_second: int | None = None
    frame_time = 0.0
    frame_types: list[str] = []
    frame_changed = False
    previous_frame_time: float | None = None

    def finish_frame() -> None:
        nonlocal frame_changed, previous_frame_time
        if frame_changed and accumulator.snapshot is not None:
            frame = ReplayFrame(
                frame_time,
                accumulator.snapshot,
                accumulator.diagnostics,
                accumulator.enrichments,
                tuple(frame_types),
                accumulator.awaiting_checkpoint,
            )
            if previous_frame_time is not None and speed > 0:
                delay = max(0.0, frame.recorded_at_epoch - previous_frame_time) / speed
                if delay:
                    sleep(delay)
            if on_frame is not None:
                on_frame(frame)
            if retain_frames:
                frames.append(frame)
            previous_frame_time = frame.recorded_at_epoch
        frame_changed = False

    for record in records:
        timestamp = _recorded_at(record)
        second = math.floor(timestamp)
        if frame_second is None:
            frame_second = second
            frame_time = timestamp
        elif second != frame_second:
            finish_frame()
            frame_second = second
            frame_time = timestamp
            frame_types = []
        record_type = str(record.to_json_dict()["record_type"])
        frame_types.append(record_type)
        frame_changed = accumulator.consume(record) or frame_changed
    finish_frame()
    if accumulator.header is None:
        raise ReplayStreamError("event stream is empty")

    return ReplayResult(
        accumulator.header,
        accumulator.snapshot,
        accumulator.diagnostics,
        accumulator.enrichments,
        tuple(frames),
        accumulator.awaiting_checkpoint,
        accumulator.ignored_records,
        accumulator.stream_replacements,
        trailing_bytes(),
    )


def _recorded_at(record: EventRecord) -> float:
    if isinstance(record, StreamHeader):
        return record.created_at_epoch
    return record.recorded_at_epoch


def _apply_job_transition(
    snapshot: DatabaseSnapshot, record: JobTransitionRecord
) -> DatabaseSnapshot | None:
    transition = record.transition
    if transition.workflow != snapshot.workflow.workflow:
        return None
    matching_index: int | None = None
    matching_job: JobSnapshot | None = None
    for index, job in enumerate(snapshot.jobs):
        if job.exec_job_id == transition.exec_job_id:
            matching_index = index
            matching_job = job
            break
    if matching_job is None or matching_index is None:
        return None
    matching_attempt = next(
        (
            attempt.identity
            for attempt in matching_job.attempts
            if attempt.identity.job_instance_id == transition.identity.job_instance_id
            and attempt.identity.job_submit_seq == transition.job_submit_seq
        ),
        None,
    )
    if matching_attempt is None:
        return None

    recent = {item.identity: item for item in snapshot.recent_transitions}
    if transition.identity in recent:
        return replace(
            snapshot,
            epoch=max(snapshot.epoch, record.snapshot_epoch),
            snapshot_at_epoch=record.recorded_at_epoch,
        )
    recent[transition.identity] = transition

    watermarks = {item.job_instance_id: item for item in snapshot.watermarks}
    watermark = watermarks.get(transition.identity.job_instance_id)
    if watermark is None:
        watermark = JobTransitionWatermark(
            transition.identity.job_instance_id,
            transition.identity.jobstate_submit_seq,
            (transition.identity,),
        )
    elif (
        transition.identity.jobstate_submit_seq > watermark.highest_jobstate_submit_seq
    ):
        watermark = JobTransitionWatermark(
            transition.identity.job_instance_id,
            transition.identity.jobstate_submit_seq,
            (transition.identity,),
        )
    elif (
        transition.identity.jobstate_submit_seq == watermark.highest_jobstate_submit_seq
    ):
        identities = set(watermark.identities_at_highest_seq)
        identities.add(transition.identity)
        watermark = JobTransitionWatermark(
            watermark.job_instance_id,
            watermark.highest_jobstate_submit_seq,
            tuple(sorted(identities, key=_job_identity_order)),
        )
    watermarks[watermark.job_instance_id] = watermark

    jobs = list(snapshot.jobs)
    current = matching_job.transition
    if matching_job.current_attempt == matching_attempt and (
        current is None
        or transition.authoritative_sort_key > current.authoritative_sort_key
    ):
        jobs[matching_index] = replace(
            matching_job,
            state=transition.identity.state,
            state_timestamp=transition.identity.timestamp,
            transition=transition,
        )

    try:
        return replace(
            snapshot,
            epoch=max(snapshot.epoch, record.snapshot_epoch),
            snapshot_at_epoch=record.recorded_at_epoch,
            jobs=tuple(jobs),
            recent_transitions=tuple(
                sorted(recent.values(), key=lambda item: item.recent_event_sort_key)
            ),
            watermarks=tuple(watermarks[key] for key in sorted(watermarks)),
        )
    except ValueError:
        return None


def _apply_workflow_transition(
    snapshot: DatabaseSnapshot, record: WorkflowTransitionRecord
) -> DatabaseSnapshot | None:
    transition = record.transition
    workflow = snapshot.workflow
    if (
        transition.workflow != workflow.workflow
        or transition.identity.wf_id != workflow.wf_id
    ):
        return None
    recent = {item.identity: item for item in snapshot.recent_workflow_transitions}
    if transition.identity in recent:
        return replace(
            snapshot,
            epoch=max(snapshot.epoch, record.snapshot_epoch),
            snapshot_at_epoch=record.recorded_at_epoch,
        )
    recent[transition.identity] = transition

    watermark = snapshot.workflow_watermark
    if transition.restart_count > workflow.restart_count:
        watermark = WorkflowTransitionWatermark(
            WorkflowRestartIdentity(
                workflow.workflow, workflow.wf_id, transition.restart_count
            ),
            (transition.identity,),
        )
        workflow = _workflow_from_transition(workflow, transition, new_restart=True)
    elif transition.restart_count == workflow.restart_count:
        identities = set(watermark.identities)
        identities.add(transition.identity)
        watermark = WorkflowTransitionWatermark(
            watermark.restart,
            tuple(sorted(identities, key=_workflow_identity_order)),
        )
        current = workflow.transition
        if (
            current is None
            or transition.authoritative_sort_key > current.authoritative_sort_key
        ):
            workflow = _workflow_from_transition(workflow, transition)

    try:
        return replace(
            snapshot,
            epoch=max(snapshot.epoch, record.snapshot_epoch),
            snapshot_at_epoch=record.recorded_at_epoch,
            workflow=workflow,
            recent_workflow_transitions=tuple(
                sorted(recent.values(), key=lambda item: item.authoritative_sort_key)
            ),
            workflow_watermark=watermark,
        )
    except ValueError:
        return None


def _workflow_from_transition(
    workflow: WorkflowSnapshot,
    transition: DBWorkflowTransition,
    *,
    new_restart: bool = False,
) -> WorkflowSnapshot:
    state = normalize_workflow_state(transition.identity.state)
    started_at = workflow.started_at
    ended_at = workflow.ended_at
    if state == "WORKFLOW_STARTED":
        started_at = transition.identity.timestamp
        if new_restart:
            ended_at = None
    elif state == "WORKFLOW_TERMINATED":
        ended_at = transition.identity.timestamp
    return replace(
        workflow,
        state=transition.identity.state,
        status=transition.status,
        restart_count=transition.restart_count,
        started_at=started_at,
        ended_at=ended_at,
        transition=transition,
    )


def _job_identity_order(identity: DBTransitionIdentity) -> tuple[object, ...]:
    return (identity.timestamp, state_precedence(identity.state), identity.state)


def _workflow_identity_order(
    identity: DBWorkflowTransitionIdentity,
) -> tuple[object, ...]:
    state = normalize_workflow_state(identity.state)
    return (0 if state == "WORKFLOW_STARTED" else 1, identity.timestamp, identity.state)
