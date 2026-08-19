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

import heapq
import math
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from Pegasus.monitor.event_log import EventLogFormatError, EventRecord, decode_json_line
from Pegasus.monitor.models import (
    CheckpointRecord,
    DatabaseSnapshot,
    DBJobTransition,
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


# Match CoordinatorConfig without importing the live coordinator/source graph.
_RECENT_JOB_LIMIT = 8192
_RECENT_WORKFLOW_LIMIT = 64


def _record_type(record: EventRecord) -> str:
    """Return the canonical type name without materializing a JSON payload."""

    if isinstance(record, StreamHeader):
        return "header"
    if isinstance(record, CheckpointRecord):
        return "checkpoint"
    if isinstance(record, JobTransitionRecord):
        return "job_transition"
    if isinstance(record, WorkflowTransitionRecord):
        return "workflow_transition"
    if isinstance(record, EnrichmentRecord):
        return "enrichment"
    if isinstance(record, DiagnosticRecord):
        return "diagnostic_result"
    if isinstance(record, GapRecord):
        return "gap"
    raise ReplayStreamError(f"unsupported replay record: {type(record).__name__}")


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

    def __init__(
        self,
        *,
        recent_transition_limit: int = _RECENT_JOB_LIMIT,
        recent_workflow_transition_limit: int = _RECENT_WORKFLOW_LIMIT,
    ) -> None:
        if recent_transition_limit <= 0 or recent_workflow_transition_limit <= 0:
            raise ValueError("replay recent-transition limits must be positive")
        self.header: StreamHeader | None = None
        self.awaiting_checkpoint = True
        self.ignored_records = 0
        self.stream_replacements = 0
        self._recent_transition_limit = recent_transition_limit
        self._recent_workflow_transition_limit = recent_workflow_transition_limit
        self._last_sequence: int | None = None
        self._diagnostics: list[DiagnosticRecord] = []
        self._enrichments: list[EnrichmentRecord] = []
        self._snapshot_cache: DatabaseSnapshot | None = None
        self._indexed = False
        self._epoch = None
        self._generation = None
        self._snapshot_at_epoch = 0.0
        self._workflow: WorkflowSnapshot | None = None
        self._jobs: list[JobSnapshot] = []
        self._job_indexes: dict[str, int] = {}
        self._attempts_by_job: dict[str, dict[tuple[int, int], object]] = {}
        self._watermarks: dict[int, JobTransitionWatermark] = {}
        self._recent_jobs: dict[DBTransitionIdentity, DBJobTransition] = {}
        self._recent_job_heap: list[
            tuple[tuple[object, ...], tuple[object, ...], DBTransitionIdentity]
        ] = []
        self._recent_workflows: dict[
            DBWorkflowTransitionIdentity, DBWorkflowTransition
        ] = {}
        self._recent_workflow_heap: list[
            tuple[
                tuple[object, ...],
                tuple[object, ...],
                DBWorkflowTransitionIdentity,
            ]
        ] = []
        self._workflow_watermark: WorkflowTransitionWatermark | None = None

    @property
    def snapshot(self) -> DatabaseSnapshot | None:
        """Freeze indexed mutable state only when a consumer needs a frame."""

        if self._epoch is None:
            return None
        if self._snapshot_cache is None:
            assert self._generation is not None
            assert self._workflow is not None
            assert self._workflow_watermark is not None
            self._snapshot_cache = DatabaseSnapshot(
                self._epoch,
                self._generation,
                self._snapshot_at_epoch,
                self._workflow,
                tuple(self._jobs),
                tuple(
                    sorted(
                        self._recent_jobs.values(),
                        key=lambda item: item.recent_event_sort_key,
                    )
                ),
                tuple(
                    sorted(
                        self._recent_workflows.values(),
                        key=lambda item: item.authoritative_sort_key,
                    )
                ),
                tuple(self._watermarks[key] for key in sorted(self._watermarks)),
                self._workflow_watermark,
            )
        return self._snapshot_cache

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
            return self._epoch is not None
        if isinstance(record, CheckpointRecord):
            self._consume_checkpoint(record)
            return True
        if self.awaiting_checkpoint or self._epoch is None:
            self.ignored_records += 1
            return False

        self._expire_enrichments(record.recorded_at_epoch)
        if isinstance(record, JobTransitionRecord):
            return self._apply_job_transition(record)
        if isinstance(record, WorkflowTransitionRecord):
            return self._apply_workflow_transition(record)
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
        self._clear_snapshot_state()
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
        self._load_snapshot(record.snapshot)
        self.awaiting_checkpoint = False
        self._diagnostics = [
            item
            for item in self._diagnostics
            if item.snapshot_epoch == record.snapshot.epoch
        ]
        self._expire_enrichments(record.recorded_at_epoch)

    def prepare_for_checkpoint_decode(self) -> None:
        """Release the prior immutable graph before decoding its replacement."""

        self._clear_snapshot_state()

    def _clear_snapshot_state(self) -> None:
        self._snapshot_cache = None
        self._indexed = False
        self._epoch = None
        self._generation = None
        self._snapshot_at_epoch = 0.0
        self._workflow = None
        self._jobs.clear()
        self._job_indexes.clear()
        self._attempts_by_job.clear()
        self._watermarks.clear()
        self._recent_jobs.clear()
        self._recent_job_heap.clear()
        self._recent_workflows.clear()
        self._recent_workflow_heap.clear()
        self._workflow_watermark = None

    def _load_snapshot(self, snapshot: DatabaseSnapshot) -> None:
        self._epoch = snapshot.epoch
        self._generation = snapshot.generation
        self._snapshot_at_epoch = snapshot.snapshot_at_epoch
        self._workflow = snapshot.workflow
        self._workflow_watermark = snapshot.workflow_watermark
        self._snapshot_cache = snapshot
        self._indexed = False
        self._jobs.clear()
        self._job_indexes.clear()
        self._attempts_by_job.clear()
        self._watermarks.clear()
        self._recent_jobs.clear()
        self._recent_job_heap.clear()
        self._recent_workflows.clear()
        self._recent_workflow_heap.clear()
        if (
            len(snapshot.recent_transitions) > self._recent_transition_limit
            or len(snapshot.recent_workflow_transitions)
            > self._recent_workflow_transition_limit
        ):
            self._ensure_indexed_state()
            self._snapshot_cache = None

    def _ensure_indexed_state(self) -> None:
        if self._indexed:
            return
        snapshot = self._snapshot_cache
        if snapshot is None:
            raise ReplayStreamError("replay snapshot indexes are unavailable")
        self._jobs = list(snapshot.jobs)
        self._job_indexes = {
            job.exec_job_id: index for index, job in enumerate(snapshot.jobs)
        }
        self._attempts_by_job = {
            job.exec_job_id: {
                (
                    attempt.identity.job_instance_id,
                    attempt.identity.job_submit_seq,
                ): attempt.identity
                for attempt in job.attempts
            }
            for job in snapshot.jobs
        }
        self._watermarks = {
            watermark.job_instance_id: watermark for watermark in snapshot.watermarks
        }
        recent_jobs = snapshot.recent_transitions[-self._recent_transition_limit :]
        self._recent_jobs = {item.identity: item for item in recent_jobs}
        self._recent_job_heap = [
            (
                item.recent_event_sort_key,
                _job_identity_order(item.identity),
                item.identity,
            )
            for item in recent_jobs
        ]
        heapq.heapify(self._recent_job_heap)
        recent_workflows = snapshot.recent_workflow_transitions[
            -self._recent_workflow_transition_limit :
        ]
        self._recent_workflows = {item.identity: item for item in recent_workflows}
        self._recent_workflow_heap = [
            (
                item.authoritative_sort_key,
                _workflow_identity_order(item.identity),
                item.identity,
            )
            for item in recent_workflows
        ]
        heapq.heapify(self._recent_workflow_heap)
        self._workflow_watermark = snapshot.workflow_watermark
        self._indexed = True

    def _reject_incremental(self) -> bool:
        self.awaiting_checkpoint = True
        self.ignored_records += 1
        return False

    def _advance_snapshot(
        self, record: JobTransitionRecord | WorkflowTransitionRecord
    ) -> None:
        snapshot_epoch = record.snapshot_epoch
        recorded_at_epoch = record.recorded_at_epoch
        assert self._epoch is not None
        self._epoch = max(self._epoch, snapshot_epoch)
        self._snapshot_at_epoch = recorded_at_epoch
        self._snapshot_cache = None
        self._diagnostics = [
            item for item in self._diagnostics if item.snapshot_epoch == self._epoch
        ]

    def _remember_job_transition(self, transition: DBJobTransition) -> None:
        if transition.identity in self._recent_jobs:
            return
        self._recent_jobs[transition.identity] = transition
        heapq.heappush(
            self._recent_job_heap,
            (
                transition.recent_event_sort_key,
                _job_identity_order(transition.identity),
                transition.identity,
            ),
        )
        while len(self._recent_jobs) > self._recent_transition_limit:
            _key, _identity_key, identity = heapq.heappop(self._recent_job_heap)
            self._recent_jobs.pop(identity, None)

    def _remember_workflow_transition(self, transition: DBWorkflowTransition) -> None:
        if transition.identity in self._recent_workflows:
            return
        self._recent_workflows[transition.identity] = transition
        heapq.heappush(
            self._recent_workflow_heap,
            (
                transition.authoritative_sort_key,
                _workflow_identity_order(transition.identity),
                transition.identity,
            ),
        )
        while len(self._recent_workflows) > self._recent_workflow_transition_limit:
            _key, _identity_key, identity = heapq.heappop(self._recent_workflow_heap)
            self._recent_workflows.pop(identity, None)

    def _apply_job_transition(self, record: JobTransitionRecord) -> bool:
        self._ensure_indexed_state()
        transition = record.transition
        if self._workflow is None or transition.workflow != self._workflow.workflow:
            return self._reject_incremental()
        matching_index = self._job_indexes.get(transition.exec_job_id)
        if matching_index is None:
            return self._reject_incremental()
        matching_job = self._jobs[matching_index]
        matching_attempt = self._attempts_by_job.get(transition.exec_job_id, {}).get(
            (
                transition.identity.job_instance_id,
                transition.job_submit_seq,
            )
        )
        if matching_attempt is None:
            return self._reject_incremental()

        self._remember_job_transition(transition)
        watermark = self._watermarks.get(transition.identity.job_instance_id)
        if watermark is None:
            watermark = JobTransitionWatermark(
                transition.identity.job_instance_id,
                transition.identity.jobstate_submit_seq,
                (transition.identity,),
            )
        elif (
            transition.identity.jobstate_submit_seq
            > watermark.highest_jobstate_submit_seq
        ):
            watermark = JobTransitionWatermark(
                transition.identity.job_instance_id,
                transition.identity.jobstate_submit_seq,
                (transition.identity,),
            )
        elif (
            transition.identity.jobstate_submit_seq
            == watermark.highest_jobstate_submit_seq
        ):
            identities = set(watermark.identities_at_highest_seq)
            identities.add(transition.identity)
            watermark = JobTransitionWatermark(
                watermark.job_instance_id,
                watermark.highest_jobstate_submit_seq,
                tuple(sorted(identities, key=_job_identity_order)),
            )
        self._watermarks[watermark.job_instance_id] = watermark

        current = matching_job.transition
        if matching_job.current_attempt == matching_attempt and (
            current is None
            or transition.authoritative_sort_key > current.authoritative_sort_key
        ):
            self._jobs[matching_index] = replace(
                matching_job,
                state=transition.identity.state,
                state_timestamp=transition.identity.timestamp,
                transition=transition,
            )
        self._advance_snapshot(record)
        return True

    def _apply_workflow_transition(self, record: WorkflowTransitionRecord) -> bool:
        self._ensure_indexed_state()
        transition = record.transition
        workflow = self._workflow
        if (
            workflow is None
            or transition.workflow != workflow.workflow
            or transition.identity.wf_id != workflow.wf_id
        ):
            return self._reject_incremental()
        watermark = self._workflow_watermark
        assert watermark is not None
        self._remember_workflow_transition(transition)
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
        self._workflow = workflow
        self._workflow_watermark = watermark
        self._advance_snapshot(record)
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
        accumulator = ReplayAccumulator()
        records = _iter_jsonl_records(self.path, self.max_record_bytes, stream_state)
        return _process_records(
            records,
            trailing_bytes=lambda: stream_state.trailing_bytes,
            retain_frames=self.retain_frames,
            on_frame=on_frame,
            speed=self.speed,
            sleep=self._sleep,
            accumulator=accumulator,
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


@dataclass(frozen=True, slots=True)
class _CheckpointDecodeBoundary:
    """Flush and release the prior frame before checkpoint decoding."""


_CHECKPOINT_DECODE_BOUNDARY = _CheckpointDecodeBoundary()


def _iter_jsonl_records(
    path: Path,
    max_record_bytes: int,
    state: _StreamState,
) -> Iterable[EventRecord | _CheckpointDecodeBoundary]:
    """Yield complete records with one bounded allocation per JSONL line."""

    try:
        with path.open("r", encoding="utf-8", errors="strict", newline="") as stream:
            while True:
                line = stream.readline(max_record_bytes + 1)
                if not line:
                    return
                if _utf8_size(line, maximum=max_record_bytes) > max_record_bytes:
                    raise EventLogFormatError(
                        "event-log record exceeds the configured replay byte limit"
                    )
                if not line.endswith("\n"):
                    state.trailing_bytes = line.encode("utf-8")
                    return
                if '"record_type":"checkpoint"' in line:
                    yield _CHECKPOINT_DECODE_BOUNDARY
                record = decode_json_line(line)
                line = ""
                yield record
                record = None
    except UnicodeDecodeError as error:
        raise EventLogFormatError("record is not valid UTF-8") from error


def _utf8_size(value: str, *, maximum: int) -> int:
    """Count UTF-8 bytes with bounded temporary chunks and an early cutoff."""

    total = 0
    chunk_chars = 1024 * 1024
    for offset in range(0, len(value), chunk_chars):
        total += len(value[offset : offset + chunk_chars].encode("utf-8"))
        if total > maximum:
            return total
    return total


def _process_records(
    records: Iterable[EventRecord | _CheckpointDecodeBoundary],
    *,
    trailing_bytes: Callable[[], bytes],
    retain_frames: bool,
    on_frame: Callable[[ReplayFrame], None] | None = None,
    speed: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    accumulator: ReplayAccumulator | None = None,
) -> ReplayResult:
    """Consume a possibly streaming record iterable and emit bounded frames."""

    accumulator = accumulator or ReplayAccumulator()
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
        if record is _CHECKPOINT_DECODE_BOUNDARY:
            finish_frame()
            frame_second = None
            frame_types = []
            accumulator.prepare_for_checkpoint_decode()
            continue
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
        frame_types.append(_record_type(record))
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
