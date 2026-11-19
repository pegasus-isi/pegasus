"""Bounded, read-only observation of monitord's ``jobstate.log``.

``LiveEventTail`` deliberately owns no clock, thread, task, or scheduler.  The
coordinator arms it before the Stampede bootstrap and supplies the sole clock
sample to each synchronous :meth:`LiveEventTail.poll` call.  "Nonblocking"
means that a poll never waits for append data; ordinary synchronous regular-file
system calls still cannot be hard-time-bounded if the kernel or backing
filesystem itself hangs.
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

import errno
import os
import re
import stat
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TypeAlias

from Pegasus.monitor.models import (
    MAX_TAIL_LINE_CHARS,
    DatabaseGeneration,
    HealthState,
    SourceHealth,
    SourceName,
    TailGap,
    TailGeneration,
    TailJobEvent,
    TailPollRequest,
    TailPollResult,
    TailSourceEvent,
    TailSourceMarker,
    TailTransitionIdentity,
    TailWorkflowEvent,
    WorkflowIdentity,
)

DEFAULT_MAX_LINE_BYTES = MAX_TAIL_LINE_CHARS
DEFAULT_MAX_BUFFER_BYTES = 1024 * 1024
DEFAULT_ANCHOR_BYTES = 256

LiveEvent: TypeAlias = TailJobEvent | TailWorkflowEvent | TailSourceEvent

_INTEGER_RE = re.compile(r"^[0-9]+$")
_SIGNED_INTEGER_RE = re.compile(r"^[+-]?[0-9]+$")
_INTERNAL_RE = re.compile(
    r"^(?P<timestamp>[0-9]+) INTERNAL \*\*\* "
    r"(?P<marker>MONITORD_STARTED|MONITORD_FINISHED|"
    r"DAGMAN_STARTED|DAGMAN_FINISHED)"
    r"(?: (?P<value>[^ ]+))? \*\*\*$"
)
_STATUS_STATES = frozenset(
    {
        "JOB_SUCCESS",
        "JOB_FAILURE",
        "JOB_FAILED",
        "PRE_SCRIPT_SUCCESS",
        "PRE_SCRIPT_FAILURE",
        "PRE_SCRIPT_FAILED",
        "POST_SCRIPT_SUCCESS",
        "POST_SCRIPT_FAILURE",
        "POST_SCRIPT_FAILED",
    }
)
_JOB_MISSING_VALUE = "-"
_DAGMAN_UNSET_VALUE = "None"


class LiveEventParseError(ValueError):
    """A complete line does not satisfy the monitord producer contract."""


def _parse_timestamp(value: str) -> Decimal:
    if not _INTEGER_RE.fullmatch(value):
        raise LiveEventParseError("timestamp must use producer .0f integer format")
    try:
        timestamp = Decimal(value)
    except InvalidOperation as error:  # pragma: no cover - guarded by the regex
        raise LiveEventParseError("invalid event timestamp") from error
    if not timestamp.is_finite():
        raise LiveEventParseError("event timestamp must be finite")
    return timestamp


def _optional_integer(value: str, field: str) -> int | None:
    if value == _DAGMAN_UNSET_VALUE:
        return None
    if not _SIGNED_INTEGER_RE.fullmatch(value):
        raise LiveEventParseError(f"{field} must be an integer or None")
    return int(value)


def _required_integer(value: str, field: str) -> int:
    if not _SIGNED_INTEGER_RE.fullmatch(value):
        raise LiveEventParseError(f"{field} must be an integer")
    return int(value)


def _event_identity(
    generation: TailGeneration, start_offset: int
) -> TailTransitionIdentity:
    return TailTransitionIdentity(generation, start_offset)


def parse_live_event_line(
    line: str,
    *,
    workflow: WorkflowIdentity,
    source_generation: TailGeneration,
    start_offset: int,
    end_offset: int,
    observed_at_monotonic: float,
    base_db_generation: DatabaseGeneration | None,
) -> LiveEvent:
    """Parse one complete line while preserving its raw producer fields.

    ``line`` excludes the newline.  Offsets are byte offsets in the observed
    generation and ``end_offset`` includes the terminating newline.
    """

    if "\n" in line or "\r" in line:
        raise LiveEventParseError("line must not contain newline characters")
    if len(line) > MAX_TAIL_LINE_CHARS:
        raise LiveEventParseError("line exceeds the shared contract limit")

    internal = _INTERNAL_RE.fullmatch(line)
    if internal is not None:
        timestamp = _parse_timestamp(internal.group("timestamp"))
        marker = internal.group("marker")
        value = internal.group("value")
        common = {
            "workflow": workflow,
            "identity": _event_identity(source_generation, start_offset),
            "base_db_generation": base_db_generation,
            "end_offset": end_offset,
            "observed_at_monotonic": observed_at_monotonic,
            "event_timestamp": timestamp,
            "original_line": line,
        }
        if marker == "MONITORD_STARTED":
            if value is not None:
                raise LiveEventParseError("MONITORD_STARTED takes no value")
            return TailSourceEvent(
                **common,
                marker=TailSourceMarker.MONITORD_STARTED,
                status=None,
            )
        if value is None:
            raise LiveEventParseError(f"{marker} requires one value")
        if marker == "MONITORD_FINISHED":
            return TailSourceEvent(
                **common,
                marker=TailSourceMarker.MONITORD_FINISHED,
                status=_required_integer(value, "monitord status"),
            )
        if marker == "DAGMAN_STARTED":
            if value == _JOB_MISSING_VALUE:
                raise LiveEventParseError("DAGMan cluster cannot be -")
            return TailWorkflowEvent(
                **common,
                marker=marker,
                status=None,
                dagman_cluster=(None if value == _DAGMAN_UNSET_VALUE else value),
            )
        return TailWorkflowEvent(
            **common,
            marker=marker,
            status=_optional_integer(value, "DAGMan status"),
            dagman_cluster=None,
        )

    fields = line.split(" ")
    if len(fields) != 7 or any(not field for field in fields):
        raise LiveEventParseError("job event must contain exactly seven fields")
    timestamp_raw, exec_job_id, state, raw_value, raw_site, raw_walltime, seq = fields
    timestamp = _parse_timestamp(timestamp_raw)
    if not state:
        raise LiveEventParseError("job state must not be empty")
    if not _INTEGER_RE.fullmatch(seq):
        raise LiveEventParseError("job_submit_seq must be a non-negative integer")
    job_submit_seq = int(seq)
    if "None" in {raw_value, raw_site, raw_walltime}:
        raise LiveEventParseError("job fields use - rather than literal None")

    normalized_state = state.strip().upper()
    status: int | None = None
    scheduler_id: str | None = None
    if raw_value != _JOB_MISSING_VALUE:
        if normalized_state in _STATUS_STATES:
            status = _optional_integer(raw_value, "job status")
        else:
            scheduler_id = raw_value

    walltime_seconds = (
        None
        if raw_walltime == _JOB_MISSING_VALUE
        else _required_integer(raw_walltime, "walltime")
    )
    if walltime_seconds is not None and walltime_seconds < 0:
        raise LiveEventParseError("walltime must be non-negative")

    return TailJobEvent(
        workflow=workflow,
        identity=_event_identity(source_generation, start_offset),
        base_db_generation=base_db_generation,
        end_offset=end_offset,
        observed_at_monotonic=observed_at_monotonic,
        event_timestamp=timestamp,
        exec_job_id=exec_job_id,
        state=state,
        job_submit_seq=job_submit_seq,
        raw_value=raw_value,
        raw_site=raw_site,
        raw_walltime=raw_walltime,
        original_line=line,
        status=status,
        scheduler_id=scheduler_id,
        walltime_seconds=walltime_seconds,
    )


@dataclass(slots=True)
class _PollState:
    request: TailPollRequest
    bytes_read: int = 0
    lines_read: int = 0
    malformed_lines: int = 0
    overlong_lines: int = 0
    last_error: str | None = None


class LiveEventTail:
    """A bounded synchronous tail over one selected ``jobstate.log`` path."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
        max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES,
        anchor_bytes: int = DEFAULT_ANCHOR_BYTES,
    ) -> None:
        if max_line_bytes <= 0:
            raise ValueError("max_line_bytes must be positive")
        if max_line_bytes > MAX_TAIL_LINE_CHARS:
            raise ValueError("max_line_bytes exceeds the shared contract limit")
        if max_buffer_bytes <= max_line_bytes:
            raise ValueError("max_buffer_bytes must exceed max_line_bytes")
        if anchor_bytes <= 0:
            raise ValueError("anchor_bytes must be positive")

        self.path = Path(path)
        self.max_line_bytes = max_line_bytes
        self.max_buffer_bytes = max_buffer_bytes
        self.anchor_bytes = min(anchor_bytes, max_line_bytes)

        self._fd: int | None = None
        self._generation: TailGeneration | None = None
        self._generation_counter = 0
        self._cursor = 0
        self._buffer = bytearray()
        self._buffer_start = 0
        self._anchor_start = 0
        self._anchor = b""
        self._anchor_expected = 0
        self._anchor_fault: str | None = None
        self._discarding_overlong = False
        self._discarded_overlong_bytes = 0
        self._drain_limit: int | None = None
        self._drain_stable_eof = False
        self._missing_seen = False
        self._bound_workflow: WorkflowIdentity | None = None
        self._pending_gaps: list[TailGap] = []
        self._pending_parse_errors: list[str] = []
        self._last_success_epoch: float | None = None
        self._io_error: str | None = None
        self._poll_io_failed = False
        self._closed = False

        self._attach(initial=True)

    @property
    def generation(self) -> TailGeneration | None:
        return self._generation

    @property
    def attachment_offset(self) -> int | None:
        """The current byte cursor, mainly for deterministic integration tests."""

        return self._cursor if self._fd is not None else None

    def close(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError as error:
                self._record_io_error("close", error)
            self._fd = None
        self._closed = True

    def __enter__(self) -> LiveEventTail:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _open_read_only(self) -> tuple[int, os.stat_result]:
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NONBLOCK"):
            flags |= os.O_NONBLOCK
        fd = os.open(self.path, flags)
        try:
            return fd, os.fstat(fd)
        except OSError:
            try:
                os.close(fd)
            except OSError:
                pass
            raise

    def _record_io_error(self, operation: str, error: OSError) -> None:
        self._io_error = f"{operation}: {error.__class__.__name__}: {error}"
        self._poll_io_failed = True

    def _mark_io_success(self) -> None:
        if not self._poll_io_failed:
            self._io_error = None

    def _attach(self, *, initial: bool, seed_prefix: bool = True) -> bool:
        try:
            fd, stat_result = self._open_read_only()
        except FileNotFoundError:
            self._missing_seen = True
            self._io_error = None
            return False
        except OSError as error:
            self._record_io_error("open/fstat", error)
            return False
        if not stat.S_ISREG(stat_result.st_mode):
            try:
                os.close(fd)
            except OSError as error:
                self._record_io_error("close non-regular source", error)
            self._io_error = "jobstate.log is not a regular file"
            return False

        self._mark_io_success()
        self._generation_counter += 1
        generation = TailGeneration(
            self._generation_counter, stat_result.st_dev, stat_result.st_ino
        )
        size = stat_result.st_size
        self._fd = fd
        self._generation = generation
        self._cursor = size
        self._buffer.clear()
        self._buffer_start = size
        self._discarding_overlong = False
        self._discarded_overlong_bytes = 0
        self._drain_limit = None
        self._drain_stable_eof = False

        try:
            if not seed_prefix:
                os.lseek(fd, size, os.SEEK_SET)
            elif initial or not self._missing_seen:
                self._seed_attachment_prefix(size)
            elif size == 0:
                os.lseek(fd, 0, os.SEEK_SET)
            else:
                os.lseek(fd, size, os.SEEK_SET)
                self._pending_gaps.append(
                    TailGap(generation, "created_with_existing_content", size, 0)
                )
        except OSError as error:
            self._record_io_error("attachment boundary", error)
            self._close_generation()
            return False

        self._update_anchor()
        self._missing_seen = False
        return True

    def _seed_attachment_prefix(self, size: int) -> None:
        assert self._fd is not None
        if size == 0:
            os.lseek(self._fd, 0, os.SEEK_SET)
            return
        window = min(size, self.max_line_bytes + 1)
        start = size - window
        data = os.pread(self._fd, window, start)
        if len(data) != window:
            raise OSError(errno.EIO, "short attachment prefix read")
        if data.endswith(b"\n"):
            os.lseek(self._fd, size, os.SEEK_SET)
            return

        newline = data.rfind(b"\n")
        if newline >= 0:
            boundary = start + newline + 1
            self._buffer.extend(data[newline + 1 :])
            self._buffer_start = boundary
            os.lseek(self._fd, size, os.SEEK_SET)
            return

        if start == 0 and size <= self.max_line_bytes:
            self._buffer.extend(data)
            self._buffer_start = 0
            os.lseek(self._fd, size, os.SEEK_SET)
            return

        self._pending_parse_errors.append("overlong attachment line discarded")
        self._pending_gaps.append(
            TailGap(self._generation, "overlong_attachment_line", window, 0)
        )
        os.lseek(self._fd, size, os.SEEK_SET)

    def _update_anchor(self) -> bool:
        if self._fd is None:
            self._anchor_start = 0
            self._anchor = b""
            self._anchor_expected = 0
            self._anchor_fault = None
            return True
        length = min(self.anchor_bytes, self._cursor)
        self._anchor_start = self._cursor - length
        self._anchor_expected = length
        try:
            data = os.pread(self._fd, length, self._anchor_start)
        except OSError as error:
            self._record_io_error("anchor update", error)
            self._anchor = b""
            self._anchor_fault = "io"
            return False
        self._mark_io_success()
        if len(data) != length:
            self._anchor = b""
            self._anchor_fault = "short"
            return False
        self._anchor = data
        self._anchor_fault = None
        return True

    def _anchor_status(self) -> str:
        if self._fd is None or self._anchor_expected == 0:
            return "match"
        if self._anchor_fault == "short":
            return "short"
        try:
            data = os.pread(self._fd, self._anchor_expected, self._anchor_start)
        except OSError as error:
            self._record_io_error("anchor probe", error)
            self._anchor_fault = "io"
            return "io"
        self._mark_io_success()
        if len(data) != self._anchor_expected:
            self._anchor_fault = "short"
            return "short"
        if self._anchor_fault == "io":
            return "recovered"
        return "match" if data == self._anchor else "mismatch"

    def _close_generation(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError as error:
                self._record_io_error("close generation", error)
        self._fd = None
        self._generation = None
        self._cursor = 0
        self._buffer.clear()
        self._buffer_start = 0
        self._anchor = b""
        self._anchor_expected = 0
        self._anchor_fault = None
        self._discarding_overlong = False
        self._discarded_overlong_bytes = 0
        self._drain_limit = None
        self._drain_stable_eof = False

    def _replace_generation(
        self,
        reason: str,
        dropped_bytes: int = 0,
        *,
        seed_prefix: bool = True,
    ) -> None:
        old_generation = self._generation
        self._pending_gaps.append(
            TailGap(old_generation, reason, max(0, dropped_bytes), 0)
        )
        self._close_generation()
        self._attach(initial=False, seed_prefix=seed_prefix)

    def _check_generation(self) -> None:
        if self._fd is None:
            return
        if self._drain_limit is not None:
            self._refresh_drain_limit()
            return
        try:
            current = os.fstat(self._fd)
        except OSError as error:
            self._record_io_error("generation fstat", error)
            return
        self._mark_io_success()
        try:
            path_stat = os.stat(self.path)
        except FileNotFoundError:
            self._missing_seen = True
            self._drain_limit = current.st_size
            self._drain_stable_eof = False
            self._pending_gaps.append(TailGap(self._generation, "source_deleted", 0, 0))
            return
        except OSError as error:
            self._record_io_error("generation stat", error)
            return
        self._mark_io_success()

        if (path_stat.st_dev, path_stat.st_ino) != (
            current.st_dev,
            current.st_ino,
        ):
            self._drain_limit = current.st_size
            self._drain_stable_eof = False
            self._pending_gaps.append(
                TailGap(self._generation, "source_replaced", 0, 0)
            )
            return
        if current.st_size < self._cursor:
            self._replace_generation("source_truncated", self._cursor - current.st_size)
            return
        if current.st_size >= self._cursor:
            anchor_status = self._anchor_status()
            if anchor_status == "io":
                return
            if anchor_status in {"short", "recovered"}:
                self._replace_generation("source_anchor_probe_gap", len(self._buffer))
                return
            if anchor_status == "mismatch":
                self._replace_generation("source_regrew_after_truncate", 0)

    def _refresh_drain_limit(self) -> bool:
        if self._fd is None or self._drain_limit is None:
            return False
        try:
            current = os.fstat(self._fd)
        except OSError as error:
            self._record_io_error("replacement drain fstat", error)
            return False
        self._mark_io_success()
        if current.st_size > self._drain_limit:
            self._drain_limit = current.st_size
            self._drain_stable_eof = False
        elif self._cursor <= current.st_size < self._drain_limit:
            dropped = self._drain_limit - current.st_size
            self._pending_gaps.append(
                TailGap(
                    self._generation,
                    "replacement_source_truncated",
                    dropped,
                    0,
                )
            )
            self._drain_limit = current.st_size
            self._drain_stable_eof = False
        elif current.st_size < self._cursor:
            self._pending_gaps.append(
                TailGap(
                    self._generation,
                    "replacement_source_truncated",
                    self._cursor - current.st_size,
                    0,
                )
            )
            self._drain_limit = self._cursor
            self._drain_stable_eof = False
        return True

    def _append_read(self, state: _PollState) -> bool:
        assert self._fd is not None
        remaining = state.request.max_bytes - state.bytes_read
        if remaining <= 0:
            return False
        buffer_room_with_sentinel = self.max_buffer_bytes - len(self._buffer) + 1
        read_size = min(remaining, buffer_room_with_sentinel)
        if self._drain_limit is not None:
            read_size = min(read_size, max(0, self._drain_limit - self._cursor))
        if read_size <= 0:
            return False
        try:
            data = os.read(self._fd, read_size)
        except BlockingIOError:
            return False
        except OSError as error:
            if error.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                return False
            self._record_io_error("tail read", error)
            return False
        self._mark_io_success()
        if not data:
            return False
        if not self._buffer and not self._discarding_overlong:
            self._buffer_start = self._cursor
        self._cursor += len(data)
        state.bytes_read += len(data)
        self._buffer.extend(data)
        self._update_anchor()
        return True

    def _overflow(self) -> bool:
        assert self._fd is not None
        try:
            stat_result = os.fstat(self._fd)
        except OSError as error:
            self._record_io_error("overflow fstat", error)
            return False
        self._mark_io_success()
        dropped = len(self._buffer) + max(0, stat_result.st_size - self._cursor)
        self._replace_generation("buffer_overflow", dropped, seed_prefix=False)
        return True

    def _consume_lines(self, state: _PollState, events: list[LiveEvent]) -> None:
        while state.lines_read < state.request.max_lines:
            if self._discarding_overlong:
                newline = self._buffer.find(b"\n")
                if newline < 0:
                    self._discarded_overlong_bytes += len(self._buffer)
                    self._buffer.clear()
                    self._buffer_start = self._cursor
                    return
                self._discarded_overlong_bytes += newline + 1
                del self._buffer[: newline + 1]
                self._buffer_start += newline + 1
                self._discarding_overlong = False
                self._discarded_overlong_bytes = 0
                state.lines_read += 1
                continue

            newline = self._buffer.find(b"\n")
            if newline < 0:
                if len(self._buffer) > self.max_line_bytes:
                    self._discarding_overlong = True
                    self._discarded_overlong_bytes = len(self._buffer)
                    self._buffer.clear()
                    self._buffer_start = self._cursor
                    state.overlong_lines += 1
                    state.last_error = "overlong line discarded"
                return

            start_offset = self._buffer_start
            end_offset = start_offset + newline + 1
            raw_line = bytes(self._buffer[:newline])
            del self._buffer[: newline + 1]
            self._buffer_start = end_offset
            state.lines_read += 1
            if len(raw_line) > self.max_line_bytes:
                state.overlong_lines += 1
                state.last_error = "overlong line discarded"
                continue
            if raw_line.endswith(b"\r"):
                raw_line = raw_line[:-1]
            try:
                line = raw_line.decode("utf-8", errors="strict")
                event = parse_live_event_line(
                    line,
                    workflow=state.request.workflow,
                    source_generation=self._generation,  # type: ignore[arg-type]
                    start_offset=start_offset,
                    end_offset=end_offset,
                    observed_at_monotonic=state.request.clock.monotonic,
                    base_db_generation=state.request.base_db_generation,
                )
            except (UnicodeDecodeError, LiveEventParseError, ValueError) as error:
                state.malformed_lines += 1
                state.last_error = str(error)
                continue
            events.append(event)

    def _finish_drain_if_ready(self) -> bool:
        if self._fd is None or self._drain_limit is None:
            return False
        if not self._refresh_drain_limit():
            return False
        if self._poll_io_failed:
            return False
        if self._cursor < self._drain_limit:
            return False
        if b"\n" in self._buffer:
            return False
        if not self._drain_stable_eof:
            self._drain_stable_eof = True
            return False
        if self._buffer or self._discarding_overlong:
            dropped = len(self._buffer) + self._discarded_overlong_bytes
            self._pending_gaps.append(
                TailGap(self._generation, "torn_line_during_replacement", dropped, 0)
            )
            self._buffer.clear()
            self._discarding_overlong = False
            self._discarded_overlong_bytes = 0
        old_generation = self._generation
        self._close_generation()
        attached = self._attach(initial=False)
        if not attached:
            self._missing_seen = True
        return old_generation != self._generation

    def _health(
        self,
        state: _PollState,
        *,
        generation_changed: bool,
        gaps: tuple[TailGap, ...],
    ) -> SourceHealth:
        checked = state.request.clock.epoch
        if self._closed:
            health_state = HealthState.UNAVAILABLE
            error_code = "tail_closed"
            detail = "live event tail is closed"
        elif self._io_error is not None:
            health_state = HealthState.UNAVAILABLE
            error_code = "tail_io_error"
            detail = self._io_error[-512:]
        elif generation_changed or self._drain_limit is not None:
            if any(
                gap.reason
                in {
                    "buffer_overflow",
                    "overlong_attachment_line",
                    "replacement_source_truncated",
                    "source_anchor_probe_gap",
                }
                for gap in gaps
            ):
                health_state = HealthState.GAP
                error_code = gaps[-1].reason
                detail = "tail continuity gap detected; DB catchup is required"
            else:
                health_state = HealthState.REATTACHING
                error_code = "tail_reattaching"
                detail = "source generation changed; DB catchup is required"
        elif self._fd is None:
            health_state = HealthState.WAITING
            error_code = "tail_missing"
            detail = "jobstate.log is absent; waiting for creation"
        elif gaps:
            health_state = HealthState.GAP
            error_code = gaps[-1].reason
            detail = "tail continuity gap detected; DB catchup is required"
        elif (
            state.malformed_lines or state.overlong_lines or self._pending_parse_errors
        ):
            health_state = HealthState.DEGRADED
            error_code = "tail_parse_error"
            messages = self._pending_parse_errors + (
                [state.last_error] if state.last_error else []
            )
            detail = "; ".join(messages)[-512:]
        else:
            health_state = HealthState.HEALTHY
            error_code = None
            detail = None

        if self._fd is not None and health_state in {
            HealthState.HEALTHY,
            HealthState.DEGRADED,
            HealthState.REATTACHING,
        }:
            self._last_success_epoch = checked
        return SourceHealth(
            SourceName.LIVE_TAIL,
            health_state,
            checked,
            self._last_success_epoch,
            consecutive_failures=state.malformed_lines + state.overlong_lines,
            pending_count=int(bool(self._buffer or self._discarding_overlong)),
            error_code=error_code,
            detail=detail,
        )

    def poll(self, request: TailPollRequest) -> TailPollResult:
        """Read and parse at most the limits supplied by ``request``."""

        if self._bound_workflow is None:
            self._bound_workflow = request.workflow
        elif request.workflow != self._bound_workflow:
            raise ValueError("a live tail cannot be rebound to another workflow")

        state = _PollState(request)
        events: list[LiveEvent] = []
        generation_changed = False
        self._poll_io_failed = False

        if not self._closed:
            if self._fd is None:
                generation_changed = self._attach(initial=False)
            if self._fd is not None:
                before = self._generation
                self._check_generation()
                generation_changed = generation_changed or before != self._generation

                while self._fd is not None and self._io_error is None:
                    self._consume_lines(state, events)
                    if state.lines_read >= request.max_lines:
                        break
                    if len(self._buffer) > self.max_buffer_bytes:
                        generation_changed = self._overflow() or generation_changed
                        break
                    if not self._append_read(state):
                        break
                    if self._anchor_fault is not None:
                        before = self._generation
                        self._check_generation()
                        generation_changed = (
                            generation_changed or before != self._generation
                        )
                        if self._anchor_fault is not None or before != self._generation:
                            break
                    if len(self._buffer) > self.max_buffer_bytes:
                        generation_changed = self._overflow() or generation_changed
                        break

                if self._io_error is None and self._finish_drain_if_ready():
                    generation_changed = True
                event_generations = {
                    event.identity.source_generation for event in events
                }
                if len(event_generations) > 1:  # pragma: no cover - invariant guard
                    raise RuntimeError("one poll cannot emit multiple tail generations")
                # A replacement or overflow may arm a new descriptor after
                # emitting the last batch from the old one.  Correlate this
                # result to its events; the newly armed generation is exposed
                # by the following poll.
                result_generation = (
                    next(iter(event_generations))
                    if event_generations
                    else self._generation
                )
            else:
                result_generation = None
        else:
            result_generation = None

        gaps = tuple(self._pending_gaps)
        self._pending_gaps.clear()
        health = self._health(state, generation_changed=generation_changed, gaps=gaps)
        self._pending_parse_errors.clear()

        return TailPollResult(
            request=request,
            job_events=tuple(
                event for event in events if isinstance(event, TailJobEvent)
            ),
            workflow_events=tuple(
                event for event in events if isinstance(event, TailWorkflowEvent)
            ),
            source_events=tuple(
                event for event in events if isinstance(event, TailSourceEvent)
            ),
            gaps=gaps,
            health=health,
            generation=result_generation,
            bytes_read=state.bytes_read,
            lines_read=state.lines_read,
        )
