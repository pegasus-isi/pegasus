"""Hardened, incremental reads of monitor JSONL streams over SSH.

This module deliberately stops at the transport boundary.  It validates the
remote location, bounds every subprocess read, detects stream replacement from
the parsed header, and returns the shared typed records used by replay.
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

import ipaddress
import math
import os
import re
import selectors
import shlex
import signal
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from Pegasus.monitor.event_log import (
    EventLogFormatError,
    EventRecord,
    decode_json_line,
)
from Pegasus.monitor.models import CheckpointRecord, GapRecord, StreamHeader

_SIMPLE_HOST_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_USER_RE = re.compile(r"[A-Za-z0-9_.][A-Za-z0-9._-]*\Z")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_DEFAULT_CHUNK_BYTES = 256 * 1024
_DEFAULT_HEADER_BYTES = 64 * 1024
_DEFAULT_MAX_RECORD_BYTES = 256 * 1024 * 1024
_DEFAULT_STDERR_BYTES = 16 * 1024
_PROCESS_TERMINATION_GRACE = 0.25


class RemoteError(RuntimeError):
    """Base class for remote stream errors."""


class InvalidRemoteLocation(ValueError):
    """Raised when a ``HOST:FILE`` location is unsafe or malformed."""


class RemoteCommandError(RemoteError):
    """Raised when SSH fails or violates a resource bound."""


class RemoteProtocolError(RemoteError):
    """Raised when complete remote JSONL input violates the stream contract."""


class _StreamChangedDuringRead(RemoteProtocolError):
    """The header probe and data read observed different stream generations."""


@dataclass(frozen=True, slots=True)
class RemoteLocation:
    """A validated SSH destination and remote file path."""

    target: str
    path: str

    @classmethod
    def parse(cls, value: str) -> RemoteLocation:
        if not isinstance(value, str) or not value:
            raise InvalidRemoteLocation("remote location must be HOST:FILE")
        if _CONTROL_RE.search(value):
            raise InvalidRemoteLocation("remote location contains a control character")

        target, path = _split_location(value)
        _validate_target(target)
        _validate_remote_path(path)
        return cls(target=target, path=path)


@dataclass(frozen=True, slots=True)
class RemoteCursor:
    """Durable byte cursor for reconnecting to a remote stream.

    ``offset`` always points to the first byte not terminated by a newline.
    Torn trailing records are therefore fetched again rather than retained in
    an unbounded local buffer.
    """

    offset: int = 0
    stream_id: str | None = None
    awaiting_checkpoint: bool = True
    last_sequence: int | None = None

    def __post_init__(self) -> None:
        if self.offset < 0:
            raise ValueError("remote cursor offset must not be negative")
        if self.stream_id is not None and not self.stream_id:
            raise ValueError("remote cursor stream_id must not be empty")
        if self.last_sequence is not None and self.last_sequence < 0:
            raise ValueError("remote cursor last_sequence must not be negative")
        if self.stream_id is None and self.last_sequence is not None:
            raise ValueError("remote cursor sequence requires a stream_id")


@dataclass(frozen=True, slots=True)
class RemoteReadResult:
    """One bounded remote read and the cursor to use for the next read."""

    records: tuple[EventRecord, ...]
    cursor: RemoteCursor
    stream_replaced: bool
    at_eof: bool
    bytes_read: int
    bytes_consumed: int
    ssh_argv: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class CommandRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout: float,
    ) -> CommandResult: ...


LineDecoder = Callable[[bytes], EventRecord]


def _split_location(value: str) -> tuple[str, str]:
    bracket = value.find("[")
    if bracket >= 0:
        closing = value.find("]", bracket + 1)
        if closing < 0 or closing + 1 >= len(value) or value[closing + 1] != ":":
            raise InvalidRemoteLocation(
                "bracketed IPv6 target must be followed by :FILE"
            )
        target = value[: closing + 1]
        path = value[closing + 2 :]
        if ":" in value[:bracket] or "[" in value[bracket + 1 : closing]:
            raise InvalidRemoteLocation("malformed bracketed IPv6 target")
        return target, path

    possible_ipv6, separator, _ = value.rpartition(":")
    if separator and ":" in possible_ipv6:
        try:
            ipaddress.IPv6Address(possible_ipv6)
        except ValueError:
            pass
        else:
            raise InvalidRemoteLocation("IPv6 SSH targets must use brackets")

    target, separator, path = value.partition(":")
    if not separator:
        raise InvalidRemoteLocation("remote location must be HOST:FILE")
    return target, path


def _validate_target(target: str) -> None:
    if not target or target.startswith("-") or _CONTROL_RE.search(target):
        raise InvalidRemoteLocation("unsafe SSH target")
    if any(character.isspace() for character in target):
        raise InvalidRemoteLocation("SSH target must not contain whitespace")
    if target.count("@") > 1:
        raise InvalidRemoteLocation("SSH target contains more than one user separator")

    if "@" in target:
        user, host = target.split("@", 1)
        if not _USER_RE.fullmatch(user) or user.startswith("-"):
            raise InvalidRemoteLocation("invalid SSH user")
    else:
        host = target

    if host.startswith("["):
        if not host.endswith("]"):
            raise InvalidRemoteLocation("malformed bracketed IPv6 target")
        try:
            ipaddress.IPv6Address(host[1:-1])
        except ValueError as error:
            raise InvalidRemoteLocation("invalid bracketed IPv6 address") from error
    elif not _SIMPLE_HOST_RE.fullmatch(host) or host.startswith("-"):
        raise InvalidRemoteLocation("invalid SSH host or alias")


def _validate_remote_path(path: str) -> None:
    if not path:
        raise InvalidRemoteLocation("remote file path must not be empty")
    if len(path) > 4096:
        raise InvalidRemoteLocation("remote file path is too long")
    if path.startswith("-"):
        raise InvalidRemoteLocation("remote file path must not look like an option")
    if _CONTROL_RE.search(path):
        raise InvalidRemoteLocation("remote file path contains a control character")


def _validate_local_option(value: str | os.PathLike[str], name: str) -> str:
    path = os.fspath(value)
    if not path or path.startswith("-") or _CONTROL_RE.search(path):
        raise ValueError(f"unsafe {name} path")
    return path


def _decode_event_line(line: bytes) -> EventRecord:
    try:
        return decode_json_line(line)
    except EventLogFormatError as error:
        raise RemoteProtocolError("invalid complete JSONL record") from error


class RemoteJSONLReader:
    """Read newline-complete JSONL records from a remote file by byte offset."""

    def __init__(
        self,
        location: str | RemoteLocation,
        *,
        ssh_config: str | os.PathLike[str] | None = None,
        ssh_identity: str | os.PathLike[str] | None = None,
        chunk_bytes: int = _DEFAULT_CHUNK_BYTES,
        header_bytes: int = _DEFAULT_HEADER_BYTES,
        max_record_bytes: int = _DEFAULT_MAX_RECORD_BYTES,
        stderr_bytes: int = _DEFAULT_STDERR_BYTES,
        timeout: float = 15.0,
        decode_line: LineDecoder | None = None,
        command_runner: CommandRunner | None = None,
        ssh_executable: str = "ssh",
    ) -> None:
        self.location = (
            location
            if isinstance(location, RemoteLocation)
            else RemoteLocation.parse(location)
        )
        if (
            chunk_bytes <= 0
            or header_bytes <= 0
            or max_record_bytes <= 0
            or stderr_bytes <= 0
        ):
            raise ValueError("remote byte limits must be positive")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("remote timeout must be positive and finite")
        if not ssh_executable or _CONTROL_RE.search(ssh_executable):
            raise ValueError("invalid SSH executable")

        self.ssh_config = (
            _validate_local_option(ssh_config, "SSH config")
            if ssh_config is not None
            else None
        )
        self.ssh_identity = (
            _validate_local_option(ssh_identity, "SSH identity")
            if ssh_identity is not None
            else None
        )
        self.chunk_bytes = chunk_bytes
        self.header_bytes = header_bytes
        self.max_record_bytes = max_record_bytes
        self.stderr_bytes = stderr_bytes
        self.timeout = timeout
        self.decode_line = decode_line or _decode_event_line
        self.command_runner = command_runner or run_bounded_command
        self.ssh_executable = ssh_executable

    def build_ssh_argv(self, offset: int, count: int) -> tuple[str, ...]:
        """Return the exact shell-free local argv used for a bounded read."""

        if offset < 0 or count <= 0:
            raise ValueError("remote offset/count must be non-negative/positive")
        argv = [
            self.ssh_executable,
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "PermitLocalCommand=no",
        ]
        if self.ssh_config is not None:
            argv.extend(("-F", self.ssh_config))
        if self.ssh_identity is not None:
            argv.extend(("-i", self.ssh_identity))
        argv.extend(
            (
                "--",
                self.location.target,
                "dd",
                f"if={shlex.quote(self.location.path)}",
                # Keep byte units portable across supported remote ``dd``
                # implementations. GNU ``iflag=skip_bytes,count_bytes`` is
                # faster, but would make otherwise valid non-GNU hosts fail.
                "bs=1",
                f"skip={offset}",
                f"count={count}",
            )
        )
        return tuple(argv)

    def read(self, cursor: RemoteCursor | None = None) -> RemoteReadResult:
        """Read one bounded chunk, leaving ``cursor`` reusable after failures."""

        current = cursor or RemoteCursor()
        try:
            return self._read_once(current)
        except _StreamChangedDuringRead:
            # A replacement can land after the header probe but before the
            # offset read. Retry the entire bounded operation once so the
            # replacement header and its data are observed atomically enough
            # for this polling transport. Repeated churn remains an error.
            try:
                return self._read_once(current)
            except _StreamChangedDuringRead as error:
                raise RemoteProtocolError(
                    "remote stream changed repeatedly during one read"
                ) from error

    def _read_once(self, current: RemoteCursor) -> RemoteReadResult:
        header_data, _ = self._read_range(0, self.header_bytes)
        header = self._decode_header(header_data)
        if header is None:
            return RemoteReadResult(
                records=(),
                cursor=current,
                stream_replaced=False,
                at_eof=len(header_data) < self.header_bytes,
                bytes_read=0,
                bytes_consumed=0,
                ssh_argv=self.build_ssh_argv(0, self.header_bytes),
            )

        stream_id = header.stream_id
        replaced = current.stream_id is not None and current.stream_id != stream_id
        if current.stream_id is None or replaced:
            read_offset = 0
            awaiting_checkpoint = True
            last_sequence = None
        else:
            read_offset = current.offset
            awaiting_checkpoint = current.awaiting_checkpoint
            last_sequence = current.last_sequence

        data, argv, at_eof = self._read_through_record_boundary(read_offset)
        newline = data.rfind(b"\n")
        if newline < 0:
            complete = b""
        else:
            complete = data[: newline + 1]

        records = self._decode_records(complete, stream_id)
        awaiting_checkpoint, last_sequence = self._advance_sequence_state(
            records,
            awaiting_checkpoint=awaiting_checkpoint,
            last_sequence=last_sequence,
        )

        consumed = len(complete)
        next_cursor = RemoteCursor(
            offset=read_offset + consumed,
            stream_id=stream_id,
            awaiting_checkpoint=awaiting_checkpoint,
            last_sequence=last_sequence,
        )
        return RemoteReadResult(
            records=records,
            cursor=next_cursor,
            stream_replaced=replaced,
            at_eof=at_eof,
            bytes_read=len(data),
            bytes_consumed=consumed,
            ssh_argv=argv,
        )

    def _read_through_record_boundary(
        self, offset: int
    ) -> tuple[bytes, tuple[str, ...], bool]:
        """Read bounded chunks until at least one complete record is available."""

        chunks: list[bytes] = []
        total = 0
        first_argv: tuple[str, ...] | None = None
        at_eof = False
        while True:
            remaining = self.max_record_bytes + 1 - total
            if remaining <= 0:
                raise RemoteProtocolError(
                    "JSONL record exceeds the configured record byte limit"
                )
            count = min(self.chunk_bytes, remaining)
            block, argv = self._read_range(offset + total, count)
            if first_argv is None:
                first_argv = argv
            chunks.append(block)
            total += len(block)
            at_eof = len(block) < count
            first_newline = block.find(b"\n")
            if first_newline >= 0:
                record_bytes = total - len(block) + first_newline
                if record_bytes > self.max_record_bytes:
                    raise RemoteProtocolError(
                        "JSONL record exceeds the configured record byte limit"
                    )
                break
            if at_eof:
                break
            if total > self.max_record_bytes:
                raise RemoteProtocolError(
                    "JSONL record exceeds the configured record byte limit"
                )
        assert first_argv is not None
        return b"".join(chunks), first_argv, at_eof

    def _read_range(self, offset: int, count: int) -> tuple[bytes, tuple[str, ...]]:
        argv = self.build_ssh_argv(offset, count)
        result = self.command_runner(
            argv,
            max_stdout_bytes=count,
            max_stderr_bytes=self.stderr_bytes,
            timeout=self.timeout,
        )
        if len(result.stdout) > count:
            raise RemoteCommandError("SSH output exceeded its configured byte limit")
        if result.returncode != 0:
            message = result.stderr.decode("utf-8", errors="replace").strip()
            detail = f": {message}" if message else ""
            raise RemoteCommandError(
                f"SSH remote read failed ({result.returncode}){detail}"
            )
        return result.stdout, argv

    def _decode_header(self, data: bytes) -> StreamHeader | None:
        newline = data.find(b"\n")
        if newline < 0:
            if len(data) == self.header_bytes:
                raise RemoteProtocolError("stream header exceeds the header byte limit")
            return None
        header = self.decode_line(data[: newline + 1])
        if not isinstance(header, StreamHeader):
            raise RemoteProtocolError(
                "remote stream does not begin with a valid v1 header"
            )
        return header

    def _decode_records(
        self, complete: bytes, stream_id: str
    ) -> tuple[EventRecord, ...]:
        records: list[EventRecord] = []
        for line in complete.splitlines(keepends=True):
            if not line:
                continue
            record = self.decode_line(line)
            if record.stream_id != stream_id:
                raise _StreamChangedDuringRead(
                    "header probe and data read observed different streams"
                )
            records.append(record)
        return tuple(records)

    @staticmethod
    def _advance_sequence_state(
        records: tuple[EventRecord, ...],
        *,
        awaiting_checkpoint: bool,
        last_sequence: int | None,
    ) -> tuple[bool, int | None]:
        """Track continuity so consumers cannot accidentally trust a hole."""

        for index, record in enumerate(records):
            if isinstance(record, StreamHeader):
                if index != 0 or record.sequence != 0 or last_sequence is not None:
                    raise RemoteProtocolError(
                        "stream header appeared outside a replacement boundary"
                    )
                last_sequence = 0
                awaiting_checkpoint = True
                continue

            if last_sequence is None:
                awaiting_checkpoint = True
            else:
                expected = last_sequence + 1
                if record.sequence < expected:
                    raise RemoteProtocolError(
                        "remote record sequences must increase monotonically"
                    )
                if record.sequence > expected:
                    gap_bridges_hole = (
                        isinstance(record, GapRecord)
                        and record.first_missing_sequence <= expected
                        and record.last_missing_sequence == record.sequence - 1
                    )
                    if not gap_bridges_hole:
                        awaiting_checkpoint = True

            last_sequence = record.sequence
            if isinstance(record, GapRecord):
                awaiting_checkpoint = True
            elif isinstance(record, CheckpointRecord):
                # A checkpoint is authoritative even when it is the first
                # record observed after an unreported transport hole.
                awaiting_checkpoint = False

        return awaiting_checkpoint, last_sequence


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - owned child groups are signalable
        return True
    return True


def _signal_process_group(
    process: subprocess.Popen[bytes], sig: signal.Signals
) -> None:
    try:
        os.killpg(process.pid, sig)
        return
    except (AttributeError, ProcessLookupError):
        pass
    except OSError:
        pass

    if process.poll() is not None:
        return
    try:
        if sig == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
    except ProcessLookupError:
        pass


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    """Terminate SSH and descendants, escalating after a short grace period."""

    _signal_process_group(process, signal.SIGTERM)
    deadline = time.monotonic() + _PROCESS_TERMINATION_GRACE
    while time.monotonic() < deadline:
        if not _process_group_exists(process.pid):
            break
        time.sleep(0.01)

    if _process_group_exists(process.pid):
        _signal_process_group(process, signal.SIGKILL)

    try:
        process.wait(timeout=_PROCESS_TERMINATION_GRACE)
    except subprocess.TimeoutExpired:  # pragma: no cover - fallback is defensive
        try:
            process.kill()
        except ProcessLookupError:
            pass
        process.wait()


def run_bounded_command(
    argv: Sequence[str],
    *,
    max_stdout_bytes: int,
    max_stderr_bytes: int,
    timeout: float,
) -> CommandResult:
    """Run a subprocess without a shell while bounding time and captured output."""

    process = subprocess.Popen(  # noqa: S603 - argv is constructed and validated above
        tuple(argv),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        start_new_session=True,
    )
    if (
        process.stdout is None or process.stderr is None
    ):  # pragma: no cover - Popen contract
        _terminate_process_group(process)
        raise RemoteCommandError("unable to capture SSH output")

    selector = selectors.DefaultSelector()
    selector.register(
        process.stdout, selectors.EVENT_READ, ("stdout", max_stdout_bytes)
    )
    selector.register(
        process.stderr, selectors.EVENT_READ, ("stderr", max_stderr_bytes)
    )
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout

    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RemoteCommandError("SSH remote read timed out")
            events = selector.select(remaining)
            if not events:
                raise RemoteCommandError("SSH remote read timed out")
            for key, _ in events:
                name, limit = key.data
                chunk = os.read(key.fd, min(65536, limit + 1 - len(buffers[name])))
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                buffers[name].extend(chunk)
                if len(buffers[name]) > limit:
                    raise RemoteCommandError(f"SSH {name} exceeded its byte limit")

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RemoteCommandError("SSH remote read timed out")
        try:
            returncode = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as error:
            raise RemoteCommandError("SSH remote read timed out") from error
    except BaseException:
        _terminate_process_group(process)
        raise
    finally:
        selector.close()

    return CommandResult(
        returncode=returncode,
        stdout=bytes(buffers["stdout"]),
        stderr=bytes(buffers["stderr"]),
    )


__all__ = [
    "CommandResult",
    "InvalidRemoteLocation",
    "RemoteCommandError",
    "RemoteCursor",
    "RemoteError",
    "RemoteJSONLReader",
    "RemoteLocation",
    "RemoteProtocolError",
    "RemoteReadResult",
    "run_bounded_command",
]
