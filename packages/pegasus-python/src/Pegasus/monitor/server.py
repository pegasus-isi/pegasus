"""Secure singleton lifecycle helpers for headless workflow monitoring.

The module deliberately knows nothing about coordinator, event-log, or CLI
types.  Callers inject one asynchronous lifecycle object and an argv sequence,
which keeps daemon policy separate from monitor construction.
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

import asyncio
import contextlib
import ctypes
import errno
import fcntl
import json
import math
import os
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

_METADATA_SCHEMA_VERSION = 1
_MAX_METADATA_BYTES = 64 * 1024
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_DEFAULT_STARTUP_TIMEOUT = 60.0


class ServerError(RuntimeError):
    """Base class for server lifecycle failures."""


class UnsafeServerPath(ServerError):
    """A lifecycle path could follow or replace an unsafe filesystem object."""


class ServerMetadataError(ServerError):
    """PID metadata is missing, malformed, or insecure."""


class ServerAlreadyRunning(ServerError):
    """Another process owns the server lock."""

    def __init__(self, metadata: ServerMetadata | None = None) -> None:
        self.metadata = metadata
        detail = f" (pid {metadata.pid})" if metadata is not None else ""
        super().__init__(f"a pegasus-monitor server is already running{detail}")


class ServerIdentityMismatch(ServerError):
    """PID metadata no longer identifies the process currently using the PID."""


class ServerLaunchError(ServerError):
    """The detached foreground command failed its startup handshake."""


class ServerStopTimeout(ServerError):
    """The server did not release its singleton lock before the deadline."""


class ServerLifecycle(Protocol):
    """Narrow lifecycle implemented by ``MonitorCoordinator`` or a wrapper."""

    def run(self) -> Awaitable[None]: ...

    def close(self) -> Awaitable[None]: ...


@dataclass(frozen=True, slots=True)
class ServerPaths:
    """Adjacent files used for singleton ownership and PID discovery."""

    metadata: Path
    lock: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", Path(self.metadata))
        object.__setattr__(self, "lock", Path(self.lock))
        if self.metadata == self.lock:
            raise ValueError("server metadata and lock paths must differ")

    @classmethod
    def for_submit_dir(cls, submit_dir: str | os.PathLike[str]) -> ServerPaths:
        directory = Path(submit_dir)
        return cls(
            directory / ".pegasus-monitor.pid",
            directory / ".pegasus-monitor.lock",
        )

    @classmethod
    def from_log_path(cls, log_path: str | os.PathLike[str]) -> ServerPaths:
        path = Path(log_path)
        return cls(
            path.with_name(f".{path.name}.pid"),
            path.with_name(f".{path.name}.lock"),
        )


@dataclass(frozen=True, slots=True)
class ServerMetadata:
    """Atomic, non-secret identity record for one lock owner."""

    pid: int
    process_identity: str
    instance_id: str
    started_at: float
    schema_version: int = _METADATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != _METADATA_SCHEMA_VERSION:
            raise ValueError("unsupported server metadata schema")
        if isinstance(self.pid, bool) or self.pid <= 0:
            raise ValueError("server PID must be positive")
        if not self.process_identity or len(self.process_identity) > 512:
            raise ValueError("server process identity is invalid")
        try:
            parsed = uuid.UUID(self.instance_id)
        except (ValueError, AttributeError) as error:
            raise ValueError("server instance ID must be a UUID") from error
        if str(parsed) != self.instance_id:
            raise ValueError("server instance ID must use canonical UUID syntax")
        if isinstance(self.started_at, bool) or not math.isfinite(self.started_at):
            raise ValueError("server start time must be finite")
        if self.started_at <= 0:
            raise ValueError("server start time must be positive")

    @classmethod
    def current(
        cls,
        *,
        identity_reader: Callable[[int], str | None] = lambda pid: process_identity(
            pid
        ),
        clock: Callable[[], float] = time.time,
        uuid_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> ServerMetadata:
        pid = os.getpid()
        identity = identity_reader(pid)
        if identity is None:
            raise ServerMetadataError(
                "this platform cannot establish a safe process-birth identity"
            )
        return cls(pid, identity, str(uuid_factory()), clock())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "pid": self.pid,
            "process_identity": self.process_identity,
            "instance_id": self.instance_id,
            "started_at": self.started_at,
        }

    @classmethod
    def from_dict(cls, value: object) -> ServerMetadata:
        if not isinstance(value, dict):
            raise ValueError("server metadata must be a JSON object")
        expected = {
            "schema_version",
            "pid",
            "process_identity",
            "instance_id",
            "started_at",
        }
        if set(value) != expected:
            raise ValueError("server metadata contains unknown or missing fields")
        schema_version = value["schema_version"]
        pid = value["pid"]
        process_identity_value = value["process_identity"]
        instance_id = value["instance_id"]
        started_at = value["started_at"]
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            raise ValueError("server metadata schema version must be an integer")
        if not isinstance(pid, int) or isinstance(pid, bool):
            raise ValueError("server metadata PID must be an integer")
        if not isinstance(process_identity_value, str):
            raise ValueError("server process identity must be a string")
        if not isinstance(instance_id, str):
            raise ValueError("server instance ID must be a string")
        if not isinstance(started_at, (int, float)) or isinstance(started_at, bool):
            raise ValueError("server start time must be numeric")
        return cls(
            pid,
            process_identity_value,
            instance_id,
            float(started_at),
            schema_version,
        )


class ServerRunReason(Enum):
    COMPLETED = "completed"
    STOP_REQUESTED = "stop_requested"
    SIGNAL = "signal"


@dataclass(frozen=True, slots=True)
class ServerRunResult:
    metadata: ServerMetadata
    reason: ServerRunReason
    signal_number: int | None = None


@dataclass(frozen=True, slots=True)
class ServerLaunchResult:
    metadata: ServerMetadata
    pid: int


class ServerStopStatus(Enum):
    NOT_RUNNING = "not_running"
    STALE_METADATA_REMOVED = "stale_metadata_removed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class ServerStopResult:
    status: ServerStopStatus
    metadata: ServerMetadata | None = None


def _darwin_process_identity(pid: int) -> str | None:
    """Read a microsecond-resolution process birth time without a helper."""

    class ProcBSDInfo(ctypes.Structure):
        _fields_ = [
            ("flags", ctypes.c_uint32),
            ("status", ctypes.c_uint32),
            ("xstatus", ctypes.c_uint32),
            ("pid", ctypes.c_uint32),
            ("ppid", ctypes.c_uint32),
            ("uid", ctypes.c_uint32),
            ("gid", ctypes.c_uint32),
            ("ruid", ctypes.c_uint32),
            ("rgid", ctypes.c_uint32),
            ("svuid", ctypes.c_uint32),
            ("svgid", ctypes.c_uint32),
            ("reserved", ctypes.c_uint32),
            ("comm", ctypes.c_char * 16),
            ("name", ctypes.c_char * 32),
            ("nfiles", ctypes.c_uint32),
            ("pgid", ctypes.c_uint32),
            ("pjobc", ctypes.c_uint32),
            ("tdev", ctypes.c_uint32),
            ("tpgid", ctypes.c_uint32),
            ("nice", ctypes.c_int32),
            ("start_seconds", ctypes.c_uint64),
            ("start_microseconds", ctypes.c_uint64),
        ]

    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        proc_pidinfo = library.proc_pidinfo
        proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        proc_pidinfo.restype = ctypes.c_int
        info = ProcBSDInfo()
        size = ctypes.sizeof(info)
        read = proc_pidinfo(pid, 3, 0, ctypes.byref(info), size)
    except (AttributeError, OSError):
        return None
    if read != size or info.pid != pid or info.start_seconds <= 0:
        return None
    return f"darwin:{info.start_seconds}:{info.start_microseconds}"


def _linux_process_identity(pid: int) -> str | None:
    try:
        stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    closing_paren = stat_line.rfind(")")
    if closing_paren < 0:
        return None
    fields = stat_line[closing_paren + 1 :].split()
    # The suffix starts with field 3 (state); starttime is field 22.
    if len(fields) <= 19 or not fields[19].isdigit():
        return None
    return f"linux:{boot_id.strip()}:{fields[19]}"


def process_identity(pid: int) -> str | None:
    """Return a PID-reuse-resistant kernel process identity when supported."""

    if pid <= 0:
        return None
    if sys.platform.startswith("linux"):
        return _linux_process_identity(pid)
    if sys.platform == "darwin":
        return _darwin_process_identity(pid)
    return None


def read_server_metadata(path: str | os.PathLike[str]) -> ServerMetadata | None:
    """Read secure metadata, returning ``None`` only when it does not exist."""

    metadata_path = Path(path)
    _validate_parent(metadata_path)
    try:
        descriptor = os.open(metadata_path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EMLINK}:
            raise UnsafeServerPath(
                f"refusing symlink metadata path: {metadata_path}"
            ) from error
        raise ServerMetadataError(
            f"cannot open server metadata: {metadata_path}"
        ) from error
    try:
        file_stat = os.fstat(descriptor)
        _validate_owned_regular(file_stat, metadata_path, require_secure=True)
        payload = bytearray()
        while len(payload) <= _MAX_METADATA_BYTES:
            block = os.read(
                descriptor, min(8192, _MAX_METADATA_BYTES + 1 - len(payload))
            )
            if not block:
                break
            payload.extend(block)
        if len(payload) > _MAX_METADATA_BYTES:
            raise ServerMetadataError("server metadata exceeds the size limit")
    finally:
        os.close(descriptor)
    try:
        decoded = json.loads(payload.decode("utf-8"))
        return ServerMetadata.from_dict(decoded)
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise ServerMetadataError("server metadata is malformed") from error


def _validate_parent(path: Path) -> None:
    parent = path.parent
    try:
        parent_stat = parent.stat(follow_symlinks=False)
    except FileNotFoundError as error:
        raise UnsafeServerPath(f"server directory does not exist: {parent}") from error
    if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
        raise UnsafeServerPath(f"server directory is not a real directory: {parent}")


def _validate_owned_regular(
    file_stat: os.stat_result,
    path: Path,
    *,
    require_secure: bool,
) -> None:
    if not stat.S_ISREG(file_stat.st_mode):
        raise UnsafeServerPath(f"server path is not a regular file: {path}")
    if file_stat.st_uid != os.geteuid():
        raise UnsafeServerPath(f"server path is not owned by the current user: {path}")
    if file_stat.st_nlink != 1:
        raise UnsafeServerPath(f"server path has multiple hard links: {path}")
    if require_secure and stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise ServerMetadataError(f"server metadata permissions are not 0600: {path}")


def _validate_replace_target(path: Path) -> None:
    _validate_parent(path)
    try:
        target_stat = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(target_stat.st_mode):
        raise UnsafeServerPath(f"refusing symlink server path: {path}")
    _validate_owned_regular(target_stat, path, require_secure=False)


def _write_server_metadata(path: Path, metadata: ServerMetadata) -> None:
    _validate_replace_target(path)
    payload = (
        json.dumps(metadata.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _CLOEXEC | _NOFOLLOW,
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        # Recheck for an existing unsafe leaf. Replacing a symlink is itself
        # non-following, but rejecting it makes accidental/malicious paths loud.
        _validate_replace_target(path)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _fsync_directory(directory: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(directory, os.O_RDONLY | _CLOEXEC | _DIRECTORY)
        os.fsync(descriptor)
    except OSError:
        # Some network filesystems do not permit directory fsync. The file was
        # still atomically replaced and fsynced before this durability attempt.
        pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _remove_metadata_if_current(path: Path, instance_id: str) -> bool:
    try:
        current = read_server_metadata(path)
    except ServerMetadataError:
        return False
    if current is None or current.instance_id != instance_id:
        return False
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    _fsync_directory(path.parent)
    return True


class _ServerFileLock:
    """An exclusive lock whose inode is intentionally never unlinked."""

    def __init__(self, path: Path, descriptor: int) -> None:
        self.path = path
        self._descriptor = descriptor

    @classmethod
    def acquire(cls, path: Path) -> _ServerFileLock:
        _validate_parent(path)
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            file_stat = os.fstat(descriptor)
            _validate_owned_regular(file_stat, path, require_secure=False)
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            if descriptor >= 0:
                os.close(descriptor)
            raise ServerAlreadyRunning from error
        except OSError as error:
            if descriptor >= 0:
                os.close(descriptor)
            if error.errno in {errno.ELOOP, errno.EMLINK}:
                raise UnsafeServerPath(f"refusing symlink lock path: {path}") from error
            raise
        return cls(path, descriptor)

    def close(self) -> None:
        if self._descriptor < 0:
            return
        descriptor = self._descriptor
        self._descriptor = -1
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    def __enter__(self) -> _ServerFileLock:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class ServerLease:
    """Own one singleton lock and its matching atomic metadata record."""

    def __init__(
        self,
        paths: ServerPaths,
        lock: _ServerFileLock,
        metadata: ServerMetadata,
        *,
        published: bool = True,
    ) -> None:
        self.paths = paths
        self.metadata = metadata
        self._lock = lock
        self._closed = False
        self._published = published

    @classmethod
    def acquire(
        cls,
        paths: ServerPaths,
        *,
        metadata_factory: Callable[[], ServerMetadata] = ServerMetadata.current,
        publish_metadata: bool = True,
    ) -> ServerLease:
        try:
            lock = _ServerFileLock.acquire(paths.lock)
        except ServerAlreadyRunning as error:
            try:
                metadata = read_server_metadata(paths.metadata)
            except ServerError:
                metadata = None
            raise ServerAlreadyRunning(metadata) from error
        try:
            metadata = metadata_factory()
            if publish_metadata:
                _write_server_metadata(paths.metadata, metadata)
        except BaseException:
            lock.close()
            raise
        return cls(paths, lock, metadata, published=publish_metadata)

    def publish(self) -> None:
        """Publish PID metadata after the server lifecycle has started."""

        if self._closed:
            raise ServerMetadataError("cannot publish a closed server lease")
        if self._published:
            return
        _write_server_metadata(self.paths.metadata, self.metadata)
        self._published = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._published:
                _remove_metadata_if_current(
                    self.paths.metadata, self.metadata.instance_id
                )
        finally:
            self._lock.close()

    def __enter__(self) -> ServerLease:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class _SignalState:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.signal_number: int | None = None

    def request(self, signal_number: int) -> None:
        if self.signal_number is None:
            self.signal_number = signal_number
        self.stop_event.set()


@contextmanager
def _temporary_signal_handlers(state: _SignalState, *, enabled: bool):
    if not enabled or threading.current_thread() is not threading.main_thread():
        yield
        return
    previous: dict[int, Any] = {}

    def handle(signal_number: int, _frame: object | None) -> None:
        state.request(signal_number)

    try:
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            previous[signal_number] = signal.getsignal(signal_number)
            signal.signal(signal_number, handle)
        yield
    finally:
        for signal_number, handler in previous.items():
            signal.signal(signal_number, handler)


async def run_server_foreground(
    lifecycle: ServerLifecycle,
    paths: ServerPaths,
    *,
    stop_event: asyncio.Event | None = None,
    install_signal_handlers: bool = True,
    metadata_factory: Callable[[], ServerMetadata] = ServerMetadata.current,
    readiness_probe: Callable[[], Awaitable[None]] | None = None,
) -> ServerRunResult:
    """Run one headless lifecycle until completion or a clean stop request.

    PID metadata is not published until ``lifecycle.run()`` has started.  A
    caller with a stronger bootstrap contract can provide ``readiness_probe``;
    successful completion of that awaitable becomes the publication barrier.
    """

    lease = ServerLease.acquire(
        paths,
        metadata_factory=metadata_factory,
        publish_metadata=False,
    )
    requested = stop_event or asyncio.Event()
    signal_state = _SignalState(requested)
    run_task: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[bool] | None = None
    readiness_task: asyncio.Task[None] | None = None
    close_task: asyncio.Task[None] | None = None

    async def close_lifecycle() -> None:
        nonlocal close_task
        if close_task is None:
            close_task = asyncio.create_task(
                lifecycle.close(), name="pegasus-monitor-server-close"
            )
        interrupted = False
        while not close_task.done():
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                interrupted = True
                if close_task.done():
                    break
        if interrupted:
            raise asyncio.CancelledError
        close_task.result()

    result: ServerRunResult | None = None
    primary_error: BaseException | None = None
    try:
        with _temporary_signal_handlers(
            signal_state, enabled=install_signal_handlers and stop_event is None
        ):
            run_task = asyncio.create_task(
                lifecycle.run(), name="pegasus-monitor-server"
            )
            stop_task = asyncio.create_task(
                requested.wait(), name="pegasus-monitor-server-stop"
            )
            if readiness_probe is None:
                # Give run() one event-loop turn.  A bootstrap failure before
                # its first suspension is therefore never advertised as ready.
                await asyncio.sleep(0)
            else:
                readiness_task = asyncio.create_task(
                    readiness_probe(), name="pegasus-monitor-server-readiness"
                )
                ready_done, _pending = await asyncio.wait(
                    {run_task, stop_task, readiness_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if run_task in ready_done:
                    await run_task
                    result = ServerRunResult(
                        lease.metadata,
                        ServerRunReason.COMPLETED,
                        signal_state.signal_number,
                    )
                elif stop_task in ready_done:
                    reason = (
                        ServerRunReason.SIGNAL
                        if signal_state.signal_number is not None
                        else ServerRunReason.STOP_REQUESTED
                    )
                    result = ServerRunResult(
                        lease.metadata, reason, signal_state.signal_number
                    )
                else:
                    await readiness_task

            if result is None and run_task.done():
                await run_task
                result = ServerRunResult(
                    lease.metadata,
                    ServerRunReason.COMPLETED,
                    signal_state.signal_number,
                )
            if result is None and stop_task.done():
                reason = (
                    ServerRunReason.SIGNAL
                    if signal_state.signal_number is not None
                    else ServerRunReason.STOP_REQUESTED
                )
                result = ServerRunResult(
                    lease.metadata, reason, signal_state.signal_number
                )
            if result is None:
                lease.publish()
                done, _pending = await asyncio.wait(
                    {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if run_task in done:
                    await run_task
                    reason = ServerRunReason.COMPLETED
                else:
                    reason = (
                        ServerRunReason.SIGNAL
                        if signal_state.signal_number is not None
                        else ServerRunReason.STOP_REQUESTED
                    )
                result = ServerRunResult(
                    lease.metadata, reason, signal_state.signal_number
                )
    except BaseException as error:
        primary_error = error
    finally:
        cleanup_error: BaseException | None = None
        try:
            await close_lifecycle()
        except BaseException as error:
            cleanup_error = error
        finally:
            tasks = tuple(
                task
                for task in (run_task, stop_task, readiness_task)
                if task is not None
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            try:
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                lease.close()
        if primary_error is None:
            primary_error = cleanup_error

    if primary_error is not None:
        raise primary_error
    assert result is not None
    return result


def _read_handshake_metadata(path: Path) -> ServerMetadata | None:
    try:
        return read_server_metadata(path)
    except ServerMetadataError:
        return None


def _terminate_spawned_process(
    process: subprocess.Popen[Any], *, timeout: float
) -> None:
    """Terminate the detached process group, with a direct-child fallback."""

    pid = process.pid
    group_signaled = False
    if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
        try:
            os.killpg(pid, signal.SIGTERM)
            group_signaled = True
        except OSError:
            pass
    if not group_signaled and process.poll() is None:
        with contextlib.suppress(OSError):
            process.terminate()
    try:
        process.wait(timeout=max(0.1, timeout))
    except subprocess.TimeoutExpired:
        pass
    if group_signaled:
        with contextlib.suppress(OSError):
            os.killpg(pid, signal.SIGKILL)
    elif process.poll() is None:
        with contextlib.suppress(OSError):
            process.kill()
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=max(0.1, timeout))


def _require_positive_finite(value: float, description: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{description} must be finite and positive")


def launch_server(
    command: Sequence[str | os.PathLike[str]],
    paths: ServerPaths,
    *,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    startup_timeout: float = _DEFAULT_STARTUP_TIMEOUT,
    poll_interval: float = 0.05,
    stdout: int | IO[Any] = subprocess.DEVNULL,
    stderr: int | IO[Any] = subprocess.DEVNULL,
    popen: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
    identity_reader: Callable[[int], str | None] = process_identity,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ServerLaunchResult:
    """Start one detached foreground command and await atomic PID metadata."""

    argv = tuple(os.fspath(part) for part in command)
    if not argv or any(not part or "\0" in part for part in argv):
        raise ValueError("server command must be a non-empty argv sequence")
    _require_positive_finite(startup_timeout, "server startup timeout")
    _require_positive_finite(poll_interval, "server startup poll interval")
    _validate_replace_target(paths.metadata)
    _validate_replace_target(paths.lock)
    previous = _read_handshake_metadata(paths.metadata)
    available_lock = _try_lock(paths.lock)
    if available_lock is None:
        raise ServerAlreadyRunning(previous)
    available_lock.close()
    previous_instance = previous.instance_id if previous is not None else None
    process = popen(
        argv,
        cwd=os.fspath(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        close_fds=True,
        start_new_session=True,
        shell=False,
    )
    deadline = monotonic() + startup_timeout
    try:
        while monotonic() < deadline:
            metadata = _read_handshake_metadata(paths.metadata)
            if (
                metadata is not None
                and metadata.pid == process.pid
                and metadata.instance_id != previous_instance
            ):
                if identity_reader(process.pid) != metadata.process_identity:
                    raise ServerLaunchError(
                        "server PID metadata does not match the spawned process"
                    )
                if process.poll() is not None:
                    raise ServerLaunchError(
                        "server exited during the startup handshake"
                    )
                return ServerLaunchResult(metadata, process.pid)
            return_code = process.poll()
            if return_code is not None:
                raise ServerLaunchError(
                    f"server foreground command exited with status {return_code}"
                )
            sleep(min(poll_interval, max(0.0, deadline - monotonic())))
        raise ServerLaunchError("server startup timed out before PID metadata appeared")
    except BaseException:
        _terminate_spawned_process(process, timeout=min(startup_timeout, 2.0))
        raise


def _try_lock(path: Path) -> _ServerFileLock | None:
    try:
        return _ServerFileLock.acquire(path)
    except ServerAlreadyRunning:
        return None


def stop_server(
    paths: ServerPaths,
    *,
    timeout: float = 10.0,
    poll_interval: float = 0.05,
    identity_reader: Callable[[int], str | None] = process_identity,
    signaler: Callable[[int, int], None] = os.kill,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ServerStopResult:
    """Stop the recorded lock owner without trusting a potentially reused PID."""

    _require_positive_finite(timeout, "server stop timeout")
    _require_positive_finite(poll_interval, "server stop poll interval")
    metadata = read_server_metadata(paths.metadata)
    free_lock = _try_lock(paths.lock)
    if free_lock is not None:
        try:
            if metadata is None:
                return ServerStopResult(ServerStopStatus.NOT_RUNNING)
            _remove_metadata_if_current(paths.metadata, metadata.instance_id)
            return ServerStopResult(ServerStopStatus.STALE_METADATA_REMOVED, metadata)
        finally:
            free_lock.close()
    if metadata is None:
        raise ServerMetadataError(
            "server lock is held but atomic PID metadata is unavailable"
        )

    current_identity = identity_reader(metadata.pid)
    if current_identity != metadata.process_identity:
        raise ServerIdentityMismatch(
            "refusing to signal a PID whose process-birth identity changed"
        )
    try:
        signaler(metadata.pid, signal.SIGTERM)
    except ProcessLookupError:
        # The verified process exited between identity verification and signal.
        pass

    deadline = monotonic() + timeout
    while monotonic() < deadline:
        free_lock = _try_lock(paths.lock)
        if free_lock is not None:
            try:
                _remove_metadata_if_current(paths.metadata, metadata.instance_id)
                return ServerStopResult(ServerStopStatus.STOPPED, metadata)
            finally:
                free_lock.close()
        sleep(min(poll_interval, max(0.0, deadline - monotonic())))
    raise ServerStopTimeout(
        f"server pid {metadata.pid} did not stop within {timeout:g} seconds"
    )
