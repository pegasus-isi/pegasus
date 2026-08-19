"""Secure singleton and headless server lifecycle tests."""

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
import json
import signal
import stat
import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

import pytest

import Pegasus.monitor.server as server_module
from Pegasus.monitor.server import (
    ServerAlreadyRunning,
    ServerIdentityMismatch,
    ServerLaunchError,
    ServerLease,
    ServerMetadata,
    ServerMetadataError,
    ServerPaths,
    ServerRunReason,
    ServerStopStatus,
    UnsafeServerPath,
    launch_server,
    read_server_metadata,
    run_server_foreground,
    stop_server,
)

INSTANCE_1 = "11111111-1111-4111-8111-111111111111"
INSTANCE_2 = "22222222-2222-4222-8222-222222222222"


def metadata(
    *,
    pid: int = 12001,
    identity: str = "test-process-birth-1",
    instance_id: str = INSTANCE_1,
) -> ServerMetadata:
    return ServerMetadata(pid, identity, instance_id, 1000.0)


def write_metadata(path: Path, value: ServerMetadata, mode: int = 0o600) -> None:
    path.write_text(json.dumps(value.to_dict()) + "\n", encoding="utf-8")
    path.chmod(mode)


def test_paths_are_hidden_and_adjacent(tmp_path: Path) -> None:
    submit_paths = ServerPaths.for_submit_dir(tmp_path)
    assert submit_paths.metadata == tmp_path / ".pegasus-monitor.pid"
    assert submit_paths.lock == tmp_path / ".pegasus-monitor.lock"

    log_paths = ServerPaths.from_log_path(tmp_path / "monitor.jsonl")
    assert log_paths.metadata == tmp_path / ".monitor.jsonl.pid"
    assert log_paths.lock == tmp_path / ".monitor.jsonl.lock"


def test_exclusive_lock_rejects_double_start(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    first = metadata()
    lease = ServerLease.acquire(paths, metadata_factory=lambda: first)
    try:
        with pytest.raises(ServerAlreadyRunning) as raised:
            ServerLease.acquire(
                paths,
                metadata_factory=lambda: metadata(instance_id=INSTANCE_2),
            )
        assert raised.value.metadata == first
        assert read_server_metadata(paths.metadata) == first
    finally:
        lease.close()

    assert not paths.metadata.exists()
    # The inode is retained so a successor can never lock a different file.
    assert paths.lock.exists()


def test_stale_pid_metadata_is_replaced_without_signaling(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    stale = metadata(pid=999_999)
    write_metadata(paths.metadata, stale)
    fresh = metadata(pid=12002, identity="test-process-birth-2", instance_id=INSTANCE_2)

    lease = ServerLease.acquire(paths, metadata_factory=lambda: fresh)
    try:
        assert read_server_metadata(paths.metadata) == fresh
    finally:
        lease.close()


def test_stop_removes_unlocked_stale_pid_without_a_process_probe(
    tmp_path: Path,
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    stale = metadata(pid=999_999)
    write_metadata(paths.metadata, stale)

    def unexpected_probe(_pid: int) -> str | None:
        raise AssertionError("stale unlocked metadata must not probe or signal a PID")

    result = stop_server(paths, identity_reader=unexpected_probe)
    assert result.status is ServerStopStatus.STALE_METADATA_REMOVED
    assert result.metadata == stale
    assert not paths.metadata.exists()


@pytest.mark.parametrize("leaf", ["metadata", "lock"])
def test_symlink_lifecycle_paths_are_rejected(tmp_path: Path, leaf: str) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    victim = tmp_path / "victim"
    victim.write_text("unchanged", encoding="utf-8")
    target = getattr(paths, leaf)
    target.symlink_to(victim)

    with pytest.raises(UnsafeServerPath):
        ServerLease.acquire(paths, metadata_factory=metadata)
    assert victim.read_text(encoding="utf-8") == "unchanged"


def test_pid_metadata_and_lock_are_private(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    lease = ServerLease.acquire(paths, metadata_factory=metadata)
    try:
        assert stat.S_IMODE(paths.metadata.stat().st_mode) == 0o600
        assert stat.S_IMODE(paths.lock.stat().st_mode) == 0o600
        assert not list(tmp_path.glob(".*.tmp"))
    finally:
        lease.close()


def test_insecure_metadata_is_never_used_to_signal(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    write_metadata(paths.metadata, metadata(), mode=0o644)
    called = False

    def signaler(_pid: int, _signal_number: int) -> None:
        nonlocal called
        called = True

    with pytest.raises(ServerMetadataError, match="permissions are not 0600"):
        stop_server(paths, signaler=signaler)
    assert called is False


def test_stop_signals_only_verified_lock_owner(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    active = metadata()
    lease = ServerLease.acquire(paths, metadata_factory=lambda: active)
    signals: list[tuple[int, int]] = []

    def signaler(pid: int, signal_number: int) -> None:
        signals.append((pid, signal_number))
        lease.close()

    result = stop_server(
        paths,
        identity_reader=lambda pid: (
            active.process_identity if pid == active.pid else None
        ),
        signaler=signaler,
    )
    assert result.status is ServerStopStatus.STOPPED
    assert result.metadata == active
    assert signals == [(active.pid, signal.SIGTERM)]
    assert not paths.metadata.exists()


def test_stop_refuses_reused_pid(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    active = metadata()
    lease = ServerLease.acquire(paths, metadata_factory=lambda: active)
    signaled: list[int] = []
    try:
        with pytest.raises(ServerIdentityMismatch):
            stop_server(
                paths,
                identity_reader=lambda _pid: "different-process-birth",
                signaler=lambda pid, _sig: signaled.append(pid),
            )
        assert signaled == []
    finally:
        lease.close()


class FakeLifecycle:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.stopped = asyncio.Event()
        self.run_started = asyncio.Event()
        self.close_calls = 0

    async def run(self) -> None:
        self.run_started.set()
        if self.fail:
            raise RuntimeError("coordinator failed")
        await self.stopped.wait()

    async def close(self) -> None:
        self.close_calls += 1
        self.stopped.set()


class SlowCloseLifecycle(FakeLifecycle):
    def __init__(self) -> None:
        super().__init__()
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()
        self.run_cancelled = asyncio.Event()

    async def run(self) -> None:
        self.run_started.set()
        try:
            await self.stopped.wait()
        except asyncio.CancelledError:
            self.run_cancelled.set()
            raise

    async def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        await self.release_close.wait()
        self.stopped.set()


class CloseRaisesLifecycle(FakeLifecycle):
    def __init__(self) -> None:
        super().__init__()
        self.run_cancelled = asyncio.Event()

    async def run(self) -> None:
        self.run_started.set()
        try:
            await self.stopped.wait()
        except asyncio.CancelledError:
            self.run_cancelled.set()
            raise

    async def close(self) -> None:
        self.close_calls += 1
        raise RuntimeError("close failed")


def test_foreground_stop_cleans_lifecycle_metadata_and_helper_tasks(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = FakeLifecycle()
        requested = asyncio.Event()
        task = asyncio.create_task(
            run_server_foreground(
                lifecycle,
                paths,
                stop_event=requested,
                metadata_factory=metadata,
            )
        )
        await lifecycle.run_started.wait()
        assert read_server_metadata(paths.metadata) == metadata()
        requested.set()
        result = await task

        assert result.reason is ServerRunReason.STOP_REQUESTED
        assert lifecycle.close_calls == 1
        assert not paths.metadata.exists()
        helper_names = {
            pending.get_name()
            for pending in asyncio.all_tasks()
            if pending is not asyncio.current_task()
        }
        assert "pegasus-monitor-server" not in helper_names
        assert "pegasus-monitor-server-stop" not in helper_names

    asyncio.run(scenario())


def test_foreground_failure_still_closes_and_releases_lock(tmp_path: Path) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = FakeLifecycle(fail=True)
        with pytest.raises(RuntimeError, match="coordinator failed"):
            await run_server_foreground(
                lifecycle,
                paths,
                install_signal_handlers=False,
                metadata_factory=metadata,
            )
        assert lifecycle.close_calls == 1
        assert not paths.metadata.exists()
        successor = ServerLease.acquire(
            paths,
            metadata_factory=lambda: metadata(instance_id=INSTANCE_2),
        )
        successor.close()

    asyncio.run(scenario())


def test_foreground_close_failure_cancels_run_and_releases_lock(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = CloseRaisesLifecycle()
        requested = asyncio.Event()
        task = asyncio.create_task(
            run_server_foreground(
                lifecycle,
                paths,
                stop_event=requested,
                metadata_factory=metadata,
            )
        )
        await lifecycle.run_started.wait()
        requested.set()
        with pytest.raises(RuntimeError, match="close failed"):
            await task
        assert lifecycle.close_calls == 1
        assert lifecycle.run_cancelled.is_set()
        assert not paths.metadata.exists()
        successor = ServerLease.acquire(
            paths,
            metadata_factory=lambda: metadata(instance_id=INSTANCE_2),
        )
        successor.close()

    asyncio.run(scenario())


def test_foreground_outer_cancellation_does_not_interrupt_slow_close(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = SlowCloseLifecycle()
        requested = asyncio.Event()
        task = asyncio.create_task(
            run_server_foreground(
                lifecycle,
                paths,
                stop_event=requested,
                metadata_factory=metadata,
            )
        )
        await lifecycle.run_started.wait()
        requested.set()
        await lifecycle.close_started.wait()

        task.cancel()
        await asyncio.sleep(0)
        assert lifecycle.close_calls == 1
        assert not task.done()
        assert paths.metadata.exists()

        lifecycle.release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not paths.metadata.exists()

    asyncio.run(scenario())


def test_foreground_readiness_probe_delays_metadata_publication(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = FakeLifecycle()
        ready = asyncio.Event()
        requested = asyncio.Event()
        task = asyncio.create_task(
            run_server_foreground(
                lifecycle,
                paths,
                stop_event=requested,
                metadata_factory=metadata,
                readiness_probe=ready.wait,
            )
        )
        await lifecycle.run_started.wait()
        await asyncio.sleep(0)
        assert not paths.metadata.exists()

        ready.set()
        for _attempt in range(10):
            if paths.metadata.exists():
                break
            await asyncio.sleep(0)
        assert read_server_metadata(paths.metadata) == metadata()

        requested.set()
        result = await task
        assert result.reason is ServerRunReason.STOP_REQUESTED

    asyncio.run(scenario())


def test_foreground_bootstrap_failure_is_never_published(tmp_path: Path) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = FakeLifecycle(fail=True)
        with pytest.raises(RuntimeError, match="coordinator failed"):
            await run_server_foreground(
                lifecycle,
                paths,
                install_signal_handlers=False,
                metadata_factory=metadata,
            )
        assert not paths.metadata.exists()

    asyncio.run(scenario())


def test_foreground_readiness_failure_is_never_published(tmp_path: Path) -> None:
    async def scenario() -> None:
        paths = ServerPaths.for_submit_dir(tmp_path)
        lifecycle = FakeLifecycle()

        async def fail_readiness() -> None:
            raise RuntimeError("bootstrap readiness failed")

        with pytest.raises(RuntimeError, match="bootstrap readiness failed"):
            await run_server_foreground(
                lifecycle,
                paths,
                install_signal_handlers=False,
                metadata_factory=metadata,
                readiness_probe=fail_readiness,
            )
        assert lifecycle.close_calls == 1
        assert not paths.metadata.exists()

    asyncio.run(scenario())


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeProcess:
    def __init__(self, pid: int = 12001, return_code: int | None = None) -> None:
        self.pid = pid
        self.return_code = return_code
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.return_code

    def terminate(self) -> None:
        self.terminated = True
        self.return_code = -signal.SIGTERM

    def kill(self) -> None:
        self.killed = True
        self.return_code = -signal.SIGKILL

    def wait(self, timeout: float | None = None) -> int:
        if self.return_code is None:
            raise subprocess.TimeoutExpired("fake", timeout)
        return self.return_code


def test_launch_uses_argv_detachment_and_atomic_metadata_handshake(
    tmp_path: Path,
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    clock = FakeClock()
    process = FakeProcess()
    popen_calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []
    launched = metadata(pid=process.pid)
    wrote_metadata = False

    def popen(argv: tuple[str, ...], **kwargs: Any) -> FakeProcess:
        popen_calls.append((argv, kwargs))
        return process

    def sleep(seconds: float) -> None:
        nonlocal wrote_metadata
        clock.sleep(seconds)
        if not wrote_metadata:
            write_metadata(paths.metadata, launched)
            wrote_metadata = True

    result = launch_server(
        ("python", "-m", "Pegasus.monitor.cli", "--serve-foreground"),
        paths,
        popen=popen,  # type: ignore[arg-type]
        identity_reader=lambda _pid: launched.process_identity,
        monotonic=clock.monotonic,
        sleep=sleep,
    )

    assert result.pid == process.pid
    assert result.metadata == launched
    argv, kwargs = popen_calls[0]
    assert argv[-1] == "--serve-foreground"
    assert kwargs["shell"] is False
    assert kwargs["start_new_session"] is True
    assert kwargs["close_fds"] is True


def test_launch_default_timeout_allows_slow_authoritative_bootstrap(
    tmp_path: Path,
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    clock = FakeClock()
    process = FakeProcess()
    launched = metadata(pid=process.pid)
    wrote_metadata = False

    def sleep(seconds: float) -> None:
        nonlocal wrote_metadata
        clock.sleep(seconds)
        if clock.now >= 30.0 and not wrote_metadata:
            write_metadata(paths.metadata, launched)
            wrote_metadata = True

    result = launch_server(
        ("pegasus-monitor", "--serve-foreground"),
        paths,
        poll_interval=5.0,
        popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
        identity_reader=lambda _pid: launched.process_identity,
        monotonic=clock.monotonic,
        sleep=sleep,
    )

    assert result.metadata == launched
    assert 30.0 <= clock.now < 60.0


def test_launch_failure_reaps_the_spawned_process(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    process = FakeProcess(return_code=7)

    with pytest.raises(ServerLaunchError, match="status 7"):
        launch_server(
            ("pegasus-monitor", "--serve-foreground"),
            paths,
            popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
        )
    assert process.poll() == 7


def test_launch_rejects_metadata_if_bootstrap_process_then_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    process = FakeProcess()
    clock = FakeClock()
    launched = metadata(pid=process.pid)

    monkeypatch.setattr(server_module.os, "killpg", lambda _pid, _signal: None)

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        write_metadata(paths.metadata, launched)
        process.return_code = 9

    with pytest.raises(ServerLaunchError, match="exited during"):
        launch_server(
            ("pegasus-monitor", "--serve-foreground"),
            paths,
            startup_timeout=0.2,
            poll_interval=0.05,
            popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
            identity_reader=lambda _pid: launched.process_identity,
            monotonic=clock.monotonic,
            sleep=sleep,
        )


def test_launch_failure_terminates_the_detached_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    process = FakeProcess(return_code=7)
    signals: list[tuple[int, int]] = []

    monkeypatch.setattr(
        server_module.os,
        "killpg",
        lambda pid, signal_number: signals.append((pid, signal_number)),
    )

    with pytest.raises(ServerLaunchError, match="status 7"):
        launch_server(
            ("pegasus-monitor", "--serve-foreground"),
            paths,
            popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
        )

    assert signals == [
        (process.pid, signal.SIGTERM),
        (process.pid, signal.SIGKILL),
    ]
    assert process.terminated is False
    assert process.killed is False


def test_launch_failure_falls_back_to_direct_child_termination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    process = FakeProcess()
    clock = FakeClock()

    def unavailable_group_signal(_pid: int, _signal_number: int) -> None:
        raise OSError("process groups unavailable")

    monkeypatch.setattr(server_module.os, "killpg", unavailable_group_signal)

    with pytest.raises(ServerLaunchError, match="startup timed out"):
        launch_server(
            ("pegasus-monitor", "--serve-foreground"),
            paths,
            startup_timeout=0.1,
            poll_interval=0.05,
            popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert process.terminated is True
    assert process.killed is False


@pytest.mark.parametrize(
    "invalid", [0.0, -1.0, float("nan"), float("inf"), float("-inf"), True]
)
@pytest.mark.parametrize("parameter", ["startup_timeout", "poll_interval"])
def test_launch_rejects_non_finite_or_non_positive_timing(
    tmp_path: Path, invalid: float, parameter: str
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    options = {parameter: invalid}
    with pytest.raises(ValueError, match="finite and positive"):
        launch_server(
            ("pegasus-monitor", "--serve-foreground"),
            paths,
            **options,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "invalid", [0.0, -1.0, float("nan"), float("inf"), float("-inf"), True]
)
@pytest.mark.parametrize("parameter", ["timeout", "poll_interval"])
def test_stop_rejects_non_finite_or_non_positive_timing(
    tmp_path: Path, invalid: float, parameter: str
) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    options = {parameter: invalid}
    with pytest.raises(ValueError, match="finite and positive"):
        stop_server(paths, **options)  # type: ignore[arg-type]


def test_launch_rejects_active_server_before_spawning(tmp_path: Path) -> None:
    paths = ServerPaths.for_submit_dir(tmp_path)
    active = metadata()
    lease = ServerLease.acquire(paths, metadata_factory=lambda: active)
    spawned = False

    def popen(*_args: Any, **_kwargs: Any) -> FakeProcess:
        nonlocal spawned
        spawned = True
        return FakeProcess()

    try:
        with pytest.raises(ServerAlreadyRunning) as raised:
            launch_server(
                ("pegasus-monitor", "--serve-foreground"),
                paths,
                popen=popen,  # type: ignore[arg-type]
            )
        assert raised.value.metadata == active
        assert spawned is False
    finally:
        lease.close()
