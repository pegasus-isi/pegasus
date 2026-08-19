"""Offline integration gates for WP7 logging, replay, server, and remote modes."""

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
import io
import os
import stat
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from conftest import ROOT, create_run

from Pegasus.monitor import cli
from Pegasus.monitor.event_log import EventLogWriter, encode_record, read_jsonl
from Pegasus.monitor.models import (
    CheckpointRecord,
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    GapRecord,
    JobTransitionRecord,
    SnapshotEpoch,
    StreamHeader,
)
from Pegasus.monitor.remote import (
    CommandResult,
    InvalidRemoteLocation,
    RemoteCursor,
    RemoteJSONLReader,
    RemoteLocation,
)
from Pegasus.monitor.replay import ReplayEngine, replay_records
from Pegasus.monitor.server import (
    ServerAlreadyRunning,
    ServerLease,
    ServerMetadata,
    ServerPaths,
    ServerRunReason,
    UnsafeServerPath,
    run_server_foreground,
)
from Pegasus.monitor.stampede import StampedeReader

if TYPE_CHECKING:
    from collections.abc import Sequence


GOLDEN = Path(__file__).parents[1] / "fixtures" / "event_log" / "schema-v1-golden.jsonl"


def _snapshot_signature(snapshot: object) -> tuple[object, ...]:
    workflow = snapshot.workflow
    jobs = tuple(
        (
            job.exec_job_id,
            job.state,
            job.current_attempt,
            job.provenance,
            (None if job.transition is None else job.transition.authoritative_sort_key),
        )
        for job in snapshot.jobs
    )
    return (
        workflow.workflow,
        workflow.state,
        workflow.status,
        workflow.restart_count,
        jobs,
    )


def _refresh_snapshot(database: Path) -> object:
    result = StampedeReader(database, ROOT).refresh(
        DBRefreshRequest(
            workflow=ROOT,
            next_epoch=SnapshotEpoch(1),
            mode=DBRefreshMode.FULL_REBOOTSTRAP,
            clock=ClockSample(epoch=1000.0, monotonic=500.0),
        )
    )
    assert result.snapshot is not None
    return result.snapshot


def test_local_jsonl_replays_to_the_same_authoritative_snapshot(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, tail = create_run(run)
    database_before = database.stat()
    tail_before = tail.stat()
    event_log = tmp_path / "workflow-events.jsonl"
    local_output = io.StringIO()
    local_errors = io.StringIO()

    local_status = cli.main(
        [
            "--once",
            "--no-condor",
            "--no-live-events",
            "--log",
            str(event_log),
            str(run),
        ],
        stdout=local_output,
        stderr=local_errors,
    )

    assert local_status == 0, local_errors.getvalue()
    assert stat.S_IMODE(event_log.stat().st_mode) == 0o600
    assert database.stat().st_mtime_ns == database_before.st_mtime_ns
    assert tail.stat().st_mtime_ns == tail_before.st_mtime_ns

    replayed = ReplayEngine(event_log, speed=0).replay()
    expected = _refresh_snapshot(database)
    assert replayed.complete is True
    assert replayed.snapshot is not None
    assert _snapshot_signature(replayed.snapshot) == _snapshot_signature(expected)

    replay_output = io.StringIO()
    replay_errors = io.StringIO()
    replay_status = cli.main(
        ["--replay", str(event_log), "--once"],
        stdout=replay_output,
        stderr=replay_errors,
    )
    assert replay_status == 0, replay_errors.getvalue()
    for expected_text in (
        "integration-diamond",
        "WORKFLOW_TERMINATED",
        "cluster_ID0000001",
        "QUEUED",
        "compute jobs 1",
        "wall time 00:03:20",
        "pending evidence 0",
        "authoritative final yes",
    ):
        assert expected_text in local_output.getvalue()
        assert expected_text in replay_output.getvalue()


def test_server_restart_remote_and_replay_cli_converge(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    create_run(run)
    event_log = tmp_path / "workflow-events.jsonl"
    server_args = [
        "--serve-foreground",
        "--no-condor",
        "--no-live-events",
        "--log",
        str(event_log),
        str(run),
    ]

    assert cli.main(server_args, stdout=io.StringIO(), stderr=io.StringIO()) == 0
    first_header = read_jsonl(event_log).records[0]
    assert isinstance(first_header, StreamHeader)
    paths = ServerPaths.from_log_path(event_log)
    assert not paths.metadata.exists()

    assert cli.main(server_args, stdout=io.StringIO(), stderr=io.StringIO()) == 0
    second_header = read_jsonl(event_log).records[0]
    assert isinstance(second_header, StreamHeader)
    assert second_header.stream_id != first_header.stream_id
    assert not paths.metadata.exists()

    content = event_log.read_bytes()

    class LocalRemoteReader:
        def __init__(self, location: str, **_options: object) -> None:
            self.reader = RemoteJSONLReader(
                location,
                chunk_bytes=128,
                header_bytes=1024,
                max_record_bytes=1024 * 1024,
                stderr_bytes=256,
                timeout=0.5,
                command_runner=_BoundedRemoteRunner(content),
            )

        def read(self, cursor: RemoteCursor):
            return self.reader.read(cursor)

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=LocalRemoteReader)
    remote_output = io.StringIO()
    remote_errors = io.StringIO()
    remote_status = cli.main(
        ["--remote", "user@submit.example:/tmp/workflow-events.jsonl", "--once"],
        runtime=runtime,
        stdout=remote_output,
        stderr=remote_errors,
    )
    replay_output = io.StringIO()
    replay_errors = io.StringIO()
    replay_status = cli.main(
        ["--replay", str(event_log), "--once"],
        runtime=runtime,
        stdout=replay_output,
        stderr=replay_errors,
    )

    assert remote_status == 0, remote_errors.getvalue()
    assert replay_status == 0, replay_errors.getvalue()
    for expected_text in (
        "integration-diamond",
        "WORKFLOW_TERMINATED",
        "cluster_ID0000001",
        "QUEUED",
        "compute jobs 1",
        "wall time 00:03:20",
        "pending evidence 0",
        "authoritative final yes",
    ):
        assert expected_text in remote_output.getvalue()
        assert expected_text in replay_output.getvalue()


def test_disk_guard_recovers_at_an_authoritative_checkpoint(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, _tail = create_run(run)
    snapshot = _refresh_snapshot(database)
    event_log = tmp_path / "workflow-events.jsonl"
    free_bytes = [2 * 1024 * 1024]
    writer = EventLogWriter(
        event_log,
        ROOT,
        "wp9b-integration",
        min_free_mb=1,
        checkpoint_interval=300,
        disk_usage=lambda _path: SimpleNamespace(free=free_bytes[0]),
    )
    try:
        writer.record_snapshot(snapshot, recorded_at_epoch=1000.0)
        free_bytes[0] = 0
        writer.record_snapshot(snapshot, recorded_at_epoch=1001.0)
        assert writer.status.paused is True
        free_bytes[0] = 2 * 1024 * 1024
        writer.record_snapshot(snapshot, recorded_at_epoch=1002.0)
    finally:
        writer.close()

    records = read_jsonl(event_log).records
    gaps = [record for record in records if isinstance(record, GapRecord)]
    checkpoints = [record for record in records if isinstance(record, CheckpointRecord)]
    replayed = ReplayEngine(event_log, speed=0).replay()

    assert len(gaps) == 1
    assert gaps[0].first_missing_sequence == 2
    assert gaps[0].last_missing_sequence == 2
    assert checkpoints[-1].reason == "recovery"
    assert replayed.complete is True
    assert replayed.awaiting_checkpoint is False
    assert replayed.snapshot is not None
    assert _snapshot_signature(replayed.snapshot) == _snapshot_signature(snapshot)


def test_gap_recovery_ignores_incremental_state_and_tolerates_torn_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    torn = b'{"schema_version":1,"record_type":"checkpoint"'
    path.write_bytes(GOLDEN.read_bytes() + torn)

    result = ReplayEngine(path, speed=0).replay()

    assert result.complete is True
    assert result.awaiting_checkpoint is False
    assert result.ignored_records == 1
    assert result.trailing_bytes == torn
    assert result.snapshot is not None
    assert result.snapshot.epoch.value == 4
    assert result.snapshot.jobs[0].state == "JOB_SUCCESS"


def test_stream_replacement_discards_old_and_precheckpoint_state(
    tmp_path: Path,
) -> None:
    records = read_jsonl(GOLDEN).records
    old_header = records[0]
    old_final = records[-1]
    precheckpoint = next(
        record for record in records if isinstance(record, JobTransitionRecord)
    )
    replacement_id = "wp9b-replacement-stream"
    replacement_header = replace(old_header, stream_id=replacement_id)
    replacement_transition = replace(
        precheckpoint,
        sequence=1,
        stream_id=replacement_id,
    )
    replacement_checkpoint = replace(
        records[1],
        sequence=2,
        stream_id=replacement_id,
    )
    path = tmp_path / "replacement.jsonl"
    path.write_bytes(
        b"".join(
            encode_record(record)
            for record in (
                old_header,
                old_final,
                replacement_header,
                replacement_transition,
                replacement_checkpoint,
            )
        )
    )

    result = ReplayEngine(path, speed=0).replay()

    assert result.complete is True
    assert result.stream_replacements == 1
    assert result.ignored_records == 1
    assert result.snapshot is not None
    assert result.snapshot.epoch.value == 1
    assert result.snapshot.jobs[0].state == "SUBMIT"


def test_server_lifecycle_uses_secure_singleton_files_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    paths = ServerPaths.from_log_path(tmp_path / "workflow-events.jsonl")

    def metadata_factory() -> ServerMetadata:
        return ServerMetadata(
            os.getpid(),
            "wp9b-integration-process",
            str(uuid.uuid4()),
            1.0,
        )

    class Lifecycle:
        def __init__(self) -> None:
            self.ready = asyncio.Event()
            self.finished = asyncio.Event()
            self.close_calls = 0

        async def run(self) -> None:
            self.ready.set()
            await self.finished.wait()

        async def close(self) -> None:
            self.close_calls += 1
            self.finished.set()

    async def exercise() -> None:
        lifecycle = Lifecycle()
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_server_foreground(
                lifecycle,
                paths,
                stop_event=stop,
                install_signal_handlers=False,
                metadata_factory=metadata_factory,
                readiness_probe=lifecycle.ready.wait,
            )
        )
        await lifecycle.ready.wait()
        for _ in range(100):
            if paths.metadata.exists():
                break
            await asyncio.sleep(0)
        assert paths.metadata.exists()
        assert stat.S_IMODE(paths.metadata.stat().st_mode) == 0o600
        assert stat.S_IMODE(paths.lock.stat().st_mode) == 0o600
        with pytest.raises(ServerAlreadyRunning):
            ServerLease.acquire(paths, metadata_factory=metadata_factory)

        stop.set()
        result = await asyncio.wait_for(task, 1.0)
        assert result.reason is ServerRunReason.STOP_REQUESTED
        assert lifecycle.close_calls == 1
        assert not paths.metadata.exists()

        released = ServerLease.acquire(paths, metadata_factory=metadata_factory)
        released.close()

    asyncio.run(exercise())

    symlink_target = tmp_path / "do-not-touch"
    symlink_target.write_text("sentinel", encoding="utf-8")
    paths.metadata.symlink_to(symlink_target)
    with pytest.raises(UnsafeServerPath):
        ServerLease.acquire(paths, metadata_factory=metadata_factory)
    assert symlink_target.read_text(encoding="utf-8") == "sentinel"


class _BoundedRemoteRunner:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[tuple[str, ...], int, int, float]] = []

    def __call__(
        self,
        argv: Sequence[str],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout: float,
    ) -> CommandResult:
        command = tuple(argv)
        self.calls.append((command, max_stdout_bytes, max_stderr_bytes, timeout))
        skip = int(
            next(part for part in command if part.startswith("skip=")).split("=", 1)[1]
        )
        count = int(
            next(part for part in command if part.startswith("count=")).split("=", 1)[1]
        )
        return CommandResult(0, self.content[skip : skip + count], b"")


def test_remote_reader_matches_replay_with_bounded_shell_free_ssh() -> None:
    content = GOLDEN.read_bytes()
    runner = _BoundedRemoteRunner(content)
    reader = RemoteJSONLReader(
        "user@submit.example:/tmp/run events'1/workflow-events.jsonl",
        chunk_bytes=128,
        header_bytes=1024,
        max_record_bytes=1024 * 1024,
        stderr_bytes=256,
        timeout=0.5,
        command_runner=runner,
    )
    cursor = RemoteCursor()
    records = []

    for _ in range(100):
        read = reader.read(cursor)
        records.extend(read.records)
        cursor = read.cursor
        if read.at_eof:
            break
    else:  # pragma: no cover - bounded loop is a regression guard
        pytest.fail("remote reader did not reach EOF within its bounded reads")

    expected = ReplayEngine(GOLDEN, speed=0).replay()
    remote = replay_records(records)
    assert expected.snapshot is not None
    assert remote.snapshot is not None
    assert remote.complete is True
    assert _snapshot_signature(remote.snapshot) == _snapshot_signature(
        expected.snapshot
    )
    assert cursor.offset == len(content)
    assert cursor.awaiting_checkpoint is False

    assert runner.calls
    for argv, stdout_limit, stderr_limit, timeout in runner.calls:
        separator = argv.index("--")
        assert argv[separator + 1] == "user@submit.example"
        assert argv[separator + 2] == "dd"
        assert stdout_limit <= 1024
        assert stderr_limit == 256
        assert timeout == 0.5
    assert any("'" in part for part in runner.calls[-1][0] if part.startswith("if="))

    with pytest.raises(InvalidRemoteLocation):
        RemoteLocation.parse("-oProxyCommand=bad:/tmp/events.jsonl")
    with pytest.raises(ValueError, match="SSH config"):
        RemoteJSONLReader("host:/tmp/events.jsonl", ssh_config="-unsafe")
