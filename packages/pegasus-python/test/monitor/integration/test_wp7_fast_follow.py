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
import json
import os
import stat
import threading
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from .conftest import ROOT, create_run, insert_state

from Pegasus.monitor import cli
from Pegasus.monitor.coordinator import MonitorCoordinator
from Pegasus.monitor.diagnostics import (
    DiagnosticCategory,
    DiagnosticFinding,
    DiagnosticsEngine,
)
from Pegasus.monitor.event_log import EventLogWriter, encode_record, read_jsonl
from Pegasus.monitor.models import (
    CheckpointRecord,
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    DiagnosticRecord,
    DiagnosticSeverity,
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


@pytest.mark.parametrize("mode", ["--once", "--serve", "--serve-foreground"])
@pytest.mark.parametrize(
    "filename",
    [
        "diamond-0.stampede.db",
        "diamond-0.stampede.db-wal",
        "diamond-0.stampede.db-shm",
        "diamond-0.stampede.db-journal",
        "DIAMOND-0.stampede.db-journal",
        "jobstate.log",
        "braindump.yml",
        "pegasus.properties",
        "diamond-0.dag",
    ],
)
def test_log_rejects_workflow_inputs_before_starting(
    tmp_path, monkeypatch, mode, filename
) -> None:
    run = tmp_path / "run0001"
    create_run(run)
    braindump = run / "braindump.yml"
    metadata = json.loads(braindump.read_text())
    metadata["properties"] = "pegasus.properties"
    braindump.write_text(json.dumps(metadata))
    (run / "pegasus.properties").write_text("# workflow properties\n")
    (run / "diamond-0.dag").write_text("# workflow DAG\n")
    before = {path: path.read_bytes() for path in run.iterdir()}
    monkeypatch.setattr(
        cli, "_make_coordinator", lambda *_: pytest.fail("must reject before startup")
    )
    monkeypatch.setattr(
        "Pegasus.monitor.server.launch_server",
        lambda *_args, **_kwargs: pytest.fail("must reject before spawning"),
    )
    errors = io.StringIO()

    assert (
        cli.main(
            [str(run), mode, "--no-condor", "--log", str(run / filename)],
            stdout=io.StringIO(),
            stderr=errors,
        )
        == 1
    )
    assert "overlaps workflow input" in errors.getvalue()
    assert {path: path.read_bytes() for path in run.iterdir()} == before


@pytest.mark.parametrize("alias", ["relative", "symlink_parent", "hardlink"])
def test_log_rejects_aliases_of_workflow_database(tmp_path, alias) -> None:
    run = tmp_path / "run0001"
    database, _ = create_run(run)
    if alias == "relative":
        target = Path(os.path.relpath(database, Path.cwd()))
    elif alias == "symlink_parent":
        link = tmp_path / "alias"
        link.symlink_to(run, target_is_directory=True)
        target = link / database.name
    else:
        target = tmp_path / "alias.db"
        os.link(database, target)
    before = database.read_bytes()
    errors = io.StringIO()

    assert (
        cli.main(
            [str(run), "--once", "--no-condor", "--log", str(target)],
            stdout=io.StringIO(),
            stderr=errors,
        )
        == 1
    )
    assert "overlaps workflow input" in errors.getvalue()
    assert database.read_bytes() == before


@pytest.mark.parametrize("suffix", ["pid", "lock"])
def test_server_metadata_cannot_replace_explicit_jobstate(tmp_path, suffix) -> None:
    run = tmp_path / "run0001"
    create_run(run)
    tail = run / f".workflow-events.jsonl.{suffix}"
    tail.write_text("100 INTERNAL *** MONITORD_STARTED ***\n")
    before = tail.read_bytes()
    errors = io.StringIO()

    assert (
        cli.main(
            [
                str(run),
                "--serve-foreground",
                "--no-condor",
                "--jobstate-path",
                str(tail),
            ],
            stdout=io.StringIO(),
            stderr=errors,
        )
        == 1
    )
    assert "overlaps workflow input" in errors.getvalue()
    assert tail.read_bytes() == before


def test_late_diagnostic_is_visible_in_replay_once(tmp_path) -> None:
    database, _ = create_run(tmp_path / "run0001", terminated=False)
    insert_state(database, 100, "JOB_HELD", 125, 2)
    initial = _refresh_snapshot(database)
    path = tmp_path / "events.jsonl"
    finding = DiagnosticFinding(
        "cluster_ID0000001",
        DiagnosticCategory.HELD,
        DiagnosticSeverity.WARNING,
        "held_unknown",
        "Job is held by HTCondor",
        (),
        "db_confirmed",
        (),
    )
    writer = EventLogWriter(path, ROOT, "6.0", min_free_mb=0)
    writer.record_snapshot(initial, recorded_at_epoch=1000)
    refreshed = replace(initial, epoch=SnapshotEpoch(2), snapshot_at_epoch=1002)
    writer.record_snapshot(refreshed, recorded_at_epoch=1002)
    writer.record_diagnostics(
        SimpleNamespace(findings=(finding,), new_findings=(finding,)),
        snapshot_epoch=refreshed.epoch,
        recorded_at_epoch=1002,
    )
    writer.close()

    replayed = ReplayEngine(path, speed=0, retain_frames=False).replay()
    assert replayed.snapshot == initial
    assert len(replayed.current_diagnostics) == 1
    output = io.StringIO()
    assert (
        cli.main(["--replay", str(path), "--once"], stdout=output, stderr=io.StringIO())
        == 0
    )
    assert finding.summary in output.getvalue()


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


def test_diagnostic_capture_recovers_while_workflow_writer_continues(
    tmp_path: Path, monkeypatch
) -> None:
    run = tmp_path / "run0001"
    database, _tail = create_run(run, terminated=False)
    disk = SimpleNamespace(free=1024**3)
    event_log = tmp_path / "workflow-events.jsonl"
    writer = EventLogWriter(
        event_log, ROOT, "integration", min_free_mb=1, disk_usage=lambda _path: disk
    )
    recorder = cli._EventRecorder(writer)
    coordinator = MonitorCoordinator(ROOT, StampedeReader(database, ROOT))
    diagnostics = DiagnosticsEngine()
    checkpoint_started = threading.Event()
    finish_checkpoint = threading.Event()
    write_records = writer._write_records

    def delayed_recovery(fd, records):
        if any(
            isinstance(record, CheckpointRecord) and record.reason == "recovery"
            for record in records
        ):
            checkpoint_started.set()
            assert finish_checkpoint.wait(5)
        write_records(fd, records)

    monkeypatch.setattr(writer, "_write_records", delayed_recovery)

    async def capture():
        publication = coordinator.latest
        analysis = SimpleNamespace(
            diagnostics=diagnostics.analyze(publication, submit_dir=run)
        )
        await recorder.capture(coordinator, publication, analysis=analysis)

    async def scenario():
        try:
            await coordinator.bootstrap()
            await capture()
            disk.free = 0
            insert_state(database, 100, "JOB_HELD", 120, 2)
            await coordinator.refresh_database_once()
            await capture()
            assert writer.status.paused
            assert coordinator.latest.effective.jobs[0].state == "JOB_HELD"

            disk.free = 1024**3
            await coordinator.refresh_database_once()
            recovery = asyncio.create_task(capture())
            try:
                assert await asyncio.to_thread(checkpoint_started.wait, 2)
                # A real workflow DB commit must finish while the monitor is
                # blocked writing its recovery checkpoint on another thread.
                await asyncio.wait_for(
                    asyncio.to_thread(
                        insert_state, database, 100, "JOB_RELEASED", 130, 3
                    ),
                    timeout=2,
                )
            finally:
                finish_checkpoint.set()
                await recovery
            assert not writer.status.paused
            await coordinator.refresh_database_once()
            await capture()
            return coordinator.reconciler.database
        finally:
            finish_checkpoint.set()
            await coordinator.close()
            await recorder.close()

    latest = asyncio.run(scenario())
    assert latest.jobs[0].state == "JOB_RELEASED"
    records = read_jsonl(event_log).records
    assert any(isinstance(record, GapRecord) for record in records)
    assert any(isinstance(record, DiagnosticRecord) for record in records)
    replayed = ReplayEngine(event_log, speed=0).replay()
    assert replayed.complete
    assert _snapshot_signature(replayed.snapshot) == _snapshot_signature(latest)


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


def test_log_validation_accepts_location_without_optional_paths(tmp_path):
    from types import SimpleNamespace

    args = SimpleNamespace(
        log=str(tmp_path / "events.jsonl"), serve=False, serve_foreground=False
    )
    location = SimpleNamespace(
        submit_dir=tmp_path, root_submit_dir=tmp_path, dag_name="workflow.dag"
    )
    assert cli._validated_event_log_path(args, location) == tmp_path / "events.jsonl"


def test_foreground_reuses_validated_log_path(tmp_path, monkeypatch):
    run = tmp_path / "run0001"
    create_run(run)
    log = tmp_path / "events.jsonl"
    calls = []
    original = cli._event_log_path

    def event_log_path(*args):
        calls.append(1)
        assert len(calls) == 1, "event log path recomputed after validation"
        return original(*args)

    monkeypatch.setattr(cli, "_event_log_path", event_log_path)
    errors = io.StringIO()
    assert (
        cli.main(
            [
                str(run),
                "--serve-foreground",
                "--no-condor",
                "--no-live-events",
                "--log",
                str(log),
            ],
            stdout=io.StringIO(),
            stderr=errors,
        )
        == 0
    ), errors.getvalue()
    assert len(calls) == 1
    assert log.exists()
