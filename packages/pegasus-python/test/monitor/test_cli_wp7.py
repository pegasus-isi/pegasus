"""Fast-follow CLI contracts for JSONL, replay, server, and remote modes."""

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
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from Pegasus.monitor import cli, server
from Pegasus.monitor.display import (
    DisplayAnalysis,
    DisplayContext,
    DisplayOptions,
    rendering_gc_guard,
)
from Pegasus.monitor.event_log import encode_record, read_jsonl
from Pegasus.monitor.models import CheckpointRecord, SchedulerQueryKind
from Pegasus.monitor.server import ServerLaunchError, ServerLaunchResult

GOLDEN = Path(__file__).parent / "fixtures" / "event_log" / "schema-v1-golden.jsonl"


class _Output(io.StringIO):
    def __init__(self, *, tty: bool = False) -> None:
        super().__init__()
        self._tty = tty
        self.width = 100

    def isatty(self) -> bool:
        return self._tty


def test_parser_exposes_wp7_flags_and_validates_numeric_bounds() -> None:
    parser = cli.build_parser()
    parsed = parser.parse_args(
        [
            "--log",
            "events.jsonl",
            "--speed",
            "4",
            "--min-free-mb",
            "0",
            "--max-log-mb",
            "50",
            "--sync-interval",
            "3",
            "run0001",
        ]
    )
    assert parsed.log == "events.jsonl"
    assert parsed.speed == 4
    assert parsed.min_free_mb == 0
    assert parsed.max_log_mb == 50
    assert parsed.sync_interval == 3

    for option, value in (
        ("--speed", "nan"),
        ("--speed", "-1"),
        ("--min-free-mb", "-1"),
        ("--max-log-mb", "0"),
        ("--sync-interval", "inf"),
    ):
        with pytest.raises(SystemExit) as raised:
            parser.parse_args([option, value])
        assert raised.value.code == 2


def test_invalid_mode_combinations_fail_before_runtime_loading(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_load_runtime",
        lambda: pytest.fail("local runtime must not load"),
    )
    monkeypatch.setattr(
        cli,
        "_load_replay_runtime",
        lambda: pytest.fail("replay runtime must not load"),
    )
    with pytest.raises(SystemExit) as raised:
        cli.main(["--replay", str(GOLDEN), "--remote", "host:/tmp/events", "--once"])
    assert raised.value.code == 2

    with pytest.raises(SystemExit) as raised:
        cli.main(["--serve", "--once"])
    assert raised.value.code == 2


def test_replay_once_renders_without_loading_live_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_load_runtime",
        lambda: pytest.fail("live workflow runtime must not load during replay"),
    )
    output = _Output()
    errors = _Output()

    assert (
        cli.main(
            ["--replay", str(GOLDEN), "--once"],
            stdout=output,
            stderr=errors,
        )
        == 0
    )
    assert "compute_ID0001" in output.getvalue()
    assert errors.getvalue() == ""


def test_remote_once_uses_typed_records_and_persistent_replay_state() -> None:
    records = read_jsonl(GOLDEN).records
    calls: list[object] = []

    class FakeReader:
        def __init__(self, location: str, **options: object) -> None:
            calls.append((location, options))

        def read(self, cursor: object) -> object:
            calls.append(cursor)
            return SimpleNamespace(
                records=records,
                cursor=SimpleNamespace(awaiting_checkpoint=False),
                at_eof=True,
            )

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=FakeReader)
    output = _Output()
    errors = _Output()

    assert (
        cli.main(
            ["--remote", "host:/tmp/workflow-events.jsonl", "--once"],
            runtime=runtime,
            stdout=output,
            stderr=errors,
        )
        == 0
    )
    assert calls
    assert "compute_ID0001" in output.getvalue()
    assert errors.getvalue() == ""


def test_remote_once_reads_through_eof_before_rendering_latest_checkpoint() -> None:
    records = read_jsonl(GOLDEN).records
    chunks = [records[:2], records[2:]]
    calls: list[object] = []

    class FakeReader:
        def __init__(self, _location: str, **_options: object) -> None:
            pass

        def read(self, cursor: object) -> object:
            calls.append(cursor)
            selected = chunks.pop(0)
            return SimpleNamespace(
                records=selected,
                cursor=object(),
                stream_replaced=False,
                at_eof=not chunks,
            )

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=FakeReader)
    output = _Output()

    assert (
        cli.main(
            ["--remote", "host:/tmp/workflow-events.jsonl", "--once"],
            runtime=runtime,
            stdout=output,
            stderr=_Output(),
        )
        == 0
    )
    assert len(calls) == 2
    assert "JOB_SUCCESS" in output.getvalue()
    assert "SUCCEEDED" in output.getvalue()


def test_remote_once_discards_completed_state_across_stream_replacement() -> None:
    records = read_jsonl(GOLDEN).records
    stream_id = "replacement-stream"
    replacement_header = replace(records[0], stream_id=stream_id)
    replacement_checkpoint = replace(records[1], stream_id=stream_id)
    chunks = [
        ((records[0], records[-1]), False, False),
        ((replacement_header,), True, False),
        ((replacement_checkpoint,), False, True),
    ]
    calls = 0

    class FakeReader:
        def __init__(self, _location: str, **_options: object) -> None:
            pass

        def read(self, _cursor: object) -> object:
            nonlocal calls
            calls += 1
            selected, replaced, at_eof = chunks.pop(0)
            return SimpleNamespace(
                records=selected,
                cursor=object(),
                stream_replaced=replaced,
                at_eof=at_eof,
            )

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=FakeReader)
    output = _Output()

    assert (
        cli.main(
            ["--remote", "host:/tmp/workflow-events.jsonl", "--once"],
            runtime=runtime,
            stdout=output,
            stderr=_Output(),
        )
        == 0
    )
    assert calls == 3
    assert "QUEUED" in output.getvalue()
    assert "SUCCEEDED" not in output.getvalue()


def test_event_recorder_captures_publication_and_database_without_queue() -> None:
    captured: list[tuple[object, object]] = []

    class Writer:
        def record_publication(self, publication: object, database: object) -> None:
            captured.append((publication, database))

        def close(self) -> None:
            captured.append(("close", "close"))

    database = object()
    coordinator = SimpleNamespace(reconciler=SimpleNamespace(database=database))
    publication = SimpleNamespace(
        sequence=7,
        clock=SimpleNamespace(epoch=1.0),
    )
    recorder = cli._EventRecorder(Writer())

    async def scenario() -> None:
        await recorder.capture(coordinator, publication)
        await recorder.capture(coordinator, publication)
        await recorder.close()

    asyncio.run(scenario())

    assert captured == [(publication, database), ("close", "close")]


def test_event_recorder_drains_cancelled_write_before_close() -> None:
    write_started = threading.Event()
    release_write = threading.Event()
    write_active = threading.Event()
    closed = threading.Event()

    class Writer:
        def record_publication(self, _publication: object, _database: object) -> None:
            write_active.set()
            write_started.set()
            assert release_write.wait(2)
            write_active.clear()

        def close(self) -> None:
            assert not write_active.is_set()
            closed.set()

    coordinator = SimpleNamespace(reconciler=SimpleNamespace(database=object()))
    publication = SimpleNamespace(
        sequence=1,
        clock=SimpleNamespace(epoch=1.0),
    )
    recorder = cli._EventRecorder(Writer())

    async def scenario() -> None:
        capture = asyncio.create_task(recorder.capture(coordinator, publication))
        assert await asyncio.to_thread(write_started.wait, 1)
        capture.cancel()
        close = asyncio.create_task(recorder.close())
        await asyncio.sleep(0)
        assert not close.done()
        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await capture
        await close

    asyncio.run(scenario())
    assert closed.is_set()


def test_event_recorder_discards_capture_queued_behind_close() -> None:
    close_started = threading.Event()
    release_close = threading.Event()
    writer_closed = threading.Event()

    class Writer:
        def record_publication(self, _publication: object, _database: object) -> None:
            assert not writer_closed.is_set()

        def close(self) -> None:
            close_started.set()
            assert release_close.wait(2)
            writer_closed.set()

    coordinator = SimpleNamespace(reconciler=SimpleNamespace(database=object()))
    publication = SimpleNamespace(
        sequence=1,
        clock=SimpleNamespace(epoch=1.0),
    )
    recorder = cli._EventRecorder(Writer())

    async def scenario() -> None:
        close = asyncio.create_task(recorder.close())
        assert await asyncio.to_thread(close_started.wait, 1)
        capture = asyncio.create_task(recorder.capture(coordinator, publication))
        await asyncio.sleep(0)
        assert not capture.done()
        release_close.set()
        await close
        await capture

    asyncio.run(scenario())
    assert writer_closed.is_set()


def test_headless_cleanup_attempts_all_steps_after_capture_failure() -> None:
    calls: list[str] = []

    class Coordinator:
        latest = object()

        async def close(self) -> None:
            calls.append("coordinator-close")

    class Recorder:
        async def capture(self, *_args: object, **_kwargs: object) -> None:
            calls.append("capture")
            raise RuntimeError("final capture failed")

        async def close(self) -> None:
            calls.append("recorder-close")

    lifecycle = cli._HeadlessMonitorLifecycle(
        SimpleNamespace(), Coordinator(), object(), SimpleNamespace(), Recorder()
    )

    with pytest.raises(RuntimeError, match="final capture failed"):
        asyncio.run(lifecycle.close())
    assert calls == ["capture", "coordinator-close", "recorder-close"]


def test_local_once_log_writes_canonical_stream(tmp_path) -> None:
    checkpoint = next(
        record
        for record in read_jsonl(GOLDEN).records
        if isinstance(record, CheckpointRecord)
    )
    database = checkpoint.snapshot
    publication = cli._replay_publication(
        database,
        sequence=1,
        recorded_at_epoch=database.snapshot_at_epoch,
    )
    braindump = tmp_path / "braindump.yml"
    braindump.write_text(
        "\n".join(
            (
                "user: alice",
                f"wf_uuid: {database.workflow.workflow.wf_uuid}",
                f"root_wf_uuid: {database.workflow.workflow.root_wf_uuid}",
                f"submit_dir: {tmp_path}",
                f"basedir: {tmp_path.parent}",
                "dax_label: golden",
                "planner_version: 6.0",
            )
        ),
        encoding="utf-8",
    )
    location = SimpleNamespace(
        workflow=database.workflow.workflow,
        braindump_path=braindump,
        submit_dir=tmp_path,
        recorded_submit_dir=tmp_path,
        basedir=tmp_path.parent,
        recorded_basedir=tmp_path.parent,
        root_submit_dir=tmp_path,
        dag_name="golden.dag",
        jobstate_path=tmp_path / "jobstate.log",
        database_path=tmp_path / "golden.stampede.db",
    )

    class Locator:
        def locate(self, *_args: object, **_kwargs: object) -> object:
            return location

    class Stampede:
        def __init__(self, _location: object) -> None:
            pass

    class CoordinatorConfig:
        def __init__(self, **_values: object) -> None:
            pass

    class Coordinator:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.latest = publication
            self.reconciler = SimpleNamespace(database=database)

        async def bootstrap(self) -> object:
            return publication

        async def close(self) -> None:
            return None

    runtime = cli.RuntimeComponents(
        Locator,
        Stampede,
        object,
        Coordinator,
        CoordinatorConfig,
        DisplayContext,
        DisplayOptions,
        DisplayAnalysis,
        object,
        lambda _snapshot: SimpleNamespace(),
        lambda *_args, **_kwargs: None,
        lambda *_args, **_kwargs: "dashboard",
        lambda *_args, **_kwargs: "rendered\n",
        rendering_gc_guard,
        object,
        object,
        lambda **_kwargs: None,
        SchedulerQueryKind,
    )
    log_path = tmp_path / "events.jsonl"

    assert (
        cli.main(
            [
                "--once",
                "--no-condor",
                "--no-live-events",
                "--log",
                str(log_path),
                str(tmp_path),
            ],
            runtime=runtime,
            stdout=_Output(),
            stderr=_Output(),
        )
        == 0
    )
    records = read_jsonl(log_path).records
    assert [record.to_json_dict()["record_type"] for record in records[:2]] == [
        "header",
        "checkpoint",
    ]


def test_server_modes_dispatch_without_a_tty(monkeypatch) -> None:
    sentinel = object()
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(cli, "_load_runtime", lambda: sentinel)
    monkeypatch.setattr(
        cli,
        "_run_server_launch_mode",
        lambda _args, runtime, _stdout: calls.append(("serve", runtime)) or 0,
    )
    monkeypatch.setattr(
        cli,
        "_run_stop_server_mode",
        lambda _args, runtime, _stdout: calls.append(("stop", runtime)) or 0,
    )

    assert cli.main(["--serve"], stdout=_Output(), stderr=_Output()) == 0
    assert cli.main(["--stop-server"], stdout=_Output(), stderr=_Output()) == 0
    assert calls == [("serve", sentinel), ("stop", sentinel)]


def _write_golden_log(path: Path, *, reason: str) -> None:
    records = read_jsonl(GOLDEN).records
    header = records[0]
    checkpoint = next(r for r in records if isinstance(r, CheckpointRecord))
    staged = path.with_name(f"{path.name}.tmp")
    staged.write_bytes(
        encode_record(header) + encode_record(replace(checkpoint, reason=reason))
    )
    staged.replace(path)


def _completed_launch(monkeypatch, tmp_path, server_writes) -> Path:
    log_path = tmp_path / "events.jsonl"
    monkeypatch.setattr(
        cli, "_locate_for_server", lambda *_: SimpleNamespace(submit_dir=tmp_path)
    )
    monkeypatch.setattr(cli, "_validated_event_log_path", lambda *_: log_path)
    monkeypatch.setattr(cli, "_server_foreground_argv", lambda *_a, **_k: ["serve"])

    def launch_server(*_args: object, **_kwargs: object) -> ServerLaunchResult:
        if server_writes is not None:
            _write_golden_log(log_path, reason=server_writes)
        return ServerLaunchResult(None, 4242, completed=True)

    monkeypatch.setattr("Pegasus.monitor.server.launch_server", launch_server)
    return log_path


def test_serve_accepts_server_that_finished_with_a_final_log(
    tmp_path, monkeypatch
) -> None:
    log_path = _completed_launch(monkeypatch, tmp_path, "final")
    output = _Output()

    assert cli._run_server_launch_mode(SimpleNamespace(), object(), output) == 0
    assert "workflow already complete" in output.getvalue()
    assert str(log_path) in output.getvalue()


def test_serve_launches_server_outside_the_submit_dir(tmp_path, monkeypatch) -> None:
    _completed_launch(monkeypatch, tmp_path, "final")
    completed = server.launch_server
    launches: list[dict[str, object]] = []

    def launch_server(*args: object, **kwargs: object) -> ServerLaunchResult:
        launches.append(kwargs)
        return completed(*args, **kwargs)

    monkeypatch.setattr("Pegasus.monitor.server.launch_server", launch_server)
    assert cli._run_server_launch_mode(SimpleNamespace(), object(), _Output()) == 0

    # `python -m` puts the cwd first on sys.path; the submit dir must not be it.
    assert launches[0]["cwd"] == Path("/")


@pytest.mark.parametrize("server_writes", ["initial", None])
def test_serve_rejects_finished_server_without_a_fresh_final_log(
    tmp_path, monkeypatch, server_writes
) -> None:
    log_path = _completed_launch(monkeypatch, tmp_path, server_writes)
    # A previous run's final log must not pass for this server's output.
    _write_golden_log(log_path, reason="final")

    with pytest.raises(ServerLaunchError, match="complete event log"):
        cli._run_server_launch_mode(SimpleNamespace(), object(), _Output())


def test_detached_foreground_argv_preserves_scoped_options() -> None:
    args = cli.build_parser().parse_args(
        [
            "--serve",
            "--no-condor",
            "--no-live-events",
            "--diagnose",
            "--log",
            "/tmp/events.jsonl",
            "--min-free-mb",
            "10",
            "--max-log-mb",
            "20",
            "run0001",
        ]
    )
    argv = cli._server_foreground_argv(args)

    assert "--serve-foreground" in argv
    assert "--no-condor" in argv
    assert "--no-live-events" in argv
    assert "--diagnose" in argv
    assert "/tmp/events.jsonl" in argv
    assert argv[-1] == "run0001"


@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("once", [False, True])
def test_midrun_stream_is_not_final_in_redirected_output(tmp_path, remote, once):
    from Pegasus.monitor.models import StreamHeader

    records = read_jsonl(GOLDEN).records
    header = next(r for r in records if isinstance(r, StreamHeader))
    checkpoint = next(r for r in records if isinstance(r, CheckpointRecord))
    workflow = replace(
        checkpoint.snapshot.workflow, state="WORKFLOW_STARTED", status=None
    )
    checkpoint = replace(
        checkpoint, snapshot=replace(checkpoint.snapshot, workflow=workflow)
    )
    records = (header, checkpoint)
    path = tmp_path / "midrun.jsonl"
    path.write_bytes(b"".join(encode_record(r) for r in records))

    class Reader:
        def __init__(self, *args, **kwargs):
            pass

        def read(self, cursor):
            return SimpleNamespace(records=records, cursor=cursor, at_eof=True)

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=Reader)
    args = ["--remote", "host:/tmp/events"] if remote else ["--replay", str(path)]
    output, errors = _Output(), _Output()
    assert (
        cli.main(
            [*args, *(["--once"] if once else [])],
            runtime=runtime,
            stdout=output,
            stderr=errors,
        )
        == 0
    ), errors.getvalue()
    assert "FINAL" not in output.getvalue()
    assert "ONCE" in output.getvalue()


@pytest.mark.parametrize("target", ["--diagnose", "-workflow", "run0001"])
def test_serve_relaunch_terminates_options_before_target(target):
    parser = cli.build_parser()
    args = parser.parse_args(["--serve", "--", target])
    argv = cli._server_foreground_argv(args)
    assert argv[-2:] == ["--", target]
    child = parser.parse_args(argv[3:])
    assert child.serve_foreground
    assert child.target == target
    assert not child.diagnose


@pytest.mark.parametrize("remote", [False, True])
def test_recorded_workflow_removal_is_visible_in_replay(tmp_path, remote):
    from Pegasus.monitor.event_log import EventLogWriter
    from Pegasus.monitor.models import (
        ClockSample,
        FrozenPayload,
        HealthState,
        SchedulerEvidence,
        SchedulerQueryRequest,
        SchedulerQueryResult,
        SourceHealth,
        SourceName,
    )
    from Pegasus.monitor.replay import ReplayEngine

    snapshot = next(
        record.snapshot
        for record in read_jsonl(GOLDEN).records
        if isinstance(record, CheckpointRecord)
    )
    transition = replace(
        snapshot.workflow.transition,
        identity=replace(
            snapshot.workflow.transition.identity,
            state="WORKFLOW_TERMINATED",
            timestamp=100,
        ),
        status=1,
    )
    snapshot = replace(
        snapshot,
        workflow=replace(
            snapshot.workflow,
            state="WORKFLOW_TERMINATED",
            status=1,
            ended_at=100,
            transition=transition,
        ),
        recent_workflow_transitions=(*snapshot.recent_workflow_transitions, transition),
        workflow_watermark=replace(
            snapshot.workflow_watermark,
            identities=(*snapshot.workflow_watermark.identities, transition.identity),
        ),
    )
    publication = cli._replay_publication(snapshot, sequence=1, recorded_at_epoch=100)
    history = SchedulerQueryResult(
        SchedulerQueryRequest(
            snapshot.workflow.workflow,
            SchedulerQueryKind.HISTORY,
            ClockSample(100, 100),
            5,
            10,
        ),
        SourceHealth(SourceName.CONDOR_HISTORY, HealthState.HEALTHY, 100),
        0,
        (
            SchedulerEvidence(
                SchedulerQueryKind.HISTORY,
                FrozenPayload.from_mapping({"ClusterId": 900, "ProcId": 0}),
                FrozenPayload.from_mapping(
                    {
                        "JobUniverse": 7,
                        "JobStatus": 3,
                        "RemoveReason": "Removed by pegasus-remove token=TOP_SECRET",
                        "EnteredCurrentStatus": 100,
                    }
                ),
            ),
        ),
    )
    publication = replace(publication, scheduler_results=(history,))
    path = tmp_path / "removed.jsonl"
    writer = EventLogWriter(path, snapshot.workflow.workflow, "test")
    writer.record_publication(publication, snapshot)
    writer.finish()
    writer.close()
    result = ReplayEngine(path, speed=0).replay()
    assert (
        result.workflow_removal_reason == "Removed by pegasus-remove token=<redacted>"
    )
    assert result.snapshot == snapshot  # Scheduler evidence never rewrites DB truth.
    records = read_jsonl(path).records

    class FakeReader:
        def __init__(self, *args, **kwargs):
            pass

        def read(self, cursor):
            return SimpleNamespace(records=records, cursor=object(), at_eof=True)

    runtime = replace(cli._load_replay_runtime(), remote_reader_type=FakeReader)
    output, errors = _Output(), _Output()
    args = ["--remote", "host:file"] if remote else ["--replay", str(path)]
    assert (
        cli.main([*args, "--once"], stdout=output, stderr=errors, runtime=runtime) == 0
    )
    assert "REMOVED" in output.getvalue()
    assert "pegasus-remove" in output.getvalue()
    assert "TOP_SECRET" not in output.getvalue()
