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

from Pegasus.monitor import cli
from Pegasus.monitor.display import (
    DisplayAnalysis,
    DisplayContext,
    DisplayOptions,
    rendering_gc_guard,
)
from Pegasus.monitor.event_log import read_jsonl
from Pegasus.monitor.models import CheckpointRecord, SchedulerQueryKind

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
