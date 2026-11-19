"""Server finalization when Stampede wins the race with scheduler history."""

from __future__ import annotations

import asyncio
import io
import signal
import threading
import time
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from Pegasus.monitor import cli, server
from Pegasus.monitor.coordinator import CoordinatorConfig, MonitorCoordinator
from Pegasus.monitor.display import DisplayAnalysis
from Pegasus.monitor.event_log import EventLogWriter, read_jsonl
from Pegasus.monitor.models import (
    CheckpointRecord,
    ClockSample,
    DBRefreshResult,
    FrozenPayload,
    HealthState,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryResult,
    SourceHealth,
    SourceName,
)
from Pegasus.monitor.replay import ReplayEngine

GOLDEN = Path(__file__).parent / "fixtures/event_log/schema-v1-golden.jsonl"
END = 1793904497
REMOVAL = "via condor_rm (by user ubuntu)"
GRACE = 0.6


def completed(snapshot, status=2):
    # Shape of pm-idle: Stampede knows SIGUSR1/status 2, while the job stays SUBMIT.
    transition = replace(
        snapshot.workflow.transition,
        identity=replace(
            snapshot.workflow.transition.identity,
            state="WORKFLOW_TERMINATED",
            timestamp=END,
        ),
        status=status,
        reason="DAGMan aborted as it received SIGUSR1 signal." if status else None,
    )
    return replace(
        snapshot,
        workflow=replace(
            snapshot.workflow,
            state="WORKFLOW_TERMINATED",
            status=status,
            ended_at=END,
            transition=transition,
        ),
        recent_workflow_transitions=(*snapshot.recent_workflow_transitions, transition),
        workflow_watermark=replace(
            snapshot.workflow_watermark,
            identities=(*snapshot.workflow_watermark.identities, transition.identity),
        ),
    )


class Stampede:
    def __init__(self):
        self.snapshot = next(
            r.snapshot
            for r in read_jsonl(GOLDEN).records
            if isinstance(r, CheckpointRecord)
        )

    def refresh(self, request):
        return DBRefreshResult(
            request,
            replace(
                self.snapshot,
                epoch=request.next_epoch,
                snapshot_at_epoch=request.clock.epoch,
            ),
            SourceHealth(SourceName.STAMPEDE, HealthState.HEALTHY, request.clock.epoch),
            self.snapshot.generation,
        )


class Scheduler:
    def __init__(self):
        self.terminal = False
        self.terminal_kind = SchedulerQueryKind.HISTORY
        self.health = HealthState.HEALTHY
        self.removed = True
        self.post_end_polls = 0
        self.completed = False
        self.observed_at = None
        self.workflow = None
        self.ended_at = END
        self.block = False
        self.blocked = threading.Event()
        self.release = threading.Event()

    def cancel(self):
        self.release.set()

    def query(self, request):
        if request.kind is SchedulerQueryKind.HISTORY and self.completed:
            self.post_end_polls += 1
            if self.block:
                self.blocked.set()
                self.release.wait(2)
        if self.observed_at is not None:
            request = replace(
                request, clock=ClockSample(self.observed_at, request.clock.monotonic)
            )
        if self.workflow is not None:
            request = replace(request, workflow=self.workflow)
        rows = []
        if request.kind is SchedulerQueryKind.QUEUE:
            # A healthy queue can still show the pre-removal DAGMan ad.
            rows = [{"ClusterId": 420, "ProcId": 0, "JobUniverse": 7, "JobStatus": 2}]
        elif request.kind is SchedulerQueryKind.HISTORY:
            rows = [
                {
                    "ClusterId": 440,
                    "ProcId": 0,
                    "DAGManJobId": 420,
                    "JobUniverse": 12,
                    "JobStatus": 4,
                    "CompletionDate": END - 168,
                }
            ]
        if self.terminal and request.kind is self.terminal_kind:
            terminal_row = {
                "ClusterId": 420,
                "ProcId": 0,
                "JobUniverse": 7,
                "JobStatus": 3 if self.removed else 4,
                "CompletionDate": 0 if self.removed else self.ended_at,
                "EnteredCurrentStatus": self.ended_at,
                "QDate": END - 194,
                **({"RemoveReason": REMOVAL} if self.removed else {}),
            }
            rows = (
                [terminal_row]
                if request.kind is SchedulerQueryKind.QUEUE
                else [*rows, terminal_row]
            )
        return SchedulerQueryResult(
            request,
            SourceHealth(
                SourceName.CONDOR_HISTORY
                if request.kind is SchedulerQueryKind.HISTORY
                else SourceName.CONDOR_QUEUE,
                self.health,
                request.clock.epoch,
            ),
            300 if self.health is not HealthState.HEALTHY else 0,
            tuple(
                SchedulerEvidence(
                    request.kind,
                    FrozenPayload.from_mapping(
                        {"ClusterId": row["ClusterId"], "ProcId": 0}
                    ),
                    FrozenPayload.from_mapping(row),
                )
                for row in rows
            ),
        )


async def eventually(predicate, timeout=2):
    # asyncio.timeout() is 3.11+; the package supports 3.10.
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), timeout)


class Scenario:
    def __init__(self, tmp_path, monkeypatch, *, scheduler=True):
        monkeypatch.setattr(
            cli, "SERVER_FINAL_EVIDENCE_GRACE_SECONDS", GRACE, raising=False
        )
        monkeypatch.setattr(
            cli, "SERVER_FINAL_HISTORY_INTERVAL_SECONDS", 0.02, raising=False
        )

        async def analyze(*_args):
            return DisplayAnalysis()

        monkeypatch.setattr(cli, "_analyze", analyze)
        self.stampede = Stampede()
        self.scheduler = Scheduler() if scheduler else None
        self.coordinator = MonitorCoordinator(
            self.stampede.snapshot.workflow.workflow,
            self.stampede,
            scheduler=self.scheduler,
            config=CoordinatorConfig(
                database_interval=60,
                # Only the explicit grace refreshes can retrieve a later history row.
                scheduler_intervals=((SchedulerQueryKind.HISTORY, 60),),
            ),
            clock=lambda: ClockSample(END + 3.6, time.monotonic()),
        )
        self.path = tmp_path / "events.jsonl"
        self.writer = EventLogWriter(
            self.path,
            self.stampede.snapshot.workflow.workflow,
            "test",
            min_free_mb=0,
            clock=lambda: END + 4,
            stream_id="server-finalization",
        )
        self.lifecycle = cli._HeadlessMonitorLifecycle(
            SimpleNamespace(diagnose=False, no_condor=False),
            self.coordinator,
            object(),
            SimpleNamespace(scheduler_kind=SchedulerQueryKind),
            cli._EventRecorder(self.writer),
        )

    async def complete(self, status=2):
        await self.lifecycle.ready.wait()
        if self.scheduler:
            self.scheduler.completed = True
        self.stampede.snapshot = completed(self.stampede.snapshot, status)
        await self.coordinator.refresh_database_once()

    def final_records(self):
        return [
            r
            for r in read_jsonl(self.path).records
            if isinstance(r, CheckpointRecord) and r.reason == "final"
        ]


@pytest.mark.parametrize("removed", [True, False])
def test_server_waits_for_later_terminal_dag_history(tmp_path, monkeypatch, removed):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        case.scheduler.removed = removed
        task = asyncio.create_task(case.lifecycle.run())
        try:
            await case.complete()
            await eventually(lambda: case.scheduler.post_end_polls >= 1)
            await asyncio.sleep(0.15)
            assert not task.done(), "child-only history must not finalize the workflow"
            assert not case.final_records()
            case.scheduler.terminal = True
            await asyncio.wait_for(task, 1)
            assert case.scheduler.post_end_polls >= 2
        finally:
            await case.lifecycle.close()
            await task
        final = case.final_records()
        assert len(final) == 1
        assert final[0].workflow_removal_reason == (REMOVAL if removed else None)
        replay = ReplayEngine(case.path, speed=0).replay()
        assert replay.stop_reason == "final"
        assert replay.workflow_removal_reason == (REMOVAL if removed else None)
        assert replay.snapshot == final[0].snapshot
        output = io.StringIO()
        assert cli.main(["--once", "--replay", str(case.path)], stdout=output) == 0
        assert ("REMOVED" if removed else "FAILED") in output.getvalue()
        if removed:
            assert REMOVAL in output.getvalue()
        else:
            assert "workflow_removal_reason" not in case.path.read_text()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "source",
    [
        "child_only",
        "unhealthy",
        "absent",
        "blocked",
        "pre_end",
        "wrong_workflow",
        "old_dag",
    ],
)
def test_server_failure_grace_is_bounded(tmp_path, monkeypatch, source):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch, scheduler=source != "absent")
        if source == "unhealthy":
            case.scheduler.health = HealthState.STALE
            case.scheduler.terminal = True  # Cached removal is not healthy evidence.
        if source in {"pre_end", "wrong_workflow", "old_dag"}:
            case.scheduler.terminal = True
            case.scheduler.removed = False
            if source == "pre_end":
                case.scheduler.observed_at = END - 1
            elif source == "wrong_workflow":
                case.scheduler.workflow = replace(
                    case.stampede.snapshot.workflow.workflow, wf_uuid="other"
                )
            else:
                case.scheduler.ended_at = END - 1
        task = asyncio.create_task(case.lifecycle.run())
        try:
            if source == "blocked":
                case.scheduler.block = True
            start = asyncio.get_running_loop().time()
            await case.complete()
            done, _ = await asyncio.wait({task}, timeout=1.2)
            assert task in done, "finalization must not await a blocked scheduler query"
            await task
            elapsed = asyncio.get_running_loop().time() - start
            assert GRACE <= elapsed < GRACE + 0.3
            assert len(case.final_records()) == 1
        finally:
            await case.lifecycle.close()
            await task
        assert "workflow_removal_reason" not in case.path.read_text()
        assert ReplayEngine(case.path, speed=0).replay().stop_reason == "final"

    asyncio.run(scenario())


def test_server_success_keeps_existing_stream_bytes_without_grace(
    tmp_path, monkeypatch
):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        captures = []
        original = case.writer.record_publication

        def record(publication, database, **kwargs):
            captures.append((publication, database))
            original(publication, database, **kwargs)

        case.writer.record_publication = record
        task = asyncio.create_task(case.lifecycle.run())
        try:
            await case.complete(status=0)
            start = asyncio.get_running_loop().time()
            await asyncio.wait_for(task, GRACE)
            assert asyncio.get_running_loop().time() - start < GRACE
        finally:
            await case.lifecycle.close()
            await task
        expected = tmp_path / "expected.jsonl"
        with closing(
            EventLogWriter(
                expected,
                case.stampede.snapshot.workflow.workflow,
                "test",
                min_free_mb=0,
                clock=lambda: END + 4,
                stream_id="server-finalization",
            )
        ) as writer:
            for publication, database in captures:
                writer.record_publication(publication, database)
            writer.finish()
        assert case.path.read_bytes() == expected.read_bytes()
        assert len(case.final_records()) == 1
        assert "workflow_removal_reason" not in case.path.read_text()

    asyncio.run(scenario())


@pytest.mark.parametrize("stop_kind", ["request", "sigterm"])
@pytest.mark.parametrize("timing", ["before_grace", "during_grace", "blocked_query"])
def test_server_stop_during_grace_records_stopped(
    tmp_path, monkeypatch, stop_kind, timing
):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        # Make the interruption test independent of the grace expiry.
        monkeypatch.setattr(
            cli, "SERVER_FINAL_EVIDENCE_GRACE_SECONDS", 30, raising=False
        )
        requested = asyncio.Event()
        handlers = {}
        loop = asyncio.get_running_loop()
        if stop_kind == "sigterm":
            monkeypatch.setattr(
                signal, "signal", lambda sig, cb: handlers.update({sig: cb})
            )
        paths = server.ServerPaths.from_log_path(case.path)
        task = asyncio.create_task(
            server.run_server_foreground(
                case.lifecycle,
                paths,
                stop_event=requested if stop_kind == "request" else None,
                readiness_probe=case.lifecycle.ready.wait,
            )
        )
        try:
            if timing == "blocked_query":
                case.scheduler.block = True
            await case.complete()
            await eventually(lambda: case.scheduler.post_end_polls >= 1)
            await asyncio.sleep(0.03 if timing == "before_grace" else 0.15)
            assert not task.done()
            assert not case.final_records()
            start = loop.time()
            if stop_kind == "sigterm":
                handlers[signal.SIGTERM](signal.SIGTERM, None)
            else:
                requested.set()
            result = await asyncio.wait_for(task, 0.5)
            assert loop.time() - start < 0.5
            assert result.reason is (
                server.ServerRunReason.SIGNAL
                if stop_kind == "sigterm"
                else server.ServerRunReason.STOP_REQUESTED
            )
        finally:
            requested.set()
            await case.lifecycle.close()
            await task
        assert not case.final_records()
        assert read_jsonl(case.path).records[-1].reason == "stopped"
        assert ReplayEngine(case.path, speed=0).replay().stop_reason == "stopped"
        assert not paths.metadata.exists()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", [SchedulerQueryKind.HISTORY, SchedulerQueryKind.QUEUE])
def test_server_accepts_terminal_evidence_observed_at_workflow_end(
    tmp_path, monkeypatch, kind
):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        case.scheduler.terminal = True
        case.scheduler.terminal_kind = kind
        case.scheduler.observed_at = END
        # Evidence already exists when an already-failed workflow is opened.
        await case.coordinator.poll_scheduler_once(kind)
        case.stampede.snapshot = completed(case.stampede.snapshot)
        start = asyncio.get_running_loop().time()
        try:
            await asyncio.wait_for(case.lifecycle.run(), GRACE / 2)
        finally:
            await case.lifecycle.close()
        assert asyncio.get_running_loop().time() - start < GRACE / 2
        assert len(case.final_records()) == 1
        assert case.final_records()[0].workflow_removal_reason == REMOVAL

    asyncio.run(scenario())


def test_server_already_failed_bootstrap_waits_for_history(tmp_path, monkeypatch):
    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        case.stampede.snapshot = completed(case.stampede.snapshot)
        case.scheduler.completed = True
        task = asyncio.create_task(case.lifecycle.run())
        try:
            await eventually(lambda: case.scheduler.post_end_polls >= 1)
            await asyncio.sleep(0.15)
            assert not task.done()
            case.scheduler.terminal = True
            await asyncio.wait_for(task, 1)
        finally:
            await case.lifecycle.close()
            await task
        assert case.lifecycle.ready.is_set()
        assert case.final_records()[0].workflow_removal_reason == REMOVAL

    asyncio.run(scenario())


def test_server_uses_removed_dag_timestamp_from_condor_history_projection(
    tmp_path, monkeypatch
):
    import json

    from Pegasus.monitor import condor

    async def scenario():
        case = Scenario(tmp_path, monkeypatch)
        case.stampede.snapshot = completed(case.stampede.snapshot)
        case.coordinator.scheduler = condor.CondorObserver()
        commands = []

        def subprocess(command, **_kwargs):
            commands.append(command)
            attributes = command[command.index("-attributes") + 1].split(",")
            raw = {
                "ClusterId": 420,
                "ProcId": 0,
                "JobUniverse": 7,
                "JobStatus": 3,
                "CompletionDate": 0,
                "EnteredCurrentStatus": END,
                "QDate": END - 194,
                "RemoveReason": REMOVAL,
                "pegasus_wf_uuid": case.stampede.snapshot.workflow.workflow.wf_uuid,
            }
            return SimpleNamespace(
                stdout=json.dumps(
                    [{k: v for k, v in raw.items() if k in attributes}]
                ).encode(),
                stderr=b"",
                returncode=0,
            )

        monkeypatch.setattr(condor, "_bounded_subprocess", subprocess)
        try:
            # The terminal row is immediately available; it must end the wait.
            await asyncio.wait_for(case.lifecycle.run(), GRACE / 2)
        finally:
            await case.lifecycle.close()
        assert case.final_records()[0].workflow_removal_reason == REMOVAL
        assert all(
            "EnteredCurrentStatus" in command[3].split(",") for command in commands
        )

    asyncio.run(scenario())
