"""Command-line entry point for native Pegasus workflow monitoring."""

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

import argparse
import asyncio
import importlib.metadata
import math
import signal
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

if TYPE_CHECKING:
    from collections.abc import Callable


DESCRIPTION = """\
Monitor one Pegasus workflow using read-only Stampede snapshots, the live
jobstate.log append stream, and optional read-only HTCondor observations.

TARGET may be a submit directory, a workflow base directory, or a
braindump.yml file. The latest numeric run is selected for a workflow base.
"""


def _positive_float(value: str) -> float:
    try:
        result = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be positive and finite")
    return result


def _nonnegative_float(value: str) -> float:
    try:
        result = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("must be non-negative and finite")
    return result


def _nonnegative_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if result < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return result


def _version() -> str:
    try:
        return importlib.metadata.version("pegasus-wms")
    except importlib.metadata.PackageNotFoundError:
        return "development"


@contextmanager
def _temporary_sigterm_handler(callback: Callable[[int, object | None], None]):
    """Install SIGTERM only on the main thread and restore prior ownership."""

    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, callback)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def build_parser() -> argparse.ArgumentParser:
    """Build exactly the version 1 parser without constructing any source."""

    parser = argparse.ArgumentParser(
        prog="pegasus-monitor",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=".",
        metavar="TARGET",
        help="Submit directory, workflow base, or braindump.yml (default: cwd)",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"pegasus-monitor {_version()}",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=_positive_float,
        default=2.0,
        metavar="SECONDS",
        help="Stampede refresh interval (default: 2.0)",
    )
    parser.add_argument(
        "-a",
        "--all-jobs",
        action="store_true",
        help="Show infrastructure jobs as well as compute jobs",
    )
    parser.add_argument(
        "--sort-by-activity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Put running and recently active jobs first (default: enabled)",
    )
    parser.add_argument(
        "-e",
        "--events",
        type=_nonnegative_int,
        default=15,
        metavar="N",
        help="Number of recent events to display (default: 15)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print one non-interactive snapshot and exit",
    )
    parser.add_argument(
        "--why-idle",
        action="store_true",
        help="Query bounded scheduler evidence once and explain idle jobs",
    )
    parser.add_argument(
        "--remap-submit-dir",
        choices=("auto", "always", "never"),
        default="auto",
        help="Remap recorded submit paths onto the local braindump location",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Show bounded failure and stall diagnostics without a sidecar",
    )
    parser.add_argument(
        "--log",
        nargs="?",
        const="auto",
        metavar="PATH",
        help="Write canonical JSONL (default: SUBMIT_DIR/workflow-events.jsonl)",
    )
    parser.add_argument(
        "--replay",
        metavar="PATH",
        help="Replay canonical JSONL without querying workflow sources",
    )
    parser.add_argument(
        "--speed",
        type=_nonnegative_float,
        default=1.0,
        metavar="MULTIPLIER",
        help="Replay speed (default: 1.0; 0 disables delays)",
    )
    parser.add_argument(
        "--no-condor",
        action="store_true",
        help="Never construct or invoke an HTCondor observer",
    )
    parser.add_argument(
        "--no-live-events",
        action="store_true",
        help="Disable the jobstate.log overlay and use Stampede only",
    )
    parser.add_argument(
        "--jobstate-path",
        metavar="PATH",
        help="Use an explicit jobstate.log path",
    )

    logging_guard = parser.add_argument_group("Logging safeguards")
    logging_guard.add_argument(
        "--min-free-mb",
        type=_nonnegative_float,
        default=200.0,
        metavar="MB",
        help="Pause logging below this free-space floor (default: 200)",
    )
    logging_guard.add_argument(
        "--max-log-mb",
        type=_positive_float,
        default=None,
        metavar="MB",
        help="Hard maximum event-log size (default: unlimited)",
    )

    server = parser.add_argument_group("Server, replay, and remote options")
    server.add_argument(
        "--serve",
        action="store_true",
        help="Launch a detached headless monitor server",
    )
    server.add_argument(
        "--serve-foreground",
        action="store_true",
        help="Run the headless monitor server in the foreground",
    )
    server.add_argument(
        "--stop-server",
        nargs="?",
        const="auto",
        metavar="PID_FILE",
        help="Stop the server selected by TARGET or an explicit PID file",
    )
    server.add_argument(
        "--remote",
        metavar="USER@HOST:PATH",
        help="Read and display a server JSONL stream over bounded SSH",
    )
    server.add_argument(
        "--sync-interval",
        type=_positive_float,
        default=5.0,
        metavar="SECONDS",
        help="Remote synchronization interval (default: 5.0)",
    )
    server.add_argument(
        "--ssh-config",
        metavar="PATH",
        help="SSH configuration file for --remote",
    )
    server.add_argument(
        "--ssh-identity",
        metavar="PATH",
        help="SSH identity file for --remote",
    )

    condor = parser.add_argument_group("HTCondor options")
    condor.add_argument("--schedd", metavar="NAME", help="Select a schedd")
    condor.add_argument("--collector", metavar="HOST[:PORT]", help="Select a collector")
    credentials = parser.add_argument_group("HTCondor credentials")
    credentials.add_argument(
        "--token", metavar="DIRECTORY", help="HTCondor IDTOKEN directory"
    )
    credentials.add_argument("--cert", metavar="PATH", help="X.509 certificate")
    credentials.add_argument("--key", metavar="PATH", help="X.509 private key")
    credentials.add_argument(
        "--password-file", metavar="PATH", help="HTCondor password file"
    )
    return parser


@dataclass(frozen=True, slots=True)
class RuntimeComponents:
    """Lazy implementation bundle and deterministic CLI test seam."""

    locator_type: type
    stampede_type: type
    live_tail_type: type
    coordinator_type: type
    coordinator_config_type: type
    display_context_type: type
    display_options_type: type
    display_analysis_type: type
    diagnostics_engine_type: type
    compute_stats: Callable[..., Any]
    analyze_why_idle: Callable[..., Any]
    render_dashboard: Callable[..., Any]
    render_text: Callable[..., str]
    rendering_gc_guard: Callable[..., Any]
    console_type: type
    live_type: type
    scheduler_factory: Callable[..., Any]
    scheduler_kind: type


@dataclass(frozen=True, slots=True)
class ReplayRuntime:
    """Source-free replay/remote bundle loaded without workflow providers."""

    replay_engine_type: type
    replay_accumulator_type: type
    remote_reader_type: type
    remote_cursor_type: type
    display_context_type: type
    display_options_type: type
    display_analysis_type: type
    compute_stats: Callable[..., Any]
    render_dashboard: Callable[..., Any]
    render_text: Callable[..., str]
    rendering_gc_guard: Callable[..., Any]
    console_type: type
    live_type: type


def _scheduler_factory(**values: object) -> object:
    """Import and construct HTCondor support only when it is enabled."""

    from Pegasus.monitor.condor import CondorObserver, CondorObserverConfig

    return CondorObserver(CondorObserverConfig(**values))


def _load_runtime() -> RuntimeComponents:
    from rich.console import Console
    from rich.live import Live

    from Pegasus.monitor.coordinator import CoordinatorConfig, MonitorCoordinator
    from Pegasus.monitor.diagnostics import DiagnosticsEngine
    from Pegasus.monitor.display import (
        DisplayAnalysis,
        DisplayContext,
        DisplayOptions,
        render_dashboard,
        render_text,
        rendering_gc_guard,
    )
    from Pegasus.monitor.live_events import LiveEventTail
    from Pegasus.monitor.locator import WorkflowLocator
    from Pegasus.monitor.models import SchedulerQueryKind
    from Pegasus.monitor.stampede import StampedeReader
    from Pegasus.monitor.stats import compute_workflow_stats
    from Pegasus.monitor.why_idle import analyze_why_idle

    return RuntimeComponents(
        WorkflowLocator,
        StampedeReader,
        LiveEventTail,
        MonitorCoordinator,
        CoordinatorConfig,
        DisplayContext,
        DisplayOptions,
        DisplayAnalysis,
        DiagnosticsEngine,
        compute_workflow_stats,
        analyze_why_idle,
        render_dashboard,
        render_text,
        rendering_gc_guard,
        Console,
        Live,
        _scheduler_factory,
        SchedulerQueryKind,
    )


def _load_replay_runtime() -> ReplayRuntime:
    """Load only schema, presentation, and SSH transport code."""

    from rich.console import Console
    from rich.live import Live

    from Pegasus.monitor.display import (
        DisplayAnalysis,
        DisplayContext,
        DisplayOptions,
        render_dashboard,
        render_text,
        rendering_gc_guard,
    )
    from Pegasus.monitor.remote import RemoteCursor, RemoteJSONLReader
    from Pegasus.monitor.replay import ReplayAccumulator, ReplayEngine
    from Pegasus.monitor.stats import compute_workflow_stats

    return ReplayRuntime(
        ReplayEngine,
        ReplayAccumulator,
        RemoteJSONLReader,
        RemoteCursor,
        DisplayContext,
        DisplayOptions,
        DisplayAnalysis,
        compute_workflow_stats,
        render_dashboard,
        render_text,
        rendering_gc_guard,
        Console,
        Live,
    )


def _load_display_context(location: object, runtime: RuntimeComponents) -> object:
    """Read the selected braindump once and freeze all presentation metadata."""

    from Pegasus import braindump

    braindump_path = Path(location.braindump_path)
    with braindump_path.open("r", encoding="utf-8") as stream:
        data = braindump.load(stream)
    dag_stem = Path(location.dag_name).name.removesuffix(".dag")
    return runtime.display_context_type(
        label=str(data.dax_label or data.pegasus_wf_name or dag_stem),
        owner=str(data.user) if data.user else None,
        planner_version=str(data.planner_version or "unknown"),
        planning_timestamp=(str(data.timestamp) if data.timestamp else None),
        submit_dir=Path(location.submit_dir),
        recorded_submit_dir=Path(location.recorded_submit_dir),
        basedir=Path(location.basedir),
        recorded_basedir=Path(location.recorded_basedir),
        root_submit_dir=Path(location.root_submit_dir),
        jobstate_path=Path(location.jobstate_path),
        database_path=(
            Path(location.database_path) if location.database_path is not None else None
        ),
        wf_uuid=location.workflow.wf_uuid,
        root_wf_uuid=location.workflow.root_wf_uuid,
        dag_name=str(location.dag_name),
    )


def _validate_mode_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    exclusive = [
        bool(args.replay),
        bool(args.remote),
        bool(args.serve),
        bool(args.serve_foreground),
        args.stop_server is not None,
    ]
    if sum(exclusive) > 1:
        parser.error(
            "--replay, --remote, --serve, --serve-foreground, and "
            "--stop-server are mutually exclusive"
        )
    if (args.replay or args.remote) and args.log is not None:
        parser.error("--log cannot be combined with --replay or --remote")
    if (args.serve or args.serve_foreground) and (args.once or args.why_idle):
        parser.error("server modes cannot be combined with one-shot modes")
    if args.stop_server is not None and args.log is not None:
        parser.error("--stop-server cannot be combined with --log")
    if not args.remote and (args.ssh_config or args.ssh_identity):
        parser.error("--ssh-config and --ssh-identity require --remote")


def _record_type_order(record: object) -> tuple[object, ...]:
    from Pegasus.monitor.models import DBJobTransition

    if isinstance(record, DBJobTransition):
        return (
            record.identity.timestamp,
            1,
            record.exec_job_id,
            record.identity.jobstate_submit_seq,
            record.identity.state,
        )
    return (
        record.identity.timestamp,
        0,
        record.restart_count,
        record.identity.state,
    )


def _replay_publication(
    database: object,
    *,
    sequence: int,
    recorded_at_epoch: float,
    awaiting_checkpoint: bool = False,
) -> object:
    """Adapt a canonical DB checkpoint to the existing source-free renderer."""

    from Pegasus.monitor.coordinator import CoordinatorSnapshot
    from Pegasus.monitor.models import (
        ClockSample,
        EffectiveEvent,
        EffectiveSnapshot,
        FrozenPayload,
        HealthState,
        Provenance,
        SourceHealth,
        SourceName,
    )

    transitions = sorted(
        database.recent_transitions + database.recent_workflow_transitions,
        key=_record_type_order,
    )
    events = tuple(
        EffectiveEvent(index, Provenance.DB_CONFIRMED, transition)
        for index, transition in enumerate(transitions, 1)
    )
    state = HealthState.GAP if awaiting_checkpoint else HealthState.HEALTHY
    health = (
        SourceHealth(
            SourceName.STAMPEDE,
            state,
            recorded_at_epoch,
            last_success_epoch=(None if awaiting_checkpoint else recorded_at_epoch),
            error_code=("event_log_gap" if awaiting_checkpoint else None),
        ),
        SourceHealth(
            SourceName.LIVE_TAIL,
            HealthState.DISABLED,
            recorded_at_epoch,
            error_code="replay_source_disabled",
        ),
    )
    effective = EffectiveSnapshot(
        database.epoch,
        database.workflow,
        database.jobs,
        database.generation,
        None,
        recorded_at_epoch,
        recorded_at_epoch,
        health,
        events,
    )
    complete = (
        not awaiting_checkpoint
        and database.workflow.state == "WORKFLOW_TERMINATED"
        and database.workflow.status is not None
    )
    latest = events[-1] if events else None
    return CoordinatorSnapshot(
        max(1, sequence),
        ClockSample(recorded_at_epoch, recorded_at_epoch),
        effective,
        health,
        (),
        0,
        (),
        None,
        len(events),
        latest,
        True,
        complete,
        coordinator_errors=FrozenPayload(),
    )


def _replay_context(header: object, path: Path, runtime: ReplayRuntime) -> object:
    metadata = header.source_metadata.to_json_dict()

    def text_value(name: str, default: str) -> str:
        value = metadata.get(name)
        return value if isinstance(value, str) and value else default

    submit_dir = Path(text_value("submit_dir", str(path.parent)))
    basedir = Path(text_value("basedir", str(submit_dir.parent)))
    return runtime.display_context_type(
        label=text_value("label", header.workflow.wf_uuid),
        owner=(
            metadata.get("owner") if isinstance(metadata.get("owner"), str) else None
        ),
        planner_version=text_value("planner_version", header.monitor_version),
        planning_timestamp=(
            metadata.get("planning_timestamp")
            if isinstance(metadata.get("planning_timestamp"), str)
            else None
        ),
        submit_dir=submit_dir,
        recorded_submit_dir=submit_dir,
        basedir=basedir,
        recorded_basedir=basedir,
        root_submit_dir=Path(text_value("root_submit_dir", str(submit_dir))),
        jobstate_path=Path(
            text_value("jobstate_path", str(submit_dir / "jobstate.log"))
        ),
        database_path=None,
        wf_uuid=header.workflow.wf_uuid,
        root_wf_uuid=header.workflow.root_wf_uuid,
        dag_name=text_value("dag_name", "workflow.dag"),
    )


def _replay_analysis(
    publication: object,
    diagnostics: tuple[object, ...],
    runtime: ReplayRuntime,
) -> object:
    errors = tuple(
        f"{item.severity.value}: {item.summary}" for item in diagnostics[-20:]
    )
    try:
        stats = runtime.compute_stats(publication)
    except (TypeError, ValueError, ArithmeticError):
        stats = None
    return runtime.display_analysis_type(stats=stats, errors=errors)


def _replay_options(
    args: argparse.Namespace,
    runtime: ReplayRuntime,
    *,
    width: int,
    live: bool,
    final: bool = False,
) -> object:
    return runtime.display_options_type(
        show_all_jobs=args.all_jobs,
        sort_by_activity=args.sort_by_activity,
        event_limit=args.events,
        job_row_limit=20 if live else 100,
        analysis_line_limit=8,
        width=max(40, width),
        condor_enabled=False,
        expected_scheduler_sources=(),
        live=live,
        final=final,
    )


class _EventRecorder:
    """Serialize immutable coordinator/DB pairs without the lossy publisher queue."""

    def __init__(self, writer: object) -> None:
        self.writer = writer
        self._last_publication_sequence = 0
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def started(self) -> bool:
        return bool(getattr(getattr(self.writer, "status", None), "started", False))

    async def capture(
        self,
        coordinator: object,
        publication: object,
        *,
        analysis: object | None = None,
        force: bool = False,
    ) -> None:
        async with self._lock:
            if self._closed:
                return
            # These references must be captured in the same event-loop turn.  The
            # reconciler mutates only between awaits, and both objects are immutable.
            database = coordinator.reconciler.database
            selected = publication
            if database is None:
                return
            if force or selected.sequence > self._last_publication_sequence:
                cancelled = await self._call_writer(
                    "record-publication",
                    self.writer.record_publication,
                    selected,
                    database,
                )
                self._last_publication_sequence = max(
                    self._last_publication_sequence, selected.sequence
                )
                if cancelled is not None:
                    raise cancelled
            diagnostics = getattr(analysis, "diagnostics", None)
            if diagnostics is not None:
                cancelled = await self._call_writer(
                    "record-diagnostics",
                    self.writer.record_diagnostics,
                    diagnostics,
                    snapshot_epoch=database.epoch,
                    recorded_at_epoch=selected.clock.epoch,
                )
                if cancelled is not None:
                    raise cancelled

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            cancelled = await self._call_writer("close", self.writer.close)
            if cancelled is not None:
                raise cancelled

    async def _call_writer(
        self,
        operation: str,
        callback: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> asyncio.CancelledError | None:
        """Shield one writer thread and drain it before propagating cancellation."""

        worker = asyncio.create_task(
            asyncio.to_thread(callback, *args, **kwargs),
            name=f"pegasus-monitor-event-log-{operation}",
        )
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancelled:
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    continue
            worker.result()
            return cancelled
        return None


def _event_log_path(args: argparse.Namespace, submit_dir: Path) -> Path:
    if args.log is None or args.log == "auto":
        return submit_dir / "workflow-events.jsonl"
    return Path(args.log).expanduser().absolute()


def _make_event_recorder(
    args: argparse.Namespace,
    location: object,
    context: object,
) -> _EventRecorder:
    from Pegasus.monitor.event_log import EventLogWriter

    metadata = {
        "label": context.label,
        "owner": context.owner,
        "planner_version": context.planner_version,
        "planning_timestamp": context.planning_timestamp,
        "submit_dir": str(context.submit_dir),
        "basedir": str(context.basedir),
        "root_submit_dir": str(context.root_submit_dir),
        "jobstate_path": str(context.jobstate_path),
        "dag_name": context.dag_name,
    }
    writer = EventLogWriter(
        _event_log_path(args, Path(location.submit_dir)),
        location.workflow,
        _version(),
        source_metadata=metadata,
        min_free_mb=args.min_free_mb,
        max_log_mb=args.max_log_mb,
    )
    return _EventRecorder(writer)


def _make_scheduler(
    args: argparse.Namespace, runtime: RuntimeComponents
) -> object | None:
    if args.no_condor:
        return None
    return runtime.scheduler_factory(
        schedd=args.schedd,
        collector=args.collector,
        token=args.token,
        cert=args.cert,
        key=args.key,
        password_file=args.password_file,
    )


def _make_coordinator(
    args: argparse.Namespace, location: object, runtime: RuntimeComponents
) -> object:
    stampede = runtime.stampede_type(location)
    tail_factory = None
    if not args.no_live_events:
        path = Path(location.jobstate_path)

        def tail_factory() -> object:
            return runtime.live_tail_type(path)

    scheduler = _make_scheduler(args, runtime)
    config = runtime.coordinator_config_type(database_interval=args.interval)
    return runtime.coordinator_type(
        location.workflow,
        stampede,
        tail_factory=tail_factory,
        scheduler=scheduler,
        config=config,
    )


def _disabled_scheduler_kinds(
    args: argparse.Namespace, runtime: RuntimeComponents
) -> frozenset[object]:
    return frozenset(runtime.scheduler_kind) if args.no_condor else frozenset()


async def _analyze(
    snapshot: object,
    context: object,
    args: argparse.Namespace,
    runtime: RuntimeComponents,
    diagnostics_engine: object | None,
) -> object:
    if snapshot.effective is None:
        return runtime.display_analysis_type(
            errors=("authoritative Stampede snapshot unavailable",)
        )

    disabled = _disabled_scheduler_kinds(args, runtime)

    def analyze_sync() -> object:
        errors: list[str] = []
        try:
            stats = runtime.compute_stats(snapshot)
        except (TypeError, ValueError, ArithmeticError) as error:
            stats = None
            errors.append(f"statistics:{type(error).__name__}")
        diagnostics = None
        if diagnostics_engine is not None:
            try:
                diagnostics = diagnostics_engine.analyze(
                    snapshot,
                    submit_dir=context.submit_dir,
                    workflow_owner=context.owner,
                    disabled_scheduler_kinds=disabled,
                )
            except (OSError, TypeError, ValueError, ArithmeticError) as error:
                errors.append(f"diagnostics:{type(error).__name__}")
        why_idle = None
        if args.why_idle:
            try:
                why_idle = runtime.analyze_why_idle(
                    snapshot,
                    workflow_owner=context.owner,
                    disabled_scheduler_kinds=disabled,
                )
            except (TypeError, ValueError, ArithmeticError) as error:
                errors.append(f"why_idle:{type(error).__name__}")
        return runtime.display_analysis_type(
            stats=stats,
            diagnostics=diagnostics,
            why_idle=why_idle,
            errors=tuple(errors),
        )

    # Kickstart parsing and all potentially large analyses stay off the event loop.
    return await asyncio.to_thread(analyze_sync)


def _display_options(
    args: argparse.Namespace,
    runtime: RuntimeComponents,
    *,
    width: int,
    height: int = 40,
    live: bool,
    final: bool = False,
) -> object:
    # Reserve the live viewport for source state, analyses, and final statistics.
    # Row-heavy job/event detail is capped independently of workflow size.
    analysis_panels = int(args.diagnose) + int(args.why_idle)
    analysis_line_limit = max(
        2,
        min(
            8,
            ((max(20, height) - 26) // max(1, analysis_panels)) - 2,
        ),
    )
    if live:
        event_limit = min(args.events, 5)
        reserved = 28 + analysis_panels * (analysis_line_limit + 2)
        row_limit = max(3, min(20, height - reserved))
    else:
        event_limit = args.events
        row_limit = 100
    if args.no_condor:
        scheduler_sources = ()
    elif args.why_idle:
        scheduler_sources = (
            "condor_queue",
            "condor_pool",
            "condor_priority",
            "condor_negotiator",
        )
    elif live or final:
        scheduler_sources = ("condor_queue", "condor_history", "condor_pool")
    else:
        scheduler_sources = ("condor_queue",)
    return runtime.display_options_type(
        show_all_jobs=args.all_jobs,
        sort_by_activity=args.sort_by_activity,
        event_limit=event_limit,
        job_row_limit=row_limit,
        analysis_line_limit=analysis_line_limit,
        width=max(40, width),
        condor_enabled=not args.no_condor,
        expected_scheduler_sources=scheduler_sources,
        live=live,
        final=final,
    )


def _preserve_analysis(previous: object, current: object, runtime: RuntimeComponents):
    """Retain the last active panel when a refreshed analysis is partial."""

    if "authoritative Stampede snapshot unavailable" in current.errors:
        return current
    stall = current.diagnostics.stall if current.diagnostics is not None else None
    resolved = stall is not None and stall.event.value == "stall_resolved"
    return runtime.display_analysis_type(
        stats=current.stats or previous.stats,
        diagnostics=current.diagnostics or previous.diagnostics,
        why_idle=(None if resolved else current.why_idle or previous.why_idle),
        errors=current.errors,
    )


def _stall_enrichment_key(analysis: object) -> tuple[object, object] | None:
    diagnostics = analysis.diagnostics
    stall = diagnostics.stall if diagnostics is not None else None
    if stall is None or stall.event.value != "stall_detected" or stall.queued_jobs <= 0:
        return None
    return stall.kind, stall.since_monotonic


def _updated_active_stall(
    active: tuple[object, object] | None, analysis: object
) -> tuple[object, object] | None:
    diagnostics = analysis.diagnostics
    stall = diagnostics.stall if diagnostics is not None else None
    if stall is None:
        return active
    if stall.event.value == "stall_resolved":
        return None
    return _stall_enrichment_key(analysis) or active


def _apply_stall_enrichment(
    analysis: object,
    enriched_idle: object | None,
    pending_key: tuple[object, object] | None,
    active_key: tuple[object, object] | None,
    runtime: RuntimeComponents,
) -> object:
    if enriched_idle is None or pending_key is None or pending_key != active_key:
        return analysis
    return runtime.display_analysis_type(
        stats=analysis.stats,
        diagnostics=analysis.diagnostics,
        why_idle=enriched_idle,
        errors=analysis.errors,
    )


async def _enrich_stall(
    coordinator: object,
    context: object,
    args: argparse.Namespace,
    runtime: RuntimeComponents,
) -> object | None:
    for kind in (
        runtime.scheduler_kind.PRIORITY,
        runtime.scheduler_kind.NEGOTIATOR,
    ):
        await coordinator.poll_scheduler_once(kind)
    snapshot = coordinator.latest
    if snapshot is None or snapshot.effective is None:
        return None
    return await asyncio.to_thread(
        runtime.analyze_why_idle,
        snapshot,
        workflow_owner=context.owner,
        disabled_scheduler_kinds=_disabled_scheduler_kinds(args, runtime),
    )


async def _query_once(coordinator: object, kinds: tuple[object, ...]) -> object:
    for kind in kinds:
        await coordinator.poll_scheduler_once(kind)
    return coordinator.latest


@contextmanager
def _live_rendering_session(
    runtime: RuntimeComponents | ReplayRuntime,
    renderable_factory: Callable[[], object],
    *,
    console: object,
):
    """Enter and leave Rich Live with GC bounded to its render operations."""

    with runtime.rendering_gc_guard():
        manager = runtime.live_type(
            renderable_factory(),
            console=console,
            refresh_per_second=4,
            auto_refresh=False,
            transient=False,
            screen=True,
        )
        live = manager.__enter__()
    try:
        yield live
    except BaseException:
        with runtime.rendering_gc_guard():
            suppressed = manager.__exit__(*sys.exc_info())
        if not suppressed:
            raise
    else:
        with runtime.rendering_gc_guard():
            manager.__exit__(None, None, None)


def _refresh_live(
    runtime: RuntimeComponents | ReplayRuntime,
    live: object,
    renderable_factory: Callable[[], object],
) -> None:
    """Perform one production Rich refresh under the rendering GC guard."""

    with runtime.rendering_gc_guard():
        live.update(renderable_factory(), refresh=True)


def _read_replay_header(path: Path) -> object:
    from Pegasus.monitor.event_log import EventLogFormatError, decode_json_line
    from Pegasus.monitor.models import StreamHeader

    with path.open("rb") as stream:
        line = stream.readline(64 * 1024 + 1)
    if len(line) > 64 * 1024:
        raise EventLogFormatError("stream header exceeds 64 KiB")
    record = decode_json_line(line)
    if not isinstance(record, StreamHeader):
        raise EventLogFormatError("event stream does not begin with a header")
    return record


def _run_replay_mode(
    args: argparse.Namespace,
    runtime: ReplayRuntime,
    stdout: TextIO,
) -> int:
    path = Path(args.replay)
    header = _read_replay_header(path)
    context = _replay_context(header, path, runtime)
    engine = runtime.replay_engine_type(
        path,
        speed=(0 if args.once else args.speed),
        retain_frames=False,
    )

    if args.once:
        result = engine.replay()
        if not result.complete or result.snapshot is None:
            raise RuntimeError("replay has no complete checkpoint")
        publication = _replay_publication(
            result.snapshot,
            sequence=1,
            recorded_at_epoch=result.snapshot.snapshot_at_epoch,
            awaiting_checkpoint=result.awaiting_checkpoint,
        )
        analysis = _replay_analysis(publication, result.current_diagnostics, runtime)
        width = getattr(stdout, "width", 120)
        stdout.write(
            runtime.render_text(
                context,
                publication,
                analysis,
                _replay_options(args, runtime, width=width, live=False, final=True),
                width=max(40, width),
            )
        )
        stdout.flush()
        return 0

    console = runtime.console_type(file=stdout)
    manager = None
    live = None
    frame_sequence = 0

    def display_frame(frame: object) -> None:
        nonlocal manager, live, frame_sequence
        frame_sequence += 1
        publication = _replay_publication(
            frame.snapshot,
            sequence=frame_sequence,
            recorded_at_epoch=frame.recorded_at_epoch,
            awaiting_checkpoint=frame.awaiting_checkpoint,
        )
        analysis = _replay_analysis(publication, frame.diagnostics, runtime)
        options = _replay_options(args, runtime, width=console.width, live=True)
        renderable = runtime.render_dashboard(context, publication, analysis, options)
        if manager is None:
            manager = runtime.live_type(
                renderable,
                console=console,
                refresh_per_second=4,
                auto_refresh=False,
                transient=False,
                screen=True,
            )
            with runtime.rendering_gc_guard():
                live = manager.__enter__()
        else:
            _refresh_live(runtime, live, lambda: renderable)

    try:
        result = engine.replay(display_frame)
    finally:
        if manager is not None:
            with runtime.rendering_gc_guard():
                manager.__exit__(*sys.exc_info())
    if not result.complete:
        raise RuntimeError("replay ended while awaiting a recovery checkpoint")
    return 0


async def _run_remote_mode(
    args: argparse.Namespace,
    runtime: ReplayRuntime,
    stdout: TextIO,
) -> int:
    reader = runtime.remote_reader_type(
        args.remote,
        ssh_config=args.ssh_config,
        ssh_identity=args.ssh_identity,
    )
    cursor = runtime.remote_cursor_type()
    accumulator = runtime.replay_accumulator_type()
    console = None if args.once else runtime.console_type(file=stdout)
    manager = None
    live = None
    publication_sequence = 0
    latest_publication = None
    latest_analysis = None
    context = None

    try:
        while True:
            result = await asyncio.to_thread(reader.read, cursor)
            cursor = result.cursor
            if getattr(result, "stream_replaced", False):
                latest_publication = None
                latest_analysis = None
                context = None
                publication_sequence = 0
            changed = False
            last_timestamp = None
            for record in result.records:
                changed = accumulator.consume(record) or changed
                last_timestamp = getattr(
                    record,
                    "recorded_at_epoch",
                    getattr(record, "created_at_epoch", None),
                )
            if accumulator.header is not None and context is None:
                context = _replay_context(
                    accumulator.header, Path("workflow-events.jsonl"), runtime
                )
            if changed and accumulator.snapshot is not None and context is not None:
                publication_sequence += 1
                latest_publication = _replay_publication(
                    accumulator.snapshot,
                    sequence=publication_sequence,
                    recorded_at_epoch=(
                        float(last_timestamp)
                        if last_timestamp is not None
                        else accumulator.snapshot.snapshot_at_epoch
                    ),
                    awaiting_checkpoint=accumulator.awaiting_checkpoint,
                )
                latest_analysis = _replay_analysis(
                    latest_publication, accumulator.diagnostics, runtime
                )
                if not args.once:
                    options = _replay_options(
                        args, runtime, width=console.width, live=True
                    )
                    renderable = runtime.render_dashboard(
                        context, latest_publication, latest_analysis, options
                    )
                    if manager is None:
                        manager = runtime.live_type(
                            renderable,
                            console=console,
                            refresh_per_second=4,
                            auto_refresh=False,
                            transient=False,
                            screen=True,
                        )
                        with runtime.rendering_gc_guard():
                            live = manager.__enter__()
                    else:
                        _refresh_live(runtime, live, lambda: renderable)

            if args.once:
                if result.at_eof:
                    if accumulator.snapshot is None or accumulator.awaiting_checkpoint:
                        raise RuntimeError("remote stream has no complete checkpoint")
                    break
            elif (
                accumulator.snapshot is not None
                and not accumulator.awaiting_checkpoint
                and latest_publication is not None
                and latest_publication.authoritative_complete
            ):
                break

            # A large checkpoint may need another bounded range immediately.
            if not result.at_eof:
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(args.sync_interval)
    finally:
        if manager is not None:
            with runtime.rendering_gc_guard():
                manager.__exit__(*sys.exc_info())

    if latest_publication is None or latest_analysis is None or context is None:
        raise RuntimeError("remote stream has no complete checkpoint")
    if args.once:
        width = getattr(stdout, "width", 120)
        stdout.write(
            runtime.render_text(
                context,
                latest_publication,
                latest_analysis,
                _replay_options(args, runtime, width=width, live=False, final=True),
                width=max(40, width),
            )
        )
        stdout.flush()
    return 0


async def _run_once(
    args: argparse.Namespace,
    coordinator: object,
    context: object,
    runtime: RuntimeComponents,
    stdout: TextIO,
    stderr: TextIO,
    recorder: _EventRecorder | None = None,
) -> int:
    diagnostics_engine = runtime.diagnostics_engine_type() if args.diagnose else None
    snapshot = await coordinator.bootstrap()
    if not args.no_condor:
        if args.why_idle:
            kinds = (
                runtime.scheduler_kind.QUEUE,
                runtime.scheduler_kind.POOL,
                runtime.scheduler_kind.PRIORITY,
                runtime.scheduler_kind.NEGOTIATOR,
            )
        else:
            kinds = (runtime.scheduler_kind.QUEUE,)
        snapshot = await _query_once(coordinator, kinds) or snapshot
    analysis = await _analyze(snapshot, context, args, runtime, diagnostics_engine)
    if recorder is not None:
        await recorder.capture(coordinator, snapshot, analysis=analysis, force=True)
    width = getattr(stdout, "width", 120)
    options = _display_options(args, runtime, width=width, live=False)
    stdout.write(
        runtime.render_text(
            context,
            snapshot,
            analysis,
            options,
            width=max(40, width),
        )
    )
    stdout.flush()
    # Tail-only evidence is useful in live mode but cannot satisfy --once.
    if snapshot.has_authoritative_base:
        return 0
    print(
        "pegasus-monitor: --once requires an authoritative Stampede snapshot",
        file=stderr,
    )
    return 1


async def _run_live(
    args: argparse.Namespace,
    coordinator: object,
    context: object,
    runtime: RuntimeComponents,
    stdout: TextIO,
    recorder: _EventRecorder | None = None,
) -> int:
    diagnostics_engine = runtime.diagnostics_engine_type() if args.diagnose else None
    console = runtime.console_type(file=stdout)
    options = _display_options(
        args, runtime, width=console.width, height=console.height, live=True
    )
    analysis = runtime.display_analysis_type()
    analysis_sequence = 0
    analysis_task: asyncio.Task[object] | None = None
    enrichment_task: asyncio.Task[object | None] | None = None
    pending_enrichment_key: tuple[object, object] | None = None
    enriched_stalls: set[tuple[object, object]] = set()
    active_stall_key: tuple[object, object] | None = None
    scheduled_sequence = 0
    snapshot = None
    last_rendered_snapshot = None
    cancelled = False
    try:
        snapshot = await coordinator.start()
        analysis = await _analyze(snapshot, context, args, runtime, diagnostics_engine)
        if recorder is not None:
            await recorder.capture(coordinator, snapshot, analysis=analysis)
        active_stall_key = _updated_active_stall(active_stall_key, analysis)
        analysis_sequence = snapshot.sequence
        with _live_rendering_session(
            runtime,
            lambda: runtime.render_dashboard(context, snapshot, analysis, options),
            console=console,
        ) as live:
            while True:
                current = coordinator.latest or snapshot
                if recorder is not None:
                    await recorder.capture(coordinator, current)
                if analysis_task is not None and analysis_task.done():
                    try:
                        analysis = _preserve_analysis(
                            analysis, analysis_task.result(), runtime
                        )
                    except (OSError, TypeError, ValueError, ArithmeticError) as error:
                        analysis = runtime.display_analysis_type(
                            errors=(f"analysis:{type(error).__name__}",)
                        )
                    analysis_sequence = scheduled_sequence
                    analysis_task = None
                    active_stall_key = _updated_active_stall(active_stall_key, analysis)
                    if recorder is not None and current.sequence == analysis_sequence:
                        await recorder.capture(coordinator, current, analysis=analysis)
                if enrichment_task is not None and enrichment_task.done():
                    try:
                        enriched_idle = enrichment_task.result()
                    except (OSError, TypeError, ValueError, ArithmeticError) as error:
                        analysis = runtime.display_analysis_type(
                            stats=analysis.stats,
                            diagnostics=analysis.diagnostics,
                            why_idle=analysis.why_idle,
                            errors=analysis.errors
                            + (f"stall_enrichment:{type(error).__name__}",),
                        )
                    else:
                        analysis = _apply_stall_enrichment(
                            analysis,
                            enriched_idle,
                            pending_enrichment_key,
                            active_stall_key,
                            runtime,
                        )
                    if pending_enrichment_key is not None:
                        enriched_stalls.add(pending_enrichment_key)
                    pending_enrichment_key = None
                    enrichment_task = None
                if (
                    not args.no_condor
                    and active_stall_key is not None
                    and active_stall_key not in enriched_stalls
                    and enrichment_task is None
                ):
                    pending_enrichment_key = active_stall_key
                    enrichment_task = asyncio.create_task(
                        _enrich_stall(
                            coordinator,
                            context,
                            args,
                            runtime,
                        ),
                        name="pegasus-monitor-stall-enrichment",
                    )
                if current.sequence > analysis_sequence and analysis_task is None:
                    scheduled_sequence = current.sequence
                    analysis_task = asyncio.create_task(
                        _analyze(
                            current,
                            context,
                            args,
                            runtime,
                            diagnostics_engine,
                        ),
                        name="pegasus-monitor-analysis",
                    )
                options = _display_options(
                    args,
                    runtime,
                    width=console.width,
                    height=console.height,
                    live=True,
                )
                _refresh_live(
                    runtime,
                    live,
                    lambda: runtime.render_dashboard(
                        context, current, analysis, options
                    ),
                )
                last_rendered_snapshot = current
                if current.authoritative_complete:
                    if not args.no_condor:
                        await coordinator.poll_scheduler_once(
                            runtime.scheduler_kind.HISTORY
                        )
                        current = coordinator.latest or current
                    if analysis_task is not None:
                        analysis = _preserve_analysis(
                            analysis, await analysis_task, runtime
                        )
                        analysis_task = None
                        analysis_sequence = scheduled_sequence
                    if analysis_sequence != current.sequence:
                        analysis = _preserve_analysis(
                            analysis,
                            await _analyze(
                                current,
                                context,
                                args,
                                runtime,
                                diagnostics_engine,
                            ),
                            runtime,
                        )
                        analysis_sequence = current.sequence
                    _refresh_live(
                        runtime,
                        live,
                        lambda: runtime.render_dashboard(
                            context, current, analysis, options
                        ),
                    )
                    last_rendered_snapshot = current
                    if recorder is not None:
                        await recorder.capture(
                            coordinator, current, analysis=analysis, force=True
                        )
                    return 0
                await asyncio.sleep(0.25)
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if analysis_task is not None:
            analysis_task.cancel()
            await asyncio.gather(analysis_task, return_exceptions=True)
        if enrichment_task is not None:
            enrichment_task.cancel()
            await asyncio.gather(enrichment_task, return_exceptions=True)
        if snapshot is not None and not cancelled:
            final_snapshot = last_rendered_snapshot or coordinator.latest or snapshot
            final_options = _display_options(
                args,
                runtime,
                width=console.width,
                height=console.height,
                live=False,
                final=True,
            )
            with runtime.rendering_gc_guard():
                console.print(
                    runtime.render_dashboard(
                        context, final_snapshot, analysis, final_options
                    )
                )


async def _run(
    args: argparse.Namespace,
    runtime: RuntimeComponents,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    termination = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_termination(_signum: int, _frame: object | None) -> None:
        loop.call_soon_threadsafe(termination.set)

    with _temporary_sigterm_handler(request_termination):
        locator = runtime.locator_type()
        try:
            location = locator.locate(
                args.target,
                remap_submit_dir=args.remap_submit_dir,
                jobstate_path=args.jobstate_path,
            )
            context = _load_display_context(location, runtime)
            coordinator = _make_coordinator(args, location, runtime)
            recorder = (
                _make_event_recorder(args, location, context)
                if args.log is not None
                else None
            )
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            print(f"pegasus-monitor: {error}", file=stderr)
            return 1

        operation = asyncio.create_task(
            (
                _run_once(
                    args,
                    coordinator,
                    context,
                    runtime,
                    stdout,
                    stderr,
                    recorder=recorder,
                )
                if args.once or args.why_idle
                else _run_live(
                    args,
                    coordinator,
                    context,
                    runtime,
                    stdout,
                    recorder=recorder,
                )
            ),
            name="pegasus-monitor-operation",
        )
        termination_waiter = asyncio.create_task(
            termination.wait(), name="pegasus-monitor-sigterm"
        )
        result = 0
        try:
            done, _pending = await asyncio.wait(
                (operation, termination_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if termination_waiter in done:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
            else:
                result = await operation
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            print(f"pegasus-monitor: {error}", file=stderr)
            result = 1
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            termination_waiter.cancel()
            await asyncio.gather(termination_waiter, return_exceptions=True)
            cleanup_errors = await _close_monitor_resources(coordinator, recorder)
            if cleanup_errors:
                cancellation = next(
                    (
                        error
                        for error in cleanup_errors
                        if isinstance(error, asyncio.CancelledError)
                    ),
                    None,
                )
                if cancellation is not None:
                    raise cancellation
                print(f"pegasus-monitor: {cleanup_errors[0]}", file=stderr)
                result = 1
        return result


async def _close_monitor_resources(
    coordinator: object,
    recorder: _EventRecorder | None,
) -> tuple[BaseException, ...]:
    """Attempt final capture and both closes, retaining errors in call order."""

    errors: list[BaseException] = []
    if recorder is not None and coordinator.latest is not None:
        try:
            await recorder.capture(coordinator, coordinator.latest, force=True)
        except BaseException as error:
            errors.append(error)
    try:
        await coordinator.close()
    except BaseException as error:
        errors.append(error)
    if recorder is not None:
        try:
            await recorder.close()
        except BaseException as error:
            errors.append(error)
    return tuple(errors)


class _HeadlessMonitorLifecycle:
    """Coordinator/event-log lifecycle used only by explicit server mode."""

    def __init__(
        self,
        args: argparse.Namespace,
        coordinator: object,
        context: object,
        runtime: RuntimeComponents,
        recorder: _EventRecorder,
    ) -> None:
        self.args = args
        self.coordinator = coordinator
        self.context = context
        self.runtime = runtime
        self.recorder = recorder
        self.ready = asyncio.Event()
        self.stop = asyncio.Event()
        self._closed = False

    async def run(self) -> None:
        diagnostics_engine = (
            self.runtime.diagnostics_engine_type() if self.args.diagnose else None
        )
        publication = await self.coordinator.start()
        last_sequence = 0
        while not self.stop.is_set():
            current = self.coordinator.latest or publication
            if current.sequence > last_sequence:
                await self.recorder.capture(
                    self.coordinator,
                    current,
                    force=(current.authoritative_complete or not self.recorder.started),
                )
                if not self.recorder.started:
                    await asyncio.sleep(0.1)
                    continue
                last_sequence = current.sequence
                if not self.ready.is_set():
                    self.ready.set()
                analysis = await _analyze(
                    current,
                    self.context,
                    self.args,
                    self.runtime,
                    diagnostics_engine,
                )
                latest = self.coordinator.latest or current
                if latest.sequence == current.sequence:
                    await self.recorder.capture(
                        self.coordinator, current, analysis=analysis
                    )
                if current.authoritative_complete:
                    return
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stop.set()
        errors = await _close_monitor_resources(self.coordinator, self.recorder)
        if errors:
            raise errors[0]


def _locate_for_server(args: argparse.Namespace, runtime: RuntimeComponents) -> object:
    return runtime.locator_type().locate(
        args.target,
        remap_submit_dir=args.remap_submit_dir,
        jobstate_path=args.jobstate_path,
    )


def _server_paths_for_log(log_path: Path) -> object:
    from Pegasus.monitor.server import ServerPaths

    return ServerPaths.from_log_path(log_path)


async def _run_server_foreground_mode(
    args: argparse.Namespace,
    runtime: RuntimeComponents,
) -> int:
    from Pegasus.monitor.server import run_server_foreground

    location = _locate_for_server(args, runtime)
    context = _load_display_context(location, runtime)
    coordinator = _make_coordinator(args, location, runtime)
    recorder = _make_event_recorder(args, location, context)
    lifecycle = _HeadlessMonitorLifecycle(args, coordinator, context, runtime, recorder)
    paths = _server_paths_for_log(_event_log_path(args, Path(location.submit_dir)))
    await run_server_foreground(
        lifecycle,
        paths,
        readiness_probe=lifecycle.ready.wait,
    )
    return 0


def _append_option(argv: list[str], name: str, value: object | None) -> None:
    if value is not None:
        argv.extend((name, str(value)))


def _server_foreground_argv(
    args: argparse.Namespace,
    *,
    target: str | None = None,
    log_path: Path | None = None,
) -> list[str]:
    argv = [sys.executable, "-m", "Pegasus.monitor.cli", "--serve-foreground"]
    _append_option(argv, "--interval", args.interval)
    _append_option(argv, "--events", args.events)
    _append_option(argv, "--remap-submit-dir", args.remap_submit_dir)
    _append_option(argv, "--min-free-mb", args.min_free_mb)
    _append_option(argv, "--max-log-mb", args.max_log_mb)
    if args.all_jobs:
        argv.append("--all-jobs")
    if not args.sort_by_activity:
        argv.append("--no-sort-by-activity")
    if args.diagnose:
        argv.append("--diagnose")
    if args.no_condor:
        argv.append("--no-condor")
    if args.no_live_events:
        argv.append("--no-live-events")
    for name in ("schedd", "collector"):
        _append_option(argv, f"--{name.replace('_', '-')}", getattr(args, name))
    for name in ("jobstate_path", "token", "cert", "key", "password_file"):
        value = getattr(args, name)
        normalized = None if value is None else Path(value).expanduser().absolute()
        _append_option(argv, f"--{name.replace('_', '-')}", normalized)
    if args.log not in (None, "auto"):
        _append_option(argv, "--log", log_path or args.log)
    argv.append(target or args.target)
    return argv


def _run_server_launch_mode(
    args: argparse.Namespace,
    runtime: RuntimeComponents,
    stdout: TextIO,
) -> int:
    from Pegasus.monitor.server import launch_server

    location = _locate_for_server(args, runtime)
    log_path = _event_log_path(args, Path(location.submit_dir))
    result = launch_server(
        _server_foreground_argv(
            args,
            target=str(location.submit_dir),
            log_path=log_path,
        ),
        _server_paths_for_log(log_path),
        cwd=Path(location.submit_dir),
    )
    print(
        f"pegasus-monitor server pid {result.pid}; log {log_path}",
        file=stdout,
    )
    return 0


def _explicit_server_paths(metadata_path: Path) -> object:
    from Pegasus.monitor.server import ServerPaths

    lock_name = (
        f"{metadata_path.name[:-4]}.lock"
        if metadata_path.name.endswith(".pid")
        else f"{metadata_path.name}.lock"
    )
    return ServerPaths(metadata_path, metadata_path.with_name(lock_name))


def _run_stop_server_mode(
    args: argparse.Namespace,
    runtime: RuntimeComponents,
    stdout: TextIO,
) -> int:
    from Pegasus.monitor.server import stop_server

    if args.stop_server == "auto":
        location = _locate_for_server(args, runtime)
        log_path = Path(location.submit_dir) / "workflow-events.jsonl"
        paths = _server_paths_for_log(log_path)
    else:
        paths = _explicit_server_paths(Path(args.stop_server))
    result = stop_server(paths)
    print(f"pegasus-monitor server: {result.status.value}", file=stdout)
    return 0


def main(
    argv: list[str] | None = None,
    *,
    runtime: RuntimeComponents | ReplayRuntime | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run ``pegasus-monitor`` and return its stable process exit status."""

    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_mode_args(parser, args)
    output = stdout or sys.stdout
    errors = stderr or sys.stderr

    if args.replay or args.remote:
        one_shot = args.once
        if not one_shot and not output.isatty():
            print(
                "pegasus-monitor: replay/remote live mode requires a TTY; use --once",
                file=errors,
            )
            return 1
        selected_replay = runtime or _load_replay_runtime()
        try:
            if args.replay:
                return _run_replay_mode(args, selected_replay, output)
            return asyncio.run(_run_remote_mode(args, selected_replay, output))
        except KeyboardInterrupt:
            return 0
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            print(f"pegasus-monitor: {error}", file=errors)
            return 1

    server_action = args.stop_server is not None or args.serve or args.serve_foreground
    one_shot = args.once or args.why_idle
    if not server_action and not one_shot and not output.isatty():
        print(
            "pegasus-monitor: interactive live mode requires a TTY; use --once",
            file=errors,
        )
        return 1

    selected_runtime = runtime or _load_runtime()
    try:
        if args.stop_server is not None:
            return _run_stop_server_mode(args, selected_runtime, output)
        if args.serve:
            return _run_server_launch_mode(args, selected_runtime, output)
        if args.serve_foreground:
            return asyncio.run(_run_server_foreground_mode(args, selected_runtime))
    except KeyboardInterrupt:
        return 0
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        print(f"pegasus-monitor: {error}", file=errors)
        return 1

    try:
        return asyncio.run(_run(args, selected_runtime, output, errors))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
