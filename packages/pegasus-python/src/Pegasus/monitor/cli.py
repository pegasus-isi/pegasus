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
    runtime: RuntimeComponents,
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
    runtime: RuntimeComponents,
    live: object,
    renderable_factory: Callable[[], object],
) -> None:
    """Perform one production Rich refresh under the rendering GC guard."""

    with runtime.rendering_gc_guard():
        live.update(renderable_factory(), refresh=True)


async def _run_once(
    args: argparse.Namespace,
    coordinator: object,
    context: object,
    runtime: RuntimeComponents,
    stdout: TextIO,
    stderr: TextIO,
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
        active_stall_key = _updated_active_stall(active_stall_key, analysis)
        analysis_sequence = snapshot.sequence
        with _live_rendering_session(
            runtime,
            lambda: runtime.render_dashboard(context, snapshot, analysis, options),
            console=console,
        ) as live:
            while True:
                current = coordinator.latest or snapshot
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
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            print(f"pegasus-monitor: {error}", file=stderr)
            return 1

        operation = asyncio.create_task(
            (
                _run_once(args, coordinator, context, runtime, stdout, stderr)
                if args.once or args.why_idle
                else _run_live(args, coordinator, context, runtime, stdout)
            ),
            name="pegasus-monitor-operation",
        )
        termination_waiter = asyncio.create_task(
            termination.wait(), name="pegasus-monitor-sigterm"
        )
        try:
            done, _pending = await asyncio.wait(
                (operation, termination_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if termination_waiter in done:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
                return 0
            return await operation
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            print(f"pegasus-monitor: {error}", file=stderr)
            return 1
        finally:
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            termination_waiter.cancel()
            await asyncio.gather(termination_waiter, return_exceptions=True)
            await coordinator.close()


def main(
    argv: list[str] | None = None,
    *,
    runtime: RuntimeComponents | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run ``pegasus-monitor`` and return its stable process exit status."""

    parser = build_parser()
    args = parser.parse_args(argv)
    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    one_shot = args.once or args.why_idle
    if not one_shot and not output.isatty():
        print(
            "pegasus-monitor: interactive live mode requires a TTY; use --once",
            file=errors,
        )
        return 1
    selected_runtime = runtime or _load_runtime()
    try:
        return asyncio.run(_run(args, selected_runtime, output, errors))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
