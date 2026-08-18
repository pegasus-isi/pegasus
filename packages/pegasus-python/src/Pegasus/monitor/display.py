"""Pure Rich presentation for the native Pegasus workflow monitor."""

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

import io
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from Pegasus.monitor.models import (
    DBJobTransition,
    DBWorkflowTransition,
    EffectiveEvent,
    HealthState,
    JobSnapshot,
    Lifecycle,
    Provenance,
    SchedulerQueryKind,
    SourceHealth,
    TailJobEvent,
    TailWorkflowEvent,
)

if TYPE_CHECKING:
    from pathlib import Path

    from Pegasus.monitor.coordinator import CoordinatorSnapshot
    from Pegasus.monitor.diagnostics import DiagnosticsBatch
    from Pegasus.monitor.stats import WorkflowStats
    from Pegasus.monitor.why_idle import IdleAnalysis


_STATE_STYLE = {
    Lifecycle.UNSUBMITTED: "dim",
    Lifecycle.PRE: "blue",
    Lifecycle.QUEUED: "yellow",
    Lifecycle.RUNNING: "bold cyan",
    Lifecycle.HELD: "bold magenta",
    Lifecycle.POST: "blue",
    Lifecycle.SUCCEEDED: "green",
    Lifecycle.FAILED: "bold red",
    Lifecycle.OTHER: "white",
}

_HEALTH_STYLE = {
    HealthState.HEALTHY: "green",
    HealthState.WAITING: "yellow",
    HealthState.STALE: "yellow",
    HealthState.DEGRADED: "yellow",
    HealthState.GAP: "bold red",
    HealthState.REATTACHING: "yellow",
    HealthState.RESYNC: "yellow",
    HealthState.FAILED_UNCONFIRMED: "bold red",
    HealthState.UNAVAILABLE: "red",
    HealthState.DISABLED: "dim",
}


@dataclass(frozen=True, slots=True)
class DisplayContext:
    """Braindump and locator metadata captured once by the CLI."""

    label: str
    owner: str | None
    planner_version: str
    planning_timestamp: str | None
    submit_dir: Path
    recorded_submit_dir: Path
    basedir: Path
    recorded_basedir: Path
    root_submit_dir: Path
    jobstate_path: Path
    database_path: Path | None
    wf_uuid: str
    root_wf_uuid: str
    dag_name: str


@dataclass(frozen=True, slots=True)
class DisplayOptions:
    show_all_jobs: bool = False
    sort_by_activity: bool = True
    event_limit: int = 15
    job_row_limit: int = 200
    analysis_line_limit: int = 8
    width: int = 120
    condor_enabled: bool = True
    expected_scheduler_sources: tuple[str, ...] = (
        "condor_queue",
        "condor_history",
        "condor_pool",
    )
    live: bool = True
    final: bool = False

    def __post_init__(self) -> None:
        if self.event_limit < 0:
            raise ValueError("event_limit must be non-negative")
        if self.job_row_limit <= 0:
            raise ValueError("job_row_limit must be positive")
        if self.analysis_line_limit <= 0:
            raise ValueError("analysis_line_limit must be positive")
        if self.width <= 0:
            raise ValueError("display width must be positive")
        object.__setattr__(
            self, "expected_scheduler_sources", tuple(self.expected_scheduler_sources)
        )


@dataclass(frozen=True, slots=True)
class DisplayAnalysis:
    """Already-computed analysis data; renderers never acquire evidence."""

    stats: WorkflowStats | None = None
    diagnostics: DiagnosticsBatch | None = None
    why_idle: IdleAnalysis | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "errors", tuple(self.errors))


@dataclass(frozen=True, slots=True)
class _JobRoster:
    visible: tuple[JobSnapshot, ...]
    selected_total: int
    lifecycle_counts: Mapping[Lifecycle, int]
    provenance_counts: Mapping[Provenance, int]


def _duration(seconds: float | Decimal | None) -> str:
    if seconds is None:
        return "-"
    value = max(0, int(float(seconds)))
    days, value = divmod(value, 86400)
    hours, value = divmod(value, 3600)
    minutes, value = divmod(value, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}:{value:02d}"
    return f"{hours:02d}:{minutes:02d}:{value:02d}"


def _timestamp(value: float | Decimal | None) -> str:
    if value is None:
        return "-"
    return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")


def _short_uuid(value: str) -> str:
    return value if len(value) <= 12 else f"{value[:12]}..."


def _current_attempt(job: JobSnapshot):
    if job.current_attempt is None:
        return None
    return next(
        (
            attempt
            for attempt in job.attempts
            if attempt.identity == job.current_attempt
        ),
        None,
    )


def _scheduler_rows(job: JobSnapshot, kind: str) -> tuple[Mapping[str, object], ...]:
    scheduler = getattr(job, "scheduler", None)
    if scheduler is None:
        return ()
    payload = scheduler.to_json_dict()
    value = payload.get(kind)
    if isinstance(value, Mapping):
        return (value,)
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, Mapping))
    return ()


def _scheduler_row(
    job: JobSnapshot, kind: str, attempt: object | None
) -> Mapping[str, object] | None:
    rows = _scheduler_rows(job, kind)
    scheduler_id = getattr(attempt, "scheduler_id", None)
    if scheduler_id:
        for row in rows:
            cluster = row.get("ClusterId")
            process = row.get("ProcId", 0)
            if cluster is not None and f"{cluster}.{process}" == scheduler_id:
                return row
        return None
    if len(rows) != 1:
        return None
    row = rows[0]
    if row.get("DAGNodeName") != job.exec_job_id:
        return None
    cluster = row.get("ClusterId")
    row_scheduler_id = (
        f"{cluster}.{row.get('ProcId', 0)}" if cluster is not None else None
    )
    other_scheduler_ids = {
        candidate.scheduler_id
        for candidate in job.attempts
        if candidate is not attempt and candidate.scheduler_id
    }
    if row_scheduler_id in other_scheduler_ids:
        return None
    if row_scheduler_id is None and other_scheduler_ids:
        return None
    return row


def _number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _memory(maxrss_kb: int | None) -> str:
    if maxrss_kb is None:
        return "-"
    if maxrss_kb < 1024:
        return f"{maxrss_kb}K"
    return f"{maxrss_kb / 1024:.1f}M"


def _scheduler_display(
    job: JobSnapshot, attempt: object | None
) -> tuple[str, str, str]:
    queue = _scheduler_row(job, "queue", attempt)
    history = _scheduler_row(job, "history", attempt)
    row = queue if queue is not None else history
    if row is None:
        return "-", "-", "-"
    if queue is not None:
        status = {
            1: "IDLE",
            2: "RUN",
            3: "REMOVED",
            4: "DONE",
            5: "HELD",
            6: "XFER",
            7: "SUSP",
        }.get(int(_number(row.get("JobStatus")) or 0), "QUEUE")
    else:
        status = "HISTORY"
    cpus = _number(row.get("RequestCpus"))
    memory = _number(row.get("RequestMemory"))
    gpus = _number(row.get("RequestGpus", row.get("RequestGPUs")))
    request = "/".join(
        (
            f"C{int(cpus)}" if cpus is not None else "C?",
            f"M{int(memory)}" if memory is not None else "M?",
            f"G{int(gpus)}" if gpus is not None and gpus > 0 else "G0",
        )
    )
    host = str(
        row.get("RemoteHost") or row.get("LastRemoteHost") or row.get("Machine") or "-"
    )
    return status, request, host


def _activity_sort_key(
    index: int, job: JobSnapshot, lifecycle: Lifecycle
) -> tuple[tuple[int, float], int]:
    cached = getattr(job, "activity_sort_key", None)
    if cached is not None:
        return cached, index
    timestamp = float(job.state_timestamp) if job.state_timestamp is not None else -1.0
    if lifecycle is Lifecycle.RUNNING:
        return (0, -timestamp), index
    if job.state_timestamp is not None:
        return (1, -timestamp), index
    return (2, 0.0), index


def _visible_jobs(
    jobs: tuple[JobSnapshot, ...], options: DisplayOptions
) -> tuple[list[JobSnapshot], int]:
    roster = _job_roster(jobs, options)
    return list(roster.visible), roster.selected_total


def _job_roster(jobs: tuple[JobSnapshot, ...], options: DisplayOptions) -> _JobRoster:
    """Select bounded rows and aggregate the full roster in one traversal."""

    total = 0
    counts = dict.fromkeys(Lifecycle, 0)
    provenance_counts = dict.fromkeys(Provenance, 0)

    def selected():
        nonlocal total
        for index, job in enumerate(jobs):
            lifecycle = job.lifecycle
            counts[lifecycle] += 1
            provenance_counts[job.provenance] += 1
            is_compute = getattr(job, "is_compute", None)
            if is_compute is None:
                is_compute = job.type_desc.lower() == "compute"
            if options.show_all_jobs or is_compute:
                total += 1
                yield _activity_sort_key(index, job, lifecycle), job, lifecycle

    if options.sort_by_activity:
        rows = sorted(selected())[: options.job_row_limit]
    else:
        rows = []
        for item in selected():
            if len(rows) < options.job_row_limit:
                rows.append(item)
    return _JobRoster(
        tuple(job for _, job, _ in rows), total, counts, provenance_counts
    )


def _header(
    context: DisplayContext,
    snapshot: CoordinatorSnapshot,
    options: DisplayOptions,
) -> Panel:
    title = Text("Pegasus Workflow Monitor", style="bold white")
    title.append("  ")
    if options.final:
        title.append(" FINAL ", style="bold white on blue")
    elif snapshot.has_authoritative_base:
        title.append(
            " LIVE " if options.live else " ONCE ", style="bold white on green"
        )
    else:
        title.append(" DB FAILED / LIVE UNCONFIRMED ", style="bold black on yellow")

    grid = Table.grid(expand=True)
    grid.add_column(ratio=1)
    grid.add_column(justify="right", no_wrap=True)
    grid.add_row(title, Text(_timestamp(snapshot.clock.epoch), style="dim"))
    details = Text()
    details.append(context.label, style="bold yellow")
    details.append("  uuid ", style="dim")
    details.append(_short_uuid(context.wf_uuid), style="cyan")
    details.append("  owner ", style="dim")
    details.append(context.owner or "unknown", style="cyan")
    version = Text(f"Pegasus {context.planner_version}", style="dim")
    grid.add_row(details, version)
    return Panel(grid, padding=(0, 1), style="on grey7")


def _health_label(health: SourceHealth) -> Text:
    text = Text()
    text.append(health.source.value.replace("_", " "), style="bold")
    text.append(" ")
    text.append(health.state.value.upper(), style=_HEALTH_STYLE[health.state])
    if health.last_good_age is not None:
        suffix = "+" if health.last_good_age.capped else ""
        text.append(f" ({health.last_good_age.seconds:.0f}{suffix}s old)", style="dim")
    if health.pending_count:
        text.append(f" [{health.pending_count} pending]", style="yellow")
    if health.error_code:
        text.append(f" {health.error_code}", style="dim red")
    return text


def _health_panel(snapshot: CoordinatorSnapshot, options: DisplayOptions) -> Panel:
    health = list(snapshot.source_health)
    known = {item.source for item in health}
    for result in snapshot.scheduler_results:
        if result.health.source not in known:
            health.append(result.health)
            known.add(result.health.source)
    entries: list[Text] = []
    if health:
        for item in sorted(health, key=lambda value: value.source.value):
            entries.append(_health_label(item))
    else:
        entries.append(Text("No source health has been published", style="dim"))
    if not options.condor_enabled:
        entries.append(Text("HTCondor DISABLED", style="dim"))
    else:
        known_names = {item.value for item in known}
        for source in sorted(set(options.expected_scheduler_sources) - known_names):
            entries.append(
                Text(
                    f"{source.replace('_', ' ')} WAITING (not yet polled)",
                    style="yellow",
                )
            )
    if snapshot.publisher_failures:
        entries.append(
            Text(
                f"display publisher degraded ({snapshot.publisher_failures} failures)",
                style="yellow",
            )
        )
    for name, detail in snapshot.coordinator_errors.to_json_dict().items():
        entries.append(Text(f"{name}: {detail}", style="red"))
    columns = 2 if options.width >= 88 else 1
    grid = Table.grid(expand=True, padding=(0, 2))
    for _ in range(columns):
        grid.add_column(ratio=1)
    for index in range(0, len(entries), columns):
        row = entries[index : index + columns]
        grid.add_row(*row, *(Text() for _ in range(columns - len(row))))
    return Panel(grid, title="Sources", padding=(0, 1))


def _status_panel(
    snapshot: CoordinatorSnapshot,
    analysis: DisplayAnalysis,
    roster: _JobRoster | None = None,
) -> Panel:
    effective = snapshot.effective
    if effective is None:
        body = Text()
        body.append("Stampede authority is unavailable. ", style="bold yellow")
        body.append(
            f"{len(snapshot.unconfirmed_tail_events)} bounded live event(s) are visible; "
            "job counts and completion are intentionally withheld.",
            style="yellow",
        )
        return Panel(body, title="Workflow Status", padding=(0, 1))

    jobs = effective.jobs
    if roster is None:
        roster = _job_roster(jobs, DisplayOptions())
    counts = roster.lifecycle_counts
    provenance_counts = roster.provenance_counts
    succeeded = counts[Lifecycle.SUCCEEDED]
    failed = counts[Lifecycle.FAILED]
    done = succeeded + failed
    total = len(jobs)
    state = Text()
    if snapshot.authoritative_complete and effective.workflow.status == 0:
        state.append("SUCCESS", style="bold green")
    elif snapshot.authoritative_complete:
        state.append("FAILED", style="bold red")
    else:
        state.append(effective.workflow.state, style="bold cyan")
    state.append(f"  {done}/{total} terminal", style="white")
    state.append(f"  run {counts[Lifecycle.RUNNING]}", style="cyan")
    state.append(f"  queued {counts[Lifecycle.QUEUED]}", style="yellow")
    if counts[Lifecycle.HELD]:
        state.append(f"  held {counts[Lifecycle.HELD]}", style="magenta")
    if failed:
        state.append(f"  failed {failed}", style="red")
    if effective.pending_overlay_count:
        state.append(
            f"  pending overlay {effective.pending_overlay_count}", style="yellow"
        )
    if analysis.stats is not None:
        state.append(f"  elapsed {_duration(analysis.stats.wall_time)}", style="dim")
    provenance = Text()
    provenance.append(f"workflow {effective.workflow.provenance.value}", style="dim")
    provenance.append(
        "  jobs: "
        + ", ".join(
            f"{source.value}={provenance_counts[source]}"
            for source in Provenance
            if provenance_counts[source]
        ),
        style="dim",
    )
    return Panel(Group(state, provenance), title="Workflow Status", padding=(0, 1))


def _job_panel(
    snapshot: CoordinatorSnapshot,
    options: DisplayOptions,
    roster: _JobRoster | None = None,
) -> Panel | None:
    effective = snapshot.effective
    if effective is None:
        return None
    if roster is None:
        roster = _job_roster(effective.jobs, options)
    jobs = roster.visible
    total = roster.selected_total
    compact = options.width < 88
    wide = options.width >= 132
    table = Table(box=box.SIMPLE, expand=True, pad_edge=False)
    table.add_column("Job", ratio=3, overflow="ellipsis", no_wrap=True)
    if wide:
        table.add_column("Transformation", ratio=2, overflow="ellipsis")
    table.add_column("State", width=9, no_wrap=True)
    if compact:
        table.add_column("Try/Exit/RSS", justify="right", no_wrap=True)
    else:
        table.add_column("Try", justify="right", no_wrap=True)
        table.add_column("Site", ratio=1, overflow="ellipsis")
        table.add_column("Runtime", justify="right", no_wrap=True)
        table.add_column("Exit", justify="right", no_wrap=True)
        table.add_column("RSS", justify="right", no_wrap=True)
    table.add_column("Live", ratio=1, overflow="ellipsis", no_wrap=True)
    if wide:
        table.add_column("Request", no_wrap=True)
        table.add_column("Host", ratio=2, overflow="ellipsis")
    if not compact:
        table.add_column("Source", ratio=1, overflow="ellipsis")
    for job in jobs:
        attempt = _current_attempt(job)
        live_status, request, host = _scheduler_display(job, attempt)
        runtime = None
        if attempt is not None and attempt.start_time is not None:
            end = attempt.end_time or Decimal(str(snapshot.clock.epoch))
            runtime = max(Decimal(0), end - attempt.start_time)
        values: list[RenderableType] = [
            Text(job.exec_job_id, overflow="ellipsis"),
        ]
        if wide:
            values.append(
                job.transformations[0]
                if getattr(job, "transformations", ())
                else job.type_desc
            )
        values.append(
            Text(job.lifecycle.value.upper(), style=_STATE_STYLE[job.lifecycle])
        )
        exit_code = (
            "-"
            if attempt is None or attempt.exit_code is None
            else str(attempt.exit_code)
        )
        memory = _memory(None if attempt is None else attempt.maxrss_kb)
        if compact:
            values.append(f"{len(job.attempts)}/{exit_code}/{memory}")
        else:
            values.append(str(len(job.attempts)))
            values.extend(
                [
                    "-" if attempt is None or not attempt.site else attempt.site,
                    _duration(runtime),
                ]
            )
            values.extend((exit_code, memory))
        values.append(live_status)
        if wide:
            values.extend((request, host))
        if not compact:
            values.append(job.provenance.value.replace("_", " "))
        table.add_row(*values)
    title = f"Jobs ({len(jobs)} shown"
    if total > len(jobs):
        title += f" of {total}"
    title += ")"
    return Panel(table, title=title, padding=(0, 1))


def _event_parts(event: EffectiveEvent) -> tuple[float, str, str, str]:
    value = event.event
    if isinstance(value, DBJobTransition):
        return (
            float(value.identity.timestamp),
            value.exec_job_id,
            value.normalized_state,
            "DB CONFIRMED",
        )
    if isinstance(value, DBWorkflowTransition):
        return (
            float(value.identity.timestamp),
            "workflow",
            value.normalized_state,
            "DB CONFIRMED",
        )
    if isinstance(value, TailJobEvent):
        return (
            float(value.event_timestamp),
            value.exec_job_id,
            value.normalized_state,
            "LIVE PENDING",
        )
    assert isinstance(value, TailWorkflowEvent)
    return (
        float(value.event_timestamp),
        "workflow",
        value.normalized_state,
        "LIVE PENDING",
    )


def _events_panel(snapshot: CoordinatorSnapshot, options: DisplayOptions) -> Panel:
    events = (
        snapshot.effective.events
        if snapshot.effective is not None
        else snapshot.unconfirmed_tail_events
    )
    visible = events[-options.event_limit :] if options.event_limit else ()
    table = Table(box=box.SIMPLE, expand=True, pad_edge=False)
    table.add_column("Time", no_wrap=True)
    table.add_column("Job", ratio=2, overflow="ellipsis")
    table.add_column("Event", ratio=2, overflow="ellipsis")
    table.add_column("Provenance", ratio=1, overflow="ellipsis")
    for event in visible:
        when, target, state, provenance = _event_parts(event)
        style = "yellow" if provenance.startswith("LIVE") else "green"
        table.add_row(
            datetime.fromtimestamp(when).strftime("%H:%M:%S"),
            target,
            state,
            Text(provenance, style=style),
        )
    if not visible:
        table.add_row("-", "-", "No recent events", "-")
    title = f"Recent Events ({len(visible)} shown"
    if len(events) > len(visible):
        title += f" of {len(events)}"
    title += ")"
    return Panel(table, title=title, padding=(0, 1))


def _pool_panel(snapshot: CoordinatorSnapshot) -> Panel | None:
    result = next(
        (
            item
            for item in reversed(snapshot.scheduler_results)
            if item.request.kind is SchedulerQueryKind.POOL
        ),
        None,
    )
    if result is None:
        return None
    summary = result.summary.to_json_dict()
    text = Text()
    if not summary:
        text.append("No pool capacity was reported", style="dim")
    else:
        for name in (
            "machines",
            "total_slots",
            "idle_slots",
            "total_cpus",
            "idle_cpus",
            "total_memory_mb",
            "idle_memory_mb",
            "total_gpus",
            "idle_gpus",
        ):
            if name in summary and summary[name] is not None:
                if text:
                    text.append("  ")
                text.append(f"{name.replace('_', ' ')} {summary[name]}")
    return Panel(text, title="HTCondor Pool", padding=(0, 1))


def _stats_panel(stats: WorkflowStats | None) -> Panel | None:
    if stats is None:
        return None
    table = Table.grid(expand=True, padding=(0, 2))
    table.add_column(ratio=1)
    table.add_column(ratio=1)
    table.add_row(
        f"compute jobs {stats.compute_jobs}",
        f"wall time {_duration(stats.wall_time)}",
    )
    table.add_row(
        f"compute time {_duration(stats.total_compute_time)}",
        "parallelism -"
        if stats.parallelism is None
        else f"parallelism {stats.parallelism:.2f}",
    )
    table.add_row(
        "peak RSS -"
        if stats.peak_maxrss_kb is None
        else f"peak RSS {stats.peak_maxrss_kb / 1024:.1f} MiB",
        f"pending evidence {stats.pending_jobs}",
    )
    coverage = getattr(stats, "scheduler_coverage", ())
    table.add_row(
        "authoritative final "
        + ("yes" if getattr(stats, "authoritative_final", False) else "no"),
        "scheduler coverage "
        + (
            ", ".join(f"{item.kind.value}:{item.health.value}" for item in coverage)
            if coverage
            else "none"
        ),
    )
    return Panel(table, title="Statistics", padding=(0, 1))


def _bounded_lines(lines: list[Text], limit: int) -> list[Text]:
    if len(lines) <= limit:
        return lines
    omitted = len(lines) - max(1, limit - 1)
    return lines[: max(1, limit - 1)] + [
        Text(f"... {omitted} additional line(s) omitted", style="dim")
    ]


def _diagnostics_panel(
    batch: DiagnosticsBatch | None, options: DisplayOptions
) -> Panel | None:
    if batch is None:
        return None
    lines: list[Text] = []
    if batch.stall is not None:
        active = batch.stall.event.value == "stall_detected"
        lines.append(
            Text(
                f"{'STALL' if active else 'STALL RESOLVED'}: {batch.stall.summary}",
                style="bold red" if active else "green",
            )
        )
    for finding in batch.findings[:20]:
        lines.append(
            Text(
                f"{finding.exec_job_id}: {finding.summary} [{finding.code}]",
                style=("red" if finding.severity.value == "error" else "yellow"),
            )
        )
        lines.extend(
            Text(f"  Suggestion: {suggestion}", style="cyan")
            for suggestion in finding.suggestions[:3]
        )
    for error in batch.errors:
        lines.append(Text(error, style="yellow"))
    if not lines:
        lines.append(Text("No active diagnostic findings", style="dim"))
    return Panel(
        Group(*_bounded_lines(lines, options.analysis_line_limit)),
        title="Diagnostics",
        padding=(0, 1),
    )


def _why_idle_panel(
    analysis: IdleAnalysis | None, options: DisplayOptions
) -> Panel | None:
    if analysis is None:
        return None
    lines: list[Text] = []
    lines.append(
        Text(
            f"queued {analysis.queued_jobs}  unsubmitted {analysis.unsubmitted_jobs}",
            style="bold",
        )
    )
    if analysis.idle_jobs:
        lines.append(
            Text(
                "idle jobs: "
                + ", ".join(
                    f"{job.exec_job_id}({job.state.value})"
                    for job in analysis.idle_jobs[:8]
                ),
                style="dim",
            )
        )
    capacity = []
    for label, value in (
        ("machines", analysis.pool_machines),
        ("slots", analysis.pool_total_slots),
        ("idle slots", analysis.pool_idle_slots),
        ("cpus", analysis.pool_total_cpus),
        ("idle cpus", analysis.pool_idle_cpus),
        ("memory MiB", analysis.pool_total_memory_mb),
        ("idle memory MiB", analysis.pool_idle_memory_mb),
        ("gpus", analysis.pool_total_gpus),
        ("idle gpus", analysis.pool_idle_gpus),
    ):
        if value is not None:
            capacity.append(f"{label} {value}")
    if capacity:
        lines.append(Text("pool: " + "  ".join(capacity), style="dim"))
    if (
        analysis.negotiation_cycle_seconds is not None
        or analysis.negotiation_matches is not None
    ):
        lines.append(
            Text(
                "negotiation: "
                f"cycle {analysis.negotiation_cycle_seconds}s  "
                f"matches {analysis.negotiation_matches}",
                style="dim",
            )
        )
    if analysis.priority is not None:
        lines.append(
            Text(
                f"priority {analysis.priority.principal}: "
                f"effective {analysis.priority.effective_priority}  "
                f"real {analysis.priority.real_priority}  "
                f"used {analysis.priority.resources_used}  "
                f"better users {analysis.priority.better_priority_users}",
                style="dim",
            )
        )
    lines.extend(Text(item, style="yellow") for item in analysis.findings[:20])
    lines.extend(
        Text(f"Suggestion: {item}", style="cyan") for item in analysis.suggestions[:10]
    )
    for mismatch in analysis.requirement_mismatches[:10]:
        lines.append(Text(f"{mismatch.exec_job_id}: {mismatch.reason}", style="yellow"))
    for source in analysis.sources:
        lines.append(
            Text(
                f"{source.kind.value}: {source.disposition.value}",
                style="dim",
            )
        )
    return Panel(
        Group(*_bounded_lines(lines, options.analysis_line_limit)),
        title="Why Idle",
        padding=(0, 1),
    )


def render_dashboard(
    context: DisplayContext,
    snapshot: CoordinatorSnapshot,
    analysis: DisplayAnalysis | None = None,
    options: DisplayOptions | None = None,
) -> Group:
    """Return one source-free, immutable dashboard render tree."""

    selected_analysis = analysis or DisplayAnalysis()
    selected_options = options or DisplayOptions()
    roster = (
        _job_roster(snapshot.effective.jobs, selected_options)
        if snapshot.effective is not None
        else None
    )
    panels: list[RenderableType] = [
        _header(context, snapshot, selected_options),
        _health_panel(snapshot, selected_options),
        _status_panel(snapshot, selected_analysis, roster),
    ]
    # Evidence and final summaries precede row-heavy detail so Rich clipping on
    # a bounded alternate screen cannot hide diagnostics with no scroll path.
    for panel in (
        _diagnostics_panel(selected_analysis.diagnostics, selected_options),
        _why_idle_panel(
            selected_analysis.why_idle
            or (
                selected_analysis.diagnostics.idle
                if selected_analysis.diagnostics is not None
                else None
            ),
            selected_options,
        ),
        _stats_panel(selected_analysis.stats),
        _pool_panel(snapshot),
    ):
        if panel is not None:
            panels.append(panel)
    if selected_analysis.errors:
        panels.append(
            Panel(
                Group(
                    *(Text(error, style="yellow") for error in selected_analysis.errors)
                ),
                title="Analysis Health",
                padding=(0, 1),
            )
        )
    job_panel = _job_panel(snapshot, selected_options, roster)
    if job_panel is not None:
        panels.append(job_panel)
    panels.append(_events_panel(snapshot, selected_options))
    return Group(*panels)


def render_text(
    context: DisplayContext,
    snapshot: CoordinatorSnapshot,
    analysis: DisplayAnalysis | None = None,
    options: DisplayOptions | None = None,
    *,
    width: int = 120,
) -> str:
    """Render deterministic, non-ANSI text for one-shot output and tests."""

    stream = io.StringIO()
    selected = options or DisplayOptions(live=False, width=width)
    if selected.width != width or selected.live:
        selected = DisplayOptions(
            show_all_jobs=selected.show_all_jobs,
            sort_by_activity=selected.sort_by_activity,
            event_limit=selected.event_limit,
            job_row_limit=selected.job_row_limit,
            analysis_line_limit=selected.analysis_line_limit,
            width=width,
            condor_enabled=selected.condor_enabled,
            expected_scheduler_sources=selected.expected_scheduler_sources,
            live=False,
            final=selected.final,
        )
    console = Console(
        file=stream,
        force_terminal=False,
        color_system=None,
        width=width,
        legacy_windows=False,
    )
    console.print(render_dashboard(context, snapshot, analysis, selected))
    return stream.getvalue()
