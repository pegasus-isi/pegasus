"""JSON-emitting scale, latency, and workflow-writer impact runner."""

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
import json
import math
import os
import platform
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

if __package__:
    from .workload import (
        WORKFLOW_UUID,
        GeneratedWorkload,
        ScaleConfig,
        generate_workload,
        scale_config,
        validate_workload,
    )
else:  # Direct execution by the separately measured worker subprocess.
    sys.path.insert(0, str(Path(__file__).parent))
    from workload import (  # type: ignore[no-redef]
        WORKFLOW_UUID,
        GeneratedWorkload,
        ScaleConfig,
        generate_workload,
        scale_config,
        validate_workload,
    )

from Pegasus.monitor.coordinator import CoordinatorSnapshot
from Pegasus.monitor.diagnostics import DiagnosticsEngine
from Pegasus.monitor.display import (
    DisplayContext,
    DisplayOptions,
    render_text,
)
from Pegasus.monitor.live_events import LiveEventTail
from Pegasus.monitor.models import (
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    EffectiveSnapshot,
    SnapshotEpoch,
    TailPollRequest,
    WorkflowIdentity,
)
from Pegasus.monitor.reconcile import Reconciler
from Pegasus.monitor.stampede import StampedeReader
from Pegasus.monitor.stats import compute_workflow_stats

WORKFLOW = WorkflowIdentity(WORKFLOW_UUID, WORKFLOW_UUID)


def _quantile(samples: list[float], probability: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil(probability * len(ordered)) - 1)
    return ordered[index]


def _sample_summary(samples: list[float]) -> dict[str, object]:
    return {
        "count": len(samples),
        "samples": samples,
        "min": min(samples, default=0.0),
        "median": _quantile(samples, 0.5),
        "p95": _quantile(samples, 0.95),
        "max": max(samples, default=0.0),
    }


def _rss_bytes() -> int:
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB; macOS and the BSDs report bytes.
    return int(maximum if sys.platform == "darwin" else maximum * 1024)


def _refresh_request(sample: int, mode: DBRefreshMode, prior=None) -> DBRefreshRequest:
    return DBRefreshRequest(
        workflow=WORKFLOW,
        next_epoch=SnapshotEpoch(sample + 1),
        mode=mode,
        clock=ClockSample(2_000.0 + sample, 1_000.0 + sample),
        prior_generation=prior,
        recent_transition_limit=256,
        recent_workflow_transition_limit=64,
    )


def _coordinator_snapshot(
    effective: EffectiveSnapshot, sequence: int, reconciler: Reconciler
) -> CoordinatorSnapshot:
    return CoordinatorSnapshot(
        sequence=sequence,
        clock=ClockSample(
            effective.published_at_epoch, effective.published_at_monotonic
        ),
        effective=effective,
        source_health=effective.source_health,
        scheduler_results=(),
        pending_tail_events=effective.pending_overlay_count,
        unconfirmed_tail_events=(),
        last_tail_event_age=None,
        semantic_progress=reconciler.semantic_progress,
        latest_effective_event=effective.events[-1] if effective.events else None,
        has_authoritative_base=True,
        authoritative_complete=False,
    )


def _display_context(workspace: Path, database: Path, tail: Path) -> DisplayContext:
    return DisplayContext(
        label="scale-gate",
        owner="acceptance",
        planner_version="scale-fixture",
        planning_timestamp="generated",
        submit_dir=workspace,
        recorded_submit_dir=workspace,
        basedir=workspace,
        recorded_basedir=workspace,
        root_submit_dir=workspace,
        jobstate_path=tail,
        database_path=database,
        wf_uuid=WORKFLOW_UUID,
        root_wf_uuid=WORKFLOW_UUID,
        dag_name="scale-0.dag",
    )


def _timed(function: Callable[[], Any], samples: int) -> tuple[list[float], Any]:
    durations: list[float] = []
    result = None
    for _ in range(samples):
        started = time.perf_counter()
        result = function()
        durations.append(time.perf_counter() - started)
    return durations, result


def _run_probes(
    config: ScaleConfig,
    reconciler: Reconciler,
    workspace: Path,
    database: Path,
    tail: Path,
) -> tuple[dict[str, object], CoordinatorSnapshot]:
    epoch = 10_000

    def publish() -> CoordinatorSnapshot:
        nonlocal epoch
        epoch += 1
        effective = reconciler.build_snapshot(
            SnapshotEpoch(epoch), ClockSample(3_000.0 + epoch, 2_000.0 + epoch)
        )
        if effective is None:
            raise AssertionError("scale publication lost its authoritative base")
        return _coordinator_snapshot(effective, epoch, reconciler)

    publication_samples, publication = _timed(publish, config.probe_samples)
    context = _display_context(workspace, database, tail)
    options = DisplayOptions(
        live=False, condor_enabled=False, job_row_limit=200, event_limit=15
    )
    display_samples, _ = _timed(
        lambda: render_text(context, publication, options=options, width=120),
        config.probe_samples,
    )
    stats_samples, _ = _timed(
        lambda: compute_workflow_stats(publication), config.probe_samples
    )
    engine = DiagnosticsEngine()
    diagnostics_samples, _ = _timed(
        lambda: engine.analyze(publication), config.probe_samples
    )
    probes = {
        "job_count": len(publication.effective.jobs),
        "publication_seconds": _sample_summary(publication_samples),
        "display_seconds": _sample_summary(display_samples),
        "stats_seconds": _sample_summary(stats_samples),
        "diagnostics_seconds": _sample_summary(diagnostics_samples),
        "hotspots_descending": sorted(
            (
                {
                    "probe": name,
                    "median_seconds": _quantile(values, 0.5),
                    "p95_seconds": _quantile(values, 0.95),
                }
                for name, values in (
                    ("publication", publication_samples),
                    ("display", display_samples),
                    ("stats", stats_samples),
                    ("diagnostics", diagnostics_samples),
                )
            ),
            key=lambda item: item["median_seconds"],
            reverse=True,
        ),
    }
    return probes, publication


def _live_burst(
    config: ScaleConfig,
    reconciler: Reconciler,
    workspace: Path,
    database: Path,
    tail_path: Path,
    start_sequence: int,
) -> tuple[dict[str, object], int]:
    writes: dict[str, float] = {}
    writer_error: list[str] = []
    interval = 0.1
    chunk_size = max(1, int(config.burst_rate * interval))
    context = _display_context(workspace, database, tail_path)
    options = DisplayOptions(
        live=False, condor_enabled=False, job_row_limit=200, event_limit=15
    )
    latencies: list[float] = []
    poll_samples: list[float] = []
    ingest_samples: list[float] = []
    publication_samples: list[float] = []
    display_samples: list[float] = []
    call_counts = {"tail_poll": 0, "publication": 0, "display": 0}

    with LiveEventTail(tail_path) as tail:

        def write_burst() -> None:
            try:
                with tail_path.open("ab", buffering=0) as stream:
                    emitted = 0
                    while emitted < config.burst_lines:
                        count = min(chunk_size, config.burst_lines - emitted)
                        block = bytearray()
                        written = time.perf_counter()
                        for offset in range(count):
                            index = emitted + offset
                            exec_job_id = f"live_scale_{index:07d}"
                            writes[exec_job_id] = written
                            block.extend(
                                (
                                    f"{5_000 + index} {exec_job_id} SUBMIT "
                                    f"{900_000 + index}.0 local - "
                                    f"{900_000 + index}\n"
                                ).encode()
                            )
                        stream.write(block)
                        emitted += count
                        if emitted < config.burst_lines:
                            time.sleep(interval)
            except Exception as error:  # pragma: no cover - surfaced below
                writer_error.append(f"{type(error).__name__}: {error}")

        producer = threading.Thread(target=write_burst, name="scale-tail-writer")
        producer.start()
        seen: set[str] = set()
        sequence = start_sequence
        deadline = time.monotonic() + max(5.0, config.burst_lines / 100.0)
        while producer.is_alive() or len(seen) < config.burst_lines:
            polled_at = time.time()
            monotonic = time.perf_counter()
            poll_started = time.perf_counter()
            result = tail.poll(
                TailPollRequest(
                    workflow=WORKFLOW,
                    base_db_generation=reconciler.database_generation,
                    clock=ClockSample(polled_at, monotonic),
                    max_bytes=4 * 1024 * 1024,
                    max_lines=max(5_000, config.burst_lines),
                )
            )
            poll_samples.append(time.perf_counter() - poll_started)
            call_counts["tail_poll"] += 1
            if result.job_events:
                ingest_started = time.perf_counter()
                reconciler.ingest_tail(result)
                ingest_samples.append(time.perf_counter() - ingest_started)
                sequence += 1
                publication_started = time.perf_counter()
                effective = reconciler.build_snapshot(
                    SnapshotEpoch(20_000 + sequence),
                    ClockSample(time.time(), time.perf_counter()),
                )
                if effective is None:
                    raise AssertionError("live burst lost its authoritative base")
                publication = _coordinator_snapshot(effective, sequence, reconciler)
                publication_samples.append(time.perf_counter() - publication_started)
                call_counts["publication"] += 1
                display_started = time.perf_counter()
                render_text(context, publication, options=options, width=120)
                display_samples.append(time.perf_counter() - display_started)
                call_counts["display"] += 1
                displayed = time.perf_counter()
                for event in result.job_events:
                    seen.add(event.exec_job_id)
                    latencies.append(displayed - writes[event.exec_job_id])
            if len(seen) >= config.burst_lines and not producer.is_alive():
                break
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"live burst observed {len(seen)}/{config.burst_lines} lines"
                )
            time.sleep(0.002)
        producer.join(timeout=2.0)
        if producer.is_alive():
            raise RuntimeError("tail writer did not terminate")
    if writer_error:
        raise RuntimeError(writer_error[0])
    return (
        {
            "configured_lines_per_second": config.burst_rate,
            "lines": config.burst_lines,
            "observed_lines": len(latencies),
            "display_latency_seconds": _sample_summary(latencies),
            "stage_seconds": {
                "tail_poll": _sample_summary(poll_samples),
                "reconciler_ingest": _sample_summary(ingest_samples),
                "publication": _sample_summary(publication_samples),
                "display": _sample_summary(display_samples),
            },
            "call_counts": call_counts,
        },
        sequence,
    )


def _writer_command(
    database: Path, operations: int, duration_seconds: float
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--writer",
        str(database),
        str(operations),
        str(duration_seconds),
    ]


def _one_writer(
    database: Path, operations: int, duration_seconds: float
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        _writer_command(database, operations, duration_seconds),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _finish_writer(process: subprocess.Popen[str]) -> float:
    stdout, stderr = process.communicate(timeout=120)
    if process.returncode:
        raise RuntimeError(f"writer failed ({process.returncode}): {stderr.strip()}")
    payload = json.loads(stdout)
    if payload["operations"] <= 0:
        raise RuntimeError("writer performed no work")
    # The writer's own monotonic interval excludes delayed parent observation
    # when one synchronous monitor refresh outlasts the workflow subprocess.
    return float(payload["elapsed_seconds"])


def _set_journal_mode(database: Path, mode: str) -> str:
    connection = sqlite3.connect(database, timeout=30.0)
    try:
        if mode == "delete":
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        selected = str(connection.execute(f"PRAGMA journal_mode={mode}").fetchone()[0])
        connection.commit()
        return selected.lower()
    finally:
        connection.close()


def _writer_impact(
    database: Path, config: ScaleConfig, journal_mode: str
) -> dict[str, object]:
    production_cadence = 2.0
    selected = _set_journal_mode(database, journal_mode)
    if selected != journal_mode:
        raise RuntimeError(f"requested journal mode {journal_mode}, got {selected}")
    reader = StampedeReader(database, WORKFLOW, busy_timeout_seconds=0.05)
    current = reader.refresh(_refresh_request(50_000, DBRefreshMode.FULL_REBOOTSTRAP))
    if current.snapshot is None:
        raise RuntimeError(
            f"writer-impact bootstrap failed: {current.health.error_code}"
        )

    def baseline() -> float:
        process = _one_writer(
            database, config.writer_operations, config.writer_duration_seconds
        )
        return _finish_writer(process)

    def monitored(
        phase_offset: float,
        *,
        stress: bool = False,
        duration_seconds: float | None = None,
    ) -> tuple[float, int, list[float]]:
        nonlocal current
        process = _one_writer(
            database,
            config.writer_operations,
            (
                config.writer_duration_seconds
                if duration_seconds is None
                else duration_seconds
            ),
        )
        calls = 0
        refreshes: list[float] = []
        next_refresh = time.perf_counter() + phase_offset
        while process.poll() is None:
            now = time.perf_counter()
            if not stress and now < next_refresh:
                time.sleep(min(0.005, next_refresh - now))
                continue
            before = time.perf_counter()
            result = reader.refresh(
                _refresh_request(
                    50_001 + calls,
                    DBRefreshMode.CURRENT_SNAPSHOT,
                    current.generation,
                )
            )
            refreshes.append(time.perf_counter() - before)
            calls += 1
            if result.snapshot is not None:
                current = result
            if not stress:
                next_refresh += production_cadence
        return _finish_writer(process), calls, refreshes

    for _ in range(config.writer_warmups):
        baseline()
        monitored(0.0)

    baseline_samples: list[float] = []
    monitored_samples: list[float] = []
    refresh_samples: list[float] = []
    refresh_calls = 0
    phase_offsets: list[float] = []
    pair_order: list[str] = []
    for trial in range(config.writer_trials):
        phase = production_cadence * (trial + 0.5) / config.writer_trials
        phase_offsets.append(phase)
        if trial % 2 == 0:
            pair_order.append("baseline_then_monitor")
            baseline_elapsed = baseline()
            elapsed, calls, refreshes = monitored(phase)
        else:
            pair_order.append("monitor_then_baseline")
            elapsed, calls, refreshes = monitored(phase)
            baseline_elapsed = baseline()
        baseline_samples.append(baseline_elapsed)
        monitored_samples.append(elapsed)
        refresh_calls += calls
        refresh_samples.extend(refreshes)
    baseline_median = _quantile(baseline_samples, 0.5)
    monitored_median = _quantile(monitored_samples, 0.5)
    impact = (
        (monitored_median - baseline_median) / baseline_median
        if baseline_median > 0
        else float("inf")
    )
    stress_process = _one_writer(database, config.writer_operations, 0.0)
    stress_baseline = _finish_writer(stress_process)
    stress_elapsed, stress_calls, stress_refreshes = monitored(
        0.0, stress=True, duration_seconds=0.0
    )
    stress_impact = (
        (stress_elapsed - stress_baseline) / stress_baseline
        if stress_baseline > 0
        else float("inf")
    )
    return {
        "journal_mode": selected,
        "warmups": config.writer_warmups,
        "paired_trials": config.writer_trials,
        "writer_operations_per_trial": config.writer_operations,
        "target_writer_duration_seconds": config.writer_duration_seconds,
        "production_cadence_seconds": production_cadence,
        "phase_offsets_seconds": phase_offsets,
        "pair_order": pair_order,
        "baseline_seconds": _sample_summary(baseline_samples),
        "with_monitor_seconds": _sample_summary(monitored_samples),
        "median_makespan_impact_fraction": impact,
        "monitor_refresh_calls": refresh_calls,
        "monitor_refresh_seconds": _sample_summary(refresh_samples),
        "tight_loop_stress": {
            "baseline_seconds": stress_baseline,
            "with_monitor_seconds": stress_elapsed,
            "makespan_impact_fraction": stress_impact,
            "monitor_refresh_calls": stress_calls,
            "monitor_refresh_seconds": _sample_summary(stress_refreshes),
        },
    }


def _environment() -> dict[str, object]:
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "sqlite": sqlite3.sqlite_version,
        "pid": os.getpid(),
    }


def _budget(value: float, limit: float, *, enforced: bool = True) -> dict[str, object]:
    return {
        "value": value,
        "limit": limit,
        "passed": value < limit,
        "enforced": enforced,
    }


def _runtime_workload(workload: GeneratedWorkload, label: str) -> GeneratedWorkload:
    """Copy validated base inputs so measurement never invalidates their hash."""

    database = workload.database_path.parent / f"runtime-{label}.stampede.db"
    jobstate = workload.jobstate_path.parent / f"runtime-{label}.jobstate.log"
    for suffix in ("", "-shm", "-wal", "-journal"):
        Path(f"{database}{suffix}").unlink(missing_ok=True)
    jobstate.unlink(missing_ok=True)
    shutil.copyfile(workload.database_path, database)
    shutil.copyfile(workload.jobstate_path, jobstate)
    return GeneratedWorkload(
        database,
        jobstate,
        workload.fixture_sha256,
        workload.manifest,
    )


def run_worker(
    config: ScaleConfig,
    workload: GeneratedWorkload,
) -> dict[str, object]:
    observed_cardinalities = validate_workload(workload, config)
    workload = _runtime_workload(workload, "worker")
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    reader = StampedeReader(workload.database_path, WORKFLOW, busy_timeout_seconds=0.25)
    refresh_samples: list[float] = []
    transaction_samples: list[float] = []
    result = None
    for sample in range(config.refresh_samples):
        mode = (
            DBRefreshMode.FULL_REBOOTSTRAP
            if sample == 0
            else DBRefreshMode.CURRENT_SNAPSHOT
        )
        request = _refresh_request(
            sample, mode, None if result is None else result.generation
        )
        before = time.perf_counter()
        result = reader.refresh(request)
        refresh_samples.append(time.perf_counter() - before)
        transaction_samples.append(reader.last_transaction_seconds)
        if result.snapshot is None:
            raise RuntimeError(
                f"Stampede refresh {sample} failed: {result.health.error_code}: "
                f"{result.health.detail}"
            )
    assert result is not None and result.snapshot is not None
    reconciler = Reconciler(
        WORKFLOW,
        max_pending_events=max(4_096, config.burst_lines * 2),
        max_pending_bytes=max(4 * 1024 * 1024, config.burst_lines * 256),
    )
    reconciler.apply_database(result)
    probes, publication = _run_probes(
        config,
        reconciler,
        workload.database_path.parent,
        workload.database_path,
        workload.jobstate_path,
    )
    live, sequence = _live_burst(
        config,
        reconciler,
        workload.database_path.parent,
        workload.database_path,
        workload.jobstate_path,
        publication.sequence,
    )
    monitor_elapsed = time.perf_counter() - started_wall
    monitor_cpu_seconds = time.process_time() - started_cpu
    cpu_cores = (
        monitor_cpu_seconds / monitor_elapsed if monitor_elapsed > 0 else float("inf")
    )
    monitor_rss = _rss_bytes()
    writer = {
        mode: _writer_impact(workload.database_path, config, mode)
        for mode in ("wal", "delete")
    }
    total_elapsed = time.perf_counter() - started_wall
    refresh = _sample_summary(refresh_samples)
    transactions = _sample_summary(transaction_samples)
    live_p95 = float(live["display_latency_seconds"]["p95"])
    full = config.mode == "full"
    budgets = {
        "live_display_p95_seconds": _budget(live_p95, 1.0),
        "monitor_average_logical_cores": _budget(cpu_cores, 1.0),
        "monitor_peak_rss_bytes": _budget(monitor_rss, 1024**3),
        "stampede_transaction_p95_seconds": _budget(float(transactions["p95"]), 0.5),
        "stampede_transaction_max_seconds": _budget(float(transactions["max"]), 2.0),
        "wal_writer_median_impact_fraction": _budget(
            float(writer["wal"]["median_makespan_impact_fraction"]),
            0.02,
            enforced=full,
        ),
        "delete_writer_median_impact_fraction": _budget(
            float(writer["delete"]["median_makespan_impact_fraction"]),
            0.02,
            enforced=full,
        ),
    }
    return {
        "schema_version": 1,
        "mode": config.mode,
        "config": config.to_dict(),
        "environment": _environment(),
        "fixture_sha256": workload.fixture_sha256,
        "fixture_manifest": workload.manifest,
        "observed": {
            "database_jobs": len(result.snapshot.jobs),
            "database_attempts": sum(len(job.attempts) for job in result.snapshot.jobs),
            "configured_database_transitions": config.transitions,
            "configured_prefilled_tail_lines": config.prefilled_tail_lines,
            "actual_database_transitions": observed_cardinalities[
                "database_transitions"
            ],
            "actual_tail_lines_before_burst": observed_cardinalities["tail_lines"],
        },
        "stampede_refresh_seconds": refresh,
        "stampede_transaction_seconds": transactions,
        "live_burst": live,
        "probes": probes,
        "writer_impact": writer,
        "resources": {
            "monitor_benchmark_wall_seconds": monitor_elapsed,
            "total_gate_wall_seconds": total_elapsed,
            "monitor_cpu_seconds": monitor_cpu_seconds,
            "monitor_average_logical_cores": cpu_cores,
            "monitor_peak_rss_bytes": monitor_rss,
        },
        "call_counts": {
            "stampede_refresh": config.refresh_samples,
            "tail_poll": live["call_counts"]["tail_poll"],
            "publication": config.probe_samples + live["call_counts"]["publication"],
            "display": config.probe_samples + live["call_counts"]["display"],
            "stats": config.probe_samples,
            "diagnostics": config.probe_samples,
            "writer_trials": 2 * config.writer_trials,
            "writer_warmups": 2 * config.writer_warmups,
        },
        "budgets": budgets,
        "publication_sequence": sequence,
    }


def run_live_probe(
    config: ScaleConfig, workload: GeneratedWorkload
) -> dict[str, object]:
    """Rerun only the append-to-display path against an existing fixture."""

    validate_workload(workload, config)
    workload = _runtime_workload(workload, "live")
    reader = StampedeReader(workload.database_path, WORKFLOW)
    result = reader.refresh(_refresh_request(0, DBRefreshMode.FULL_REBOOTSTRAP))
    if result.snapshot is None:
        raise RuntimeError(f"live-probe bootstrap failed: {result.health.error_code}")
    reconciler = Reconciler(
        WORKFLOW,
        max_pending_events=max(4_096, config.burst_lines * 2),
        max_pending_bytes=max(4 * 1024 * 1024, config.burst_lines * 256),
    )
    reconciler.apply_database(result)
    live, _ = _live_burst(
        config,
        reconciler,
        workload.database_path.parent,
        workload.database_path,
        workload.jobstate_path,
        1,
    )
    return {
        "mode": config.mode,
        "environment": _environment(),
        "fixture_sha256": workload.fixture_sha256,
        "live_burst": live,
    }


def _write_probe(
    database: Path, operations: int, target_duration_seconds: float
) -> dict[str, object]:
    connection = sqlite3.connect(database, timeout=120.0, isolation_level=None)
    started = time.perf_counter()
    try:
        base = time.time_ns()
        for index in range(operations):
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT OR REPLACE INTO writer_probe VALUES (?, ?)",
                (base + index, time.time()),
            )
            connection.execute("COMMIT")
            target = target_duration_seconds * (index + 1) / operations
            remaining = target - (time.perf_counter() - started)
            if remaining > 0:
                time.sleep(remaining)
    finally:
        connection.close()
    return {"operations": operations, "elapsed_seconds": time.perf_counter() - started}


def run_subprocess_gate(config: ScaleConfig, workspace: Path) -> dict[str, object]:
    workload = generate_workload(workspace, config)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--mode",
        config.mode,
        "--workspace",
        str(workspace),
        "--fixture-sha256",
        workload.fixture_sha256,
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=3_600,
    )
    if completed.returncode:
        raise RuntimeError(
            f"scale worker failed ({completed.returncode}): {completed.stderr.strip()}"
        )
    return json.loads(completed.stdout)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fast", "default", "full"))
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--live-only", action="store_true")
    parser.add_argument("--fixture-sha256")
    parser.add_argument(
        "--writer", nargs=3, metavar=("DATABASE", "OPERATIONS", "DURATION")
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.writer:
        database, operations, duration = args.writer
        print(
            json.dumps(_write_probe(Path(database), int(operations), float(duration)))
        )
        return 0
    config = scale_config(args.mode)
    if args.worker or args.live_only:
        if args.workspace is None or not args.fixture_sha256:
            raise SystemExit(
                "--worker/--live-only require --workspace and --fixture-sha256"
            )
        workload = GeneratedWorkload(
            args.workspace / "scale.stampede.db",
            args.workspace / "jobstate.log",
            args.fixture_sha256,
            json.loads((args.workspace / "fixture-manifest.json").read_text()),
        )
        payload = (
            run_live_probe(config, workload)
            if args.live_only
            else run_worker(config, workload)
        )
    else:
        if args.workspace is None:
            with tempfile.TemporaryDirectory(prefix="pegasus-monitor-scale-") as raw:
                payload = run_subprocess_gate(config, Path(raw))
        else:
            payload = run_subprocess_gate(config, args.workspace)
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(encoded + "\n")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
