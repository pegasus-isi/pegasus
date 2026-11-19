"""JSON-emitting WP7 checkpoint, replay, and synthetic-stream scale probe."""

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
import os
import platform
import resource
import subprocess
import sys
import time
from functools import partial
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

from Pegasus.monitor.event_log import EventLogWriter
from Pegasus.monitor.models import (
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    SnapshotEpoch,
    WorkflowIdentity,
)
from Pegasus.monitor.replay import ReplayEngine
from Pegasus.monitor.stampede import StampedeReader

WORKFLOW = WorkflowIdentity(WORKFLOW_UUID, WORKFLOW_UUID)
CHECKPOINT_INTERVAL_SECONDS = 300.0
REPLAY_RECORD_LIMIT_BYTES = 256 * 1024 * 1024


def _rss_bytes() -> int:
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum if sys.platform == "darwin" else maximum * 1024)


def _environment() -> dict[str, object]:
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "pid": os.getpid(),
    }


def _measure(function: Callable[[], Any]) -> tuple[dict[str, float | int], Any]:
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    before_rss = _rss_bytes()
    value = function()
    wall = time.perf_counter() - started_wall
    cpu = time.process_time() - started_cpu
    peak = _rss_bytes()
    return (
        {
            "wall_seconds": wall,
            "cpu_seconds": cpu,
            "average_logical_cores": cpu / wall if wall > 0 else 0.0,
            "peak_rss_bytes": peak,
            "rss_growth_bytes": max(0, peak - before_rss),
        },
        value,
    )


def _refresh_snapshot(workload: GeneratedWorkload):
    reader = StampedeReader(workload.database_path, WORKFLOW)
    result = reader.refresh(
        DBRefreshRequest(
            workflow=WORKFLOW,
            next_epoch=SnapshotEpoch(1),
            mode=DBRefreshMode.FULL_REBOOTSTRAP,
            clock=ClockSample(1_000.0, 1_000.0),
        )
    )
    if result.snapshot is None:
        raise RuntimeError(
            f"WP7 scale bootstrap failed: {result.health.error_code}: "
            f"{result.health.detail}"
        )
    return result.snapshot


def _checkpoint_line_lengths(path: Path) -> list[int]:
    lengths: list[int] = []
    current = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            start = 0
            while True:
                newline = block.find(b"\n", start)
                if newline < 0:
                    current += len(block) - start
                    break
                current += newline + 1 - start
                lengths.append(current)
                current = 0
                start = newline + 1
    if current:
        lengths.append(current)
    return lengths


def _serialize_checkpoints(path: Path, snapshot) -> dict[str, object]:
    writer = EventLogWriter(
        path,
        WORKFLOW,
        "wp9b-scale",
        min_free_mb=0,
        checkpoint_interval=CHECKPOINT_INTERVAL_SECONDS,
        stream_id="wp7-scale-stream",
    )
    try:
        writer.record_snapshot(snapshot, recorded_at_epoch=1_000.0)
        initial_size = path.stat().st_size
        initial_sequence = writer.status.sequence

        writer.record_snapshot(snapshot, recorded_at_epoch=1_299.999)
        before_due_size = path.stat().st_size
        before_due_sequence = writer.status.sequence

        writer.record_snapshot(snapshot, recorded_at_epoch=1_300.0)
        periodic_size = path.stat().st_size
        periodic_sequence = writer.status.sequence
    finally:
        writer.close()

    line_lengths = _checkpoint_line_lengths(path)
    if len(line_lengths) != 3:
        raise RuntimeError(f"expected header plus two checkpoints, got {line_lengths}")
    return {
        "interval_seconds": CHECKPOINT_INTERVAL_SECONDS,
        "initial_sequence": initial_sequence,
        "before_due_sequence": before_due_sequence,
        "periodic_sequence": periodic_sequence,
        "initial_prefix_bytes": initial_size,
        "before_due_bytes": before_due_size,
        "periodic_stream_bytes": periodic_size,
        "periodic_checkpoint_bytes": periodic_size - before_due_size,
        "header_bytes": line_lengths[0],
        "initial_checkpoint_bytes": line_lengths[1],
        "periodic_checkpoint_line_bytes": line_lengths[2],
        "max_record_bytes": max(line_lengths),
    }


def _append_enrichment_stream(
    path: Path,
    *,
    stream_id: str,
    snapshot_epoch: int,
    first_sequence: int,
    count: int,
) -> int:
    encoded_stream_id = json.dumps(stream_id, separators=(",", ":"))
    suffix = (
        f',"stream_id":{encoded_stream_id},"snapshot_epoch":{snapshot_epoch},'
        '"recorded_at_epoch":1301.0,"source":"condor_queue",'
        '"target":{"exec_job_id":"scale_ID0000000"},'
        '"payload":{"job_status":2},"expires_at_epoch":null}\n'
    ).encode()
    prefix = b'{"schema_version":1,"record_type":"enrichment","sequence":'
    with path.open("ab", buffering=1024 * 1024) as stream:
        block = bytearray()
        for offset in range(count):
            block.extend(prefix)
            block.extend(str(first_sequence + offset).encode("ascii"))
            block.extend(suffix)
            if len(block) >= 4 * 1024 * 1024:
                stream.write(block)
                block.clear()
        if block:
            stream.write(block)
    return path.stat().st_size


def _replay(path: Path) -> dict[str, object]:
    result = ReplayEngine(
        path,
        speed=0,
        max_record_bytes=REPLAY_RECORD_LIMIT_BYTES,
        retain_frames=False,
    ).replay()
    if not result.complete or result.snapshot is None:
        raise RuntimeError("WP7 scale replay did not finish from a complete checkpoint")
    return {
        "complete": result.complete,
        "job_count": len(result.snapshot.jobs),
        "retained_frames": len(result.frames),
        "current_enrichments": len(result.enrichments),
        "ignored_records": result.ignored_records,
        "stream_replacements": result.stream_replacements,
        "trailing_bytes": len(result.trailing_bytes),
    }


def _run_local_phase(
    config: ScaleConfig, workload: GeneratedWorkload
) -> dict[str, object]:
    observed = validate_workload(workload, config)
    started_wall = time.perf_counter()
    started_cpu = time.process_time()

    load_metrics, snapshot = _measure(lambda: _refresh_snapshot(workload))
    expected_jobs = len(snapshot.jobs)
    snapshot_epoch = snapshot.epoch.value
    stream_path = workload.database_path.parent / "wp7-scale-events.jsonl"

    serialize = partial(_serialize_checkpoints, stream_path, snapshot)
    checkpoint_metrics, checkpoint = _measure(serialize)
    del serialize
    event_count = config.prefilled_tail_lines
    generation_metrics, stream_size = _measure(
        lambda: _append_enrichment_stream(
            stream_path,
            stream_id="wp7-scale-stream",
            snapshot_epoch=snapshot_epoch,
            first_sequence=int(checkpoint["periodic_sequence"]) + 1,
            count=event_count,
        )
    )

    total_wall = time.perf_counter() - started_wall
    total_cpu = time.process_time() - started_cpu
    peak_rss = _rss_bytes()
    event_bytes = stream_size - int(checkpoint["periodic_stream_bytes"])
    return {
        "schema_version": 1,
        "mode": config.mode,
        "config": config.to_dict(),
        "environment": _environment(),
        "fixture_sha256": workload.fixture_sha256,
        "observed": observed,
        "checkpoint": checkpoint,
        "synthetic_stream": {
            "record_type": "bounded_replacement_enrichment",
            "event_count": event_count,
            "stream_bytes": stream_size,
            "event_bytes": event_bytes,
            "bytes_per_event": event_bytes / event_count,
        },
        "measurements": {
            "snapshot_load": load_metrics,
            "checkpoint_serialization": checkpoint_metrics,
            "synthetic_stream_generation": generation_metrics,
            "total_wall_seconds": total_wall,
            "total_cpu_seconds": total_cpu,
            "total_average_logical_cores": (
                total_cpu / total_wall if total_wall > 0 else 0.0
            ),
            "process_peak_rss_bytes": peak_rss,
        },
        "expected_jobs": expected_jobs,
    }


def _run_replay_phase(
    config: ScaleConfig, workload: GeneratedWorkload
) -> dict[str, object]:
    validate_workload(workload, config)
    stream_path = workload.database_path.parent / "wp7-scale-events.jsonl"
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    replay_metrics, replay = _measure(lambda: _replay(stream_path))
    total_wall = time.perf_counter() - started_wall
    total_cpu = time.process_time() - started_cpu
    return {
        "environment": _environment(),
        "replay": replay,
        "measurements": {
            "replay": replay_metrics,
            "total_wall_seconds": total_wall,
            "total_cpu_seconds": total_cpu,
            "process_peak_rss_bytes": _rss_bytes(),
        },
    }


def _run_phase_subprocess(
    config: ScaleConfig,
    workload: GeneratedWorkload,
    phase: str,
) -> dict[str, object]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--phase",
        phase,
        "--mode",
        config.mode,
        "--workspace",
        str(workload.database_path.parent),
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
            f"WP7 scale {phase} worker failed ({completed.returncode}): "
            f"{completed.stderr.strip()}"
        )
    return json.loads(completed.stdout)


def run_subprocess_gate(config: ScaleConfig, workspace: Path) -> dict[str, object]:
    workload = generate_workload(workspace, config)
    local = _run_phase_subprocess(config, workload, "local")
    replay = _run_phase_subprocess(config, workload, "replay")
    local_measurements = local["measurements"]
    replay_measurements = replay["measurements"]
    local_peak = int(local_measurements["process_peak_rss_bytes"])
    replay_peak = int(replay_measurements["process_peak_rss_bytes"])
    peak_rss = max(local_peak, replay_peak)
    total_wall = float(local_measurements["total_wall_seconds"]) + float(
        replay_measurements["total_wall_seconds"]
    )
    total_cpu = float(local_measurements["total_cpu_seconds"]) + float(
        replay_measurements["total_cpu_seconds"]
    )
    average_logical_cores = total_cpu / total_wall if total_wall > 0 else 0.0
    checkpoint = local["checkpoint"]
    max_record_bytes = int(checkpoint["max_record_bytes"])
    return {
        **{key: value for key, value in local.items() if key != "measurements"},
        "replay": replay["replay"],
        "phase_environments": {
            "local": local["environment"],
            "replay": replay["environment"],
        },
        "measurements": {
            "snapshot_load": local_measurements["snapshot_load"],
            "checkpoint_serialization": local_measurements["checkpoint_serialization"],
            "synthetic_stream_generation": local_measurements[
                "synthetic_stream_generation"
            ],
            "replay": replay_measurements["replay"],
            "total_wall_seconds": total_wall,
            "total_cpu_seconds": total_cpu,
            "total_average_logical_cores": average_logical_cores,
            "process_peak_rss_bytes": peak_rss,
            "local_process_peak_rss_bytes": local_peak,
            "replay_process_peak_rss_bytes": replay_peak,
        },
        "budgets": {
            "checkpoint_record_within_replay_limit": {
                "value": max_record_bytes,
                "limit": REPLAY_RECORD_LIMIT_BYTES,
                "passed": max_record_bytes <= REPLAY_RECORD_LIMIT_BYTES,
            },
            "local_process_peak_rss_below_1_gib": {
                "value": local_peak,
                "limit": 1024**3,
                "passed": local_peak < 1024**3,
            },
            "replay_process_peak_rss_below_1_gib": {
                "value": replay_peak,
                "limit": 1024**3,
                "passed": replay_peak < 1024**3,
            },
            "average_cpu_below_one_logical_core": {
                "value": average_logical_cores,
                "limit": 1.0,
                "passed": average_logical_cores < 1.0,
            },
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fast", "default", "full"))
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--phase", choices=("local", "replay"))
    parser.add_argument("--fixture-sha256")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.phase is None or args.workspace is None or not args.fixture_sha256:
        raise SystemExit("--phase requires --workspace and --fixture-sha256")
    config = scale_config(args.mode)
    workload = GeneratedWorkload(
        args.workspace / "scale.stampede.db",
        args.workspace / "jobstate.log",
        args.fixture_sha256,
        json.loads((args.workspace / "fixture-manifest.json").read_text()),
    )
    if args.phase == "local":
        payload = _run_local_phase(config, workload)
    else:
        payload = _run_replay_phase(config, workload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
