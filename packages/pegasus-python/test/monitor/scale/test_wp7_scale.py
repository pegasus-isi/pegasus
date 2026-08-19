"""WP7 checkpoint, replay, and synthetic-stream scale verification."""

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

import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from .workload import PROFILES, scale_config, selected_mode
from .wp7_runner import REPLAY_RECORD_LIMIT_BYTES, run_subprocess_gate

from Pegasus.monitor.event_log import read_jsonl
from Pegasus.monitor.models import (
    CheckpointRecord,
    DBJobTransition,
    DBTransitionIdentity,
    JobTransitionRecord,
    SnapshotEpoch,
    StreamHeader,
)
from Pegasus.monitor.replay import ReplayAccumulator

GOLDEN = Path(__file__).parents[1] / "fixtures" / "event_log" / "schema-v1-golden.jsonl"


def _assert_report(payload: dict[str, object], *, strict_cardinality: bool) -> None:
    config = payload["config"]
    observed = payload["observed"]
    checkpoint = payload["checkpoint"]
    synthetic = payload["synthetic_stream"]
    replay = payload["replay"]
    measurements = payload["measurements"]

    assert payload["fixture_sha256"]
    assert payload["environment"]["python"]
    assert observed["database_transitions"] == config["transitions"]
    assert observed["tail_lines"] == config["prefilled_tail_lines"]

    assert checkpoint["interval_seconds"] == 300.0
    assert checkpoint["initial_sequence"] == 1
    assert checkpoint["before_due_sequence"] == 1
    assert checkpoint["periodic_sequence"] == 2
    assert checkpoint["initial_prefix_bytes"] == checkpoint["before_due_bytes"]
    assert checkpoint["periodic_stream_bytes"] > checkpoint["before_due_bytes"]
    assert (
        checkpoint["periodic_checkpoint_bytes"]
        == checkpoint["periodic_checkpoint_line_bytes"]
    )
    assert checkpoint["initial_checkpoint_bytes"] > 0
    assert checkpoint["max_record_bytes"] <= REPLAY_RECORD_LIMIT_BYTES

    assert synthetic["record_type"] == "bounded_replacement_enrichment"
    assert synthetic["event_count"] == config["prefilled_tail_lines"]
    assert synthetic["event_bytes"] > synthetic["event_count"]
    assert synthetic["bytes_per_event"] > 0

    assert replay == {
        "complete": True,
        "current_enrichments": 1,
        "ignored_records": 0,
        "job_count": config["jobs"],
        "retained_frames": 0,
        "stream_replacements": 0,
        "trailing_bytes": 0,
    }
    assert payload["expected_jobs"] == config["jobs"]

    for phase in (
        "snapshot_load",
        "checkpoint_serialization",
        "synthetic_stream_generation",
        "replay",
    ):
        values = measurements[phase]
        assert values["wall_seconds"] >= 0
        assert values["cpu_seconds"] >= 0
        assert values["average_logical_cores"] >= 0
        assert values["peak_rss_bytes"] > 0
        assert values["rss_growth_bytes"] >= 0
    assert measurements["total_wall_seconds"] > 0
    assert measurements["total_cpu_seconds"] > 0
    assert measurements["process_peak_rss_bytes"] > 0

    failures = {
        name: budget
        for name, budget in payload["budgets"].items()
        if not budget["passed"]
    }
    assert not failures, json.dumps(failures, indent=2, sort_keys=True)
    assert json.dumps(payload, allow_nan=False)

    if strict_cardinality:
        assert config["jobs"] == 100_000
        assert synthetic["event_count"] == 1_000_000


def test_wp7_fast_scale_smoke(tmp_path: Path) -> None:
    payload = run_subprocess_gate(PROFILES["fast"], tmp_path)
    _assert_report(payload, strict_cardinality=False)


@pytest.mark.skipif(
    selected_mode() == "fast",
    reason="set PEGASUS_MONITOR_RUN_SCALE=default or full for the extended gate",
)
def test_wp7_requested_scale_gate(tmp_path: Path) -> None:
    config = scale_config(os.environ["PEGASUS_MONITOR_RUN_SCALE"])
    payload = run_subprocess_gate(config, tmp_path)
    _assert_report(payload, strict_cardinality=config.mode == "full")


def test_incremental_transition_replay_has_a_bounded_recent_window() -> None:
    records = read_jsonl(GOLDEN).records
    header = next(record for record in records if isinstance(record, StreamHeader))
    checkpoint = next(
        record for record in records if isinstance(record, CheckpointRecord)
    )
    accumulator = ReplayAccumulator(recent_transition_limit=256)
    accumulator.consume(header)
    accumulator.consume(checkpoint)
    job = checkpoint.snapshot.jobs[0]
    attempt = job.current_attempt
    assert attempt is not None

    for sequence in range(2, 514):
        transition = DBJobTransition(
            job.workflow,
            job.exec_job_id,
            attempt.job_submit_seq,
            DBTransitionIdentity(
                attempt.job_instance_id,
                "EXECUTE",
                Decimal(sequence),
                sequence,
            ),
        )
        accumulator.consume(
            JobTransitionRecord(
                sequence,
                header.stream_id,
                SnapshotEpoch(sequence),
                2.0,
                transition,
            )
        )

    assert accumulator.snapshot is not None
    assert len(accumulator.snapshot.recent_transitions) <= 256
