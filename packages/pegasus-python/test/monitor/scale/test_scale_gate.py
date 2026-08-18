"""Executable scale and workflow-impact release gates."""

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

import pytest

from .runner import run_subprocess_gate
from .workload import (
    PROFILES,
    GeneratedWorkload,
    generate_workload,
    scale_config,
    selected_mode,
    validate_workload,
)


def _assert_report(payload, *, strict_cardinality: bool) -> None:
    config = payload["config"]
    observed = payload["observed"]
    assert payload["fixture_sha256"]
    assert payload["environment"]["python"]
    assert observed["database_jobs"] == config["jobs"]
    assert observed["database_attempts"] == config["attempts"]
    assert observed["actual_database_transitions"] == config["transitions"]
    assert observed["actual_tail_lines_before_burst"] == config["prefilled_tail_lines"]
    assert payload["live_burst"]["observed_lines"] == config["burst_lines"]
    assert set(payload["live_burst"]["stage_seconds"]) == {
        "tail_poll",
        "reconciler_ingest",
        "publication",
        "display",
    }
    assert payload["stampede_refresh_seconds"]["count"] == config["refresh_samples"]
    assert payload["stampede_transaction_seconds"]["count"] == config["refresh_samples"]
    assert all(
        sample >= 0.0 for sample in payload["stampede_transaction_seconds"]["samples"]
    )
    assert {
        "stampede_transaction_p95_seconds",
        "stampede_transaction_max_seconds",
    } <= payload["budgets"].keys()
    assert payload["probes"]["job_count"] == config["jobs"]
    assert payload["call_counts"]["stampede_refresh"] == config["refresh_samples"]
    assert payload["writer_impact"]["wal"]["paired_trials"] == config["writer_trials"]
    assert (
        payload["writer_impact"]["delete"]["paired_trials"] == config["writer_trials"]
    )
    assert payload["writer_impact"]["wal"]["monitor_refresh_calls"] >= 1
    assert payload["writer_impact"]["delete"]["monitor_refresh_calls"] >= 1
    assert len(payload["probes"]["hotspots_descending"]) == 4
    assert json.dumps(payload, allow_nan=False)
    failures = {
        name: budget
        for name, budget in payload["budgets"].items()
        if budget["enforced"] and not budget["passed"]
    }
    assert not failures, json.dumps(failures, indent=2, sort_keys=True)
    if strict_cardinality:
        assert config["jobs"] == 100_000
        assert config["attempts"] == 100_000
        assert config["transitions"] == 1_000_000
        assert config["prefilled_tail_lines"] == 1_000_000
        assert config["burst_rate"] == 1_000
        assert config["burst_lines"] == 1_000
        assert config["refresh_samples"] == 40
        assert config["writer_warmups"] >= 1
        assert config["writer_trials"] == 9


def test_scale_profiles_pin_release_cardinalities() -> None:
    assert selected_mode(None) in PROFILES
    full = PROFILES["full"]
    assert (
        full.jobs,
        full.attempts,
        full.transitions,
        full.prefilled_tail_lines,
    ) == (100_000, 100_000, 1_000_000, 1_000_000)
    assert full.refresh_samples == 40
    assert full.writer_trials == 9
    assert full.writer_warmups >= 1


def test_generated_fixture_hash_is_workspace_independent(tmp_path) -> None:
    first = generate_workload(tmp_path / "first", PROFILES["fast"])
    second = generate_workload(tmp_path / "second", PROFILES["fast"])

    assert first.fixture_sha256 == second.fixture_sha256
    assert first.manifest == second.manifest
    assert validate_workload(first, PROFILES["fast"]) == {
        "database_transitions": PROFILES["fast"].transitions,
        "tail_lines": PROFILES["fast"].prefilled_tail_lines,
    }
    invalid = GeneratedWorkload(
        first.database_path,
        first.jobstate_path,
        "0" * 64,
        first.manifest,
    )
    with pytest.raises(RuntimeError, match="fixture SHA-256"):
        validate_workload(invalid, PROFILES["fast"])


def test_fast_scale_smoke(tmp_path) -> None:
    payload = run_subprocess_gate(PROFILES["fast"], tmp_path)
    _assert_report(payload, strict_cardinality=False)


@pytest.mark.skipif(
    selected_mode() == "fast",
    reason="set PEGASUS_MONITOR_RUN_SCALE=default or full for the extended gate",
)
def test_requested_scale_gate(tmp_path) -> None:
    config = scale_config(os.environ["PEGASUS_MONITOR_RUN_SCALE"])
    payload = run_subprocess_gate(config, tmp_path)
    _assert_report(payload, strict_cardinality=config.mode == "full")
