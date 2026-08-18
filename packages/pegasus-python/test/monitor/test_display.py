"""Tests for source-free pegasus-monitor presentation."""

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

import gc
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from rich.console import Console

from Pegasus.monitor.display import (
    DisplayAnalysis,
    DisplayContext,
    DisplayOptions,
    _scheduler_display,
    _visible_jobs,
    render_text,
)
from Pegasus.monitor.models import (
    ClockSample,
    EffectiveEvent,
    FrozenPayload,
    HealthState,
    Lifecycle,
    Provenance,
    SourceHealth,
    SourceName,
    TailGeneration,
    TailJobEvent,
    TailTransitionIdentity,
    WorkflowIdentity,
)


def _context(tmp_path: Path) -> DisplayContext:
    return DisplayContext(
        "diamond",
        "alice",
        "6.0.0",
        "20260818T120000-0400",
        tmp_path,
        Path("/recorded/run0001"),
        tmp_path.parent,
        Path("/recorded"),
        tmp_path,
        tmp_path / "jobstate.log",
        tmp_path / "diamond-0.stampede.db",
        "wf-uuid-1234567890",
        "wf-uuid-1234567890",
        "diamond-0.dag",
    )


def _job(name: str, lifecycle: Lifecycle, *, provenance=Provenance.DB_CONFIRMED):
    return SimpleNamespace(
        exec_job_id=name,
        type_desc="compute",
        lifecycle=lifecycle,
        provenance=provenance,
        state_timestamp=Decimal("10"),
        current_attempt=None,
        attempts=(),
        transformations=(name,),
    )


def _snapshot(*, authoritative: bool = True):
    workflow = WorkflowIdentity("wf-uuid-1234567890", "wf-uuid-1234567890")
    effective = None
    unconfirmed = ()
    if authoritative:
        effective = SimpleNamespace(
            workflow=SimpleNamespace(
                state="WORKFLOW_STARTED",
                status=None,
                provenance=Provenance.DB_CONFIRMED,
            ),
            jobs=(
                _job("compute_a", Lifecycle.RUNNING),
                _job(
                    "compute_b",
                    Lifecycle.HELD,
                    provenance=Provenance.DB_WITH_TAIL_OVERLAY,
                ),
            ),
            pending_overlay_count=1,
            events=(),
        )
    else:
        generation = TailGeneration(1, 2, 3)
        tail = TailJobEvent(
            workflow,
            TailTransitionIdentity(generation, 0),
            None,
            40,
            1.0,
            Decimal("10"),
            "compute_a",
            "JOB_FAILURE",
            1,
            "1",
            "local",
            "0",
            "10 compute_a JOB_FAILURE 1 local 0 1",
            status=1,
        )
        unconfirmed = (EffectiveEvent(1, Provenance.TAIL_PENDING, tail),)
    return SimpleNamespace(
        sequence=1,
        clock=ClockSample(20.0, 20.0),
        effective=effective,
        source_health=(
            SourceHealth(
                SourceName.STAMPEDE,
                HealthState.HEALTHY if authoritative else HealthState.UNAVAILABLE,
                20.0,
                error_code=None if authoritative else "database_missing",
            ),
        ),
        scheduler_results=(),
        pending_tail_events=len(unconfirmed),
        unconfirmed_tail_events=unconfirmed,
        has_authoritative_base=authoritative,
        authoritative_complete=False,
        publisher_failures=0,
        coordinator_errors=FrozenPayload(),
    )


def _stats():
    return SimpleNamespace(
        compute_jobs=2,
        wall_time=10.0,
        total_compute_time=5.0,
        parallelism=0.5,
        peak_maxrss_kb=2048,
        pending_jobs=1,
    )


def test_render_golden_contains_health_provenance_and_bounded_rows(tmp_path):
    text = render_text(
        _context(tmp_path),
        _snapshot(),
        DisplayAnalysis(stats=_stats()),
        DisplayOptions(event_limit=5, job_row_limit=1, width=100, live=False),
        width=100,
    )
    flat = " ".join(text.split())

    assert "Pegasus Workflow Monitor" in text
    assert "stampede HEALTHY" in text
    assert "db_confirmed=1" in flat
    assert "db_with_tail_overlay=1" in flat
    assert "Jobs (1 shown of 2)" in text
    assert "Statistics" in text
    assert "\x1b[" not in text


def test_narrow_render_omits_wide_columns(tmp_path):
    text = render_text(
        _context(tmp_path),
        _snapshot(),
        options=DisplayOptions(width=54, live=False),
        width=54,
    )

    assert "Job" in text
    assert "State" in text
    assert "Site" not in text
    assert "Runtime" not in text


def test_event_only_view_does_not_fabricate_counts_or_completion(tmp_path):
    text = render_text(
        _context(tmp_path),
        _snapshot(authoritative=False),
        options=DisplayOptions(live=True),
        width=100,
    )
    flat = " ".join(text.split())

    assert "LIVE UNCONFIRMED" in text
    assert "job counts" in flat
    assert "completion are intentionally withheld" in flat
    assert "compute_a" in text
    assert "LIVE PENDING" in text
    assert "Statistics" not in text
    assert "Jobs (" not in text


def test_render_path_does_not_open_workflow_sources(tmp_path, monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("render attempted a source call")

    monkeypatch.setattr(Path, "open", forbidden)
    text = render_text(_context(tmp_path), _snapshot(), width=80)
    assert "Workflow Status" in text


def test_render_text_suspends_gc_and_restores_enabled_state(tmp_path, monkeypatch):
    original_print = Console.print
    observed_gc_states = []

    def print_spy(self, *args, **kwargs):
        observed_gc_states.append(gc.isenabled())
        return original_print(self, *args, **kwargs)

    gc.enable()
    monkeypatch.setattr(Console, "print", print_spy)

    text = render_text(_context(tmp_path), _snapshot(), width=80)

    assert "Workflow Status" in text
    assert observed_gc_states == [False]
    assert gc.isenabled()


def test_render_text_restores_gc_after_render_failure(tmp_path, monkeypatch):
    def fail_print(*_args, **_kwargs):
        assert not gc.isenabled()
        raise RuntimeError("render failed")

    gc.enable()
    monkeypatch.setattr(Console, "print", fail_print)

    with pytest.raises(RuntimeError, match="render failed"):
        render_text(_context(tmp_path), _snapshot(), width=80)

    assert gc.isenabled()


def test_render_text_preserves_already_disabled_gc_and_output(tmp_path):
    gc.enable()
    enabled_output = render_text(_context(tmp_path), _snapshot(), width=80)

    gc.disable()
    try:
        disabled_output = render_text(_context(tmp_path), _snapshot(), width=80)
        assert not gc.isenabled()
    finally:
        gc.enable()

    assert disabled_output == enabled_output


def test_visible_jobs_is_row_bounded_for_100k_jobs():
    class CountingJobs(tuple):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    jobs = CountingJobs(
        _job(f"job_{index}", Lifecycle.QUEUED) for index in range(100_000)
    )
    visible, total = _visible_jobs(
        jobs,
        DisplayOptions(job_row_limit=37, sort_by_activity=True),
    )

    assert len(visible) == 37
    assert total == 100_000
    assert jobs.iterations == 1


def test_visible_jobs_preserves_activity_order_and_stable_ties():
    jobs = [
        _job("old", Lifecycle.QUEUED),
        _job("running", Lifecycle.RUNNING),
        _job("new_a", Lifecycle.FAILED),
        _job("new_b", Lifecycle.FAILED),
        _job("undated", Lifecycle.QUEUED),
    ]
    jobs[0].state_timestamp = Decimal("8")
    jobs[1].state_timestamp = Decimal("1")
    jobs[2].state_timestamp = Decimal("20")
    jobs[3].state_timestamp = Decimal("20")
    jobs[4].state_timestamp = None

    visible, total = _visible_jobs(
        tuple(jobs),
        DisplayOptions(job_row_limit=4, sort_by_activity=True),
    )

    assert total == 5
    assert [job.exec_job_id for job in visible] == [
        "running",
        "new_a",
        "new_b",
        "old",
    ]


def test_disabled_condor_is_explicit(tmp_path):
    text = render_text(
        _context(tmp_path),
        _snapshot(),
        options=DisplayOptions(condor_enabled=False, live=False),
        width=90,
    )
    assert "HTCondor DISABLED" in text


def test_job_rows_show_current_attempt_exit_rss_and_scheduler_enrichment(tmp_path):
    current_identity = SimpleNamespace(job_submit_seq=9)
    old_attempt = SimpleNamespace(
        identity=SimpleNamespace(job_submit_seq=2),
        scheduler_id="40.0",
        site="old",
        start_time=Decimal("1"),
        end_time=Decimal("2"),
        exit_code=0,
        maxrss_kb=100,
    )
    current_attempt = SimpleNamespace(
        identity=current_identity,
        scheduler_id="42.0",
        site="local",
        start_time=Decimal("10"),
        end_time=Decimal("15"),
        exit_code=17,
        maxrss_kb=2048,
    )
    job = _job("compute_rich", Lifecycle.FAILED)
    job.current_attempt = current_identity
    job.attempts = (old_attempt, current_attempt)
    job.scheduler = FrozenPayload.from_mapping(
        {
            "queue": {
                "ClusterId": 42,
                "ProcId": 0,
                "JobStatus": 1,
                "RequestCpus": 4,
                "RequestMemory": 8192,
                "RequestGpus": 1,
                "RemoteHost": "slot1@worker.example",
            }
        }
    )
    assert _scheduler_display(job, current_attempt) == (
        "IDLE",
        "C4/M8192/G1",
        "slot1@worker.example",
    )
    job.scheduler = FrozenPayload.from_mapping(
        {
            "history": {
                "ClusterId": 42,
                "ProcId": 0,
                "RequestCpus": 4,
                "RequestMemory": 8192,
                "LastRemoteHost": "slot1@history.example",
            }
        }
    )
    assert _scheduler_display(job, current_attempt) == (
        "HISTORY",
        "C4/M8192/G0",
        "slot1@history.example",
    )
    job.scheduler = FrozenPayload.from_mapping(
        {
            "history": {
                "ClusterId": 40,
                "ProcId": 0,
                "DAGNodeName": "compute_rich",
                "RequestCpus": 8,
                "RequestMemory": 4096,
                "LastRemoteHost": "old-attempt",
            }
        }
    )
    assert _scheduler_display(job, current_attempt) == ("-", "-", "-")
    job.scheduler = FrozenPayload.from_mapping(
        {
            "queue": {
                "ClusterId": 42,
                "ProcId": 0,
                "JobStatus": 1,
                "RequestCpus": 4,
                "RequestMemory": 8192,
                "RequestGpus": 1,
                "RemoteHost": "slot1@worker.example",
            }
        }
    )
    snapshot = _snapshot()
    snapshot.effective.jobs = (job,)
    text = render_text(
        _context(tmp_path),
        snapshot,
        options=DisplayOptions(width=150, live=False),
        width=150,
    )
    flat = " ".join(text.split())
    assert "Try" in text
    assert "Exit" in text
    assert "RSS" in text
    assert "17" in flat
    assert "2.0M" in flat
    assert "IDLE" in flat
    assert "C4/M8192/G1" in flat
    assert "slot1@" in flat


def test_scheduler_fallback_requires_unique_current_attempt_evidence():
    identity = SimpleNamespace(job_submit_seq=1)
    current_attempt = SimpleNamespace(identity=identity, scheduler_id=None)
    job = _job("compute_unique", Lifecycle.QUEUED)
    job.current_attempt = identity
    job.attempts = (current_attempt,)
    job.scheduler = FrozenPayload.from_mapping(
        {
            "queue": {
                "ClusterId": 42,
                "ProcId": 0,
                "DAGNodeName": "compute_unique",
                "JobStatus": 1,
            }
        }
    )

    assert _scheduler_display(job, current_attempt)[0] == "IDLE"

    old_attempt = SimpleNamespace(
        identity=SimpleNamespace(job_submit_seq=0), scheduler_id="40.0"
    )
    job.attempts = (old_attempt, current_attempt)
    job.scheduler = FrozenPayload.from_mapping(
        {
            "history": {
                "ClusterId": 40,
                "ProcId": 0,
                "DAGNodeName": "compute_unique",
            }
        }
    )

    assert _scheduler_display(job, current_attempt) == ("-", "-", "-")


def test_why_idle_detail_and_diagnostics_precede_row_heavy_panels(tmp_path):
    priority = SimpleNamespace(
        principal="alice@pool",
        effective_priority=10.0,
        real_priority=5.0,
        resources_used=3.0,
        better_priority_users=2,
    )
    idle = SimpleNamespace(
        queued_jobs=1,
        unsubmitted_jobs=1,
        idle_jobs=(SimpleNamespace(exec_job_id="compute_a", state=Lifecycle.QUEUED),),
        pool_machines=3,
        pool_total_slots=8,
        pool_idle_slots=2,
        pool_total_cpus=16,
        pool_idle_cpus=4,
        pool_total_memory_mb=64000,
        pool_idle_memory_mb=8000,
        pool_total_gpus=2,
        pool_idle_gpus=1,
        negotiation_cycle_seconds=4.5,
        negotiation_matches=7,
        findings=("One queued job is waiting.",),
        suggestions=("Inspect requirements.",),
        requirement_mismatches=(),
        priority=priority,
        sources=(),
    )
    diagnostics = SimpleNamespace(
        stall=None,
        findings=(),
        errors=(),
        idle=None,
    )
    text = render_text(
        _context(tmp_path),
        _snapshot(),
        DisplayAnalysis(stats=_stats(), diagnostics=diagnostics, why_idle=idle),
        DisplayOptions(width=110, analysis_line_limit=20, live=False),
        width=110,
    )
    flat = " ".join(text.split())
    lines = text.splitlines()
    assert text.index("Diagnostics") < text.index("Jobs (")
    assert text.index("Why Idle") < text.index("Jobs (")
    assert next(index for index, line in enumerate(lines) if "Diagnostics" in line) < 40
    assert next(index for index, line in enumerate(lines) if "Why Idle" in line) < 40
    assert next(index for index, line in enumerate(lines) if "Statistics" in line) < 40
    assert "compute_a(queued)" in flat
    assert "machines 3" in flat
    assert "cycle 4.5s" in flat
    assert "better users 2" in flat
