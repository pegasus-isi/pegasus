"""Read-only Stampede snapshot and reconciliation-suffix tests."""

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

import os
import sqlite3
import threading
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from queue import Queue

import pytest

import Pegasus.monitor.stampede as stampede_module
from Pegasus.monitor.locator import DatabaseBackend, WorkflowLocation
from Pegasus.monitor.models import (
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    HealthState,
    JobSemanticKey,
    SnapshotEpoch,
    WorkflowIdentity,
)
from Pegasus.monitor.stampede import StampedeReader, decode_wait_status

FIXTURES = Path(__file__).parent / "fixtures" / "stampede"
ROOT = WorkflowIdentity("root-uuid", "root-uuid")
CHILD = WorkflowIdentity("child-uuid", "root-uuid")


def _create_database(path: Path, *, include_child: bool = False) -> Path:
    connection = sqlite3.connect(path)
    try:
        connection.executescript((FIXTURES / "schema.sql").read_text())
        connection.execute(
            "INSERT INTO workflow VALUES (1, 'root-uuid', 1, 'root-0.dag', ?)",
            (str(path.parent),),
        )
        connection.executemany(
            "INSERT INTO workflowstate VALUES (?,?,?,?,?,?)",
            [
                (1, "WORKFLOW_STARTED", "100.499999", 0, None, None),
                (1, "WORKFLOW_TERMINATED", "300.500001", 0, 0, None),
            ],
        )
        connection.executemany(
            "INSERT INTO job VALUES (?,?,?,?,?)",
            [
                (10, 1, "cluster_ID0000001", "compute", 2),
                (20, 1, "held_ID0000002", "compute", 1),
            ],
        )
        connection.executemany(
            "INSERT INTO job_instance VALUES (?,?,?,?,?,?,?,?)",
            [
                (100, 10, 1, "1000.0", "local", "cluster.out", "cluster.err", 2304),
                (200, 20, 2, "1001.0", "condorpool", "held.out", "held.err", None),
            ],
        )
        connection.executemany(
            "INSERT INTO jobstate VALUES (?,?,?,?,?)",
            [
                (100, "SUBMIT", "110.100001", 1, None),
                (100, "EXECUTE", "120.200002", 2, None),
                (100, "JOB_ABORTED", "130.300003", 3, "removed"),
                (100, "JOB_FAILURE", "130.300003", 3, "removed"),
                (200, "SUBMIT", "110.100001", 1, None),
                (200, "JOB_HELD", "125.250000", 2, "operator hold"),
            ],
        )
        connection.executemany(
            "INSERT INTO task VALUES (?,?,?,?)",
            [
                (1, 1, 10, "example::alpha"),
                (2, 1, 10, "example::beta"),
                (3, 1, 20, "example::held"),
            ],
        )
        connection.executemany(
            "INSERT INTO invocation VALUES (?,?,?,?,?)",
            [
                (1, 1, 100, -1, 999999),
                (6, 1, 100, 0, 777777),
                (2, 1, 100, 1, 4096),
                (3, 1, 100, 2, 8192),
                (4, 1, 200, -2, 888888),
                (5, 1, 200, 1, 2048),
            ],
        )
        if include_child:
            connection.execute(
                "INSERT INTO workflow VALUES (2, 'child-uuid', 1, 'child-0.dag', ?)",
                (str(path.parent / "child"),),
            )
            connection.execute(
                "INSERT INTO workflowstate VALUES "
                "(2, 'WORKFLOW_STARTED', 150, 0, NULL, NULL)"
            )
            connection.execute(
                "INSERT INTO job VALUES (30, 2, 'child_job', 'compute', 1)"
            )
            connection.execute(
                "INSERT INTO job_instance VALUES "
                "(300, 30, 3, '1002.0', 'local', NULL, NULL, NULL)"
            )
            connection.execute(
                "INSERT INTO jobstate VALUES (300, 'SUBMIT', 151, 1, NULL)"
            )
            connection.execute("INSERT INTO task VALUES (4, 2, 30, 'example::child')")
        connection.commit()
    finally:
        connection.close()
    return path


def _create_large_suffix_database(
    path: Path, *, instance_count: int = 1005, grouped_index: int = 700
) -> Path:
    connection = sqlite3.connect(path)
    try:
        connection.executescript((FIXTURES / "schema.sql").read_text())
        connection.execute(
            "INSERT INTO workflow VALUES (1, 'root-uuid', 1, 'root-0.dag', ?)",
            (str(path.parent),),
        )
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 2000, 0, NULL, NULL)"
        )
        jobs = []
        attempts = []
        states = []
        for index in range(instance_count):
            job_id = 1000 + index
            instance_id = 10000 + index
            job_submit_seq = index + 1
            timestamp = (index * 37 + 230) % instance_count
            if index in {10, 900}:
                timestamp = 500
            jobs.append((job_id, 1, f"bulk_{index:04d}", "compute", 1))
            attempts.append(
                (
                    instance_id,
                    job_id,
                    job_submit_seq,
                    f"{job_id}.0",
                    "local",
                    None,
                    None,
                    None,
                )
            )
            state = "JOB_ABORTED" if index == grouped_index else "SUBMIT"
            states.append((instance_id, state, timestamp, 1, None))
            if index == grouped_index:
                states.append((instance_id, "JOB_FAILURE", timestamp, 1, None))
        connection.executemany("INSERT INTO job VALUES (?,?,?,?,?)", jobs)
        connection.executemany(
            "INSERT INTO job_instance VALUES (?,?,?,?,?,?,?,?)", attempts
        )
        connection.executemany("INSERT INTO jobstate VALUES (?,?,?,?,?)", states)
        connection.commit()
    finally:
        connection.close()
    return path


def _request(
    workflow: WorkflowIdentity = ROOT,
    *,
    epoch: int = 1,
    now: float = 1000.0,
    mode: DBRefreshMode = DBRefreshMode.FULL_REBOOTSTRAP,
    prior=None,
    job_watermarks=(),
    job_keys=(),
    workflow_watermark=None,
    recent_limit: int = 256,
    workflow_limit: int = 64,
) -> DBRefreshRequest:
    return DBRefreshRequest(
        workflow=workflow,
        next_epoch=SnapshotEpoch(epoch),
        mode=mode,
        clock=ClockSample(epoch=now, monotonic=500.0 + epoch),
        prior_generation=prior,
        pending_job_watermarks=job_watermarks,
        pending_job_keys=job_keys,
        pending_workflow_watermark=workflow_watermark,
        recent_transition_limit=recent_limit,
        recent_workflow_transition_limit=workflow_limit,
    )


def _writer(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def test_full_snapshot_has_exact_jobs_attempts_transitions_and_metadata(tmp_path):
    path = _create_database(tmp_path / "workflow.db")

    result = StampedeReader(path, ROOT).refresh(_request(now=1234.5))

    assert result.health.state is HealthState.HEALTHY
    assert result.health.checked_at_epoch == 1234.5
    assert result.snapshot is not None
    snapshot = result.snapshot
    assert snapshot.snapshot_at_epoch == 1234.5
    assert snapshot.workflow.workflow == ROOT
    assert snapshot.workflow.state == "WORKFLOW_TERMINATED"
    assert snapshot.workflow.status == 0
    assert snapshot.workflow.started_at == Decimal("100.499999")
    assert snapshot.workflow.ended_at == Decimal("300.500001")
    assert [job.exec_job_id for job in snapshot.jobs] == [
        "cluster_ID0000001",
        "held_ID0000002",
    ]

    cluster = snapshot.jobs[0]
    assert cluster.task_count == 2
    assert cluster.transformations == ("example::alpha", "example::beta")
    assert cluster.state == "JOB_FAILURE"
    assert cluster.transition is not None
    assert cluster.transition.identity.jobstate_submit_seq == 3
    assert cluster.transition.identity.state == "JOB_FAILURE"
    assert cluster.attempts[0].raw_wait_status == 2304
    assert cluster.attempts[0].exit_code == 9
    assert cluster.attempts[0].submit_time == Decimal("110.100001")
    assert cluster.attempts[0].start_time == Decimal("120.200002")
    assert cluster.attempts[0].end_time == Decimal("130.300003")
    assert cluster.attempts[0].maxrss_kb == 8192

    held = snapshot.jobs[1]
    assert held.state == "JOB_HELD"
    assert held.transition.reason == "operator hold"
    assert held.attempts[0].maxrss_kb == 2048
    assert held.attempts[0].end_time is None

    cluster_watermark = next(
        item for item in snapshot.watermarks if item.job_instance_id == 100
    )
    assert cluster_watermark.highest_jobstate_submit_seq == 3
    assert [item.state for item in cluster_watermark.identities_at_highest_seq] == [
        "JOB_ABORTED",
        "JOB_FAILURE",
    ]


def test_full_snapshot_commits_before_building_immutable_models(tmp_path, monkeypatch):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    original = reader._build_jobs
    writer_committed = False

    def build_after_commit(*args, **kwargs):
        nonlocal writer_committed
        connection = sqlite3.connect(path, timeout=0.01)
        try:
            connection.execute(
                "UPDATE workflow SET dag_file_name = dag_file_name || '.concurrent'"
            )
            connection.commit()
            writer_committed = True
        finally:
            connection.close()
        return original(*args, **kwargs)

    monkeypatch.setattr(reader, "_build_jobs", build_after_commit)
    result = reader.refresh(_request())

    assert result.snapshot is not None
    assert writer_committed
    assert reader.last_transaction_seconds > 0


@pytest.mark.parametrize(
    ("raw", "decoded"),
    [(None, None), (-1, -128), (0, 0), (256, 1), (9, -9), (137, -9), (11, -11)],
)
def test_wait_status_uses_existing_pegasus_decode_behavior(raw, decoded):
    assert decode_wait_status(raw) == decoded


def test_current_attempt_is_highest_workflow_global_submit_sequence(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(101, 10, 4, '1003.0', 'retry-site', NULL, NULL, 0)"
        )
        connection.execute("INSERT INTO jobstate VALUES (101, 'SUBMIT', 200, 1, NULL)")

    snapshot = StampedeReader(path, ROOT).refresh(_request()).snapshot

    assert snapshot is not None
    job = snapshot.jobs[0]
    assert [attempt.identity.job_submit_seq for attempt in job.attempts] == [1, 4]
    assert job.current_attempt.job_instance_id == 101
    assert job.state == "SUBMIT"
    assert {watermark.job_instance_id for watermark in snapshot.watermarks} == {
        100,
        101,
        200,
    }


def test_full_recent_frontier_falls_back_for_older_newer_timestamp(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (100, 'REMOTE_ERROR', 10000, 4, NULL)"
        )
        connection.executemany(
            "INSERT INTO jobstate VALUES (100, 'IMAGE_SIZE', ?, ?, NULL)",
            ((1000 + index, 5 + index) for index in range(40)),
        )
    monkeypatch.setattr(stampede_module, "_RECENT_FRONTIER_INITIAL_ROWS", 4)
    monkeypatch.setattr(stampede_module, "_RECENT_FRONTIER_MAX_ROWS", 24)
    reader = StampedeReader(path, ROOT)
    original = reader._query_sorted_recent_transition_rows
    fallbacks = 0

    def counted(*args, **kwargs):
        nonlocal fallbacks
        fallbacks += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(reader, "_query_sorted_recent_transition_rows", counted)

    result = reader.refresh(_request(recent_limit=2))

    assert result.snapshot is not None
    assert fallbacks == 1
    assert [item.identity.timestamp for item in result.snapshot.recent_transitions] == [
        Decimal("1039"),
        Decimal("10000"),
    ]


def test_full_recent_frontier_proves_monotonic_suffix_without_fallback(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.executemany(
            "INSERT INTO jobstate VALUES (100, 'IMAGE_SIZE', ?, ?, NULL)",
            ((1000 + index, 4 + index) for index in range(40)),
        )
    monkeypatch.setattr(stampede_module, "_RECENT_FRONTIER_INITIAL_ROWS", 4)
    monkeypatch.setattr(stampede_module, "_RECENT_FRONTIER_MAX_ROWS", 24)
    reader = StampedeReader(path, ROOT)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("monotonic frontier unexpectedly used the full sort")

    monkeypatch.setattr(reader, "_query_sorted_recent_transition_rows", forbidden)

    result = reader.refresh(_request(recent_limit=2))

    assert result.snapshot is not None
    assert [item.identity.timestamp for item in result.snapshot.recent_transitions] == [
        Decimal("1038"),
        Decimal("1039"),
    ]


def test_full_head_fallback_keeps_scope_and_state_less_attempt(tmp_path, monkeypatch):
    path = _create_database(tmp_path / "workflow.db", include_child=True)
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO job VALUES (40, 1, 'waiting_job', 'compute', 1)"
        )
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(400, 40, 4, '1004.0', 'local', NULL, NULL, NULL)"
        )
        connection.execute("INSERT INTO task VALUES (5, 1, 40, 'example::waiting')")
        connection.execute(
            "INSERT INTO jobstate VALUES (300, 'JOB_SUCCESS', 9999, 999, NULL)"
        )
    monkeypatch.setattr(stampede_module, "_HEAD_SEQUENCE_LAYER_LIMIT", 2)
    reader = StampedeReader(path, ROOT)
    original = reader._query_jobstate_summary
    fallbacks = 0

    def counted(*args, **kwargs):
        nonlocal fallbacks
        fallbacks += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(reader, "_query_jobstate_summary", counted)

    result = reader.refresh(_request(ROOT))

    assert result.snapshot is not None
    assert fallbacks == 1
    jobs = {job.exec_job_id: job for job in result.snapshot.jobs}
    assert set(jobs) == {"cluster_ID0000001", "held_ID0000002", "waiting_job"}
    assert jobs["waiting_job"].current_attempt.job_instance_id == 400
    assert jobs["waiting_job"].state is None
    assert all(item.workflow == ROOT for item in result.snapshot.recent_transitions)


def test_optimized_full_snapshot_matches_exact_fallbacks_on_complex_database(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db", include_child=True)
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(101, 10, 4, '1003.0', 'retry-site', NULL, NULL, 0)"
        )
        connection.executemany(
            "INSERT INTO jobstate VALUES (?,?,?,?,NULL)",
            [
                (101, "SUBMIT", 200, 1),
                (101, "EXECUTE", 210, 2),
                (101, "JOB_TERMINATED", 220, 3),
                (101, "JOB_SUCCESS", 220, 3),
            ],
        )
        connection.execute(
            "INSERT INTO job VALUES (40, 1, 'waiting_job', 'compute', 1)"
        )
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(400, 40, 5, '1004.0', 'local', NULL, NULL, NULL)"
        )
        connection.execute("INSERT INTO task VALUES (5, 1, 40, 'example::waiting')")
        connection.execute(
            "INSERT INTO jobstate VALUES (300, 'JOB_SUCCESS', 9999, 999, NULL)"
        )

    optimized_reader = StampedeReader(path, ROOT)
    optimized = optimized_reader.refresh(_request(ROOT, recent_limit=64))
    assert optimized.snapshot is not None

    monkeypatch.setattr(stampede_module, "_RECENT_FRONTIER_MAX_ROWS", 0)
    monkeypatch.setattr(stampede_module, "_HEAD_SEQUENCE_LAYER_LIMIT", 0)
    exact = StampedeReader(path, ROOT).refresh(_request(ROOT, recent_limit=64))

    assert exact.snapshot is not None
    assert optimized.snapshot == exact.snapshot

    # A later refresh opens a new read-only connection, so the tuple row
    # factory used only inside full bootstrap cannot leak into current reads.
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 230, 3, NULL)"
        )
    current = optimized_reader.refresh(
        _request(
            ROOT,
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=optimized.generation,
        )
    )
    assert current.snapshot is not None
    assert {job.exec_job_id: job.state for job in current.snapshot.jobs}[
        "held_ID0000002"
    ] == "JOB_RELEASED"


def test_root_and_subworkflow_scopes_do_not_contaminate_each_other(tmp_path):
    path = _create_database(tmp_path / "workflow.db", include_child=True)

    root = StampedeReader(path, ROOT).refresh(_request(ROOT)).snapshot
    child = StampedeReader(path, CHILD).refresh(_request(CHILD)).snapshot

    assert root is not None and child is not None
    assert {job.exec_job_id for job in root.jobs} == {
        "cluster_ID0000001",
        "held_ID0000002",
    }
    assert [job.exec_job_id for job in child.jobs] == ["child_job"]
    assert all(item.workflow == ROOT for item in root.recent_transitions)
    assert all(item.workflow == CHILD for item in child.recent_transitions)


def test_recent_event_order_is_stable_across_reader_instances(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 130.300003, 3, NULL)"
        )

    first = StampedeReader(path, ROOT).refresh(_request()).snapshot
    second = StampedeReader(path, ROOT).refresh(_request()).snapshot

    assert first is not None and second is not None
    first_ids = [item.identity for item in first.recent_transitions]
    assert first_ids == [item.identity for item in second.recent_transitions]
    assert first.recent_transitions == tuple(
        sorted(
            first.recent_transitions,
            key=lambda transition: transition.recent_event_sort_key,
        )
    )


def test_recent_feed_obeys_both_request_bounds(tmp_path):
    path = _create_database(tmp_path / "workflow.db")

    snapshot = (
        StampedeReader(path, ROOT)
        .refresh(_request(recent_limit=2, workflow_limit=1))
        .snapshot
    )

    assert snapshot is not None
    assert len(snapshot.recent_transitions) == 2
    assert len(snapshot.recent_workflow_transitions) == 1


def test_recent_feed_never_splits_same_sequence_group(tmp_path):
    path = _create_database(tmp_path / "workflow.db")

    result = StampedeReader(path, ROOT).refresh(_request(recent_limit=1))

    assert result.snapshot is None
    assert result.health.state is HealthState.DEGRADED
    assert result.health.error_code == "recent_transition_group_exceeds_limit"


def test_current_snapshot_requires_and_preserves_generation(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())

    second = reader.refresh(
        _request(
            epoch=2,
            now=1002.0,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert second.snapshot is not None
    assert second.generation == first.generation
    assert second.snapshot.epoch == SnapshotEpoch(2)
    assert second.snapshot.snapshot_at_epoch == 1002.0


def test_unchanged_current_snapshot_reuses_last_good_without_opening_sqlite(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unchanged database opened")

    monkeypatch.setattr(reader, "_open_read_only", forbidden)
    second = reader.refresh(
        _request(
            epoch=2,
            now=1002.0,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert second.snapshot is not None
    assert second.snapshot.epoch == SnapshotEpoch(2)
    assert second.snapshot.snapshot_at_epoch == 1002.0
    assert second.snapshot.jobs is first.snapshot.jobs
    assert reader.last_transaction_seconds == 0.0


def test_unchanged_current_snapshot_honors_smaller_history_limits(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request(recent_limit=8, workflow_limit=8))
    assert first.snapshot is not None

    second = reader.refresh(
        _request(
            epoch=2,
            now=1002.0,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
            recent_limit=1,
            workflow_limit=1,
        )
    )

    assert second.snapshot is not None
    assert len(second.snapshot.recent_transitions) <= 1
    assert len(second.snapshot.recent_workflow_transitions) == 1


def test_bounded_suffix_uses_watermark_and_provisional_attempt_keys(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    watermark = next(
        item for item in first.snapshot.watermarks if item.job_instance_id == 100
    )
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (100, 'JOB_RELEASED', 140, 4, NULL)"
        )
        connection.execute("INSERT INTO job VALUES (30, 1, 'new_job', 'compute', 1)")
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(300, 30, 3, '1002.0', 'local', NULL, NULL, NULL)"
        )
        connection.execute("INSERT INTO jobstate VALUES (300, 'SUBMIT', 141, 1, NULL)")
        connection.execute("INSERT INTO task VALUES (4, 1, 30, 'example::new')")

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=(watermark,),
            job_keys=(JobSemanticKey("root-uuid", "new_job", 3, "141", "SUBMIT"),),
            recent_limit=8,
        )
    )

    assert result.snapshot is not None
    assert {job.exec_job_id for job in result.snapshot.jobs} == {
        "cluster_ID0000001",
        "held_ID0000002",
        "new_job",
    }
    suffix_instances = {
        item.identity.job_instance_id for item in result.snapshot.recent_transitions
    }
    assert suffix_instances == {100, 300}
    assert any(
        item.identity.jobstate_submit_seq == 4
        for item in result.snapshot.recent_transitions
    )
    assert not result.snapshot.recent_workflow_transitions


def test_bounded_suffix_overflow_is_visible_and_preserves_last_good(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    watermark = next(
        item for item in first.snapshot.watermarks if item.job_instance_id == 100
    )
    with _writer(path) as connection:
        connection.executemany(
            "INSERT INTO jobstate VALUES (100, ?, ?, ?, NULL)",
            [
                ("JOB_RELEASED", 140, 4),
                ("EXECUTE", 141, 5),
                ("JOB_HELD", 142, 6),
            ],
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=(watermark,),
            recent_limit=3,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "reconciliation_suffix_overflow"
    assert reader.last_good_snapshot is first.snapshot


def test_bounded_suffix_absorbs_two_seconds_of_1000hz_paired_transitions(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request(recent_limit=8192))
    assert first.snapshot is not None
    watermark = next(
        item for item in first.snapshot.watermarks if item.job_instance_id == 100
    )
    rows = []
    for sequence in range(4, 2004):
        timestamp = 140 + sequence / 10_000
        rows.extend(
            (
                (100, "JOB_ABORTED", timestamp, sequence, "removed"),
                (100, "JOB_FAILURE", timestamp, sequence, "removed"),
            )
        )
    with _writer(path) as connection:
        connection.executemany("INSERT INTO jobstate VALUES (?,?,?,?,?)", rows)

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=(watermark,),
            recent_limit=8192,
        )
    )

    assert result.snapshot is not None
    assert result.health.state is HealthState.HEALTHY
    assert len(result.snapshot.recent_transitions) == 4002


def test_bounded_suffix_batches_large_watermark_set_in_global_order(tmp_path):
    path = _create_large_suffix_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request(recent_limit=2000))
    assert first.snapshot is not None
    assert len(first.snapshot.watermarks) == 1005

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=first.snapshot.watermarks,
            recent_limit=1100,
        )
    )

    assert result.snapshot is not None
    transitions = result.snapshot.recent_transitions
    assert len(transitions) == 1006
    assert transitions == tuple(
        sorted(
            transitions,
            key=lambda transition: transition.recent_event_sort_key,
        )
    )
    assert transitions[0].identity.job_instance_id == 10700
    timestamp_tie = [
        transition.exec_job_id
        for transition in transitions
        if transition.identity.timestamp == Decimal("500")
    ]
    assert timestamp_tie == ["bulk_0010", "bulk_0795", "bulk_0900"]
    grouped = [
        transition
        for transition in transitions
        if transition.identity.job_instance_id == 10700
        and transition.identity.jobstate_submit_seq == 1
    ]
    assert [transition.identity.state for transition in grouped] == [
        "JOB_ABORTED",
        "JOB_FAILURE",
    ]


def test_bounded_suffix_batches_apply_one_global_overflow_rule(tmp_path):
    path = _create_large_suffix_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request(recent_limit=2000))
    assert first.snapshot is not None

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=first.snapshot.watermarks,
            recent_limit=900,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "reconciliation_suffix_overflow"
    assert reader.last_good_snapshot is first.snapshot


def test_bounded_suffix_keeps_nonpending_job_current(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    watermark = next(
        item for item in first.snapshot.watermarks if item.job_instance_id == 100
    )
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (100, 'JOB_RELEASED', 140, 4, NULL)"
        )
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 141, 3, NULL)"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=(watermark,),
        )
    )

    assert result.snapshot is not None
    assert {
        item.identity.job_instance_id for item in result.snapshot.recent_transitions
    } == {100}
    assert result.snapshot.jobs[1].state == "JOB_RELEASED"


def test_incremental_refresh_does_not_rescan_all_jobstate_heads(tmp_path, monkeypatch):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None

    def forbidden(*_args, **_kwargs):
        raise AssertionError("non-full refresh rescanned all jobstate heads")

    monkeypatch.setattr(reader, "_load_jobstate_heads", forbidden)
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 141, 3, NULL)"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is not None
    assert result.snapshot.jobs[1].state == "JOB_RELEASED"


def test_failed_changed_refresh_is_retried_instead_of_reusing_stale_snapshot(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 141, 3, NULL)"
        )

    original = reader._load_snapshot
    attempts = 0

    def fail_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("database is busy")
        return original(*args, **kwargs)

    monkeypatch.setattr(reader, "_load_snapshot", fail_once)
    failed = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )
    retried = reader.refresh(
        _request(
            epoch=3,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert failed.snapshot is None
    assert failed.health.error_code == "database_busy"
    assert retried.snapshot is not None
    assert retried.snapshot.jobs[1].state == "JOB_RELEASED"
    assert attempts == 2


def test_commit_after_snapshot_is_loaded_on_next_refresh(tmp_path, monkeypatch):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 141, 3, NULL)"
        )

    original = reader._load_snapshot
    injected = False

    def load_then_commit(*args, **kwargs):
        nonlocal injected
        loaded = original(*args, **kwargs)
        if not injected:
            injected = True
            with _writer(path) as connection:
                connection.execute(
                    "INSERT INTO jobstate VALUES (200, 'JOB_SUCCESS', 142, 4, NULL)"
                )
        return loaded

    monkeypatch.setattr(reader, "_load_snapshot", load_then_commit)
    second = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )
    third = reader.refresh(
        _request(
            epoch=3,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert second.snapshot is not None
    assert second.snapshot.jobs[1].state == "JOB_RELEASED"
    assert third.snapshot is not None
    assert third.snapshot.jobs[1].state == "JOB_SUCCESS"


def test_bounded_suffix_refreshes_maxrss_without_state_advance(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    watermark = next(
        item for item in first.snapshot.watermarks if item.job_instance_id == 100
    )
    with _writer(path) as connection:
        connection.execute("INSERT INTO invocation VALUES (7, 1, 200, 2, 16384)")

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            job_watermarks=(watermark,),
        )
    )

    assert result.snapshot is not None
    assert result.snapshot.jobs[1].state == "JOB_HELD"
    assert result.snapshot.jobs[1].attempts[0].maxrss_kb == 16384


def test_bounded_workflow_suffix_advances_restart_watermark(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 400, 1, NULL, 'restart')"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            workflow_watermark=first.snapshot.workflow_watermark,
        )
    )

    assert result.snapshot is not None
    assert result.snapshot.workflow.restart_count == 1
    assert result.snapshot.workflow.state == "WORKFLOW_STARTED"
    assert result.snapshot.workflow_watermark.restart.restart_count == 1
    assert result.snapshot.recent_workflow_transitions[-1].restart_count == 1


def test_bounded_workflow_suffix_overflow_is_visible(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 400, 1, NULL, 'restart')"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=first.generation,
            workflow_watermark=first.snapshot.workflow_watermark,
            workflow_limit=2,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "workflow_suffix_overflow"


def test_bounded_suffix_without_reader_base_requests_resync(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    bootstrap = StampedeReader(path, ROOT).refresh(_request())
    assert bootstrap.snapshot is not None
    watermark = bootstrap.snapshot.watermarks[0]

    result = StampedeReader(path, ROOT).refresh(
        _request(
            mode=DBRefreshMode.BOUNDED_SUFFIX,
            prior=bootstrap.generation,
            job_watermarks=(watermark,),
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "missing_base_snapshot"


def test_missing_database_waits_without_creating_a_file(tmp_path):
    path = tmp_path / "absent.db"

    result = StampedeReader(path, ROOT).refresh(_request())

    assert result.snapshot is None
    assert result.health.state is HealthState.WAITING
    assert result.health.error_code == "database_missing"
    assert not path.exists()
    assert list(tmp_path.iterdir()) == []


def test_schema_version_skew_is_reported_without_migration(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute("UPDATE dbversion SET version_number = 999")

    result = StampedeReader(path, ROOT).refresh(_request())

    assert result.snapshot is None
    assert result.health.state is HealthState.DEGRADED
    assert result.health.error_code == "schema_version_mismatch"
    with _writer(path) as connection:
        assert (
            connection.execute("SELECT version_number FROM dbversion").fetchone()[0]
            == 999
        )


def test_dbversion_table_is_required(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        connection.execute("DROP TABLE dbversion")

    result = StampedeReader(path, ROOT).refresh(_request())

    assert result.snapshot is None
    assert result.health.state is HealthState.DEGRADED
    assert result.health.error_code == "schema_mismatch"
    assert "dbversion" in result.health.detail


def test_missing_required_column_is_a_schema_mismatch(tmp_path):
    path = tmp_path / "bad.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE workflow (wf_id INTEGER, wf_uuid TEXT)")
    connection.commit()
    connection.close()

    result = StampedeReader(path, ROOT).refresh(_request())

    assert result.snapshot is None
    assert result.health.error_code == "schema_mismatch"


def test_missing_selected_workflow_is_reported(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    missing = WorkflowIdentity("missing", "missing")

    result = StampedeReader(path, missing).refresh(_request(missing))

    assert result.snapshot is None
    assert result.health.error_code == "workflow_not_found"


def test_root_uuid_mismatch_is_never_merged(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    wrong = WorkflowIdentity("root-uuid", "another-root")

    result = StampedeReader(path, wrong).refresh(_request(wrong))

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "root_workflow_mismatch"


def test_unsupported_backend_never_attempts_a_guessed_sqlite_path(tmp_path):
    placeholder = tmp_path / "would-have-been.db"
    location = WorkflowLocation(
        workflow=ROOT,
        braindump_path=tmp_path / "braindump.yml",
        root_braindump_path=tmp_path / "braindump.yml",
        recorded_submit_dir=tmp_path,
        recorded_basedir=tmp_path,
        submit_dir=tmp_path,
        basedir=tmp_path,
        root_submit_dir=tmp_path,
        dag_name="root.dag",
        properties_path=None,
        database_uri="postgresql://db/workflows",
        database_backend=DatabaseBackend.POSTGRESQL,
        database_path=None,
        jobstate_path=tmp_path / "jobstate.log",
        jobstate_path_overridden=False,
    )

    result = StampedeReader(location).refresh(_request())

    assert result.snapshot is None
    assert result.health.error_code == "unsupported_database_backend"
    assert not placeholder.exists()


def test_connection_enforces_query_only(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)

    connection = reader._open_read_only()
    try:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE monitor_wrote_this (id INTEGER)")
    finally:
        connection.close()
    with _writer(path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='monitor_wrote_this'"
            ).fetchone()[0]
            == 0
        )


def test_read_does_not_change_mode_size_mtime_or_create_sidecars(tmp_path):
    directory = tmp_path / "readonly"
    directory.mkdir()
    path = _create_database(directory / "workflow.db")
    before = path.stat()
    os.chmod(path, 0o444)
    os.chmod(directory, 0o555)
    try:
        result = StampedeReader(path, ROOT).refresh(_request())
        after = path.stat()
        names = {item.name for item in directory.iterdir()}
    finally:
        os.chmod(directory, 0o755)
        os.chmod(path, 0o644)

    assert result.snapshot is not None
    assert (after.st_mode, after.st_size, after.st_mtime_ns) == (
        before.st_mode & ~0o222,
        before.st_size,
        before.st_mtime_ns,
    )
    assert names == {"workflow.db"}


def test_wal_writer_can_commit_while_reader_takes_snapshot(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    writer = _writer(path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO jobstate VALUES (200, 'JOB_RELEASED', 150, 3, NULL)"
        )

        result = StampedeReader(path, ROOT).refresh(_request())

        assert result.snapshot is not None
        writer.commit()
    finally:
        writer.close()
    confirmed = StampedeReader(path, ROOT).refresh(_request()).snapshot
    assert confirmed is not None
    assert confirmed.jobs[1].state == "JOB_RELEASED"


@pytest.mark.parametrize("journal_mode", ["WAL", "DELETE"])
def test_concurrent_writer_commit_finishes_within_busy_bound(tmp_path, journal_mode):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as setup:
        assert (
            setup.execute(f"PRAGMA journal_mode={journal_mode}").fetchone()[0].lower()
            == journal_mode.lower()
        )

    read_started = threading.Event()
    release_reader = threading.Event()
    results: Queue[object] = Queue()

    class CoordinatedReader(StampedeReader):
        def _load_workflow_row(self, connection, workflow):
            read_started.set()
            if not release_reader.wait(timeout=1.0):
                raise AssertionError("test reader was not released")
            return super()._load_workflow_row(connection, workflow)

    reader = CoordinatedReader(path, ROOT, busy_timeout_seconds=0.25)

    def read_snapshot():
        try:
            results.put(reader.refresh(_request()))
        except BaseException as error:  # pragma: no cover - surfaced below
            results.put(error)

    read_thread = threading.Thread(target=read_snapshot)
    read_thread.start()
    assert read_started.wait(timeout=1.0)

    writer = sqlite3.connect(path, timeout=0.5)
    timer = threading.Timer(0.05, release_reader.set)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE workflow SET dag_file_name = dag_file_name")
        timer.start()
        started = time.monotonic()
        writer.commit()
        commit_elapsed = time.monotonic() - started
    finally:
        release_reader.set()
        timer.cancel()
        writer.close()
    read_thread.join(timeout=1.0)

    assert not read_thread.is_alive()
    outcome = results.get_nowait()
    if isinstance(outcome, BaseException):
        raise outcome
    assert outcome.snapshot is not None
    assert commit_elapsed < 0.5


def test_locked_full_refresh_degrades_and_retains_last_good(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT, busy_timeout_seconds=0.01)
    first = reader.refresh(_request())
    lock = _writer(path)
    try:
        lock.execute("BEGIN EXCLUSIVE")
        lock.execute("UPDATE workflow SET dag_file_name = dag_file_name || '.locked'")
        result = reader.refresh(
            _request(
                epoch=2,
                mode=DBRefreshMode.FULL_REBOOTSTRAP,
            )
        )
    finally:
        lock.rollback()
        lock.close()

    assert result.snapshot is None
    assert result.health.state is HealthState.STALE
    assert result.health.error_code == "database_busy"
    assert reader.last_good_snapshot is first.snapshot


def test_inode_replacement_requires_full_rebootstrap(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    replacement = _create_database(tmp_path / "replacement.db")
    os.replace(replacement, path)

    changed = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert changed.snapshot is None
    assert changed.health.state is HealthState.RESYNC
    assert changed.health.error_code == "database_generation_changed"
    assert changed.generation != first.generation

    rebuilt = reader.refresh(_request(epoch=3, mode=DBRefreshMode.FULL_REBOOTSTRAP))
    assert rebuilt.snapshot is not None
    assert rebuilt.generation == changed.generation


def test_transition_watermark_rollback_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM jobstate WHERE job_instance_id = 100 "
            "AND jobstate_submit_seq = 3"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "transition_watermark_rollback"
    assert result.generation != first.generation

    rebuilt = reader.refresh(_request(epoch=3, mode=DBRefreshMode.FULL_REBOOTSTRAP))
    assert rebuilt.snapshot is not None
    assert rebuilt.generation == result.generation


def test_same_sequence_identity_group_rollback_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM jobstate WHERE job_instance_id = 100 "
            "AND state = 'JOB_FAILURE' AND jobstate_submit_seq = 3"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.error_code == "transition_watermark_rollback"


def test_same_cursor_transition_replacement_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "UPDATE jobstate SET state = 'JOB_SUCCESS' "
            "WHERE job_instance_id = 100 AND state = 'JOB_FAILURE' "
            "AND jobstate_submit_seq = 3"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "transition_watermark_rollback"
    assert result.generation != first.generation


def test_large_workflow_same_cursor_update_forces_audit_without_more_writes(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    monkeypatch.setattr(stampede_module, "_LARGE_WORKFLOW_AUDIT_THRESHOLD", 0)
    monkeypatch.setattr(stampede_module, "_CURSOR_STABLE_AUDIT_INTERVAL", 2)
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "UPDATE jobstate SET state = 'JOB_SUCCESS' "
            "WHERE job_instance_id = 100 AND state = 'JOB_FAILURE' "
            "AND jobstate_submit_seq = 3"
        )

    cached = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )
    audited = reader.refresh(
        _request(
            epoch=3,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert cached.snapshot is not None
    assert cached.snapshot.jobs[0].state == "JOB_FAILURE"
    assert audited.snapshot is None
    assert audited.health.state is HealthState.RESYNC
    assert audited.health.error_code == "transition_watermark_rollback"
    assert audited.generation != first.generation


def test_explicit_full_rebootstrap_accepts_rollback_as_new_generation(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM jobstate WHERE job_instance_id = 100 "
            "AND jobstate_submit_seq = 3"
        )

    rebuilt = reader.refresh(_request(epoch=2, mode=DBRefreshMode.FULL_REBOOTSTRAP))

    assert rebuilt.snapshot is not None
    assert rebuilt.generation != first.generation
    assert rebuilt.snapshot.jobs[0].state == "EXECUTE"


def test_roster_rollback_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute("DELETE FROM invocation WHERE job_instance_id = 200")
        connection.execute("DELETE FROM task WHERE job_id = 20")
        connection.execute("DELETE FROM jobstate WHERE job_instance_id = 200")
        connection.execute("DELETE FROM job_instance WHERE job_id = 20")
        connection.execute("DELETE FROM job WHERE job_id = 20")

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.error_code == "database_roster_rollback"


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE job SET exec_job_id = 'replacement_job' WHERE job_id = 20",
        "UPDATE job_instance SET job_submit_seq = 99 WHERE job_instance_id = 200",
    ],
)
def test_roster_identity_replacement_is_rejected(tmp_path, statement):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(statement)

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "database_roster_rollback"
    assert result.generation != first.generation
    assert reader.last_good_snapshot is first.snapshot


def test_workflow_transition_rollback_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM workflowstate WHERE state = 'WORKFLOW_TERMINATED'"
        )

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.error_code == "workflow_watermark_rollback"


def test_workflow_start_identity_regression_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute("DELETE FROM workflowstate WHERE state = 'WORKFLOW_STARTED'")

    result = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.state is HealthState.RESYNC
    assert result.health.error_code == "workflow_watermark_rollback"
    assert result.generation != first.generation
    assert reader.last_good_snapshot is first.snapshot


def test_prior_restart_workflow_identity_regression_is_rejected(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    first = reader.refresh(_request())
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 400, 1, NULL, 'restart')"
        )
    second = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )
    assert second.snapshot is not None
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM workflowstate WHERE restart_count = 0 "
            "AND state = 'WORKFLOW_STARTED'"
        )

    result = reader.refresh(
        _request(
            epoch=3,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=second.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.error_code == "workflow_watermark_rollback"
    assert result.generation != second.generation


def test_sqlite_header_detects_mutation_with_coarse_file_timestamps(
    tmp_path, monkeypatch
):
    path = _create_database(tmp_path / "workflow.db")
    reader = StampedeReader(path, ROOT)
    original = reader._stat_database

    def coarse_marker():
        marker = original()
        return replace(marker, mtime_ns=0, wal_mtime_ns=0)

    monkeypatch.setattr(reader, "_stat_database", coarse_marker)
    first = reader.refresh(_request())
    assert first.snapshot is not None
    first_header = reader._last_snapshot_marker.header
    with _writer(path) as connection:
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 400, 1, NULL, 'restart')"
        )
    second = reader.refresh(
        _request(
            epoch=2,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=first.generation,
        )
    )
    assert second.snapshot is not None
    assert reader._last_snapshot_marker.header != first_header
    with _writer(path) as connection:
        connection.execute(
            "DELETE FROM workflowstate WHERE restart_count = 0 "
            "AND state = 'WORKFLOW_STARTED'"
        )

    result = reader.refresh(
        _request(
            epoch=3,
            mode=DBRefreshMode.CURRENT_SNAPSHOT,
            prior=second.generation,
        )
    )

    assert result.snapshot is None
    assert result.health.error_code == "workflow_watermark_rollback"


def test_rollback_journal_database_is_observable(tmp_path):
    path = _create_database(tmp_path / "workflow.db")
    with _writer(path) as connection:
        assert (
            connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        )

    result = StampedeReader(path, ROOT).refresh(_request())

    assert result.snapshot is not None
