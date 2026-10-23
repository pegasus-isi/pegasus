"""Smoke-report regressions through real DB, coordinator, and CLI rendering."""

from __future__ import annotations

import io
import sqlite3
from dataclasses import replace

import pytest

from .conftest import create_run

from Pegasus.monitor import cli
from Pegasus.monitor.models import (
    FrozenPayload,
    HealthState,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryResult,
    SourceHealth,
    SourceName,
)


@pytest.mark.parametrize("width", [120, 140])
@pytest.mark.parametrize("diagnose", [False, True])
def test_removed_workflow_final_snapshot_queries_history(tmp_path, width, diagnose):
    run = tmp_path / "run0001"
    database, _ = create_run(run, terminated=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE workflowstate SET status=1 WHERE state='WORKFLOW_TERMINATED'"
        )
        connection.execute(
            "INSERT INTO jobstate VALUES (100, 'JOB_HELD', 290, 2, 'manual hold')"
        )
    queried = []

    class Scheduler:
        def query(self, request):
            queried.append(request.kind)
            rows = []
            source = SourceName.CONDOR_QUEUE
            if request.kind is SchedulerQueryKind.HISTORY:
                source = SourceName.CONDOR_HISTORY
                rows = [
                    {
                        "ClusterId": 999,
                        "ProcId": 0,
                        "JobUniverse": 7,
                        "JobStatus": 3,
                        "QDate": 95,
                        "CompletionDate": 300,
                        "RemoveReason": "Removed by user alice",
                    },
                    {
                        "ClusterId": 1000,
                        "ProcId": 0,
                        "DAGNodeName": "cluster_ID0000001",
                        "JobStatus": 3,
                        "RemoveReason": "Removed because DAGMan was removed",
                    },
                ]
            return SchedulerQueryResult(
                request,
                SourceHealth(source, HealthState.HEALTHY, request.clock.epoch),
                0.0,
                evidence=tuple(
                    SchedulerEvidence(
                        request.kind,
                        FrozenPayload.from_mapping(
                            {k: row[k] for k in ("ClusterId", "ProcId")}
                        ),
                        FrozenPayload.from_mapping(row),
                    )
                    for row in rows
                ),
            )

    runtime = replace(cli._load_runtime(), scheduler_factory=lambda **_: Scheduler())
    output, errors = io.StringIO(), io.StringIO()
    output.width = width
    args = [
        "--once",
        "--no-live-events",
        str(run),
        *(["--diagnose"] if diagnose else []),
    ]
    assert cli.main(args, runtime=runtime, stdout=output, stderr=errors) == 0, (
        errors.getvalue()
    )
    assert queried == [SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY]
    text = output.getvalue()
    assert "REMOVED" in text
    assert "Removed by user alice" in text
    assert "removed 1 (HTCondor)" in text
    assert "held 1" not in text
    if diagnose:
        assert "Removed because DAGMan was removed" in text
