"""Shared, on-disk fixtures for pegasus-monitor integration tests."""

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
import sqlite3
from pathlib import Path

import pytest

from Pegasus.monitor.models import WorkflowIdentity

ROOT = WorkflowIdentity("root-uuid", "root-uuid")
CHILD = WorkflowIdentity("child-uuid", "root-uuid")
SCHEMA = Path(__file__).parents[1] / "fixtures" / "stampede" / "schema.sql"


def append_line(path: Path, line: str) -> None:
    with path.open("ab", buffering=0) as stream:
        stream.write(line.encode("utf-8"))


def job_line(
    timestamp: int,
    state: str,
    *,
    job: str = "cluster_ID0000001",
    value: str = "1000.0",
    submit_sequence: int = 1,
) -> str:
    return f"{timestamp} {job} {state} {value} local - {submit_sequence}\n"


def insert_state(
    path: Path,
    instance_id: int,
    state: str,
    timestamp: str | int,
    sequence: int,
    reason: str | None = None,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO jobstate VALUES (?,?,?,?,?)",
            (instance_id, state, timestamp, sequence, reason),
        )


def create_database(
    path: Path,
    *,
    terminated: bool = False,
    include_child: bool = False,
    include_retry: bool = False,
    include_held: bool = False,
) -> Path:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO workflow VALUES (1, 'root-uuid', 1, 'diamond-0.dag', ?)",
            (str(path.parent),),
        )
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 100, 0, NULL, NULL)"
        )
        if terminated:
            connection.execute(
                "INSERT INTO workflowstate VALUES "
                "(1, 'WORKFLOW_TERMINATED', 300, 0, 0, NULL)"
            )
        connection.execute(
            "INSERT INTO job VALUES (10, 1, 'cluster_ID0000001', 'compute', 2)"
        )
        connection.execute(
            "INSERT INTO job_instance VALUES "
            "(100, 10, 1, '1000.0', 'local', NULL, NULL, NULL)"
        )
        connection.execute("INSERT INTO jobstate VALUES (100, 'SUBMIT', 110, 1, NULL)")
        connection.executemany(
            "INSERT INTO task VALUES (?,?,?,?)",
            (
                (1, 1, 10, "example::alpha"),
                (2, 1, 10, "example::beta"),
            ),
        )
        if include_retry:
            connection.execute(
                "INSERT INTO job_instance VALUES "
                "(101, 10, 4, '1004.0', 'local', NULL, NULL, 0)"
            )
            connection.execute(
                "INSERT INTO jobstate VALUES (101, 'SUBMIT', 140, 1, NULL)"
            )
            connection.execute(
                "INSERT INTO jobstate VALUES (101, 'JOB_SUCCESS', 160, 2, NULL)"
            )
        if include_held:
            connection.execute(
                "INSERT INTO job VALUES (20, 1, 'held_ID0000002', 'compute', 1)"
            )
            connection.execute(
                "INSERT INTO job_instance VALUES "
                "(200, 20, 2, '1001.0', 'local', NULL, NULL, NULL)"
            )
            connection.execute(
                "INSERT INTO jobstate VALUES (200, 'SUBMIT', 111, 1, NULL)"
            )
            connection.execute(
                "INSERT INTO jobstate VALUES (200, 'JOB_HELD', 125, 2, 'operator hold')"
            )
            connection.execute("INSERT INTO task VALUES (3, 1, 20, 'example::held')")
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


def create_run(
    directory: Path,
    *,
    terminated: bool = True,
    create_tail: bool = True,
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    database = create_database(
        directory / "diamond-0.stampede.db", terminated=terminated
    )
    braindump = {
        "user": "integration-user",
        "wf_uuid": ROOT.wf_uuid,
        "root_wf_uuid": ROOT.root_wf_uuid,
        "submit_dir": str(directory),
        "basedir": str(directory.parent),
        "rundir": directory.name,
        "dag": "diamond-0.dag",
        "jsd": "jobstate.log",
        "dax_label": "integration-diamond",
        "planner_version": "5.2.0-dev",
        "timestamp": "20260818T120000-0400",
        "properties": None,
    }
    (directory / "braindump.yml").write_text(json.dumps(braindump), encoding="utf-8")
    tail = directory / "jobstate.log"
    if create_tail:
        tail.write_text("100 INTERNAL *** MONITORD_STARTED ***\n", encoding="utf-8")
    return database, tail


@pytest.fixture
def workflow_database(tmp_path: Path) -> Path:
    return create_database(tmp_path / "workflow.db")
