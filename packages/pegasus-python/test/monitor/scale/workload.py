"""Deterministic, generated inputs for the pegasus-monitor scale gate."""

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

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

WORKFLOW_UUID = "scale-root-uuid"


@dataclass(frozen=True, slots=True)
class ScaleConfig:
    """One reproducible workload profile.

    ``fast`` is the ordinary pytest smoke. ``default`` is a substantial local
    or CI probe. ``full`` is the release gate and retains the exact fixed
    acceptance cardinalities.
    """

    mode: str
    jobs: int
    attempts: int
    transitions: int
    prefilled_tail_lines: int
    burst_rate: int
    burst_lines: int
    refresh_samples: int
    writer_trials: int
    writer_warmups: int
    writer_operations: int
    writer_duration_seconds: float
    probe_samples: int

    def to_dict(self) -> dict[str, float | int | str]:
        return asdict(self)


PROFILES = {
    "fast": ScaleConfig(
        "fast", 250, 250, 2_500, 10_000, 1_000, 100, 4, 3, 1, 40, 0.5, 1
    ),
    "default": ScaleConfig(
        "default",
        10_000,
        10_000,
        100_000,
        100_000,
        1_000,
        500,
        40,
        5,
        1,
        500,
        2.5,
        2,
    ),
    "full": ScaleConfig(
        "full",
        100_000,
        100_000,
        1_000_000,
        1_000_000,
        1_000,
        1_000,
        40,
        9,
        1,
        2_000,
        4.0,
        3,
    ),
}


def selected_mode(value: str | None = None) -> str:
    """Resolve ``PEGASUS_MONITOR_RUN_SCALE`` without surprising normal tests."""

    raw = os.environ.get("PEGASUS_MONITOR_RUN_SCALE") if value is None else value
    normalized = (raw or "fast").strip().lower()
    aliases = {
        "": "fast",
        "0": "fast",
        "false": "fast",
        "no": "fast",
        "1": "default",
        "true": "default",
        "yes": "default",
    }
    mode = aliases.get(normalized, normalized)
    if mode not in PROFILES:
        choices = ", ".join(PROFILES)
        raise ValueError(f"unknown scale mode {raw!r}; expected one of {choices}")
    return mode


def scale_config(value: str | None = None) -> ScaleConfig:
    return PROFILES[selected_mode(value)]


SCHEMA = """
CREATE TABLE dbversion (
    id INTEGER PRIMARY KEY,
    version_number INTEGER NOT NULL,
    version TEXT NOT NULL,
    version_timestamp INTEGER NOT NULL
);
INSERT INTO dbversion VALUES (1, 14, '14', 0);
CREATE TABLE workflow (
    wf_id INTEGER PRIMARY KEY,
    wf_uuid TEXT NOT NULL UNIQUE,
    root_wf_id INTEGER,
    dag_file_name TEXT,
    submit_dir TEXT
);
CREATE TABLE workflowstate (
    wf_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    timestamp NUMERIC NOT NULL,
    restart_count INTEGER NOT NULL,
    status INTEGER,
    reason TEXT,
    PRIMARY KEY (wf_id, state, timestamp)
);
CREATE TABLE job (
    job_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    exec_job_id TEXT NOT NULL,
    type_desc TEXT NOT NULL,
    task_count INTEGER NOT NULL,
    UNIQUE (wf_id, exec_job_id)
);
CREATE TABLE job_instance (
    job_instance_id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    job_submit_seq INTEGER NOT NULL,
    sched_id TEXT,
    site TEXT,
    stdout_file TEXT,
    stderr_file TEXT,
    exitcode INTEGER,
    UNIQUE (job_id, job_submit_seq)
);
CREATE TABLE jobstate (
    job_instance_id INTEGER NOT NULL,
    state TEXT NOT NULL,
    timestamp NUMERIC NOT NULL,
    jobstate_submit_seq INTEGER NOT NULL,
    reason TEXT,
    PRIMARY KEY (job_instance_id, state, timestamp, jobstate_submit_seq)
);
CREATE INDEX jobstate_jobstate_submit_seq_COL
    ON jobstate(jobstate_submit_seq);
CREATE TABLE task (
    task_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    job_id INTEGER,
    transformation TEXT NOT NULL
);
CREATE TABLE invocation (
    invocation_id INTEGER PRIMARY KEY,
    wf_id INTEGER NOT NULL,
    job_instance_id INTEGER NOT NULL,
    task_submit_seq INTEGER NOT NULL,
    maxrss INTEGER
);
CREATE TABLE writer_probe (
    sequence INTEGER PRIMARY KEY,
    written_at NUMERIC NOT NULL
);
"""

_STATES = (
    "SUBMIT",
    "EXECUTE",
    "IMAGE_SIZE",
    "JOB_HELD",
    "JOB_RELEASED",
    "EXECUTE",
    "JOB_EVICTED",
    "EXECUTE",
    "JOB_TERMINATED",
    "JOB_SUCCESS",
)


@dataclass(frozen=True, slots=True)
class GeneratedWorkload:
    database_path: Path
    jobstate_path: Path
    fixture_sha256: str
    manifest: dict[str, object]


def _chunks(total: int, size: int = 5_000):
    for start in range(0, total, size):
        yield range(start, min(total, start + size))


def _populate_database(path: Path, config: ScaleConfig) -> None:
    if config.attempts != config.jobs:
        raise ValueError("the v1 generated workload requires one attempt per job")
    if config.transitions % config.attempts:
        raise ValueError("transitions must divide evenly across attempts")
    per_attempt = config.transitions // config.attempts
    if per_attempt > len(_STATES):
        raise ValueError("generated state sequence is too short")

    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA)
        connection.execute(
            "INSERT INTO workflow VALUES (1, ?, 1, 'scale-0.dag', ?)",
            (WORKFLOW_UUID, "/scale/submit"),
        )
        connection.execute(
            "INSERT INTO workflowstate VALUES "
            "(1, 'WORKFLOW_STARTED', 1000, 0, NULL, NULL)"
        )
        connection.commit()
        connection.execute("BEGIN")
        for indexes in _chunks(config.jobs):
            connection.executemany(
                "INSERT INTO job VALUES (?,?,?,?,?)",
                (
                    (index + 1, 1, f"scale_ID{index:07d}", "compute", 1)
                    for index in indexes
                ),
            )
            connection.executemany(
                "INSERT INTO job_instance VALUES (?,?,?,?,?,?,?,?)",
                (
                    (
                        index + 1,
                        index + 1,
                        index + 1,
                        f"{index + 10_000}.0",
                        "local",
                        f"scale_ID{index:07d}.out.000",
                        f"scale_ID{index:07d}.err",
                        0,
                    )
                    for index in indexes
                ),
            )
            connection.executemany(
                "INSERT INTO task VALUES (?,?,?,?)",
                ((index + 1, 1, index + 1, "scale::compute") for index in indexes),
            )
            connection.executemany(
                "INSERT INTO invocation VALUES (?,?,?,?,?)",
                ((index + 1, 1, index + 1, 1, 64 * 1024) for index in indexes),
            )
            states = []
            for index in indexes:
                for sequence, state in enumerate(_STATES[:per_attempt], start=1):
                    states.append(
                        (
                            index + 1,
                            state,
                            1001 + index * per_attempt + sequence,
                            sequence,
                            None,
                        )
                    )
            connection.executemany("INSERT INTO jobstate VALUES (?,?,?,?,?)", states)
        connection.commit()
        connection.execute("ANALYZE")
        connection.commit()
    finally:
        connection.close()


def _write_prefilled_tail(path: Path, config: ScaleConfig) -> str:
    digest = hashlib.sha256()
    with path.open("wb", buffering=1024 * 1024) as stream:
        for indexes in _chunks(config.prefilled_tail_lines, 10_000):
            block = bytearray()
            for index in indexes:
                job_index = index % config.jobs
                line = (
                    f"{1001 + index} scale_ID{job_index:07d} SUBMIT "
                    f"{job_index + 10_000}.0 local - {job_index + 1}\n"
                ).encode()
                block.extend(line)
            stream.write(block)
            digest.update(block)
    return digest.hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _fixture_hash(manifest: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def inspect_workload(workload: GeneratedWorkload) -> dict[str, int]:
    """Measure source cardinalities without trusting generator configuration."""

    uri = f"file:{workload.database_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        transition_count = int(
            connection.execute("SELECT COUNT(*) FROM jobstate").fetchone()[0]
        )
    finally:
        connection.close()
    tail_lines = 0
    with workload.jobstate_path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            tail_lines += block.count(b"\n")
    return {
        "database_transitions": transition_count,
        "tail_lines": tail_lines,
    }


def validate_workload(
    workload: GeneratedWorkload, config: ScaleConfig
) -> dict[str, int]:
    """Validate the manifest, source bytes, expected hash, and cardinalities."""

    if workload.manifest.get("config") != config.to_dict():
        raise RuntimeError("fixture manifest configuration does not match the mode")
    database_hash = _hash_file(workload.database_path)
    tail_hash = _hash_file(workload.jobstate_path)
    if database_hash != workload.manifest.get("database_sha256"):
        raise RuntimeError("fixture database SHA-256 validation failed")
    if tail_hash != workload.manifest.get("prefilled_tail_sha256"):
        raise RuntimeError("fixture jobstate SHA-256 validation failed")
    actual_hash = _fixture_hash(workload.manifest)
    if actual_hash != workload.fixture_sha256:
        raise RuntimeError("fixture SHA-256 does not match the validated manifest")
    return inspect_workload(workload)


def generate_workload(directory: Path, config: ScaleConfig) -> GeneratedWorkload:
    """Generate the SQLite and tail fixtures; no large artifact is committed."""

    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "scale.stampede.db"
    jobstate = directory / "jobstate.log"
    _populate_database(database, config)
    tail_hash = _write_prefilled_tail(jobstate, config)
    database_hash = _hash_file(database)
    manifest: dict[str, object] = {
        "generator_version": 1,
        "config": config.to_dict(),
        "schema_sha256": hashlib.sha256(SCHEMA.encode()).hexdigest(),
        "database_sha256": database_hash,
        "prefilled_tail_sha256": tail_hash,
        "sqlite_version": sqlite3.sqlite_version,
    }
    fixture_hash = _fixture_hash(manifest)
    (directory / "fixture-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return GeneratedWorkload(database, jobstate, fixture_hash, manifest)
