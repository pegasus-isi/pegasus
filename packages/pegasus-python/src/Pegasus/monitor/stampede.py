"""Bounded, read-only Stampede SQLite snapshots for ``pegasus-monitor``."""

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

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

from .locator import DatabaseBackend, WorkflowLocation
from .models import (
    BoundedAge,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBRefreshMode,
    DBRefreshRequest,
    DBRefreshResult,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSemanticKey,
    JobSnapshot,
    JobTransitionWatermark,
    Provenance,
    SourceHealth,
    SourceName,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionWatermark,
    db_timestamp,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from decimal import Decimal

CURRENT_STAMPEDE_SCHEMA_VERSION = 14

_STALE_AFTER_SECONDS = 10.0
_MAX_HEALTH_AGE_SECONDS = 30 * 24 * 60 * 60.0
_SUFFIX_THRESHOLD_BATCH_SIZE = 400
_TERMINAL_MAIN_JOB_STATES = (
    "JOB_SUCCESS",
    "JOB_FAILURE",
    "JOB_FAILED",
    "JOB_ABORTED",
    "SUBMIT_FAILED",
    "GRID_SUBMIT_FAILED",
    "GLOBUS_SUBMIT_FAILED",
)
_SUBMIT_STATES = ("SUBMIT", "GRID_SUBMIT", "GLOBUS_SUBMIT")

_REQUIRED_COLUMNS = {
    "dbversion": {"version_number"},
    "workflow": {"wf_id", "wf_uuid", "root_wf_id"},
    "workflowstate": {
        "wf_id",
        "state",
        "timestamp",
        "restart_count",
        "status",
        "reason",
    },
    "job": {"job_id", "wf_id", "exec_job_id", "type_desc", "task_count"},
    "job_instance": {
        "job_instance_id",
        "job_id",
        "job_submit_seq",
        "sched_id",
        "site",
        "stdout_file",
        "stderr_file",
        "exitcode",
    },
    "jobstate": {
        "job_instance_id",
        "state",
        "timestamp",
        "jobstate_submit_seq",
        "reason",
    },
    "task": {"task_id", "wf_id", "job_id", "transformation"},
    "invocation": {
        "wf_id",
        "job_instance_id",
        "task_submit_seq",
        "maxrss",
    },
}

_STATE_PRECEDENCE_SQL = """
CASE js.state
    WHEN 'PRE_SCRIPT_STARTED' THEN 10
    WHEN 'PRE_SCRIPT_TERMINATED' THEN 11
    WHEN 'PRE_SCRIPT_SUCCESS' THEN 12
    WHEN 'SUBMIT' THEN 20
    WHEN 'GRID_SUBMIT' THEN 20
    WHEN 'GLOBUS_SUBMIT' THEN 20
    WHEN 'JOB_RELEASED' THEN 25
    WHEN 'EXECUTE' THEN 30
    WHEN 'IMAGE_SIZE' THEN 31
    WHEN 'REMOTE_ERROR' THEN 32
    WHEN 'JOB_HELD' THEN 40
    WHEN 'JOB_EVICTED' THEN 45
    WHEN 'JOB_TERMINATED' THEN 50
    WHEN 'POST_SCRIPT_STARTED' THEN 60
    WHEN 'POST_SCRIPT_TERMINATED' THEN 61
    WHEN 'POST_SCRIPT_SUCCESS' THEN 70
    WHEN 'PRE_SCRIPT_FAILED' THEN 80
    WHEN 'POST_SCRIPT_FAILED' THEN 80
    WHEN 'SUBMIT_FAILED' THEN 80
    WHEN 'GRID_SUBMIT_FAILED' THEN 80
    WHEN 'GLOBUS_SUBMIT_FAILED' THEN 80
    WHEN 'JOB_ABORTED' THEN 85
    WHEN 'JOB_FAILED' THEN 90
    WHEN 'JOB_FAILURE' THEN 100
    WHEN 'JOB_SUCCESS' THEN 100
    ELSE 0
END
"""


class StampedeReadError(RuntimeError):
    """A safe, non-mutating Stampede read could not be completed."""

    def __init__(self, code: str, detail: str, *, state: HealthState) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.state = state


@dataclass(frozen=True, slots=True)
class _FileMarker:
    device: int
    inode: int
    size: int


@dataclass(frozen=True, slots=True)
class _WorkflowRow:
    wf_id: int
    root_uuid: str


@dataclass(frozen=True, slots=True)
class _JobRow:
    job_id: int
    exec_job_id: str
    type_desc: str
    declared_task_count: int


@dataclass(frozen=True, slots=True)
class _AttemptRow:
    job_id: int
    job_instance_id: int
    job_submit_seq: int
    sched_id: str | None
    site: str | None
    stdout_file: str | None
    stderr_file: str | None
    raw_wait_status: int | None


@dataclass(frozen=True, slots=True)
class _LoadedSnapshot:
    snapshot: DatabaseSnapshot
    heads: dict[int, tuple[DBJobTransition, ...]]
    timings: dict[int, tuple[Decimal | None, Decimal | None, Decimal | None]]
    maxrss: dict[int, int]
    workflow_identities: frozenset[DBWorkflowTransitionIdentity]


def decode_wait_status(raw_wait_status: int | None) -> int | None:
    """Decode Stampede's raw POSIX wait status like Pegasus ``raw_to_regular``."""

    if raw_wait_status is None:
        return None
    if raw_wait_status < 0:
        return -128
    signal = raw_wait_status & 127
    if signal:
        return -signal
    return raw_wait_status >> 8


def _decimal(value: object, field: str) -> Decimal:
    if value is None:
        raise StampedeReadError(
            "malformed_row",
            f"Stampede {field} is NULL",
            state=HealthState.DEGRADED,
        )
    try:
        return db_timestamp(value)  # type: ignore[arg-type]
    except ValueError as error:
        raise StampedeReadError(
            "malformed_row",
            f"invalid Stampede {field}: {value!r}",
            state=HealthState.DEGRADED,
        ) from error


def _int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise StampedeReadError(
            "malformed_row",
            f"invalid Stampede {field}: {value!r}",
            state=HealthState.DEGRADED,
        )
    try:
        result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise StampedeReadError(
            "malformed_row",
            f"invalid Stampede {field}: {value!r}",
            state=HealthState.DEGRADED,
        ) from error
    if result < 0:
        raise StampedeReadError(
            "malformed_row",
            f"negative Stampede {field}: {result}",
            state=HealthState.DEGRADED,
        )
    return result


def _optional_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise StampedeReadError(
            "malformed_row",
            f"invalid Stampede {field}: {value!r}",
            state=HealthState.DEGRADED,
        ) from error


def _transition_from_row(
    row: sqlite3.Row, workflow: WorkflowIdentity
) -> DBJobTransition:
    return DBJobTransition(
        workflow=workflow,
        exec_job_id=str(row["exec_job_id"]),
        job_submit_seq=_int(row["job_submit_seq"], "job_submit_seq"),
        identity=DBTransitionIdentity(
            job_instance_id=_int(row["job_instance_id"], "job_instance_id"),
            state=str(row["state"]),
            timestamp=_decimal(row["timestamp"], "jobstate.timestamp"),
            jobstate_submit_seq=_int(row["jobstate_submit_seq"], "jobstate_submit_seq"),
        ),
        reason=row["reason"],
    )


def _workflow_transition_from_row(
    row: sqlite3.Row, workflow: WorkflowIdentity
) -> DBWorkflowTransition:
    return DBWorkflowTransition(
        workflow=workflow,
        identity=DBWorkflowTransitionIdentity(
            wf_id=_int(row["wf_id"], "wf_id"),
            state=str(row["state"]),
            timestamp=_decimal(row["timestamp"], "workflowstate.timestamp"),
        ),
        restart_count=_int(row["restart_count"], "restart_count"),
        status=_optional_int(row["status"], "workflow status"),
        reason=row["reason"],
    )


class StampedeReader:
    """One-call-at-a-time reader implementing ``StampedeSnapshotProvider``.

    It owns no clock, task, thread, timer, or scheduling policy.  A connection
    exists only for the duration of :meth:`refresh` and is opened with SQLite's
    URI read-only mode and ``query_only`` protection.
    """

    def __init__(
        self,
        database: WorkflowLocation | str | Path,
        workflow: WorkflowIdentity | None = None,
        *,
        busy_timeout_seconds: float = 0.25,
    ) -> None:
        if busy_timeout_seconds <= 0:
            raise ValueError("busy_timeout_seconds must be positive")
        if isinstance(database, WorkflowLocation):
            self.database_path = database.database_path
            self.database_backend = database.database_backend
            self.workflow = database.workflow
        else:
            self.database_path = Path(database).expanduser()
            self.database_backend = DatabaseBackend.SQLITE
            self.workflow = workflow
        self.busy_timeout_seconds = busy_timeout_seconds
        self._last_marker: _FileMarker | None = None
        self._generation: DatabaseGeneration | None = None
        self._generation_counter = 0
        self._last_good: DatabaseSnapshot | None = None
        self._last_heads: dict[int, tuple[DBJobTransition, ...]] = {}
        self._last_timings: dict[
            int, tuple[Decimal | None, Decimal | None, Decimal | None]
        ] = {}
        self._last_maxrss: dict[int, int] = {}
        self._last_workflow_identities: frozenset[DBWorkflowTransitionIdentity] = (
            frozenset()
        )
        self._last_success_epoch: float | None = None
        self._consecutive_failures = 0

    @property
    def last_good_snapshot(self) -> DatabaseSnapshot | None:
        return self._last_good

    def refresh(self, request: DBRefreshRequest) -> DBRefreshResult:
        if self.workflow is not None and request.workflow != self.workflow:
            return self._failure(
                request,
                StampedeReadError(
                    "workflow_scope_mismatch",
                    "refresh workflow does not match the located workflow",
                    state=HealthState.UNAVAILABLE,
                ),
                self._generation,
            )
        if self.database_backend is not DatabaseBackend.SQLITE:
            return self._failure(
                request,
                StampedeReadError(
                    "unsupported_database_backend",
                    f"v1 supports SQLite, not {self.database_backend.value}",
                    state=HealthState.UNAVAILABLE,
                ),
                None,
            )
        if self.database_path is None:
            return self._failure(
                request,
                StampedeReadError(
                    "database_path_unavailable",
                    "no local SQLite path was resolved",
                    state=HealthState.UNAVAILABLE,
                ),
                None,
            )

        try:
            marker = self._stat_database()
            generation = self._observe_generation(marker)
            if (
                request.mode is not DBRefreshMode.FULL_REBOOTSTRAP
                and generation != request.prior_generation
            ):
                raise StampedeReadError(
                    "database_generation_changed",
                    "Stampede file changed; a full rebootstrap is required",
                    state=HealthState.RESYNC,
                )
            if request.mode is DBRefreshMode.BOUNDED_SUFFIX and (
                self._last_good is None
                or self._last_good.generation != request.prior_generation
            ):
                raise StampedeReadError(
                    "missing_base_snapshot",
                    "bounded suffix refresh has no matching last-good base",
                    state=HealthState.RESYNC,
                )

            connection = self._open_read_only()
            try:
                connection.execute("BEGIN")
                self._check_schema(connection)
                loaded = self._load_snapshot(connection, request, generation)
                snapshot = loaded.snapshot
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

            after = self._stat_database()
            if (after.device, after.inode) != (marker.device, marker.inode):
                self._observe_generation(after)
                raise StampedeReadError(
                    "database_replaced_during_read",
                    "Stampede file was replaced during the snapshot transaction",
                    state=HealthState.RESYNC,
                )
            if after.size < marker.size:
                self._observe_generation(after)
                raise StampedeReadError(
                    "database_shrank_during_read",
                    "Stampede file shrank during the snapshot transaction",
                    state=HealthState.RESYNC,
                )
            self._last_marker = after
            try:
                self._reject_watermark_rollback(snapshot, loaded.workflow_identities)
            except StampedeReadError:
                new_generation = self._advance_logical_generation(after)
                if request.mode is not DBRefreshMode.FULL_REBOOTSTRAP:
                    raise
                snapshot = replace(snapshot, generation=new_generation)
                loaded = replace(loaded, snapshot=snapshot)
        except StampedeReadError as error:
            return self._failure(request, error, self._generation)
        except sqlite3.OperationalError as error:
            message = str(error)
            locked = "locked" in message.lower() or "busy" in message.lower()
            return self._failure(
                request,
                StampedeReadError(
                    "database_busy" if locked else "database_io_error",
                    message,
                    state=HealthState.STALE
                    if self._last_good
                    else HealthState.UNAVAILABLE,
                ),
                self._generation,
            )
        except (sqlite3.DatabaseError, OSError, ValueError) as error:
            return self._failure(
                request,
                StampedeReadError(
                    "database_read_error",
                    str(error),
                    state=HealthState.STALE
                    if self._last_good
                    else HealthState.DEGRADED,
                ),
                self._generation,
            )

        self._last_good = snapshot
        self._last_heads = loaded.heads
        self._last_timings = loaded.timings
        self._last_maxrss = loaded.maxrss
        self._last_workflow_identities = loaded.workflow_identities
        self._last_success_epoch = request.clock.epoch
        self._consecutive_failures = 0
        health = SourceHealth(
            source=SourceName.STAMPEDE,
            state=HealthState.HEALTHY,
            checked_at_epoch=request.clock.epoch,
            last_success_epoch=request.clock.epoch,
            last_good_age=BoundedAge.between(
                request.clock.epoch,
                request.clock.epoch,
                _MAX_HEALTH_AGE_SECONDS,
            ),
            stale_after_seconds=_STALE_AFTER_SECONDS,
        )
        return DBRefreshResult(request, snapshot, health, snapshot.generation)

    def _stat_database(self) -> _FileMarker:
        assert self.database_path is not None
        try:
            status = self.database_path.stat()
        except FileNotFoundError as error:
            raise StampedeReadError(
                "database_missing",
                f"Stampede database does not exist: {self.database_path}",
                state=HealthState.STALE if self._last_good else HealthState.WAITING,
            ) from error
        except OSError as error:
            raise StampedeReadError(
                "database_stat_failed",
                str(error),
                state=HealthState.STALE if self._last_good else HealthState.UNAVAILABLE,
            ) from error
        if not self.database_path.is_file():
            raise StampedeReadError(
                "database_not_file",
                f"Stampede path is not a regular file: {self.database_path}",
                state=HealthState.UNAVAILABLE,
            )
        return _FileMarker(status.st_dev, status.st_ino, status.st_size)

    def _observe_generation(self, marker: _FileMarker) -> DatabaseGeneration:
        changed = self._last_marker is not None and (
            (marker.device, marker.inode)
            != (self._last_marker.device, self._last_marker.inode)
            or marker.size < self._last_marker.size
        )
        if self._generation is None:
            self._generation = DatabaseGeneration(
                self._generation_counter, marker.device, marker.inode
            )
        elif changed:
            self._generation_counter += 1
            self._generation = DatabaseGeneration(
                self._generation_counter, marker.device, marker.inode
            )
        self._last_marker = marker
        return self._generation

    def _advance_logical_generation(self, marker: _FileMarker) -> DatabaseGeneration:
        """Quarantine a same-file rollback so a full rebootstrap can proceed."""

        self._generation_counter += 1
        self._generation = DatabaseGeneration(
            self._generation_counter, marker.device, marker.inode
        )
        self._last_marker = marker
        return self._generation

    def _open_read_only(self) -> sqlite3.Connection:
        assert self.database_path is not None
        absolute = self.database_path.resolve()
        uri = f"file:{quote(str(absolute), safe='/')}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=self.busy_timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute(
            f"PRAGMA busy_timeout={max(1, int(self.busy_timeout_seconds * 1000))}"
        )
        query_only = connection.execute("PRAGMA query_only").fetchone()
        if query_only is None or int(query_only[0]) != 1:
            connection.close()
            raise StampedeReadError(
                "query_only_failed",
                "SQLite did not enable query_only",
                state=HealthState.UNAVAILABLE,
            )
        return connection

    def _check_schema(self, connection: sqlite3.Connection) -> None:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table, required in _REQUIRED_COLUMNS.items():
            if table not in tables:
                raise StampedeReadError(
                    "schema_mismatch",
                    f"Stampede schema has no {table} table",
                    state=HealthState.DEGRADED,
                )
            columns = {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info('{table}')")
            }
            missing = sorted(required - columns)
            if missing:
                raise StampedeReadError(
                    "schema_mismatch",
                    f"Stampede {table} table lacks: {', '.join(missing)}",
                    state=HealthState.DEGRADED,
                )
        row = connection.execute("SELECT MAX(version_number) FROM dbversion").fetchone()
        try:
            version = int(row[0]) if row and row[0] is not None else None
        except (TypeError, ValueError) as error:
            raise StampedeReadError(
                "schema_version_mismatch",
                f"invalid Stampede schema version: {row[0]!r}",
                state=HealthState.DEGRADED,
            ) from error
        if version != CURRENT_STAMPEDE_SCHEMA_VERSION:
            raise StampedeReadError(
                "schema_version_mismatch",
                "unsupported Stampede schema version "
                f"{version!r} (expected {CURRENT_STAMPEDE_SCHEMA_VERSION})",
                state=HealthState.DEGRADED,
            )

    def _load_snapshot(
        self,
        connection: sqlite3.Connection,
        request: DBRefreshRequest,
        generation: DatabaseGeneration,
    ) -> _LoadedSnapshot:
        workflow_row = self._load_workflow_row(connection, request.workflow)
        workflow_transitions = self._load_workflow_transitions(
            connection, workflow_row.wf_id, request.workflow
        )
        if not workflow_transitions:
            raise StampedeReadError(
                "workflow_state_waiting",
                "selected workflow has no authoritative workflowstate row yet",
                state=HealthState.WAITING,
            )
        workflow, workflow_watermark = self._build_workflow_snapshot(
            workflow_row, request.workflow, workflow_transitions
        )

        jobs = self._load_job_rows(connection, workflow_row.wf_id)
        attempts = self._load_attempt_rows(connection, workflow_row.wf_id)
        tasks = self._load_tasks(connection, workflow_row.wf_id)
        instance_ids = {
            attempt.job_instance_id
            for job_attempts in attempts.values()
            for attempt in job_attempts
        }
        # Every refresh publishes an authoritative current snapshot for the
        # entire selected workflow. BOUNDED_SUFFIX narrows only the detailed
        # transition history used for reconciliation, never current heads.
        heads = self._load_jobstate_heads(
            connection, workflow_row.wf_id, request.workflow
        )

        if request.mode is DBRefreshMode.FULL_REBOOTSTRAP:
            changed_ids = instance_ids
            timings: dict[
                int, tuple[Decimal | None, Decimal | None, Decimal | None]
            ] = {}
        else:
            previous_sequences = {
                instance_id: group[0].identity.jobstate_submit_seq
                for instance_id, group in self._last_heads.items()
                if group
            }
            changed_ids = {
                instance_id
                for instance_id in instance_ids
                if instance_id not in previous_sequences
                or (
                    instance_id in heads
                    and heads[instance_id][0].identity.jobstate_submit_seq
                    > previous_sequences[instance_id]
                )
            }
            timings = {
                instance_id: values
                for instance_id, values in self._last_timings.items()
                if instance_id in instance_ids
            }
        changed_scope = (
            None if request.mode is DBRefreshMode.FULL_REBOOTSTRAP else changed_ids
        )
        timings.update(
            self._load_attempt_timings(connection, workflow_row.wf_id, changed_scope)
        )
        # Invocation rows are written independently of jobstate transitions.
        # Refresh them independently so a late kickstart record is visible even
        # when the authoritative state head did not advance.
        maxrss = self._load_maxrss(connection, workflow_row.wf_id)
        watermarks = self._build_job_watermarks(heads)
        job_snapshots = self._build_jobs(
            request.workflow,
            jobs,
            attempts,
            tasks,
            maxrss,
            timings,
            heads,
        )

        if request.mode is DBRefreshMode.BOUNDED_SUFFIX:
            recent = self._load_suffix_transitions(
                connection, workflow_row.wf_id, request
            )
            recent_workflow = self._bounded_workflow_suffix(
                workflow_transitions, request
            )
        else:
            recent = self._load_recent_transitions(
                connection,
                workflow_row.wf_id,
                request.workflow,
                request.recent_transition_limit,
            )
            recent_workflow = tuple(
                sorted(
                    workflow_transitions,
                    key=lambda transition: transition.authoritative_sort_key,
                )[-request.recent_workflow_transition_limit :]
            )

        return _LoadedSnapshot(
            snapshot=DatabaseSnapshot(
                epoch=request.next_epoch,
                generation=generation,
                snapshot_at_epoch=request.clock.epoch,
                workflow=workflow,
                jobs=tuple(job_snapshots),
                recent_transitions=tuple(recent),
                recent_workflow_transitions=tuple(recent_workflow),
                watermarks=tuple(watermarks),
                workflow_watermark=workflow_watermark,
            ),
            heads=heads,
            timings=timings,
            maxrss=maxrss,
            workflow_identities=frozenset(
                transition.identity for transition in workflow_transitions
            ),
        )

    def _load_workflow_row(
        self, connection: sqlite3.Connection, workflow: WorkflowIdentity
    ) -> _WorkflowRow:
        rows = connection.execute(
            """
            SELECT selected.wf_id, root.wf_uuid AS root_uuid
            FROM workflow AS selected
            LEFT JOIN workflow AS root ON root.wf_id = selected.root_wf_id
            WHERE selected.wf_uuid = ?
            """,
            (workflow.wf_uuid,),
        ).fetchall()
        if len(rows) != 1:
            code = "workflow_not_found" if not rows else "workflow_identity_ambiguous"
            raise StampedeReadError(
                code,
                f"selected workflow UUID resolved to {len(rows)} rows",
                state=HealthState.RESYNC if self._last_good else HealthState.WAITING,
            )
        root_uuid = rows[0]["root_uuid"]
        if root_uuid != workflow.root_wf_uuid:
            raise StampedeReadError(
                "root_workflow_mismatch",
                "Stampede root workflow UUID does not match braindump identity",
                state=HealthState.RESYNC,
            )
        return _WorkflowRow(_int(rows[0]["wf_id"], "wf_id"), str(root_uuid))

    def _load_workflow_transitions(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        workflow: WorkflowIdentity,
    ) -> tuple[DBWorkflowTransition, ...]:
        rows = connection.execute(
            """
            SELECT wf_id, state, timestamp, restart_count, status, reason
            FROM workflowstate
            WHERE wf_id = ?
            """,
            (wf_id,),
        ).fetchall()
        transitions = tuple(
            sorted(
                (_workflow_transition_from_row(row, workflow) for row in rows),
                key=lambda transition: transition.authoritative_sort_key,
            )
        )
        if len({item.identity for item in transitions}) != len(transitions):
            raise StampedeReadError(
                "workflow_identity_conflict",
                "duplicate workflowstate primary-key identity",
                state=HealthState.DEGRADED,
            )
        return transitions

    def _build_workflow_snapshot(
        self,
        row: _WorkflowRow,
        workflow: WorkflowIdentity,
        transitions: Sequence[DBWorkflowTransition],
    ) -> tuple[WorkflowSnapshot, WorkflowTransitionWatermark]:
        current = max(transitions, key=lambda item: item.authoritative_sort_key)
        restart = current.restart_count
        current_restart = tuple(
            item for item in transitions if item.restart_count == restart
        )
        starts = [
            item.identity.timestamp
            for item in current_restart
            if item.normalized_state == "WORKFLOW_STARTED"
        ]
        ends = [
            item.identity.timestamp
            for item in current_restart
            if item.normalized_state == "WORKFLOW_TERMINATED"
        ]
        snapshot = WorkflowSnapshot(
            workflow=workflow,
            wf_id=row.wf_id,
            state=current.identity.state,
            status=current.status,
            restart_count=restart,
            started_at=max(starts) if starts else None,
            ended_at=max(ends) if ends else None,
            transition=current,
            provenance=Provenance.DB_CONFIRMED,
        )
        watermark = WorkflowTransitionWatermark(
            restart=WorkflowRestartIdentity(workflow, row.wf_id, restart),
            identities=tuple(item.identity for item in current_restart),
        )
        return snapshot, watermark

    def _load_job_rows(
        self, connection: sqlite3.Connection, wf_id: int
    ) -> tuple[_JobRow, ...]:
        rows = connection.execute(
            """
            SELECT job_id, exec_job_id, type_desc, task_count
            FROM job
            WHERE wf_id = ?
            ORDER BY job_id
            """,
            (wf_id,),
        ).fetchall()
        result = tuple(
            _JobRow(
                _int(row["job_id"], "job_id"),
                str(row["exec_job_id"]),
                str(row["type_desc"]),
                _int(row["task_count"], "task_count"),
            )
            for row in rows
        )
        if len({row.job_id for row in result}) != len(result) or len(
            {row.exec_job_id for row in result}
        ) != len(result):
            raise StampedeReadError(
                "duplicate_job_row",
                "selected workflow contains duplicate job identity",
                state=HealthState.DEGRADED,
            )
        return result

    def _load_attempt_rows(
        self, connection: sqlite3.Connection, wf_id: int
    ) -> dict[int, tuple[_AttemptRow, ...]]:
        rows = connection.execute(
            """
            SELECT ji.job_id, ji.job_instance_id, ji.job_submit_seq,
                   ji.sched_id, ji.site, ji.stdout_file, ji.stderr_file,
                   ji.exitcode
            FROM job_instance AS ji
            JOIN job AS j ON j.job_id = ji.job_id
            WHERE j.wf_id = ?
            ORDER BY ji.job_id, ji.job_submit_seq, ji.job_instance_id
            """,
            (wf_id,),
        ).fetchall()
        grouped: dict[int, list[_AttemptRow]] = defaultdict(list)
        seen_instances: set[int] = set()
        seen_submit_sequences: set[int] = set()
        for row in rows:
            attempt = _AttemptRow(
                job_id=_int(row["job_id"], "job_id"),
                job_instance_id=_int(row["job_instance_id"], "job_instance_id"),
                job_submit_seq=_int(row["job_submit_seq"], "job_submit_seq"),
                sched_id=row["sched_id"],
                site=row["site"],
                stdout_file=row["stdout_file"],
                stderr_file=row["stderr_file"],
                raw_wait_status=_optional_int(row["exitcode"], "exitcode"),
            )
            if attempt.job_instance_id in seen_instances:
                raise StampedeReadError(
                    "duplicate_job_attempt",
                    "job_instance_id is not unique",
                    state=HealthState.DEGRADED,
                )
            if attempt.job_submit_seq in seen_submit_sequences:
                raise StampedeReadError(
                    "duplicate_job_submit_seq",
                    "job_submit_seq is not workflow-global unique",
                    state=HealthState.DEGRADED,
                )
            seen_instances.add(attempt.job_instance_id)
            seen_submit_sequences.add(attempt.job_submit_seq)
            grouped[attempt.job_id].append(attempt)
        return {job_id: tuple(items) for job_id, items in grouped.items()}

    def _load_tasks(
        self, connection: sqlite3.Connection, wf_id: int
    ) -> dict[int, tuple[int, tuple[str, ...]]]:
        rows = connection.execute(
            """
            SELECT task_id, job_id, transformation
            FROM task
            WHERE wf_id = ? AND job_id IS NOT NULL
            ORDER BY job_id, task_id
            """,
            (wf_id,),
        ).fetchall()
        task_ids: dict[int, set[int]] = defaultdict(set)
        transformations: dict[int, list[str]] = defaultdict(list)
        for row in rows:
            job_id = _int(row["job_id"], "task.job_id")
            task_ids[job_id].add(_int(row["task_id"], "task_id"))
            transformation = row["transformation"]
            if (
                transformation is not None
                and str(transformation) not in transformations[job_id]
            ):
                transformations[job_id].append(str(transformation))
        return {
            job_id: (len(ids), tuple(transformations[job_id]))
            for job_id, ids in task_ids.items()
        }

    def _load_maxrss(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        instance_ids: set[int] | None = None,
    ) -> dict[int, int]:
        if instance_ids is not None and not instance_ids:
            return {}
        if instance_ids is not None and len(instance_ids) > 400:
            result: dict[int, int] = {}
            ordered = sorted(instance_ids)
            for offset in range(0, len(ordered), 400):
                result.update(
                    self._load_maxrss(
                        connection, wf_id, set(ordered[offset : offset + 400])
                    )
                )
            return result
        instance_filter = ""
        parameters: list[int] = [wf_id]
        if instance_ids is not None:
            placeholders = ",".join("?" for _ in instance_ids)
            instance_filter = f" AND job_instance_id IN ({placeholders})"
            parameters.extend(sorted(instance_ids))
        rows = connection.execute(
            f"""
            SELECT job_instance_id, MAX(maxrss) AS maxrss
            FROM invocation
            WHERE wf_id = ? AND task_submit_seq >= 1 AND maxrss IS NOT NULL
                  {instance_filter}
            GROUP BY job_instance_id
            """,
            parameters,
        ).fetchall()
        return {
            _int(row["job_instance_id"], "invocation.job_instance_id"): _int(
                row["maxrss"], "maxrss"
            )
            for row in rows
        }

    def _load_attempt_timings(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        instance_ids: set[int] | None = None,
    ) -> dict[int, tuple[Decimal | None, Decimal | None, Decimal | None]]:
        if instance_ids is not None and not instance_ids:
            return {}
        if instance_ids is not None and len(instance_ids) > 400:
            result: dict[
                int, tuple[Decimal | None, Decimal | None, Decimal | None]
            ] = {}
            ordered = sorted(instance_ids)
            for offset in range(0, len(ordered), 400):
                result.update(
                    self._load_attempt_timings(
                        connection, wf_id, set(ordered[offset : offset + 400])
                    )
                )
            return result
        submit_placeholders = ",".join("?" for _ in _SUBMIT_STATES)
        terminal_placeholders = ",".join("?" for _ in _TERMINAL_MAIN_JOB_STATES)
        instance_filter = ""
        parameters: list[object] = [*_SUBMIT_STATES, *_TERMINAL_MAIN_JOB_STATES, wf_id]
        if instance_ids is not None:
            placeholders = ",".join("?" for _ in instance_ids)
            instance_filter = f" AND js.job_instance_id IN ({placeholders})"
            parameters.extend(sorted(instance_ids))
        rows = connection.execute(
            f"""
            SELECT js.job_instance_id,
                   MIN(CASE WHEN js.state IN ({submit_placeholders})
                            THEN js.timestamp END) AS submit_time,
                   MIN(CASE WHEN js.state = 'EXECUTE'
                            THEN js.timestamp END) AS start_time,
                   MAX(CASE WHEN js.state IN ({terminal_placeholders})
                            THEN js.timestamp END) AS end_time
            FROM jobstate AS js
            JOIN job_instance AS ji ON ji.job_instance_id = js.job_instance_id
            JOIN job AS j ON j.job_id = ji.job_id
            WHERE j.wf_id = ?
                  {instance_filter}
            GROUP BY js.job_instance_id
            """,
            parameters,
        ).fetchall()
        return {
            _int(row["job_instance_id"], "jobstate.job_instance_id"): (
                db_timestamp(row["submit_time"])
                if row["submit_time"] is not None
                else None,
                db_timestamp(row["start_time"])
                if row["start_time"] is not None
                else None,
                db_timestamp(row["end_time"]) if row["end_time"] is not None else None,
            )
            for row in rows
        }

    def _load_jobstate_heads(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        workflow: WorkflowIdentity,
        instance_ids: set[int] | None = None,
    ) -> dict[int, tuple[DBJobTransition, ...]]:
        if instance_ids is not None and not instance_ids:
            return {}
        if instance_ids is not None and len(instance_ids) > 400:
            result: dict[int, tuple[DBJobTransition, ...]] = {}
            ordered = sorted(instance_ids)
            for offset in range(0, len(ordered), 400):
                result.update(
                    self._load_jobstate_heads(
                        connection,
                        wf_id,
                        workflow,
                        set(ordered[offset : offset + 400]),
                    )
                )
            return result
        instance_filter = ""
        parameters: list[int] = [wf_id]
        if instance_ids is not None:
            placeholders = ",".join("?" for _ in instance_ids)
            instance_filter = f" AND js.job_instance_id IN ({placeholders})"
            parameters.extend(sorted(instance_ids))
        parameters.append(wf_id)
        rows = connection.execute(
            f"""
            WITH highest AS (
                SELECT js.job_instance_id,
                       MAX(js.jobstate_submit_seq) AS highest_seq
                FROM jobstate AS js
                JOIN job_instance AS ji
                  ON ji.job_instance_id = js.job_instance_id
                JOIN job AS j ON j.job_id = ji.job_id
                WHERE j.wf_id = ?
                      {instance_filter}
                GROUP BY js.job_instance_id
            )
            SELECT j.exec_job_id, ji.job_submit_seq,
                   js.job_instance_id, js.state, js.timestamp,
                   js.jobstate_submit_seq, js.reason
            FROM highest AS h
            JOIN jobstate AS js
              ON js.job_instance_id = h.job_instance_id
             AND js.jobstate_submit_seq = h.highest_seq
            JOIN job_instance AS ji
              ON ji.job_instance_id = js.job_instance_id
            JOIN job AS j ON j.job_id = ji.job_id
            WHERE j.wf_id = ?
            """,
            parameters,
        ).fetchall()
        grouped: dict[int, list[DBJobTransition]] = defaultdict(list)
        for row in rows:
            transition = _transition_from_row(row, workflow)
            grouped[transition.identity.job_instance_id].append(transition)
        return {
            instance_id: tuple(
                sorted(items, key=lambda item: item.authoritative_sort_key)
            )
            for instance_id, items in grouped.items()
        }

    def _build_job_watermarks(
        self, heads: dict[int, tuple[DBJobTransition, ...]]
    ) -> tuple[JobTransitionWatermark, ...]:
        return tuple(
            JobTransitionWatermark(
                instance_id,
                items[0].identity.jobstate_submit_seq,
                tuple(item.identity for item in items),
            )
            for instance_id, items in sorted(heads.items())
        )

    def _build_jobs(
        self,
        workflow: WorkflowIdentity,
        jobs: Sequence[_JobRow],
        attempts_by_job: dict[int, tuple[_AttemptRow, ...]],
        tasks: dict[int, tuple[int, tuple[str, ...]]],
        maxrss: dict[int, int],
        timings: dict[int, tuple[Decimal | None, Decimal | None, Decimal | None]],
        heads: dict[int, tuple[DBJobTransition, ...]],
    ) -> tuple[JobSnapshot, ...]:
        snapshots: list[JobSnapshot] = []
        for job in jobs:
            attempt_rows = attempts_by_job.get(job.job_id, ())
            attempts: list[JobAttempt] = []
            for row in attempt_rows:
                submit_time, start_time, end_time = timings.get(
                    row.job_instance_id, (None, None, None)
                )
                attempts.append(
                    JobAttempt(
                        identity=JobAttemptIdentity(
                            job.job_id, row.job_instance_id, row.job_submit_seq
                        ),
                        scheduler_id=row.sched_id,
                        site=row.site,
                        submit_time=submit_time,
                        start_time=start_time,
                        end_time=end_time,
                        raw_wait_status=row.raw_wait_status,
                        exit_code=decode_wait_status(row.raw_wait_status),
                        stdout_path=row.stdout_file,
                        stderr_path=row.stderr_file,
                        maxrss_kb=maxrss.get(row.job_instance_id),
                    )
                )
            current_attempt = attempts[-1].identity if attempts else None
            current_group = (
                heads.get(current_attempt.job_instance_id, ())
                if current_attempt is not None
                else ()
            )
            transition = (
                max(current_group, key=lambda item: item.authoritative_sort_key)
                if current_group
                else None
            )
            actual_count, transformations = tasks.get(job.job_id, (0, ()))
            snapshots.append(
                JobSnapshot(
                    workflow=workflow,
                    job_id=job.job_id,
                    exec_job_id=job.exec_job_id,
                    type_desc=job.type_desc,
                    task_count=max(job.declared_task_count, actual_count),
                    transformations=transformations,
                    attempts=tuple(attempts),
                    current_attempt=current_attempt,
                    state=transition.identity.state if transition else None,
                    state_timestamp=(
                        transition.identity.timestamp if transition else None
                    ),
                    transition=transition,
                    provenance=Provenance.DB_CONFIRMED,
                )
            )
        return tuple(snapshots)

    def _load_recent_transitions(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        workflow: WorkflowIdentity,
        limit: int,
    ) -> tuple[DBJobTransition, ...]:
        rows = connection.execute(
            f"""
            SELECT j.exec_job_id, ji.job_submit_seq,
                   js.job_instance_id, js.state, js.timestamp,
                   js.jobstate_submit_seq, js.reason
            FROM jobstate AS js
            JOIN job_instance AS ji ON ji.job_instance_id = js.job_instance_id
            JOIN job AS j ON j.job_id = ji.job_id
            WHERE j.wf_id = ?
            ORDER BY js.timestamp DESC, j.exec_job_id DESC,
                     js.job_instance_id DESC, ji.job_submit_seq DESC,
                     js.jobstate_submit_seq DESC,
                     {_STATE_PRECEDENCE_SQL} DESC, js.state DESC
            LIMIT ?
            """,
            (wf_id, limit + 1),
        ).fetchall()
        if len(rows) > limit:
            boundary = rows[limit - 1]
            hidden = rows[limit]
            boundary_group = (
                boundary["job_instance_id"],
                boundary["jobstate_submit_seq"],
            )
            hidden_group = (
                hidden["job_instance_id"],
                hidden["jobstate_submit_seq"],
            )
            if boundary_group == hidden_group:
                rows = [
                    row
                    for row in rows[:limit]
                    if (row["job_instance_id"], row["jobstate_submit_seq"])
                    != boundary_group
                ]
                if not rows:
                    raise StampedeReadError(
                        "recent_transition_group_exceeds_limit",
                        "one complete jobstate_submit_seq group exceeds the "
                        "recent transition limit",
                        state=HealthState.DEGRADED,
                    )
            else:
                rows = rows[:limit]
        return tuple(
            sorted(
                (_transition_from_row(row, workflow) for row in rows),
                key=lambda transition: transition.recent_event_sort_key,
            )
        )

    def _load_suffix_transitions(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        request: DBRefreshRequest,
    ) -> tuple[DBJobTransition, ...]:
        thresholds = {
            watermark.job_instance_id: watermark.highest_jobstate_submit_seq
            for watermark in request.pending_job_watermarks
        }
        provisional_instances = self._resolve_provisional_instances(
            connection, wf_id, request.pending_job_keys
        )
        for instance_id in provisional_instances:
            thresholds.setdefault(instance_id, 0)
        if not thresholds:
            return ()
        transitions: list[DBJobTransition] = []
        ordered_thresholds = sorted(thresholds.items())
        for offset in range(0, len(ordered_thresholds), _SUFFIX_THRESHOLD_BATCH_SIZE):
            clauses = []
            parameters: list[int] = [wf_id]
            for instance_id, sequence in ordered_thresholds[
                offset : offset + _SUFFIX_THRESHOLD_BATCH_SIZE
            ]:
                clauses.append(
                    "(js.job_instance_id = ? AND js.jobstate_submit_seq >= ?)"
                )
                parameters.extend((instance_id, sequence))
            parameters.append(request.recent_transition_limit + 1)
            rows = connection.execute(
                f"""
                SELECT j.exec_job_id, ji.job_submit_seq,
                       js.job_instance_id, js.state, js.timestamp,
                       js.jobstate_submit_seq, js.reason
                FROM jobstate AS js
                JOIN job_instance AS ji
                  ON ji.job_instance_id = js.job_instance_id
                JOIN job AS j ON j.job_id = ji.job_id
                WHERE j.wf_id = ? AND ({" OR ".join(clauses)})
                ORDER BY js.timestamp DESC, j.exec_job_id DESC,
                         js.job_instance_id DESC, ji.job_submit_seq DESC,
                         js.jobstate_submit_seq DESC,
                         {_STATE_PRECEDENCE_SQL} DESC, js.state DESC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
            transitions.extend(
                _transition_from_row(row, request.workflow) for row in rows
            )

        # A batch that reached limit + 1 may have truncated its final sequence
        # group, but the global overflow below prevents any partial group from
        # being published.  Instances never span batches.
        ordered = tuple(
            sorted(
                transitions,
                key=lambda transition: transition.recent_event_sort_key,
            )
        )
        if len(ordered) > request.recent_transition_limit:
            raise StampedeReadError(
                "reconciliation_suffix_overflow",
                "pending reconciliation suffix exceeds the requested transition limit",
                state=HealthState.RESYNC,
            )
        return ordered

    def _resolve_provisional_instances(
        self,
        connection: sqlite3.Connection,
        wf_id: int,
        keys: Iterable[JobSemanticKey],
    ) -> set[int]:
        result: set[int] = set()
        for key in keys:
            rows = connection.execute(
                """
                SELECT ji.job_instance_id
                FROM job AS j
                JOIN job_instance AS ji ON ji.job_id = j.job_id
                WHERE j.wf_id = ? AND j.exec_job_id = ?
                  AND ji.job_submit_seq = ?
                """,
                (wf_id, key.exec_job_id, key.job_submit_seq),
            ).fetchall()
            if len(rows) > 1:
                raise StampedeReadError(
                    "attempt_identity_ambiguous",
                    "provisional semantic key maps to multiple DB attempts",
                    state=HealthState.DEGRADED,
                )
            if rows:
                result.add(_int(rows[0][0], "job_instance_id"))
        return result

    def _bounded_workflow_suffix(
        self,
        transitions: Sequence[DBWorkflowTransition],
        request: DBRefreshRequest,
    ) -> tuple[DBWorkflowTransition, ...]:
        watermark = request.pending_workflow_watermark
        if watermark is None:
            return ()
        suffix = [
            transition
            for transition in transitions
            if transition.restart_count >= watermark.restart.restart_count
        ]
        if len(suffix) > request.recent_workflow_transition_limit:
            raise StampedeReadError(
                "workflow_suffix_overflow",
                "pending workflow reconciliation suffix exceeds the requested limit",
                state=HealthState.RESYNC,
            )
        return tuple(
            sorted(suffix, key=lambda transition: transition.authoritative_sort_key)[
                -request.recent_workflow_transition_limit :
            ]
        )

    def _reject_watermark_rollback(
        self,
        snapshot: DatabaseSnapshot,
        workflow_identities: frozenset[DBWorkflowTransitionIdentity],
    ) -> None:
        previous = self._last_good
        if previous is None or previous.generation != snapshot.generation:
            return
        if not self._last_workflow_identities.issubset(workflow_identities):
            raise StampedeReadError(
                "workflow_watermark_rollback",
                "Stampede workflow transition history moved backwards",
                state=HealthState.RESYNC,
            )
        previous_job_identities = {
            (job.job_id, job.exec_job_id) for job in previous.jobs
        }
        current_job_identities = {
            (job.job_id, job.exec_job_id) for job in snapshot.jobs
        }
        previous_attempt_identities = {
            attempt.identity for job in previous.jobs for attempt in job.attempts
        }
        current_attempt_identities = {
            attempt.identity for job in snapshot.jobs for attempt in job.attempts
        }
        if not previous_job_identities.issubset(
            current_job_identities
        ) or not previous_attempt_identities.issubset(current_attempt_identities):
            raise StampedeReadError(
                "database_roster_rollback",
                "Stampede job or attempt identity roster moved backwards",
                state=HealthState.RESYNC,
            )
        current_instance_ids = {
            identity.job_instance_id for identity in current_attempt_identities
        }
        old = {
            watermark.job_instance_id: watermark for watermark in previous.watermarks
        }
        new_instance_ids = {
            watermark.job_instance_id for watermark in snapshot.watermarks
        }
        if any(
            instance_id in current_instance_ids and instance_id not in new_instance_ids
            for instance_id in old
        ):
            raise StampedeReadError(
                "transition_watermark_rollback",
                "Stampede job transition watermark disappeared",
                state=HealthState.RESYNC,
            )
        for watermark in snapshot.watermarks:
            previous_watermark = old.get(watermark.job_instance_id)
            if previous_watermark is not None and (
                watermark.highest_jobstate_submit_seq
                < previous_watermark.highest_jobstate_submit_seq
            ):
                raise StampedeReadError(
                    "transition_watermark_rollback",
                    "Stampede job transition watermark moved backwards",
                    state=HealthState.RESYNC,
                )
            if (
                previous_watermark is not None
                and watermark.highest_jobstate_submit_seq
                == previous_watermark.highest_jobstate_submit_seq
                and not set(previous_watermark.identities_at_highest_seq).issubset(
                    watermark.identities_at_highest_seq
                )
            ):
                raise StampedeReadError(
                    "transition_watermark_rollback",
                    "Stampede job transition identity group moved backwards",
                    state=HealthState.RESYNC,
                )
        if (
            snapshot.workflow_watermark.restart.restart_count
            < previous.workflow_watermark.restart.restart_count
        ):
            raise StampedeReadError(
                "workflow_watermark_rollback",
                "Stampede workflow restart count moved backwards",
                state=HealthState.RESYNC,
            )
        if (
            snapshot.workflow_watermark.restart.restart_count
            == previous.workflow_watermark.restart.restart_count
            and not set(previous.workflow_watermark.identities).issubset(
                snapshot.workflow_watermark.identities
            )
        ):
            raise StampedeReadError(
                "workflow_watermark_rollback",
                "Stampede workflow transition identity group moved backwards",
                state=HealthState.RESYNC,
            )
        previous_transition = previous.workflow.transition
        current_transition = snapshot.workflow.transition
        if (
            previous_transition is not None
            and current_transition is not None
            and current_transition.authoritative_sort_key
            < previous_transition.authoritative_sort_key
        ):
            raise StampedeReadError(
                "workflow_watermark_rollback",
                "Stampede workflow transition moved backwards",
                state=HealthState.RESYNC,
            )

    def _failure(
        self,
        request: DBRefreshRequest,
        error: StampedeReadError,
        generation: DatabaseGeneration | None,
    ) -> DBRefreshResult:
        self._consecutive_failures += 1
        last_good_age = (
            BoundedAge.between(
                request.clock.epoch,
                self._last_success_epoch,
                _MAX_HEALTH_AGE_SECONDS,
            )
            if self._last_success_epoch is not None
            else None
        )
        state = error.state
        if self._last_good is not None and state not in {
            HealthState.RESYNC,
            HealthState.REATTACHING,
        }:
            state = HealthState.STALE
        health = SourceHealth(
            source=SourceName.STAMPEDE,
            state=state,
            checked_at_epoch=request.clock.epoch,
            last_success_epoch=self._last_success_epoch,
            last_good_age=last_good_age,
            stale_after_seconds=_STALE_AFTER_SECONDS,
            consecutive_failures=self._consecutive_failures,
            error_code=error.code,
            detail=error.detail[:512],
        )
        return DBRefreshResult(request, None, health, generation)
