"""Immutable contracts shared by the native Pegasus workflow monitor.

This module is intentionally presentation- and source-neutral.  Importing it
must not open a database, arm a file tail, invoke HTCondor, create a task, or
import Rich.  WP4 owns all scheduling and supplies clocks to the bounded source
provider interfaces declared here.
"""

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

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

TimestampInput = Decimal | int | float | str
JSONScalar = type(None) | bool | int | float | str
JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

JSONL_V1_SCHEMA_VERSION = 1
JSONL_V1_CONTRACT_STATUS = "draft-pending-wp4-reconciliation"
JSONL_V1_DEFERRED_FIELDS = (
    "checkpoint.reconciliation_cursor",
    "checkpoint.per_instance_suffix_window",
    "gap.resume_checkpoint_sequence",
)
"""Fields whose exact encoding waits for WP4 executable reconciliation tests.

They are deliberately absent from the v1 record classes below.  The stable
parts already guarantee DB-confirmed transitions, exact row identities,
checkpoints, and post-gap checkpoint recovery without exposing a speculative
cursor format.
"""

MAX_HEALTH_DETAIL_CHARS = 512
MAX_DIAGNOSTIC_SUMMARY_CHARS = 2048
MAX_TAIL_LINE_CHARS = 65536
MAX_SCHEDULER_BACKOFF_SECONDS = 300.0


def db_timestamp(value: TimestampInput) -> Decimal:
    """Return a finite, full-precision timestamp suitable for DB identity.

    Floats are converted through ``str`` so their user-visible decimal value is
    retained instead of importing binary expansion noise.  No integer-second
    rounding or six-place quantization is performed.
    """

    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid timestamp: {value!r}") from error
    if not result.is_finite():
        raise ValueError("timestamp must be finite")
    return result


def producer_timestamp_key(value: TimestampInput) -> str:
    """Match monitord's producer-side ``format(timestamp, '.0f')`` behavior.

    This is nearest-integer, ties-to-even formatting, not truncation.  It is
    used only for semantic reconciliation keys; DB identities and ordering keep
    the unrounded :class:`~decimal.Decimal` value.
    """

    return format(db_timestamp(value), ".0f")


def _require_nonnegative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _bounded_text(value: str | None, maximum: int, name: str) -> str | None:
    if value is not None and len(value) > maximum:
        raise ValueError(f"{name} exceeds {maximum} characters")
    return value


def _json_value(value: Any) -> JSONValue:
    """Normalize supported contract values without ``dataclasses.asdict``."""

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON contract floats must be finite")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (Path, UUID)):
        return str(value)
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, FrozenPayload):
        return value.to_json_dict()
    if hasattr(value, "to_json_dict"):
        result = value.to_json_dict()
        if not isinstance(result, dict):
            raise TypeError("to_json_dict() must return a dict")
        return result
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise TypeError(f"unsupported JSON contract value: {type(value).__name__}")


def _record_json(
    record_type: str, sequence: int, **values: Any
) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {
        "schema_version": JSONL_V1_SCHEMA_VERSION,
        "record_type": record_type,
        "sequence": sequence,
    }
    result.update({key: _json_value(value) for key, value in values.items()})
    return result


def _validate_stream_record(
    sequence: int, stream_id: str, recorded_at_epoch: float, *, header: bool = False
) -> None:
    if (header and sequence != 0) or (not header and sequence <= 0):
        expected = "zero" if header else "positive"
        raise ValueError(f"record sequence must be {expected}")
    if not stream_id:
        raise ValueError("stream_id must not be empty")
    if not math.isfinite(recorded_at_epoch):
        raise ValueError("record epoch must be finite")


class Provenance(str, Enum):
    """Origin of an effective value; scheduler data never defines state."""

    DB_CONFIRMED = "db_confirmed"
    TAIL_PENDING = "tail_pending"
    DB_WITH_TAIL_OVERLAY = "db_with_tail_overlay"
    PROVISIONAL_JOB = "provisional_job"


class SourceName(str, Enum):
    BRAINDUMP = "braindump"
    STAMPEDE = "stampede"
    LIVE_TAIL = "live_tail"
    CONDOR_QUEUE = "condor_queue"
    CONDOR_HISTORY = "condor_history"
    CONDOR_POOL = "condor_pool"
    CONDOR_PRIORITY = "condor_priority"
    CONDOR_NEGOTIATOR = "condor_negotiator"
    KICKSTART = "kickstart"


class HealthState(str, Enum):
    HEALTHY = "healthy"
    WAITING = "waiting"
    STALE = "stale"
    DEGRADED = "degraded"
    GAP = "gap"
    REATTACHING = "reattaching"
    RESYNC = "resync"
    FAILED_UNCONFIRMED = "failed_unconfirmed"
    UNAVAILABLE = "unavailable"
    DISABLED = "disabled"


class Lifecycle(str, Enum):
    UNSUBMITTED = "unsubmitted"
    PRE = "pre"
    QUEUED = "queued"
    RUNNING = "running"
    HELD = "held"
    POST = "post"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    OTHER = "other"


class DiagnosticSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class GapReason(str, Enum):
    DISK_GUARD = "disk_guard"
    SIZE_LIMIT = "size_limit"
    STREAM_REPLACED = "stream_replaced"
    WRITER_ERROR = "writer_error"


class SchedulerQueryKind(str, Enum):
    QUEUE = "queue"
    HISTORY = "history"
    POOL = "pool"
    PRIORITY = "priority"
    NEGOTIATOR = "negotiator"


_SCHEDULER_SOURCE_BY_KIND = {
    SchedulerQueryKind.QUEUE: SourceName.CONDOR_QUEUE,
    SchedulerQueryKind.HISTORY: SourceName.CONDOR_HISTORY,
    SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
    SchedulerQueryKind.PRIORITY: SourceName.CONDOR_PRIORITY,
    SchedulerQueryKind.NEGOTIATOR: SourceName.CONDOR_NEGOTIATOR,
}


class DBRefreshMode(str, Enum):
    FULL_REBOOTSTRAP = "full_rebootstrap"
    CURRENT_SNAPSHOT = "current_snapshot"
    BOUNDED_SUFFIX = "bounded_suffix"


class TailSourceMarker(str, Enum):
    MONITORD_STARTED = "MONITORD_STARTED"
    MONITORD_FINISHED = "MONITORD_FINISHED"


_JOB_STATE_ALIASES = {
    "JOB_HELD_REASON": "JOB_HELD",
    "PRE_SCRIPT_FAILURE": "PRE_SCRIPT_FAILED",
    "POST_SCRIPT_FAILURE": "POST_SCRIPT_FAILED",
}

_WORKFLOW_STATE_ALIASES = {
    "DAGMAN_STARTED": "WORKFLOW_STARTED",
    "DAGMAN_FINISHED": "WORKFLOW_TERMINATED",
}


def normalize_job_state(state: str) -> str:
    """Normalize known producer/Stampede spellings and preserve unknowns."""

    normalized = state.strip().upper()
    if not normalized:
        raise ValueError("job state must not be empty")
    known = _JOB_STATE_ALIASES.get(normalized)
    if known is not None:
        return known
    return normalized


def normalize_workflow_state(state: str) -> str:
    normalized = state.strip().upper()
    if not normalized:
        raise ValueError("workflow state must not be empty")
    return _WORKFLOW_STATE_ALIASES.get(normalized, normalized)


_LIFECYCLES = {
    "PRE_SCRIPT_STARTED": Lifecycle.PRE,
    "PRE_SCRIPT_TERMINATED": Lifecycle.PRE,
    "PRE_SCRIPT_SUCCESS": Lifecycle.PRE,
    "PRE_SCRIPT_FAILED": Lifecycle.FAILED,
    "SUBMIT": Lifecycle.QUEUED,
    "GRID_SUBMIT": Lifecycle.QUEUED,
    "GLOBUS_SUBMIT": Lifecycle.QUEUED,
    "EXECUTE": Lifecycle.RUNNING,
    "JOB_HELD": Lifecycle.HELD,
    "JOB_RELEASED": Lifecycle.QUEUED,
    "JOB_EVICTED": Lifecycle.HELD,
    "JOB_TERMINATED": Lifecycle.POST,
    "POST_SCRIPT_STARTED": Lifecycle.POST,
    "POST_SCRIPT_TERMINATED": Lifecycle.POST,
    "POST_SCRIPT_SUCCESS": Lifecycle.SUCCEEDED,
    "POST_SCRIPT_FAILED": Lifecycle.FAILED,
    "JOB_SUCCESS": Lifecycle.SUCCEEDED,
    "JOB_FAILURE": Lifecycle.FAILED,
    "JOB_FAILED": Lifecycle.FAILED,
    "JOB_ABORTED": Lifecycle.FAILED,
    "SUBMIT_FAILED": Lifecycle.FAILED,
    "GRID_SUBMIT_FAILED": Lifecycle.FAILED,
    "GLOBUS_SUBMIT_FAILED": Lifecycle.FAILED,
}


def job_lifecycle(state: str | None) -> Lifecycle:
    if state is None:
        return Lifecycle.UNSUBMITTED
    return _LIFECYCLES.get(normalize_job_state(state), Lifecycle.OTHER)


# Higher values win when sequence and timestamp are equal.  Unknown states are
# deterministic through the final lexical state tie-breaker and remain visible.
_STATE_PRECEDENCE = {
    "PRE_SCRIPT_STARTED": 10,
    "PRE_SCRIPT_TERMINATED": 11,
    "PRE_SCRIPT_SUCCESS": 12,
    "SUBMIT": 20,
    "GRID_SUBMIT": 20,
    "GLOBUS_SUBMIT": 20,
    "JOB_RELEASED": 25,
    "EXECUTE": 30,
    "IMAGE_SIZE": 31,
    "REMOTE_ERROR": 32,
    "JOB_HELD": 40,
    "JOB_EVICTED": 45,
    "JOB_TERMINATED": 50,
    "POST_SCRIPT_STARTED": 60,
    "POST_SCRIPT_TERMINATED": 61,
    "POST_SCRIPT_SUCCESS": 70,
    "PRE_SCRIPT_FAILED": 80,
    "POST_SCRIPT_FAILED": 80,
    "SUBMIT_FAILED": 80,
    "GRID_SUBMIT_FAILED": 80,
    "GLOBUS_SUBMIT_FAILED": 80,
    "JOB_ABORTED": 85,
    "JOB_FAILED": 90,
    "JOB_FAILURE": 100,
    "JOB_SUCCESS": 100,
}


def state_precedence(state: str) -> int:
    return _STATE_PRECEDENCE.get(normalize_job_state(state), 0)


SYNTHETIC_FAILURE_CAUSES = frozenset(
    {
        "JOB_ABORTED",
        "PRE_SCRIPT_FAILED",
        "SUBMIT_FAILED",
        "GRID_SUBMIT_FAILED",
        "GLOBUS_SUBMIT_FAILED",
    }
)


def transition_group_equivalent(
    tail_states: Sequence[str],
    db_states: Sequence[str],
    *,
    plain_held_is_confirming: bool = False,
) -> bool:
    """Return whether one ordered tail group maps to one same-sequence DB group.

    Multiplicity is retained.  The only many-to-one rule is the known held pair,
    and the only one-to-many rules are monitord's reviewed synthetic failure
    rows.  Unknown combinations are never guessed.
    """

    raw_tail = tuple(state.strip().upper() for state in tail_states)
    tail = tuple(normalize_job_state(state) for state in raw_tail)
    db = tuple(normalize_job_state(state) for state in db_states)
    if db == ("JOB_HELD",):
        if raw_tail == ("JOB_HELD", "JOB_HELD_REASON"):
            return True
        if raw_tail == ("JOB_HELD",):
            return plain_held_is_confirming
        if raw_tail == ("JOB_HELD_REASON",):
            return True
        if tail and all(state == "JOB_HELD" for state in tail):
            return False
    if tail == db:
        return True
    if len(tail) == 1 and tail[0] in SYNTHETIC_FAILURE_CAUSES:
        return db == (tail[0], "JOB_FAILURE")
    return False


@dataclass(frozen=True, slots=True, order=True)
class WorkflowIdentity:
    wf_uuid: str
    root_wf_uuid: str

    def __post_init__(self) -> None:
        if not self.wf_uuid or not self.root_wf_uuid:
            raise ValueError("workflow UUIDs must not be empty")

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {"wf_uuid": self.wf_uuid, "root_wf_uuid": self.root_wf_uuid}


@dataclass(frozen=True, slots=True, order=True)
class DatabaseGeneration:
    generation: int
    device: int
    inode: int

    def __post_init__(self) -> None:
        _require_nonnegative("generation", self.generation)
        _require_nonnegative("device", self.device)
        _require_nonnegative("inode", self.inode)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "generation": self.generation,
            "device": self.device,
            "inode": self.inode,
        }


@dataclass(frozen=True, slots=True, order=True)
class TailGeneration:
    generation: int
    device: int
    inode: int

    def __post_init__(self) -> None:
        _require_nonnegative("generation", self.generation)
        _require_nonnegative("device", self.device)
        _require_nonnegative("inode", self.inode)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "generation": self.generation,
            "device": self.device,
            "inode": self.inode,
        }


@dataclass(frozen=True, slots=True, order=True)
class SnapshotEpoch:
    value: int

    def __post_init__(self) -> None:
        _require_nonnegative("snapshot epoch", self.value)


@dataclass(frozen=True, slots=True, order=True)
class ClockSample:
    """Coordinator-owned wall and monotonic readings for one source call."""

    epoch: float
    monotonic: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.epoch) or not math.isfinite(self.monotonic):
            raise ValueError("clock samples must be finite")


@dataclass(frozen=True, slots=True, order=True)
class DBTransitionIdentity:
    """Exact four-column Stampede ``jobstate`` primary key."""

    job_instance_id: int
    state: str
    timestamp: Decimal
    jobstate_submit_seq: int

    def __post_init__(self) -> None:
        _require_nonnegative("job_instance_id", self.job_instance_id)
        _require_nonnegative("jobstate_submit_seq", self.jobstate_submit_seq)
        if not self.state:
            raise ValueError("state must not be empty")
        object.__setattr__(self, "timestamp", db_timestamp(self.timestamp))

    @property
    def producer_timestamp(self) -> str:
        return producer_timestamp_key(self.timestamp)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "job_instance_id": self.job_instance_id,
            "state": self.state,
            "timestamp": str(self.timestamp),
            "jobstate_submit_seq": self.jobstate_submit_seq,
        }


@dataclass(frozen=True, slots=True, order=True)
class DBWorkflowTransitionIdentity:
    """Exact three-column Stampede ``workflowstate`` primary key."""

    wf_id: int
    state: str
    timestamp: Decimal

    def __post_init__(self) -> None:
        _require_nonnegative("wf_id", self.wf_id)
        if not self.state:
            raise ValueError("state must not be empty")
        object.__setattr__(self, "timestamp", db_timestamp(self.timestamp))

    @property
    def producer_timestamp(self) -> str:
        return producer_timestamp_key(self.timestamp)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "wf_id": self.wf_id,
            "state": self.state,
            "timestamp": str(self.timestamp),
        }


@dataclass(frozen=True, slots=True, order=True)
class TailTransitionIdentity:
    source_generation: TailGeneration
    start_offset: int

    def __post_init__(self) -> None:
        _require_nonnegative("start_offset", self.start_offset)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "source_generation": self.source_generation.to_json_dict(),
            "start_offset": self.start_offset,
        }


@dataclass(frozen=True, slots=True, order=True)
class JobSemanticKey:
    wf_uuid: str
    exec_job_id: str
    job_submit_seq: int
    producer_timestamp: str
    normalized_state: str


@dataclass(frozen=True, slots=True, order=True)
class WorkflowSemanticKey:
    wf_uuid: str
    restart_count: int
    producer_timestamp: str
    normalized_state: str
    status: int | None


@dataclass(frozen=True, slots=True)
class DBJobTransition:
    workflow: WorkflowIdentity
    exec_job_id: str
    job_submit_seq: int
    identity: DBTransitionIdentity
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_nonnegative("job_submit_seq", self.job_submit_seq)
        if not self.exec_job_id:
            raise ValueError("exec_job_id must not be empty")

    @property
    def normalized_state(self) -> str:
        return normalize_job_state(self.identity.state)

    @property
    def semantic_key(self) -> JobSemanticKey:
        return JobSemanticKey(
            self.workflow.wf_uuid,
            self.exec_job_id,
            self.job_submit_seq,
            self.identity.producer_timestamp,
            self.normalized_state,
        )

    @property
    def authoritative_sort_key(self) -> tuple[int, Decimal, int, str]:
        return (
            self.identity.jobstate_submit_seq,
            self.identity.timestamp,
            state_precedence(self.identity.state),
            self.identity.state,
        )

    @property
    def recent_event_sort_key(
        self,
    ) -> tuple[Decimal, str, str, int, int, int, int, str]:
        """Stable cross-instance ordering for the bounded recent-event feed."""

        return (
            self.identity.timestamp,
            self.workflow.wf_uuid,
            self.exec_job_id,
            self.identity.job_instance_id,
            self.job_submit_seq,
            self.identity.jobstate_submit_seq,
            state_precedence(self.identity.state),
            self.identity.state,
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "exec_job_id": self.exec_job_id,
            "job_submit_seq": self.job_submit_seq,
            "identity": self.identity.to_json_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class DBWorkflowTransition:
    workflow: WorkflowIdentity
    identity: DBWorkflowTransitionIdentity
    restart_count: int
    status: int | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_nonnegative("restart_count", self.restart_count)

    @property
    def normalized_state(self) -> str:
        return normalize_workflow_state(self.identity.state)

    @property
    def semantic_key(self) -> WorkflowSemanticKey:
        return WorkflowSemanticKey(
            self.workflow.wf_uuid,
            self.restart_count,
            self.identity.producer_timestamp,
            self.normalized_state,
            self.status if self.normalized_state == "WORKFLOW_TERMINATED" else None,
        )

    @property
    def authoritative_sort_key(self) -> tuple[int, int, Decimal, str]:
        phase = 0 if self.normalized_state == "WORKFLOW_STARTED" else 1
        return (
            self.restart_count,
            phase,
            self.identity.timestamp,
            self.identity.state,
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "identity": self.identity.to_json_dict(),
            "restart_count": self.restart_count,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class TailJobEvent:
    workflow: WorkflowIdentity
    identity: TailTransitionIdentity
    base_db_generation: DatabaseGeneration | None
    end_offset: int
    observed_at_monotonic: float = field(compare=False, repr=False)
    event_timestamp: Decimal
    exec_job_id: str
    state: str
    job_submit_seq: int
    raw_value: str
    raw_site: str
    raw_walltime: str
    original_line: str = field(compare=False)
    status: int | None = None
    scheduler_id: str | None = None
    walltime_seconds: int | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at_monotonic):
            raise ValueError("observed_at_monotonic must be finite")
        if self.end_offset <= self.identity.start_offset:
            raise ValueError("end_offset must be after start_offset")
        if not self.exec_job_id:
            raise ValueError("exec_job_id must not be empty")
        normalize_job_state(self.state)
        _require_nonnegative("job_submit_seq", self.job_submit_seq)
        _bounded_text(self.original_line, MAX_TAIL_LINE_CHARS, "tail line")
        if self.walltime_seconds is not None:
            _require_nonnegative("walltime_seconds", self.walltime_seconds)
        object.__setattr__(self, "event_timestamp", db_timestamp(self.event_timestamp))

    @property
    def normalized_state(self) -> str:
        return normalize_job_state(self.state)

    @property
    def semantic_key(self) -> JobSemanticKey:
        return JobSemanticKey(
            self.workflow.wf_uuid,
            self.exec_job_id,
            self.job_submit_seq,
            producer_timestamp_key(self.event_timestamp),
            self.normalized_state,
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        """Diagnostic shape; intentionally excludes the monotonic clock."""

        return {
            "workflow": self.workflow.to_json_dict(),
            "identity": self.identity.to_json_dict(),
            "base_db_generation": _json_value(self.base_db_generation),
            "end_offset": self.end_offset,
            "event_timestamp": str(self.event_timestamp),
            "exec_job_id": self.exec_job_id,
            "state": self.state,
            "job_submit_seq": self.job_submit_seq,
            "raw_value": self.raw_value,
            "raw_site": self.raw_site,
            "raw_walltime": self.raw_walltime,
            "original_line": self.original_line,
            "status": self.status,
            "scheduler_id": self.scheduler_id,
            "walltime_seconds": self.walltime_seconds,
        }


@dataclass(frozen=True, slots=True)
class TailWorkflowEvent:
    workflow: WorkflowIdentity
    identity: TailTransitionIdentity
    base_db_generation: DatabaseGeneration | None
    end_offset: int
    observed_at_monotonic: float = field(compare=False, repr=False)
    event_timestamp: Decimal
    marker: str
    status: int | None
    original_line: str = field(compare=False)
    dagman_cluster: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at_monotonic):
            raise ValueError("observed_at_monotonic must be finite")
        if self.end_offset <= self.identity.start_offset:
            raise ValueError("end_offset must be after start_offset")
        _bounded_text(self.original_line, MAX_TAIL_LINE_CHARS, "tail line")
        object.__setattr__(self, "event_timestamp", db_timestamp(self.event_timestamp))
        if self.normalized_state not in {
            "WORKFLOW_STARTED",
            "WORKFLOW_TERMINATED",
        }:
            raise ValueError("tail workflow event must be a DAGMan marker")

    @property
    def normalized_state(self) -> str:
        return normalize_workflow_state(self.marker)

    def semantic_key_for(self, restart_count: int) -> WorkflowSemanticKey:
        """Build the match key after WP4 assigns the provisional restart."""

        _require_nonnegative("restart_count", restart_count)
        return WorkflowSemanticKey(
            self.workflow.wf_uuid,
            restart_count,
            producer_timestamp_key(self.event_timestamp),
            self.normalized_state,
            self.status if self.normalized_state == "WORKFLOW_TERMINATED" else None,
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "identity": self.identity.to_json_dict(),
            "base_db_generation": _json_value(self.base_db_generation),
            "end_offset": self.end_offset,
            "event_timestamp": str(self.event_timestamp),
            "marker": self.marker,
            "status": self.status,
            "dagman_cluster": self.dagman_cluster,
            "original_line": self.original_line,
        }


@dataclass(frozen=True, slots=True)
class TailSourceEvent:
    """A monitord lifecycle marker; it never defines workflow outcome."""

    workflow: WorkflowIdentity
    identity: TailTransitionIdentity
    base_db_generation: DatabaseGeneration | None
    end_offset: int
    observed_at_monotonic: float = field(compare=False, repr=False)
    event_timestamp: Decimal
    marker: TailSourceMarker
    status: int | None
    original_line: str = field(compare=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at_monotonic):
            raise ValueError("observed_at_monotonic must be finite")
        if self.end_offset <= self.identity.start_offset:
            raise ValueError("end_offset must be after start_offset")
        _bounded_text(self.original_line, MAX_TAIL_LINE_CHARS, "tail line")
        object.__setattr__(self, "event_timestamp", db_timestamp(self.event_timestamp))

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "identity": self.identity.to_json_dict(),
            "base_db_generation": _json_value(self.base_db_generation),
            "end_offset": self.end_offset,
            "event_timestamp": str(self.event_timestamp),
            "marker": self.marker.value,
            "status": self.status,
            "original_line": self.original_line,
        }


@dataclass(frozen=True, slots=True, order=True)
class BoundedAge:
    seconds: float
    maximum_seconds: float
    capped: bool

    def __post_init__(self) -> None:
        if not math.isfinite(self.seconds) or not math.isfinite(self.maximum_seconds):
            raise ValueError("age values must be finite")
        if self.seconds < 0 or self.maximum_seconds <= 0:
            raise ValueError("age values must be positive")
        if self.seconds > self.maximum_seconds:
            raise ValueError("bounded age exceeds its maximum")

    @classmethod
    def between(
        cls, now_epoch: float, then_epoch: float, maximum_seconds: float
    ) -> BoundedAge:
        raw = max(0.0, now_epoch - then_epoch)
        return cls(min(raw, maximum_seconds), maximum_seconds, raw > maximum_seconds)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "seconds": self.seconds,
            "maximum_seconds": self.maximum_seconds,
            "capped": self.capped,
        }


@dataclass(frozen=True, slots=True)
class SourceHealth:
    source: SourceName
    state: HealthState
    checked_at_epoch: float
    last_success_epoch: float | None = None
    last_good_age: BoundedAge | None = None
    stale_after_seconds: float | None = None
    consecutive_failures: int = 0
    pending_count: int = 0
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.checked_at_epoch):
            raise ValueError("checked_at_epoch must be finite")
        if self.last_success_epoch is not None and not math.isfinite(
            self.last_success_epoch
        ):
            raise ValueError("last_success_epoch must be finite")
        if self.stale_after_seconds is not None and (
            not math.isfinite(self.stale_after_seconds) or self.stale_after_seconds <= 0
        ):
            raise ValueError("stale_after_seconds must be positive and finite")
        _require_nonnegative("consecutive_failures", self.consecutive_failures)
        _require_nonnegative("pending_count", self.pending_count)
        _bounded_text(self.detail, MAX_HEALTH_DETAIL_CHARS, "health detail")

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "source": self.source.value,
            "state": self.state.value,
            "checked_at_epoch": self.checked_at_epoch,
            "last_success_epoch": self.last_success_epoch,
            "last_good_age": _json_value(self.last_good_age),
            "stale_after_seconds": self.stale_after_seconds,
            "consecutive_failures": self.consecutive_failures,
            "pending_count": self.pending_count,
            "error_code": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class FrozenPayload:
    """Hashable, recursively immutable provider/diagnostic JSON payload."""

    fields: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        frozen = tuple(
            sorted((str(key), self._freeze(value)) for key, value in self.fields)
        )
        if len({key for key, _ in frozen}) != len(frozen):
            raise ValueError("payload keys must be unique")
        object.__setattr__(self, "fields", frozen)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> FrozenPayload:
        return cls(tuple(values.items()))

    @classmethod
    def _freeze(cls, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("payload floats must be finite")
            return value
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (Path, UUID)):
            return str(value)
        if isinstance(value, Enum):
            return cls._freeze(value.value)
        if isinstance(value, FrozenPayload):
            return value
        if isinstance(value, Mapping):
            return cls.from_mapping(value)
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            return tuple(cls._freeze(item) for item in value)
        raise TypeError(f"unsupported payload value: {type(value).__name__}")

    @classmethod
    def _thaw(cls, value: Any) -> JSONValue:
        if isinstance(value, FrozenPayload):
            return value.to_json_dict()
        if isinstance(value, tuple):
            return [cls._thaw(item) for item in value]
        return _json_value(value)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {key: self._thaw(value) for key, value in self.fields}


@dataclass(frozen=True, slots=True, order=True)
class JobAttemptIdentity:
    job_id: int
    job_instance_id: int
    job_submit_seq: int

    def __post_init__(self) -> None:
        _require_nonnegative("job_id", self.job_id)
        _require_nonnegative("job_instance_id", self.job_instance_id)
        _require_nonnegative("job_submit_seq", self.job_submit_seq)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "job_id": self.job_id,
            "job_instance_id": self.job_instance_id,
            "job_submit_seq": self.job_submit_seq,
        }


@dataclass(frozen=True, slots=True)
class JobAttempt:
    identity: JobAttemptIdentity
    scheduler_id: str | None = None
    site: str | None = None
    submit_time: Decimal | None = None
    start_time: Decimal | None = None
    end_time: Decimal | None = None
    raw_wait_status: int | None = None
    exit_code: int | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    maxrss_kb: int | None = None

    def __post_init__(self) -> None:
        for name in ("submit_time", "start_time", "end_time"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, db_timestamp(value))
        if self.maxrss_kb is not None:
            _require_nonnegative("maxrss_kb", self.maxrss_kb)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "identity": self.identity.to_json_dict(),
            "scheduler_id": self.scheduler_id,
            "site": self.site,
            "submit_time": _json_value(self.submit_time),
            "start_time": _json_value(self.start_time),
            "end_time": _json_value(self.end_time),
            "raw_wait_status": self.raw_wait_status,
            "exit_code": self.exit_code,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "maxrss_kb": self.maxrss_kb,
        }


@dataclass(frozen=True, slots=True)
class WorkflowSnapshot:
    workflow: WorkflowIdentity
    wf_id: int
    state: str
    status: int | None
    restart_count: int
    started_at: Decimal | None
    ended_at: Decimal | None
    transition: DBWorkflowTransition | None
    provenance: Provenance = Provenance.DB_CONFIRMED
    pending_tail: tuple[TailTransitionIdentity, ...] = ()

    def __post_init__(self) -> None:
        _require_nonnegative("wf_id", self.wf_id)
        _require_nonnegative("restart_count", self.restart_count)
        if not self.state:
            raise ValueError("workflow state must not be empty")
        for name in ("started_at", "ended_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, db_timestamp(value))
        object.__setattr__(self, "pending_tail", tuple(self.pending_tail))
        if self.pending_tail and self.provenance is Provenance.DB_CONFIRMED:
            raise ValueError("pending workflow events require live provenance")
        if self.provenance is Provenance.DB_CONFIRMED and self.transition is None:
            raise ValueError(
                "authoritative workflow state requires an exact transition"
            )
        if self.transition is not None and (
            self.transition.workflow != self.workflow
            or self.transition.identity.wf_id != self.wf_id
        ):
            raise ValueError("workflow transition must match the workflow snapshot")
        if self.provenance is Provenance.DB_CONFIRMED and self.transition is not None:
            if normalize_workflow_state(self.state) != self.transition.normalized_state:
                raise ValueError(
                    "authoritative workflow state must match its transition"
                )
            if self.restart_count != self.transition.restart_count:
                raise ValueError(
                    "authoritative workflow restart must match its transition"
                )
            if (
                self.transition.normalized_state == "WORKFLOW_TERMINATED"
                and self.status != self.transition.status
            ):
                raise ValueError(
                    "authoritative workflow status must match its transition"
                )
            transition_timestamp = self.transition.identity.timestamp
            if (
                self.transition.normalized_state == "WORKFLOW_STARTED"
                and self.started_at != transition_timestamp
            ):
                raise ValueError(
                    "authoritative workflow start time must match its transition"
                )
            if (
                self.transition.normalized_state == "WORKFLOW_TERMINATED"
                and self.ended_at != transition_timestamp
            ):
                raise ValueError(
                    "authoritative workflow end time must match its transition"
                )

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "wf_id": self.wf_id,
            "state": self.state,
            "status": self.status,
            "restart_count": self.restart_count,
            "started_at": _json_value(self.started_at),
            "ended_at": _json_value(self.ended_at),
            "transition": _json_value(self.transition),
            "provenance": self.provenance.value,
            "pending_tail": _json_value(self.pending_tail),
        }


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    workflow: WorkflowIdentity
    job_id: int | None
    exec_job_id: str
    type_desc: str
    task_count: int
    transformations: tuple[str, ...]
    attempts: tuple[JobAttempt, ...]
    current_attempt: JobAttemptIdentity | None
    state: str | None
    state_timestamp: Decimal | None
    transition: DBJobTransition | None
    provenance: Provenance
    pending_tail: tuple[TailTransitionIdentity, ...] = ()
    scheduler: FrozenPayload = field(default_factory=FrozenPayload)

    def __post_init__(self) -> None:
        if self.job_id is not None:
            _require_nonnegative("job_id", self.job_id)
        _require_nonnegative("task_count", self.task_count)
        if not self.exec_job_id:
            raise ValueError("exec_job_id must not be empty")
        if (self.job_id is None) != (self.provenance is Provenance.PROVISIONAL_JOB):
            raise ValueError("only provisional jobs may omit the DB job_id")
        if self.provenance is Provenance.PROVISIONAL_JOB and (
            self.attempts
            or self.current_attempt is not None
            or self.transition is not None
        ):
            raise ValueError(
                "provisional jobs cannot fabricate DB attempts or transitions"
            )
        if self.provenance is Provenance.PROVISIONAL_JOB and not self.pending_tail:
            raise ValueError("provisional jobs require a pending tail identity")
        if self.provenance is Provenance.DB_CONFIRMED and (
            (self.state is None) != (self.transition is None)
            or (self.state is None) != (self.state_timestamp is None)
        ):
            raise ValueError(
                "authoritative job state requires an exact transition and timestamp"
            )
        object.__setattr__(self, "transformations", tuple(self.transformations))
        object.__setattr__(self, "attempts", tuple(self.attempts))
        object.__setattr__(self, "pending_tail", tuple(self.pending_tail))
        if self.state_timestamp is not None:
            object.__setattr__(
                self, "state_timestamp", db_timestamp(self.state_timestamp)
            )
        attempt_id_list = [attempt.identity for attempt in self.attempts]
        if len(set(attempt_id_list)) != len(attempt_id_list):
            raise ValueError("job attempts must be unique")
        attempt_instance_ids = [
            identity.job_instance_id for identity in attempt_id_list
        ]
        if len(set(attempt_instance_ids)) != len(attempt_instance_ids):
            raise ValueError("job attempt instance IDs must be unique")
        attempt_submit_seqs = [identity.job_submit_seq for identity in attempt_id_list]
        if len(set(attempt_submit_seqs)) != len(attempt_submit_seqs):
            raise ValueError("job attempt submit sequences must be unique")
        if self.job_id is not None and any(
            identity.job_id != self.job_id for identity in attempt_id_list
        ):
            raise ValueError("job attempts must match the DB job_id")
        attempt_ids = set(attempt_id_list)
        if self.current_attempt is not None and self.current_attempt not in attempt_ids:
            raise ValueError("current_attempt must identify an included attempt")
        if attempt_id_list and self.current_attempt != max(
            attempt_id_list, key=lambda identity: identity.job_submit_seq
        ):
            raise ValueError("current_attempt must have the highest job_submit_seq")
        if self.transition is not None:
            attempts_by_instance = {
                identity.job_instance_id: identity for identity in attempt_ids
            }
            transition_attempt = attempts_by_instance.get(
                self.transition.identity.job_instance_id
            )
            if (
                self.transition.workflow != self.workflow
                or self.transition.exec_job_id != self.exec_job_id
                or transition_attempt is None
                or transition_attempt.job_submit_seq != self.transition.job_submit_seq
            ):
                raise ValueError("job transition must match an included attempt")
        if self.provenance is Provenance.DB_CONFIRMED and self.transition is not None:
            if (
                self.state is None
                or normalize_job_state(self.state) != self.transition.normalized_state
            ):
                raise ValueError("authoritative job state must match its transition")
            if self.state_timestamp != self.transition.identity.timestamp:
                raise ValueError(
                    "authoritative job timestamp must match its transition"
                )
            if (
                self.current_attempt is None
                or self.current_attempt.job_instance_id
                != self.transition.identity.job_instance_id
                or self.current_attempt.job_submit_seq != self.transition.job_submit_seq
            ):
                raise ValueError(
                    "authoritative transition must match the current attempt"
                )

    @property
    def lifecycle(self) -> Lifecycle:
        return job_lifecycle(self.state)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "job_id": self.job_id,
            "exec_job_id": self.exec_job_id,
            "type_desc": self.type_desc,
            "task_count": self.task_count,
            "transformations": _json_value(self.transformations),
            "attempts": _json_value(self.attempts),
            "current_attempt": _json_value(self.current_attempt),
            "state": self.state,
            "state_timestamp": _json_value(self.state_timestamp),
            "transition": _json_value(self.transition),
            "provenance": self.provenance.value,
            "pending_tail": _json_value(self.pending_tail),
            "scheduler": self.scheduler.to_json_dict(),
        }


@dataclass(frozen=True, slots=True)
class JobTransitionWatermark:
    job_instance_id: int
    highest_jobstate_submit_seq: int
    identities_at_highest_seq: tuple[DBTransitionIdentity, ...]

    def __post_init__(self) -> None:
        _require_nonnegative("job_instance_id", self.job_instance_id)
        _require_nonnegative(
            "highest_jobstate_submit_seq", self.highest_jobstate_submit_seq
        )
        identities = tuple(self.identities_at_highest_seq)
        if not identities:
            raise ValueError("job watermark requires at least one transition identity")
        if any(
            identity.job_instance_id != self.job_instance_id for identity in identities
        ):
            raise ValueError("watermark identities must belong to the same instance")
        if any(
            identity.jobstate_submit_seq != self.highest_jobstate_submit_seq
            for identity in identities
        ):
            raise ValueError("watermark identities must be at the highest sequence")
        if len(set(identities)) != len(identities):
            raise ValueError("watermark identities must be unique")
        object.__setattr__(self, "identities_at_highest_seq", identities)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "job_instance_id": self.job_instance_id,
            "highest_jobstate_submit_seq": self.highest_jobstate_submit_seq,
            "identities_at_highest_seq": _json_value(self.identities_at_highest_seq),
        }


@dataclass(frozen=True, slots=True, order=True)
class WorkflowRestartIdentity:
    workflow: WorkflowIdentity
    wf_id: int
    restart_count: int

    def __post_init__(self) -> None:
        _require_nonnegative("wf_id", self.wf_id)
        _require_nonnegative("restart_count", self.restart_count)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "workflow": self.workflow.to_json_dict(),
            "wf_id": self.wf_id,
            "restart_count": self.restart_count,
        }


@dataclass(frozen=True, slots=True)
class WorkflowTransitionWatermark:
    restart: WorkflowRestartIdentity
    identities: tuple[DBWorkflowTransitionIdentity, ...]

    def __post_init__(self) -> None:
        identities = tuple(self.identities)
        if any(identity.wf_id != self.restart.wf_id for identity in identities):
            raise ValueError("workflow watermark identities must match wf_id")
        if len(set(identities)) != len(identities):
            raise ValueError("workflow watermark identities must be unique")
        object.__setattr__(self, "identities", identities)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "restart": self.restart.to_json_dict(),
            "identities": _json_value(self.identities),
        }


@dataclass(frozen=True, slots=True)
class DatabaseSnapshot:
    """One consistent, authoritative Stampede read transaction."""

    epoch: SnapshotEpoch
    generation: DatabaseGeneration
    snapshot_at_epoch: float
    workflow: WorkflowSnapshot
    jobs: tuple[JobSnapshot, ...]
    recent_transitions: tuple[DBJobTransition, ...]
    recent_workflow_transitions: tuple[DBWorkflowTransition, ...]
    watermarks: tuple[JobTransitionWatermark, ...]
    workflow_watermark: WorkflowTransitionWatermark

    def __post_init__(self) -> None:
        if not math.isfinite(self.snapshot_at_epoch):
            raise ValueError("snapshot_at_epoch must be finite")
        jobs = tuple(self.jobs)
        transitions = tuple(self.recent_transitions)
        workflow_transitions = tuple(self.recent_workflow_transitions)
        watermarks = tuple(self.watermarks)
        if self.workflow.provenance is not Provenance.DB_CONFIRMED:
            raise ValueError("database workflow state must be DB-confirmed")
        if self.workflow.pending_tail:
            raise ValueError(
                "database workflow state cannot contain pending tail events"
            )
        if any(job.workflow != self.workflow.workflow for job in jobs):
            raise ValueError("all DB jobs must match the exact selected wf_uuid scope")
        if any(
            job.provenance is not Provenance.DB_CONFIRMED
            or job.job_id is None
            or job.pending_tail
            or job.scheduler.fields
            for job in jobs
        ):
            raise ValueError("database snapshots contain DB-confirmed jobs only")
        job_ids = [job.job_id for job in jobs]
        if len(set(job_ids)) != len(job_ids):
            raise ValueError("database snapshots contain one row per unique job_id")
        exec_job_ids = [job.exec_job_id for job in jobs]
        if len(set(exec_job_ids)) != len(exec_job_ids):
            raise ValueError("database snapshots contain one job per exec_job_id")
        attempts = [attempt.identity for job in jobs for attempt in job.attempts]
        if len(set(attempts)) != len(attempts):
            raise ValueError("database snapshots contain unique job attempts")
        attempt_instance_ids = [attempt.job_instance_id for attempt in attempts]
        if len(set(attempt_instance_ids)) != len(attempt_instance_ids):
            raise ValueError("job_instance_id must be workflow-unique")
        attempt_submit_seqs = [attempt.job_submit_seq for attempt in attempts]
        if len(set(attempt_submit_seqs)) != len(attempt_submit_seqs):
            raise ValueError("job_submit_seq must be workflow-global unique")
        included_instance_ids = set(attempt_instance_ids)
        watermark_instance_ids = [watermark.job_instance_id for watermark in watermarks]
        if len(set(watermark_instance_ids)) != len(watermark_instance_ids):
            raise ValueError("job watermarks must be unique per job_instance_id")
        watermarks_by_instance = {
            watermark.job_instance_id: watermark for watermark in watermarks
        }
        if any(
            watermark.job_instance_id not in included_instance_ids
            for watermark in watermarks
        ):
            raise ValueError("job watermarks must match included attempts")
        watermark_identities = [
            identity
            for watermark in watermarks
            for identity in watermark.identities_at_highest_seq
        ]
        if len(set(watermark_identities)) != len(watermark_identities):
            raise ValueError("job watermark transition identities must be unique")
        if any(
            transition.workflow != self.workflow.workflow for transition in transitions
        ):
            raise ValueError("DB job transitions must match the selected wf_uuid")
        transition_identities = [transition.identity for transition in transitions]
        if len(set(transition_identities)) != len(transition_identities):
            raise ValueError("DB job transition identities must be unique")
        if transitions != tuple(
            sorted(transitions, key=lambda transition: transition.recent_event_sort_key)
        ):
            raise ValueError("DB recent job transitions must use stable feed order")
        jobs_by_exec_id = {job.exec_job_id: job for job in jobs}
        for transition in transitions:
            job = jobs_by_exec_id.get(transition.exec_job_id)
            if job is None:
                raise ValueError("DB job transition has no included job row")
            matching_attempts = {
                attempt.identity.job_submit_seq: attempt.identity.job_instance_id
                for attempt in job.attempts
            }
            if (
                matching_attempts.get(transition.job_submit_seq)
                != transition.identity.job_instance_id
            ):
                raise ValueError("DB job transition has no included attempt row")
            watermark = watermarks_by_instance.get(transition.identity.job_instance_id)
            if (
                watermark is None
                or watermark.highest_jobstate_submit_seq
                < transition.identity.jobstate_submit_seq
                or (
                    watermark.highest_jobstate_submit_seq
                    == transition.identity.jobstate_submit_seq
                    and transition.identity not in watermark.identities_at_highest_seq
                )
            ):
                raise ValueError("job watermark is behind the recent transition feed")
        for job in jobs:
            if job.transition is None:
                continue
            watermark = watermarks_by_instance.get(
                job.transition.identity.job_instance_id
            )
            if (
                watermark is None
                or job.transition.identity not in watermark.identities_at_highest_seq
            ):
                raise ValueError(
                    "current job transition must be contained in its watermark"
                )
            selected_identity = max(
                watermark.identities_at_highest_seq,
                key=lambda identity: (
                    identity.timestamp,
                    state_precedence(identity.state),
                    identity.state,
                ),
            )
            if job.transition.identity != selected_identity:
                raise ValueError(
                    "current job transition must be the authoritative watermark row"
                )
            same_instance_recent = [
                transition
                for transition in transitions
                if transition.identity.job_instance_id
                == job.transition.identity.job_instance_id
            ]
            if same_instance_recent and (
                max(
                    same_instance_recent + [job.transition],
                    key=lambda transition: transition.authoritative_sort_key,
                ).identity
                != job.transition.identity
            ):
                raise ValueError(
                    "current job transition cannot lag the recent transition feed"
                )
        if any(
            transition.workflow != self.workflow.workflow
            for transition in workflow_transitions
        ):
            raise ValueError("DB workflow transitions must match the selected wf_uuid")
        if any(
            transition.identity.wf_id != self.workflow.wf_id
            for transition in workflow_transitions
        ):
            raise ValueError("DB workflow transition has no included workflow row")
        if any(
            transition.restart_count > self.workflow.restart_count
            for transition in workflow_transitions
        ):
            raise ValueError("DB workflow transition belongs to a future restart")
        workflow_transition_identities = [
            transition.identity for transition in workflow_transitions
        ]
        if len(set(workflow_transition_identities)) != len(
            workflow_transition_identities
        ):
            raise ValueError("DB workflow transition identities must be unique")
        if workflow_transitions != tuple(
            sorted(
                workflow_transitions,
                key=lambda transition: transition.authoritative_sort_key,
            )
        ):
            raise ValueError(
                "DB recent workflow transitions must use authoritative order"
            )
        if self.workflow_watermark.restart.workflow != self.workflow.workflow:
            raise ValueError("workflow watermark must match the selected wf_uuid")
        if (
            self.workflow_watermark.restart.wf_id != self.workflow.wf_id
            or self.workflow_watermark.restart.restart_count
            != self.workflow.restart_count
        ):
            raise ValueError("workflow watermark must identify the current restart")
        if (
            self.workflow.transition is not None
            and self.workflow.transition.identity
            not in self.workflow_watermark.identities
        ):
            raise ValueError(
                "current workflow transition must be contained in its watermark"
            )
        if self.workflow.transition is not None:
            selected_workflow_identity = max(
                self.workflow_watermark.identities,
                key=lambda identity: (
                    0
                    if normalize_workflow_state(identity.state) == "WORKFLOW_STARTED"
                    else 1,
                    identity.timestamp,
                    identity.state,
                ),
            )
            if self.workflow.transition.identity != selected_workflow_identity:
                raise ValueError(
                    "current workflow transition must be the authoritative watermark row"
                )
            current_restart_identities = {
                transition.identity
                for transition in workflow_transitions
                if transition.restart_count == self.workflow.restart_count
            }
            if not current_restart_identities.issubset(
                set(self.workflow_watermark.identities)
            ):
                raise ValueError(
                    "workflow watermark is behind the recent transition feed"
                )
            if workflow_transitions and (
                max(
                    workflow_transitions + (self.workflow.transition,),
                    key=lambda transition: transition.authoritative_sort_key,
                ).identity
                != self.workflow.transition.identity
            ):
                raise ValueError(
                    "current workflow transition cannot lag the recent transition feed"
                )
        object.__setattr__(self, "jobs", jobs)
        object.__setattr__(self, "recent_transitions", transitions)
        object.__setattr__(self, "recent_workflow_transitions", workflow_transitions)
        object.__setattr__(self, "watermarks", watermarks)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "epoch": self.epoch.value,
            "generation": self.generation.to_json_dict(),
            "snapshot_at_epoch": self.snapshot_at_epoch,
            "workflow": self.workflow.to_json_dict(),
            "jobs": _json_value(self.jobs),
            "recent_transitions": _json_value(self.recent_transitions),
            "recent_workflow_transitions": _json_value(
                self.recent_workflow_transitions
            ),
            "watermarks": _json_value(self.watermarks),
            "workflow_watermark": self.workflow_watermark.to_json_dict(),
        }


EffectiveEventValue = (
    DBJobTransition | DBWorkflowTransition | TailJobEvent | TailWorkflowEvent
)


@dataclass(frozen=True, slots=True)
class EffectiveEvent:
    """One presentation-neutral event in coordinator publication order."""

    order: int
    provenance: Provenance
    event: EffectiveEventValue

    def __post_init__(self) -> None:
        _require_nonnegative("event order", self.order)
        if isinstance(self.event, (DBJobTransition, DBWorkflowTransition)):
            expected = Provenance.DB_CONFIRMED
        else:
            expected = Provenance.TAIL_PENDING
        if self.provenance is not expected:
            raise ValueError("effective event provenance does not match its source")

    @property
    def workflow(self) -> WorkflowIdentity:
        return self.event.workflow

    @property
    def kind(self) -> str:
        if isinstance(self.event, DBJobTransition):
            return "db_job_transition"
        if isinstance(self.event, DBWorkflowTransition):
            return "db_workflow_transition"
        if isinstance(self.event, TailJobEvent):
            return "tail_job_transition"
        return "tail_workflow_transition"

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "order": self.order,
            "provenance": self.provenance.value,
            "kind": self.kind,
            "event": self.event.to_json_dict(),
        }


@dataclass(frozen=True, slots=True)
class EffectiveSnapshot:
    epoch: SnapshotEpoch
    workflow: WorkflowSnapshot
    jobs: tuple[JobSnapshot, ...]
    db_generation: DatabaseGeneration | None
    tail_generation: TailGeneration | None
    published_at_epoch: float
    published_at_monotonic: float = field(compare=False, repr=False)
    source_health: tuple[SourceHealth, ...] = ()
    events: tuple[EffectiveEvent, ...] = ()

    def __post_init__(self) -> None:
        if not math.isfinite(self.published_at_epoch) or not math.isfinite(
            self.published_at_monotonic
        ):
            raise ValueError("snapshot clocks must be finite")
        jobs = tuple(self.jobs)
        health = tuple(self.source_health)
        events = tuple(self.events)
        if any(job.workflow != self.workflow.workflow for job in jobs):
            raise ValueError("all jobs must match the exact selected wf_uuid scope")
        exec_job_ids = [job.exec_job_id for job in jobs]
        if len(set(exec_job_ids)) != len(exec_job_ids):
            raise ValueError("effective snapshots require unique exec_job_id values")
        attempt_identities = [
            attempt.identity for job in jobs for attempt in job.attempts
        ]
        attempt_instance_ids = [
            identity.job_instance_id for identity in attempt_identities
        ]
        if len(set(attempt_instance_ids)) != len(attempt_instance_ids):
            raise ValueError(
                "effective snapshots require workflow-unique job_instance_id values"
            )
        attempt_submit_seqs = [
            identity.job_submit_seq for identity in attempt_identities
        ]
        if len(set(attempt_submit_seqs)) != len(attempt_submit_seqs):
            raise ValueError(
                "effective snapshots require workflow-global job_submit_seq values"
            )
        if any(event.workflow != self.workflow.workflow for event in events):
            raise ValueError("all events must match the exact selected wf_uuid scope")
        if any(
            previous.order >= current.order
            for previous, current in zip(events, events[1:])
        ):
            raise ValueError("effective events must have strictly increasing order")
        workflow_pending_identities = tuple(self.workflow.pending_tail)
        job_pending_identities = {
            job.exec_job_id: tuple(job.pending_tail) for job in jobs if job.pending_tail
        }
        pending_identities = workflow_pending_identities + tuple(
            identity
            for identities in job_pending_identities.values()
            for identity in identities
        )
        if len(set(pending_identities)) != len(pending_identities):
            raise ValueError("pending overlay identities must be unique")
        if pending_identities and self.tail_generation is None:
            raise ValueError("pending overlays require a tail generation")
        if any(
            identity.source_generation != self.tail_generation
            for identity in pending_identities
        ):
            raise ValueError("pending overlay tail generation conflicts with snapshot")
        workflow_tail_event_identities: list[TailTransitionIdentity] = []
        job_tail_events: dict[str, list[TailJobEvent]] = {}
        ordered_tail_events: list[TailJobEvent | TailWorkflowEvent] = []
        db_event_identities: list[
            DBTransitionIdentity | DBWorkflowTransitionIdentity
        ] = []
        for effective_event in events:
            event = effective_event.event
            if isinstance(event, TailJobEvent):
                job_tail_events.setdefault(event.exec_job_id, []).append(event)
                ordered_tail_events.append(event)
            elif isinstance(event, TailWorkflowEvent):
                workflow_tail_event_identities.append(event.identity)
                ordered_tail_events.append(event)
            else:
                db_event_identities.append(event.identity)
            if isinstance(event, (TailJobEvent, TailWorkflowEvent)):
                if event.identity.source_generation != self.tail_generation:
                    raise ValueError("event tail generation conflicts with snapshot")
                if event.base_db_generation != self.db_generation:
                    raise ValueError("event DB generation conflicts with snapshot")
        if any(
            previous.identity.start_offset >= current.identity.start_offset
            for previous, current in zip(ordered_tail_events, ordered_tail_events[1:])
        ):
            raise ValueError("effective tail events must follow file offset order")
        if len(set(db_event_identities)) != len(db_event_identities):
            raise ValueError("effective DB event identities must be unique")
        if len(set(workflow_tail_event_identities)) != len(
            workflow_tail_event_identities
        ):
            raise ValueError("effective workflow tail event identities must be unique")
        if tuple(workflow_tail_event_identities) != workflow_pending_identities:
            raise ValueError(
                "workflow pending identities must match workflow tail events"
            )
        jobs_by_exec_id = {job.exec_job_id: job for job in jobs}
        if set(job_tail_events) != set(job_pending_identities):
            raise ValueError("job pending identities must match job tail events")
        for exec_job_id, tail_events in job_tail_events.items():
            identities = [event.identity for event in tail_events]
            if len(set(identities)) != len(identities):
                raise ValueError("effective job tail event identities must be unique")
            if tuple(identities) != job_pending_identities[exec_job_id]:
                raise ValueError("job pending identities must match job tail events")
            job = jobs_by_exec_id[exec_job_id]
            current_attempt = job.current_attempt
            if current_attempt is not None and any(
                event.job_submit_seq < current_attempt.job_submit_seq
                for event in tail_events
            ):
                raise ValueError("job tail event cannot predate the current attempt")
            final_event = tail_events[-1]
            if (
                job.state is None
                or normalize_job_state(job.state) != final_event.normalized_state
                or job.state_timestamp != final_event.event_timestamp
            ):
                raise ValueError(
                    "effective job state must match its final pending tail event"
                )
        if workflow_pending_identities:
            workflow_tail_events = [
                effective_event.event
                for effective_event in events
                if isinstance(effective_event.event, TailWorkflowEvent)
            ]
            base_transition = self.workflow.transition
            expected_restart = (
                base_transition.restart_count if base_transition is not None else -1
            )
            restart_open = bool(
                base_transition is not None
                and base_transition.normalized_state == "WORKFLOW_STARTED"
            )
            effective_state = (
                base_transition.normalized_state
                if base_transition is not None
                else None
            )
            effective_status = (
                base_transition.status if base_transition is not None else None
            )
            effective_started_at = (
                base_transition.identity.timestamp
                if restart_open and base_transition is not None
                else self.workflow.started_at
                if base_transition is not None
                else None
            )
            effective_ended_at = (
                base_transition.identity.timestamp
                if base_transition is not None
                and base_transition.normalized_state == "WORKFLOW_TERMINATED"
                else None
            )
            for tail_event in workflow_tail_events:
                if tail_event.normalized_state == "WORKFLOW_STARTED":
                    if not restart_open:
                        expected_restart += 1
                        restart_open = True
                        effective_state = "WORKFLOW_STARTED"
                        effective_status = tail_event.status
                        effective_started_at = tail_event.event_timestamp
                        effective_ended_at = None
                elif restart_open:
                    restart_open = False
                    effective_state = "WORKFLOW_TERMINATED"
                    effective_status = tail_event.status
                    effective_ended_at = tail_event.event_timestamp
            if (
                effective_state is None
                or normalize_workflow_state(self.workflow.state) != effective_state
                or self.workflow.restart_count != expected_restart
            ):
                raise ValueError(
                    "effective workflow state must match its pending tail transitions"
                )
            if (
                self.workflow.status != effective_status
                or self.workflow.started_at != effective_started_at
                or self.workflow.ended_at != effective_ended_at
            ):
                raise ValueError(
                    "effective workflow timing/outcome must match its pending tail transitions"
                )
        object.__setattr__(self, "jobs", jobs)
        object.__setattr__(self, "source_health", health)
        object.__setattr__(self, "events", events)

    @property
    def pending_overlay_count(self) -> int:
        return len(self.workflow.pending_tail) + sum(
            len(job.pending_tail) for job in self.jobs
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        """Serialize epoch time only; monotonic time is process-local."""

        return {
            "epoch": self.epoch.value,
            "workflow": self.workflow.to_json_dict(),
            "jobs": _json_value(self.jobs),
            "db_generation": _json_value(self.db_generation),
            "tail_generation": _json_value(self.tail_generation),
            "published_at_epoch": self.published_at_epoch,
            "source_health": _json_value(self.source_health),
            "events": _json_value(self.events),
            "pending_overlay_count": self.pending_overlay_count,
        }


# Bounded, synchronous provider calls.  Implementations must not start threads,
# tasks, timers, or subprocess loops; WP4 invokes them and owns cadence.
@dataclass(frozen=True, slots=True)
class DBRefreshRequest:
    workflow: WorkflowIdentity
    next_epoch: SnapshotEpoch
    mode: DBRefreshMode
    clock: ClockSample
    prior_generation: DatabaseGeneration | None = None
    pending_job_watermarks: tuple[JobTransitionWatermark, ...] = ()
    pending_job_keys: tuple[JobSemanticKey, ...] = ()
    pending_workflow_watermark: WorkflowTransitionWatermark | None = None
    recent_transition_limit: int = 256
    recent_workflow_transition_limit: int = 64

    def __post_init__(self) -> None:
        if self.recent_transition_limit <= 0:
            raise ValueError("recent_transition_limit must be positive")
        if self.recent_workflow_transition_limit <= 0:
            raise ValueError("recent_workflow_transition_limit must be positive")
        object.__setattr__(
            self, "pending_job_watermarks", tuple(self.pending_job_watermarks)
        )
        object.__setattr__(self, "pending_job_keys", tuple(self.pending_job_keys))
        watermark_instances = [
            watermark.job_instance_id for watermark in self.pending_job_watermarks
        ]
        if len(set(watermark_instances)) != len(watermark_instances):
            raise ValueError("refresh watermarks must be unique per job instance")
        if len(set(self.pending_job_keys)) != len(self.pending_job_keys):
            raise ValueError("pending provisional job keys must be unique")
        if any(key.wf_uuid != self.workflow.wf_uuid for key in self.pending_job_keys):
            raise ValueError("pending job keys must match the requested workflow")
        if (
            self.pending_workflow_watermark is not None
            and self.pending_workflow_watermark.restart.workflow != self.workflow
        ):
            raise ValueError(
                "pending workflow watermark must match the requested workflow"
            )
        has_suffix_cursor = bool(
            self.pending_job_watermarks
            or self.pending_job_keys
            or self.pending_workflow_watermark is not None
        )
        if (
            self.mode
            in {
                DBRefreshMode.FULL_REBOOTSTRAP,
                DBRefreshMode.CURRENT_SNAPSHOT,
            }
            and has_suffix_cursor
        ):
            raise ValueError(
                "full/current snapshot refresh cannot carry suffix cursors"
            )
        if (
            self.mode is DBRefreshMode.CURRENT_SNAPSHOT
            and self.prior_generation is None
        ):
            raise ValueError("current snapshot refresh requires a prior generation")
        if self.mode is DBRefreshMode.BOUNDED_SUFFIX and (
            self.prior_generation is None or not has_suffix_cursor
        ):
            raise ValueError(
                "bounded suffix refresh requires a prior base and pending cursor"
            )


@dataclass(frozen=True, slots=True)
class DBRefreshResult:
    request: DBRefreshRequest
    snapshot: DatabaseSnapshot | None
    health: SourceHealth
    generation: DatabaseGeneration | None

    def __post_init__(self) -> None:
        if self.health.source is not SourceName.STAMPEDE:
            raise ValueError("DB refresh health must describe Stampede")
        if self.health.checked_at_epoch != self.request.clock.epoch:
            raise ValueError("DB refresh health must use the request clock")
        if self.snapshot is not None and self.snapshot.generation != self.generation:
            raise ValueError("result generation must match the database snapshot")
        if (
            self.snapshot is not None
            and self.request.mode is not DBRefreshMode.FULL_REBOOTSTRAP
            and self.generation != self.request.prior_generation
        ):
            raise ValueError("generation changes require a full database rebootstrap")
        if self.snapshot is not None and (
            self.snapshot.epoch != self.request.next_epoch
            or self.snapshot.workflow.workflow != self.request.workflow
            or self.snapshot.snapshot_at_epoch != self.request.clock.epoch
        ):
            raise ValueError("database snapshot must correlate to its refresh request")
        if self.snapshot is not None and (
            len(self.snapshot.recent_transitions) > self.request.recent_transition_limit
            or len(self.snapshot.recent_workflow_transitions)
            > self.request.recent_workflow_transition_limit
        ):
            raise ValueError("database recent transition feed exceeds request limits")


@dataclass(frozen=True, slots=True)
class TailPollRequest:
    workflow: WorkflowIdentity
    base_db_generation: DatabaseGeneration | None
    clock: ClockSample
    max_bytes: int
    max_lines: int

    def __post_init__(self) -> None:
        if self.max_bytes <= 0 or self.max_lines <= 0:
            raise ValueError("tail poll bounds must be positive")


@dataclass(frozen=True, slots=True)
class TailGap:
    generation: TailGeneration | None
    reason: str
    dropped_bytes: int = 0
    dropped_lines: int = 0

    def __post_init__(self) -> None:
        _require_nonnegative("dropped_bytes", self.dropped_bytes)
        _require_nonnegative("dropped_lines", self.dropped_lines)


@dataclass(frozen=True, slots=True)
class TailPollResult:
    request: TailPollRequest
    job_events: tuple[TailJobEvent, ...]
    workflow_events: tuple[TailWorkflowEvent, ...]
    source_events: tuple[TailSourceEvent, ...]
    gaps: tuple[TailGap, ...]
    health: SourceHealth
    generation: TailGeneration | None
    bytes_read: int
    lines_read: int

    def __post_init__(self) -> None:
        if self.health.source is not SourceName.LIVE_TAIL:
            raise ValueError("tail poll health must describe the live tail")
        if self.health.checked_at_epoch != self.request.clock.epoch:
            raise ValueError("tail poll health must use the request clock")
        _require_nonnegative("bytes_read", self.bytes_read)
        _require_nonnegative("lines_read", self.lines_read)
        if self.bytes_read > self.request.max_bytes:
            raise ValueError("tail poll exceeded the requested byte limit")
        if self.lines_read > self.request.max_lines:
            raise ValueError("tail poll exceeded the requested line limit")
        object.__setattr__(self, "job_events", tuple(self.job_events))
        object.__setattr__(self, "workflow_events", tuple(self.workflow_events))
        object.__setattr__(self, "source_events", tuple(self.source_events))
        object.__setattr__(self, "gaps", tuple(self.gaps))
        events = self.job_events + self.workflow_events + self.source_events
        if self.lines_read < len(events):
            raise ValueError("tail lines read cannot be fewer than parsed events")
        if any(event.workflow != self.request.workflow for event in events):
            raise ValueError("tail events must match the poll workflow")
        if any(
            event.base_db_generation != self.request.base_db_generation
            for event in events
        ):
            raise ValueError("tail events must use the requested DB generation")
        if any(
            event.observed_at_monotonic != self.request.clock.monotonic
            for event in events
        ):
            raise ValueError("tail events must use the request monotonic clock")
        if any(event.identity.source_generation != self.generation for event in events):
            raise ValueError("tail events must match the returned tail generation")


@dataclass(frozen=True, slots=True)
class SchedulerQueryRequest:
    workflow: WorkflowIdentity
    kind: SchedulerQueryKind
    clock: ClockSample
    timeout_seconds: float
    result_limit: int

    def __post_init__(self) -> None:
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if self.result_limit <= 0:
            raise ValueError("result_limit must be positive")


@dataclass(frozen=True, slots=True)
class SchedulerEvidence:
    kind: SchedulerQueryKind
    target: FrozenPayload
    payload: FrozenPayload


@dataclass(frozen=True, slots=True)
class SchedulerQueryResult:
    request: SchedulerQueryRequest
    health: SourceHealth
    backoff_seconds: float
    evidence: tuple[SchedulerEvidence, ...] = ()
    summary: FrozenPayload = field(default_factory=FrozenPayload)

    def __post_init__(self) -> None:
        evidence = tuple(self.evidence)
        if self.health.source is not _SCHEDULER_SOURCE_BY_KIND[self.request.kind]:
            raise ValueError("scheduler health source must match the request kind")
        if self.health.checked_at_epoch != self.request.clock.epoch:
            raise ValueError("scheduler health must use the request clock")
        if len(evidence) > self.request.result_limit:
            raise ValueError("scheduler evidence exceeds the request result limit")
        if any(item.kind is not self.request.kind for item in evidence):
            raise ValueError("scheduler evidence kind must match the request")
        if len(set(evidence)) != len(evidence):
            raise ValueError("scheduler evidence must be unique")
        if (
            not math.isfinite(self.backoff_seconds)
            or self.backoff_seconds < 0
            or self.backoff_seconds > MAX_SCHEDULER_BACKOFF_SECONDS
        ):
            raise ValueError("backoff_seconds must be finite and between 0 and 300")
        object.__setattr__(self, "evidence", evidence)


@runtime_checkable
class StampedeSnapshotProvider(Protocol):
    def refresh(self, request: DBRefreshRequest) -> DBRefreshResult:
        """Perform one bounded, read-only refresh and return."""


@runtime_checkable
class LiveTailProvider(Protocol):
    def poll(self, request: TailPollRequest) -> TailPollResult:
        """Perform one bounded nonblocking append poll and return."""


@runtime_checkable
class SchedulerProvider(Protocol):
    def query(self, request: SchedulerQueryRequest) -> SchedulerQueryResult:
        """Perform one time-bounded, read-only scheduler query and return."""


@dataclass(frozen=True, slots=True)
class StreamHeader:
    sequence: int
    stream_id: str
    workflow: WorkflowIdentity
    created_at_epoch: float
    monitor_version: str
    source_metadata: FrozenPayload = field(default_factory=FrozenPayload)

    def __post_init__(self) -> None:
        _validate_stream_record(
            self.sequence, self.stream_id, self.created_at_epoch, header=True
        )

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "header",
            self.sequence,
            stream_id=self.stream_id,
            workflow=self.workflow,
            created_at_epoch=self.created_at_epoch,
            monitor_version=self.monitor_version,
            source_metadata=self.source_metadata,
            contract_status=JSONL_V1_CONTRACT_STATUS,
        )


@dataclass(frozen=True, slots=True)
class CheckpointRecord:
    sequence: int
    stream_id: str
    recorded_at_epoch: float
    snapshot: DatabaseSnapshot
    reason: str

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)
        if not isinstance(self.snapshot, DatabaseSnapshot):
            raise TypeError("canonical checkpoints require a DatabaseSnapshot")

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "checkpoint",
            self.sequence,
            stream_id=self.stream_id,
            recorded_at_epoch=self.recorded_at_epoch,
            snapshot=self.snapshot,
            reason=self.reason,
        )


@dataclass(frozen=True, slots=True)
class JobTransitionRecord:
    sequence: int
    stream_id: str
    snapshot_epoch: SnapshotEpoch
    recorded_at_epoch: float
    transition: DBJobTransition

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "job_transition",
            self.sequence,
            stream_id=self.stream_id,
            snapshot_epoch=self.snapshot_epoch.value,
            recorded_at_epoch=self.recorded_at_epoch,
            confirmed=True,
            transition=self.transition,
        )


@dataclass(frozen=True, slots=True)
class WorkflowTransitionRecord:
    sequence: int
    stream_id: str
    snapshot_epoch: SnapshotEpoch
    recorded_at_epoch: float
    transition: DBWorkflowTransition

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "workflow_transition",
            self.sequence,
            stream_id=self.stream_id,
            snapshot_epoch=self.snapshot_epoch.value,
            recorded_at_epoch=self.recorded_at_epoch,
            confirmed=True,
            transition=self.transition,
        )


@dataclass(frozen=True, slots=True)
class EnrichmentRecord:
    sequence: int
    stream_id: str
    snapshot_epoch: SnapshotEpoch
    recorded_at_epoch: float
    source: SourceName
    target: FrozenPayload
    payload: FrozenPayload
    expires_at_epoch: float | None

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)
        if self.expires_at_epoch is not None and not math.isfinite(
            self.expires_at_epoch
        ):
            raise ValueError("expires_at_epoch must be finite")

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "enrichment",
            self.sequence,
            stream_id=self.stream_id,
            snapshot_epoch=self.snapshot_epoch.value,
            recorded_at_epoch=self.recorded_at_epoch,
            source=self.source,
            target=self.target,
            payload=self.payload,
            expires_at_epoch=self.expires_at_epoch,
        )


@dataclass(frozen=True, slots=True)
class DiagnosticEvidence:
    source: SourceName
    code: str
    payload: FrozenPayload = field(default_factory=FrozenPayload)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "source": self.source.value,
            "code": self.code,
            "payload": self.payload.to_json_dict(),
        }


@dataclass(frozen=True, slots=True)
class DiagnosticRecord:
    sequence: int
    stream_id: str
    snapshot_epoch: SnapshotEpoch
    recorded_at_epoch: float
    target: FrozenPayload
    code: str
    severity: DiagnosticSeverity
    summary: str
    evidence: tuple[DiagnosticEvidence, ...]

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)
        _bounded_text(self.summary, MAX_DIAGNOSTIC_SUMMARY_CHARS, "diagnostic summary")
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "diagnostic_result",
            self.sequence,
            stream_id=self.stream_id,
            snapshot_epoch=self.snapshot_epoch.value,
            recorded_at_epoch=self.recorded_at_epoch,
            target=self.target,
            code=self.code,
            severity=self.severity,
            summary=self.summary,
            evidence=self.evidence,
            changes_state=False,
        )


@dataclass(frozen=True, slots=True)
class GapRecord:
    sequence: int
    stream_id: str
    recorded_at_epoch: float
    reason: GapReason
    first_missing_sequence: int
    last_missing_sequence: int | None = None

    def __post_init__(self) -> None:
        _validate_stream_record(self.sequence, self.stream_id, self.recorded_at_epoch)
        if self.first_missing_sequence <= 0:
            raise ValueError("first_missing_sequence must be positive")
        if (
            self.last_missing_sequence is not None
            and self.last_missing_sequence < self.first_missing_sequence
        ):
            raise ValueError("last_missing_sequence precedes first_missing_sequence")

    def to_json_dict(self) -> dict[str, JSONValue]:
        return _record_json(
            "gap",
            self.sequence,
            stream_id=self.stream_id,
            recorded_at_epoch=self.recorded_at_epoch,
            reason=self.reason,
            first_missing_sequence=self.first_missing_sequence,
            last_missing_sequence=self.last_missing_sequence,
            next_checkpoint_required=True,
        )
