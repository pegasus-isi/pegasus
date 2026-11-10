"""Structured JSON output for the one-shot ``pegasus-monitor`` modes."""

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
import math
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TextIO

from Pegasus.monitor.event_log import _redact_payload, _redact_text
from Pegasus.monitor.models import (
    HealthState,
    Lifecycle,
    SourceName,
    _json_value,
)
from Pegasus.monitor.scheduler_evidence import (
    observed_job_counts,
    workflow_removal_reason,
)
from Pegasus.monitor.stall_detector import assess_stall_once
from Pegasus.monitor.terminal import sanitize_display

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

JSON_OUTPUT_FORMAT = "pegasus-monitor-json"
JSON_OUTPUT_VERSION = 1
"""Version of this document, independent of the JSONL log's ``schema_version``.

Adding a key keeps the version. Removing or renaming a key, or changing the
meaning or type of an existing one, increments it.
"""

MAX_LISTED_SUB_WORKFLOWS = 100
MAX_IDLE_JOBS = 1000
MAX_RESOURCE_MISMATCHES = 1000
MAX_MESSAGE_CHARS = 2048
_SUB_WORKFLOW_TYPES = frozenset({"dax", "dag"})

# Every source the monitor can consult, in the order consumers see them.
_SOURCES = (
    SourceName.STAMPEDE,
    SourceName.LIVE_TAIL,
    SourceName.CONDOR_QUEUE,
    SourceName.CONDOR_HISTORY,
    SourceName.CONDOR_POOL,
    SourceName.CONDOR_PRIORITY,
    SourceName.CONDOR_NEGOTIATOR,
)
_CONDOR_SOURCES = frozenset(_SOURCES[2:])
NOT_QUERIED = "not_queried"

ERROR_USAGE = "usage_error"
ERROR_NO_SNAPSHOT = "no_authoritative_snapshot"
ERROR_LOCATION = "workflow_location_error"
ERROR_NO_CHECKPOINT = "no_complete_checkpoint"
ERROR_EVENT_LOG = "invalid_event_log"
ERROR_IO = "io_error"
ERROR_INTERRUPTED = "interrupted"
ERROR_CLEANUP = "cleanup_failed"
ERROR_MONITOR = "monitor_error"


class NoCompleteCheckpoint(RuntimeError):
    """A replayed or remote stream ended before a usable checkpoint."""


def error_code(error: BaseException) -> str:
    """Map a monitor failure to its stable ``error.code``."""

    from Pegasus.monitor.event_log import EventLogFormatError
    from Pegasus.monitor.locator import WorkflowLocationError

    if isinstance(error, WorkflowLocationError):
        return ERROR_LOCATION
    if isinstance(error, NoCompleteCheckpoint):
        return ERROR_NO_CHECKPOINT
    if isinstance(error, EventLogFormatError):
        return ERROR_EVENT_LOG
    if isinstance(error, OSError):
        return ERROR_IO
    return ERROR_MONITOR


def requested_modes(args: object) -> tuple[str, ...]:
    """Name the one-shot modes the caller asked for, in a fixed order."""

    return tuple(
        name
        for name, selected in (
            ("once", getattr(args, "once", False)),
            ("diagnose", getattr(args, "diagnose", False)),
            ("why_idle", getattr(args, "why_idle", False)),
        )
        if selected
    )


def _iso(epoch: float) -> str:
    return (
        datetime.fromtimestamp(epoch, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _finite(value: Any) -> Any:
    """Replace non-finite floats, which JSON cannot represent, with null."""

    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    return value


def _text(value: object, maximum: int = MAX_MESSAGE_CHARS) -> str:
    return _redact_text(str(value), maximum)


def envelope(
    *,
    monitor_version: str,
    modes: Sequence[str],
    origin: str,
    target: str | None,
) -> dict[str, Any]:
    """Return a complete document with every section empty.

    Success and failure documents share this key set, so a consumer can read
    any key without first checking which kind it received.
    """

    return {
        "format": JSON_OUTPUT_FORMAT,
        "format_version": JSON_OUTPUT_VERSION,
        "monitor_version": monitor_version,
        "generated_at_epoch": None,
        "generated_at": None,
        "observed_at_epoch": None,
        "modes": list(modes),
        "origin": origin,
        "target": target,
        "exit_code": None,
        "error": None,
        "workflow": None,
        "sources": [],
        "summary": None,
        "snapshot": None,
        "stats": None,
        "diagnostics": None,
        "stall": None,
        "why_idle": None,
        "stream": None,
        "analysis_errors": [],
    }


def _workflow(context: object, effective: object | None) -> dict[str, Any]:
    sub_workflows = None
    if effective is not None:
        names = [
            job.exec_job_id
            for job in effective.jobs
            if job.type_desc.lower() in _SUB_WORKFLOW_TYPES
        ]
        sub_workflows = {
            "count": len(names),
            "aggregated": False,
            "exec_job_ids": names[:MAX_LISTED_SUB_WORKFLOWS],
            "exec_job_ids_omitted": max(0, len(names) - MAX_LISTED_SUB_WORKFLOWS),
        }
    return {
        "wf_uuid": context.wf_uuid,
        "root_wf_uuid": context.root_wf_uuid,
        "is_sub_workflow": context.wf_uuid != context.root_wf_uuid,
        "submit_dir": str(context.submit_dir),
        "label": context.label,
        "owner": context.owner,
        "dag_name": context.dag_name,
        "planner_version": context.planner_version,
        "sub_workflows": sub_workflows,
    }


def _sources(snapshot: object, *, condor_disabled: bool) -> list[dict[str, Any]]:
    """Report every source, including the ones this run never consulted."""

    observed = {health.source: health for health in snapshot.source_health}
    for result in snapshot.scheduler_results:
        observed[result.health.source] = result.health
    sources = []
    for source in _SOURCES:
        health = observed.get(source)
        if health is not None:
            entry = health.to_json_dict()
            if entry.get("detail") is not None:
                entry["detail"] = _text(entry["detail"], 512)
        else:
            disabled = condor_disabled and source in _CONDOR_SOURCES
            entry = {
                "source": source.value,
                "state": HealthState.DISABLED.value if disabled else NOT_QUERIED,
                "checked_at_epoch": None,
                "last_success_epoch": None,
                "last_good_age": None,
                "stale_after_seconds": None,
                "consecutive_failures": 0,
                "pending_count": 0,
                "error_code": "disabled" if disabled else None,
                "detail": None,
            }
        entry["queried"] = entry["state"] not in {
            HealthState.DISABLED.value,
            NOT_QUERIED,
        }
        sources.append(entry)
    return sources


def _summary(snapshot: object, removal_reason: str | None) -> dict[str, Any] | None:
    effective = snapshot.effective
    if effective is None:
        return None
    workflow = effective.workflow
    counts = dict.fromkeys((item.value for item in Lifecycle), 0)
    compute = 0
    for job in effective.jobs:
        counts[job.lifecycle.value] += 1
        compute += job.is_compute
    removal = removal_reason or workflow_removal_reason(snapshot)
    return {
        "workflow_state": workflow.state,
        "workflow_status": workflow.status,
        "restart_count": workflow.restart_count,
        "started_at": _json_value(workflow.started_at),
        "ended_at": _json_value(workflow.ended_at),
        "provenance": workflow.provenance.value,
        "authoritative_complete": bool(snapshot.authoritative_complete),
        "removal_reason": None if removal is None else _text(removal),
        "job_counts": {
            "total": len(effective.jobs),
            "compute": compute,
            **counts,
        },
        # job_counts is what Stampede recorded; the queue may already know
        # better, and Stampede never records a removed job's end.
        "job_counts_source": SourceName.STAMPEDE.value,
        "observed_job_counts": {
            "total": len(effective.jobs),
            **observed_job_counts(snapshot),
        },
        "pending_tail_events": snapshot.pending_tail_events,
    }


def _snapshot(snapshot: object, event_limit: int) -> dict[str, Any] | None:
    """Serialize the authoritative base through the models' own codecs."""

    effective = snapshot.effective
    if effective is None:
        return None
    events = effective.events[-event_limit:] if event_limit > 0 else ()
    return {
        "epoch": effective.epoch.value,
        "published_at_epoch": effective.published_at_epoch,
        "db_generation": _json_value(effective.db_generation),
        "tail_generation": _json_value(effective.tail_generation),
        "pending_overlay_count": effective.pending_overlay_count,
        "workflow": effective.workflow.to_json_dict(),
        "jobs": [
            {
                **job.to_json_dict(),
                "lifecycle": job.lifecycle.value,
                "is_compute": job.is_compute,
            }
            for job in effective.jobs
        ],
        "events": [event.to_json_dict() for event in events],
        "events_omitted": len(effective.events) - len(events),
    }


def _evidence(items: Sequence[object]) -> list[dict[str, Any]]:
    return [
        {
            "source": item.source.value,
            "code": str(item.code),
            "payload": _redact_payload(item.payload.to_json_dict()),
        }
        for item in items
    ]


def _finding(finding: object, workflow: Mapping[str, Any]) -> dict[str, Any]:
    data = finding.to_json_dict()
    return {
        "target": {
            "workflow": dict(workflow),
            "exec_job_id": data["exec_job_id"],
            "job_provenance": data["job_provenance"],
        },
        "category": data["category"],
        "severity": data["severity"],
        "code": data["code"],
        "summary": _text(data["summary"]),
        "suggestions": [_text(item) for item in data["suggestions"]],
        "evidence": _evidence(finding.evidence),
    }


def _recorded_finding(record: object) -> dict[str, Any]:
    """Adapt a JSONL diagnostic record, which never stored suggestions."""

    return {
        "target": _redact_payload(record.target.to_json_dict()),
        "category": None,
        "severity": record.severity.value,
        "code": record.code,
        "summary": _text(record.summary),
        "suggestions": None,
        "evidence": _evidence(record.evidence),
    }


def _why_idle(analysis: object) -> dict[str, Any]:
    data = analysis.to_json_dict()
    idle = data["idle_jobs"]
    mismatches = data["resource_fit"]["mismatches"]
    data["idle_jobs"] = idle[:MAX_IDLE_JOBS]
    data["idle_jobs_omitted"] = max(0, len(idle) - MAX_IDLE_JOBS)
    data["resource_fit"] = {
        "mismatches": mismatches[:MAX_RESOURCE_MISMATCHES],
        "mismatches_omitted": max(0, len(mismatches) - MAX_RESOURCE_MISMATCHES),
    }
    data["findings"] = [_text(item) for item in data["findings"]]
    return {"available": True, "reason": None, **data}


def _unavailable(reason: str) -> dict[str, Any]:
    return {"available": False, "reason": reason}


def build_document(
    base: Mapping[str, Any],
    *,
    context: object,
    snapshot: object,
    stats: object | None = None,
    diagnostics: object | None = None,
    recorded_diagnostics: Sequence[object] | None = None,
    why_idle: object | None = None,
    analysis_errors: Sequence[str] = (),
    removal_reason: str | None = None,
    condor_disabled: bool = False,
    event_limit: int = 15,
    stream: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fill the envelope from one already-acquired publication and analysis.

    Nothing here reads a file or queries a source: every value comes from the
    snapshot and analysis the text renderer would have received.
    """

    document = dict(base)
    modes = set(base["modes"])
    replayed = base["origin"] != "live"
    effective = snapshot.effective
    failed = {error.split(":", 1)[0] for error in analysis_errors}
    document["observed_at_epoch"] = snapshot.clock.epoch
    document["workflow"] = _workflow(context, effective)
    document["sources"] = _sources(snapshot, condor_disabled=condor_disabled)
    document["summary"] = _summary(snapshot, removal_reason)
    document["stall"] = assess_stall_once(snapshot).to_json_dict()
    document["stream"] = None if stream is None else dict(stream)
    document["analysis_errors"] = [_text(error, 256) for error in analysis_errors]
    if stats is not None:
        document["stats"] = _finite(stats.to_dict())
    if "once" in modes:
        document["snapshot"] = _snapshot(snapshot, event_limit)
    if "diagnose" in modes:
        workflow = {
            "wf_uuid": context.wf_uuid,
            "root_wf_uuid": context.root_wf_uuid,
        }
        if replayed:
            document["diagnostics"] = {
                "available": True,
                "reason": None,
                "origin": "event_log",
                "findings": [
                    _recorded_finding(record) for record in recorded_diagnostics or ()
                ],
                "errors": [],
            }
        elif diagnostics is None:
            document["diagnostics"] = _unavailable(
                "analysis_failed"
                if "diagnostics" in failed
                else "authoritative_snapshot_unavailable"
            )
        else:
            document["diagnostics"] = {
                "available": True,
                "reason": None,
                "origin": "analysis",
                "findings": [
                    _finding(finding, workflow) for finding in diagnostics.findings
                ],
                "errors": [_text(error, 256) for error in diagnostics.errors],
            }
    if "why_idle" in modes:
        if replayed:
            document["why_idle"] = _unavailable("scheduler_evidence_not_recorded")
        elif why_idle is None:
            document["why_idle"] = _unavailable(
                "analysis_failed"
                if "why_idle" in failed
                else "authoritative_snapshot_unavailable"
            )
        else:
            document["why_idle"] = _why_idle(why_idle)
    return document


class JsonEmitter:
    """Write exactly one JSON document, whatever path the command takes.

    The command stores a document or a failure as it runs; nothing reaches
    stdout until ``emit``, so cleanup failures can still be reported in the
    single document. ``error`` is non-null exactly when the exit code is
    non-zero.
    """

    def __init__(self, stream: TextIO, base: Mapping[str, Any]) -> None:
        self._stream = stream
        self.base = dict(base)
        self.document: dict[str, Any] | None = None
        self._error: dict[str, str] | None = None
        self._exit_code: int | None = None

    def fail(self, code: str, message: object) -> None:
        """Record a failure; the first one names the cause."""

        if self._error is None:
            # Strip terminal controls but keep paths: the caller supplied them.
            text = sanitize_display(str(message))[:MAX_MESSAGE_CHARS]
            self._error = {"code": code, "message": text}

    def emit(self, exit_code: int, *, now_epoch: float | None = None) -> int:
        """Write the document once and return the process exit status."""

        if self._exit_code is not None:
            return self._exit_code
        if self._error is None and self.document is None:
            self.fail(ERROR_INTERRUPTED, "monitoring stopped before any result")
        if self._error is None and exit_code != 0:
            self.fail(ERROR_MONITOR, "pegasus-monitor failed")
        if self._error is not None and exit_code == 0:
            exit_code = 1
        document = dict(self.document if self.document is not None else self.base)
        now = time.time() if now_epoch is None else now_epoch
        document["generated_at_epoch"] = now
        document["generated_at"] = _iso(now)
        document["exit_code"] = exit_code
        document["error"] = self._error
        self._exit_code = exit_code
        self._stream.write(
            json.dumps(
                document, ensure_ascii=True, allow_nan=False, separators=(",", ":")
            )
        )
        self._stream.write("\n")
        self._stream.flush()
        return exit_code
