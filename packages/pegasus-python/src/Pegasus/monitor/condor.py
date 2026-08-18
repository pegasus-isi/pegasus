"""Bounded, observational HTCondor subprocess queries.

The provider in this module implements the source-neutral scheduler contracts in
``Pegasus.monitor.models``.  It deliberately uses only the reviewed read-side
HTCondor command allowlist.  It never imports the HTCondor bindings (whose
configuration is process-global), creates a polling loop, or decides cadence;
the monitor coordinator supplies a clock sample and invokes one bounded query.
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

import json
import math
import os
import random
import re
import selectors
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

from Pegasus.monitor.models import (
    BoundedAge,
    FrozenPayload,
    HealthState,
    SchedulerEvidence,
    SchedulerProvider,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SourceHealth,
    SourceName,
    WorkflowIdentity,
)

MAX_STDOUT_BYTES = 4 * 1024 * 1024
MAX_STDERR_BYTES = 64 * 1024
MAX_CACHE_ENTRIES = 4096
MAX_PAYLOAD_STRING_CHARS = 4096
MAX_HEALTH_DETAIL_CHARS = 512
MAX_BACKOFF_SECONDS = 300.0

_READ_COMMANDS = frozenset(
    {
        "condor_q",
        "condor_history",
        "condor_status",
        "condor_userprio",
    }
)

_QUEUE_ATTRIBUTES = (
    "ClusterId",
    "ProcId",
    "JobStatus",
    "Cmd",
    "RemoteHost",
    "QDate",
    "JobStartDate",
    "DAGNodeName",
    "Owner",
    "HoldReason",
    "HoldReasonCode",
    "RequestCpus",
    "RequestMemory",
    "RequestDisk",
    "RequestGpus",
    "Requirements",
    "ImageSize",
    "NumJobStarts",
    "AccountingGroup",
    "TransferInputSizeMB",
    "BytesSent",
    "BytesRecvd",
    "pegasus_wf_uuid",
    "pegasus_root_wf_uuid",
)

_HISTORY_ATTRIBUTES = (
    "ClusterId",
    "ProcId",
    "DAGNodeName",
    "Owner",
    "Cmd",
    "JobStatus",
    "ExitCode",
    "QDate",
    "JobStartDate",
    "RemoteWallClockTime",
    "RemoteUserCpu",
    "RemoteSysCpu",
    "CumulativeRemoteUserCpu",
    "RequestCpus",
    "RequestMemory",
    "RequestDisk",
    "RequestGpus",
    "ImageSize",
    "DiskUsage",
    "LastRemoteHost",
    "BytesSent",
    "BytesRecvd",
    "NumJobStarts",
    "pegasus_wf_uuid",
    "pegasus_root_wf_uuid",
)

_POOL_ATTRIBUTES = (
    "Name",
    "Machine",
    "SlotType",
    "DynamicSlot",
    "PartitionableSlot",
    "ParentSlotName",
    "Cpus",
    "Memory",
    "Disk",
    "TotalSlotCpus",
    "TotalSlotMemory",
    "TotalSlotDisk",
    "TotalSlotGPUs",
    "TotalLoadAvg",
    "Activity",
    "State",
    "OpSys",
    "Arch",
    "GPUs",
)

_NEGOTIATOR_ATTRIBUTES = (
    "Name",
    "LastNegotiationCycleDuration0",
    "LastNegotiationCycleMatches0",
    "LastNegotiationCycleDuration1",
    "LastNegotiationCycleMatches1",
)

_ATTRIBUTES_BY_KIND = {
    SchedulerQueryKind.QUEUE: _QUEUE_ATTRIBUTES,
    SchedulerQueryKind.HISTORY: _HISTORY_ATTRIBUTES,
    SchedulerQueryKind.POOL: _POOL_ATTRIBUTES,
    SchedulerQueryKind.NEGOTIATOR: _NEGOTIATOR_ATTRIBUTES,
}

_SOURCE_BY_KIND = {
    SchedulerQueryKind.QUEUE: SourceName.CONDOR_QUEUE,
    SchedulerQueryKind.HISTORY: SourceName.CONDOR_HISTORY,
    SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
    SchedulerQueryKind.PRIORITY: SourceName.CONDOR_PRIORITY,
    SchedulerQueryKind.NEGOTIATOR: SourceName.CONDOR_NEGOTIATOR,
}

_BASE_BACKOFF_SECONDS = {
    SchedulerQueryKind.QUEUE: 5.0,
    SchedulerQueryKind.HISTORY: 30.0,
    SchedulerQueryKind.POOL: 30.0,
    SchedulerQueryKind.PRIORITY: 30.0,
    SchedulerQueryKind.NEGOTIATOR: 30.0,
}

_STALE_AFTER_SECONDS = {
    SchedulerQueryKind.QUEUE: 10.0,
    SchedulerQueryKind.HISTORY: 60.0,
    SchedulerQueryKind.POOL: 60.0,
    SchedulerQueryKind.PRIORITY: 300.0,
    SchedulerQueryKind.NEGOTIATOR: 300.0,
}

_JOB_STATUS_NAMES = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "transfer_output",
    7: "suspended",
}

_SAFE_ENDPOINT = re.compile(r"^[A-Za-z0-9_.:@%+\-\[\]]{1,512}$")
_CLASSAD_ATTRIBUTE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_INHERITED_ENVIRONMENT = frozenset(
    {
        "PATH",
        "HOME",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "KRB5CCNAME",
        "BEARER_TOKEN_FILE",
    }
)
_INHERITED_ENVIRONMENT_PREFIXES = ("CONDOR_", "_CONDOR_", "X509_")


class CondorObserverError(RuntimeError):
    """Internal classified failure from one read-only command."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class CondorObserverConfig:
    """Per-provider HTCondor endpoint and child-process configuration."""

    schedd: str | None = None
    collector: str | None = None
    token: str | None = None
    cert: str | None = None
    key: str | None = None
    password_file: str | None = None
    condor_config: str | None = None
    environment: Mapping[str, str] = field(default_factory=dict, compare=False)
    tree_scope: bool = False

    def __post_init__(self) -> None:
        for name in ("schedd", "collector"):
            value = getattr(self, name)
            if value is not None and not _SAFE_ENDPOINT.fullmatch(value):
                raise ValueError(f"unsafe HTCondor {name} value")
        for key, value in self.environment.items():
            if not key or "=" in key or "\x00" in key or "\x00" in value:
                raise ValueError("invalid child environment override")
        object.__setattr__(
            self, "environment", MappingProxyType(dict(self.environment))
        )


@dataclass(frozen=True, slots=True)
class _CommandOutcome:
    stdout: bytes
    stderr: bytes
    returncode: int


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    evidence: tuple[SchedulerEvidence, ...]
    summary: FrozenPayload
    last_success_epoch: float


_QueryStateKey = tuple[SchedulerQueryKind, str, str]


class _ActiveProcessRegistry:
    """Coordinate explicit cancellation without installing signal handlers."""

    def __init__(self) -> None:
        # RLock avoids deadlock if a Python signal interrupts a tiny registry
        # critical section and the coordinator's handler requests cancellation.
        self._lock = threading.RLock()
        self._launching = False
        self._cancel_pending = False
        self._process: subprocess.Popen[bytes] | None = None
        self._cancelled_processes: set[int] = set()

    def begin_launch(self) -> None:
        with self._lock:
            if self._launching or self._process is not None:
                raise RuntimeError("an HTCondor process is already registered")
            self._launching = True
            self._cancel_pending = False

    def abort_launch(self) -> None:
        with self._lock:
            self._launching = False
            self._cancel_pending = False

    def register(self, process: subprocess.Popen[bytes]) -> bool:
        with self._lock:
            self._launching = False
            self._process = process
            cancel_now = self._cancel_pending
            self._cancel_pending = False
            if cancel_now:
                self._cancelled_processes.add(id(process))
            return cancel_now

    def unregister(self, process: subprocess.Popen[bytes]) -> None:
        with self._lock:
            if self._process is process:
                self._process = None
            self._cancelled_processes.discard(id(process))

    def was_cancelled(self, process: subprocess.Popen[bytes]) -> bool:
        with self._lock:
            return id(process) in self._cancelled_processes

    def cancel(self) -> bool:
        with self._lock:
            process = self._process
            if process is None:
                if self._launching:
                    self._cancel_pending = True
                    return True
                return False
            self._cancelled_processes.add(id(process))
        _terminate_process_group(process)
        return True


def _classad_string(value: str) -> str:
    """Encode a value as one safe ClassAd string literal."""

    if not value or len(value) > 512 or "\x00" in value:
        raise ValueError("workflow UUID is not safe for an HTCondor constraint")
    return json.dumps(value, ensure_ascii=True)


def build_workflow_constraint(
    workflow: WorkflowIdentity, *, tree_scope: bool = False
) -> str:
    """Return the exact reviewed Pegasus ClassAd constraint.

    Version 1 callers use the selected ``pegasus_wf_uuid``.  Root UUID scope is
    available only through the explicit ``tree_scope`` setting for the future
    workflow-tree coordinator; it is never inferred from the two UUID values.
    """

    if tree_scope:
        return f"pegasus_root_wf_uuid =?= {_classad_string(workflow.root_wf_uuid)}"
    return f"pegasus_wf_uuid =?= {_classad_string(workflow.wf_uuid)}"


def _build_command(
    request: SchedulerQueryRequest, config: CondorObserverConfig
) -> tuple[str, ...]:
    """Construct only one of the five allowlisted read operations."""

    constraint = build_workflow_constraint(
        request.workflow, tree_scope=config.tree_scope
    )
    if request.kind is SchedulerQueryKind.QUEUE:
        command = [
            "condor_q",
            "-json",
            "-attributes",
            ",".join(_QUEUE_ATTRIBUTES),
            "-constraint",
            constraint,
            "-limit",
            str(request.result_limit),
        ]
        if config.schedd:
            command.extend(("-name", config.schedd))
        if config.collector:
            command.extend(("-pool", config.collector))
    elif request.kind is SchedulerQueryKind.HISTORY:
        command = [
            "condor_history",
            "-json",
            "-attributes",
            ",".join(_HISTORY_ATTRIBUTES),
            "-constraint",
            constraint,
            "-match",
            str(request.result_limit),
        ]
        if config.schedd:
            command.extend(("-name", config.schedd))
        if config.collector:
            command.extend(("-pool", config.collector))
    elif request.kind is SchedulerQueryKind.POOL:
        command = [
            "condor_status",
            "-json",
            "-attributes",
            ",".join(_POOL_ATTRIBUTES),
        ]
        if config.collector:
            command.extend(("-pool", config.collector))
    elif request.kind is SchedulerQueryKind.PRIORITY:
        command = ["condor_userprio", "-long"]
        if config.collector:
            command.extend(("-pool", config.collector))
    elif request.kind is SchedulerQueryKind.NEGOTIATOR:
        command = [
            "condor_status",
            "-negotiator",
            "-json",
            "-attributes",
            ",".join(_NEGOTIATOR_ATTRIBUTES),
        ]
        if config.collector:
            command.extend(("-pool", config.collector))
    else:  # pragma: no cover - SchedulerQueryKind is exhaustive
        raise ValueError(f"unsupported scheduler query kind: {request.kind}")

    if command[0] not in _READ_COMMANDS:
        raise AssertionError("HTCondor command escaped the read-only allowlist")
    return tuple(command)


def _build_environment(config: CondorObserverConfig) -> dict[str, str]:
    """Build a minimal child environment without mutating process globals."""

    environment = {
        key: value
        for key, value in os.environ.items()
        if key in _INHERITED_ENVIRONMENT
        or key.startswith(_INHERITED_ENVIRONMENT_PREFIXES)
    }
    environment.update(config.environment)
    credentials = {
        "_CONDOR_SEC_TOKEN_DIRECTORY": config.token,
        "X509_USER_CERT": config.cert,
        "X509_USER_KEY": config.key,
        "_CONDOR_PASSWORD_FILE": config.password_file,
        "CONDOR_CONFIG": config.condor_config,
    }
    environment.update(
        {name: value for name, value in credentials.items() if value is not None}
    )
    return environment


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    """Terminate the complete isolated group, escalating immediately to KILL."""

    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        except PermissionError:
            try:
                process.send_signal(sig)
            except ProcessLookupError:
                break
        except OSError:
            try:
                process.send_signal(sig)
            except OSError:
                break
        if sig is signal.SIGTERM:
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
            # Give group children which handle SIGTERM a small cleanup window
            # even when the group leader exits immediately.
            time.sleep(0.05)
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass


def _bounded_subprocess(
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
    timeout_seconds: float,
    stdout_limit: int,
    stderr_limit: int,
    registry: _ActiveProcessRegistry,
    monotonic: Callable[[], float] = time.monotonic,
) -> _CommandOutcome:
    """Run one child with bounded pipes and a hard process-group deadline."""

    registry.begin_launch()
    process: subprocess.Popen[bytes] | None = None
    registered = False
    selector: selectors.BaseSelector | None = None
    streams: dict[int, tuple[str, Any, int]] = {}
    try:
        process = subprocess.Popen(
            tuple(command),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            env=dict(environment),
            start_new_session=True,
            close_fds=True,
            bufsize=0,
        )
        cancel_now = registry.register(process)
        registered = True
        if cancel_now:
            raise CondorObserverError(
                "cancelled", f"{command[0]} was cancelled during launch"
            )

        assert process.stdout is not None
        assert process.stderr is not None
        streams = {
            process.stdout.fileno(): ("stdout", process.stdout, stdout_limit),
            process.stderr.fileno(): ("stderr", process.stderr, stderr_limit),
        }
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        selector = selectors.DefaultSelector()
        for fd, (name, stream, _limit) in streams.items():
            os.set_blocking(fd, False)
            selector.register(stream, selectors.EVENT_READ, name)

        deadline = monotonic() + timeout_seconds
        while selector.get_map():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise CondorObserverError(
                    "timeout", f"{command[0]} exceeded its hard deadline"
                )
            ready = selector.select(remaining)
            if not ready:
                raise CondorObserverError(
                    "timeout", f"{command[0]} exceeded its hard deadline"
                )
            for key, _events in ready:
                name = key.data
                stream = key.fileobj
                limit = stdout_limit if name == "stdout" else stderr_limit
                try:
                    chunk = os.read(stream.fileno(), min(65536, limit + 1))
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(stream)
                    stream.close()
                    continue
                buffers[name].extend(chunk)
                if len(buffers[name]) > limit:
                    raise CondorObserverError(
                        "output_limit", f"{command[0]} exceeded the {name} limit"
                    )

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise CondorObserverError(
                "timeout", f"{command[0]} exceeded its hard deadline"
            )
        try:
            returncode = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as error:
            raise CondorObserverError(
                "timeout", f"{command[0]} exceeded its hard deadline"
            ) from error
        if registry.was_cancelled(process):
            raise CondorObserverError("cancelled", f"{command[0]} was cancelled")
        return _CommandOutcome(
            bytes(buffers["stdout"]), bytes(buffers["stderr"]), returncode
        )
    except FileNotFoundError as error:
        if process is not None:
            _terminate_process_group(process)
            raise CondorObserverError(
                "io_error", f"failed while reading bounded output from {command[0]}"
            ) from error
        raise CondorObserverError(
            "missing_command", f"{command[0]} is not installed or not on PATH"
        ) from error
    except CondorObserverError:
        if process is not None:
            _terminate_process_group(process)
        raise
    except (KeyboardInterrupt, SystemExit):
        if process is not None:
            _terminate_process_group(process)
        raise
    except (OSError, ValueError, TypeError, RuntimeError, KeyError) as error:
        if process is not None:
            _terminate_process_group(process)
        if isinstance(error, OSError) and process is None:
            raise CondorObserverError(
                "launch_failed",
                f"unable to start {command[0]}: {error.strerror or error}",
            ) from error
        raise CondorObserverError(
            "io_error", f"failed while reading bounded output from {command[0]}"
        ) from error
    # Cleanup is mandatory even for an unexpected BaseException; only the
    # recognized operational exceptions above are translated into source health.
    except BaseException:
        if process is not None:
            _terminate_process_group(process)
        raise
    finally:
        if registered and process is not None:
            registry.unregister(process)
        else:
            registry.abort_launch()
        if selector is not None:
            selector.close()
        for _name, stream, _limit in streams.values():
            if not stream.closed:
                stream.close()


def _decode_output(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def _classify_nonzero(returncode: int, stderr: str, executable: str) -> None:
    detail = stderr
    lowered = " ".join(detail.split()).lower()
    if any(
        marker in lowered
        for marker in ("not authorized", "authorization", "permission denied")
    ):
        code = "authorization"
    elif any(
        marker in lowered
        for marker in ("authentication", "authenticate", "credential", "token")
    ):
        code = "authentication"
    elif any(
        marker in lowered
        for marker in (
            "failed to connect",
            "connection refused",
            "communication error",
            "can't find address",
            "cannot find address",
            "daemon",
            "schedd",
            "collector",
        )
    ):
        code = "daemon_unreachable"
    else:
        code = "command_failed"
    suffix = f": {detail}" if detail.strip() else ""
    raise CondorObserverError(
        code, f"{executable} exited with status {returncode}{suffix}"
    )


def _strict_json_list(text: str, executable: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    try:
        value = json.loads(
            text,
            parse_constant=lambda constant: (_ for _ in ()).throw(
                ValueError(f"invalid JSON constant {constant}")
            ),
        )
    except (json.JSONDecodeError, ValueError) as error:
        raise CondorObserverError(
            "parse_error", f"{executable} returned malformed JSON"
        ) from error
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise CondorObserverError(
            "parse_error", f"{executable} returned an unexpected JSON shape"
        )
    return value


def _parse_long_classads(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    records: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            if current:
                records.append(current)
                current = {}
            continue
        if " = " not in stripped:
            raise CondorObserverError(
                "parse_error", "condor_userprio returned a non-assignment line"
            )
        key, raw_value = stripped.split(" = ", 1)
        key = key.strip()
        if not _CLASSAD_ATTRIBUTE_NAME.fullmatch(key):
            raise CondorObserverError(
                "parse_error", "condor_userprio returned an invalid attribute name"
            )
        if key in current:
            raise CondorObserverError(
                "parse_error", "condor_userprio returned a duplicate attribute"
            )
        raw_value = raw_value.strip()
        if not raw_value:
            raise CondorObserverError(
                "parse_error", "condor_userprio returned an empty value"
            )
        if raw_value.startswith('"') or raw_value.endswith('"'):
            if not (len(raw_value) >= 2 and raw_value[0] == raw_value[-1] == '"'):
                raise CondorObserverError(
                    "parse_error", "condor_userprio returned malformed quoted value"
                )
            try:
                value: Any = json.loads(raw_value)
            except json.JSONDecodeError as error:
                raise CondorObserverError(
                    "parse_error", "condor_userprio returned malformed quoted value"
                ) from error
            if not isinstance(value, str):
                raise CondorObserverError(
                    "parse_error", "condor_userprio returned non-string quoted value"
                )
        elif raw_value.lower() in {"true", "false"}:
            value = raw_value.lower() == "true"
        elif raw_value.lower() in {"undefined", "error"}:
            value = raw_value.lower()
        else:
            try:
                value = int(raw_value)
            except ValueError:
                try:
                    value = float(raw_value)
                except ValueError as error:
                    raise CondorObserverError(
                        "parse_error",
                        "condor_userprio returned an invalid unquoted value",
                    ) from error
                if not math.isfinite(value):
                    raise CondorObserverError(
                        "parse_error",
                        "condor_userprio returned a non-finite numeric value",
                    )
        current[key] = value
    if current:
        records.append(current)
    if not records:
        raise CondorObserverError("parse_error", "condor_userprio returned no ClassAds")
    return records


_DROP = object()


def _clean_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _DROP
    if isinstance(value, str):
        if len(value) <= MAX_PAYLOAD_STRING_CHARS:
            return value
        return f"{value[: MAX_PAYLOAD_STRING_CHARS - 3]}..."
    return _DROP


def _normalize_ad(
    ad: Mapping[str, Any], allowed_attributes: Sequence[str] | None
) -> dict[str, Any]:
    canonical = (
        {attribute.lower(): attribute for attribute in allowed_attributes}
        if allowed_attributes is not None
        else None
    )
    result: dict[str, Any] = {}
    for raw_key, raw_value in ad.items():
        key = str(raw_key)
        if canonical is not None:
            key = canonical.get(key.lower(), "")
            if not key:
                continue
        value = _clean_scalar(raw_value)
        if value is not _DROP:
            result[key] = value
    return result


def _case_insensitive_value(ad: Mapping[str, Any], attribute: str) -> Any:
    expected = attribute.lower()
    for key, value in ad.items():
        if str(key).lower() == expected:
            return value
    return _DROP


def _validate_workflow_scope(
    request: SchedulerQueryRequest,
    config: CondorObserverConfig,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    if request.kind not in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}:
        return
    if config.tree_scope:
        attribute = "pegasus_root_wf_uuid"
        expected = request.workflow.root_wf_uuid
    else:
        attribute = "pegasus_wf_uuid"
        expected = request.workflow.wf_uuid
    for index, row in enumerate(rows):
        value = _case_insensitive_value(row, attribute)
        if value is _DROP or not isinstance(value, str) or value != expected:
            raise CondorObserverError(
                "scope_mismatch",
                f"{request.kind.value} result {index} did not match the requested "
                "workflow scope",
            )


def _query_state_key(
    request: SchedulerQueryRequest, config: CondorObserverConfig
) -> _QueryStateKey:
    if request.kind in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}:
        if config.tree_scope:
            return (
                request.kind,
                "pegasus_root_wf_uuid",
                request.workflow.root_wf_uuid,
            )
        return (request.kind, "pegasus_wf_uuid", request.workflow.wf_uuid)
    return (request.kind, "global", "")


def _target_for(kind: SchedulerQueryKind, ad: Mapping[str, Any]) -> FrozenPayload:
    if kind in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}:
        keys = ("ClusterId", "ProcId", "DAGNodeName")
    elif kind is SchedulerQueryKind.POOL:
        keys = ("Name", "Machine")
    elif kind is SchedulerQueryKind.PRIORITY:
        keys = ("Name",)
    else:
        keys = ("Name",)
    target = {key: ad[key] for key in keys if key in ad}
    return FrozenPayload.from_mapping(target)


def _to_evidence(
    kind: SchedulerQueryKind,
    rows: Sequence[Mapping[str, Any]],
    allowed_attributes: Sequence[str] | None,
) -> tuple[SchedulerEvidence, ...]:
    evidence: list[SchedulerEvidence] = []
    seen: set[SchedulerEvidence] = set()
    for raw in rows:
        normalized = _normalize_ad(raw, allowed_attributes)
        item = SchedulerEvidence(
            kind,
            _target_for(kind, normalized),
            FrozenPayload.from_mapping(normalized),
        )
        if item not in seen:
            evidence.append(item)
            seen.add(item)
    return tuple(evidence)


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _float_value(value: Any) -> float:
    try:
        result = float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _queue_summary(rows: Sequence[Mapping[str, Any]]) -> FrozenPayload:
    counts = dict.fromkeys(_JOB_STATUS_NAMES.values(), 0)
    counts["other"] = 0
    requested_cpus = 0
    requested_memory_mb = 0
    requested_gpus = 0
    for row in rows:
        name = _JOB_STATUS_NAMES.get(_int_value(row.get("JobStatus")), "other")
        counts[name] += 1
        requested_cpus += _int_value(row.get("RequestCpus"))
        requested_memory_mb += _int_value(row.get("RequestMemory"))
        requested_gpus += _int_value(row.get("RequestGpus"))
    return FrozenPayload.from_mapping(
        {
            "total_jobs": len(rows),
            "status_counts": counts,
            "requested_cpus": requested_cpus,
            "requested_memory_mb": requested_memory_mb,
            "requested_gpus": requested_gpus,
        }
    )


def _history_summary(rows: Sequence[Mapping[str, Any]]) -> FrozenPayload:
    return FrozenPayload.from_mapping(
        {
            "total_jobs": len(rows),
            "remote_wall_seconds": sum(
                _float_value(row.get("RemoteWallClockTime")) for row in rows
            ),
            "remote_user_cpu_seconds": sum(
                _float_value(row.get("RemoteUserCpu")) for row in rows
            ),
            "remote_sys_cpu_seconds": sum(
                _float_value(row.get("RemoteSysCpu")) for row in rows
            ),
            "bytes_sent": sum(_int_value(row.get("BytesSent")) for row in rows),
            "bytes_received": sum(_int_value(row.get("BytesRecvd")) for row in rows),
            "restarts": sum(
                max(0, _int_value(row.get("NumJobStarts")) - 1) for row in rows
            ),
        }
    )


def _pool_summary(rows: Sequence[Mapping[str, Any]]) -> FrozenPayload:
    summary: dict[str, Any] = {
        "total_slots": 0,
        "idle_slots": 0,
        "claimed_slots": 0,
        "other_slots": 0,
        "total_cpus": 0,
        "idle_cpus": 0,
        "total_memory_mb": 0,
        "idle_memory_mb": 0,
        "total_disk_kb": 0,
        "total_gpus": 0,
        "idle_gpus": 0,
        "machines": 0,
        "load_avg": 0.0,
        "os_arch": "",
    }
    machines: set[str] = set()
    platforms: set[str] = set()
    partitionable_parent_machines = {
        str(row.get("Name", "")): str(row.get("Machine", ""))
        for row in rows
        if str(row.get("Name", ""))
        and str(row.get("Machine", ""))
        and _is_partitionable_parent(row)
    }
    partitionable_machines = set(partitionable_parent_machines.values())
    for row in rows:
        machine = str(row.get("Machine", "")) or partitionable_parent_machines.get(
            str(row.get("ParentSlotName", "")), ""
        )
        if machine:
            machines.add(machine)
        opsys = str(row.get("OpSys", ""))
        arch = str(row.get("Arch", ""))
        if opsys or arch:
            platforms.add(f"{opsys}/{arch}")
        cpus = _int_value(row.get("Cpus"))
        memory = _int_value(row.get("Memory"))
        disk = _int_value(row.get("Disk"))
        gpus = _int_value(row.get("GPUs"))
        if _is_partitionable_parent(row):
            summary["load_avg"] += _float_value(row.get("TotalLoadAvg"))
            summary["total_cpus"] += _int_value(row.get("TotalSlotCpus")) or cpus
            summary["total_memory_mb"] += (
                _int_value(row.get("TotalSlotMemory")) or memory
            )
            summary["idle_cpus"] += cpus
            summary["idle_memory_mb"] += memory
            summary["total_disk_kb"] += _int_value(row.get("TotalSlotDisk")) or disk
            total_gpus = _int_value(row.get("TotalSlotGPUs")) or gpus
            summary["total_gpus"] += total_gpus
            summary["idle_gpus"] += gpus
            continue

        summary["total_slots"] += 1
        state = str(row.get("State", ""))
        activity = str(row.get("Activity", ""))
        idle = state == "Unclaimed" or activity == "Idle"
        if idle:
            summary["idle_slots"] += 1
        elif state == "Claimed":
            summary["claimed_slots"] += 1
        else:
            summary["other_slots"] += 1
        # A partitionable parent already represents physical capacity and its
        # remaining idle resources.  Child ads still contribute slot/activity
        # counts, but never duplicate that machine's CPUs, memory, disk, or GPUs.
        dynamic_partitionable_child = (
            machine in partitionable_machines and _is_dynamic_child(row)
        )
        if not dynamic_partitionable_child:
            summary["total_cpus"] += cpus
            summary["total_memory_mb"] += memory
            summary["total_disk_kb"] += disk
            summary["total_gpus"] += gpus
            if idle:
                summary["idle_cpus"] += cpus
                summary["idle_memory_mb"] += memory
                summary["idle_gpus"] += gpus
    summary["machines"] = len(machines)
    summary["os_arch"] = ", ".join(sorted(platforms))
    return FrozenPayload.from_mapping(summary)


def _is_partitionable_parent(row: Mapping[str, Any]) -> bool:
    if str(row.get("SlotType", "")) == "Partitionable":
        return True
    return _ad_boolean(row.get("PartitionableSlot")) and not _ad_boolean(
        row.get("DynamicSlot")
    )


def _is_dynamic_child(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("SlotType", "")) == "Dynamic"
        or _ad_boolean(row.get("DynamicSlot"))
        or bool(str(row.get("ParentSlotName", "")))
    )


def _ad_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _priority_summary(rows: Sequence[Mapping[str, Any]]) -> FrozenPayload:
    priorities = [
        _float_value(row.get("EffectivePriority", row.get("Priority")))
        for row in rows
        if row.get("EffectivePriority", row.get("Priority")) is not None
    ]
    return FrozenPayload.from_mapping(
        {
            "users": len(rows),
            "best_effective_priority": min(priorities) if priorities else None,
            "worst_effective_priority": max(priorities) if priorities else None,
        }
    )


def _negotiator_summary(rows: Sequence[Mapping[str, Any]]) -> FrozenPayload:
    first = rows[0] if rows else {}
    return FrozenPayload.from_mapping(
        {
            "ads": len(rows),
            "last_cycle_duration_seconds": first.get("LastNegotiationCycleDuration0"),
            "last_cycle_matches": first.get("LastNegotiationCycleMatches0"),
        }
    )


def _evidence_identity(item: SchedulerEvidence) -> tuple[tuple[str, Any], ...]:
    return item.target.fields or item.payload.fields


class CondorObserver(SchedulerProvider):
    """Synchronous one-call HTCondor provider with bounded last-good state."""

    def __init__(
        self,
        config: CondorObserverConfig | None = None,
        *,
        stdout_limit: int = MAX_STDOUT_BYTES,
        stderr_limit: int = MAX_STDERR_BYTES,
        cache_limit: int = MAX_CACHE_ENTRIES,
        jitter: Callable[[float], float] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if stdout_limit <= 0 or stderr_limit <= 0 or cache_limit <= 0:
            raise ValueError("observer bounds must be positive")
        self.config = config or CondorObserverConfig()
        self.stdout_limit = stdout_limit
        self.stderr_limit = stderr_limit
        self.cache_limit = cache_limit
        self._jitter = jitter or (
            lambda delay: random.uniform(0.0, min(delay * 0.1, 5.0))
        )
        self._monotonic = monotonic
        self._in_flight = threading.Lock()
        self._active_process = _ActiveProcessRegistry()
        self._cache: dict[_QueryStateKey, _CacheEntry] = {}
        self._failures: dict[_QueryStateKey, int] = {}

    def cancel(self) -> bool:
        """Terminate the active observer process group, if any.

        WP4 owns SIGINT/SIGTERM policy: its handler arranges coordinator
        shutdown, and its ``finally`` path must call ``cancel()`` or ``close()``.
        The provider installs no global signal handler.
        """

        return self._active_process.cancel()

    def close(self) -> None:
        """Idempotently cancel any in-flight helper during coordinator teardown."""

        self.cancel()

    def query(self, request: SchedulerQueryRequest) -> SchedulerQueryResult:
        """Perform exactly one requested query and return without scheduling."""

        if not self._in_flight.acquire(blocking=False):
            return self._failure_result(
                request,
                CondorObserverError(
                    "query_in_flight", "another HTCondor query is already in flight"
                ),
                record_failure=False,
            )
        try:
            command = _build_command(request, self.config)
            outcome = _bounded_subprocess(
                command,
                environment=_build_environment(self.config),
                timeout_seconds=request.timeout_seconds,
                stdout_limit=self.stdout_limit,
                stderr_limit=self.stderr_limit,
                registry=self._active_process,
                monotonic=self._monotonic,
            )
            stderr = _decode_output(outcome.stderr)
            if outcome.returncode:
                _classify_nonzero(
                    outcome.returncode, self._redact_detail(stderr), command[0]
                )
            stdout = _decode_output(outcome.stdout)
            if request.kind is SchedulerQueryKind.PRIORITY:
                rows = _parse_long_classads(stdout)
                allowed = None
            else:
                rows = _strict_json_list(stdout, command[0])
                allowed = _ATTRIBUTES_BY_KIND[request.kind]
            _validate_workflow_scope(request, self.config, rows)
            return self._success_result(request, rows, allowed)
        except CondorObserverError as error:
            return self._failure_result(
                request, error, record_failure=error.code != "cancelled"
            )
        except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as error:
            return self._failure_result(
                request,
                CondorObserverError(
                    "internal_error",
                    f"unexpected {request.kind.value} observer failure: "
                    f"{type(error).__name__}",
                ),
            )
        finally:
            self._in_flight.release()

    def _success_result(
        self,
        request: SchedulerQueryRequest,
        rows: Sequence[Mapping[str, Any]],
        allowed_attributes: Sequence[str] | None,
    ) -> SchedulerQueryResult:
        normalized_rows = [_normalize_ad(row, allowed_attributes) for row in rows]
        observed = _to_evidence(request.kind, normalized_rows, allowed_attributes)
        state_key = _query_state_key(request, self.config)
        if request.kind is SchedulerQueryKind.HISTORY:
            evidence = self._merge_history(state_key, observed)
            summary_rows = [item.payload.to_json_dict() for item in evidence]
            summary = _history_summary(summary_rows)
        else:
            evidence = observed[: self.cache_limit]
            if request.kind is SchedulerQueryKind.QUEUE:
                summary = _queue_summary(normalized_rows)
            elif request.kind is SchedulerQueryKind.POOL:
                summary = _pool_summary(normalized_rows)
            elif request.kind is SchedulerQueryKind.PRIORITY:
                summary = _priority_summary(normalized_rows)
            else:
                summary = _negotiator_summary(normalized_rows)

        cached = tuple(evidence[: self.cache_limit])
        self._cache[state_key] = _CacheEntry(cached, summary, request.clock.epoch)
        self._failures[state_key] = 0
        health = SourceHealth(
            source=_SOURCE_BY_KIND[request.kind],
            state=HealthState.HEALTHY,
            checked_at_epoch=request.clock.epoch,
            last_success_epoch=request.clock.epoch,
            stale_after_seconds=_STALE_AFTER_SECONDS[request.kind],
        )
        return SchedulerQueryResult(
            request=request,
            health=health,
            backoff_seconds=0.0,
            evidence=cached[: request.result_limit],
            summary=summary,
        )

    def _merge_history(
        self,
        state_key: _QueryStateKey,
        observed: tuple[SchedulerEvidence, ...],
    ) -> tuple[SchedulerEvidence, ...]:
        previous = self._cache.get(state_key)
        combined = observed + (() if previous is None else previous.evidence)
        unique: list[SchedulerEvidence] = []
        seen: set[tuple[tuple[str, Any], ...]] = set()
        for item in combined:
            identity = _evidence_identity(item)
            if identity not in seen:
                unique.append(item)
                seen.add(identity)
            if len(unique) >= self.cache_limit:
                break
        return tuple(unique)

    def _failure_result(
        self,
        request: SchedulerQueryRequest,
        error: CondorObserverError,
        *,
        record_failure: bool = True,
    ) -> SchedulerQueryResult:
        state_key = _query_state_key(request, self.config)
        if record_failure:
            self._failures[state_key] = self._failures.get(state_key, 0) + 1
        failures = self._failures.get(state_key, 0)
        local_interruption = error.code in {"query_in_flight", "cancelled"}
        if local_interruption:
            delay = 0.0
        else:
            effective_failures = max(1, failures)
            base = _BASE_BACKOFF_SECONDS[request.kind]
            delay = min(
                base * (2 ** min(effective_failures - 1, 10)),
                MAX_BACKOFF_SECONDS,
            )
            delay = min(
                MAX_BACKOFF_SECONDS,
                max(0.0, delay + max(0.0, self._jitter(delay))),
            )
        cache = self._cache.get(state_key)
        redacted_detail = self._redact_detail(error.detail)
        detail = " ".join(redacted_detail.split())[:MAX_HEALTH_DETAIL_CHARS]
        health = SourceHealth(
            source=_SOURCE_BY_KIND[request.kind],
            state=(
                HealthState.STALE
                if cache is not None
                else (
                    HealthState.DEGRADED
                    if local_interruption
                    else HealthState.UNAVAILABLE
                )
            ),
            checked_at_epoch=request.clock.epoch,
            last_success_epoch=None if cache is None else cache.last_success_epoch,
            last_good_age=(
                None
                if cache is None
                else BoundedAge.between(
                    request.clock.epoch,
                    cache.last_success_epoch,
                    7 * 24 * 60 * 60.0,
                )
            ),
            stale_after_seconds=_STALE_AFTER_SECONDS[request.kind],
            consecutive_failures=failures,
            error_code=error.code,
            detail=detail,
        )
        return SchedulerQueryResult(
            request=request,
            health=health,
            backoff_seconds=delay,
            evidence=(() if cache is None else cache.evidence[: request.result_limit]),
            summary=FrozenPayload() if cache is None else cache.summary,
        )

    def _redact_detail(self, detail: str) -> str:
        values = [
            self.config.token,
            self.config.cert,
            self.config.key,
            self.config.password_file,
            self.config.condor_config,
        ]
        child_environment = _build_environment(self.config)
        values.extend(
            value
            for name, value in child_environment.items()
            if any(
                marker in name.upper()
                for marker in ("TOKEN", "PASSWORD", "CERT", "KEY", "PROXY")
            )
        )
        for value in values:
            if value:
                detail = detail.replace(value, "<redacted>")
        return detail


__all__ = (
    "CondorObserver",
    "CondorObserverConfig",
    "CondorObserverError",
    "build_workflow_constraint",
)
