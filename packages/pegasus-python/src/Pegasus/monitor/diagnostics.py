"""Structured failure diagnostics over immutable monitor publications."""

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
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from Pegasus.monitor.models import (
    DiagnosticEvidence,
    DiagnosticSeverity,
    EffectiveSnapshot,
    FrozenPayload,
    JobAttempt,
    JobSnapshot,
    Lifecycle,
    Provenance,
    SchedulerQueryKind,
    SchedulerQueryResult,
    SourceName,
)
from Pegasus.monitor.stall_detector import StallDetector, StallEventKind, StallResult
from Pegasus.monitor.why_idle import IdleAnalysis, analyze_why_idle

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from Pegasus.monitor.coordinator import CoordinatorSnapshot

MAX_KICKSTART_BYTES = 1024 * 1024
MAX_KICKSTART_NODES = 5000
MAX_KICKSTART_DEPTH = 32
MAX_EXCERPT_CHARS = 512
MAX_SUGGESTIONS = 8
_SAFE_JOB_COMPONENT = re.compile(r"^[A-Za-z0-9_.+@=-]{1,512}$")
_AUTH_SECRET = re.compile(
    r"(?i)(bearer\s+|(?:authorization\s*:\s*)?basic\s+)([^\s,;|\"']+)"
)
_NAMED_SECRET = re.compile(
    r"(?ix)(?<![A-Za-z0-9_])("  # bounded environment/config key
    r"[\"']?(?:[A-Za-z0-9]{1,32}_){0,4}"
    r"(?:token|password|passwd|secret|private[_-]key|api[_-]key|access[_-]key)"
    r"(?:_[A-Za-z0-9]{1,32}){0,4}[\"']?\s*[=:]\s*[\"']?)"
    r"([^\s,;|\"']+)"
)
_URL_CREDENTIAL = re.compile(r"(?i)([a-z][a-z0-9+.-]{0,31}://)[^/@\s]+@")
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
    re.DOTALL,
)
_HOME_PATH = re.compile(r"(?:/Users|/home)/[^/\s]+/[^\s|,;]*")


class DiagnosticCategory(str, Enum):
    HELD = "held"
    FAILED = "failed"


class KickstartReadStatus(str, Enum):
    AVAILABLE = "available"
    MISSING = "missing"
    REJECTED = "rejected"
    TOO_LARGE = "too_large"
    MALFORMED = "malformed"
    UNREADABLE = "unreadable"


@dataclass(frozen=True, slots=True)
class StderrAnalysis:
    missing_files: tuple[str, ...] = ()
    transfer_attempts: int = 1
    transfers_failed: bool = False
    integrity_error: bool = False
    permission_denied: tuple[str, ...] = ()
    no_space: bool = False
    connection_error: bool = False

    @property
    def has_findings(self) -> bool:
        return bool(
            self.missing_files
            or self.transfer_attempts > 1
            or self.transfers_failed
            or self.integrity_error
            or self.permission_denied
            or self.no_space
            or self.connection_error
        )


@dataclass(frozen=True, slots=True)
class KickstartInfo:
    exit_code: int | None = None
    stdout_excerpt: str = ""
    stderr_excerpt: str = ""
    executable: str = ""
    argv: tuple[str, ...] = ()
    duration_seconds: float | None = None
    transformation: str = ""
    stderr_analysis: StderrAnalysis | None = None


@dataclass(frozen=True, slots=True)
class KickstartReadResult:
    status: KickstartReadStatus
    relative_path: str | None = None
    info: KickstartInfo | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class DiagnosticFinding:
    exec_job_id: str
    category: DiagnosticCategory
    severity: DiagnosticSeverity
    code: str
    summary: str
    suggestions: tuple[str, ...]
    job_provenance: str
    evidence: tuple[DiagnosticEvidence, ...]


@dataclass(frozen=True, slots=True)
class DiagnosticsBatch:
    findings: tuple[DiagnosticFinding, ...] = ()
    new_findings: tuple[DiagnosticFinding, ...] = ()
    stall: StallResult | None = None
    idle: IdleAnalysis | None = None
    errors: tuple[str, ...] = ()


_HOLD_PATTERNS: tuple[tuple[re.Pattern[str], str, str, tuple[str, ...]], ...] = (
    (
        re.compile(r"Transfer output files failure.*No such file", re.I),
        "output_missing",
        "Output transfer failed because an expected file was not created",
        (
            "Check the job stdout and stderr for the reason the output was not created.",
            "Verify declared output names match the executable's output names.",
            "Run pegasus-analyzer on the submit directory for full context.",
        ),
    ),
    (
        re.compile(r"Transfer output files failure", re.I),
        "output_transfer_failed",
        "Output file transfer failed",
        (
            "Check job stderr and available disk space.",
            "Verify network connectivity between execute and submit hosts.",
        ),
    ),
    (
        re.compile(r"Transfer input files failure", re.I),
        "input_transfer_failed",
        "Input file transfer failed",
        (
            "Verify input files and Replica Catalog entries.",
            "Check execute-node disk space and network connectivity.",
        ),
    ),
    (
        re.compile(r"memory (?:usage|limit)|MEMORY_EXCEEDED|OOM|oom-kill", re.I),
        "memory_exceeded",
        "Job exceeded its memory limit",
        (
            "Increase the job memory request if the observed use is expected.",
            "Inspect the executable for unexpectedly high memory growth.",
        ),
    ),
    (
        re.compile(r"disk (?:usage|limit)|DISK_EXCEEDED", re.I),
        "disk_exceeded",
        "Job exceeded its disk limit",
        ("Increase request_disk or reduce temporary-file use.",),
    ),
    (
        re.compile(r"wall.?time|TIME_EXCEEDED|exceeded.*time", re.I),
        "walltime_exceeded",
        "Job exceeded its wall-time limit",
        ("Increase the wall-time request or investigate a hung executable.",),
    ),
    (
        re.compile(r"credential|proxy|certificate|myproxy", re.I),
        "credential_failure",
        "Credential or proxy validation failed",
        ("Verify that the configured credential is present and unexpired.",),
    ),
    (
        re.compile(r"SHADOW_EXCEPTION|shadow", re.I),
        "shadow_exception",
        "HTCondor shadow reported an exception",
        ("Inspect the schedd shadow log and network connectivity.",),
    ),
    (
        re.compile(r"UNRESOLVABLE|cannot resolve|DNS", re.I),
        "dns_failure",
        "Hostname resolution failed",
        ("Verify DNS and collector/submit-host name resolution.",),
    ),
    (
        re.compile(r"docker|container|singularity|apptainer", re.I),
        "container_failure",
        "Container runtime failed",
        ("Verify the image and runtime are available on the execute resource.",),
    ),
    (
        re.compile(r"periodic.*hold|policy", re.I),
        "policy_hold",
        "Job was held by scheduler policy",
        ("Review periodic_hold and site policy limits.",),
    ),
)

_FAILURE_SUGGESTIONS: dict[int | None, tuple[str, ...]] = {
    None: (
        "Run pegasus-analyzer on the submit directory.",
        "Inspect the current attempt's stdout and stderr.",
    ),
    1: ("Exit code 1 indicates a general executable error; inspect stderr.",),
    2: ("Exit code 2 commonly indicates invalid arguments or shell usage.",),
    126: ("Exit code 126 indicates that the command was not executable.",),
    127: ("Exit code 127 indicates that the command was not found.",),
    137: ("Exit code 137 commonly indicates SIGKILL or an out-of-memory kill.",),
    139: ("Exit code 139 indicates a segmentation fault.",),
}

_RE_MISSING_FILE = re.compile(r"Expected local file does not exist:\s*(.+)", re.I)
_RE_TRANSFER_ATTEMPT = re.compile(r"Starting transfers - attempt (\d+)", re.I)
_RE_TRANSFERS_FAILED = re.compile(r"Some transfers failed", re.I)
_RE_INTEGRITY_ERROR = re.compile(r"integrity verification failed", re.I)
_RE_PERMISSION_DENIED = re.compile(r"Permission denied.*?:\s*(.+)", re.I)
_RE_NO_SPACE = re.compile(r"No space left on device", re.I)
_RE_CONNECTION = re.compile(
    r"Connection refused|Connection timed out|Network is unreachable", re.I
)


def redact_excerpt(value: str, maximum: int = MAX_EXCERPT_CHARS) -> str:
    """Redact common secrets and user-home prefixes before bounding text."""

    if maximum < 4:
        raise ValueError("excerpt maximum must be at least four characters")
    redacted = _PRIVATE_KEY.sub("<redacted-private-key>", value)
    redacted = _AUTH_SECRET.sub(lambda match: f"{match.group(1)}<redacted>", redacted)
    redacted = _NAMED_SECRET.sub(lambda match: f"{match.group(1)}<redacted>", redacted)
    redacted = _URL_CREDENTIAL.sub(r"\1<redacted>@", redacted)
    redacted = _HOME_PATH.sub(
        lambda match: f"<home>/{Path(match.group()).name}", redacted
    )
    redacted = " ".join(redacted.split())
    return redacted if len(redacted) <= maximum else f"{redacted[: maximum - 3]}..."


def _analyze_stderr(stderr: str) -> StderrAnalysis:
    missing = tuple(
        dict.fromkeys(
            redact_excerpt(match.group(1), 256)
            for match in _RE_MISSING_FILE.finditer(stderr)
        )
    )
    attempts = [int(match.group(1)) for match in _RE_TRANSFER_ATTEMPT.finditer(stderr)]
    denied = tuple(
        dict.fromkeys(
            redact_excerpt(match.group(1), 256)
            for match in _RE_PERMISSION_DENIED.finditer(stderr)
        )
    )
    return StderrAnalysis(
        missing,
        max(attempts, default=1),
        bool(_RE_TRANSFERS_FAILED.search(stderr)),
        bool(_RE_INTEGRITY_ERROR.search(stderr)),
        denied,
        bool(_RE_NO_SPACE.search(stderr)),
        bool(_RE_CONNECTION.search(stderr)),
    )


def _safe_relative_path(exec_job_id: str, stdout_file: str | None) -> Path | None:
    if stdout_file is None:
        if not _SAFE_JOB_COMPONENT.fullmatch(exec_job_id):
            return None
        return Path("00") / "00" / f"{exec_job_id}.out.000"
    path = Path(stdout_file)
    if (
        len(stdout_file) > 4096
        or path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        return None
    if any("\x00" in part or len(part) > 255 for part in path.parts):
        return None
    return path


def _bounded_yaml(
    value: Any, *, depth: int = 0, budget: list[int] | None = None
) -> None:
    if budget is None:
        budget = [MAX_KICKSTART_NODES]
    budget[0] -= 1
    if budget[0] < 0 or depth > MAX_KICKSTART_DEPTH:
        raise ValueError("kickstart YAML structure exceeds limits")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, (str, int, float, bool, type(None))):
                raise ValueError("kickstart YAML key has unsupported type")
            _bounded_yaml(item, depth=depth + 1, budget=budget)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _bounded_yaml(item, depth=depth + 1, budget=budget)
    elif not isinstance(value, (str, int, float, bool, type(None))):
        raise ValueError("kickstart YAML value has unsupported type")


def _read_bounded(descriptor: int, maximum_bytes: int) -> bytes:
    chunks: list[bytes] = []
    remaining = maximum_bytes + 1
    while remaining > 0:
        chunk = os.read(descriptor, min(65536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def parse_kickstart_output(
    submit_dir: Path,
    exec_job_id: str,
    stdout_file: str | None = None,
    *,
    maximum_bytes: int = MAX_KICKSTART_BYTES,
) -> KickstartReadResult:
    """Read one local kickstart file without following an attacker-controlled path."""

    if maximum_bytes <= 0 or maximum_bytes > MAX_KICKSTART_BYTES:
        raise ValueError(f"maximum_bytes must be in 1..{MAX_KICKSTART_BYTES}")
    relative = _safe_relative_path(exec_job_id, stdout_file)
    if relative is None:
        return KickstartReadResult(
            KickstartReadStatus.REJECTED, error_code="unsafe_path"
        )
    try:
        base = submit_dir.resolve(strict=True)
        if not base.is_dir():
            return KickstartReadResult(
                KickstartReadStatus.REJECTED, error_code="submit_not_directory"
            )
        candidate = base.joinpath(relative)
        cursor = base
        for part in relative.parts[:-1]:
            cursor = cursor / part
            if stat.S_ISLNK(cursor.lstat().st_mode):
                return KickstartReadResult(
                    KickstartReadStatus.REJECTED,
                    str(relative),
                    error_code="symlink_parent",
                )
        metadata = candidate.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            return KickstartReadResult(
                KickstartReadStatus.REJECTED, str(relative), error_code="symlink"
            )
        resolved = candidate.resolve(strict=True)
        if not resolved.is_relative_to(base) or not stat.S_ISREG(metadata.st_mode):
            return KickstartReadResult(
                KickstartReadStatus.REJECTED, str(relative), error_code="unsafe_file"
            )
        if metadata.st_size > maximum_bytes:
            return KickstartReadResult(
                KickstartReadStatus.TOO_LARGE, str(relative), error_code="size_limit"
            )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(resolved, flags)
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
                metadata.st_dev,
                metadata.st_ino,
            ):
                return KickstartReadResult(
                    KickstartReadStatus.REJECTED,
                    str(relative),
                    error_code="file_changed",
                )
            content = _read_bounded(descriptor, maximum_bytes)
        finally:
            os.close(descriptor)
    except FileNotFoundError:
        return KickstartReadResult(
            KickstartReadStatus.MISSING, str(relative), error_code="not_found"
        )
    except OSError:
        return KickstartReadResult(
            KickstartReadStatus.UNREADABLE, str(relative), error_code="read_error"
        )
    if len(content) > maximum_bytes:
        return KickstartReadResult(
            KickstartReadStatus.TOO_LARGE, str(relative), error_code="size_limit"
        )
    try:
        text = content.decode("utf-8")
        tokens = yaml.scan(text)
        forbidden = (yaml.AliasToken, yaml.AnchorToken, yaml.TagToken)
        if any(isinstance(token, forbidden) for token in tokens):
            raise ValueError("YAML aliases, anchors, and tags are not accepted")
        data = yaml.safe_load(text)
        _bounded_yaml(data)
    except (UnicodeDecodeError, yaml.YAMLError, ValueError, RecursionError):
        return KickstartReadResult(
            KickstartReadStatus.MALFORMED, str(relative), error_code="invalid_yaml"
        )
    if isinstance(data, list):
        data = data[0] if len(data) == 1 else None
    if not isinstance(data, Mapping):
        return KickstartReadResult(
            KickstartReadStatus.MALFORMED, str(relative), error_code="invalid_shape"
        )
    mainjob = data.get("mainjob")
    files = data.get("files")
    mainjob = mainjob if isinstance(mainjob, Mapping) else {}
    files = files if isinstance(files, Mapping) else {}
    status_value = mainjob.get("status")
    executable_value = mainjob.get("executable")
    status_value = status_value if isinstance(status_value, Mapping) else {}
    executable_value = executable_value if isinstance(executable_value, Mapping) else {}
    stdout_value = files.get("stdout")
    stderr_value = files.get("stderr")
    stdout_value = stdout_value if isinstance(stdout_value, Mapping) else {}
    stderr_value = stderr_value if isinstance(stderr_value, Mapping) else {}
    stderr = str(stderr_value.get("data") or "")
    argv_value = mainjob.get("argument_vector")
    argv = (
        tuple(redact_excerpt(str(item), 256) for item in argv_value[:128])
        if isinstance(argv_value, list)
        else ()
    )
    info = KickstartInfo(
        _as_int(status_value.get("regular_exitcode")),
        redact_excerpt(str(stdout_value.get("data") or "")),
        redact_excerpt(stderr),
        redact_excerpt(str(executable_value.get("file_name") or ""), 256),
        argv,
        _as_float(mainjob.get("duration")),
        redact_excerpt(str(data.get("transformation") or ""), 256),
        _analyze_stderr(stderr) if stderr else None,
    )
    return KickstartReadResult(KickstartReadStatus.AVAILABLE, str(relative), info)


def _as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        result = float(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result is None or math.isfinite(result) else None


def _current_attempt(job: JobSnapshot) -> JobAttempt | None:
    if job.current_attempt is None:
        return None
    return next(
        (item for item in job.attempts if item.identity == job.current_attempt), None
    )


def _real_exit_code(
    attempt: JobAttempt | None, kickstart: KickstartInfo | None
) -> int | None:
    if kickstart is not None and kickstart.exit_code is not None:
        return kickstart.exit_code
    if attempt is None:
        return None
    if attempt.exit_code is not None:
        return attempt.exit_code
    raw = attempt.raw_wait_status
    if raw is None:
        return None
    try:
        return os.waitstatus_to_exitcode(raw)
    except ValueError:
        return None


def _hold_diagnostic(job: JobSnapshot, reason: str | None) -> DiagnosticFinding:
    summary = "Job is held by HTCondor"
    code = "held_unknown"
    suggestions = (
        "Inspect the full HoldReason and site policy before releasing the job.",
        "Run pegasus-analyzer on the submit directory for Pegasus-level context.",
    )
    if reason:
        for pattern, match_code, match_summary, match_suggestions in _HOLD_PATTERNS:
            if pattern.search(reason):
                code, summary, suggestions = (
                    match_code,
                    match_summary,
                    match_suggestions,
                )
                break
    evidence = (
        _state_evidence(job),
        DiagnosticEvidence(
            SourceName.CONDOR_QUEUE,
            "hold_reason" if reason else "hold_reason_unavailable",
            FrozenPayload.from_mapping(
                {"reason_excerpt": redact_excerpt(reason or "No hold reason available")}
            ),
        ),
    )
    return DiagnosticFinding(
        job.exec_job_id,
        DiagnosticCategory.HELD,
        DiagnosticSeverity.WARNING,
        code,
        summary,
        suggestions[:MAX_SUGGESTIONS],
        job.provenance.value,
        evidence,
    )


def _stderr_diagnosis(
    analysis: StderrAnalysis | None, exit_code: int | None
) -> tuple[str, str, tuple[str, ...]] | None:
    if analysis is None or not analysis.has_findings:
        return None
    if analysis.missing_files:
        suggestions = tuple(f"Missing: {path}" for path in analysis.missing_files) + (
            "Verify the file and Replica Catalog entry.",
        )
        return "missing_input", "Input file is missing", suggestions
    if analysis.transfers_failed:
        return (
            "transfer_failed",
            f"File transfer failed after {analysis.transfer_attempts} attempt(s)",
            ("Inspect transfer stderr and network/disk availability.",),
        )
    if analysis.integrity_error:
        return (
            "integrity_failed",
            "File integrity verification failed",
            (
                "Verify expected checksums and retry the transfer from a trusted replica.",
            ),
        )
    if analysis.permission_denied:
        return (
            "permission_denied",
            "Permission was denied while accessing a file",
            tuple(f"Permission denied: {path}" for path in analysis.permission_denied),
        )
    if analysis.no_space:
        return (
            "no_space",
            "No space left on device",
            ("Free space or increase request_disk.",),
        )
    if analysis.connection_error:
        return (
            "network_failure",
            "A network error interrupted file transfer",
            (
                "Verify the staging endpoint, routing, firewall, and proxy configuration.",
            ),
        )
    return None


def _failure_diagnostic(
    job: JobSnapshot, kickstart: KickstartReadResult
) -> DiagnosticFinding:
    attempt = _current_attempt(job)
    info = kickstart.info
    exit_code = _real_exit_code(attempt, info)
    specific = _stderr_diagnosis(
        None if info is None else info.stderr_analysis, exit_code
    )
    if specific is None:
        code = "exit_code_unknown" if exit_code is None else f"exit_code_{exit_code}"
        summary = (
            "Job failed without a recorded exit code"
            if exit_code is None
            else f"Job failed with exit code {exit_code}"
        )
        suggestions = _FAILURE_SUGGESTIONS.get(exit_code, _FAILURE_SUGGESTIONS[None])
    else:
        code, summary, suggestions = specific
    evidence: list[DiagnosticEvidence] = [
        _state_evidence(job),
        DiagnosticEvidence(
            SourceName.STAMPEDE,
            "current_attempt_exit",
            FrozenPayload.from_mapping(
                {
                    "exit_code": exit_code,
                    "raw_wait_status": None
                    if attempt is None
                    else attempt.raw_wait_status,
                    "job_submit_seq": None
                    if job.current_attempt is None
                    else job.current_attempt.job_submit_seq,
                }
            ),
        ),
    ]
    kickstart_payload: dict[str, Any] = {
        "status": kickstart.status.value,
        "relative_path": kickstart.relative_path,
        "error_code": kickstart.error_code,
    }
    if info is not None:
        kickstart_payload.update(
            {
                "stderr_excerpt": info.stderr_excerpt,
                "stdout_excerpt": info.stdout_excerpt,
                "executable": info.executable,
                "transformation": info.transformation,
            }
        )
    evidence.append(
        DiagnosticEvidence(
            SourceName.KICKSTART,
            "kickstart_parse",
            FrozenPayload.from_mapping(kickstart_payload),
        )
    )
    return DiagnosticFinding(
        job.exec_job_id,
        DiagnosticCategory.FAILED,
        DiagnosticSeverity.ERROR,
        code,
        redact_excerpt(summary, 1024),
        tuple(suggestions[:MAX_SUGGESTIONS]),
        job.provenance.value,
        tuple(evidence),
    )


def _state_evidence(job: JobSnapshot) -> DiagnosticEvidence:
    source = (
        SourceName.STAMPEDE
        if job.provenance is Provenance.DB_CONFIRMED
        else SourceName.LIVE_TAIL
    )
    return DiagnosticEvidence(
        source,
        "effective_job_state",
        FrozenPayload.from_mapping(
            {
                "state": job.state,
                "state_timestamp": None
                if job.state_timestamp is None
                else str(job.state_timestamp),
                "provenance": job.provenance.value,
            }
        ),
    )


def _effective_inputs(
    value: EffectiveSnapshot | CoordinatorSnapshot,
    results: Sequence[SchedulerQueryResult] | None,
) -> tuple[EffectiveSnapshot, tuple[SchedulerQueryResult, ...]]:
    if isinstance(value, EffectiveSnapshot):
        return value, tuple(results or ())
    if value.effective is None:
        raise ValueError("diagnostics require an authoritative effective snapshot")
    return value.effective, tuple(
        value.scheduler_results if results is None else results
    )


def _latest_queue(
    results: Iterable[SchedulerQueryResult],
) -> SchedulerQueryResult | None:
    queue = [item for item in results if item.request.kind is SchedulerQueryKind.QUEUE]
    return max(queue, default=None, key=lambda item: item.request.clock.monotonic)


def collect_diagnostics(
    snapshot: EffectiveSnapshot | CoordinatorSnapshot,
    scheduler_results: Sequence[SchedulerQueryResult] | None = None,
    *,
    submit_dir: Path | None = None,
    job_exec_ids: frozenset[str] | None = None,
) -> tuple[DiagnosticFinding, ...]:
    """Diagnose all currently held/failed jobs with no DB or scheduler calls."""

    effective, results = _effective_inputs(snapshot, scheduler_results)
    queue = _latest_queue(results)
    hold_reasons: dict[str, str] = {}
    if queue is not None:
        for item in queue.evidence:
            row = item.payload.to_json_dict()
            node = str(row.get("DAGNodeName") or "")
            reason = str(row.get("HoldReason") or "")
            if node and reason:
                hold_reasons[node] = reason
    findings: list[DiagnosticFinding] = []
    for job in effective.jobs:
        if job_exec_ids is not None and job.exec_job_id not in job_exec_ids:
            continue
        if job.lifecycle is Lifecycle.HELD:
            findings.append(_hold_diagnostic(job, hold_reasons.get(job.exec_job_id)))
        elif job.lifecycle is Lifecycle.FAILED:
            attempt = _current_attempt(job)
            kickstart = (
                parse_kickstart_output(
                    submit_dir,
                    job.exec_job_id,
                    None if attempt is None else attempt.stdout_path,
                )
                if submit_dir is not None
                else KickstartReadResult(
                    KickstartReadStatus.MISSING, error_code="submit_dir_unavailable"
                )
            )
            findings.append(_failure_diagnostic(job, kickstart))
    return tuple(findings)


class DiagnosticsEngine:
    """In-memory deduplication and optional stall/why-idle orchestration."""

    def __init__(self, stall_detector: StallDetector | None = None) -> None:
        self._stall_detector = stall_detector or StallDetector()
        self._active_findings: dict[tuple[Any, ...], DiagnosticFinding] = {}

    def analyze(
        self,
        snapshot: CoordinatorSnapshot,
        *,
        submit_dir: Path | None = None,
        workflow_owner: str | None = None,
        disabled_scheduler_kinds: frozenset[SchedulerQueryKind] = frozenset(),
    ) -> DiagnosticsBatch:
        if snapshot.effective is None:
            self._active_findings.clear()
            return DiagnosticsBatch(errors=("authoritative_snapshot_unavailable",))
        problem_jobs = tuple(
            job
            for job in snapshot.effective.jobs
            if job.lifecycle in {Lifecycle.HELD, Lifecycle.FAILED}
        )
        problem_jobs_by_id = {job.exec_job_id: job for job in problem_jobs}
        current_keys = {self._problem_key(job): job for job in problem_jobs}
        retained = {
            key: finding
            for key, finding in self._active_findings.items()
            if key in current_keys
        }
        new_keys = set(current_keys) - set(retained)
        if new_keys:
            try:
                diagnosed = collect_diagnostics(
                    snapshot,
                    submit_dir=submit_dir,
                    job_exec_ids=frozenset(
                        current_keys[key].exec_job_id for key in new_keys
                    ),
                )
            except (OSError, ValueError, TypeError, yaml.YAMLError) as error:
                diagnosed = ()
                errors = (f"diagnostics:{type(error).__name__}",)
            else:
                errors = ()
            for finding in diagnosed:
                job = problem_jobs_by_id.get(finding.exec_job_id)
                if job is None:
                    continue
                key = self._problem_key(job)
                if key in new_keys:
                    retained[key] = finding
        else:
            errors = ()
        emitted = tuple(finding for key, finding in retained.items() if key in new_keys)
        self._active_findings = retained
        stall = self._stall_detector.check(snapshot)
        idle = None
        if (
            stall is not None
            and stall.event is StallEventKind.DETECTED
            and stall.queued_jobs > 0
        ):
            idle = analyze_why_idle(
                snapshot,
                workflow_owner=workflow_owner,
                disabled_scheduler_kinds=disabled_scheduler_kinds,
            )
        return DiagnosticsBatch(tuple(retained.values()), emitted, stall, idle, errors)

    @staticmethod
    def _problem_key(job: JobSnapshot) -> tuple[Any, ...]:
        attempt = job.current_attempt
        return (
            job.lifecycle,
            job.exec_job_id,
            job.state,
            None if job.state_timestamp is None else str(job.state_timestamp),
            None if attempt is None else attempt.job_submit_seq,
        )
