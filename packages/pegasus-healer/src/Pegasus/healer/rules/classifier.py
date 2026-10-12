from __future__ import annotations

import re

from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis, FailureType

RULE_VERSION = "2.0.0"

# ── Signal sets ───────────────────────────────────────────────────────────────

OOM_EXIT_CODES: set[int] = {137}
OOM_SIGNALS: set[int] = {9}
OOM_HOLD_PATTERNS: list[str] = [r"memory", r"oom", r"out.of.memory", r"mem.?limit"]

DISK_HOLD_PATTERNS: list[str] = [r"disk", r"quota", r"no.space", r"disk.?exceeded"]
DISK_STDERR_PATTERNS: list[str] = [
    r"no\s+space\s+left",
    r"disk.?full",
    r"disk.?quota",
    r"errno\s*28",        # ENOSPC
    r"disk.?exceeded",
    r"diskspace",
    r"exceeded.*disk",
]
# Scratch-specific: same ENOSPC but path is /tmp, scratch, or TMPDIR
SCRATCH_PATH_PATTERNS: list[str] = [
    r"/tmp/", r"/scratch/", r"tmpdir", r"\$tmpdir", r"temp.?dir",
    r"/local/", r"/lscratch/",
]

WALLTIME_PATTERNS: list[str] = [
    r"wall.?time", r"time.?limit", r"runtime.?exceeded", r"job.?killed.*time",
    r"maxwalltime", r"wall.?clock", r"timelimit", r"time limit reached",
]

# PREEMPTED — checked before generic TRANSIENT
PREEMPTED_SIGNALS: set[int] = {15}   # SIGTERM from preemption
PREEMPTED_EXIT_CODES: set[int] = {143}  # 128 + 15
PREEMPTED_SCHEDULER_STATES: set[str] = {"PREEMPTED", "REQUEUED"}
PREEMPTED_PATTERNS: list[str] = [r"preempt", r"requeue"]

# NODE_FAILURE — SIGKILL without exit 137 (OOM), or explicit node-fail state
NODE_FAILURE_SCHEDULER_STATES: set[str] = {"NODE_FAIL", "BOOT_FAIL", "FAILED"}
NODE_FAILURE_PATTERNS: list[str] = [
    r"node.?fail", r"node.?down", r"hardware.?fail", r"node.?error",
    r"bus.?error", r"segmentation.?fault.*node",
]

TRANSIENT_PATTERNS: list[str] = [
    r"network", r"evict",
    r"hold.?reason.*250",   # condor shadow exception / preemption code
    r"disconnected", r"lost.?connection", r"timed.?out.*connect",
    r"temporary.*failure", r"try.?again",
]

SCHEDULER_ADMISSION_PATTERNS: list[str] = [
    r"invalid.?queue", r"unknown.?queue", r"unknown.?partition",
    r"qos", r"request.?exceed", r"too.?many", r"over.?limit",
    r"not.?authorized", r"invalid.?account",
]

ACCOUNT_EXPIRED_PATTERNS: list[str] = [
    r"account.*expired", r"allocation.*exhausted", r"budget.*exceeded",
    r"project.*expired", r"no.?allocation", r"insufficient.*balance",
    r"account.*invalid.*allocation", r"project.*closed",
]

STAGING_STDERR_PATTERNS: list[str] = [
    r"pegasus.?transfer", r"transfer.?failed", r"transfer.?error",
    r"failed.?to.?transfer", r"stale.?file.?handle",
    r"connection.*refused.*transfer", r"timeout.*transfer",
    # PegasusLite .meta file check — happens when stage_in job didn't complete
    r"cannot\s+stat.*\.meta", r"\.meta.*no such file",
    r"stage_in.*\.meta", r"cp.*meta.*no such",
]
STAGING_TRANSFORMATIONS: list[str] = [
    "stage_in", "stage_out", "stagein", "stageout",
    "transfer", "pegasus_transfer",
]

LICENSE_PATTERNS: list[str] = [
    r"license", r"licens", r"checkout.?fail", r"cannot.?connect.*license",
    r"license.?server", r"feature.?not.?available", r"no.?licenses.?available",
    r"license.?expired", r"flexnet", r"rlm.*error",
]

MPI_PATTERNS: list[str] = [
    r"mpi_abort", r"fatal.?error.?in.?pmpi", r"mpi.?error",
    r"orte_ess_base_std_abort", r"orted.*failed", r"mpirun.*killed",
    r"srun.*error.*mpi", r"rank.*died", r"collective.*mpi",
    r"process.*terminated.*signal.*mpi",
]

CONTAINER_PATTERNS: list[str] = [
    r"fatal.*unable.?to.?pull", r"unable.?to.?pull",
    r"error.?response.?from.?daemon", r"manifest.?unknown",
    r"docker.*error", r"singularity.*error.*image",
    r"failed.?to.?fetch.*image", r"image.?not.?found",
    r"repository.?does.?not.?exist", r"pull.?access.?denied",
]

MODULE_PATTERNS: list[str] = [
    r"no.?module.?named", r"module.?not.?found",
    r"unable.?to.?locate.?a.?modulefile", r"command.?not.?found",
    r"no.?such.?file.?or.?directory.*bin",
    r"module.*error", r"cannot.?find.?module",
]

GPU_PATTERNS: list[str] = [
    r"cuda.?error", r"no.?cuda.?capable.?device", r"gpu.?not.?available",
    r"failed.?to.?initialize.?nvml", r"cuda_error_no_device",
    r"nvidia.?smi.*not.?found", r"no.?gpu.*allocated",
    r"cuda.*initialization.*failed", r"opencl.*no.?platform",
]

DATA_CORRUPTION_PATTERNS: list[str] = [
    r"checksum.?mismatch", r"hash.?mismatch", r"integrity.?check.?fail",
    r"md5.*mismatch", r"sha.*mismatch", r"corrupt", r"truncated.?file",
    r"unexpected.?eof", r"invalid.?magic", r"bad.?header",
    r"crc.?error", r"file.?corrupt",
]

SSH_PATTERNS: list[str] = [
    r"permission.?denied.*publickey", r"host.?key.?verification.?fail",
    r"authentication.?fail", r"ssh.*connect.*fail",
    r"ssh.*no.?route.?to.?host", r"ssh.*connection.?refused",
    r"ssh_exchange_identification", r"unable.?to.?negotiate",
]


# ── Public interface ──────────────────────────────────────────────────────────

def classify(ctx: FailureContext) -> Diagnosis | None:
    """
    Run deterministic rules against the FailureContext.

    Rules are evaluated in priority order — most specific / highest-signal first.
    Returns a Diagnosis if a rule fires; None hands off to the LLM agent.

    Detection sources used (in order of reliability):
      1. SLURM job state (scheduler_state from sacct via SLURMClient)
      2. HTCondor event code / hold reason (scheduler_reason)
      3. Exit signal / exit code
      4. stderr pattern match
      5. Wall-time anomaly
    """

    # ── OUT_OF_MEMORY ─────────────────────────────────────────────────────────
    oom_signals: list[str] = []
    ev: list[str] = []
    if ctx.exit_code in OOM_EXIT_CODES:
        oom_signals.append("exit_137")
        ev.append(f"exit_code:{ctx.exit_code}")
    if ctx.termination_signal in OOM_SIGNALS and ctx.exit_code not in OOM_EXIT_CODES:
        # signal 9 without exit 137 may be node failure — handled separately below
        pass
    if _match(ctx.scheduler_reason, OOM_HOLD_PATTERNS):
        oom_signals.append("scheduler_oom_hold")
        ev.append("scheduler_reason:oom")
    mem_near = _memory_near_limit(ctx)
    if mem_near:
        ev.append("memory_near_limit")
    if len(oom_signals) >= 1:
        confidence = 0.97 if (len(oom_signals) >= 2 or mem_near) else 0.90
        return _diag(FailureType.OUT_OF_MEMORY, confidence, ev,
                     f"OOM signals: {', '.join(oom_signals)}. Memory near limit: {mem_near}.",
                     "RESOURCE_MEMORY")

    # ── PREEMPTED ─────────────────────────────────────────────────────────────
    # Detection: SLURM state=PREEMPTED, exit signal 15 / exit code 143,
    # or scheduler reason contains "preempt"
    ev = []
    slurm_state = (ctx.scheduler_state or "").upper()
    if slurm_state in PREEMPTED_SCHEDULER_STATES:
        ev.append(f"slurm_state:{slurm_state}")
    if ctx.exit_code in PREEMPTED_EXIT_CODES:
        ev.append(f"exit_code:{ctx.exit_code}")
    if ctx.termination_signal in PREEMPTED_SIGNALS:
        ev.append(f"signal:{ctx.termination_signal}")
    if _match(ctx.scheduler_reason, PREEMPTED_PATTERNS):
        ev.append("scheduler_reason:preempt")
    if ev:
        return _diag(FailureType.PREEMPTED, 0.95, ev,
                     "Job was preempted by a higher-priority job.",
                     "RETRY_NO_CHANGE")

    # ── NODE_FAILURE ──────────────────────────────────────────────────────────
    # Detection: SLURM state=NODE_FAIL, signal 9 without exit 137 (not OOM),
    # or scheduler reason mentions node failure
    ev = []
    if slurm_state in NODE_FAILURE_SCHEDULER_STATES:
        ev.append(f"slurm_state:{slurm_state}")
    if ctx.termination_signal == 9 and ctx.exit_code not in OOM_EXIT_CODES:
        ev.append("signal_9_no_oom")
    if _match(ctx.scheduler_reason, NODE_FAILURE_PATTERNS):
        ev.append("scheduler_reason:node_fail")
    if _match(ctx.stderr_excerpt, NODE_FAILURE_PATTERNS):
        ev.append("stderr_node_fail")
    if ev:
        return _diag(FailureType.NODE_FAILURE, 0.93, ev,
                     "Hardware/node failure killed the job.",
                     "RETRY_NO_CHANGE")

    # ── DISK_EXCEEDED ─────────────────────────────────────────────────────────
    # Only fires when the Pegasus disk slot (request_disk) is the culprit.
    # Distinguished from SCRATCH_FULL by disk_near_limit (slot usage tracked).
    ev = []
    if _match(ctx.scheduler_reason, DISK_HOLD_PATTERNS):
        ev.append("scheduler_reason:disk")
    disk_near = _disk_near_limit(ctx)
    if disk_near:
        ev.append("disk_near_limit")
    if _match(ctx.stderr_excerpt, DISK_STDERR_PATTERNS) and not _scratch_path_in_stderr(ctx):
        ev.append("stderr_disk_error")
    if ev:
        return _diag(FailureType.DISK_EXCEEDED, 0.95, ev,
                     f"Disk slot exhausted: {ev}.",
                     "RESOURCE_DISK")

    # ── SCRATCH_FULL ──────────────────────────────────────────────────────────
    # Same ENOSPC stderr but on /tmp, /scratch, or TMPDIR — not the slot.
    ev = []
    if _match(ctx.stderr_excerpt, DISK_STDERR_PATTERNS) and _scratch_path_in_stderr(ctx):
        ev.append("stderr_scratch_enospc")
    if ev:
        return _diag(FailureType.SCRATCH_FULL, 0.92, ev,
                     "Physical scratch/tmp filesystem is full (not the Pegasus disk slot).",
                     "RESOURCE_SCRATCH")

    # ── WALLTIME_EXCEEDED ─────────────────────────────────────────────────────
    ev = []
    if _match(ctx.scheduler_reason, WALLTIME_PATTERNS):
        ev.append("scheduler_reason:walltime")
    rt_near = _runtime_near_limit(ctx)
    if rt_near:
        ev.append("runtime_near_limit")
    if slurm_state == "TIMEOUT":
        ev.append("slurm_state:TIMEOUT")
    if ev:
        return _diag(FailureType.WALLTIME_EXCEEDED, 0.93, ev,
                     "Wall-time/runtime limit exceeded.",
                     "RESOURCE_RUNTIME")

    # ── STAGING_FAILURE ───────────────────────────────────────────────────────
    # Detection: transformation name is a stage_in/stage_out job, OR
    # stderr from pegasus-transfer contains transfer error patterns.
    ev = []
    if _transformation_is_staging(ctx):
        ev.append("transformation:staging")
    if _match(ctx.stderr_excerpt, STAGING_STDERR_PATTERNS):
        ev.append("stderr_transfer_error")
    if ev:
        return _diag(FailureType.STAGING_FAILURE, 0.94, ev,
                     "pegasus-transfer staging I/O failure.",
                     "RETRY_NO_CHANGE")

    # ── MPI_FAILURE ───────────────────────────────────────────────────────────
    # Detection: stderr contains MPI abort/fatal patterns.
    ev = []
    if _match(ctx.stderr_excerpt, MPI_PATTERNS):
        ev.append("stderr_mpi_abort")
    if ev:
        return _diag(FailureType.MPI_FAILURE, 0.91, ev,
                     "MPI rank communication failure (often transient).",
                     "RETRY_NO_CHANGE")

    # ── LICENSE_UNAVAILABLE ───────────────────────────────────────────────────
    # Detection: stderr mentions license server + wall time suspiciously short.
    ev = []
    if _match(ctx.stderr_excerpt, LICENSE_PATTERNS):
        ev.append("stderr_license_error")
    if _wall_time_short(ctx) and ev:
        ev.append("wall_time_short")
    if ev:
        return _diag(FailureType.LICENSE_UNAVAILABLE, 0.90, ev,
                     "Software license checkout failed.",
                     "RETRY_OR_SITE")

    # ── CONTAINER_PULL_FAILED ────────────────────────────────────────────────
    # Detection: stderr from Docker/Singularity pull; typically exit code 255.
    ev = []
    if _match(ctx.stderr_excerpt, CONTAINER_PATTERNS):
        ev.append("stderr_container_pull")
    if ctx.exit_code == 255 and ev:
        ev.append("exit_255")
    if ev:
        return _diag(FailureType.CONTAINER_PULL_FAILED, 0.92, ev,
                     "Container image pull failed (registry unreachable or image missing).",
                     "HUMAN_REVIEW")

    # ── MODULE_NOT_FOUND ─────────────────────────────────────────────────────
    # Detection: exit code 127 (command not found) + stderr module pattern.
    ev = []
    if _match(ctx.stderr_excerpt, MODULE_PATTERNS):
        ev.append("stderr_module_not_found")
    if ctx.exit_code == 127:
        ev.append("exit_127")
    if ev:
        confidence = 0.93 if len(ev) >= 2 else 0.85
        return _diag(FailureType.MODULE_NOT_FOUND, confidence, ev,
                     "Required module or binary not available on execution site.",
                     "HUMAN_REVIEW")

    # ── GPU_UNAVAILABLE ───────────────────────────────────────────────────────
    ev = []
    if _match(ctx.stderr_excerpt, GPU_PATTERNS):
        ev.append("stderr_gpu_error")
    if _wall_time_short(ctx) and ev:
        ev.append("wall_time_short")
    if ev:
        return _diag(FailureType.GPU_UNAVAILABLE, 0.90, ev,
                     "GPU not available or failed to initialise.",
                     "RETRY_OR_SITE")

    # ── DATA_CORRUPTION ───────────────────────────────────────────────────────
    # Detection: pegasus-integrity checksum failure or corruption patterns.
    ev = []
    if _match(ctx.stderr_excerpt, DATA_CORRUPTION_PATTERNS):
        ev.append("stderr_corruption")
    if ev:
        return _diag(FailureType.DATA_CORRUPTION, 0.91, ev,
                     "Input data failed integrity/checksum check.",
                     "DATA_RESTAGE")

    # ── SSH_AUTH_FAILURE ─────────────────────────────────────────────────────
    # Detection: exit 255 + SSH error patterns (grid universe / remote site).
    ev = []
    if _match(ctx.stderr_excerpt, SSH_PATTERNS):
        ev.append("stderr_ssh_auth")
    if ctx.exit_code == 255 and ev:
        ev.append("exit_255")
    if ev:
        return _diag(FailureType.SSH_AUTH_FAILURE, 0.92, ev,
                     "SSH authentication or connectivity failure to remote site.",
                     "HUMAN_REVIEW")

    # ── ACCOUNT_EXPIRED ───────────────────────────────────────────────────────
    # Detection: scheduler rejection with account/allocation keywords
    # AND very short (or zero) wall time (job never ran).
    ev = []
    if _match(ctx.scheduler_reason, ACCOUNT_EXPIRED_PATTERNS):
        ev.append("scheduler_reason:account_expired")
    if _match(ctx.stderr_excerpt, ACCOUNT_EXPIRED_PATTERNS):
        ev.append("stderr_account_expired")
    if ev and _wall_time_short(ctx, threshold=10.0):
        ev.append("wall_time_near_zero")
    if len(ev) >= 2:
        return _diag(FailureType.ACCOUNT_EXPIRED, 0.93, ev,
                     "HPC allocation or account has expired — job immediately rejected.",
                     "HUMAN_REVIEW")

    # ── TRANSIENT_INFRASTRUCTURE ─────────────────────────────────────────────
    if _match(ctx.scheduler_reason, TRANSIENT_PATTERNS):
        return _diag(FailureType.TRANSIENT_INFRASTRUCTURE, 0.90,
                     ["scheduler_reason:transient"],
                     "Transient infrastructure failure (network, eviction).",
                     "RETRY_OR_SITE")

    # ── SCHEDULER_ADMISSION ───────────────────────────────────────────────────
    if _match(ctx.scheduler_reason, SCHEDULER_ADMISSION_PATTERNS):
        return _diag(FailureType.SCHEDULER_ADMISSION, 0.92,
                     ["scheduler_reason:admission"],
                     "Scheduler admission failure (invalid queue, QoS, or account).",
                     "SCHEDULER_CONFIGURATION")

    # ── MISSING_INPUT ─────────────────────────────────────────────────────────
    failed = [c for c in ctx.input_checks if not c.exists or not c.accessible]
    if failed:
        return _diag(FailureType.MISSING_INPUT, 0.95,
                     [f"input:{c.logical_filename}" for c in failed],
                     f"Missing or inaccessible input files: {[c.logical_filename for c in failed]}.",
                     "DATA_BINDING")

    # No rule fired → LLM agent
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _diag(
    ftype: FailureType,
    confidence: float,
    evidence_ids: list[str],
    explanation: str,
    fix_category: str,
) -> Diagnosis:
    return Diagnosis(
        failure_type=ftype,
        confidence=confidence,
        evidence_ids=evidence_ids,
        explanation=explanation,
        recommended_fix_category=fix_category,
        source="RULE",
        rule_version=RULE_VERSION,
    )


def _match(text: str | None, patterns: list[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(re.search(p, t) for p in patterns)


def _scratch_path_in_stderr(ctx: FailureContext) -> bool:
    """True when the ENOSPC error mentions a scratch/tmp path, not the slot dir."""
    return _match(ctx.stderr_excerpt, SCRATCH_PATH_PATTERNS)


def _transformation_is_staging(ctx: FailureContext) -> bool:
    if not ctx.transformation:
        return False
    t = ctx.transformation.lower()
    return any(s in t for s in STAGING_TRANSFORMATIONS)


def _wall_time_short(ctx: FailureContext, threshold: float = 60.0) -> bool:
    """True when the job ran for less than ``threshold`` seconds."""
    rt = ctx.measured_resources.runtime_seconds
    return bool(rt is not None and rt < threshold and ctx.exit_code != 0)


def _memory_near_limit(ctx: FailureContext, threshold: float = 0.85) -> bool:
    peak = ctx.measured_resources.peak_memory_mb
    req = ctx.requested_resources.memory_mb
    return bool(peak and req and peak >= req * threshold)


def _disk_near_limit(ctx: FailureContext, threshold: float = 0.90) -> bool:
    used = ctx.measured_resources.disk_used_mb
    req = ctx.requested_resources.disk_mb
    return bool(used and req and used >= req * threshold)


def _runtime_near_limit(ctx: FailureContext, threshold: float = 0.95) -> bool:
    rt = ctx.measured_resources.runtime_seconds
    req = ctx.requested_resources.runtime_seconds
    return bool(rt and req and rt >= req * threshold)
