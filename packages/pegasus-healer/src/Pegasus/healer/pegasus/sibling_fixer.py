from __future__ import annotations

"""
Sibling fix broadcast for Pegasus family jobs.

When a resource fix is diagnosed for one failing job, all sibling jobs that
share the same transformation AND the same resource value are patched
proactively — preventing them from hitting the same limit.

State-based strategy per sibling:
  NOT SUBMITTED  → patch .sub only
                   (DAGMan reads patched file on submission)
  IDLE / PENDING → patch .sub + condor_qedit
                   (update live ClassAd — no disruption, no condor_rm)
  RUNNING        → patch .sub only
                   (let it run; patched .sub used on retry if it fails)
  FAILED / HELD  → skip
                   (handled by their own POST script + marker fast path)

Marker file
───────────
  {submit_dir}/.healer_{transformation}_{failure_type}.applied

Written atomically by the first POST script to handle this family.
Subsequent POST scripts for the same family read it to skip agent diagnosis
(marker fast path: fix already applied → just retry).

Marker format:
  {
    "transformation":  "mifaser",
    "failure_type":    "OUT_OF_MEMORY",
    "original_config": {"memory_mb": 2048},
    "new_config":      {"memory_mb": 3072},
    "thread_id":       "wf-uuid/101",
    "applied_at":      "2026-09-16T14:00:00Z",
    "siblings_patched": 9
  }
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from Pegasus.healer.pegasus.sub_file import apply_overlay_to_sub_file, read_resource_requests
from Pegasus.healer.scheduler.factory import get_scheduler_client

log = structlog.get_logger(__name__)

# Exit codes reliably associated with each failure type
# Used by the fast-path check to verify the current failure matches the marker
_FAILURE_EXIT_CODES: dict[str, set[int]] = {
    "OUT_OF_MEMORY":  {137},        # SIGKILL (OOM killer)
    "DISK_EXCEEDED":  {1, 2, 28},   # ENOSPC = 28; many tools exit 1/2
    "WALLTIME_EXCEEDED": set(),     # unreliable — skip exit-code check
}


# ── Marker file ───────────────────────────────────────────────────────────────

def _marker_path(submit_dir: Path, transformation: str, failure_type: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", f"{transformation}_{failure_type}")
    return submit_dir / f".healer_{safe}.applied"


def write_marker(
    submit_dir: Path,
    transformation: str,
    failure_type: str,
    original_config: dict[str, Any],
    new_config: dict[str, Any],
    thread_id: str,
    siblings_patched: int,
) -> bool:
    """
    Atomically create the marker file.

    Uses O_CREAT | O_EXCL — only one concurrent POST script succeeds.
    Returns True if this process won the race, False if marker already existed.
    """
    path = _marker_path(submit_dir, transformation, failure_type)
    payload = {
        "transformation":   transformation,
        "failure_type":     failure_type,
        "original_config":  original_config,
        "new_config":       new_config,
        "thread_id":        thread_id,
        "applied_at":       datetime.now(timezone.utc).isoformat(),
        "siblings_patched": siblings_patched,
    }
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        log.info("marker_written", path=str(path), siblings=siblings_patched)
        return True
    except FileExistsError:
        return False


def update_marker(
    submit_dir: Path,
    transformation: str,
    failure_type: str,
    original_config: dict[str, Any],
    new_config: dict[str, Any],
    thread_id: str,
    siblings_patched: int,
) -> None:
    """Overwrite an existing marker — used when a re-attempt escalates the fix value."""
    path = _marker_path(submit_dir, transformation, failure_type)
    payload = {
        "transformation":   transformation,
        "failure_type":     failure_type,
        "original_config":  original_config,
        "new_config":       new_config,
        "thread_id":        thread_id,
        "applied_at":       datetime.now(timezone.utc).isoformat(),
        "siblings_patched": siblings_patched,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log.info("marker_updated", path=str(path), new_config=new_config)


def read_marker(
    submit_dir: Path,
    transformation: str,
    failure_type: str,
) -> dict[str, Any] | None:
    """Return the marker dict, or None if not present / unreadable."""
    path = _marker_path(submit_dir, transformation, failure_type)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def check_fast_path(
    submit_dir: Path,
    transformation: str,
    exit_code: int,
    current_sub_resources: dict[str, int | None],
) -> bool:
    """
    Return True if the marker fast path applies for this sibling:
      1. A marker exists for this transformation + failure type
      2. This job's .sub already has the patched value (fix was broadcast to it)
      3. The current failure exit code is consistent with the marker's failure type

    When True the POST script should skip the agent and exit 1 (retry).
    """
    for failure_type in ("OUT_OF_MEMORY", "DISK_EXCEEDED", "WALLTIME_EXCEEDED"):
        marker = read_marker(submit_dir, transformation, failure_type)
        if marker is None:
            continue

        # Check exit code consistency (skip if known incompatible codes)
        known_codes = _FAILURE_EXIT_CODES.get(failure_type, set())
        if known_codes and exit_code not in known_codes:
            continue

        # Verify this job's .sub already has the patched value
        new_cfg = marker.get("new_config", {})
        for config_key, expected_value in new_cfg.items():
            actual = current_sub_resources.get(config_key)
            if actual is None:
                continue
            if actual == expected_value:
                log.info(
                    "marker_fast_path",
                    failure_type=failure_type,
                    config_key=config_key,
                    patched_value=expected_value,
                )
                return True

    return False


# ── Submit-dir scanner ────────────────────────────────────────────────────────

def _parse_transformation_from_content(content: str) -> str | None:
    m = re.search(r"^\s*#\s*transformation\s*[=:]\s*(.+)$", content,
                  re.MULTILINE | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"^\s*executable\s*=\s*(.+)$", content,
                  re.MULTILINE | re.IGNORECASE)
    if m:
        return Path(m.group(1).strip()).name
    return None


def _parse_cluster_id_from_log(log_path: Path) -> str | None:
    """
    Extract the most recent HTCondor cluster ID from a job event log.
    Event type 000 = job submitted: '000 (CLUSTER.PROC.000) ...'
    """
    if not log_path.exists():
        return None
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        matches = re.findall(r"^000\s+\((\d+)\.\d+\.\d+\)", text, re.MULTILINE)
        return matches[-1] if matches else None
    except OSError:
        return None


def find_siblings(
    submit_dir: Path,
    transformation: str,
    resource_key: str,
    original_value: int,
    excluding_job_id: str,
) -> list[dict[str, Any]]:
    """
    Scan all .sub files under submit_dir for jobs that:
      - belong to the same transformation
      - still have original_value for resource_key
      - are not the primary failing job (excluding_job_id)

    Returns a list of dicts: {sub_path, job_id, cluster_id}
    """
    siblings = []

    for sub_path in sorted(submit_dir.rglob("*.sub")):
        if sub_path.name.endswith(".dag.sub"):
            continue
        try:
            content = sub_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        t = _parse_transformation_from_content(content)
        if t != transformation:
            continue

        resources = read_resource_requests(sub_path)
        if resources.get(resource_key) != original_value:
            # Different value — already healed or intentional per-job override
            continue

        job_id = sub_path.stem.split("_ID")[0]
        if job_id == excluding_job_id:
            continue

        log_path = sub_path.with_suffix(".log")
        siblings.append({
            "sub_path":   sub_path,
            "job_id":     job_id,
            "cluster_id": _parse_cluster_id_from_log(log_path),
        })

    return siblings


# ── Broadcast ─────────────────────────────────────────────────────────────────

def broadcast_fix(
    submit_dir: Path,
    transformation: str,
    failure_type: str,
    resource_key: str,
    original_config: dict[str, Any],
    new_config: dict[str, Any],
    thread_id: str,
    excluding_job_id: str,
) -> int:
    """
    Broadcast new_config to all sibling jobs of transformation that still
    have the original resource value (original_config[resource_key]).

    Per-sibling action based on HTCondor state:
      IDLE (1)  → patch .sub + condor_qedit  (runs with new resources, no stop)
      RUNNING   → patch .sub only            (retry uses new values if it fails)
      None/other → patch .sub only           (not yet submitted by DAGMan)

    Writes or updates the marker file after patching.
    Returns the count of siblings patched.
    """
    original_value = original_config.get(resource_key)
    if original_value is None:
        return 0

    scheduler = get_scheduler_client()

    siblings = find_siblings(
        submit_dir=submit_dir,
        transformation=transformation,
        resource_key=resource_key,
        original_value=original_value,
        excluding_job_id=excluding_job_id,
    )

    patched = 0
    for sib in siblings:
        sub_path: Path = sib["sub_path"]
        cluster_id: str | None = sib["cluster_id"]

        # Query current scheduler state — works for HTCondor and SLURM
        status = scheduler.get_job_status(cluster_id) if cluster_id else None

        # Always patch the .sub file (safe — file on disk, read on next submit)
        apply_overlay_to_sub_file(sub_path, new_config)
        patched += 1

        if status and status.is_idle:
            # Job is queued but not yet running — update live resource ClassAd
            scheduler.update_job_resources(cluster_id, new_config)
            log.info("sibling_idle_patched", job_id=sib["job_id"], cluster_id=cluster_id)
        elif status and status.is_running:
            log.info("sibling_running_patched", job_id=sib["job_id"],
                     note="sub patched for retry; running job continues unchanged")
        else:
            log.info("sibling_not_submitted_patched", job_id=sib["job_id"])

    # Write marker — atomic on first call, overwrite on re-attempt
    won = write_marker(submit_dir, transformation, failure_type,
                       original_config, new_config, thread_id, patched)
    if not won:
        update_marker(submit_dir, transformation, failure_type,
                      original_config, new_config, thread_id, patched)

    log.info(
        "broadcast_complete",
        transformation=transformation,
        failure_type=failure_type,
        siblings_patched=patched,
        excluding=excluding_job_id,
    )
    return patched
