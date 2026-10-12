from __future__ import annotations

"""
SLURM implementation of SchedulerClient.

Wraps: squeue / scontrol / sacct / scancel
"""

import subprocess
from typing import Any

import structlog

from Pegasus.healer.scheduler.base import JobHistory, JobState, JobStatus, PartitionInfo

log = structlog.get_logger(__name__)

# SLURM job state string → our JobState
_STATUS_MAP: dict[str, JobState] = {
    "PENDING":       "idle",
    "RUNNING":       "running",
    "SUSPENDED":     "running",
    "COMPLETING":    "running",
    "COMPLETED":     "completed",
    "FAILED":        "completed",
    "CANCELLED":     "removed",
    "TIMEOUT":       "completed",
    "NODE_FAIL":     "completed",
    "PREEMPTED":     "completed",
    "OUT_OF_MEMORY": "completed",
    "REQUEUED":      "idle",
    "RESIZING":      "running",
}

# FixProposal config key → scontrol update field
_RESOURCE_ATTR: dict[str, str] = {
    "memory_mb": "MinMemoryNode",
    "cpus":      "NumCPUs",
    # disk_mb has no direct SLURM equivalent — skipped
    # runtime_seconds handled specially (minutes conversion)
}


def _run(cmd: list[str], timeout: int = 20) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""


class SLURMClient:
    """SchedulerClient backed by SLURM CLI tools."""

    def get_job_status(self, cluster_id: str) -> JobStatus | None:
        out = _run([
            "squeue", "-j", cluster_id,
            "--format=%T|%R|%m|%C|%M",
            "--noheader",
        ])
        line = out.strip()
        if not line:
            return None
        parts = line.split("|")
        slurm_state = parts[0].strip() if parts else "UNKNOWN"
        reason      = parts[1].strip() if len(parts) > 1 else None
        state: JobState = _STATUS_MAP.get(slurm_state, "unknown")
        return JobStatus(
            state=state,
            hold_reason=reason if reason and reason != "None" else None,
            raw={"slurm_state": slurm_state, "reason": reason},
        )

    def update_job_resources(self, cluster_id: str, resources: dict[str, Any]) -> bool:
        updates: list[str] = []
        for key, value in resources.items():
            if value is None:
                continue
            if key == "memory_mb":
                updates.append(f"MinMemoryNode={int(value)}M")
            elif key == "cpus":
                updates.append(f"NumCPUs={int(value)}")
            elif key == "runtime_seconds":
                # SLURM TimeLimit is in minutes
                minutes = max(1, int(value) // 60)
                updates.append(f"TimeLimit={minutes}")
            # disk_mb: no SLURM equivalent — skip silently

        if not updates:
            return True
        try:
            cmd = ["scontrol", "update", f"JobId={cluster_id}"] + updates
            subprocess.run(cmd, capture_output=True, text=True, timeout=15, check=True)
            log.info("scontrol_update_ok", cluster_id=cluster_id, updates=updates)
            return True
        except (FileNotFoundError, subprocess.CalledProcessError,
                subprocess.TimeoutExpired) as exc:
            log.warning("scontrol_update_failed", cluster_id=cluster_id,
                        updates=updates, error=str(exc))
            return False

    def get_job_history(self, cluster_id: str) -> JobHistory:
        out = _run([
            "sacct", "-j", cluster_id,
            "--format=JobID,ExitCode,MaxRSS,MaxDiskRead,ElapsedRaw,State",
            "--noheader", "--parsable2",
        ])
        lines = [ln for ln in out.strip().splitlines()
                 if ln.strip() and "batch" not in ln.lower()]
        if not lines:
            return JobHistory()

        fields = lines[0].split("|")
        # ExitCode format: "code:signal"
        exit_raw = fields[1] if len(fields) > 1 else "0:0"
        parts = exit_raw.split(":")
        exit_code   = int(parts[0]) if parts[0].isdigit() else None
        exit_signal = (
            int(parts[1])
            if len(parts) > 1 and parts[1].isdigit() and parts[1] != "0"
            else None
        )

        # MaxRSS in KB (sacct default) → MB
        max_rss_kb = fields[2].rstrip("K") if len(fields) > 2 else ""
        memory_mb: int | None = None
        try:
            memory_mb = int(max_rss_kb) // 1024
        except ValueError:
            pass

        elapsed_raw = fields[4] if len(fields) > 4 else ""
        wall_time: float | None = None
        try:
            wall_time = float(elapsed_raw)
        except ValueError:
            pass

        return JobHistory(
            exit_code=exit_code,
            exit_signal=exit_signal,
            memory_usage_mb=memory_mb,
            wall_time_seconds=wall_time,
            raw={"sacct_line": lines[0]},
        )

    def cancel_job(self, cluster_id: str) -> bool:
        try:
            subprocess.run(
                ["scancel", cluster_id],
                capture_output=True, text=True, timeout=15, check=True,
            )
            log.info("scancel_ok", cluster_id=cluster_id)
            return True
        except (FileNotFoundError, subprocess.CalledProcessError,
                subprocess.TimeoutExpired) as exc:
            log.warning("scancel_failed", cluster_id=cluster_id, error=str(exc))
            return False

    # ── Partition / queue introspection ───────────────────────────────────────

    def get_partition_info(self, partition_name: str) -> PartitionInfo | None:
        """
        Query SLURM for a single partition's resource limits.

        Runs ``scontrol show partition <name>`` and parses:
          - State (UP → accessible)
          - MaxMemPerNode (MB, UNLIMITED → None)
          - DefMemPerNode (MB, UNLIMITED → None)
          - MaxTime (D-HH:MM:SS, UNLIMITED → None seconds)

        Returns None when the partition does not exist or scontrol is unavailable.
        All subprocess I/O goes through the module-level ``_run`` wrapper.
        """
        out = _run(["scontrol", "show", "partition", partition_name], timeout=15)
        if not out.strip():
            log.warning("scontrol_partition_not_found", partition=partition_name)
            return None

        raw: dict[str, str] = {}
        for token in out.split():
            if "=" in token:
                k, _, v = token.partition("=")
                raw[k.strip()] = v.strip()

        state = raw.get("State", "").upper()
        accessible = state == "UP"

        return PartitionInfo(
            name=partition_name,
            max_mem_mb=_parse_mem_mb(raw.get("MaxMemPerNode", "")),
            default_mem_mb=_parse_mem_mb(raw.get("DefMemPerNode", "")),
            max_time_seconds=_parse_slurm_time(raw.get("MaxTime", "")),
            accessible=accessible,
            raw=raw,
        )

    def list_partitions(self) -> list[PartitionInfo]:
        """
        List all SLURM partitions visible to the current user.

        Runs ``sinfo --noheader --format=%P|%a`` to enumerate partition names
        and availability, then calls ``get_partition_info`` for each to fill
        in resource limits.  Returns an empty list when sinfo is unavailable.

        All subprocess I/O goes through the module-level ``_run`` wrapper.
        """
        out = _run([
            "sinfo", "--noheader", "--format=%P|%a", "--summarize",
        ], timeout=20)
        if not out.strip():
            log.warning("sinfo_no_output")
            return []

        seen: dict[str, bool] = {}   # name → up
        for line in out.strip().splitlines():
            parts = line.strip().split("|")
            if len(parts) < 2:
                continue
            name = parts[0].strip().rstrip("*")   # strip default-partition marker
            avail = parts[1].strip().lower()
            if name and name not in seen:
                seen[name] = avail == "up"

        partitions: list[PartitionInfo] = []
        for name, up in seen.items():
            info = self.get_partition_info(name)
            if info is not None:
                info.accessible = up and info.accessible
                partitions.append(info)
            else:
                # scontrol failed but sinfo saw it — create a minimal entry
                partitions.append(PartitionInfo(name=name, accessible=up))

        log.info("slurm_partitions_listed", count=len(partitions))
        return partitions


# ── SLURM time/memory parsers (module-level, used by SLURMClient) ─────────────

def _parse_slurm_time(time_str: str) -> int | None:
    """Parse a SLURM time-limit string (D-HH:MM:SS / UNLIMITED) to seconds."""
    s = time_str.strip().upper()
    if not s or s in ("UNLIMITED", "INFINITE", "N/A"):
        return None
    try:
        if "-" in s:
            days_part, rest = s.split("-", 1)
            days = int(days_part)
        else:
            days, rest = 0, s
        parts = rest.split(":")
        if len(parts) == 3:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            h, m, sec = 0, int(parts[0]), int(parts[1])
        else:
            return None
        return days * 86400 + h * 3600 + m * 60 + sec
    except (ValueError, IndexError):
        return None


def _parse_mem_mb(mem_str: str) -> int | None:
    """Parse a SLURM memory value (raw MB or UNLIMITED) to int MB."""
    s = mem_str.strip().upper()
    if not s or s in ("UNLIMITED", "INFINITE", "N/A"):
        return None
    try:
        return int(s)
    except ValueError:
        return None
