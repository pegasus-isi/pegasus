from __future__ import annotations

"""
HTCondor implementation of SchedulerClient.

Wraps: condor_q / condor_qedit / condor_history / condor_rm
"""

import subprocess
from typing import Any

import structlog

from Pegasus.healer.scheduler.base import JobHistory, JobState, JobStatus

log = structlog.get_logger(__name__)

# HTCondor JobStatus integer → our JobState
_STATUS_MAP: dict[int, JobState] = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "running",    # transferring output
    7: "running",    # suspended
}

# FixProposal config key → HTCondor ClassAd attribute name
_RESOURCE_ATTR: dict[str, str] = {
    "memory_mb":       "RequestMemory",
    "disk_mb":         "RequestDisk",
    "cpus":            "RequestCpus",
    "runtime_seconds": "MaxRuntime",
}


def _run(cmd: list[str], timeout: int = 20) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""


def _parse_classads(text: str) -> dict[str, Any]:
    """Parse HTCondor long-form classad output (key = value per line)."""
    result: dict[str, Any] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"')
        try:
            result[key] = int(val)
        except ValueError:
            try:
                result[key] = float(val)
            except ValueError:
                result[key] = val
    return result


class HTCondorClient:
    """SchedulerClient backed by HTCondor CLI tools."""

    def get_job_status(self, cluster_id: str) -> JobStatus | None:
        out = _run(["condor_q", cluster_id, "-long"])
        if not out.strip():
            return None
        ads = _parse_classads(out)
        raw_status = ads.get("JobStatus")
        state: JobState = (
            _STATUS_MAP.get(int(raw_status), "unknown")
            if raw_status is not None else "unknown"
        )
        return JobStatus(
            state=state,
            exit_code=ads.get("ExitCode"),
            exit_signal=ads.get("ExitSignal") if ads.get("ExitBySignal") else None,
            memory_usage_mb=ads.get("MemoryUsage"),
            disk_usage_mb=ads.get("DiskUsage"),
            wall_time_seconds=ads.get("RemoteWallClockTime"),
            hold_reason=ads.get("HoldReason"),
            raw=ads,
        )

    def update_job_resources(self, cluster_id: str, resources: dict[str, Any]) -> bool:
        success = True
        for key, value in resources.items():
            if value is None:
                continue
            attr = _RESOURCE_ATTR.get(key)
            if attr is None:
                continue
            try:
                subprocess.run(
                    ["condor_qedit", cluster_id, attr, str(int(value))],
                    capture_output=True, text=True, timeout=15, check=True,
                )
                log.info("condor_qedit_ok", cluster_id=cluster_id, attr=attr, value=value)
            except (FileNotFoundError, subprocess.CalledProcessError,
                    subprocess.TimeoutExpired) as exc:
                log.warning("condor_qedit_failed", cluster_id=cluster_id,
                            attr=attr, error=str(exc))
                success = False
        return success

    def get_job_history(self, cluster_id: str) -> JobHistory:
        out = _run(["condor_history", cluster_id, "-long"])
        if not out.strip():
            return JobHistory()
        ads = _parse_classads(out)
        exit_code  = ads.get("ExitCode")
        exit_sig   = ads.get("ExitSignal") if ads.get("ExitBySignal") else None
        hold_code  = ads.get("HoldReasonCode")
        return JobHistory(
            exit_code=int(exit_code)  if exit_code  is not None else None,
            exit_signal=int(exit_sig) if exit_sig    is not None else None,
            memory_usage_mb=ads.get("MemoryUsage"),
            disk_usage_mb=ads.get("DiskUsage"),
            wall_time_seconds=ads.get("RemoteWallClockTime"),
            hold_reason=ads.get("HoldReason"),
            hold_reason_code=int(hold_code) if hold_code is not None else None,
            raw=ads,
        )

    def cancel_job(self, cluster_id: str) -> bool:
        try:
            subprocess.run(
                ["condor_rm", cluster_id],
                capture_output=True, text=True, timeout=15, check=True,
            )
            log.info("condor_rm_ok", cluster_id=cluster_id)
            return True
        except (FileNotFoundError, subprocess.CalledProcessError,
                subprocess.TimeoutExpired) as exc:
            log.warning("condor_rm_failed", cluster_id=cluster_id, error=str(exc))
            return False
