from __future__ import annotations

"""
Build the appropriate SchedulerClient from environment configuration.

SCHEDULER_TYPE env var (in .env or shell):
  auto   — detect from PATH (default)
  condor — force HTCondor
  slurm  — force SLURM

Auto-detection order: condor_q → squeue → fallback HTCondor
"""

import os
import shutil

import structlog

from Pegasus.healer.scheduler.base import SchedulerClient

log = structlog.get_logger(__name__)

_client: SchedulerClient | None = None
_pegasus_client: "PegasusClient | None" = None


def build_scheduler_client() -> SchedulerClient:
    """Instantiate and return a fresh SchedulerClient."""
    scheduler_type = os.environ.get("SCHEDULER_TYPE", "auto").lower().strip()

    if scheduler_type == "condor":
        from Pegasus.healer.scheduler.condor import HTCondorClient
        log.info("scheduler_client_built", backend="htcondor", reason="SCHEDULER_TYPE=condor")
        return HTCondorClient()

    if scheduler_type == "slurm":
        from Pegasus.healer.scheduler.slurm import SLURMClient
        log.info("scheduler_client_built", backend="slurm", reason="SCHEDULER_TYPE=slurm")
        return SLURMClient()

    # Auto-detect from PATH
    if shutil.which("condor_q"):
        from Pegasus.healer.scheduler.condor import HTCondorClient
        log.info("scheduler_client_built", backend="htcondor", reason="condor_q found on PATH")
        return HTCondorClient()

    if shutil.which("squeue"):
        from Pegasus.healer.scheduler.slurm import SLURMClient
        log.info("scheduler_client_built", backend="slurm", reason="squeue found on PATH")
        return SLURMClient()

    # Fallback — Pegasus most commonly runs on HTCondor
    from Pegasus.healer.scheduler.condor import HTCondorClient
    log.warning(
        "scheduler_client_fallback",
        backend="htcondor",
        note="neither condor_q nor squeue found; defaulting to HTCondor",
    )
    return HTCondorClient()


def get_scheduler_client() -> SchedulerClient:
    """Return the module-level singleton SchedulerClient (built once)."""
    global _client
    if _client is None:
        _client = build_scheduler_client()
    return _client


def get_pegasus_client() -> "PegasusClient":
    """
    Return the module-level singleton PegasusClient (built once).

    Wraps the configured SchedulerClient (HTCondor or SLURM) and adds
    Pegasus workflow-level commands (analyze_workflow, get_workflow_status,
    remove_workflow, run_workflow).
    """
    global _pegasus_client
    if _pegasus_client is None:
        from Pegasus.healer.scheduler.pegasus import PegasusClient
        _pegasus_client = PegasusClient(get_scheduler_client())
    return _pegasus_client
