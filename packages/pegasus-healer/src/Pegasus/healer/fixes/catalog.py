from __future__ import annotations

import math
import uuid

from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis, FailureType
from Pegasus.healer.models.fixes import FixAction, FixProposal
from Pegasus.healer.policies.loader import PolicyConfig


def lookup(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    policy: PolicyConfig,
) -> FixProposal | None:
    """
    Return a known FixProposal for a known failure type.

    Returns None when the failure is too complex for a catalog entry —
    the caller should then invoke the LLM fix-planning agent.
    """
    ftype = diagnosis.failure_type

    # ── Resource fixes ────────────────────────────────────────────────────────
    if ftype == FailureType.OUT_OF_MEMORY:
        return _oom_fix(diagnosis, ctx, policy)
    if ftype == FailureType.DISK_EXCEEDED:
        return _disk_fix(diagnosis, ctx, policy)
    if ftype == FailureType.WALLTIME_EXCEEDED:
        return _walltime_fix(diagnosis, ctx, policy)

    # ── Simple retry fixes (no config change needed) ──────────────────────────
    if ftype in (
        FailureType.PREEMPTED,
        FailureType.NODE_FAILURE,
        FailureType.STAGING_FAILURE,
        FailureType.MPI_FAILURE,
        FailureType.TRANSIENT_INFRASTRUCTURE,
    ):
        return _retry_fix(diagnosis, ctx, ftype)

    # ── Fixes that require human decision (ASK) ───────────────────────────────
    if ftype == FailureType.MISSING_INPUT:
        return _missing_input_fix(diagnosis, ctx)
    if ftype == FailureType.DATA_CORRUPTION:
        return _data_corruption_fix(diagnosis, ctx)
    if ftype in (
        FailureType.LICENSE_UNAVAILABLE,
        FailureType.GPU_UNAVAILABLE,
    ):
        return _ask_fix(diagnosis, ctx, ftype)

    # ── ESCALATE — human must intervene ──────────────────────────────────────
    if ftype in (
        FailureType.ACCOUNT_EXPIRED,
        FailureType.CONTAINER_PULL_FAILED,
        FailureType.MODULE_NOT_FOUND,
        FailureType.SSH_AUTH_FAILURE,
        FailureType.SCRATCH_FULL,
    ):
        return _escalate_fix(diagnosis, ctx, ftype)

    # APPLICATION_ERROR, LOGIC_VALIDATION, UNKNOWN, SCHEDULER_ADMISSION → LLM
    return None


# ── Per-type builders ─────────────────────────────────────────────────────────

def _oom_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "OUT_OF_MEMORY"
    multiplier = policy.get_multiplier(ft)
    max_mem = policy.get_ceiling(ft, "maximum_memory_mb") or 32768
    current = ctx.requested_resources.memory_mb or 4096
    proposed = min(math.ceil(current * multiplier), max_mem)

    # ── SLURM partition ceiling check ─────────────────────────────────────────
    # When the job ran on SLURM and we know the partition profile, verify that
    # the proposed memory fits within the current partition's MaxMemPerNode.
    # If it exceeds the ceiling, migrate to the smallest accessible partition
    # that can hold the requested amount instead of patching memory in-place.
    current_partition = ctx.current_partition
    if current_partition and ctx.partition_profile:
        partition_map = {p["name"]: p for p in ctx.partition_profile}
        current_info = partition_map.get(current_partition, {})
        current_ceiling = current_info.get("max_mem_mb")  # None = unlimited

        if current_ceiling is not None and proposed > current_ceiling:
            # Proposed memory exceeds partition ceiling — need to migrate
            target = _find_target_partition(
                needed_mb=proposed,
                current_partition=current_partition,
                partition_profile=ctx.partition_profile,
            )
            if target:
                return _partition_migration_fix(
                    diagnosis, ctx,
                    proposed_memory_mb=proposed,
                    current_partition=current_partition,
                    target_partition=target["name"],
                    current_ceiling_mb=current_ceiling,
                )
            # No partition can fit — escalate (caller will handle ESCALATE)
            return FixProposal(
                fix_id=uuid.uuid4(),
                incident_id=ctx.incident_id,
                action=FixAction.HUMAN_REVIEW,
                parameters={},
                old_configuration={"memory_mb": current},
                proposed_configuration={},
                justification=(
                    f"OOM: proposed {proposed} MB exceeds partition '{current_partition}' "
                    f"ceiling of {current_ceiling} MB and no larger accessible partition "
                    f"was found. Human intervention required."
                ),
                evidence_ids=diagnosis.evidence_ids,
                confidence=diagnosis.confidence,
                source="RULE",
                requires_approval=True,
            )

    # ── In-place memory bump (fits current partition or no SLURM context) ─────
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_MEMORY,
        parameters={"multiplier": multiplier, "maximum_memory_mb": max_mem},
        old_configuration={"memory_mb": current},
        proposed_configuration={"memory_mb": proposed},
        justification=(
            f"OOM diagnosis (confidence={diagnosis.confidence:.2f}). "
            f"Increasing memory {current} MB → {proposed} MB (×{multiplier})."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _find_target_partition(
    needed_mb: int,
    current_partition: str,
    partition_profile: list[dict],
) -> dict | None:
    """
    Select the smallest accessible partition that fits needed_mb.

    Uses only the serialised partition profile dicts already in the context —
    no direct scheduler calls.  All scheduler I/O happened earlier in
    collect_partition_profile() via PegasusClient.
    """
    candidates = [
        p for p in partition_profile
        if p.get("accessible", True)
        and p["name"] != current_partition
        and (p.get("max_mem_mb") is None or p["max_mem_mb"] >= needed_mb)
    ]
    if not candidates:
        return None
    # Prefer smallest ceiling that still fits (cheapest resource tier)
    with_ceiling = [p for p in candidates if p.get("max_mem_mb") is not None]
    unlimited = [p for p in candidates if p.get("max_mem_mb") is None]
    if with_ceiling:
        return min(with_ceiling, key=lambda p: p["max_mem_mb"])
    return unlimited[0] if unlimited else None


def _partition_migration_fix(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    proposed_memory_mb: int,
    current_partition: str,
    target_partition: str,
    current_ceiling_mb: int,
) -> FixProposal:
    """
    Build a MIGRATE_PARTITION fix proposal.

    Patches both request_memory and batch_queue / +remote_queue in the .sub
    file so the job is resubmitted to the target partition with the higher
    memory allocation.  Always requires approval (ASK) because partition
    migration may affect cost, priority, and fair-share accounting.
    """
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.MIGRATE_PARTITION,
        parameters={
            "current_partition": current_partition,
            "target_partition": target_partition,
            "current_ceiling_mb": current_ceiling_mb,
        },
        old_configuration={
            "memory_mb": ctx.requested_resources.memory_mb,
            "target_partition": current_partition,
        },
        proposed_configuration={
            "memory_mb": proposed_memory_mb,
            "target_partition": target_partition,
        },
        justification=(
            f"OOM diagnosis (confidence={diagnosis.confidence:.2f}). "
            f"Proposed {proposed_memory_mb} MB exceeds '{current_partition}' "
            f"ceiling of {current_ceiling_mb} MB. "
            f"Migrating to partition '{target_partition}' and setting "
            f"memory to {proposed_memory_mb} MB."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,   # partition migration always needs ASK
    )


def _disk_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "DISK_EXCEEDED"
    multiplier = policy.get_multiplier(ft)
    max_disk = policy.get_ceiling(ft, "maximum_disk_mb") or 102400
    current = ctx.requested_resources.disk_mb or 10240
    proposed = min(math.ceil(current * multiplier), max_disk)

    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_DISK,
        parameters={"multiplier": multiplier},
        old_configuration={"disk_mb": current},
        proposed_configuration={"disk_mb": proposed},
        justification=(
            f"Disk exceeded (confidence={diagnosis.confidence:.2f}). "
            f"Increasing disk {current} MB → {proposed} MB."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _walltime_fix(diagnosis: Diagnosis, ctx: FailureContext, policy: PolicyConfig) -> FixProposal:
    ft = "WALLTIME_EXCEEDED"
    multiplier = policy.get_multiplier(ft)
    max_rt = policy.get_ceiling(ft, "maximum_runtime_seconds") or 86400
    current = ctx.requested_resources.runtime_seconds or 3600
    proposed = min(int(current * multiplier), max_rt)

    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.INCREASE_RUNTIME,
        parameters={"multiplier": multiplier},
        old_configuration={"runtime_seconds": current},
        proposed_configuration={"runtime_seconds": proposed},
        justification=(
            f"Walltime exceeded (confidence={diagnosis.confidence:.2f}). "
            f"Increasing runtime {current} s → {proposed} s."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,  # ASK per spec
    )


def _transient_fix(diagnosis: Diagnosis, ctx: FailureContext) -> FixProposal:
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.NO_ACTION_TRANSIENT_RETRY,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"Transient infrastructure failure (confidence={diagnosis.confidence:.2f}). "
            "Retrying without configuration change."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _retry_fix(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    ftype: FailureType,
) -> FixProposal:
    """Pure retry — no configuration change, just let DAGMan retry the job."""
    labels = {
        FailureType.PREEMPTED:               "Preempted by higher-priority job",
        FailureType.NODE_FAILURE:            "Hardware/node failure",
        FailureType.STAGING_FAILURE:         "pegasus-transfer staging failure",
        FailureType.MPI_FAILURE:             "MPI rank communication failure",
        FailureType.TRANSIENT_INFRASTRUCTURE: "Transient infrastructure failure",
    }
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.NO_ACTION_TRANSIENT_RETRY,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"{labels.get(ftype, str(ftype))} "
            f"(confidence={diagnosis.confidence:.2f}). "
            "Retrying without configuration change."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=False,
    )


def _ask_fix(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    ftype: FailureType,
) -> FixProposal:
    """Failure requires human decision — write ASK proposal."""
    labels = {
        FailureType.LICENSE_UNAVAILABLE: (
            "Software license unavailable. Retry on a different site with the "
            "required license, or wait and resubmit when a license is free."
        ),
        FailureType.GPU_UNAVAILABLE: (
            "GPU not available or failed to initialise. Retry on a site with "
            "available GPUs or reduce the GPU request."
        ),
    }
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.RETRY_DIFFERENT_SITE,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=labels.get(ftype, f"{ftype} requires human decision."),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,
    )


def _escalate_fix(
    diagnosis: Diagnosis,
    ctx: FailureContext,
    ftype: FailureType,
) -> FixProposal:
    """Failure cannot be fixed automatically — escalate to human."""
    labels = {
        FailureType.ACCOUNT_EXPIRED:      "HPC allocation exhausted — renew allocation.",
        FailureType.CONTAINER_PULL_FAILED: "Container image missing or registry unreachable.",
        FailureType.MODULE_NOT_FOUND:     "Required module not installed on execution site.",
        FailureType.SSH_AUTH_FAILURE:     "SSH authentication failure — check credentials.",
        FailureType.SCRATCH_FULL:         "Physical scratch filesystem full — free space or redirect TMPDIR.",
    }
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.HUMAN_REVIEW,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"{labels.get(ftype, str(ftype))} "
            f"(confidence={diagnosis.confidence:.2f}). "
            "Human intervention required."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,
    )


def _data_corruption_fix(diagnosis: Diagnosis, ctx: FailureContext) -> FixProposal:
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.RESTAGE_INPUT,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"Input data corruption detected (confidence={diagnosis.confidence:.2f}). "
            "Re-staging the input file from the replica catalog is required."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,
    )


def _missing_input_fix(diagnosis: Diagnosis, ctx: FailureContext) -> FixProposal:
    return FixProposal(
        fix_id=uuid.uuid4(),
        incident_id=ctx.incident_id,
        action=FixAction.CORRECT_DATA_BINDING,
        parameters={},
        old_configuration={},
        proposed_configuration={},
        justification=(
            f"Missing input files (confidence={diagnosis.confidence:.2f}). "
            "Manual data binding correction required."
        ),
        evidence_ids=diagnosis.evidence_ids,
        confidence=diagnosis.confidence,
        source="RULE",
        requires_approval=True,  # ASK per spec
    )
