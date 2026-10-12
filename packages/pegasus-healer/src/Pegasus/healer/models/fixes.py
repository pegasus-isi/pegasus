from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class FixAction(StrEnum):
    INCREASE_MEMORY = "INCREASE_MEMORY"
    INCREASE_DISK = "INCREASE_DISK"
    INCREASE_RUNTIME = "INCREASE_RUNTIME"
    MIGRATE_PARTITION = "MIGRATE_PARTITION"              # move job to larger SLURM partition
    RETRY_DIFFERENT_SITE = "RETRY_DIFFERENT_SITE"
    CORRECT_DATA_BINDING = "CORRECT_DATA_BINDING"       # fix wrong path in .sub file
    FIX_DATA_BINDING = "FIX_DATA_BINDING"               # alias used by LLM planner
    RESTAGE_INPUT = "RESTAGE_INPUT"                      # re-transfer corrupted input file
    PATCH_TRANSFORMATION_SCRIPT = "PATCH_TRANSFORMATION_SCRIPT"  # patch local script file
    CORRECT_SCHEDULER_CONFIG = "CORRECT_SCHEDULER_CONFIG"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    NO_ACTION_TRANSIENT_RETRY = "NO_ACTION_TRANSIENT_RETRY"


class PolicyDecision(StrEnum):
    AUTO = "AUTO"
    ASK = "ASK"
    STOP = "STOP"
    ESCALATE = "ESCALATE"


class FixOutcome(StrEnum):
    EFFECTIVE = "EFFECTIVE"
    INEFFECTIVE = "INEFFECTIVE"
    PARTIALLY_EFFECTIVE = "PARTIALLY_EFFECTIVE"
    NEW_FAILURE = "NEW_FAILURE"
    INCONCLUSIVE = "INCONCLUSIVE"


class ScriptPatch(BaseModel):
    """A proposed in-place patch to a transformation script file."""
    file_path: str              # absolute path to the script on the submit host
    original_content: str       # what the file contained before patching (backup)
    patched_content: str        # what to write to the file
    patch_description: str      # human-readable description of what changed and why


class FixProposal(BaseModel):
    fix_id: UUID = Field(default_factory=uuid4)
    incident_id: UUID
    action: FixAction
    parameters: dict[str, Any] = Field(default_factory=dict)
    old_configuration: dict[str, Any] = Field(default_factory=dict)
    proposed_configuration: dict[str, Any] = Field(default_factory=dict)
    justification: str
    evidence_ids: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    source: Literal["RULE", "SKILL", "LLM", "USER"] = "RULE"
    requires_approval: bool = False
    # Script patches — populated only for PATCH_TRANSFORMATION_SCRIPT actions
    script_patches: list[ScriptPatch] = Field(default_factory=list)


class AppliedOverlay(BaseModel):
    fix_id: str
    old_config: dict[str, Any]
    new_config: dict[str, Any]
    hash_before: str
    hash_after: str

    def configs_differ(self) -> bool:
        return self.hash_before != self.hash_after


class PolicyDecisionRecord(BaseModel):
    fix_id: str
    decision: PolicyDecision
    policy_version: str
    checks_passed: list[str]
    checks_failed: list[str]
    reason: str
