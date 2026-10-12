from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class FailureType(StrEnum):
    # ── Resource exhaustion ───────────────────────────────────────────────────
    OUT_OF_MEMORY    = "OUT_OF_MEMORY"
    DISK_EXCEEDED    = "DISK_EXCEEDED"
    WALLTIME_EXCEEDED = "WALLTIME_EXCEEDED"
    SCRATCH_FULL     = "SCRATCH_FULL"      # /tmp or scratch FS physically full

    # ── Scheduler / infrastructure events ────────────────────────────────────
    PREEMPTED              = "PREEMPTED"           # job killed by higher-priority job
    NODE_FAILURE           = "NODE_FAILURE"         # hardware fault, node went down
    TRANSIENT_INFRASTRUCTURE = "TRANSIENT_INFRASTRUCTURE"
    SCHEDULER_ADMISSION    = "SCHEDULER_ADMISSION"
    ACCOUNT_EXPIRED        = "ACCOUNT_EXPIRED"      # HPC allocation exhausted
    GPU_UNAVAILABLE        = "GPU_UNAVAILABLE"      # no GPU allocated or GPU fault

    # ── Software / environment ────────────────────────────────────────────────
    STAGING_FAILURE        = "STAGING_FAILURE"      # pegasus-transfer I/O error
    LICENSE_UNAVAILABLE    = "LICENSE_UNAVAILABLE"  # software license checkout failed
    CONTAINER_PULL_FAILED  = "CONTAINER_PULL_FAILED" # Docker/Singularity image missing
    MODULE_NOT_FOUND       = "MODULE_NOT_FOUND"     # module load / binary not on PATH
    MPI_FAILURE            = "MPI_FAILURE"          # MPI rank communication failure
    SSH_AUTH_FAILURE       = "SSH_AUTH_FAILURE"     # remote site auth error

    # ── Data ─────────────────────────────────────────────────────────────────
    MISSING_INPUT    = "MISSING_INPUT"
    DATA_MISMATCH    = "DATA_MISMATCH"     # wrong format / dimensions
    DATA_CORRUPTION  = "DATA_CORRUPTION"   # checksum / integrity failure
    SCRIPT_ERROR     = "SCRIPT_ERROR"
    APPLICATION_ERROR = "APPLICATION_ERROR"
    LOGIC_VALIDATION = "LOGIC_VALIDATION"
    UNKNOWN          = "UNKNOWN"


# Map failure type → fix category string (used by agents and catalog)
FAILURE_FIX_CATEGORY: dict[FailureType, str] = {
    FailureType.OUT_OF_MEMORY:           "RESOURCE_MEMORY",
    FailureType.DISK_EXCEEDED:           "RESOURCE_DISK",
    FailureType.WALLTIME_EXCEEDED:       "RESOURCE_RUNTIME",
    FailureType.SCRATCH_FULL:            "RESOURCE_SCRATCH",
    FailureType.PREEMPTED:               "RETRY_NO_CHANGE",
    FailureType.NODE_FAILURE:            "RETRY_NO_CHANGE",
    FailureType.TRANSIENT_INFRASTRUCTURE: "RETRY_OR_SITE",
    FailureType.SCHEDULER_ADMISSION:     "SCHEDULER_CONFIGURATION",
    FailureType.ACCOUNT_EXPIRED:         "HUMAN_REVIEW",
    FailureType.GPU_UNAVAILABLE:         "RETRY_OR_SITE",
    FailureType.STAGING_FAILURE:         "RETRY_NO_CHANGE",
    FailureType.LICENSE_UNAVAILABLE:     "RETRY_OR_SITE",
    FailureType.CONTAINER_PULL_FAILED:   "HUMAN_REVIEW",
    FailureType.MODULE_NOT_FOUND:        "HUMAN_REVIEW",
    FailureType.MPI_FAILURE:             "RETRY_NO_CHANGE",
    FailureType.SSH_AUTH_FAILURE:        "HUMAN_REVIEW",
    FailureType.MISSING_INPUT:           "DATA_BINDING",
    FailureType.DATA_MISMATCH:           "DATA_CONVERSION",
    FailureType.DATA_CORRUPTION:         "DATA_RESTAGE",
    FailureType.SCRIPT_ERROR:            "SCRIPT_PATCH",
    FailureType.APPLICATION_ERROR:       "APPLICATION_DIAGNOSIS",
    FailureType.LOGIC_VALIDATION:        "HUMAN_REVIEW",
    FailureType.UNKNOWN:                 "HUMAN_REVIEW",
}


class AlternativeCause(BaseModel):
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str


class Diagnosis(BaseModel):
    failure_type: FailureType
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_ids: list[str] = Field(default_factory=list)
    explanation: str
    missing_evidence: list[str] = Field(default_factory=list)
    alternative_causes: list[AlternativeCause] = Field(default_factory=list)
    recommended_fix_category: str | None = None
    requires_human_review: bool = False
    source: str = "RULE"          # "RULE" | "LLM"
    rule_version: str | None = None
    model_id: str | None = None   # populated when source == "LLM"
