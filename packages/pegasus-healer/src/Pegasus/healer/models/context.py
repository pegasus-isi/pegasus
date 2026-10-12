from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ResourceRequest(BaseModel):
    memory_mb: int | None = None
    disk_mb: int | None = None
    cpus: int | None = None
    runtime_seconds: int | None = None


class ResourceUsage(BaseModel):
    peak_memory_mb: int | None = None
    disk_used_mb: int | None = None
    runtime_seconds: int | None = None


class ArtifactRef(BaseModel):
    path: str
    size_bytes: int | None = None
    truncated: bool = False
    excerpt: str | None = None  # first N bytes for the agent


class DataCheck(BaseModel):
    logical_filename: str
    exists: bool
    accessible: bool
    size_bytes: int | None = None
    error: str | None = None


class DiagnosisSummary(BaseModel):
    failure_type: str
    confidence: float
    fix_category: str | None = None
    attempt_number: int


class FixSummary(BaseModel):
    fix_id: str
    action: str
    outcome: str | None = None
    attempt_number: int


class PolicySnapshot(BaseModel):
    policy_version: str
    rules: dict[str, Any] = Field(default_factory=dict)


class FailureContext(BaseModel):
    incident_id: UUID
    workflow_id: str
    job_id: str
    job_instance_id: int
    scheduler_id: str | None = None
    transformation: str | None = None
    execution_site: str | None = None
    exit_code: int | None = None
    termination_signal: int | None = None
    scheduler_state: str | None = None
    scheduler_reason: str | None = None
    requested_resources: ResourceRequest = Field(default_factory=ResourceRequest)
    measured_resources: ResourceUsage = Field(default_factory=ResourceUsage)
    stdout_ref: ArtifactRef | None = None
    stderr_ref: ArtifactRef | None = None
    kickstart_ref: ArtifactRef | None = None
    # HTCondor per-job files
    condor_event_log_ref: ArtifactRef | None = None
    submit_file_ref: ArtifactRef | None = None
    # Pegasus workflow-level logs
    dagman_out_ref: ArtifactRef | None = None
    dagman_err_ref: ArtifactRef | None = None
    workflow_log_ref: ArtifactRef | None = None
    monitord_log_ref: ArtifactRef | None = None
    input_checks: list[DataCheck] = Field(default_factory=list)
    output_checks: list[DataCheck] = Field(default_factory=list)
    attempt_number: int = 1
    previous_diagnoses: list[DiagnosisSummary] = Field(default_factory=list)
    previous_fixes: list[FixSummary] = Field(default_factory=list)
    policy_snapshot: PolicySnapshot | None = None
    job_tags: list[str] = Field(default_factory=list)  # from +PegasusHealerTags in .sub

    # Stderr tail (populated from raw_evidence.stderr_content in collect_context)
    # Used by the rule classifier for pattern-based diagnosis without agent overhead.
    stderr_excerpt: str | None = None

    # ── SLURM partition context ───────────────────────────────────────────────
    # Populated from RawEvidence.current_partition / partition_profile when
    # the job ran on a SLURM cluster.  Both fields are None / empty on HTCondor.
    current_partition: str | None = None
    partition_profile: list[dict] = Field(default_factory=list)

    # Fields that were not available at collection time
    missing_evidence: list[str] = Field(default_factory=list)

    @property
    def has_minimum_evidence(self) -> bool:
        mandatory_missing = [f for f in self.missing_evidence if "mandatory" in f]
        return len(mandatory_missing) == 0
