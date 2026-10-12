from __future__ import annotations

"""
Fix-planning agent — invoked only when the fix catalog has no entry for
a diagnosed failure type (e.g. DATA_MISMATCH, SCRIPT_ERROR, APPLICATION_ERROR).

Contract: receives FailureContext + Diagnosis + RawEvidence → returns FixProposal.
The PolicyEngine runs after this agent and may override or block the proposal.

Safety boundary
───────────────
AUTO  → only resource/scheduler parameters (memory, disk, runtime, site)
ASK   → data binding corrections, transformation script patches, env fixes
STOP  → scientific logic, algorithm parameters, corrupt data requiring re-staging
"""

from typing import Any

from pydantic import BaseModel, Field

from Pegasus.healer.llm.provider import LLMProvider
from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis
from Pegasus.healer.models.evidence import RawEvidence
from Pegasus.healer.models.fixes import FixAction, FixProposal, ScriptPatch

_SYSTEM_PROMPT = """You are a fix-planning agent for the Pegasus Workflow Management System.
You receive a diagnosed job failure and must propose ONE bounded remediation.

Safety boundary — you MUST follow this:
  AUTO (no approval needed): only resource/scheduler changes (memory_mb, disk_mb, runtime_seconds, site)
  ASK  (requires approval) : data binding path corrections, transformation script patches, env fixes
  STOP / HUMAN_REVIEW      : scientific logic bugs, algorithm parameters, missing data requiring re-staging

Constraints:
1. Propose exactly ONE fix_action from the allowed list.
2. Set requires_approval=true for ANYTHING beyond simple resource adjustment.
3. For PATCH_TRANSFORMATION_SCRIPT: populate script_patches with the exact file content.
   original_content must be the EXACT current content. patched_content must be the corrected version.
   Only patch if the script path is known and local (provided in context).
4. For CORRECT_DATA_BINDING / FIX_DATA_BINDING: put corrected paths in proposed_configuration.
5. If no safe bounded fix exists, return action=HUMAN_REVIEW with requires_approval=true.
6. Set confidence 0-1 based on certainty the fix will work.
7. Justify with specific evidence from the diagnosis.

Allowed fix actions:
  INCREASE_MEMORY, INCREASE_DISK, INCREASE_RUNTIME, RETRY_DIFFERENT_SITE,
  CORRECT_DATA_BINDING, FIX_DATA_BINDING, PATCH_TRANSFORMATION_SCRIPT,
  CORRECT_SCHEDULER_CONFIG, HUMAN_REVIEW, NO_ACTION_TRANSIENT_RETRY
"""


class _ScriptPatchOutput(BaseModel):
    file_path: str
    original_content: str
    patched_content: str
    patch_description: str


class _FixProposalLLMOutput(BaseModel):
    action: str = Field(description="One of the allowed FixAction values")
    parameters: dict[str, Any] = Field(default_factory=dict)
    old_configuration: dict[str, Any] = Field(default_factory=dict)
    proposed_configuration: dict[str, Any] = Field(default_factory=dict)
    justification: str
    confidence: float = Field(ge=0.0, le=1.0)
    requires_approval: bool = False
    script_patches: list[_ScriptPatchOutput] = Field(default_factory=list)


class FixPlanningAgent:
    """
    LLM-powered fix-planning agent.

    Returns a FixProposal validated by the Pydantic schema.
    The PolicyEngine always runs after this to enforce safety invariants.
    """

    def __init__(self, llm: LLMProvider) -> None:
        self._llm = llm

    async def run(
        self,
        ctx: FailureContext,
        diagnosis: Diagnosis,
        raw_evidence: RawEvidence | None = None,
    ) -> FixProposal:
        evidence = raw_evidence or RawEvidence()
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": self._build_user_prompt(ctx, diagnosis, evidence)},
        ]
        result = await self._llm.complete(
            messages=messages,
            response_model=_FixProposalLLMOutput,
            temperature=0.0,
        )
        output: _FixProposalLLMOutput = result  # type: ignore[assignment]

        script_patches = [
            ScriptPatch(
                file_path=p.file_path,
                original_content=p.original_content,
                patched_content=p.patched_content,
                patch_description=p.patch_description,
            )
            for p in output.script_patches
        ]

        return FixProposal(
            incident_id=ctx.incident_id,
            action=FixAction(output.action),
            parameters=output.parameters,
            old_configuration=output.old_configuration,
            proposed_configuration=output.proposed_configuration,
            justification=output.justification,
            evidence_ids=diagnosis.evidence_ids,
            confidence=output.confidence,
            source="LLM",
            requires_approval=output.requires_approval,
            script_patches=script_patches,
        )

    def _build_user_prompt(
        self,
        ctx: FailureContext,
        diagnosis: Diagnosis,
        evidence: RawEvidence,
    ) -> str:
        lines = [
            "## Diagnosis",
            f"failure_type: {diagnosis.failure_type.value}",
            f"confidence: {diagnosis.confidence:.2f}",
            f"explanation: {diagnosis.explanation}",
            f"recommended_fix_category: {diagnosis.recommended_fix_category}",
            "",
            "## Resource context",
            f"requested_memory_mb: {ctx.requested_resources.memory_mb}",
            f"peak_memory_mb: {ctx.measured_resources.peak_memory_mb}",
            f"requested_disk_mb: {ctx.requested_resources.disk_mb}",
            f"disk_used_mb: {ctx.measured_resources.disk_used_mb}",
            f"requested_runtime_s: {ctx.requested_resources.runtime_seconds}",
            f"actual_runtime_s: {ctx.measured_resources.runtime_seconds}",
            "",
        ]

        # Transformation script context
        if evidence.transformation_script_content:
            lines += [
                "## Transformation script (local, patchable)",
                f"path: {evidence.transformation_script_path}",
                "content:",
                "```",
                evidence.transformation_script_content[:3000],
                "```" + (" (truncated)" if len(evidence.transformation_script_content) > 3000 else ""),
                "",
            ]
        else:
            lines += [
                "## Transformation script",
                "not accessible from submit host — do NOT propose PATCH_TRANSFORMATION_SCRIPT",
                "",
            ]

        # Input validation context
        if evidence.input_validation:
            missing = [f for f in evidence.input_validation if not f.exists]
            lines += [
                "## Input file validation",
                f"total_declared: {len(evidence.input_validation)}",
                f"missing: {len(missing)}",
            ]
            for f in evidence.input_validation:
                status = "MISSING" if not f.exists else ("EMPTY" if f.is_empty else f"ok ({f.size_bytes} bytes)")
                lines.append(f"  {f.path}  →  {status}")
            lines.append("")

        # Previous fixes
        if ctx.previous_fixes:
            lines += [
                "## Previous fixes (avoid repeating ineffective ones)",
                *[f"- {f.action} → outcome={f.outcome}" for f in ctx.previous_fixes],
                "",
            ]

        return "\n".join(lines)
