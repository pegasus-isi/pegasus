from __future__ import annotations

"""
DiagnosisAgent — ReAct (Reason + Act) loop for Pegasus job failure diagnosis.

Loop contract
─────────────
Each iteration the agent outputs a ThoughtAction:
  thought  — what it knows and what it needs next
  action   — one tool name, OR "conclude" when confident

On "conclude" the agent must also populate the diagnosis fields
(failure_type, confidence, explanation, etc.).

The loop runs at most MAX_STEPS iterations. After that the current
best hypothesis is forced to a conclusion.

Stopping conditions (checked after each step):
  • action == "conclude"           → extract diagnosis and return
  • confidence in thought >= 0.85  → next step will be "conclude"
  • same tool called twice         → force conclude
  • MAX_STEPS reached              → force conclude with lower confidence

Provider-agnostic: uses the LLMProvider protocol (LiteLLM + instructor).
"""

import json
from typing import Any, Literal, get_args

import structlog
from pydantic import BaseModel, Field

from Pegasus.healer.agents.tools import TOOL_DESCRIPTIONS, TOOL_MAP
from Pegasus.healer.llm.provider import LLMProvider
from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import FAILURE_FIX_CATEGORY, Diagnosis, FailureType
from Pegasus.healer.models.evidence import RawEvidence

log = structlog.get_logger(__name__)

MAX_STEPS = 7

ActionType = Literal[
    "parse_failure_summary",
    "get_stderr",
    "get_kickstart_data",
    "get_resource_requests",
    "get_condor_history",
    "get_event_log",
    "get_dagman_log",
    "get_workflow_log",
    "get_transformation_script",
    "get_input_validation",
    "conclude",
]

ALL_ACTIONS: tuple[str, ...] = get_args(ActionType)


class ThoughtAction(BaseModel):
    """One step of the ReAct loop."""
    thought: str = Field(
        description="Your reasoning: what you know, your current hypothesis, what you need next"
    )
    action: ActionType = Field(
        description="The tool to call, or 'conclude' when you have enough evidence"
    )
    # ── Only populated when action == "conclude" ──────────────────────────────
    failure_type: FailureType | None = Field(
        default=None,
        description="Primary failure type — required when action is 'conclude'",
    )
    confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Confidence 0-1 — required when action is 'conclude'",
    )
    explanation: str | None = Field(
        default=None,
        description="Human-readable explanation — required when action is 'conclude'",
    )
    missing_evidence: list[str] = Field(
        default_factory=list,
        description="Evidence that would improve confidence",
    )
    requires_human_review: bool = Field(
        default=False,
        description="True only when scientific logic may be involved",
    )


_SYSTEM_PROMPT = f"""You are a Pegasus WMS / HTCondor job failure diagnostic agent.

You receive a failed job's seed context and debug it step by step using tools.
At each step output a JSON object with:
  thought  — your reasoning about what you know and what you need
  action   — one tool name, OR "conclude" when confident

{TOOL_DESCRIPTIONS}

When action is "conclude" you must also provide:
  failure_type    — one of: OUT_OF_MEMORY, DISK_EXCEEDED, WALLTIME_EXCEEDED,
                    TRANSIENT_INFRASTRUCTURE, SCHEDULER_ADMISSION, MISSING_INPUT,
                    DATA_MISMATCH, SCRIPT_ERROR, APPLICATION_ERROR, LOGIC_VALIDATION, UNKNOWN
  confidence      — float 0.0-1.0
  explanation     — what you found and why
  missing_evidence — list of fields that would improve confidence (can be empty)
  requires_human_review — true only for LOGIC_VALIDATION or scientific issues

Diagnostic rules:
  • STOP calling tools and conclude as soon as confidence >= 0.85
  • Never call the same tool twice
  • exit_code=137 or signal=9 → first check memory and stderr (OOM vs walltime kill)
  • last_state=HELD            → first check condor hold_reason
  • "No space left on device" in stderr → conclude DISK_EXCEEDED immediately
  • "Killed" in stderr + memory near limit → conclude OUT_OF_MEMORY
  • Shadow exception / eviction in event log → conclude TRANSIENT_INFRASTRUCTURE
  • FileNotFoundError / No such file in stderr → call get_input_validation → MISSING_INPUT
  • "expected format" / format error / wrong dimensions in stderr → DATA_MISMATCH
  • ModuleNotFoundError / ImportError / command not found in stderr → call get_transformation_script → SCRIPT_ERROR
  • Traceback with no resource/data/env pressure → APPLICATION_ERROR
  • Never suggest modifying scientific algorithms or scientific parameters
  • DATA_MISMATCH: wrong file format, mismatched dimensions, corrupted input
  • SCRIPT_ERROR: missing module, wrong env, incorrect argument, script bug

PegasusLite exit code rules (exit codes from the PegasusLite wrapper script):
  • exitcode 71  = the user application inside the container exited with code 1
                   → focus on WHY the application failed, not the wrapper
  • exitcode 72  = PegasusLite setup failure (worker package, staging)
  • exitcode 73  = data staging failure (transfer_input_files could not be fetched)
  • exitcode 74  = output transfer failure
  • exitcode 75  = cleanup failure (usually ignorable)

Missing-file triage rules:
  • Files named *.lof, *.meta, pegasus-worker-*.tar.gz, pegasus-lite-common.sh
    are Pegasus INFRASTRUCTURE files — their absence is normal on the compute node
    (they are staged by PegasusLite itself). Do NOT conclude MISSING_INPUT for these.
  • Only conclude MISSING_INPUT if SCIENTIFIC input files (fasta, fastq, db, csv, …) are missing.

Repeated fast-failure rule:
  • If kickstart shows N ≥ 3 attempts all with wall_time < 60 s and exit_code != 0
    → the job fails immediately on every retry
    → likely a container environment issue (wrong image, missing binary, bad env var)
    → conclude APPLICATION_ERROR with note "repeated fast failures suggest container/env problem"
    → confidence 0.80 is sufficient; do not exhaust all tools searching for more evidence
"""


def _seed_message(
    ctx: FailureContext,
    evidence: RawEvidence,
    memories: list[dict[str, Any]],
) -> str:
    available = evidence.available_sources
    memory_lines = "\n".join(
        f"  - failure={m.get('failure_type')} fix={m.get('fix_action')} outcome={m.get('outcome')}"
        for m in memories[:3]
    ) or "  (none)"

    prev_diagnoses = "\n".join(
        f"  - attempt {d.attempt_number}: {d.failure_type} (confidence={d.confidence:.2f})"
        for d in ctx.previous_diagnoses
    ) or "  (none)"

    prev_fixes = "\n".join(
        f"  - attempt {f.attempt_number}: {f.action} → {f.outcome}"
        for f in ctx.previous_fixes
    ) or "  (none)"

    return f"""== Failed job seed context ==

job_id:          {ctx.job_id}
workflow_id:     {ctx.workflow_id}
job_instance_id: {ctx.job_instance_id}
attempt:         {ctx.attempt_number}
exit_code:       {ctx.exit_code}
signal:          {ctx.termination_signal}
scheduler_state: {ctx.scheduler_state}
scheduler_reason:{ctx.scheduler_reason}
execution_site:  {ctx.execution_site}
transformation:  {ctx.transformation}

Available data sources (call tools to read them):
  {', '.join(available) if available else 'none'}

Previous diagnoses:
{prev_diagnoses}

Previous fixes applied:
{prev_fixes}

Similar past incidents:
{memory_lines}

Begin your step-by-step diagnosis now.
Start with parse_failure_summary if pegasus_analyzer is available, otherwise get_stderr."""


def _execute_tool(action: str, evidence: RawEvidence) -> str:
    fn = TOOL_MAP.get(action)
    if fn is None:
        return f"Unknown tool: {action}"
    result = fn(evidence)
    if isinstance(result, dict):
        return json.dumps(result, default=str, indent=2)
    return str(result)


def _build_diagnosis(step: ThoughtAction, ctx: FailureContext) -> Diagnosis:
    failure_type = step.failure_type or FailureType.UNKNOWN
    confidence = step.confidence if step.confidence is not None else 0.5
    return Diagnosis(
        failure_type=failure_type,
        confidence=confidence,
        explanation=step.explanation or step.thought,
        missing_evidence=step.missing_evidence,
        requires_human_review=step.requires_human_review,
        recommended_fix_category=FAILURE_FIX_CATEGORY.get(failure_type),
        evidence_ids=[],
        source="LLM",
        rule_version=None,
    )


class DiagnosisAgent:
    """
    ReAct diagnosis agent.

    Each call to run() starts a fresh multi-turn conversation with the LLM.
    The conversation grows as tools are called and observations are added.
    """

    def __init__(self, llm: LLMProvider) -> None:
        self._llm = llm

    async def run(
        self,
        ctx: FailureContext,
        evidence: RawEvidence,
        retrieved_memories: list[dict[str, Any]],
    ) -> Diagnosis:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _seed_message(ctx, evidence, retrieved_memories)},
        ]

        used_tools: set[str] = set()
        last_step: ThoughtAction | None = None

        for step_num in range(MAX_STEPS):
            step: ThoughtAction = await self._llm.complete(  # type: ignore[assignment]
                messages=messages,
                response_model=ThoughtAction,
                temperature=0.0,
            )
            last_step = step

            log.info(
                "react_step",
                step=step_num + 1,
                action=step.action,
                thought_preview=step.thought[:120],
                incident_id=str(ctx.incident_id),
            )

            # Append agent's turn to history
            messages.append({
                "role": "assistant",
                "content": json.dumps({
                    "thought": step.thought,
                    "action": step.action,
                }, ensure_ascii=False),
            })

            # ── Conclude ──────────────────────────────────────────────────────
            if step.action == "conclude":
                log.info(
                    "react_concluded",
                    failure_type=str(step.failure_type),
                    confidence=step.confidence,
                    steps_used=step_num + 1,
                    incident_id=str(ctx.incident_id),
                )
                return _build_diagnosis(step, ctx)

            # ── Guard: tool already used ──────────────────────────────────────
            if step.action in used_tools:
                messages.append({
                    "role": "user",
                    "content": (
                        f"SYSTEM: You already called '{step.action}'. "
                        "You must now call 'conclude' with your best diagnosis."
                    ),
                })
                continue

            # ── Execute tool ──────────────────────────────────────────────────
            if step.action not in TOOL_MAP:
                observation = f"Unknown tool '{step.action}'. Valid tools: {list(TOOL_MAP)}"
            else:
                observation = _execute_tool(step.action, evidence)
                used_tools.add(step.action)

            messages.append({
                "role": "user",
                "content": f"OBSERVATION from {step.action}:\n{observation}",
            })

        # ── Max steps reached — force a final conclusion ───────────────────────
        messages.append({
            "role": "user",
            "content": (
                "SYSTEM: Maximum diagnostic steps reached. "
                "You must call 'conclude' NOW with your best hypothesis. "
                "Lower confidence appropriately if evidence is incomplete."
            ),
        })
        final: ThoughtAction = await self._llm.complete(  # type: ignore[assignment]
            messages=messages,
            response_model=ThoughtAction,
            temperature=0.0,
        )

        log.warning(
            "react_max_steps_reached",
            failure_type=str(final.failure_type),
            confidence=final.confidence,
            incident_id=str(ctx.incident_id),
        )
        return _build_diagnosis(final, ctx)
