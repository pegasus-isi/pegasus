from __future__ import annotations

"""
LangGraph nodes for the Pegasus remediation two-loop graph.

Each function is a pure node: it receives the current RemediationState,
performs exactly one responsibility, and returns a partial state dict.

Node categories (per spec §4):
  - Data collector   : collect_context
  - Deterministic    : run_rule_classifier, lookup_fix_catalog,
                       validate_policy, apply_fix, authorize_retry,
                       evaluate_outcome, write_memory
  - AI agent         : run_diagnosis_agent, run_fix_planner
  - Data retrieval   : retrieve_memories

Services (llm, policy, retry_controller, etc.) are injected at runtime via
LangGraph's config["configurable"] mechanism.  Each node that needs services
calls _svc() to retrieve the dict.
"""

from typing import Any
from uuid import UUID

import structlog
from langgraph.config import get_config

from Pegasus.healer.collectors.base import ContextCollector
from Pegasus.healer.scheduler.factory import get_scheduler_client
from Pegasus.healer.fixes import lookup as catalog_lookup
from Pegasus.healer.fixes.overlay import build_overlay
from Pegasus.healer.models.context import (
    DiagnosisSummary,
    FixSummary,
    FailureContext,
    PolicySnapshot,
)
from Pegasus.healer.models.diagnosis import Diagnosis
from Pegasus.healer.models.evidence import RawEvidence
from Pegasus.healer.models.fixes import FixOutcome, FixProposal, PolicyDecision
from Pegasus.healer.policies import PolicyEngine
from Pegasus.healer.rules.classifier import classify

log = structlog.get_logger(__name__)


# ── Service accessor ──────────────────────────────────────────────────────────

def _svc() -> dict[str, Any]:
    """Return the services dict injected via config['configurable']."""
    return get_config().get("configurable", {})


# ── Shared helpers ────────────────────────────────────────────────────────────

def _ctx(state: dict[str, Any]) -> FailureContext:
    return FailureContext.model_validate(state["context"])


def _diagnosis(state: dict[str, Any]) -> Diagnosis:
    return Diagnosis.model_validate(state["diagnosis"])


def _proposal(state: dict[str, Any]) -> FixProposal:
    return FixProposal.model_validate(state["proposed_fix"])


# ── Diagnostic loop nodes ─────────────────────────────────────────────────────

async def collect_context(state: dict[str, Any]) -> dict[str, Any]:
    """
    Collect all diagnostic evidence and build a FailureContext.
    Runs before both the rule classifier and (on re-entry) the LLM agent.
    """
    svc = _svc()
    incident_id     = UUID(state["incident_id"])
    job_id          = state["job_id"]
    workflow_id     = state["workflow_id"]
    job_instance_id = state.get("source_job_instance_id", 1)
    exit_code       = state.get("exit_code")
    scheduler_id    = state.get("scheduler_id")

    attempt = state.get("attempt", 1)
    if state.get("retry_outcome"):
        attempt += 1

    previous_diagnoses = [
        DiagnosisSummary.model_validate(d)
        for d in state.get("context", {}).get("previous_diagnoses", [])
    ]
    previous_fixes = [
        FixSummary.model_validate(f)
        for f in state.get("context", {}).get("previous_fixes", [])
    ]

    policy = svc.get("policy")
    policy_snapshot = (
        PolicySnapshot(policy_version=policy.version, rules=policy.as_snapshot_dict())
        if policy else None
    )

    scheduler = svc.get("scheduler_client") or get_scheduler_client()
    ctx = await ContextCollector(scheduler).collect(
        job_id=job_id,
        workflow_id=workflow_id,
        job_instance_id=job_instance_id,
        exit_code=exit_code,
        scheduler_id=scheduler_id,
        incident_id=incident_id,
        attempt_number=attempt,
        previous_diagnoses=previous_diagnoses,
        previous_fixes=previous_fixes,
        policy_snapshot=policy_snapshot,
        submit_dir=svc.get("submit_dir"),
    )

    # Carry raw evidence from services (populated by POST script endpoint)
    # Falls back to empty RawEvidence in AMQP mode
    raw_ev_dict: dict[str, Any] = svc.get("raw_evidence") or {}
    raw_evidence = RawEvidence.model_validate(raw_ev_dict)

    # Populate healer tags from the .sub file so PolicyEngine can enforce them
    if raw_evidence.sub_file_content:
        from Pegasus.healer.collectors.submit_dir import parse_healer_tags
        tags = parse_healer_tags(raw_evidence.sub_file_content)
        if tags:
            ctx = ctx.model_copy(update={"job_tags": tags})

    # Populate transformation from the .sub file so apply_fix can broadcast
    # the fix to sibling jobs that share the same transformation.
    # The scheduler (condor_history) rarely carries the transformation name
    # in POST script mode, so the .sub file is the authoritative source.
    if raw_evidence.sub_file_content and not ctx.transformation:
        from Pegasus.healer.pegasus.sibling_fixer import _parse_transformation_from_content
        _t = _parse_transformation_from_content(raw_evidence.sub_file_content)
        if _t:
            ctx = ctx.model_copy(update={"transformation": _t})

    # Populate stderr_excerpt from raw_evidence so the rule classifier can
    # pattern-match on stderr content (e.g. "No space left on device").
    if raw_evidence.stderr_content and not ctx.stderr_excerpt:
        excerpt = raw_evidence.stderr_content[-3000:]   # tail — errors at bottom
        ctx = ctx.model_copy(update={"stderr_excerpt": excerpt})

    # When raw_evidence is provided (POST script path), always use the current
    # .sub file content for resource requests. The scheduler history (condor_history)
    # records the values from the *completed* job's ClassAd, which is the original
    # value before any healer patch. The .sub file reflects what will actually be
    # submitted on the next retry — we must use it to avoid re-proposing the same fix.
    if raw_evidence.sub_file_content:
        from Pegasus.healer.agents.tools import get_resource_requests
        parsed = get_resource_requests(raw_evidence)
        if any(v is not None for v in parsed.values()):
            ctx = ctx.model_copy(
                update={"requested_resources": ctx.requested_resources.model_copy(
                    update={k: v for k, v in parsed.items() if v is not None
                            and k in ("memory_mb", "disk_mb", "cpus", "runtime_seconds")}
                )}
            )

    updates: dict[str, Any] = {
        "context": ctx.model_dump(mode="json"),
        "raw_evidence": raw_evidence.model_dump(mode="json"),
        "attempt": attempt,
    }
    # Only block on insufficient evidence when raw_evidence is absent.
    # The POST script already collected everything — classads and submit_dir
    # are not additionally required when raw_evidence has sources.
    if not ctx.has_minimum_evidence and not raw_evidence.available_sources:
        updates["insufficient_evidence_fields"] = ctx.missing_evidence
    return updates


async def retrieve_memories(state: dict[str, Any]) -> dict[str, Any]:
    """
    Pull similar past incidents from episodic memory.
    Results are passed to the LLM agent as additional context.
    """
    svc = _svc()
    memory_repo = svc.get("memory_repo")
    if memory_repo is None:
        return {"retrieved_memories": []}

    ctx = _ctx(state)
    memories = await memory_repo.find_similar(
        workflow_id=ctx.workflow_id,
        failure_type=None,  # unknown yet — broad search
        transformation=ctx.transformation,
        project_id=ctx.workflow_id,
        limit=5,
    )
    return {"retrieved_memories": [m for m in memories]}


async def run_rule_classifier(state: dict[str, Any]) -> dict[str, Any]:
    """
    Run deterministic failure rules.  No LLM involved.
    If a rule fires with high confidence, the diagnosis is set directly.
    If not, state signals the graph to route to the LLM agent.
    """
    ctx = _ctx(state)
    diagnosis = classify(ctx)

    if diagnosis is not None:
        log.info(
            "rule_classifier_match",
            failure_type=diagnosis.failure_type,
            confidence=diagnosis.confidence,
            incident_id=state["incident_id"],
        )
        return {"diagnosis": diagnosis.model_dump(mode="json")}

    log.info("rule_classifier_no_match", incident_id=state["incident_id"])
    return {}   # empty → router will send to memory_retrieval then LLM


async def run_diagnosis_agent(state: dict[str, Any]) -> dict[str, Any]:
    """
    LLM-powered ReAct diagnosis for ambiguous failures.
    Only invoked when deterministic rules did not produce a confident result.

    Runs a multi-step Reason→Act→Observe loop using the raw file evidence
    collected by the POST script (or AMQP path if available).

    Uses svc["diagnosis_llm"] when available; falls back to svc["llm"].
    """
    from Pegasus.healer.agents.diagnosis import DiagnosisAgent

    svc = _svc()
    llm = svc.get("diagnosis_llm") or svc["llm"]
    agent = DiagnosisAgent(llm)
    ctx = _ctx(state)
    memories = state.get("retrieved_memories", [])
    raw_evidence = RawEvidence.model_validate(state.get("raw_evidence") or {})

    diagnosis = await agent.run(ctx, raw_evidence, memories)
    log.info(
        "diagnosis_agent_result",
        failure_type=diagnosis.failure_type,
        confidence=diagnosis.confidence,
        steps_used="react_chain",
        incident_id=state["incident_id"],
    )
    return {"diagnosis": diagnosis.model_dump(mode="json")}


# ── Action loop nodes ─────────────────────────────────────────────────────────

async def lookup_fix_catalog(state: dict[str, Any]) -> dict[str, Any]:
    """
    Look up a known fix for the diagnosed failure type.
    Returns a FixProposal if found, or empty dict to route to LLM planner.
    """
    svc = _svc()
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    policy = svc["policy"]

    proposal = catalog_lookup(diagnosis, ctx, policy)
    if proposal is not None:
        log.info(
            "fix_catalog_hit",
            action=proposal.action.value,
            incident_id=state["incident_id"],
        )
        return {"proposed_fix": proposal.model_dump(mode="json"), "fix_id": str(proposal.fix_id)}

    log.info("fix_catalog_miss", incident_id=state["incident_id"])
    return {}


async def run_fix_planner(state: dict[str, Any]) -> dict[str, Any]:
    """
    LLM-powered fix planning for complex/unfamiliar failure types.
    Only invoked when the catalog has no match.

    Uses svc["fix_planning_llm"] when available; falls back to svc["llm"].
    """
    from Pegasus.healer.agents.fix_planning import FixPlanningAgent

    svc = _svc()
    llm = svc.get("fix_planning_llm") or svc["llm"]
    agent = FixPlanningAgent(llm)
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    raw_evidence = RawEvidence.model_validate(state.get("raw_evidence") or {})

    proposal = await agent.run(ctx, diagnosis, raw_evidence)
    log.info(
        "fix_planner_result",
        action=proposal.action.value,
        confidence=proposal.confidence,
        incident_id=state["incident_id"],
    )
    return {"proposed_fix": proposal.model_dump(mode="json"), "fix_id": str(proposal.fix_id)}


async def validate_policy(state: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic policy gate.  Runs after every FixProposal (catalog or LLM).
    Deterministic policy ALWAYS overrides LLM output.
    """
    svc = _svc()
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    proposal = _proposal(state)

    policy: PolicyEngine = svc["policy_engine"]
    record = policy.validate(proposal, diagnosis, ctx)

    log.info(
        "policy_decision",
        decision=record.decision.value,
        fix_id=record.fix_id,
        incident_id=state["incident_id"],
        checks_failed=record.checks_failed,
    )
    return {
        "policy_decision": record.decision.value,
        "policy_record": record.model_dump(mode="json"),
    }


async def apply_fix(state: dict[str, Any]) -> dict[str, Any]:
    """
    Patch the failing job's .sub file and broadcast the fix to all sibling
    jobs that share the same transformation and original resource configuration.

    Sibling strategy (per HTCondor state):
      IDLE    → patch .sub + condor_qedit  (update ClassAd, no disruption)
      RUNNING → patch .sub only            (retry uses new values if it fails)
      other   → patch .sub only            (not yet submitted by DAGMan)
    """
    svc = _svc()
    proposal = _proposal(state)
    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    retry_ctrl = svc["retry_controller"]

    # 1. Patch the failing job's .sub file (stores pre-patch snapshot internally)
    overlay = await retry_ctrl.apply_overlay(proposal)
    log.info(
        "fix_applied",
        fix_id=str(proposal.fix_id),
        hash_before=overlay.hash_before,
        hash_after=overlay.hash_after,
        configs_differ=overlay.configs_differ(),
        incident_id=state["incident_id"],
    )

    # 2. Broadcast to siblings with same transformation + same resource config
    if hasattr(retry_ctrl, "broadcast_to_siblings") and ctx.transformation:
        thread_id = svc.get("thread_id", state.get("incident_id", ""))
        siblings_patched = await retry_ctrl.broadcast_to_siblings(
            fix=proposal,
            failure_type=diagnosis.failure_type,
            transformation=ctx.transformation,
            thread_id=thread_id,
        )
        if siblings_patched:
            log.info(
                "siblings_broadcast",
                count=siblings_patched,
                transformation=ctx.transformation,
                failure_type=diagnosis.failure_type,
                incident_id=state["incident_id"],
            )

    # 3. Apply script patches (ASK-path only; guarded by policy before reaching here)
    if proposal.script_patches and hasattr(retry_ctrl, "apply_script_patches"):
        patched = await retry_ctrl.apply_script_patches(proposal)
        log.info(
            "script_patches_applied",
            count=len(patched),
            paths=patched,
            fix_id=str(proposal.fix_id),
            incident_id=state["incident_id"],
        )

    return {"overlay": overlay.model_dump(mode="json")}


async def authorize_retry(state: dict[str, Any]) -> dict[str, Any]:
    """
    Release exactly ONE retry attempt and record its new job_instance_id.
    Acquires distributed lock before acting.
    """
    svc = _svc()
    incident_id = UUID(state["incident_id"])
    fix_id = UUID(state["fix_id"])
    retry_ctrl = svc["retry_controller"]
    redis = svc.get("redis")

    # Distributed lock to prevent concurrent fixes
    if redis:
        lock_key = f"incident_lock:{incident_id}"
        lock = redis.lock(lock_key, timeout=svc.get("lock_ttl", 300))
        async with lock:
            receipt = await retry_ctrl.authorize_retry(incident_id, fix_id)
    else:
        receipt = await retry_ctrl.authorize_retry(incident_id, fix_id)

    log.info(
        "retry_authorized",
        retry_job_instance_id=receipt.retry_job_instance_id,
        fix_id=receipt.fix_id,
        incident_id=state["incident_id"],
    )
    return {"retry_job_instance_id": receipt.retry_job_instance_id}


async def evaluate_outcome(state: dict[str, Any]) -> dict[str, Any]:
    """
    Classify the retry outcome as EFFECTIVE/INEFFECTIVE/PARTIALLY_EFFECTIVE/
    NEW_FAILURE/INCONCLUSIVE.

    The retry_outcome is set by the event ingestor when the matching
    JOB_SUCCEEDED or JOB_FAILED event for retry_job_instance_id arrives.
    This node reads that value and enriches it.
    """
    raw_outcome = state.get("retry_outcome", "INCONCLUSIVE")
    try:
        outcome = FixOutcome(raw_outcome)
    except ValueError:
        outcome = FixOutcome.INCONCLUSIVE

    log.info(
        "outcome_evaluated",
        outcome=outcome.value,
        incident_id=state["incident_id"],
        fix_id=state.get("fix_id"),
    )
    return {"retry_outcome": outcome.value}


async def write_memory(state: dict[str, Any]) -> dict[str, Any]:
    """
    Promote verified episode to durable long-term memory.
    Only called when outcome is EFFECTIVE.
    """
    svc = _svc()
    memory_repo = svc.get("memory_repo")
    if memory_repo is None:
        return {"terminal": True}

    ctx = _ctx(state)
    diagnosis = _diagnosis(state)
    proposal = _proposal(state)

    await memory_repo.store_episode(
        project_id=ctx.workflow_id,
        failure_type=diagnosis.failure_type.value,
        transformation=ctx.transformation,
        execution_site=ctx.execution_site,
        fix_action=proposal.action.value,
        outcome=state.get("retry_outcome", "EFFECTIVE"),
        configuration_delta=proposal.proposed_configuration,
        source_incident_id=ctx.incident_id,
        confidence=diagnosis.confidence,
    )
    log.info("memory_written", incident_id=state["incident_id"])
    return {"terminal": True}


async def generate_proposal_report(state: dict[str, Any]) -> dict[str, Any]:
    """
    Terminal node for ASK decisions.

    The agent diagnosed the failure and has a fix proposal, but cannot apply
    it automatically — either because the current scope level is insufficient
    (e.g. scope=job but fix requires catalog access) or because the failure
    type always requires human action (SCRIPT_ERROR, APPLICATION_ERROR).

    Writes two files to the job directory so the human has an actionable signal:
      {job_id}.healer_proposal       — human-readable plain text
      {job_id}.healer_proposal.json  — machine-readable JSON for tooling

    Exit code for this path: non-zero (DAGMan does NOT retry automatically).
    """
    proposal = _proposal(state)
    diagnosis = _diagnosis(state)
    svc = _svc()

    log.info(
        "proposal_report_generated",
        failure_type=diagnosis.failure_type,
        action=proposal.action.value,
        scope_reason=state.get("policy_record", {}).get("reason", ""),
        incident_id=state["incident_id"],
    )

    _write_proposal_files(state, proposal, diagnosis, svc)
    return {"terminal": True}


# ── Proposal file helpers ──────────────────────────────────────────────────────

def _find_job_dir_for_node(submit_dir: str, job_id: str) -> "Path":
    from pathlib import Path
    p = Path(submit_dir)
    for candidate in p.rglob(f"{job_id}.sub"):
        return candidate.parent
    return p


def _manual_steps(proposal: FixProposal) -> str:
    """Return human-readable numbered steps for applying the fix manually."""
    from Pegasus.healer.models.fixes import FixAction

    action = proposal.action
    cfg = proposal.proposed_configuration
    old = proposal.old_configuration

    if action == FixAction.INCREASE_MEMORY:
        return (
            f"1. Open the job .sub file.\n"
            f"2. Change:  request_memory = {old.get('memory_mb', '?')}\n"
            f"       to:  request_memory = {cfg.get('memory_mb', '?')}\n"
            f"3. Save the file and resubmit (see below)."
        )
    if action == FixAction.INCREASE_DISK:
        return (
            f"1. Open the job .sub file.\n"
            f"2. Change:  request_disk = {old.get('disk_mb', '?')}\n"
            f"       to:  request_disk = {cfg.get('disk_mb', '?')}\n"
            f"3. Save the file and resubmit (see below)."
        )
    if action == FixAction.INCREASE_RUNTIME:
        return (
            f"1. Open the job .sub file.\n"
            f"2. Change:  +pegasus_request_time = {old.get('runtime_seconds', '?')}\n"
            f"       to:  +pegasus_request_time = {cfg.get('runtime_seconds', '?')}\n"
            f"3. Save the file and resubmit (see below)."
        )
    if action == FixAction.MIGRATE_PARTITION:
        tgt = cfg.get("target_partition", "?")
        return (
            f"1. Open the job .sub file.\n"
            f"2. Set:  batch_queue = {tgt}\n"
            f"         +remote_queue = \"{tgt}\"\n"
            f"         request_memory = {cfg.get('memory_mb', '?')}\n"
            f"   (was: partition={old.get('target_partition', '?')}, "
            f"memory={old.get('memory_mb', '?')} MB)\n"
            f"3. Save the file and resubmit (see below)."
        )
    if action == FixAction.RETRY_DIFFERENT_SITE:
        return (
            "1. Identify an alternative execution site that has the required\n"
            "   resource (license / GPU / module).\n"
            "2. Update the site catalog or job site selector.\n"
            "3. Resubmit the workflow targeting that site."
        )
    if action == FixAction.RESTAGE_INPUT:
        return (
            "1. Identify the corrupted input LFN from the diagnosis above.\n"
            "2. Re-transfer the file:\n"
            "       pegasus-transfer --file <lfn>\n"
            "3. Verify the checksum, then resubmit."
        )
    if action == FixAction.CORRECT_DATA_BINDING:
        return (
            "1. Fix the input file path in the replica catalog (rc.txt / rc.yml).\n"
            "2. Re-plan or resubmit the workflow."
        )
    if action == FixAction.PATCH_TRANSFORMATION_SCRIPT:
        if proposal.script_patches:
            lines = ["Apply the following patch(es):"]
            for p in proposal.script_patches:
                lines.append(f"\n  File   : {p.file_path}")
                lines.append(f"  Change : {p.patch_description}")
            lines.append("\nThen resubmit the workflow.")
            return "\n".join(lines)
        return "Apply the suggested script patch and resubmit."
    if action == FixAction.CORRECT_SCHEDULER_CONFIG:
        return (
            "1. Fix the scheduler configuration in the .sub file\n"
            "   (wrong account, QOS, partition, or constraint).\n"
            "2. Resubmit the workflow."
        )
    # Generic fallback
    return (
        f"Action required: {action}\n"
        "See the justification above for details.\n"
        "Apply the fix manually and resubmit the workflow."
    )


def _write_proposal_files(
    state: dict[str, Any],
    proposal: FixProposal,
    diagnosis: Diagnosis,
    svc: dict[str, Any],
) -> None:
    """
    Write {job_id}.healer_proposal (text) and {job_id}.healer_proposal.json
    to the job's submit subdirectory so the human has an actionable signal.
    """
    import json
    import textwrap
    from datetime import datetime, timezone

    submit_dir = svc.get("submit_dir")
    job_id = state.get("job_id", "unknown")
    if not submit_dir:
        log.warning("proposal_file_skipped", reason="no submit_dir in services")
        return

    job_dir = _find_job_dir_for_node(submit_dir, job_id)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    steps = _manual_steps(proposal)
    sep = "─" * 72

    # ── Evidence summary ──────────────────────────────────────────────────────
    raw_ev = state.get("raw_evidence") or {}
    exit_code   = state.get("exit_code", raw_ev.get("exit_code", "?"))
    retry_num   = state.get("retry_number", "?")
    max_retries = state.get("max_retries", "?")

    available_sources = raw_ev.get("available_sources") or []
    stderr_content    = raw_ev.get("stderr_content") or ""
    stdout_content    = raw_ev.get("stdout_content") or ""
    kickstart_data    = raw_ev.get("kickstart_data") or {}
    sub_file          = raw_ev.get("sub_file_content") or ""

    stderr_tail = "\n".join(stderr_content.splitlines()[-30:]) if stderr_content else "(empty)"
    stdout_tail = "\n".join(stdout_content.splitlines()[-10:]) if stdout_content else "(empty)"

    # Kickstart execution summary
    ks_lines: list[str] = []
    if kickstart_data:
        ks_lines.append(f"  Executable     : {kickstart_data.get('executable', '?')}")
        ks_lines.append(f"  Exit code      : {kickstart_data.get('exit_code', '?')}")
        ks_lines.append(f"  Duration (s)   : {kickstart_data.get('duration', '?')}")
        ks_lines.append(f"  Max RSS (MB)   : {kickstart_data.get('max_rss_mb', '?')}")
        ks_lines.append(f"  CPU user (s)   : {kickstart_data.get('cpu_user', '?')}")
        ks_lines.append(f"  CPU sys  (s)   : {kickstart_data.get('cpu_sys', '?')}")
        ks_lines.append(f"  Bytes read     : {kickstart_data.get('bytes_read', '?')}")
        ks_lines.append(f"  Bytes written  : {kickstart_data.get('bytes_written', '?')}")
        ks_lines.append(f"  Machine        : {kickstart_data.get('hostname', '?')}")
        ks_lines.append(f"  Site           : {kickstart_data.get('site', '?')}")
    kickstart_block = "\n".join(ks_lines) if ks_lines else "  (no kickstart data available)"

    # Resource requests from .sub file
    sub_resources: list[str] = []
    for line in sub_file.splitlines():
        l = line.strip().lower()
        if any(k in l for k in ("request_memory", "request_disk", "request_cpus",
                                 "request_runtime", "+pegasus_request", "queue")):
            sub_resources.append(f"  {line.strip()}")
    sub_block = "\n".join(sub_resources) if sub_resources else "  (not available)"

    # ── Alternative diagnoses ─────────────────────────────────────────────────
    alt_lines: list[str] = []
    for alt in diagnosis.alternative_causes:
        alt_lines.append(f"  {alt.failure_type:<30} {alt.confidence:.0%}  {alt.explanation}")
    alt_block = "\n".join(alt_lines) if alt_lines else "  (none considered)"

    # ── Policy decision ───────────────────────────────────────────────────────
    pr = state.get("policy_record") or {}
    policy_decision  = state.get("policy_decision", "?")
    checks_passed    = pr.get("checks_passed") or []
    checks_failed    = pr.get("checks_failed") or []
    policy_reason    = pr.get("reason", "")
    policy_version   = pr.get("policy_version", "?")

    checks_pass_str  = "\n".join(f"  ✓  {c}" for c in checks_passed) or "  (none)"
    checks_fail_str  = "\n".join(f"  ✗  {c}" for c in checks_failed) or "  (none)"

    text = textwrap.dedent(f"""\
        ╔══════════════════════════════════════════════════════════════════════╗
        ║           PEGASUS HEALER — FIX PROPOSAL  (action required)         ║
        ╚══════════════════════════════════════════════════════════════════════╝

        Generated : {now}
        Incident  : {state.get('incident_id', '?')}
        Job       : {job_id}
        Workflow  : {state.get('workflow_id', '?')}
        Exit code : {exit_code}
        Retry     : {retry_num} / {max_retries}

        {sep}
        EVIDENCE ANALYZED
        {sep}
        Sources available : {', '.join(available_sources) if available_sources else 'none'}

        Resource requests (.sub):
{sub_block}

        Kickstart execution metrics:
{kickstart_block}

        Stderr (last 30 lines):
        {chr(10).join('        ' + l for l in stderr_tail.splitlines())}

        Stdout (last 10 lines):
        {chr(10).join('        ' + l for l in stdout_tail.splitlines())}

        {sep}
        DIAGNOSIS
        {sep}
        Failure type  : {diagnosis.failure_type}
        Confidence    : {diagnosis.confidence:.0%}
        Source        : {diagnosis.source}{f'  (model: {diagnosis.model_id})' if diagnosis.model_id else ''}
        Evidence IDs  : {', '.join(diagnosis.evidence_ids) if diagnosis.evidence_ids else 'none'}
        Human review  : {'Yes — LLM flagged uncertainty' if diagnosis.requires_human_review else 'No'}

        Explanation:
        {chr(10).join('        ' + l for l in diagnosis.explanation.splitlines())}

        Missing evidence that would improve confidence:
        {chr(10).join('        • ' + m for m in diagnosis.missing_evidence) if diagnosis.missing_evidence else '        (none)'}

        {sep}
        ALTERNATIVE DIAGNOSES CONSIDERED
        {sep}
{alt_block}

        {sep}
        PROPOSED FIX
        {sep}
        Action        : {proposal.action}
        Fix ID        : {proposal.fix_id}
        Confidence    : {proposal.confidence:.0%}
        Source        : {proposal.source}
        Evidence used : {', '.join(proposal.evidence_ids) if proposal.evidence_ids else 'none'}

        Justification:
        {chr(10).join('        ' + l for l in proposal.justification.splitlines())}

        Configuration change:
          Before : {proposal.old_configuration or '(none)'}
          After  : {proposal.proposed_configuration or '(none)'}

        {sep}
        POLICY DECISION
        {sep}
        Decision       : {policy_decision}
        Policy version : {policy_version}
        Reason         : {policy_reason}

        Checks passed:
{checks_pass_str}

        Checks failed (caused ASK / STOP):
{checks_fail_str}

        {sep}
        MANUAL STEPS
        {sep}
        {steps}

        {sep}
        HOW TO RESUBMIT
        {sep}
        After applying the fix, resume the workflow from the submit directory:

            pegasus-run {submit_dir}

        Or resume from the DAGMan rescue file:

            condor_submit_dag -DoRescueFrom <N> *.dag

        {sep}
        NOTE: This fix was NOT applied automatically (policy = {policy_decision}).
        Review the steps above, apply the change, then resubmit.
        To mark the job permanently failed instead, run:

            pegasus-remove {submit_dir}
        """)

    txt_path = job_dir / f"{job_id}.healer_proposal"
    json_path = job_dir / f"{job_id}.healer_proposal.json"

    try:
        txt_path.write_text(text, encoding="utf-8")
        log.info("proposal_file_written", path=str(txt_path), job_id=job_id)
    except OSError as exc:
        log.warning("proposal_file_write_failed", path=str(txt_path), error=str(exc))

    try:
        payload = {
            "schema_version": "1.0",
            "generated_at": now,
            "incident_id": str(state.get("incident_id")),
            "job_id": job_id,
            "workflow_id": state.get("workflow_id"),
            "submit_dir": submit_dir,
            "diagnosis": diagnosis.model_dump(mode="json"),
            "proposal": proposal.model_dump(mode="json"),
            "policy_record": state.get("policy_record"),
        }
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("proposal_json_written", path=str(json_path), job_id=job_id)
    except OSError as exc:
        log.warning("proposal_json_write_failed", path=str(json_path), error=str(exc))


async def handle_insufficient_evidence(state: dict[str, Any]) -> dict[str, Any]:
    """Terminal node when mandatory evidence is missing."""
    log.warning(
        "insufficient_evidence",
        missing=state.get("insufficient_evidence_fields", []),
        incident_id=state["incident_id"],
    )
    return {"terminal": True}


async def escalate(state: dict[str, Any]) -> dict[str, Any]:
    """
    Terminal node when the system cannot safely proceed.
    Writes a {job_id}.healer_escalation file so the human has a clear signal.
    """
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    svc = _svc()
    submit_dir = svc.get("submit_dir")
    job_id = state.get("job_id", "unknown")

    log.warning(
        "incident_escalated",
        incident_id=state["incident_id"],
        policy_decision=state.get("policy_decision"),
        errors=state.get("errors", []),
    )

    if submit_dir:
        job_dir = _find_job_dir_for_node(submit_dir, job_id)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        sep = "─" * 72

        # ── Collect diagnostic detail for the report ──────────────────────────
        raw_ev = RawEvidence.model_validate(state.get("raw_evidence") or {})
        diagnosis_raw = state.get("diagnosis")
        diagnosis = Diagnosis.model_validate(diagnosis_raw) if diagnosis_raw else None

        # Stderr snippet (tail of last 20 lines)
        stderr_snippet = ""
        if raw_ev.stderr_content:
            lines = raw_ev.stderr_content.strip().splitlines()
            tail = lines[-20:] if len(lines) > 20 else lines
            stderr_snippet = (
                f"{sep}\n"
                f"STDERR (last {len(tail)} lines)\n"
                f"{sep}\n"
                + "\n".join(f"    {l}" for l in tail)
                + "\n\n"
            )

        # Pegasus-analyzer output (first 30 lines)
        analyzer_snippet = ""
        if raw_ev.pegasus_analyzer_output:
            lines = raw_ev.pegasus_analyzer_output.strip().splitlines()
            head = lines[:30]
            analyzer_snippet = (
                f"{sep}\n"
                f"PEGASUS-ANALYZER OUTPUT\n"
                f"{sep}\n"
                + "\n".join(f"    {l}" for l in head)
                + ("\n    [...]\n" if len(lines) > 30 else "\n")
                + "\n"
            )

        # Diagnosis block (if the LLM or rule engine produced one)
        diag_block = ""
        if diagnosis:
            diag_block = (
                f"{sep}\n"
                f"HEALER DIAGNOSIS\n"
                f"{sep}\n"
                f"    Failure type : {diagnosis.failure_type}\n"
                f"    Confidence   : {diagnosis.confidence:.0%}\n"
                f"    Explanation  : {diagnosis.explanation}\n"
                f"    Source       : {diagnosis.source}\n\n"
            )

        # Evidence sources checked
        evidence_block = (
            f"Evidence collected : {raw_ev.available_sources or ['none']}\n"
            f"Exit code          : {state.get('exit_code', '?')}\n"
        )

        text = (
            f"╔══════════════════════════════════════════════════════════════════════╗\n"
            f"║           PEGASUS HEALER — ESCALATION  (human intervention needed) ║\n"
            f"╚══════════════════════════════════════════════════════════════════════╝\n\n"
            f"Generated : {now}\n"
            f"Incident  : {state.get('incident_id', '?')}\n"
            f"Job       : {job_id}\n"
            f"Workflow  : {state.get('workflow_id', '?')}\n\n"
            f"{sep}\n"
            f"SUMMARY\n"
            f"{sep}\n"
            f"The healer could not fix this failure automatically.\n"
            f"Policy decision    : {state.get('policy_decision', 'ESCALATE')}\n"
            f"{evidence_block}"
            f"Healer errors      : {state.get('errors', [])}\n\n"
            f"{diag_block}"
            f"{stderr_snippet}"
            f"{analyzer_snippet}"
            f"{sep}\n"
            f"FILES TO INSPECT\n"
            f"{sep}\n"
            f"    {job_dir}/{job_id}.err\n"
            f"    {job_dir}/{job_id}.out\n"
            f"    {job_dir}/{job_id}.healer.log\n\n"
            f"{sep}\n"
            f"RESUBMIT\n"
            f"{sep}\n"
            f"After resolving the issue manually:\n"
            f"    pegasus-run {submit_dir}\n"
        )
        esc_path = job_dir / f"{job_id}.healer_escalation"
        try:
            esc_path.write_text(text, encoding="utf-8")
            log.info("escalation_file_written", path=str(esc_path))
        except OSError as exc:
            log.warning("escalation_file_write_failed", path=str(esc_path), error=str(exc))

    return {"terminal": True}
