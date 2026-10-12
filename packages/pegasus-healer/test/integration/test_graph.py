"""
Graph integration tests — full pipeline, no external infrastructure.

Uses:
  • Real LLM       — Ollama (configured in .env)
  • SQLite          — in-memory checkpointer (no PostgreSQL)
  • DryRunRetryController — prints what it would apply, touches no files
  • Real policy engine   — policies/remediation.yaml

Each test runs the complete graph:
  collect_context → rule_classifier → [diagnosis_agent]
  → fix_catalog → validate_policy → [apply_fix → authorize_retry]

HOW TO RUN
──────────
    pytest tests/integration/test_graph.py -v -s
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from app.graph.graph import create_graph_with_checkpointer
from app.llm import build_llm_provider
from app.models.evidence import RawEvidence
from app.models.events import WorkflowEvent
from app.pegasus.dryrun import DryRunRetryController
from app.policies import PolicyEngine, load_policy

# ── Shared fixtures ────────────────────────────────────────────────────────────

_POLICY    = load_policy("policies/remediation.yaml")
_POLICY_ENGINE = PolicyEngine(_POLICY, confidence_threshold=0.80)


def _make_event(
    job_id: str,
    exit_code: int,
    workflow_id: str | None = None,
    scheduler_reason: str | None = None,
) -> WorkflowEvent:
    wf_id = workflow_id or str(uuid.uuid4())
    raw = {
        "workflow_id": wf_id,
        "job_id": job_id,
        "job_instance_id": 1,
        "exit_code": exit_code,
        "scheduler_reason": scheduler_reason,
    }
    event_id = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
    return WorkflowEvent(
        event_id=event_id,
        event_type="JOB_FAILED",
        workflow_id=wf_id,
        job_id=job_id,
        job_instance_id=1,
        status=exit_code,
        scheduler_reason=scheduler_reason,
        timestamp=datetime.now(timezone.utc),
        raw_event=raw,
    )


def _initial_state(event: WorkflowEvent, incident_id: str) -> dict[str, Any]:
    return {
        "incident_id": incident_id,
        "workflow_id": event.workflow_id,
        "job_id": event.job_id,
        "source_job_instance_id": event.job_instance_id,
        "failure_event": event.model_dump(mode="json"),
        "attempt": 1,
        "errors": [],
    }


async def _run_graph(
    event: WorkflowEvent,
    raw_evidence: RawEvidence,
) -> dict[str, Any]:
    """Build the graph and run it with DryRunRetryController + real LLM."""
    graph = await create_graph_with_checkpointer(pg_url="", sqlite_path=":memory:")
    llm = build_llm_provider()
    retry_ctrl = DryRunRetryController()

    services: dict[str, Any] = {
        "llm": llm,
        "policy": _POLICY,
        "policy_engine": _POLICY_ENGINE,
        "retry_controller": retry_ctrl,
        "raw_evidence": raw_evidence.model_dump(),
        "redis": None,   # no Redis — lock is skipped
    }

    incident_id = str(uuid.uuid4())
    state = _initial_state(event, incident_id)
    config = {"configurable": {"thread_id": incident_id, **services}}

    result: dict[str, Any] = await graph.ainvoke(state, config=config)
    result["_retry_controller"] = retry_ctrl
    return result


def _print_result(scenario: str, result: dict[str, Any]) -> None:
    W = 65
    print(f"\n{'═' * W}")
    print(f"GRAPH RESULT: {scenario}")
    print(f"{'─' * W}")
    diag = result.get("diagnosis") or {}
    fix  = result.get("proposed_fix") or {}
    print(f"  failure_type    : {diag.get('failure_type', '—')}")
    print(f"  confidence      : {diag.get('confidence', '—')}")
    print(f"  diagnosis_source: {diag.get('source', '—')}")
    print(f"  policy_decision : {result.get('policy_decision', '—')}")
    if fix:
        print(f"  fix_action      : {fix.get('action', '—')}")
        cfg = fix.get("proposed_configuration") or {}
        old = fix.get("old_configuration") or {}
        for k, v in cfg.items():
            print(f"    {k}: {old.get(k, '?')}  →  {v}")
    ctrl = result.get("_retry_controller")
    if ctrl and ctrl.retry_receipts:
        print(f"  retries_issued  : {len(ctrl.retry_receipts)}")
    print(f"{'═' * W}")


# ── Tests ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_graph_oom_auto_fix():
    """
    OOM failure (exit 137) → LLM diagnoses OUT_OF_MEMORY
    → catalog proposes INCREASE_MEMORY → policy AUTO
    → DryRunRetryController prints what would be patched.
    """
    evidence = RawEvidence(
        sub_file_content="pegasus_memory_mb = 65536\npegasus_cores = 8\nuniverse = vanilla\nexecutable = mifaser.sh\nqueue",
        stderr_content=(
            "PegasusLite: version 5.1.3\n"
            "Executing on host x1004c2s0b0n0.perlmutter.nersc.gov\n"
            "Loading reference database (62 GB)...\n"
            "oom-kill event(uid=12345,task=mifaser,pid=98765,totalpages=16777216)\n"
            "Killed\n"
            "PegasusLite: exitcode 137\n"
        ),
        pegasus_analyzer_output=(
            "****Summary****\n"
            " last state: POST_SCRIPT_FAILED\n"
            "       site: compute\n"
            " ===mifaser_mifaser_ARS===\n"
            " stderr of mifaser_mifaser_ARS:\n"
            "  oom-kill event... Killed\n"
            "  PegasusLite: exitcode 137\n"
        ),
    )

    event  = _make_event("mifaser_mifaser_ARS", exit_code=137)
    result = await _run_graph(event, evidence)
    _print_result("OOM → AUTO fix", result)

    diag = result.get("diagnosis") or {}
    assert diag.get("failure_type") == "OUT_OF_MEMORY", (
        f"Expected OUT_OF_MEMORY, got {diag.get('failure_type')}"
    )
    assert diag.get("confidence", 0) >= 0.80

    policy = result.get("policy_decision")
    assert policy == "AUTO", f"Expected AUTO, got {policy}"

    ctrl: DryRunRetryController = result["_retry_controller"]
    assert ctrl.applied_overlays, "Expected apply_overlay to be called"
    assert ctrl.retry_receipts, "Expected authorize_retry to be called"

    overlay = ctrl.applied_overlays[0]
    assert overlay.configs_differ(), "Config should change after OOM fix"


@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_graph_walltime_ask():
    """
    Walltime exceeded (scheduler hold) → WALLTIME_EXCEEDED
    → catalog proposes INCREASE_RUNTIME → policy ASK (cost implications)
    → graph stops without patching, awaits human approval.
    """
    evidence = RawEvidence(
        sub_file_content="pegasus_memory_mb = 32768\npegasus_cores = 4\npegasus_job_runtime = 43200\nuniverse = vanilla\nqueue",
        condor_classads_raw=json.dumps([{
            "ExitCode": None,
            "HoldReason": "Job exceeded time limit of 43200 seconds",
            "RemoteWallClockTime": 43250,
            "MemoryUsage": 28000,
            "JobStatus": 5,
        }]),
        pegasus_analyzer_output=(
            "****Summary****\n"
            " last state: JOB_HELD\n"
            "       site: compute\n"
            " ===mifaser_mifaser_ARS===\n"
            " stderr of mifaser_mifaser_ARS:\n"
            "  (job was held by HTCondor: exceeded time limit)\n"
        ),
    )

    event  = _make_event(
        "mifaser_mifaser_ARS",
        exit_code=1,
        scheduler_reason="Job exceeded time limit of 43200 seconds",
    )
    result = await _run_graph(event, evidence)
    _print_result("WALLTIME → ASK", result)

    diag = result.get("diagnosis") or {}
    assert diag.get("failure_type") == "WALLTIME_EXCEEDED", (
        f"Expected WALLTIME_EXCEEDED, got {diag.get('failure_type')}"
    )

    policy = result.get("policy_decision")
    assert policy == "ASK", f"Expected ASK (requires approval), got {policy}"

    ctrl: DryRunRetryController = result["_retry_controller"]
    assert not ctrl.applied_overlays, "ASK should NOT apply the fix automatically"
    assert not ctrl.retry_receipts, "ASK should NOT issue a retry"


@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_graph_disk_exceeded_auto_fix():
    """
    Disk exhaustion → DISK_EXCEEDED → catalog doubles disk
    → policy AUTO → DryRunRetryController prints patch.
    """
    evidence = RawEvidence(
        sub_file_content="pegasus_memory_mb = 8192\npegasus_diskspace_mb = 51200\npegasus_cores = 2\nuniverse = vanilla\nqueue",
        stderr_content=(
            "PegasusLite: version 5.1.3\n"
            "mothur v1.48.0\n"
            "Processing 16S sequences...\n"
            "OSError: [Errno 28] No space left on device: '/scratch/mothur_output.fasta'\n"
            "mothur terminated due to disk error\n"
            "PegasusLite: exitcode 1\n"
        ),
        pegasus_analyzer_output=(
            "****Summary****\n"
            " last state: POST_SCRIPT_FAILED\n"
            "       site: compute\n"
            " ===mothur_asv_mothur_asv===\n"
            " stderr of mothur_asv_mothur_asv:\n"
            "  OSError: [Errno 28] No space left on device\n"
        ),
    )

    event  = _make_event("mothur_asv_mothur_asv", exit_code=1)
    result = await _run_graph(event, evidence)
    _print_result("DISK_EXCEEDED → AUTO fix", result)

    diag = result.get("diagnosis") or {}
    assert diag.get("failure_type") == "DISK_EXCEEDED", (
        f"Expected DISK_EXCEEDED, got {diag.get('failure_type')}"
    )

    policy = result.get("policy_decision")
    assert policy == "AUTO", f"Expected AUTO, got {policy}"

    ctrl: DryRunRetryController = result["_retry_controller"]
    assert ctrl.applied_overlays, "Expected fix to be applied"
    assert ctrl.retry_receipts, "Expected retry to be authorized"


@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_graph_escalates_when_unknown():
    """
    Ambiguous failure with no useful evidence
    → LLM returns UNKNOWN or low confidence → escalate.
    """
    evidence = RawEvidence(
        stderr_content="Segmentation fault (core dumped)\nPegasusLite: exitcode 139\n",
        pegasus_analyzer_output=(
            "****Summary****\n"
            " last state: POST_SCRIPT_FAILED\n"
            "       site: compute\n"
            " ===some_job===\n"
            " stderr of some_job:\n"
            "  Segmentation fault (core dumped)\n"
        ),
    )

    event  = _make_event("some_job", exit_code=139)
    result = await _run_graph(event, evidence)
    _print_result("Segfault → ESCALATE", result)

    policy = result.get("policy_decision")
    diag   = result.get("diagnosis") or {}

    # Segfault with no other evidence → could be APPLICATION_ERROR or UNKNOWN
    # Either way, no AUTO fix should be issued
    ctrl: DryRunRetryController = result["_retry_controller"]
    assert not ctrl.retry_receipts, (
        f"Should not retry a segfault/unknown failure automatically. "
        f"Got policy={policy}, failure_type={diag.get('failure_type')}"
    )
    print(f"\n  → policy={policy}  failure_type={diag.get('failure_type')}")
