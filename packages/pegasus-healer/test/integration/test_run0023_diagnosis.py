"""
Real-LLM diagnosis tests using actual files from run0023.

Strategy
────────
run0023 is a real Pegasus workflow that ran on NERSC Perlmutter.
All jobs completed successfully (the workflow was manually aborted later),
so there are no actual failure stdout/stderr files.

We use:
  • REAL submit files (.sub)       — actual resource requests from the cluster
  • REAL workflow files            — dagman.out, monitord.log, workflow.log
  • REAL stderr from trim_reads    — real PegasusLite output (.err.000)
  • SIMULATED pegasus-analyzer     — what it would output for a failure
  • SIMULATED failure stderr       — injected at the bottom of real stderr

This gives the agent realistic Pegasus context while testing different
failure scenarios against a real LLM.

HOW TO RUN
──────────
    export LLM_MODEL=anthropic/claude-opus-4-6     # or any provider
    export LLM_API_KEY=sk-ant-...
    pytest tests/integration/test_run0023_diagnosis.py -v -s
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from app.agents.tools import get_resource_requests
from app.collectors.submit_dir import collect_evidence, collect_job_files, collect_workflow_files
from app.fixes import lookup as catalog_lookup
from app.llm import build_llm_provider
from app.models.context import FailureContext, ResourceRequest, ResourceUsage
from app.models.diagnosis import Diagnosis, FailureType
from app.models.evidence import RawEvidence
from app.models.fixes import FixProposal, PolicyDecisionRecord
from app.policies import PolicyEngine, load_policy

# ── Paths ─────────────────────────────────────────────────────────────────────
RUN_DIR = Path(__file__).parent.parent / "fixtures" / "run0023"
JOB_DIR = RUN_DIR / "00" / "00"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require_run0023():
    return pytest.mark.skipif(
        not RUN_DIR.exists(),
        reason=f"run0023 directory not found at {RUN_DIR}",
    )


def _require_llm():
    return pytest.mark.skipif(
        not os.environ.get("LLM_API_KEY")
        and not os.environ.get("LLM_MODEL", "").startswith("ollama"),
        reason="Set LLM_API_KEY (and optionally LLM_MODEL) to run real-LLM tests",
    )


def _make_ctx(
    job_id: str,
    exit_code: int,
    signal: int | None = None,
    scheduler_state: str | None = None,
    scheduler_reason: str | None = None,
    memory_mb: int | None = None,
    cpus: int | None = None,
    runtime_seconds: int | None = None,
) -> FailureContext:
    return FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id="91c16593-c21e-4cd5-bce3-9e6d076068ab",
        job_id=job_id,
        job_instance_id=1,
        exit_code=exit_code,
        termination_signal=signal,
        scheduler_state=scheduler_state,
        scheduler_reason=scheduler_reason,
        execution_site="compute",
        transformation=job_id.split("_")[0],
        requested_resources=ResourceRequest(
            memory_mb=memory_mb,
            cpus=cpus,
            runtime_seconds=runtime_seconds,
        ),
        measured_resources=ResourceUsage(),
        attempt_number=1,
    )


def _simulated_analyzer(job_id: str, last_state: str, stderr_excerpt: str) -> str:
    """Build a pegasus-analyzer-style text output for a failed job."""
    return f"""
************************************Summary*************************************

 Submit Directory   : {RUN_DIR}
 Workflow Status    : failure
 Total jobs         :     135 ( 100.00% )
 # jobs succeeded   :      15 (  11.11% )
 # jobs failed      :       1 (   0.74% )
 # jobs unsubmitted :     119 (  88.15% )

******************************Failed jobs' details******************************

 ====================={job_id}======================

 last state: {last_state}
       site: compute
  submit file: {JOB_DIR}/{job_id}.sub

 stderr of {job_id}:
{stderr_excerpt}

"""


# ── Tool-level tests (no LLM, instant) ────────────────────────────────────────

@_require_run0023()
def test_get_resource_requests_from_real_sub_file():
    """
    Parse real mifaser .sub file — uses Pegasus-native attributes:
      pegasus_memory_mb, pegasus_cores, pegasus_job_runtime
    These are NOT standard HTCondor request_memory / request_cpus.
    """
    sub_content = (JOB_DIR / "mifaser_mifaser_ARS.sub").read_text()
    ev = RawEvidence(sub_file_content=sub_content)
    result = get_resource_requests(ev)

    assert result["memory_mb"] == 65536, \
        f"Expected 65536 MB (64 GB), got {result['memory_mb']}"
    assert result["cpus"] == 8, \
        f"Expected 8 cores, got {result['cpus']}"
    assert result["runtime_seconds"] == 43200, \
        f"Expected 43200 s (12h), got {result['runtime_seconds']}"

    print(f"\nParsed from mifaser_mifaser_ARS.sub: {result}")


@_require_run0023()
def test_get_resource_requests_from_trim_reads_sub():
    """Parse real trim_reads .sub file."""
    sub_content = (JOB_DIR / "trim_reads_trim_PBS.sub").read_text()
    ev = RawEvidence(sub_file_content=sub_content)
    result = get_resource_requests(ev)

    # trim_reads should have some resource requests
    assert result["memory_mb"] is not None, "memory_mb should be parsed from trim_reads sub"
    print(f"\nParsed from trim_reads_trim_PBS.sub: {result}")


@_require_run0023()
def test_collect_job_files_finds_err_000_naming():
    """
    Verify the collector finds .err.000 / .out.000 files.
    Pegasus BLAH/grid jobs use this naming convention instead of plain .err/.out.
    """
    job_files = collect_job_files(RUN_DIR, "trim_reads_trim_PBS", instance_id=1)

    assert job_files["stderr_content"] is not None, \
        "Should find trim_reads_trim_PBS.err.000"
    assert job_files["sub_file_content"] is not None, \
        "Should find trim_reads_trim_PBS.sub"
    assert "PegasusLite" in job_files["stderr_content"], \
        "Stderr should contain PegasusLite output"

    print(f"\nCollected files: {[k for k, v in job_files.items() if v]}")
    print(f"Stderr first 200 chars: {job_files['stderr_content'][:200]}")


@_require_run0023()
def test_collect_workflow_files():
    """Verify dagman.out and monitord.log are collected from run0023."""
    wf_files = collect_workflow_files(RUN_DIR)

    assert wf_files["dagman_log_content"] is not None, \
        "Should collect *.dag.dagman.out"
    assert "DAGMan" in wf_files["dagman_log_content"] or \
           "DAGMAN" in wf_files["dagman_log_content"], \
        "dagman_log should contain DAGMan output"

    print(f"\nWorkflow files: {[k for k, v in wf_files.items() if v]}")
    print(f"DAGMan log first 300 chars:\n{wf_files['dagman_log_content'][:300]}")


# ── Real-LLM diagnosis tests ───────────────────────────────────────────────────

@_require_run0023()
@_require_llm()
@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_mifaser_oom_scenario():
    """
    Scenario: mifaser_mifaser_ARS fails with SIGKILL (exit 137).

    Real resources from .sub:  64 GB RAM, 8 cores, 12h runtime
    Simulated: OOM killer message in stderr, pegasus-analyzer says POST_SCRIPT_FAILED

    Expected diagnosis: OUT_OF_MEMORY
    """
    from app.agents.diagnosis import DiagnosisAgent

    job_id = "mifaser_mifaser_ARS"
    sub_content = (JOB_DIR / f"{job_id}.sub").read_text()

    # Real .sub + simulated stderr showing OOM kill
    simulated_stderr = (
        "2026-09-11 05:30:01: PegasusLite: version 5.1.3-dev.0\n"
        "2026-09-11 05:30:02: Executing on host x1004c2s0b0n0h0.chn.perlmutter.nersc.gov\n"
        "2026-09-11 05:30:05: mifaser starting alignment...\n"
        "2026-09-11 05:31:14: Loading reference database (62 GB)\n"
        "2026-09-11 07:45:22: oom-kill event(uid=12345,oom_memcg=/user.slice,task_memcg=/job.slice,task=mifaser,pid=98765,totalpages=16777216)\n"
        "2026-09-11 07:45:22: Killed\n"
        "PegasusLite: exitcode 137\n"
    )

    evidence = RawEvidence(
        sub_file_content=sub_content,
        stderr_content=simulated_stderr,
        pegasus_analyzer_output=_simulated_analyzer(
            job_id, "POST_SCRIPT_FAILED",
            "  oom-kill event... Killed\n  PegasusLite: exitcode 137",
        ),
        dagman_log_content=collect_workflow_files(RUN_DIR).get("dagman_log_content"),
    )

    ctx = _make_ctx(
        job_id=job_id,
        exit_code=137,
        signal=9,
        memory_mb=65536,
        cpus=8,
        runtime_seconds=43200,
    )

    llm = build_llm_provider()
    agent = DiagnosisAgent(llm)
    diagnosis = await agent.run(ctx, evidence, retrieved_memories=[])

    proposal, record = _remediate(ctx, evidence, diagnosis)
    _print_full_remediation(job_id, "OOM scenario", diagnosis, proposal, record)

    assert diagnosis.failure_type == FailureType.OUT_OF_MEMORY, (
        f"Expected OUT_OF_MEMORY, got {diagnosis.failure_type} "
        f"(confidence={diagnosis.confidence:.2f})\n{diagnosis.explanation}"
    )
    assert diagnosis.confidence >= 0.80
    assert proposal is not None, "OOM should have a catalog fix"
    assert record is not None
    assert record.decision.value == "AUTO", (
        f"Expected AUTO policy, got {record.decision.value}: {record.reason}"
    )
    assert proposal.proposed_configuration["memory_mb"] > ctx.requested_resources.memory_mb


@_require_run0023()
@_require_llm()
@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_mifaser_walltime_scenario():
    """
    Scenario: mifaser_mifaser_ARS held by scheduler for exceeding walltime.

    Real resources from .sub:  12h runtime limit (43200 seconds)
    Simulated: HTCondor hold reason "Job exceeded time limit"

    Expected diagnosis: WALLTIME_EXCEEDED
    """
    from app.agents.diagnosis import DiagnosisAgent
    import json

    job_id = "mifaser_mifaser_ARS"
    sub_content = (JOB_DIR / f"{job_id}.sub").read_text()

    simulated_classads = json.dumps([{
        "ExitCode": None,
        "ExitSignal": None,
        "MemoryUsage": 45000,
        "DiskUsage": 3000,
        "HoldReason": "Job exceeded time limit of 43200 seconds",
        "RemoteWallClockTime": 43250,
        "JobStatus": 5,
    }])

    evidence = RawEvidence(
        sub_file_content=sub_content,
        condor_classads_raw=simulated_classads,
        pegasus_analyzer_output=_simulated_analyzer(
            job_id, "JOB_HELD",
            "  (job was held by HTCondor)",
        ),
        dagman_log_content=collect_workflow_files(RUN_DIR).get("dagman_log_content"),
    )

    ctx = _make_ctx(
        job_id=job_id,
        exit_code=None,
        scheduler_state="HELD",
        scheduler_reason="Job exceeded time limit of 43200 seconds",
        memory_mb=65536,
        cpus=8,
        runtime_seconds=43200,
    )

    llm = build_llm_provider()
    agent = DiagnosisAgent(llm)
    diagnosis = await agent.run(ctx, evidence, retrieved_memories=[])

    proposal, record = _remediate(ctx, evidence, diagnosis)
    _print_full_remediation(job_id, "walltime scenario", diagnosis, proposal, record)

    assert diagnosis.failure_type == FailureType.WALLTIME_EXCEEDED, (
        f"Expected WALLTIME_EXCEEDED, got {diagnosis.failure_type} "
        f"(confidence={diagnosis.confidence:.2f})\n{diagnosis.explanation}"
    )
    assert diagnosis.confidence >= 0.85
    assert proposal is not None, "WALLTIME_EXCEEDED should have a catalog fix"
    assert record is not None
    assert record.decision.value == "ASK", (
        f"Expected ASK (requires approval), got {record.decision.value}: {record.reason}"
    )
    assert proposal.proposed_configuration["runtime_seconds"] > ctx.requested_resources.runtime_seconds


@_require_run0023()
@_require_llm()
@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_trim_reads_with_real_stderr_and_application_error():
    """
    Scenario: trim_reads_trim_PBS fails with exit code 1 (application error).

    Uses:
      • REAL .sub file from run0023
      • REAL .err.000 file (PegasusLite output), appended with simulated
        application error at the bottom
      • Simulated pegasus-analyzer output

    Expected diagnosis: APPLICATION_ERROR (trimmomatic crashed after staging)
    """
    from app.agents.diagnosis import DiagnosisAgent

    job_id = "trim_reads_trim_PBS"

    # Real .sub and real .err.000, but inject failure at the end
    sub_content = (JOB_DIR / f"{job_id}.sub").read_text()
    real_stderr = (JOB_DIR / f"{job_id}.err.000").read_text()

    # Replace the success marker with a failure scenario
    simulated_stderr = real_stderr.replace(
        "PegasusLite: exitcode 0",
        (
            "Exception in thread 'main' java.lang.OutOfMemoryError: GC overhead limit exceeded\n"
            "    at java.base/java.util.Arrays.copyOf(Arrays.java:3213)\n"
            "    at java.base/java.lang.AbstractStringBuilder.ensureCapacityInternal(AbstractStringBuilder.java:155)\n"
            "Trimmomatic error: failed to process reads\n"
            "PegasusLite: exitcode 1"
        ),
    )

    evidence = RawEvidence(
        sub_file_content=sub_content,
        stderr_content=simulated_stderr,
        pegasus_analyzer_output=_simulated_analyzer(
            job_id, "POST_SCRIPT_FAILED",
            (
                "  Exception in thread 'main' java.lang.OutOfMemoryError: GC overhead limit exceeded\n"
                "  Trimmomatic error: failed to process reads\n"
                "  PegasusLite: exitcode 1"
            ),
        ),
        dagman_log_content=collect_workflow_files(RUN_DIR).get("dagman_log_content"),
        workflow_log_content=(RUN_DIR / "monitord.log").read_text()
        if (RUN_DIR / "monitord.log").exists() else None,
    )

    ctx = _make_ctx(job_id=job_id, exit_code=1)

    llm = build_llm_provider()
    agent = DiagnosisAgent(llm)
    diagnosis = await agent.run(ctx, evidence, retrieved_memories=[])

    proposal, record = _remediate(ctx, evidence, diagnosis)
    _print_full_remediation(job_id, "application error (JVM OOM) scenario", diagnosis, proposal, record)

    # JVM OutOfMemoryError is an APPLICATION_ERROR — not cluster OOM
    # (no SIGKILL, no memory near cluster limit)
    assert diagnosis.failure_type in (
        FailureType.APPLICATION_ERROR,
        FailureType.OUT_OF_MEMORY,  # also acceptable — JVM heap exhaustion
    ), (
        f"Expected APPLICATION_ERROR or OUT_OF_MEMORY, got {diagnosis.failure_type} "
        f"(confidence={diagnosis.confidence:.2f})\n{diagnosis.explanation}"
    )
    assert diagnosis.confidence >= 0.70
    # APPLICATION_ERROR → no catalog entry (needs human), OUT_OF_MEMORY → catalog fix
    if diagnosis.failure_type == FailureType.APPLICATION_ERROR:
        assert proposal is None, "APPLICATION_ERROR should not have an automatic catalog fix"
    else:
        assert proposal is not None


@_require_run0023()
@_require_llm()
@pytest.mark.asyncio
@pytest.mark.usefixtures("require_ollama")
async def test_mothur_disk_exceeded_scenario():
    """
    Scenario: mothur_asv_mothur_asv fails — no space left on device.

    Uses real .sub file + simulated disk exhaustion in stderr.
    Expected diagnosis: DISK_EXCEEDED
    """
    from app.agents.diagnosis import DiagnosisAgent

    job_id = "mothur_asv_mothur_asv"
    sub_content = (JOB_DIR / f"{job_id}.sub").read_text()

    simulated_stderr = (
        "2026-09-11 05:35:10: PegasusLite: version 5.1.3-dev.0\n"
        "2026-09-11 05:35:11: Executing on host x1004c2s0b0n0h0.chn.perlmutter.nersc.gov\n"
        "2026-09-11 05:35:15: mothur v1.48.0\n"
        "2026-09-11 05:38:20: Processing 16S sequences...\n"
        "2026-09-11 05:41:03: OSError: [Errno 28] No space left on device: "
        "'/pscratch/sd/h/hsafri/.blah/pegasus.tmpXXX/mothur_output.fasta'\n"
        "2026-09-11 05:41:03: mothur terminated due to disk error\n"
        "PegasusLite: exitcode 1\n"
    )

    evidence = RawEvidence(
        sub_file_content=sub_content,
        stderr_content=simulated_stderr,
        pegasus_analyzer_output=_simulated_analyzer(
            job_id, "POST_SCRIPT_FAILED",
            "  OSError: [Errno 28] No space left on device",
        ),
        dagman_log_content=collect_workflow_files(RUN_DIR).get("dagman_log_content"),
    )

    ctx = _make_ctx(job_id=job_id, exit_code=1)

    llm = build_llm_provider()
    agent = DiagnosisAgent(llm)
    diagnosis = await agent.run(ctx, evidence, retrieved_memories=[])

    proposal, record = _remediate(ctx, evidence, diagnosis)
    _print_full_remediation(job_id, "disk exceeded scenario", diagnosis, proposal, record)

    assert diagnosis.failure_type == FailureType.DISK_EXCEEDED, (
        f"Expected DISK_EXCEEDED, got {diagnosis.failure_type} "
        f"(confidence={diagnosis.confidence:.2f})\n{diagnosis.explanation}"
    )
    assert diagnosis.confidence >= 0.85
    assert proposal is not None, "DISK_EXCEEDED should have a catalog fix"
    assert record is not None
    assert record.decision.value == "AUTO", (
        f"Expected AUTO policy, got {record.decision.value}: {record.reason}"
    )
    assert proposal.proposed_configuration["disk_mb"] > (ctx.requested_resources.disk_mb or 0)


# ── Remediation chain helper ───────────────────────────────────────────────────

_POLICY = load_policy("policies/remediation.yaml")
_POLICY_ENGINE = PolicyEngine(_POLICY, confidence_threshold=0.80)


def _remediate(
    ctx: FailureContext,
    evidence: RawEvidence,
    diagnosis: Diagnosis,
) -> tuple[FixProposal | None, PolicyDecisionRecord | None]:
    """
    Run the fix catalog + policy engine on an already-completed diagnosis.
    Returns (proposal, policy_record) — both None if catalog has no match.
    """
    proposal = catalog_lookup(diagnosis, ctx, _POLICY)
    if proposal is None:
        return None, None
    record = _POLICY_ENGINE.validate(proposal, diagnosis, ctx)
    return proposal, record


def _print_full_remediation(
    job_id: str,
    scenario: str,
    diagnosis: Diagnosis,
    proposal: FixProposal | None,
    record: PolicyDecisionRecord | None,
) -> None:
    W = 65
    print(f"\n{'═' * W}")
    print(f"FULL REMEDIATION: {job_id} — {scenario}")
    print(f"{'─' * W}")

    print(f"[1] DIAGNOSIS")
    print(f"    Failure    : {diagnosis.failure_type}")
    print(f"    Confidence : {diagnosis.confidence:.0%}")
    print(f"    Explanation: {diagnosis.explanation}")
    if diagnosis.missing_evidence:
        print(f"    Missing    : {diagnosis.missing_evidence}")

    if proposal is None:
        print(f"\n[2] FIX CATALOG   : no catalog entry — LLM fix planner required")
        print(f"{'═' * W}")
        return

    print(f"\n[2] FIX PROPOSAL")
    print(f"    Action  : {proposal.action.value}")
    for key, new_val in proposal.proposed_configuration.items():
        old_val = proposal.old_configuration.get(key, "—")
        print(f"    {key}: {old_val}  →  {new_val}")
    print(f"    Reason  : {proposal.justification}")

    decision = record.decision.value if record else "—"
    icon = {"AUTO": "[OK]", "ASK": "[!]", "STOP": "[X]", "ESCALATE": "[?]"}.get(decision, "   ")
    print(f"\n[3] POLICY DECISION:  {icon}  {decision}")
    if record:
        print(f"    Checks passed : {', '.join(record.checks_passed)}")
        if record.checks_failed:
            print(f"    Checks failed : {', '.join(record.checks_failed)}")
        print(f"    Reason        : {record.reason}")

    if decision == "AUTO":
        print(f"\n    → DAGMan would retry with patched .sub file")
    elif decision == "ASK":
        print(f"\n    → Fix ready but requires human approval before retry")
    else:
        print(f"\n    → No retry; manual intervention required")

    print(f"{'═' * W}")
