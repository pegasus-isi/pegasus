"""
Real-LLM diagnosis integration tests.

These tests run the full ReAct chain against real Pegasus submit directory
files using the real LLM configured in your environment.

HOW TO RUN
──────────
1. Set LLM credentials (any provider — see .env.example):
       export LLM_MODEL=gpt-4o
       export LLM_API_KEY=sk-...
   Or for Anthropic:
       export LLM_MODEL=anthropic/claude-opus-4-6
       export LLM_API_KEY=sk-ant-...

2. Point at a real submit directory with a failed job:
       export PEGASUS_TEST_SUBMIT_DIR=/path/to/submit/dir
       export PEGASUS_TEST_JOB_ID=myjob_ID0000001
       export PEGASUS_TEST_EXIT_CODE=137
       export PEGASUS_TEST_EXPECTED_FAILURE=OUT_OF_MEMORY  # optional assertion

3. Run:
       pytest tests/integration/test_real_diagnosis.py -v -s

FIXTURE SUBMIT DIRECTORIES
───────────────────────────
Place real (or anonymised) submit directories under:
    tests/fixtures/submit_dirs/<case_name>/
        00/00/{job_id}_ID{n:07d}.sub
        00/00/{job_id}_ID{n:07d}.out
        00/00/{job_id}_ID{n:07d}.err
        00/00/{job_id}_ID{n:07d}.log
        *.dag.dagman.out
        workflow.log

Then add an entry to FIXTURE_CASES below.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from app.collectors.submit_dir import collect_evidence, parse_instance_id
from app.llm import build_llm_provider
from app.models.context import FailureContext, ResourceRequest, ResourceUsage
from app.models.diagnosis import FailureType

# ── Fixture-based cases (add your submit dirs here) ──────────────────────────
#
# Each entry:
#   submit_dir     — path relative to repo root
#   job_id         — DAGMan node name (e.g. "myjob_ID0000001")
#   exit_code      — job exit code
#   condor_job_id  — HTCondor cluster.proc (optional, for condor_history query)
#   expected       — FailureType you expect (None = just check it runs cleanly)
#
FIXTURE_CASES: list[dict[str, Any]] = [
    # ── Add your submit directories here ──────────────────────────────────────
    # {
    #     "id": "oom_example",
    #     "submit_dir": "tests/fixtures/submit_dirs/oom_example",
    #     "job_id": "myjob_ID0000001",
    #     "exit_code": 137,
    #     "condor_job_id": None,
    #     "expected": FailureType.OUT_OF_MEMORY,
    # },
]

# ── Environment-driven case (PEGASUS_TEST_SUBMIT_DIR) ─────────────────────────
_env_submit_dir = os.environ.get("PEGASUS_TEST_SUBMIT_DIR")
_env_job_id     = os.environ.get("PEGASUS_TEST_JOB_ID", "job_ID0000001")
_env_exit_code  = int(os.environ.get("PEGASUS_TEST_EXIT_CODE", "1"))
_env_expected   = os.environ.get("PEGASUS_TEST_EXPECTED_FAILURE")  # e.g. "OUT_OF_MEMORY"
_env_condor_id  = os.environ.get("PEGASUS_TEST_CONDOR_JOB_ID")

if _env_submit_dir:
    FIXTURE_CASES.append({
        "id": "env_submit_dir",
        "submit_dir": _env_submit_dir,
        "job_id": _env_job_id,
        "exit_code": _env_exit_code,
        "condor_job_id": _env_condor_id,
        "expected": FailureType(_env_expected) if _env_expected else None,
    })


def _requires_llm():
    """Skip if no LLM credentials configured."""
    return pytest.mark.skipif(
        not os.environ.get("LLM_API_KEY") and not os.environ.get("LLM_MODEL", "").startswith("ollama"),
        reason="Set LLM_API_KEY (and optionally LLM_MODEL) to run real-LLM tests",
    )


def _requires_cases():
    return pytest.mark.skipif(
        not FIXTURE_CASES,
        reason=(
            "No fixture cases configured. Either add entries to FIXTURE_CASES "
            "or set PEGASUS_TEST_SUBMIT_DIR env var."
        ),
    )


def _build_context(
    job_id: str,
    exit_code: int,
    submit_dir: str,
) -> FailureContext:
    """Build a minimal FailureContext from the given parameters."""
    instance_id = parse_instance_id(job_id)
    return FailureContext(
        incident_id=uuid.uuid4(),
        workflow_id=f"test-workflow-{uuid.uuid4().hex[:8]}",
        job_id=job_id,
        job_instance_id=instance_id,
        exit_code=exit_code,
        termination_signal=9 if exit_code == 137 else None,
        scheduler_state=None,
        scheduler_reason=None,
        requested_resources=ResourceRequest(),
        measured_resources=ResourceUsage(),
        attempt_number=1,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

@_requires_llm()
@_requires_cases()
@pytest.mark.parametrize("case", FIXTURE_CASES, ids=[c["id"] for c in FIXTURE_CASES])
@pytest.mark.asyncio
async def test_real_diagnosis_from_submit_dir(case: dict[str, Any]) -> None:
    """
    Full ReAct diagnosis chain against a real Pegasus submit directory.

    Asserts:
      - diagnosis completes without error
      - failure_type is a valid FailureType
      - confidence > 0.0
      - explanation is non-empty
      - if expected is set, failure_type matches
    """
    from app.agents.diagnosis import DiagnosisAgent

    submit_dir = Path(case["submit_dir"])
    job_id     = case["job_id"]
    exit_code  = case["exit_code"]
    expected   = case.get("expected")

    assert submit_dir.exists(), f"Submit directory not found: {submit_dir}"

    # ── Collect evidence from submit directory ────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"Submit dir : {submit_dir}")
    print(f"Job ID     : {job_id}")
    print(f"Exit code  : {exit_code}")

    evidence = collect_evidence(
        submit_dir=submit_dir,
        job_id=job_id,
        condor_job_id=case.get("condor_job_id"),
    )

    print(f"Sources    : {evidence.available_sources}")

    # ── Build context ─────────────────────────────────────────────────────────
    ctx = _build_context(job_id, exit_code, str(submit_dir))

    # ── Run real LLM diagnosis ────────────────────────────────────────────────
    llm = build_llm_provider()
    agent = DiagnosisAgent(llm)
    diagnosis = await agent.run(ctx, evidence, retrieved_memories=[])

    # ── Print results ─────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"Failure type : {diagnosis.failure_type}")
    print(f"Confidence   : {diagnosis.confidence:.2f}")
    print(f"Source       : {diagnosis.source}")
    print(f"Explanation  :\n  {diagnosis.explanation}")
    if diagnosis.missing_evidence:
        print(f"Missing      : {diagnosis.missing_evidence}")
    print(f"{'═'*60}")

    # ── Assertions ────────────────────────────────────────────────────────────
    assert isinstance(diagnosis.failure_type, FailureType), (
        f"failure_type must be a FailureType, got {type(diagnosis.failure_type)}"
    )
    assert diagnosis.confidence > 0.0, "confidence must be positive"
    assert diagnosis.explanation, "explanation must not be empty"
    assert diagnosis.source == "LLM", "source must be LLM for ReAct agent"

    if expected is not None:
        assert diagnosis.failure_type == expected, (
            f"Expected {expected} but got {diagnosis.failure_type} "
            f"(confidence={diagnosis.confidence:.2f})\n"
            f"Explanation: {diagnosis.explanation}"
        )


@_requires_llm()
@_requires_cases()
@pytest.mark.parametrize("case", FIXTURE_CASES, ids=[c["id"] for c in FIXTURE_CASES])
@pytest.mark.asyncio
async def test_react_steps_visible(case: dict[str, Any]) -> None:
    """
    Verify the ReAct loop actually calls tools (not just immediately concludes).

    A good diagnosis uses at least 2 steps (one tool + conclude).
    This test checks the agent is reasoning, not short-circuiting.
    """
    from app.agents.diagnosis import DiagnosisAgent
    from app.llm.fake import FakeLLMProvider

    # Use a spy: real FakeLLMProvider that records calls but wraps real provider
    real_llm = build_llm_provider()

    class SpyLLM:
        def __init__(self, inner):
            self._inner = inner
            self.calls = []

        async def complete(self, messages, response_model, *, temperature=0.0):
            result = await self._inner.complete(messages, response_model, temperature=temperature)
            self.calls.append({
                "action": getattr(result, "action", None),
                "thought_len": len(getattr(result, "thought", "") or ""),
                "num_messages": len(messages),
            })
            return result

    submit_dir = Path(case["submit_dir"])
    evidence = collect_evidence(submit_dir, case["job_id"], case.get("condor_job_id"))
    ctx = _build_context(case["job_id"], case["exit_code"], str(submit_dir))

    spy = SpyLLM(real_llm)
    agent = DiagnosisAgent(spy)
    diagnosis = await agent.run(ctx, evidence, [])

    print(f"\nReAct steps taken: {len(spy.calls)}")
    for i, call in enumerate(spy.calls):
        print(f"  Step {i+1}: action={call['action']}  messages={call['num_messages']}")

    assert len(spy.calls) >= 2, (
        f"Expected at least 2 steps (one tool + conclude), got {len(spy.calls)}"
    )
    # Last step must be conclude
    assert spy.calls[-1]["action"] == "conclude", (
        f"Last step must be 'conclude', got {spy.calls[-1]['action']}"
    )
