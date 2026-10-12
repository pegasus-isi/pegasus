from __future__ import annotations

"""
Two-loop LangGraph remediation graph.

DIAGNOSTIC LOOP
  collect_context → run_rule_classifier ──(confident)──→ lookup_fix_catalog
                                         └──(ambiguous)─→ retrieve_memories → run_diagnosis_agent
                                                                                     ↓
  (confidence gate) ─── low ──→ escalate
                    └── high ──→ lookup_fix_catalog

ACTION LOOP
  lookup_fix_catalog ──(known fix)──→ validate_policy
                      └──(unknown)──→ run_fix_planner → validate_policy
  validate_policy ──AUTO──→ apply_fix → authorize_retry
                  ──ASK───→ [INTERRUPT: wait for human approval]
                  ──STOP/ESCALATE──→ escalate

  [retry outcome event arrives] → evaluate_outcome
  evaluate_outcome ──EFFECTIVE──────→ write_memory → (done)
                   ──INEFFECTIVE/NEW──→ (back to diagnostic loop, attempt+1)
                   ──LIMIT REACHED────→ escalate
"""

from typing import Any

from langgraph.graph import END, START, StateGraph

from Pegasus.healer.config import settings
from Pegasus.healer.graph.state import RemediationState
from Pegasus.healer.graph import nodes


# ── Routing functions (pure, no I/O) ─────────────────────────────────────────

def _route_entry(state: dict[str, Any]) -> str:
    """
    Dynamic entry point.

    Fresh invocation (first failure, no prior state):
      → collect_context

    Resume invocation (retried job just finished, prior fix was applied):
      → evaluate_outcome  (read current job exit code → EFFECTIVE or INEFFECTIVE)

    The SQLite checkpointer links both invocations via thread_id = workflow_id/job_id.
    """
    if state.get("retry_job_instance_id") is not None:
        return "evaluate_outcome"
    return "collect_context"


def _route_after_rule_classifier(state: dict[str, Any]) -> str:
    if state.get("insufficient_evidence_fields"):
        return "handle_insufficient_evidence"
    if state.get("diagnosis"):
        return "lookup_fix_catalog"
    return "retrieve_memories"


def _route_after_diagnosis_agent(state: dict[str, Any]) -> str:
    # Always proceed to lookup_fix_catalog regardless of confidence.
    # Low-confidence diagnoses still produce an ASK proposal for the human
    # rather than silently escalating with nothing actionable.
    return "lookup_fix_catalog"


def _route_after_fix_catalog(state: dict[str, Any]) -> str:
    if state.get("proposed_fix"):
        return "validate_policy"
    return "run_fix_planner"


def _route_after_policy(state: dict[str, Any]) -> str:
    decision = state.get("policy_decision", "STOP")
    if decision == "AUTO":
        return "apply_fix"
    if decision == "ASK":
        # Agent has a fix proposal but cannot apply it (scope insufficient,
        # or failure type requires human action). Generate the proposal report
        # so the human can apply it manually — no interactive confirmation.
        return "generate_proposal_report"
    return "escalate"             # STOP or ESCALATE


def _route_after_outcome(state: dict[str, Any]) -> str:
    outcome = state.get("retry_outcome", "INCONCLUSIVE")
    attempt = state.get("attempt", 1)

    if outcome == "EFFECTIVE":
        return "write_memory"

    if attempt >= settings.max_global_attempts:
        return "escalate"

    # Re-enter diagnostic loop for next attempt
    return "collect_context"


# ── Graph builder ─────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    """
    Construct and compile the two-loop remediation graph.

    Services (llm, policy, retry_controller, etc.) are injected at
    runtime via node kwargs — the graph itself has no external dependencies.
    """
    builder = StateGraph(RemediationState)

    # ── Register nodes ────────────────────────────────────────────────────────
    builder.add_node("collect_context", nodes.collect_context)
    builder.add_node("run_rule_classifier", nodes.run_rule_classifier)
    builder.add_node("retrieve_memories", nodes.retrieve_memories)
    builder.add_node("run_diagnosis_agent", nodes.run_diagnosis_agent)
    builder.add_node("lookup_fix_catalog", nodes.lookup_fix_catalog)
    builder.add_node("run_fix_planner", nodes.run_fix_planner)
    builder.add_node("validate_policy", nodes.validate_policy)
    builder.add_node("apply_fix", nodes.apply_fix)
    builder.add_node("authorize_retry", nodes.authorize_retry)
    builder.add_node("evaluate_outcome", nodes.evaluate_outcome)
    builder.add_node("generate_proposal_report", nodes.generate_proposal_report)
    builder.add_node("write_memory", nodes.write_memory)
    builder.add_node("handle_insufficient_evidence", nodes.handle_insufficient_evidence)
    builder.add_node("escalate", nodes.escalate)

    # ── Entry — dynamic: fresh failure vs resume after retry ──────────────────
    builder.add_conditional_edges(
        START,
        _route_entry,
        {
            "collect_context": "collect_context",
            "evaluate_outcome": "evaluate_outcome",
        },
    )
    builder.add_edge("collect_context", "run_rule_classifier")

    # ── Diagnostic loop routing ───────────────────────────────────────────────
    builder.add_conditional_edges(
        "run_rule_classifier",
        _route_after_rule_classifier,
        {
            "lookup_fix_catalog": "lookup_fix_catalog",
            "retrieve_memories": "retrieve_memories",
            "handle_insufficient_evidence": "handle_insufficient_evidence",
        },
    )
    builder.add_edge("retrieve_memories", "run_diagnosis_agent")
    builder.add_edge("run_diagnosis_agent", "lookup_fix_catalog")

    # ── Action loop routing ───────────────────────────────────────────────────
    builder.add_conditional_edges(
        "lookup_fix_catalog",
        _route_after_fix_catalog,
        {
            "validate_policy": "validate_policy",
            "run_fix_planner": "run_fix_planner",
        },
    )
    builder.add_edge("run_fix_planner", "validate_policy")

    builder.add_conditional_edges(
        "validate_policy",
        _route_after_policy,
        {
            "apply_fix": "apply_fix",
            "generate_proposal_report": "generate_proposal_report",
            "escalate": "escalate",
        },
    )

    builder.add_edge("apply_fix", "authorize_retry")
    # After authorize_retry the graph pauses (interrupt) waiting for
    # the retry outcome event to arrive via POST /incidents/{id}/outcome
    builder.add_edge("authorize_retry", END)

    # ── Outcome routing ───────────────────────────────────────────────────────
    # evaluate_outcome is called when the graph is resumed with the outcome
    builder.add_conditional_edges(
        "evaluate_outcome",
        _route_after_outcome,
        {
            "write_memory": "write_memory",
            "collect_context": "collect_context",
            "escalate": "escalate",
        },
    )

    # ── Terminal nodes ────────────────────────────────────────────────────────
    builder.add_edge("generate_proposal_report", END)
    builder.add_edge("write_memory", END)
    builder.add_edge("handle_insufficient_evidence", END)
    builder.add_edge("escalate", END)

    return builder


async def create_graph_with_checkpointer(
    pg_url: str = "",
    sqlite_path: str = "checkpoints.db",
) -> Any:
    """
    Build the graph with the appropriate checkpointer.

    PostgreSQL (production):
        Full durability — state survives restarts, approval waits, and
        cross-process resume. Requires a running PostgreSQL server.
        Activated when pg_url starts with "postgresql://".

    SQLite (default / no DB configured):
        File-based, no server required. Good for single-machine deployments
        and the POST script path where restarts between calls are not needed.
        Uses sqlite_path (default: checkpoints.db in the working directory).
    """
    graph = build_graph()

    import structlog
    _log = structlog.get_logger(__name__)

    if pg_url.startswith("postgresql://"):
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        checkpointer = await AsyncPostgresSaver.from_conn_string(pg_url)
        await checkpointer.setup()
        _log.info("checkpointer_postgres", url=pg_url)
    else:
        import aiosqlite
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
        conn = await aiosqlite.connect(sqlite_path)
        checkpointer = AsyncSqliteSaver(conn)
        await checkpointer.setup()
        _log.info("checkpointer_sqlite", path=sqlite_path)

    return graph.compile(checkpointer=checkpointer)
