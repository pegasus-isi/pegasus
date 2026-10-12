#!/usr/bin/env python3
"""
pegasus-healer — DAGMan POST script entry point.

Installed as a console script so it can be referenced directly in
pegasus.properties without a hard-coded script path:

    pegasus.dagman.post = pegasus-healer
    pegasus.dagman.post.arguments = $RETURN $JOB $RETRY $MAX_RETRIES

DAGMan sets CWD = submit_dir when running POST scripts. Pegasus writes
braindump.yml into submit_dir at plan time, so pegasus-healer reads
./braindump.yml to discover submit_dir (= os.getcwd()) and wf_uuid —
no extra arguments needed, making it a true drop-in for pegasus-exitcode.

EXIT CODE CONTRACT
──────────────────
  1 → DAGMan retries (AUTO fix applied)
  0 → DAGMan stops   (ASK, STOP, ESCALATE, or job succeeded)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

try:
    import yaml as _yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

try:
    from dotenv import load_dotenv as _load_dotenv
    _ENV = Path.home() / ".pegasus" / "healer.env"
    if _ENV.exists():
        _load_dotenv(_ENV, override=False)
except ImportError:
    pass

TAG = "[pegasus-healer]"
_log_file = None


# ── Per-job reasoning logger ──────────────────────────────────────────────────

try:
    from langchain_core.callbacks import BaseCallbackHandler as _BaseCallbackHandler
except ImportError:
    _BaseCallbackHandler = object  # type: ignore[assignment,misc]


class HealerReasoningLogger(_BaseCallbackHandler):
    """
    LangChain BaseCallbackHandler that writes every LLM exchange to a
    per-job  <job_id>.healer_reasoning.log  file.

    Captures:
      - Which agent/chain is running (diagnosis, fix-planner, …)
      - Full prompt / message list sent to the LLM
      - Full LLM response text
      - Token usage when available
    """

    def __init__(self, path: Path) -> None:
        if _BaseCallbackHandler is not object:
            super().__init__()
        self._f = open(path, "w", encoding="utf-8")
        self._section: str = "GRAPH"
        now = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self._write(
            f"{'═' * 70}\n"
            f"HEALER REASONING LOG\n"
            f"Generated : {now}\n"
            f"{'═' * 70}\n"
        )

    # ── helpers ───────────────────────────────────────────────────────────────

    def _write(self, text: str) -> None:
        try:
            self._f.write(text)
            self._f.flush()
        except Exception:
            pass

    def _section_header(self, title: str) -> None:
        self._write(f"\n── {title} {'─' * max(0, 66 - len(title))}\n")

    def _fmt_messages(self, messages: list) -> str:
        lines: list[str] = []
        for msg in messages:
            role = getattr(msg, "type", None) or getattr(msg, "role", "message")
            content = getattr(msg, "content", str(msg))
            if isinstance(content, list):
                content = "\n".join(
                    p.get("text", str(p)) if isinstance(p, dict) else str(p)
                    for p in content
                )
            lines.append(f"[{role.upper()}]\n{content}")
        return "\n\n".join(lines)

    # ── LangChain callback interface ──────────────────────────────────────────

    def on_chain_start(self, serialized, inputs, **kwargs):
        name = (serialized or {}).get("name") or (serialized or {}).get("id", ["?"])[-1]
        self._section = str(name)
        self._section_header(f"CHAIN START  {name}")

    def on_chain_end(self, outputs, **kwargs):
        self._section_header(f"CHAIN END  {self._section}")

    def on_chat_model_start(self, serialized, messages, **kwargs):
        name = (serialized or {}).get("name") or (serialized or {}).get("id", ["LLM"])[-1]
        self._section_header(f"LLM CALL  {name}  [{self._section}]")
        self._write("\n[PROMPT / MESSAGES]\n")
        for msg_list in messages:
            self._write(self._fmt_messages(msg_list))
            self._write("\n")

    def on_llm_start(self, serialized, prompts, **kwargs):
        name = (serialized or {}).get("name") or "LLM"
        self._section_header(f"LLM CALL  {name}  [{self._section}]")
        self._write("\n[PROMPT]\n")
        for p in prompts:
            self._write(p + "\n")

    def on_llm_end(self, response, **kwargs):
        self._write("\n[RESPONSE]\n")
        try:
            for gen_list in response.generations:
                for gen in gen_list:
                    text = getattr(gen, "text", None) or getattr(
                        getattr(gen, "message", None), "content", str(gen)
                    )
                    self._write(str(text) + "\n")
        except Exception as exc:
            self._write(f"(could not extract response text: {exc})\n")
        # Token usage
        try:
            usage = response.llm_output or {}
            token_info = usage.get("token_usage") or usage.get("usage") or {}
            if token_info:
                self._write(f"[TOKENS] {token_info}\n")
        except Exception:
            pass

    def on_llm_error(self, error, **kwargs):
        self._write(f"\n[LLM ERROR] {error}\n")

    def on_tool_start(self, serialized, input_str, **kwargs):
        name = (serialized or {}).get("name", "tool")
        self._write(f"\n  → TOOL {name}: {input_str}\n")

    def on_tool_end(self, output, **kwargs):
        self._write(f"  ← TOOL RESULT: {output}\n")

    def on_agent_action(self, action, **kwargs):
        self._write(f"\n  [ACTION] {action.tool}: {action.tool_input}\n")

    def on_agent_finish(self, finish, **kwargs):
        self._write(f"\n  [AGENT FINISH] {finish.return_values}\n")

    def close(self) -> None:
        try:
            self._f.close()
        except Exception:
            pass


def _read_braindump(submit_dir: Path) -> dict:
    """
    Read braindump.yml from submit_dir.

    DAGMan sets CWD = submit_dir when invoking POST scripts, so
    submit_dir is always Path(os.getcwd()) and braindump.yml is
    always at ./braindump.yml — no CLI args required.

    Returns an empty dict when the file is absent or unreadable.
    """
    path = submit_dir / "braindump.yml"
    if not path.exists():
        return {}
    try:
        if _HAS_YAML:
            import yaml
            return yaml.safe_load(path.read_text()) or {}
        # Minimal fallback parser for "key: value" lines
        result: dict = {}
        for line in path.read_text().splitlines():
            if ":" in line and not line.strip().startswith("#"):
                k, _, v = line.partition(":")
                result[k.strip()] = v.strip().strip('"').strip("'")
        return result
    except Exception:
        return {}


def _log(msg: str) -> None:
    line = f"{TAG} {msg}"
    print(line, file=sys.stderr, flush=True)
    if _log_file is not None:
        print(line, file=_log_file, flush=True)


def _find_job_subdir(submit_dir: Path, job_id: str) -> Path:
    try:
        from Pegasus.healer.collectors.submit_dir import _all_job_subdirs
        for jd in _all_job_subdirs(submit_dir):
            if (jd / f"{job_id}.sub").exists():
                return jd
    except Exception:
        pass
    return submit_dir


_reasoning_logger: HealerReasoningLogger | None = None


def _open_log(submit_dir: Path, job_id: str) -> None:
    global _log_file, _reasoning_logger
    job_dir = _find_job_subdir(submit_dir, job_id)
    try:
        _log_file = open(job_dir / f"{job_id}.healer.log", "a")
    except OSError:
        pass
    try:
        _reasoning_logger = HealerReasoningLogger(job_dir / f"{job_id}.healer_reasoning.log")
    except Exception:
        pass


def _marker_fast_path(
    submit_dir: Path,
    transformation: str | None,
    exit_code: int,
    sub_content: str | None,
) -> bool:
    if not transformation or not sub_content:
        return False
    try:
        from Pegasus.healer.pegasus.sibling_fixer import check_fast_path
        from Pegasus.healer.pegasus.sub_file import read_resource_requests
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sub", delete=False) as tmp:
            tmp.write(sub_content)
            tmp_path = Path(tmp.name)
        try:
            current_resources = read_resource_requests(tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)
        return check_fast_path(submit_dir, transformation, exit_code, current_resources)
    except Exception:
        return False


def _resolve_thread(
    submit_dir: Path,
    job_id: str,
    workflow_id: str,
    instance_id: int,
) -> tuple[str, bool]:
    job_dir = _find_job_subdir(submit_dir, job_id)
    thread_file = job_dir / f"{job_id}.healer_thread"
    if thread_file.exists():
        return thread_file.read_text().strip(), True
    thread_id = f"{workflow_id}/{job_id}/{instance_id}"
    thread_file.write_text(thread_id)
    return thread_id, False


def _find_policy(submit_dir: Path) -> str:
    """
    Locate policies/remediation.yaml.

    Priority:
      1. POLICY_FILE env var (absolute path)
      2. Walk up from submit_dir until policies/remediation.yaml is found
      3. Relative to the package install root (parents[4] from __file__)
      4. Bare relative path as last resort (load_policy handles missing file)
    """
    if env_path := os.environ.get("POLICY_FILE"):
        return env_path
    for parent in [submit_dir, *submit_dir.parents]:
        candidate = parent / "policies" / "remediation.yaml"
        if candidate.exists():
            return str(candidate)
    install_candidate = Path(__file__).resolve().parents[4] / "policies" / "remediation.yaml"
    if install_candidate.exists():
        return str(install_candidate)
    return "policies/remediation.yaml"


async def _run(args: argparse.Namespace) -> int:
    from Pegasus.healer.collectors.submit_dir import (
        collect_evidence, parse_instance_id, parse_healer_tags,
    )
    from Pegasus.healer.policies.loader import load_policy
    from Pegasus.healer.policies.engine import PolicyEngine
    from Pegasus.healer.pegasus.dagman_retry_controller import DAGManRetryController
    from Pegasus.healer.pegasus.sibling_fixer import _parse_transformation_from_content
    from Pegasus.healer.llm.universal_provider import UniversalProvider
    from Pegasus.healer.graph.graph import create_graph_with_checkpointer
    from Pegasus.healer.memory.sqlite_repo import SQLiteMemoryRepo

    submit_dir = Path(args.submit_dir)
    instance_id = args.job_instance_id or parse_instance_id(args.job_id)

    _log("collecting evidence...")
    evidence = collect_evidence(
        submit_dir=submit_dir,
        job_id=args.job_id,
        instance_id=instance_id,
        condor_job_id=args.condor_job_id,
    )
    _log(f"evidence: {evidence.available_sources}")
    if _reasoning_logger:
        _reasoning_logger._write(
            f"Job       : {args.job_id}\n"
            f"Workflow  : {args.workflow_id}\n"
            f"Exit code : {args.exit_code}\n"
            f"Retry     : {args.retry_number}/{args.max_retries}\n"
            f"Submit dir: {submit_dir}\n"
        )
        _reasoning_logger._section_header("EVIDENCE COLLECTED")
        _reasoning_logger._write(f"Sources: {evidence.available_sources}\n")
        if evidence.stderr_content:
            _reasoning_logger._write(f"\n[STDERR (last 50 lines)]\n")
            _reasoning_logger._write("\n".join(evidence.stderr_content.splitlines()[-50:]) + "\n")
        if evidence.transformation_script_content:
            _reasoning_logger._write(f"\n[TRANSFORMATION SCRIPT: {evidence.transformation_script_path}]\n")
            _reasoning_logger._write(evidence.transformation_script_content + "\n")

    transformation = args.transformation
    if not transformation and evidence.sub_file_content:
        transformation = _parse_transformation_from_content(evidence.sub_file_content)

    if evidence.sub_file_content:
        tags = parse_healer_tags(evidence.sub_file_content)
        if "stop" in tags:
            _log("tag 'stop': terminal — aborting workflow")
            return 0

    if _marker_fast_path(submit_dir, transformation, args.exit_code,
                         evidence.sub_file_content):
        _log("marker fast path: fix already applied → retrying with patched .sub")
        return 1

    thread_id, is_resume = _resolve_thread(
        submit_dir, args.job_id, args.workflow_id, instance_id
    )
    _log(f"{'resuming' if is_resume else 'fresh'} thread: {thread_id}")

    _policy_path = _find_policy(submit_dir)
    policy = load_policy(_policy_path)
    policy_engine = PolicyEngine(policy)

    retry_ctrl = DAGManRetryController(
        submit_dir=str(submit_dir),
        job_id=args.job_id,
        job_instance_id=instance_id,
    )

    def _llm(model_env: str, key_env: str, url_env: str) -> UniversalProvider:
        return UniversalProvider(
            model=os.environ.get(model_env) or os.environ.get("LLM_MODEL", ""),
            api_key=os.environ.get(key_env) or os.environ.get("LLM_API_KEY", ""),
            base_url=os.environ.get(url_env) or os.environ.get("LLM_BASE_URL", ""),
        )

    llm           = _llm("LLM_MODEL",            "LLM_API_KEY",            "LLM_BASE_URL")
    diagnosis_llm = _llm("DIAGNOSIS_LLM_MODEL",   "DIAGNOSIS_LLM_API_KEY",  "DIAGNOSIS_LLM_BASE_URL")
    fix_llm       = _llm("FIX_PLANNING_LLM_MODEL","FIX_PLANNING_LLM_API_KEY","FIX_PLANNING_LLM_BASE_URL")

    _pegasus_dir = Path.home() / ".pegasus"
    _pegasus_dir.mkdir(exist_ok=True)
    graph = await create_graph_with_checkpointer(
        sqlite_path=os.environ.get(
            "HEALER_CHECKPOINT", str(_pegasus_dir / "healer_checkpoint.db")
        ),
    )

    _mem_db = os.environ.get("HEALER_MEMORY_DB") or str(_pegasus_dir / "healer_memory.db")
    memory_repo = SQLiteMemoryRepo(db_path=_mem_db)
    _log(f"memory_repo: {_mem_db}")

    _callbacks = [_reasoning_logger] if _reasoning_logger else []
    config = {
        "configurable": {
            "thread_id":        thread_id,
            "llm":              llm,
            "diagnosis_llm":    diagnosis_llm,
            "fix_planning_llm": fix_llm,
            "policy":           policy,
            "policy_engine":    policy_engine,
            "retry_controller": retry_ctrl,
            "submit_dir":       str(submit_dir),
            "raw_evidence":     evidence.model_dump(),
            "memory_repo":      memory_repo,
        },
        "callbacks": _callbacks,
    }

    if is_resume:
        outcome = "EFFECTIVE" if args.exit_code == 0 else "INEFFECTIVE"
        input_data: dict = {"retry_outcome": outcome}
        _log(f"retry outcome: {outcome}")
    else:
        input_data = {
            "incident_id":            str(uuid4()),
            "workflow_id":            args.workflow_id,
            "job_id":                 args.job_id,
            "source_job_instance_id": instance_id,
            "exit_code":              args.exit_code,
            "scheduler_id":           args.condor_job_id,
            "attempt":                1,
        }

    _log("running remediation graph...")
    final_state = await graph.ainvoke(input_data, config=config)
    decision = final_state.get("policy_decision")
    _log(f"decision={decision} terminal={final_state.get('terminal', False)}")

    if _reasoning_logger:
        _reasoning_logger._section_header("POLICY DECISION")
        _reasoning_logger._write(f"Decision : {decision}\n")
        pr = final_state.get("policy_record") or {}
        if pr:
            _reasoning_logger._write(f"Passed   : {pr.get('checks_passed', [])}\n")
            _reasoning_logger._write(f"Failed   : {pr.get('checks_failed', [])}\n")
            _reasoning_logger._write(f"Reason   : {pr.get('reason', '')}\n")

    def _exit(code: int) -> int:
        if _reasoning_logger:
            _reasoning_logger._section_header("OUTCOME")
            _reasoning_logger._write(f"Exit code returned to DAGMan: {code}\n")
            _reasoning_logger._write(
                "(1=retry, 0=stop/success, other=terminal failure)\n"
            )
            _reasoning_logger.close()
        return code

    if final_state.get("retry_outcome") == "EFFECTIVE":
        _log("exit 0 → effective outcome, workflow continues")
        return _exit(0)
    if decision == "AUTO":
        _log("exit 1 → DAGMan retries with patched .sub")
        return _exit(1)
    if decision == "ASK":
        if args.exit_code != 0:
            _log(f"exit {args.exit_code} → proposal written; apply fix manually")
            return _exit(args.exit_code)
        _log("exit 0 → proposal in report; apply manually and re-submit")
        return _exit(0)
    if args.exit_code != 0:
        _log(f"exit {args.exit_code} → terminal, job failed")
        return _exit(args.exit_code)
    _log("exit 0 → terminal")
    return _exit(0)


def _run_graph(args: argparse.Namespace) -> int:
    try:
        return asyncio.run(_run(args))
    except Exception as exc:
        import traceback
        _log(f"ERROR: remediation graph raised {type(exc).__name__}: {exc}")
        traceback.print_exc(file=sys.stderr)
        if _log_file is not None:
            traceback.print_exc(file=_log_file)
        return args.exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="pegasus-healer",
        description="Pegasus DAGMan POST script — LangGraph remediation agent",
    )
    parser.add_argument("exit_code",    type=int, help="Job exit code ($RETURN)")
    parser.add_argument("job_id",                 help="DAGMan node name ($JOB)")
    parser.add_argument("retry_number", type=int, help="Current retry ($RETRY)")
    parser.add_argument("max_retries",  type=int, help="Max retries ($MAX_RETRIES)")
    # submit_dir and workflow_id are discovered from braindump.yml (CWD = submit_dir)
    # but can be overridden via optional flags when invoking outside of DAGMan.
    parser.add_argument("--submit-dir",  dest="submit_dir",  default=None,
                        help="Override submit directory (default: CWD, set by DAGMan)")
    parser.add_argument("--workflow-id", dest="workflow_id", default=None,
                        help="Override workflow UUID (default: read from braindump.yml)")
    parser.add_argument("--job-instance-id", dest="job_instance_id", type=int, default=None)
    parser.add_argument("--condor-job-id",   dest="condor_job_id",   default=None)
    parser.add_argument("--execution-site",  dest="execution_site",  default=None)
    parser.add_argument("--transformation",  default=None)
    # Pegasus always appends the job's .out file as a 5th positional arg to
    # every POST script.  Accept it so argparse doesn't fail with exit 2.
    parser.add_argument("kickstart_out", nargs="?", default=None,
                        help="kickstart .out file appended by Pegasus (ignored)")
    args = parser.parse_args()

    # Resolve submit_dir: flag > CWD (DAGMan always sets CWD = submit_dir)
    submit_dir = Path(args.submit_dir) if args.submit_dir else Path(os.getcwd())
    args.submit_dir = str(submit_dir)

    # Resolve workflow_id: flag > braindump.yml > fallback empty string
    if not args.workflow_id:
        braindump = _read_braindump(submit_dir)
        args.workflow_id = braindump.get("wf_uuid", "")

    _open_log(submit_dir, args.job_id)
    _log(
        f"invoked: exit_code={args.exit_code} "
        f"retry={args.retry_number}/{args.max_retries} job={args.job_id} "
        f"wf={args.workflow_id}"
    )

    # Pegasus infrastructure jobs — pass exit code through unchanged so DAGMan
    # gets the real result and does not mis-mark a failed staging/cleanup job
    # as succeeded.  These jobs never benefit from LLM remediation.
    _INFRA_PREFIXES = (
        "stage_in_", "stage_out_", "create_dir_", "cleanup_", "register_",
        "chmod_", "pegasus_concat_",
    )
    if any(args.job_id.startswith(p) for p in _INFRA_PREFIXES):
        _log(f"infrastructure job — passing exit_code={args.exit_code} through")
        return args.exit_code

    thread_file = (
        _find_job_subdir(submit_dir, args.job_id) / f"{args.job_id}.healer_thread"
    )

    if args.exit_code == 0 and not thread_file.exists():
        return 0

    if args.retry_number >= args.max_retries:
        _log("retry budget exhausted — no agent run")
        return args.exit_code

    return _run_graph(args)


if __name__ == "__main__":
    sys.exit(main())
