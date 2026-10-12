from __future__ import annotations

"""
Collect all diagnostic evidence from a Pegasus submit directory.

Used by:
  - scripts/pegasus_post_script.py  (on the submit host after job failure)
  - tests/integration/              (against real fixture submit directories)

The collection runs synchronously because it is called from the POST script
(a short-lived subprocess) and from tests.  No async needed here.
"""

import glob
import json
import os
import re
from pathlib import Path

from Pegasus.healer.models.evidence import InputFileStatus, RawEvidence
from Pegasus.healer.scheduler.factory import get_scheduler_client, get_pegasus_client
from Pegasus.healer.scheduler.pegasus import run_pegasus_analyzer as _pegasus_analyzer

MAX_FILE_BYTES = 200_000   # 200 KB per file cap


# ── Internal helpers ──────────────────────────────────────────────────────────

def _read_file(path: Path, max_bytes: int = MAX_FILE_BYTES) -> str | None:
    """Read a file and return its content, or None if missing / unreadable."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        if len(text) > max_bytes:
            return f"[... {len(text) - max_bytes} chars truncated ...]\n" + text[-max_bytes:]
        return text
    except OSError:
        return None


def _all_job_subdirs(submit_dir: Path) -> list[Path]:
    """
    Return all XX/YY job subdirectories found under submit_dir, sorted.

    Pegasus distributes jobs across multiple buckets to avoid filesystem
    limits: 00/00, 00/01, 00/02, ..., 01/00, 01/01, ...
    Falls back to the submit_dir root for flat/legacy layouts.
    """
    subdirs = sorted(submit_dir.glob("[0-9][0-9]/[0-9][0-9]"))
    return subdirs if subdirs else [submit_dir]


def _find_job_file(
    submit_dir: Path,
    job_id: str,
    instance_id: int,
    ext: str,
) -> str | None:
    """
    Locate a per-job file searching all XX/YY subdirectories.

    Pegasus 5.x with _ID suffix:  submit_dir/XX/YY/{job_id}_ID{n:07d}.{ext}
    Pegasus BLAH/grid jobs:        submit_dir/XX/YY/{job_id}.{ext}.000
    Older Pegasus:                 submit_dir/XX/YY/{job_id}.{ext}
    Legacy flat layout:            submit_dir/{job_id}.{ext}
    """
    for job_dir in _all_job_subdirs(submit_dir):
        candidates = [
            job_dir / f"{job_id}_ID{instance_id:07d}.{ext}",
            job_dir / f"{job_id}.{ext}.000",
            job_dir / f"{job_id}.{ext}",
        ]
        for p in candidates:
            content = _read_file(p)
            if content is not None:
                return content

    # Flat fallback (submit_dir root)
    for name in (
        f"{job_id}_ID{instance_id:07d}.{ext}",
        f"{job_id}.{ext}.000",
        f"{job_id}.{ext}",
    ):
        content = _read_file(submit_dir / name)
        if content is not None:
            return content

    return None


# ── Public API ────────────────────────────────────────────────────────────────

def parse_instance_id(job_name: str) -> int:
    """Extract the Pegasus instance number from a DAG node name.

    Pegasus encodes it as  {name}_ID{instance:07d}.
    Example: myjob_ID0000001 → 1.  Falls back to 1 if absent.
    """
    m = re.search(r"_ID(\d+)$", job_name)
    return int(m.group(1)) if m else 1


def collect_job_files(
    submit_dir: Path,
    job_id: str,
    instance_id: int,
) -> dict[str, str | None]:
    """Read all per-job files from the Pegasus submit directory."""
    return {
        "stderr_content":    _find_job_file(submit_dir, job_id, instance_id, "err"),
        "stdout_content":    _find_job_file(submit_dir, job_id, instance_id, "out"),
        "sub_file_content":  _find_job_file(submit_dir, job_id, instance_id, "sub"),
        "event_log_content": _find_job_file(submit_dir, job_id, instance_id, "log"),
    }


def collect_workflow_files(submit_dir: Path) -> dict[str, str | None]:
    """Read workflow-level log files from the submit directory root."""
    result: dict[str, str | None] = {
        "dagman_log_content":   None,
        "workflow_log_content": None,
    }
    # DAGMan log — pick the most recently modified *.dag.dagman.out
    dagman_files = sorted(
        glob.glob(str(submit_dir / "*.dag.dagman.out")),
        key=os.path.getmtime,
        reverse=True,
    )
    if dagman_files:
        result["dagman_log_content"] = _read_file(Path(dagman_files[0]))

    result["workflow_log_content"] = _read_file(submit_dir / "workflow.log")
    return result


def run_pegasus_analyzer(submit_dir: Path, job_id: str | None = None) -> str | None:
    """
    Run pegasus-analyzer via PegasusClient wrapper.

    When job_id is provided the analysis is scoped to that job node (--job flag).
    Returns None when pegasus-analyzer is not on PATH or times out.
    """
    return _pegasus_analyzer(submit_dir, job_id)


def run_condor_history(condor_job_id: str) -> str | None:
    """
    Query condor_history via the scheduler client.

    Returns a JSON string of the raw classad dict, or None when the job
    is not found or the scheduler is unavailable.
    """
    if not condor_job_id:
        return None
    history = get_scheduler_client().get_job_history(condor_job_id)
    if not history.raw:
        return None
    return json.dumps(history.raw)


def collect_transformation_script(
    sub_file_content: str | None,
    submit_dir: Path,
    max_bytes: int = MAX_FILE_BYTES,
) -> tuple[str | None, str | None]:
    """
    Locate and read the transformation script declared in the .sub file's
    ``executable`` key.  Only succeeds when the script is accessible from the
    submit host (absolute path exists, or path relative to submit_dir exists).

    Returns (content, resolved_absolute_path) or (None, None).
    """
    if not sub_file_content:
        return None, None

    m = re.search(r"^\s*executable\s*=\s*(.+)$", sub_file_content, re.MULTILINE | re.IGNORECASE)
    if not m:
        return None, None

    executable = m.group(1).strip()
    script_path = Path(executable)
    if not script_path.is_absolute():
        script_path = submit_dir / executable

    if not script_path.exists() or not script_path.is_file():
        return None, None

    content = _read_file(script_path, max_bytes)
    return content, str(script_path.resolve())


def parse_slurm_partition(sub_file_content: str | None) -> str | None:
    """
    Extract the SLURM partition name from a Pegasus .sub file.

    Pegasus writes the target partition into the submit file differently
    depending on the CE type used for SLURM submission:

      batch_queue = <partition>          ← CREAM CE / BLAHP / SLURM-CE
      +remote_queue = "<partition>"      ← HTCondor-CE grid universe

    Returns the partition name (without quotes), or None when neither
    attribute is present (HTCondor-native job, no SLURM routing).
    """
    if not sub_file_content:
        return None
    # Try batch_queue first (most common in Pegasus SLURM submissions)
    m = re.search(
        r"^\s*batch_queue\s*=\s*([^\s#\n]+)",
        sub_file_content,
        re.MULTILINE | re.IGNORECASE,
    )
    if m:
        return m.group(1).strip().strip('"').strip("'")
    # Fall back to +remote_queue (HTCondor-CE grid universe)
    m = re.search(
        r'^\s*\+remote_queue\s*=\s*["\']?([^"\'#\s\n]+)["\']?',
        sub_file_content,
        re.MULTILINE | re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    return None


def collect_partition_profile(
    current_partition: str | None,
) -> list[dict]:
    """
    Collect SLURM partition resource limits via PegasusClient.

    Returns a list of serialisable dicts (one per partition) that the fix
    catalog and LLM agents can inspect without importing scheduler types.

    All SLURM CLI calls (scontrol / sinfo) go through PegasusClient →
    SLURMClient._run() — no direct subprocess calls here.

    Returns an empty list when:
      - the cluster is not SLURM (HTCondor, or no sinfo on PATH), or
      - PegasusClient.get_partition_profile() returns nothing.
    """
    try:
        client = get_pegasus_client()
        partitions = client.get_partition_profile()
        return [
            {
                "name": p.name,
                "max_mem_mb": p.max_mem_mb,
                "default_mem_mb": p.default_mem_mb,
                "max_time_seconds": p.max_time_seconds,
                "accessible": p.accessible,
            }
            for p in partitions
        ]
    except Exception:
        return []


def parse_healer_tags(sub_file_content: str | None) -> list[str]:
    """
    Parse +PegasusHealerTags from a .sub file.

    The tag line looks like:
        +PegasusHealerTags = "no-fix,stop-jobs"

    Returns a list of normalised tag strings (lowercased, stripped).
    Returns an empty list when no tags are present or content is None.
    """
    if not sub_file_content:
        return []
    m = re.search(
        r'^\s*\+PegasusHealerTags\s*=\s*["\']?([^"\'#\n]+)["\']?',
        sub_file_content,
        re.MULTILINE | re.IGNORECASE,
    )
    if not m:
        return []
    return [t.strip().lower() for t in m.group(1).split(",") if t.strip()]


def validate_input_files(sub_file_content: str | None) -> list[InputFileStatus]:
    """
    Parse ``transfer_input_files`` from the .sub file and check whether each
    listed file is accessible on the submit host.

    Handles comma-separated lists and backslash line continuations.
    """
    if not sub_file_content:
        return []

    m = re.search(
        r"^\s*transfer_input_files\s*=\s*(.+?)(?=\n\S|\Z)",
        sub_file_content,
        re.MULTILINE | re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return []

    raw = m.group(1).replace("\\\n", " ")
    files = [f.strip() for f in raw.split(",") if f.strip()]

    results: list[InputFileStatus] = []
    for file_path_str in files:
        path = Path(file_path_str)
        try:
            if path.exists():
                size = path.stat().st_size
                results.append(InputFileStatus(
                    path=file_path_str,
                    exists=True,
                    size_bytes=size,
                    is_empty=(size == 0),
                    error="file is empty (0 bytes)" if size == 0 else None,
                ))
            else:
                results.append(InputFileStatus(
                    path=file_path_str,
                    exists=False,
                    error="file not found on submit host",
                ))
        except OSError as exc:
            results.append(InputFileStatus(
                path=file_path_str,
                exists=False,
                error=str(exc),
            ))
    return results


def collect_evidence(
    submit_dir: str | Path,
    job_id: str,
    instance_id: int | None = None,
    condor_job_id: str | None = None,
) -> RawEvidence:
    """
    Full evidence collection — runs pegasus-analyzer, reads all job and
    workflow files, and optionally queries condor_history.

    This is the single entry point used by both the POST script and tests.
    """
    submit_path = Path(submit_dir)
    if instance_id is None:
        instance_id = parse_instance_id(job_id)

    job_files = collect_job_files(submit_path, job_id, instance_id)
    wf_files  = collect_workflow_files(submit_path)
    analyzer  = run_pegasus_analyzer(submit_path, job_id)
    classads  = run_condor_history(condor_job_id) if condor_job_id else None

    sub_content = job_files.get("sub_file_content")
    script_content, script_path = collect_transformation_script(sub_content, submit_path)
    input_checks = validate_input_files(sub_content)

    # SLURM partition profile — all CLI calls go through PegasusClient
    current_partition = parse_slurm_partition(sub_content)
    partition_profile = collect_partition_profile(current_partition)

    return RawEvidence(
        pegasus_analyzer_output=analyzer,
        condor_classads_raw=classads,
        transformation_script_content=script_content,
        transformation_script_path=script_path,
        input_validation=input_checks,
        current_partition=current_partition,
        partition_profile=partition_profile,
        **job_files,
        **wf_files,
    )
