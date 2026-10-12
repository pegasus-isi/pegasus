from __future__ import annotations

import os
from pathlib import Path

import structlog

from Pegasus.healer.config import settings
from Pegasus.healer.models.context import ArtifactRef

log = structlog.get_logger(__name__)


def _read_excerpt(path: Path, max_bytes: int) -> tuple[str, bool]:
    """Read up to max_bytes from a file. Returns (content, truncated)."""
    try:
        raw = path.read_bytes()
        if len(raw) > max_bytes:
            return raw[:max_bytes].decode("utf-8", errors="replace"), True
        return raw.decode("utf-8", errors="replace"), False
    except OSError as exc:
        log.warning("file_read_error", path=str(path), error=str(exc))
        return "", False


def _find_artifact(candidates: list[Path], max_bytes: int) -> ArtifactRef | None:
    for path in candidates:
        if path.exists():
            size = path.stat().st_size
            excerpt, truncated = _read_excerpt(path, max_bytes)
            return ArtifactRef(
                path=str(path),
                size_bytes=size,
                truncated=truncated,
                excerpt=excerpt if excerpt else None,
            )
    return None


def _glob_artifact(base: Path, pattern: str, max_bytes: int) -> ArtifactRef | None:
    """Find the first file matching a glob pattern under base."""
    matches = sorted(base.glob(pattern))
    if not matches:
        return None
    path = matches[0]
    size = path.stat().st_size
    excerpt, truncated = _read_excerpt(path, max_bytes)
    return ArtifactRef(
        path=str(path),
        size_bytes=size,
        truncated=truncated,
        excerpt=excerpt if excerpt else None,
    )


# ---------------------------------------------------------------------------
# Helper: find all Pegasus XX/YY job subdirectories
# ---------------------------------------------------------------------------

def _job_subdirs(submit_dir: Path) -> list[Path]:
    """
    Return all Pegasus job subdirectories under submit_dir, sorted.

    Pegasus distributes jobs across XX/YY buckets to avoid filesystem
    inode limits: 00/00, 00/01, ..., 01/00, 01/01, ...
    Falls back to [submit_dir] for flat/legacy layouts.
    """
    subdirs = sorted(submit_dir.glob("[0-9][0-9]/[0-9][0-9]"))
    return subdirs if subdirs else [submit_dir]


def _job_candidates(submit_dir: Path, job_id: str, instance_id: int, ext: str) -> list[Path]:
    """
    Build the full candidate list for a per-job file across all XX/YY subdirs.

    Tries (in order for each subdir):
      {XX/YY}/{job_id}_ID{instance:07d}.{ext}   Pegasus 5.x with _ID suffix
      {XX/YY}/{job_id}.{ext}                     Pegasus 5.x without _ID
    Then flat fallbacks at submit_dir root:
      {job_id}_ID{instance:07d}.{ext}
      {job_id}.{ext}
      {job_id}.{instance:03d}.{ext}              legacy three-digit suffix
    """
    seen: set[Path] = set()
    candidates: list[Path] = []

    def _add(p: Path) -> None:
        if p not in seen:
            seen.add(p)
            candidates.append(p)

    for sd in _job_subdirs(submit_dir):
        _add(sd / f"{job_id}_ID{instance_id:07d}.{ext}")
        _add(sd / f"{job_id}.{ext}")
    # Flat fallbacks (already covered when _job_subdirs returns [submit_dir])
    _add(submit_dir / f"{job_id}_ID{instance_id:07d}.{ext}")
    _add(submit_dir / f"{job_id}.{ext}")
    _add(submit_dir / f"{job_id}.{instance_id:03d}.{ext}")
    return candidates


# ---------------------------------------------------------------------------
# Per-job file collectors
# ---------------------------------------------------------------------------

def collect_stdout(
    submit_dir: str,
    workflow_id: str,
    job_id: str,
    job_instance_id: int,
) -> ArtifactRef | None:
    """Locate and return a reference to the job's HTCondor stdout file."""
    return _find_artifact(
        _job_candidates(Path(submit_dir), job_id, job_instance_id, "out"),
        settings.log_excerpt_max_bytes,
    )


def collect_stderr(
    submit_dir: str,
    workflow_id: str,
    job_id: str,
    job_instance_id: int,
) -> ArtifactRef | None:
    """Locate and return a reference to the job's HTCondor stderr file."""
    return _find_artifact(
        _job_candidates(Path(submit_dir), job_id, job_instance_id, "err"),
        settings.log_excerpt_max_bytes,
    )


def collect_kickstart(
    submit_dir: str,
    workflow_id: str,
    job_id: str,
    job_instance_id: int,
) -> ArtifactRef | None:
    """Locate and return a reference to the Pegasus/Kickstart record.

    In Pegasus 5.x, kickstart writes YAML to the HTCondor stdout (.out).
    Some deployments also write a separate .kickstart.out or .kickstart.xml.
    """
    sd = Path(submit_dir)
    candidates: list[Path] = []
    for subdir in _job_subdirs(sd):
        candidates.append(subdir / f"{job_id}_ID{job_instance_id:07d}.kickstart.out")
        candidates.append(subdir / f"{job_id}.kickstart.xml")
    candidates.append(sd / f"{job_id}_ID{job_instance_id:07d}.kickstart.out")
    candidates.append(sd / f"{job_id}.{job_instance_id:03d}.kickstart.xml")
    candidates.append(sd / f"{job_id}.kickstart.xml")
    return _find_artifact(candidates, settings.kickstart_excerpt_max_bytes)


def collect_condor_event_log(
    submit_dir: str,
    job_id: str,
    job_instance_id: int,
) -> ArtifactRef | None:
    """Locate and return a reference to the HTCondor job event log (.log).

    The event log records submission, execution start, transfer, and
    completion events in HTCondor's ClassAd event format.
    """
    return _find_artifact(
        _job_candidates(Path(submit_dir), job_id, job_instance_id, "log"),
        settings.log_excerpt_max_bytes,
    )


def collect_submit_file(
    submit_dir: str,
    job_id: str,
    job_instance_id: int,
) -> ArtifactRef | None:
    """Locate and return a reference to the HTCondor submit file (.sub).

    The submit file is the ground-truth record of resource requests
    (RequestMemory, RequestDisk, RequestCpus, +ProjectName, etc.)
    as submitted to HTCondor.
    """
    return _find_artifact(
        _job_candidates(Path(submit_dir), job_id, job_instance_id, "sub"),
        settings.log_excerpt_max_bytes,
    )


# ---------------------------------------------------------------------------
# Workflow-level log collectors
# ---------------------------------------------------------------------------

def collect_dagman_out(submit_dir: str, workflow_name: str | None = None) -> ArtifactRef | None:
    """Locate the DAGMan stdout log (*.dag.dagman.out or *.dagman.out).

    DAGMan records node start/finish events, retry decisions, and PRE/POST
    script outputs.  Useful for understanding workflow-level retry context.
    """
    sd = Path(submit_dir)
    search_roots = _job_subdirs(sd) + ([sd] if sd not in _job_subdirs(sd) else [])

    for root in search_roots:
        if not root.exists():
            continue
        if workflow_name:
            ref = _find_artifact(
                [root / f"{workflow_name}.dag.dagman.out"],
                settings.log_excerpt_max_bytes,
            )
            if ref:
                return ref
        ref = _glob_artifact(root, "*.dagman.out", settings.log_excerpt_max_bytes)
        if ref:
            return ref
    return None


def collect_dagman_err(submit_dir: str, workflow_name: str | None = None) -> ArtifactRef | None:
    """Locate the DAGMan stderr log (*.dag.dagman.err or *.dagman.err)."""
    sd = Path(submit_dir)
    search_roots = _job_subdirs(sd) + ([sd] if sd not in _job_subdirs(sd) else [])

    for root in search_roots:
        if not root.exists():
            continue
        if workflow_name:
            ref = _find_artifact(
                [root / f"{workflow_name}.dag.dagman.err"],
                settings.log_excerpt_max_bytes,
            )
            if ref:
                return ref
        ref = _glob_artifact(root, "*.dagman.err", settings.log_excerpt_max_bytes)
        if ref:
            return ref
    return None


def collect_workflow_log(submit_dir: str) -> ArtifactRef | None:
    """Locate the Pegasus workflow log (workflow.log).

    Written by pegasus-monitord; records stampede events, job state
    transitions, and Pegasus-level annotations.
    """
    sd = Path(submit_dir)
    candidates = [
        sd / "workflow.log",
        sd.parent / "workflow.log",
    ]
    return _find_artifact(candidates, settings.log_excerpt_max_bytes)


def collect_monitord_log(submit_dir: str) -> ArtifactRef | None:
    """Locate the pegasus-monitord daemon log (monitord.log).

    Records AMQP publish events, parsing errors, and connectivity issues
    that can cause silent monitoring failures.
    """
    sd = Path(submit_dir)
    candidates = [
        sd / "monitord.log",
        sd.parent / "monitord.log",
        # Some versions write it one level above submit_dir
        sd.parent.parent / "monitord.log",
    ]
    return _find_artifact(candidates, settings.log_excerpt_max_bytes)


# ---------------------------------------------------------------------------
# Input file accessibility check
# ---------------------------------------------------------------------------

def check_file_accessible(logical_filename: str, replica_paths: list[str]) -> bool:
    """Return True if at least one physical replica is readable."""
    for phys in replica_paths:
        if os.access(phys, os.R_OK):
            return True
    return False
