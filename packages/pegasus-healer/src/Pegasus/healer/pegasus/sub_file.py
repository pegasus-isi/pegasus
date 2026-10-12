from __future__ import annotations

"""
Patch an HTCondor submit file (.sub) in-place with new resource requests.

HTCondor submit files are key = value text files (case-insensitive keys).
A Pegasus-generated submit file looks like:

    universe                = vanilla
    executable              = /usr/bin/pegasus-kickstart
    request_memory          = 4096
    request_disk            = 2048
    request_cpus            = 4
    +MaxRuntime             = 3600
    ...
    queue

This module maps FixProposal.proposed_configuration keys → submit file
attribute names and patches them in-place, preserving all other content.
"""

from pathlib import Path

import structlog

log = structlog.get_logger(__name__)

# FailureContext / FixProposal config key → HTCondor submit attribute(s) to patch.
# Pegasus .sub files use both request_* (what HTCondor schedules on) and
# pegasus_* ClassAds (Pegasus metadata). When the sub uses
#   request_memory = $(pegasus_memory_mb)
# patching only request_memory breaks on the next retry because the template
# re-expands. We patch both so the fix survives across all retries.
_ATTR_MAP: dict[str, list[str]] = {
    "memory_mb":       ["request_memory", "pegasus_memory_mb"],
    "disk_mb":         ["request_disk",   "pegasus_diskspace_mb"],
    "cpus":            ["request_cpus",   "pegasus_cores"],
    "runtime_seconds": ["+MaxRuntime",    "pegasus_job_runtime"],
    # SLURM partition migration — both CE types are patched so the fix works
    # regardless of whether the job was routed via BLAHP/CREAM CE (batch_queue)
    # or HTCondor-CE grid universe (+remote_queue).
    "target_partition": ["batch_queue", "+remote_queue"],
}

# Attributes whose values must be written as quoted strings in the .sub file.
# +remote_queue expects:  +remote_queue = "highmem"
# batch_queue expects:    batch_queue = highmem   (no quotes)
_QUOTED_ATTRS: frozenset[str] = frozenset({"remote_queue"})


def apply_overlay_to_sub_file(
    sub_path: Path,
    new_config: dict,
) -> dict[str, str]:
    """
    Patch resource request values in an HTCondor submit file.

    For each key in new_config that maps to a known submit attribute:
      - If the attribute already exists in the file, the line is updated.
      - If not, the attribute is inserted immediately before the final
        ``queue`` statement.

    Returns a dict of {htcondor_attr: new_value_str} for every change made.
    Lines beginning with # and blank lines are left untouched.
    """
    text = sub_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    changes: dict[str, str] = {}

    for config_key, raw_value in new_config.items():
        if raw_value is None:
            continue
        attrs = _ATTR_MAP.get(config_key)
        if not attrs:
            continue

        new_value = str(int(raw_value)) if isinstance(raw_value, float) else str(raw_value)

        for htcondor_attr in attrs:
            match_key = htcondor_attr.lstrip("+").lower()
            # +remote_queue requires a quoted string value; batch_queue does not.
            write_value = f'"{new_value}"' if match_key in _QUOTED_ATTRS else new_value

            patched = False
            for idx, line in enumerate(lines):
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if "=" not in stripped:
                    continue

                raw_key = stripped.split("=", 1)[0].strip().lstrip("+").lower()
                if raw_key == match_key:
                    indent = line[: len(line) - len(line.lstrip())]
                    original_key = stripped.split("=", 1)[0].rstrip()
                    lines[idx] = f"{indent}{original_key} = {write_value}"
                    changes[htcondor_attr] = write_value
                    patched = True
                    break

            if not patched:
                inserted = False
                for idx, line in enumerate(lines):
                    if line.strip().lower().startswith("queue"):
                        lines.insert(idx, f"{htcondor_attr} = {write_value}")
                        changes[htcondor_attr] = write_value
                        inserted = True
                        break
                if not inserted:
                    lines.append(f"{htcondor_attr} = {write_value}")
                    changes[htcondor_attr] = write_value

    sub_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("sub_file_written", path=str(sub_path), changes=changes)
    return changes


def read_resource_requests(sub_path: Path) -> dict[str, int | None]:
    """
    Read current resource request values from a .sub file.

    Returns a dict with keys: memory_mb, disk_mb, cpus, runtime_seconds.
    Values are None when the attribute is absent from the file.
    """
    # Build reverse_map: htcondor_attr_lower -> (config_key, priority_index)
    # Lower index = higher priority (request_* beats pegasus_*)
    reverse_map: dict[str, tuple[str, int]] = {}
    for config_key, attrs in _ATTR_MAP.items():
        for priority, attr in enumerate(attrs):
            reverse_map[attr.lstrip("+").lower()] = (config_key, priority)

    result: dict[str, int | None] = {k: None for k in _ATTR_MAP}
    found_priority: dict[str, int] = {}  # config_key -> priority of stored value

    text = sub_path.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        raw_key, _, raw_val = stripped.partition("=")
        match_key = raw_key.strip().lstrip("+").lower()
        if match_key not in reverse_map:
            continue
        config_key, priority = reverse_map[match_key]
        if priority < found_priority.get(config_key, 999):  # lower index = higher priority
            try:
                result[config_key] = int(raw_val.strip())
                found_priority[config_key] = priority
            except (ValueError, TypeError):
                pass

    return result
