from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


from typing import Literal

# Ordered hierarchy — higher index = more access
SCOPE_LEVELS = ["job", "script", "catalog", "workflow"]
ScopeLevel   = Literal["job", "script", "catalog", "workflow"]


def _scope_gte(current: str, required: str) -> bool:
    """Return True when current scope level includes required level."""
    try:
        return SCOPE_LEVELS.index(current) >= SCOPE_LEVELS.index(required)
    except ValueError:
        return False


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base (override wins on conflicts)."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


class PolicyConfig(BaseModel):
    version: str = "1.0.0"
    failure_policies: dict[str, dict[str, Any]] = {}
    global_config: dict[str, Any] = {}
    scope_level: ScopeLevel = "job"
    mode: str = "production"
    # Confidence threshold from the active mode; None means use settings default.
    confidence_threshold: float | None = None

    def get_required_level(self, failure_type: str) -> str:
        return self.failure_policies.get(failure_type, {}).get("required_level", "job")

    def get_fallback_decision(self, failure_type: str) -> str:
        return self.failure_policies.get(failure_type, {}).get("fallback_decision", "ESCALATE")

    def scope_allows(self, failure_type: str) -> bool:
        """True when the current scope level meets the required level for this failure type."""
        return _scope_gte(self.scope_level, self.get_required_level(failure_type))

    def get_decision(self, failure_type: str) -> str:
        return self.failure_policies.get(failure_type, {}).get("decision", "ASK")

    def get_max_attempts(self, failure_type: str) -> int:
        specific = self.failure_policies.get(failure_type, {}).get("maximum_attempts")
        if specific is not None:
            return int(specific)
        return int(self.global_config.get("maximum_attempts", 3))

    def get_multiplier(self, failure_type: str) -> float:
        return float(self.failure_policies.get(failure_type, {}).get("multiplier", 1.5))

    def get_ceiling(self, failure_type: str, key: str) -> int | None:
        val = self.failure_policies.get(failure_type, {}).get(key)
        return int(val) if val is not None else None

    def as_snapshot_dict(self) -> dict[str, Any]:
        return self.model_dump()


def load_policy(path: str) -> PolicyConfig:
    p = Path(path)
    if not p.exists():
        return PolicyConfig()
    with p.open() as f:
        data = yaml.safe_load(f) or {}

    raw_scope   = data.get("agent_scope", {})
    _yaml_level = raw_scope.get("level", "job")
    scope_level: ScopeLevel = _yaml_level if _yaml_level in SCOPE_LEVELS else "job"  # type: ignore[assignment]

    # ── Determine active mode ──────────────────────────────────────────────────
    # Priority: HEALER_POLICY_MODE env var > YAML 'mode' field > "production"
    yaml_mode = data.get("mode", "production")
    active_mode = os.environ.get("HEALER_POLICY_MODE", yaml_mode) or "production"

    # ── Merge base failure_policies with mode overrides ────────────────────────
    base_policies: dict[str, Any] = data.get("failure_policies", {})
    mode_block: dict[str, Any] = data.get("modes", {}).get(active_mode, {})
    mode_policies: dict[str, Any] = mode_block.get("failure_policies", {})
    merged_policies = _deep_merge(base_policies, mode_policies)

    # ── Confidence threshold from active mode (optional) ──────────────────────
    confidence_threshold: float | None = None
    raw_ct = mode_block.get("confidence_threshold")
    if raw_ct is not None:
        confidence_threshold = float(raw_ct)

    # ── Global config (base only — modes don't override global for now) ────────
    global_config = data.get("global", {})

    return PolicyConfig(
        version=data.get("version", "1.0.0"),
        failure_policies=merged_policies,
        global_config=global_config,
        scope_level=scope_level,
        mode=active_mode,
        confidence_threshold=confidence_threshold,
    )
