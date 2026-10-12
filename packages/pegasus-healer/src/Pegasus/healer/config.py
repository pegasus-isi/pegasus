from __future__ import annotations

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# Credential files loaded in order (later files take precedence over earlier ones;
# environment variables always win over all files):
#   1. ~/.pegasus/healer.env  — user-level credentials, shared across projects
#   2. .env                   — project-level overrides (current working directory)
_ENV_FILES = (
    Path.home() / ".pegasus" / "healer.env",
    Path(".env"),
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILES,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── LLM — shared defaults ─────────────────────────────────────────────────
    # Used by any agent that does not have its own override below.
    llm_model: str = "gpt-4o"
    llm_api_key: str | None = None    # optional: overrides provider-specific key
    llm_base_url: str | None = None   # optional: for local/institutional endpoints
    llm_max_retries: int = 3
    llm_temperature: float = 0.0

    # ── LLM — DiagnosisAgent overrides ───────────────────────────────────────
    # Falls back to the shared llm_* values when not set.
    # DiagnosisAgent does multi-step ReAct — use a strong reasoning model.
    diagnosis_llm_model: str | None = None
    diagnosis_llm_api_key: str | None = None
    diagnosis_llm_base_url: str | None = None

    # ── LLM — FixPlanningAgent overrides ─────────────────────────────────────
    # Falls back to the shared llm_* values when not set.
    # FixPlanning is a single call — a cheaper/faster model is often sufficient.
    fix_planning_llm_model: str | None = None
    fix_planning_llm_api_key: str | None = None
    fix_planning_llm_base_url: str | None = None

    # ── Policy ────────────────────────────────────────────────────────────────
    policy_file: str = "policies/remediation.yaml"
    confidence_threshold: float = 0.80
    # Active policy mode — overrides the 'mode' field in the YAML file.
    # Choices: production (default, conservative) | debug (aggressive, more AUTO)
    healer_policy_mode: str | None = None

    # ── Retry gate ────────────────────────────────────────────────────────────
    max_global_attempts: int = 5
    retry_outcome_timeout_seconds: int = 3600

    # ── Evidence collection ───────────────────────────────────────────────────
    log_excerpt_max_bytes: int = 8192   # sent to agents; full log stored by ref
    kickstart_excerpt_max_bytes: int = 16384


    # ── Observability ─────────────────────────────────────────────────────────
    log_level: str = "INFO"
    otlp_endpoint: str | None = None


settings = Settings()
