from Pegasus.healer.llm.provider import LLMProvider
from Pegasus.healer.llm.universal_provider import UniversalProvider
from Pegasus.healer.llm.fake import FakeLLMProvider
from Pegasus.healer.config import settings

# Backwards-compatibility alias
LiteLLMProvider = UniversalProvider


def build_llm_provider() -> UniversalProvider:
    """Factory: build the shared/default LLM provider from settings."""
    return UniversalProvider(
        model=settings.llm_model,
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        max_retries=settings.llm_max_retries,
    )


def build_diagnosis_provider() -> UniversalProvider:
    """Factory: build the LLM provider for DiagnosisAgent."""
    return UniversalProvider(
        model=settings.diagnosis_llm_model or settings.llm_model,
        api_key=settings.diagnosis_llm_api_key or settings.llm_api_key,
        base_url=settings.diagnosis_llm_base_url or settings.llm_base_url,
        max_retries=settings.llm_max_retries,
    )


def build_fix_planning_provider() -> UniversalProvider:
    """Factory: build the LLM provider for FixPlanningAgent."""
    return UniversalProvider(
        model=settings.fix_planning_llm_model or settings.llm_model,
        api_key=settings.fix_planning_llm_api_key or settings.llm_api_key,
        base_url=settings.fix_planning_llm_base_url or settings.llm_base_url,
        max_retries=settings.llm_max_retries,
    )


__all__ = [
    "LLMProvider", "UniversalProvider", "LiteLLMProvider", "FakeLLMProvider",
    "build_llm_provider", "build_diagnosis_provider", "build_fix_planning_provider",
]
