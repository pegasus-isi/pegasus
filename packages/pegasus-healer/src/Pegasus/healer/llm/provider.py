from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class LLMProvider(Protocol):
    """
    Provider-agnostic LLM interface.

    Any object that implements this method can be injected into the
    diagnosis and fix-planning agents — no dependency on a specific SDK.

    Implementations:
      - LiteLLMProvider  (llm/litellm_provider.py) — routes to any
        provider (OpenAI, Anthropic, Azure, Bedrock, Ollama, …) via LiteLLM.
      - UniversalProvider (llm/universal_provider.py) — native SDKs, no LiteLLM.
      - FakeLLMProvider  (llm/fake.py) — deterministic preset responses
        for unit and graph tests.
    """

    async def complete(
        self,
        messages: list[dict[str, Any]],
        response_model: type[BaseModel],
        *,
        temperature: float = 0.0,
    ) -> BaseModel:
        """
        Send messages to the LLM and return a validated Pydantic instance.

        Args:
            messages: OpenAI-style chat message list.
            response_model: The Pydantic class the response must conform to.
            temperature: Sampling temperature (0.0 = deterministic).

        Returns:
            A validated instance of response_model.

        Raises:
            Exception: Provider or validation errors bubble up as-is.
        """
        ...
