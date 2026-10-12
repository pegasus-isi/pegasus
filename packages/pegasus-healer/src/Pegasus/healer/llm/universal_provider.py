from __future__ import annotations

"""
UniversalProvider — routes to OpenAI, Anthropic, Ollama, or Google Gemini
using their native SDKs. No LiteLLM needed.

Routing by model prefix
────────────────────────
  anthropic/  or  claude-*     → Anthropic SDK   (messages API)
  ollama/     or  ollama_chat/ → OpenAI SDK   →  http://localhost:11434/v1
  google/     or  gemini/*     → OpenAI SDK   →  Google OpenAI-compat endpoint
  (anything else, incl openai/) → OpenAI SDK  →  api.openai.com or custom base_url

Examples
────────
  "gpt-4o"                           OpenAI
  "openai/MiniMaxAI/MiniMax-M2.7"    Custom OpenAI-compat  (NRP Nautilus, vLLM…)
  "anthropic/claude-sonnet-4-6"      Anthropic
  "claude-opus-4-6"                  Anthropic (no prefix needed)
  "ollama/llama3.3:70b"              Ollama  (local)
  "gemini/gemini-2.0-flash"          Google Gemini
  "google/gemini-2.0-flash"          Google Gemini
"""

from typing import Any, Callable

import instructor
from pydantic import BaseModel

# ── Provider prefix sets ───────────────────────────────────────────────────────

_ANTHROPIC_PREFIXES = ("anthropic/", "claude-")
_OLLAMA_PREFIXES    = ("ollama/", "ollama_chat/")
_GOOGLE_PREFIXES    = ("google/", "gemini/")

_GOOGLE_BASE_URL    = "https://generativelanguage.googleapis.com/v1beta/openai/"
_OLLAMA_BASE_URL    = "http://localhost:11434/v1"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip(model: str, *prefixes: str) -> str:
    """Remove the first matching prefix from the model string."""
    for p in prefixes:
        if model.startswith(p):
            return model[len(p):]
    return model


def _extract_system(messages: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
    """
    Split a message list into (system_prompt, user/assistant_messages).
    Anthropic requires the system prompt as a separate parameter.
    """
    system = next((m["content"] for m in messages if m["role"] == "system"), None)
    rest   = [m for m in messages if m["role"] != "system"]
    return system, rest


# ── Provider class ─────────────────────────────────────────────────────────────

class UniversalProvider:
    """
    Single LLM provider that selects the right SDK at construction time
    based on the model prefix.

    Parameters
    ──────────
    model       prefix-qualified model name (see module docstring)
    api_key     provider API key; not required for Ollama
    base_url    endpoint override for OpenAI-compatible servers
                (NRP Nautilus, vLLM, LM Studio, …)
    max_retries instructor retry attempts on structured-output validation failure
    """

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        base_url: str | None = None,
        max_retries: int = 3,
    ) -> None:

        if any(model.startswith(p) for p in _ANTHROPIC_PREFIXES):
            self._model = _strip(model, "anthropic/")
            self._call  = self._make_anthropic_call(api_key, max_retries)

        elif any(model.startswith(p) for p in _OLLAMA_PREFIXES):
            self._model = _strip(model, *_OLLAMA_PREFIXES)
            self._call  = self._make_openai_call(
                api_key    = "ollama",           # Ollama ignores the key
                base_url   = base_url or _OLLAMA_BASE_URL,
                mode       = instructor.Mode.JSON,   # Ollama: JSON schema mode
                max_retries= max_retries,
            )

        elif any(model.startswith(p) for p in _GOOGLE_PREFIXES):
            self._model = _strip(model, "google/", "gemini/")
            self._call  = self._make_openai_call(
                api_key    = api_key,
                base_url   = base_url or _GOOGLE_BASE_URL,
                mode       = instructor.Mode.TOOLS,
                max_retries= max_retries,
            )

        else:
            # OpenAI or any OpenAI-compatible endpoint (NRP Nautilus, vLLM…)
            self._model = _strip(model, "openai/")
            self._call  = self._make_openai_call(
                api_key    = api_key,
                base_url   = base_url,           # None → api.openai.com
                mode       = instructor.Mode.TOOLS,
                max_retries= max_retries,
            )

    # ── Internal factory helpers ───────────────────────────────────────────────

    def _make_openai_call(
        self,
        api_key: str | None,
        base_url: str | None,
        mode: instructor.Mode,
        max_retries: int,
    ) -> Callable:
        from openai import AsyncOpenAI

        raw    = AsyncOpenAI(api_key=api_key or "none", base_url=base_url)
        client = instructor.from_openai(raw, mode=mode)
        model  = self._model

        async def _call(
            messages: list[dict[str, Any]],
            response_model: type[BaseModel],
            temperature: float,
        ) -> BaseModel:
            return await client.chat.completions.create(
                model          = model,
                messages       = messages,
                response_model = response_model,
                temperature    = temperature,
                max_retries    = max_retries,
            )

        return _call

    def _make_anthropic_call(
        self,
        api_key: str | None,
        max_retries: int,
    ) -> Callable:
        from anthropic import AsyncAnthropic

        raw    = AsyncAnthropic(api_key=api_key)
        client = instructor.from_anthropic(raw)
        model  = self._model

        async def _call(
            messages: list[dict[str, Any]],
            response_model: type[BaseModel],
            temperature: float,
        ) -> BaseModel:
            system, user_messages = _extract_system(messages)
            kwargs: dict[str, Any] = dict(
                model          = model,
                max_tokens     = 4096,
                messages       = user_messages,
                response_model = response_model,
                temperature    = temperature,
                max_retries    = max_retries,
            )
            if system:
                kwargs["system"] = system
            return await client.messages.create(**kwargs)

        return _call

    # ── LLMProvider protocol ──────────────────────────────────────────────────

    async def complete(
        self,
        messages: list[dict[str, Any]],
        response_model: type[BaseModel],
        *,
        temperature: float = 0.0,
    ) -> BaseModel:
        return await self._call(messages, response_model, temperature)

    @property
    def model(self) -> str:
        return self._model
