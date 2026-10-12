from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class FakeLLMProvider:
    """
    Deterministic LLM stub for unit and graph tests.

    Two modes:
      responses  — single preset per model name, returned every time
      sequences  — ordered list per model name, one item consumed per call

    Usage (single response):
        fake = FakeLLMProvider(responses={"Diagnosis": my_diagnosis})

    Usage (ReAct loop — one response per step):
        fake = FakeLLMProvider(sequences={
            "ThoughtAction": [step1, step2, conclude_step],
        })
    """

    def __init__(
        self,
        responses: dict[str, BaseModel] | None = None,
        sequences: dict[str, list[BaseModel]] | None = None,
    ) -> None:
        self._responses: dict[str, BaseModel] = responses or {}
        # Copy so callers don't mutate their original lists
        self._sequences: dict[str, list[BaseModel]] = {
            k: list(v) for k, v in (sequences or {}).items()
        }
        self._seq_indices: dict[str, int] = {}
        self.calls: list[tuple[list[dict[str, Any]], type[BaseModel]]] = []

    def set_response(self, response_model: type[BaseModel], value: BaseModel) -> None:
        self._responses[response_model.__name__] = value

    async def complete(
        self,
        messages: list[dict[str, Any]],
        response_model: type[BaseModel],
        *,
        temperature: float = 0.0,
    ) -> BaseModel:
        self.calls.append((messages, response_model))
        key = response_model.__name__

        # Sequences take priority — consume next item in the list
        if key in self._sequences:
            idx = self._seq_indices.get(key, 0)
            seq = self._sequences[key]
            if idx < len(seq):
                self._seq_indices[key] = idx + 1
                return seq[idx]
            # Sequence exhausted — fall through to single response

        if key in self._responses:
            return self._responses[key]

        raise ValueError(
            f"FakeLLMProvider: no preset for '{key}'. "
            f"Available: responses={list(self._responses)}, "
            f"sequences={list(self._sequences)}"
        )
