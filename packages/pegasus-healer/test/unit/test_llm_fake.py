"""Tests for the FakeLLMProvider contract."""
from __future__ import annotations

import pytest
from app.llm.fake import FakeLLMProvider
from app.models.diagnosis import Diagnosis, FailureType


@pytest.mark.asyncio
async def test_fake_returns_preset_response():
    preset = Diagnosis(
        failure_type=FailureType.OUT_OF_MEMORY,
        confidence=0.97,
        evidence_ids=["exit_code:137"],
        explanation="OOM",
        source="LLM",
    )
    fake = FakeLLMProvider(responses={"Diagnosis": preset})
    result = await fake.complete(
        messages=[{"role": "user", "content": "diagnose"}],
        response_model=Diagnosis,
    )
    assert result.failure_type == FailureType.OUT_OF_MEMORY
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_fake_raises_on_missing_preset():
    fake = FakeLLMProvider()
    with pytest.raises(ValueError, match="no preset"):
        await fake.complete(
            messages=[{"role": "user", "content": "diagnose"}],
            response_model=Diagnosis,
        )


@pytest.mark.asyncio
async def test_fake_set_response_at_runtime():
    preset = Diagnosis(
        failure_type=FailureType.DISK_EXCEEDED,
        confidence=0.95,
        evidence_ids=[],
        explanation="disk",
        source="LLM",
    )
    fake = FakeLLMProvider()
    fake.set_response(Diagnosis, preset)
    result = await fake.complete(
        messages=[{"role": "user", "content": "diagnose"}],
        response_model=Diagnosis,
    )
    assert result.failure_type == FailureType.DISK_EXCEEDED
