"""Unit tests for the configuration overlay builder."""
from __future__ import annotations

import uuid
import pytest
from app.fixes.overlay import build_overlay, _hash
from app.models.fixes import FixAction, FixProposal


def _proposal(old: dict, new: dict) -> FixProposal:
    return FixProposal(
        incident_id=uuid.uuid4(),
        action=FixAction.INCREASE_MEMORY,
        old_configuration=old,
        proposed_configuration=new,
        justification="test",
        confidence=0.97,
        source="RULE",
    )


def test_overlay_hashes_differ_when_config_changes():
    proposal = _proposal({"memory_mb": 4096}, {"memory_mb": 6144})
    overlay = build_overlay(proposal)
    assert overlay.hash_before != overlay.hash_after
    assert overlay.configs_differ()


def test_overlay_hashes_same_when_no_change():
    proposal = _proposal({}, {})
    overlay = build_overlay(proposal)
    assert overlay.hash_before == overlay.hash_after
    assert not overlay.configs_differ()


def test_hash_is_deterministic():
    cfg = {"memory_mb": 4096, "cpus": 4}
    assert _hash(cfg) == _hash(cfg)


def test_hash_is_order_independent():
    a = {"memory_mb": 4096, "cpus": 4}
    b = {"cpus": 4, "memory_mb": 4096}
    assert _hash(a) == _hash(b)


def test_overlay_serialisable():
    proposal = _proposal({"memory_mb": 4096}, {"memory_mb": 6144})
    overlay = build_overlay(proposal)
    data = overlay.model_dump(mode="json")
    assert "hash_before" in data
    assert "hash_after" in data
    assert "old_config" in data
    assert "new_config" in data
