"""Shared pytest configuration and fixtures."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _set_policy_path(monkeypatch, tmp_path):
    """
    Ensure tests that call load_policy() can find the policy file
    when pytest is run from any working directory.
    """
    import os
    # Only patch if the file doesn't exist at the default relative path
    if not os.path.exists("policies/remediation.yaml"):
        import shutil
        policy_src = os.path.join(os.path.dirname(__file__), "..", "policies", "remediation.yaml")
        if os.path.exists(policy_src):
            os.makedirs(tmp_path / "policies", exist_ok=True)
            shutil.copy(policy_src, tmp_path / "policies" / "remediation.yaml")
            monkeypatch.chdir(tmp_path)
