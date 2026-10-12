"""
Integration test configuration and shared fixtures.

Ollama detection
────────────────
When LLM_MODEL starts with "ollama" or "ollama_chat", the tests skip
the API key requirement and instead verify that Ollama is reachable at
the configured base URL (default: http://localhost:11434).

Set up Ollama for local testing:
    # Install Ollama: https://ollama.com
    ollama pull llama3.3        # 70B — best accuracy
    ollama pull qwen2.5:72b     # alternative

    export LLM_MODEL=ollama/llama3.3
    # LLM_BASE_URL defaults to http://localhost:11434 — no need to set it
    pytest tests/integration/ -v -s
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Load .env into os.environ so _require_llm() and other os.environ checks
# see the same values that pydantic-settings / build_llm_provider() use.
_env_file = Path(__file__).parent.parent.parent / ".env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file, override=False)  # don't override already-set shell vars


def _ollama_base_url() -> str:
    return os.environ.get("LLM_BASE_URL", "http://localhost:11434")


def _is_ollama_model() -> bool:
    model = os.environ.get("LLM_MODEL", "")
    return model.startswith(("ollama/", "ollama_chat/"))


def _ollama_reachable() -> bool:
    """Return True if Ollama is listening at the configured base URL."""
    import urllib.request
    url = _ollama_base_url().rstrip("/") + "/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=3):
            return True
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=False)
def require_ollama():
    """
    Session-scoped fixture: skip the test if Ollama is not reachable.

    Usage in tests:
        @pytest.mark.usefixtures("require_ollama")
        async def test_something(): ...

    Or add to conftest autouse for all integration tests:
        pytestmark = [pytest.mark.usefixtures("require_ollama")]
    """
    if not _is_ollama_model():
        return  # not configured for Ollama — skip check

    if not _ollama_reachable():
        pytest.skip(
            f"Ollama not reachable at {_ollama_base_url()}. "
            "Start Ollama and ensure the model is pulled:\n"
            f"  ollama pull {os.environ.get('LLM_MODEL', 'ollama/llama3.3').split('/', 1)[-1]}"
        )
