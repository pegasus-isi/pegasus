# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-wms.common` — shared utilities for Pegasus WMS Python packages. Provides braindump serialization, YAML/JSON handling with Pegasus-specific defaults, and a client API that wraps Pegasus CLI tools.

## Build & Test

```bash
# Run full test suite with coverage (94% minimum required)
tox -e py310

# Run pytest directly (from package root)
pytest test/

# Run a single test file
pytest test/test_braindump.py
pytest test/test_yaml.py
pytest test/test_json.py
pytest test/test_condor.py
pytest test/test_status.py
pytest test/client/test_client.py

# Run a single test function
pytest test/test_braindump.py::TestBraindump::test_load -v
```

Coverage must stay at or above **94%** (`--cov-fail-under 94` in tox.ini).

## Linting & Formatting

```bash
# Run all linters/formatters via tox
tox -e lint
```

The lint environment runs, in order: autoflake, pyupgrade (--py36-plus), isort, black (target py36), flake8. Configure these in `pyproject.toml`.

## Architecture

### Namespace Package

This package shares the `Pegasus` Python namespace with three sibling packages (`pegasus-api`, `pegasus-python`, `pegasus-worker`) using `pkgutil.extend_path`. All source lives under `src/Pegasus/`. The `__init__.py` must preserve the `extend_path` call or imports across sibling packages will break.

### Modules

- **`braindump.py`** — `Braindump` dataclass with `load/loads/dump/dumps` for workflow metadata files. Auto-converts string paths to `pathlib.Path` and `uses_pmc` to bool in `__post_init__`.
- **`yaml.py`** — Wraps PyYAML with C-accelerated loaders when available. Disables YAML 1.1 bool coercion (`yes/no/on/off` stay as strings) and datetime deserialization. Adds `Path` and `OrderedDict` serializers.
- **`json.py`** — Custom encoder handles `UUID`, `Enum`, `Path`, SQLAlchemy models, and objects with `__html__()` or `__json__()` methods. `dump_all` produces NDJSON.
- **`client/`** — `Client` class wraps pegasus CLI tools (`pegasus-plan`, `pegasus-run`, `pegasus-status`, etc.) with threaded streaming I/O. `Status` class parses dagman.out files and Condor queue state for workflow monitoring.

### Key Conventions

- Python 3.6+ compatibility required (the `setup.py` targets `>=3.6`; `pyupgrade` enforces `--py36-plus`).
- Use `Pegasus.yaml` and `Pegasus.json` instead of raw PyYAML/json for consistent Pegasus-specific serialization behavior.
- The client module is a thin wrapper around CLI subprocess calls, not a reimplementation of planner logic.
