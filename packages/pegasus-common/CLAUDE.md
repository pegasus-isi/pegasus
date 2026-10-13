# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-wms.common` — shared utilities for Pegasus WMS Python packages. Provides braindump serialization, YAML/JSON handling with Pegasus-specific defaults, and a client API that wraps Pegasus CLI tools.

## Build & Test

Run from the **repo root**. Tests run against the installed pegasus-wms wheel;
pytest and coverage are configured in the root `pyproject.toml` (see the root
`tox.toml`).

```bash
# Run this package's suite with coverage
tox -m common

# Run tests matching a pattern
tox -m common -- -k braindump

# Run a single test file or function (any interpreter: 3.10 .. 3.14, py)
tox -e py -- packages/pegasus-common/test/test_braindump.py
tox -e py -- packages/pegasus-common/test/test_braindump.py::TestBraindump::test_load -v
```

Coverage for `tox -m common` must stay at or above **48%** (the `common` env in
the root `tox.toml`). Actual coverage is currently ~91%. Reports go to
`test-reports/common/` at the repo root.

## Linting & Formatting

```bash
# From the repo root; lints all three Python packages
tox -e lint
```

Runs `ruff check` then `ruff format --check`, using the `[tool.ruff]` config in this package's `pyproject.toml`. To fix formatting, run `ruff format` (or pre-commit).

## Architecture

### Namespace Package

This package shares the `Pegasus` Python namespace with two sibling packages (`pegasus-api`, `pegasus-python`) using `pkgutil.extend_path`. All source lives under `src/Pegasus/`. The `__init__.py` must preserve the `extend_path` call or imports across sibling packages will break.

### Modules

- **`braindump.py`** — `Braindump` dataclass with `load/loads/dump/dumps` for workflow metadata files. Auto-converts string paths to `pathlib.Path` and `uses_pmc` to bool in `__post_init__`.
- **`yaml.py`** — Wraps PyYAML with C-accelerated loaders when available. Disables YAML 1.1 bool coercion (`yes/no/on/off` stay as strings) and datetime deserialization. Adds `Path` and `OrderedDict` serializers.
- **`json.py`** — Custom encoder handles `UUID`, `Enum`, `Path`, SQLAlchemy models, and objects with `__html__()` or `__json__()` methods. `dump_all` produces NDJSON.
- **`client/`** — `Client` class wraps pegasus CLI tools (`pegasus-plan`, `pegasus-run`, `pegasus-status`, etc.) with threaded streaming I/O. `Status` class parses dagman.out files and Condor queue state for workflow monitoring.

### Key Conventions

- Targets Python ≥3.10, like the `pegasus-wms` wheel it is merged into (ruff `target-version = "py310"`). The old Python 3.6 floor existed for the worker tarball environment, which no longer ships any Python.
- Use `Pegasus.yaml` and `Pegasus.json` instead of raw PyYAML/json for consistent Pegasus-specific serialization behavior.
- The client module is a thin wrapper around CLI subprocess calls, not a reimplementation of planner logic.
