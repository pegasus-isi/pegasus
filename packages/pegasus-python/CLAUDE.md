# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

This is `pegasus-wms` (pegasus-python), the largest of three Python namespace packages in the Pegasus WMS. It provides CLI tools, a monitoring daemon, a Flask-based dashboard/REST API, a SQLAlchemy database layer, and statistics generation. It shares the `Pegasus` namespace with two sibling packages, `pegasus-common` and `pegasus-api`, which CMake merges with it into the single `pegasus-wms` wheel.

## Build & Install

**Do not run `pip install -e .` from this directory.** This package is not a
standalone distribution any more — the whole repo builds one `pegasus-wms`
wheel. Install from the repo root instead:

```bash
make dev                  # editable install of the full distribution
pip install ".[cwl]"      # from the repo root; extras live in the root pyproject
```

Sibling packages (`../pegasus-common`, `../pegasus-api`)
are merged into that single wheel by CMake, so there is nothing to install
separately. Test environments install them from local paths — see the root
`tox.toml`.

## Testing

```bash
# Run all tests (from the REPO ROOT — envs live in the root tox.toml)
tox -e python

# Run a single test file, or a single test
tox -e python -- test/test_statistics.py
tox -e python -- test/test_statistics.py::TestPegasusStatistics::test_initialize -v
```

Test dependencies are managed by the root `tox.toml`. Key test deps: pytest, pytest-mock, pytest-cov, pytest-resource-path, jsonschema, cwl-utils.

The coverage floor (25.5%) lives in `[tool.coverage.report] fail_under` in this package's `pyproject.toml`.

## Code Formatting

```bash
# Run linting/formatting (from the repo root)
tox -e lint-python
```

Uses ruff for checking and formatting. The `tox -e lint` environment runs autoflake, pyupgrade, isort, black, and flake8. Line length is 88. Files in `src/Pegasus/cli/` are excluded from isort/black formatting.

## Architecture

### Source Layout: `src/Pegasus/`

- **cli/** — CLI scripts (`pegasus-analyzer.py`, `pegasus-status.py`, etc.) plus `main.py` which is the unified Click entry point (`pegasus <subcommand>`). The per-tool scripts are invoked via `runpy` from `main.py`. They are excluded from auto-formatting.
- **db/** — SQLAlchemy database layer:
  - `schema.py` — All ORM models (Workflow, Job, JobInstance, Task, Invocation, Host, Ensemble, etc.)
  - `connection.py` — Database connection factory supporting SQLite, MySQL, PostgreSQL. Parses both JDBC and SQLAlchemy URIs.
  - `workflow_loader.py` — Event-driven loader that maps stampede events to database inserts with batch flushing.
  - `admin/versions/` — 15 schema migration files (v0–v14).
- **monitoring/** — Monitoring daemon internals:
  - `workflow.py` (largest file, ~129KB) — Parses DAG files, tracks workflow structure and job state transitions from DAGMan logs.
  - `job.py` — Job instance tracking and integrity metrics.
- **service/** — Flask web application:
  - `server.py` — App factory (`create_app()`). Registers `dashboard` and `monitoring` blueprints.
  - `dashboard/` — Dashboard views, queries, and logic for the workflow web UI.
  - `ensembles/` — Ensemble manager REST API and views.
  - `monitoring/` — Monitoring REST endpoints.
- **tools/** — Shared utilities (`utils.py`), properties handling (`properties.py`), kickstart output parser.
- **Top-level modules** — `analyzer.py`, `statistics.py`, `exitcode.py`, `init.py`, `submitdir.py`, catalog deserializers.

### Database Pattern

Pegasus uses a two-database architecture:

- **Master database** — Tracks all workflows (MasterWorkflow, MasterWorkflowstate tables). Queried by the dashboard for listing.
- **Per-workflow database** — Detailed execution data (Job, JobInstance, Invocation, etc.). URL stored in the master database.

The `SABase` class in `schema.py` provides `commit_to_db()` and `merge_to_db()` helpers. The workflow loader uses an event map pattern (`eventMap` dict) to dispatch stampede events to handler methods.

### Namespace Package Convention

All three Pegasus packages share the `Pegasus` namespace. Each `src/Pegasus/__init__.py` contains:

```python
__path__ = __import__("pkgutil").extend_path(__path__, __name__)
```

Do not remove or modify this line.

### Test Structure: `test/`

Tests mirror the source layout. The `conftest.py` provides session-scoped Flask app fixtures (`app`, `emapp`) and function-scoped `cli`/`runner` fixtures. Service tests use in-memory SQLite. The `test/exitcode/` directory contains 31 subdirectories of sample data for exitcode parsing tests.

## Key Constraints

- SQLAlchemy pinned to `>=1.4` (not compatible with 2.x)
- Flask pinned to `>=2.2,<3.0`
- Python 3.10+ required (set in root `pyproject.toml`)
- Version is `6.0.0-dev0`, defined in root `build.properties` and `pyproject.toml`
- This package is not installed standalone — it ships as part of the `pegasus-wms` wheel built from the repo root
