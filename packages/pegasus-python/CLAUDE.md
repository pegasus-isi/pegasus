# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

This is `pegasus-wms` (pegasus-python), the largest of four Python namespace packages in the Pegasus WMS. It provides CLI tools, a monitoring daemon, a Flask-based dashboard/REST API, a SQLAlchemy database layer, and statistics generation. It depends on the three sibling packages: `pegasus-wms.common`, `pegasus-wms.api`, and `pegasus-wms.worker`.

## Build & Install

```bash
# Install in development mode (from this directory)
pip install -e .

# Install with optional extras
pip install -e ".[cwl]"        # CWL converter support
pip install -e ".[postgresql]"  # PostgreSQL support
```

Sibling packages must be installed first (or simultaneously): `../pegasus-common`, `../pegasus-api`, `../pegasus-worker`.

## Testing

```bash
# Run all tests (from this directory)
tox -e py310                    # Replace py310 with your Python version

# Run a single test file
pytest test/test_statistics.py

# Run a single test
pytest test/test_statistics.py::TestPegasusStatistics::test_initialize -v

# Run tests with coverage
pytest --cov --cov-branch --cov-report term --cov-fail-under 25.5

# Run the full project test suite (from repo root)
ant test-python
```

Test dependencies are managed by tox.ini. Key test deps: pytest, pytest-mock, pytest-cov, pytest-resource-path, jsonschema, cwl-utils.

## Code Formatting

```bash
# Format all Python code (from repo root)
ant code-format-python

# Or run linting directly (from this directory)
tox -e lint
```

Uses ruff for checking and formatting. The `tox -e lint` environment runs autoflake, pyupgrade, isort, black, and flake8. Line length is 88. Files in `src/Pegasus/cli/` are excluded from isort/black formatting.

## Architecture

### Source Layout: `src/Pegasus/`

- **cli/** — 22 executable CLI scripts (pegasus-analyzer, pegasus-status, pegasus-monitord, etc.). These are standalone scripts, not Click-based modules. They are excluded from auto-formatting.
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

All four Pegasus packages share the `Pegasus` namespace. Each `src/Pegasus/__init__.py` contains:
```python
__path__ = __import__("pkgutil").extend_path(__path__, __name__)
```
Do not remove or modify this line.

### Test Structure: `test/`

Tests mirror the source layout. The `conftest.py` provides session-scoped Flask app fixtures (`app`, `emapp`) and function-scoped `cli`/`runner` fixtures. Service tests use in-memory SQLite. The `test/exitcode/` directory contains 31 subdirectories of sample data for exitcode parsing tests.

## Key Constraints

- SQLAlchemy pinned to >=1.3,<1.4 (not compatible with 2.x)
- Flask pinned to >1.1,<2.3
- Python 3.6+ compatibility required
- Version is `5.1.3-dev.0`, defined in both `setup.py` and root `build.properties`
