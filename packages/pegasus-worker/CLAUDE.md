# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-wms.worker` — Worker-side execution tools for the Pegasus Workflow Management System. Part of a four-package Python namespace (`Pegasus`) sharing code across pegasus-api, pegasus-common, pegasus-python, and pegasus-worker. The `__init__.py` files use `pkgutil.extend_path` for namespace package support.

## Build and Test Commands

```bash
# Run all tests (via tox, defaults to available Python interpreters)
tox

# Run tests for a specific Python version
tox -e py310

# Run a single test file
pytest test/test_pegasus_transfer.py

# Run a single test function
pytest test/test_pegasus_transfer.py::TestClassName::test_method -v

# Run tests with coverage report
pytest --cov --cov-branch --cov-report term test/

# Lint and format (local — applies fixes in-place)
tox -e lint

# Lint in CI mode (check only, no modifications)
CI=true tox -e lint

# Format/lint from repo root (runs tox -e lint for all Python packages)
ant code-format-python    # from repo root
```

## Code Formatting

- **black** (v19.10b0): target py35, line length 88. CLI files under `Pegasus/cli/` are formatted but excluded from isort/black config patterns.
- **isort**: black profile, `Pegasus` as known first-party.
- **flake8**: max line length 88, ignores W503.
- **autoflake**: removes unused imports/variables.
- **pyupgrade**: `--py36-plus` syntax upgrades.

The lint environment runs these in order: autoflake → pyupgrade → isort → black → flake8.

## Architecture

### Core Modules

- **`src/Pegasus/transfer.py`** (~5,500 lines) — Multi-protocol file transfer engine. Defines `TransferHandlerBase` with protocol-specific subclasses (FileHandler, HttpHandler, S3Handler, GridFtpHandler, GlobusOnlineHandler, IRodsHandler, ScpHandler, WebdavHandler, etc.). Uses a thread pool (`WorkThread`) for parallel transfers. Reads transfer specs in JSON format. Credentials loaded from `~/.pegasus/credentials.conf` (or `PEGASUS_CREDENTIALS` env var) with strict file permission checks.

- **`src/Pegasus/s3.py`** (~1,000 lines) — S3 operations (ls, get, put, cp, mkdir, rm) using boto3. Parses S3 URIs via `S3URI` class. Config from `~/.pegasus/credentials.conf` or `S3CFG` env var.

- **`src/Pegasus/tools/worker_utils.py`** (~325 lines) — `TimedCommand` for subprocess execution with configurable timeout (default 6 hours). `Tools` singleton for finding executables in PATH and caching version info.

### CLI Entry Points (under `src/Pegasus/cli/`)

`pegasus-transfer.py`, `pegasus-s3.py`, `pegasus-integrity.py`, `pegasus-checkpoint.py`, `pegasus-globus-online.py`, `pegasus-globus-online-init.py`. These are standalone scripts, not regular modules — isort/black exclude patterns don't fully apply to them.

## Key Environment Variables

- `PEGASUS_CREDENTIALS` — Path to credentials file
- `S3CFG` — S3 configuration file path
- `KICKSTART_MON_ENDPOINT_URL` — Real-time monitoring (Panorama) endpoint

## Dependencies

Runtime: `six>=1.9.0`, `boto3>1.12`, `globus-sdk>=3.23.0,<4` (Python 3.7+). Python >=3.6 required.

## Testing Notes

- Coverage minimum: 20% (enforced by `--cov-fail-under 20.0`)
- Test resources (fixtures) live in `test/resources/`
- `pytest-resource-path` is used for accessing test resource files
- `pytest-mock` available for mocking
