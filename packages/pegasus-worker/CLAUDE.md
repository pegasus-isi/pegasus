# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-wms.worker` — historically, worker-side execution tools for the Pegasus Workflow Management System. Part of a four-package Python namespace (`Pegasus`) sharing code across pegasus-api, pegasus-common, pegasus-python, and pegasus-worker. The `__init__.py` files use `pkgutil.extend_path` for namespace package support.

**As of the Go rewrite (see `packages/pegasus-transfer/CLAUDE.md` for the decision record), this package ships no CLI tools at all.** Every tool that used to live here — `pegasus-transfer`/`pegasus-s3`, `pegasus-checkpoint`, `pegasus-integrity`, `pegasus-globus-online`/`pegasus-globus-online-init` — has been rewritten in Go as its own package (`packages/pegasus-transfer/`, `packages/pegasus-checkpoint/`, `packages/pegasus-integrity/`, `packages/pegasus-globus-online/`) and is installed as a compiled binary instead. This package is now just namespace-package boilerplate (`src/Pegasus/__init__.py`, `src/Pegasus/cli/__init__.py`) plus a placeholder test (`test/test_namespace.py`) asserting that boilerplate still works.

It's still distributed the same two ways as before (merged into the `pegasus-wms` wheel; bundled into the worker tarball), but has nothing left to contribute to either beyond the namespace merge itself — the worker tarball (`make build-worker`) contains no Python at all now, only compiled binaries (C tools + the five Go tools above).

**This package is a candidate for outright retirement** — nothing currently requires `Pegasus.tools` or `Pegasus.cli` from this specific package to exist as a separate distribution; the namespace merge could plausibly move into `pegasus-common` instead. Not done in this session since it wasn't the stated goal, but worth revisiting.

## Build and Test Commands

```bash
# Run all tests (via tox, defaults to available Python interpreters)
tox

# Run tests for a specific Python version
tox -e py310

# Lint and format (local — applies fixes in-place)
tox -e lint

# Lint in CI mode (check only, no modifications)
CI=true tox -e lint
```

## Code Formatting

- **black** (v19.10b0): target py35, line length 88.
- **isort**: black profile, `Pegasus` as known first-party.
- **flake8**: max line length 88, ignores W503.
- **autoflake**: removes unused imports/variables.
- **pyupgrade**: `--py36-plus` syntax upgrades.

The lint environment runs these in order: autoflake → pyupgrade → isort → black → flake8. The historical `Pegasus/cli` isort/black exclude patterns are now moot (there's nothing left under `src/Pegasus/cli/` besides `__init__.py`) but haven't been cleaned up from the tox/pyproject config — harmless dead config, not a correctness issue.

## Architecture

`src/Pegasus/__init__.py`, `src/Pegasus/cli/__init__.py` — namespace-package (`pkgutil.extend_path`) boilerplate only. No other modules.

## Dependencies

None. `install_requires` in `setup.py` is empty — the last runtime dependency (`globus-sdk`) was removed when `pegasus-globus-online`/`pegasus-globus-online-init` were ported to Go.

## Testing Notes

- `test/test_namespace.py` is the only test: it exists so `tox`/`pytest test/` doesn't fail with "no tests collected" now that every real test (transfer, s3, checkpoint, integrity) has moved to its respective Go package's `go test ./...`.
- Coverage minimum (`--cov-fail-under 20.0`) is trivially met since there's almost no Python left to cover.
