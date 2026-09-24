# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

pegasus-wms.api — Python API for defining Pegasus scientific workflows. This is a namespace package under the `Pegasus` namespace, providing `Pegasus.api`. It depends only on `pegasus-wms.common` (sibling package at `../pegasus-common`).

## Commands

### Testing

Run these from the **repo root**. Tests run against the installed pegasus-wms
wheel, with pytest and coverage configured in the root `pyproject.toml`
(see the root `tox.toml`):

```bash
tox -m api                                        # Run this package's suite
tox -m api -- -k "test_add_job"                   # Run tests matching a pattern
tox -e 3.12 -- packages/pegasus-api/test/api/test_workflow.py            # A single file (any interpreter)
tox -e 3.12 -- packages/pegasus-api/test/api/test_workflow.py::TestWorkflow::test_add_job
```

Coverage minimum for `tox -m api` is 99% (the `api` env in the root `tox.toml`).
Reports go to `test-reports/api/` at the repo root.

### Linting and Formatting

```bash
tox -e lint    # from the repo root; lints all three Python packages
```

Runs `ruff check` then `ruff format --check`, using the `[tool.ruff]` config in
this package's `pyproject.toml`. To fix formatting, run `ruff format` (or
pre-commit).

## Architecture

All source lives in `src/Pegasus/api/`. The module is a fluent API — most mutating methods return `self` via the `@_chained` decorator in `_utils.py`.

### Module Responsibilities

- **workflow.py** — Core API. `Workflow`, `Job`, `SubWorkflow`, `AbstractJob`. Workflow execution methods (`plan`, `submit`, `wait`, etc.) delegate to `Pegasus.client._client.Client`.
- **site_catalog.py** — `SiteCatalog`, `Site`, `Directory`, `FileServer`, `Grid`. Defines compute resources.
- **transformation_catalog.py** — `TransformationCatalog`, `Transformation`, `TransformationSite`, `Container`. Maps logical transformations to executables.
- **replica_catalog.py** — `ReplicaCatalog`, `File`. Maps logical filenames to physical locations.
- **properties.py** — `Properties`. Dict-like interface for Pegasus configuration properties.
- **mixins.py** — `ProfileMixin`, `HookMixin`, `MetadataMixin`. Reusable behaviors mixed into jobs, workflows, and catalog entries. Defines `Namespace` and `EventType` enums.
- **writable.py** — `Writable` base class. YAML/JSON serialization with `__json__()` protocol and x-pegasus metadata stamping.
- **errors.py** — `PegasusError`, `DuplicateError`, `NotFoundError`.

### Key Patterns

- **Serialization**: All catalog and workflow classes extend `Writable`. Each has a `_DEFAULT_FILENAME` (e.g., `workflow.yml`, `sites.yml`). Serialization uses `_CustomEncoder` for JSON and `OrderedDict` to preserve field order.
- **Schema validation**: Tests validate serialized output against YAML schemas in `share/pegasus/schema/yaml/` (converted to JSON at test time via `conftest.py`).
- **Namespace profiles**: The `ProfileMixin` supports namespaces (PEGASUS, CONDOR, DAGMAN, ENV, GLOBUS, SELECTOR, STAT) with convenience methods like `add_condor_profile()` that map Python kwargs to profile keys.
- **Client integration**: `Workflow` methods decorated with `@_needs_client` and `@_needs_submit_dir` gate execution operations behind proper initialization.

## Testing

Tests are in `test/api/` and mirror the source module structure. Key fixture in `conftest.py`: `convert_yaml_schemas_to_json` (module-scoped) loads and converts YAML schemas for validation tests.
