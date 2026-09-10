# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

pegasus-wms.api — Python API for defining Pegasus scientific workflows. This is a namespace package under the `Pegasus` namespace, providing `Pegasus.api`. It depends only on `pegasus-wms.common` (sibling package at `../pegasus-common`).

## Commands

### Testing

Run these from the **repo root** — envs live in the root `tox.toml`:

```bash
tox -e api                                       # Run full test suite
tox -e api -- test/api/test_workflow.py          # Run a single test file
tox -e api -- test/api/test_workflow.py::TestWorkflow::test_add_jobs
tox -e api -- -k "test_add_jobs"                 # Run tests matching a pattern
```

Coverage minimum is 99% — set in `[tool.coverage.report] fail_under` in this
package's `pyproject.toml`. Reports go to `test-reports/`.

### Linting and Formatting

```bash
tox -e lint-api    # from the repo root
```

Runs `ruff check` then `ruff format` (which rewrites files in place), using the
`[tool.ruff]` config in this package's `pyproject.toml`.

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
