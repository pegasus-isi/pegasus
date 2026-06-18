# Pegasus WMS — Developer Guide

Pegasus Workflow Management System (v5.2.0-dev). Multi-language scientific workflow system: Java planner, Python CLI/API/monitoring, C/C++ execution wrappers.

## Build System

scikit-build-core + CMake backend. A single `pip install .` installs the full distribution: Python packages, C binaries, and Java JARs. Java is compiled via CMake's `UseJava` module; C tools via CMake subdirectories. Version defined in `build.properties`.

```bash
# Full wheel build → dist/pegasus_wms-VERSION-PLATFORM.whl
PYTHON=.venv/bin/python make build

# Editable install for development (Python changes live; C/Java compiled once)
make dev

# Targeted partial rebuilds
make build-c       # Rebuild only C tools (pegasus-kickstart, cluster, keg)
make build-java    # Rebuild only Java JARs (pegasus.jar, pegasus-aws-batch.jar)
make build-worker  # Build self-contained worker tarball + stage it for the wheel (slow)

# Cleanup
make clean         # Remove all build artifacts (_cmake_build/, dist/, egg-info, __pycache__, docs, test reports)
make clean-c       # Remove only C cmake build output
make clean-java    # Remove only Java cmake build output
make clean-worker  # Remove worker cmake staging and the staged tarball
make clean-test    # Remove test output (reports, coverage, compiled test classes) — keeps tox venvs
make clean-doc     # Remove Sphinx output and dist/doc/
```

`PYTHON`, `CMAKE`, and `BUILD_DIR` are overridable make variables (defaults: `python3`, `cmake`, `_cmake_build`). Feature flags: `PEGASUS_NO_C=1`, `PEGASUS_NO_JAVA=1`, `PEGASUS_NO_WORKER=1` env vars skip components. CMake options: `-DPEGASUS_BUILD_C=OFF`, `-DPEGASUS_BUILD_JAVA=OFF`, `-DPEGASUS_BUILD_WORKER=ON`.

Java source/target compatibility: 1.8 (`--release 8` via `CMAKE_JAVA_COMPILE_FLAGS`). All non-AWS Java sources (`edu.isi.pegasus.*`, `edu.isi.ikcap`, `org.griphyn.vdl`) compile into a single `pegasus.jar`; AWS Batch sources compile into `pegasus-aws-batch.jar`.

## Testing

```bash
# Run all tests (Python + Java + C)
make test

# Python tests — each package has its own tox.ini
make test-python   # runs tox for all four packages
# or individually:
cd packages/pegasus-python && tox -e py310
cd packages/pegasus-api && tox -e py310
cd packages/pegasus-common && tox -e py310
cd packages/pegasus-worker && tox -e py310

# Java unit tests (JUnit 5) — requires make build-java first
make test-java

# C tests — requires make build-c first
make test-c
# or directly:
cd packages/pegasus-kickstart/test
PEGASUS_BIN_DIR=$(pwd)/../../../_cmake_build/packages/pegasus-kickstart ./test.sh
```

Test framework is pytest for Python. Reports go to `test-reports/`.

## Code Formatting

```bash
# Python (run from each package directory)
cd packages/pegasus-python && tox -e lint
cd packages/pegasus-api && tox -e lint
cd packages/pegasus-common && tox -e lint
cd packages/pegasus-worker && tox -e lint
```

- **Python**: ruff (check + format), configured in `.pre-commit-config.yaml`
- **Java**: google-java-format 1.7, AOSP style. Applies to `src/**/*.java` and `test/junit/**/*.java`. See `.pre-commit-config.yaml` for the exact invocation.
- Pre-commit hooks available in `.pre-commit-config.yaml`

## Documentation

```bash
make doc           # Build all docs → dist/doc/ (Sphinx HTML+man, Javadoc, schemas)
make doc-sphinx    # Sphinx user guide (HTML + man pages; PDF if latexmk is installed)
make doc-java      # Javadoc for planner dax/selector API (needs make build-java first)
make doc-schemas   # Copy XSD/XML/YAML schemas to dist/doc/schemas/
make doc-dist      # Package dist/doc/ → dist/pegasus-doc-VERSION.tar.gz
make clean-doc     # Remove doc/sphinx/_build/, doc/sphinx/python/, dist/doc/
```

Sphinx deps (sphinx, sphinx_rtd_theme, sphinxcontrib-openapi, etc.) are managed by `tox -e docs` in `packages/pegasus-python/`. PDF generation requires `latexmk`; skipped automatically when not installed.

## Architecture

```
Java (Planner)     — Workflow planning, mapping, site selection, code generation
Python (Tools)     — CLI tools, workflow API, monitoring, dashboard, statistics
C/C++ (Execution)  — Job wrappers (kickstart), clustering, test job generation (keg)
```

## Source Locations

### Java — `src/`

Main planner: `src/edu/isi/pegasus/planner/` with these packages:

| Package        | Purpose                                      |
| -------------- | -------------------------------------------- |
| `catalog/`     | Site, Replica, Transformation, Work catalogs |
| `classes/`     | Core domain models                           |
| `client/`      | Client-facing Java APIs                      |
| `cluster/`     | Job clustering/aggregation                   |
| `code/`        | Code generation (GridStart, submit scripts)  |
| `common/`      | Shared utilities, credentials, logging       |
| `dax/`         | DAX parsing and handling                     |
| `estimate/`    | Resource estimation                          |
| `mapper/`      | Output, staging, submit mapping              |
| `namespace/`   | Job specification namespaces                 |
| `parser/`      | DAX and config parsing                       |
| `partitioner/` | Workflow partitioning                        |
| `refiner/`     | Cleanup, directory creation refinement       |
| `selector/`    | Site/replica/transformation selection        |
| `transfer/`    | Data transfer management                     |

Other Java packages: `edu.isi.pegasus.common.*`, `edu.isi.pegasus.aws.batch.*`, `org.griphyn.vdl.*`

### Python — `packages/`

Four namespace packages sharing the `Pegasus` namespace:

| Package           | What it provides                                                            |
| ----------------- | --------------------------------------------------------------------------- |
| `pegasus-api/`    | Workflow definition API (`Workflow`, `Job`, `File`, `Site`, catalogs)       |
| `pegasus-common/` | Shared utilities (`braindump`, YAML/JSON handling, client utils)            |
| `pegasus-python/` | CLI tools, monitoring daemon, database layer, dashboard, statistics         |
| `pegasus-worker/` | Worker-side execution: data transfer (`transfer.py`, `s3.py`), worker utils |

Python source lives under `packages/<pkg>/src/Pegasus/`.

### C/C++ — `packages/`

| Package                | Language | Purpose                                               |
| ---------------------- | -------- | ----------------------------------------------------- |
| `pegasus-kickstart/`   | C        | Job execution wrapper, metadata capture, checksumming |
| `pegasus-cluster/`     | C        | Groups multiple jobs into clustered execution         |
| `pegasus-keg/`         | C++      | Synthetic job generator for testing                   |
| `pegasus-mpi-cluster/` | C++      | MPI-based distributed job clustering                  |

### CLI Entry Points

Installed as pip console scripts by `pyproject.toml`. After `pip install .` or `make dev`:

- `pegasus <subcommand>` — unified Click CLI (`Pegasus.cli.main:cli`)
- `pegasus-plan`, `pegasus-status`, `pegasus-rc-client`, etc. — legacy entry points (same functions, backward-compatible names)
- C binaries (`pegasus-kickstart`, `pegasus-cluster`, `pegasus-keg`) — installed to `<venv>/bin/` directly by CMake via the wheel scripts section

## Configuration and Catalogs

Configuration files and examples in `etc/`:

- `basic.properties` — Default Pegasus properties
- `sample.sites.xml4` — Site catalog example (XML v4)
- `sample.rc.data` — Replica catalog example
- `sample.tc.text` — Transformation catalog example
- `etc/yaml/` — YAML format examples (`sc.yml`, `rc.yml`, `tc.yml`, `workflow.yml`)

Three catalog types:

- **Site Catalog** — Defines compute resources and their configurations
- **Replica Catalog** — Tracks physical locations of data files
- **Transformation Catalog** — Maps logical transformations to physical executables

## Test Directory

```
test/
├── junit/     # Java JUnit tests
├── unit/      # Unit tests
├── core/      # Integration/core tests
├── common/    # Common test utilities
├── scripts/   # Test scripts
└── visualize/ # Visualization tests
```
