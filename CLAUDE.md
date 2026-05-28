# Pegasus WMS — Developer Guide

Pegasus Workflow Management System (v5.2.0-dev). Multi-language scientific workflow system: Java planner, Python CLI/API/monitoring, C/C++ execution wrappers.

## Build System

Primary build: **scikit-build-core** (PEP 517) drives **CMake** to compile Java and C during `pip install`. No `setup.py`, no Ant for compilation.

### pip / wheel build (primary)

```bash
pip install .                    # Build and install everything (Python + Java + C)
pip install -e . --no-build-isolation  # Editable/development install (compiles once)
PEGASUS_NO_JAVA=1 pip install .  # Skip Java compilation
PEGASUS_NO_C=1 pip install .     # Skip C compilation
python -m build --wheel          # Build a distributable wheel into dist/
```

`pyproject.toml` uses `scikit_build_core.build` backend. CMake targets compile Java via `UseJava` (`add_jar()`) and C via native `add_executable()`. No `setup.py`.

### Makefile shortcuts

```bash
make build         # python -m build --wheel
make dev           # pip install -e . --no-build-isolation
make build-c       # cmake configure + build, Java OFF
make build-java    # cmake configure + build, C OFF
make build-worker  # build worker package tarball (slow: runs pip install)
make clean         # remove _cmake_build/, dist/, *.egg-info
make clean-java    # remove _cmake_build/java_src only
make clean-c       # remove _cmake_build/c_src only
make clean-worker  # remove _cmake_build/worker_package only
```

### Standalone CMake (no Python)

```bash
cmake -B _cmake_build -S .
cmake --build _cmake_build
cmake --install _cmake_build --prefix .   # installs to ./bin/, ./lib/, ./share/
```

### Install layout

| Artifact | pip install location | standalone cmake |
|----------|---------------------|-----------------|
| `pegasus-kickstart`, `pegasus-cluster`, `pegasus-keg` | `<venv>/bin/` (on PATH) | `<prefix>/bin/` |
| `libinterpose.so` | `<venv>/lib/pegasus/` | `<prefix>/lib/pegasus/` |
| `pegasus.jar`, `pegasus-aws-batch.jar` | `<site-packages>/Pegasus/data/java/` | `<prefix>/share/pegasus/java/` |
| `pegasus-worker-VERSION-PLATFORM.tar.gz` | `<site-packages>/Pegasus/data/worker-packages/` | `<prefix>/share/pegasus/worker-packages/` |

C binaries are ELF executables installed directly to `<venv>/bin/` — **not** Python wrapper scripts.

### CMake structure

```
CMakeLists.txt               # top-level: version, feature flags, install destinations
c_src/kickstart/CMakeLists.txt
c_src/cluster/CMakeLists.txt
c_src/keg/CMakeLists.txt
c_src/mpi-cluster/CMakeLists.txt  # optional; enable with -DPEGASUS_BUILD_MPI=ON
java_src/CMakeLists.txt      # add_jar() for pegasus.jar + pegasus-aws-batch.jar
worker_package/CMakeLists.txt   # worker tarball (ON by default)
worker_package/assemble.cmake   # assembly script invoked by cmake -P
```

Feature flags (CMake options and env var overrides):

| Flag | Default | Env override |
|------|---------|--------------|
| `PEGASUS_BUILD_C` | ON | `PEGASUS_NO_C=1` |
| `PEGASUS_BUILD_JAVA` | ON | `PEGASUS_NO_JAVA=1` |
| `PEGASUS_BUILD_MPI` | OFF | — |
| `PEGASUS_BUILD_WORKER` | ON | `PEGASUS_NO_WORKER=1` |

### Worker package

The worker package (`pegasus-worker-VERSION-PLATFORM.tar.gz`) is a self-contained tarball deployed to remote compute nodes for nonsharedfs/PegasusLite execution. It bundles:
- C binaries (`pegasus-kickstart`, `pegasus-cluster`, `pegasus-keg`)
- Python worker scripts (`pegasus-transfer`, `pegasus-s3`, `pegasus-exitcode`, `pegasus-integrity`, `pegasus-checkpoint`)
- The Pegasus Python package (`src/Pegasus/`)
- External Python deps from `src/requirements.txt` (boto3, globus_sdk, Flask, etc.)
- Shell helpers from `src/Pegasus/data/sh/`, `notification/`, `htcondor/`, `schema/`

Built by default as part of `pip install .` and `make build`. To skip it:

```bash
PEGASUS_NO_WORKER=1 pip install .
```

To build only the worker package:

```bash
make build-worker
```

The Java planner (`CreateWorkerPackage.java`) looks for the tarball at `${PEGASUS_SHARE_DIR}/worker-packages/`.

Java source/target level: 1.8. Version read from `build.properties`.

### Ant (legacy dist packaging only)

Ant is **not** used for compilation. It remains available for distribution packaging targets only:

```bash
ant dist              # Build binary distribution (no docs)
ant dist-release      # Build binary distribution with documentation
ant test              # Run ALL tests (python → java → c → transfer)
ant test-python       # Run Python tests via tox
ant test-java         # Run Java JUnit tests
ant test-c            # Run C unit tests (kickstart + PMC)
```

## CLI

All CLI entry points are Click subcommands under the top-level `pegasus` command:

```bash
pegasus --help          # Show all subcommands
pegasus plan            # Run the planner (Java)
pegasus status          # Check workflow status
pegasus analyzer        # Analyze workflow failures
pegasus config --bin    # Query installation paths
```

Legacy `pegasus-<tool>` commands still work via `[project.scripts]` entries in `pyproject.toml`. Note: `pegasus-kickstart`, `pegasus-cluster`, and `pegasus-keg` are C binaries installed directly to `<venv>/bin/` by CMake — they are not Python wrapper scripts.

CLI source: `src/Pegasus/cli/main.py` (lazy-loaded subcommand group).

## Testing

```bash
tox                   # Run Python tests (all Python versions)
tox -e py310          # Run Python tests with specific Python
tox -e lint           # Run linting (ruff)
ant test              # Run ALL tests (python → java → c → transfer)
ant test-python       # Run Python tests via tox
ant test-java         # Run Java JUnit tests
ant test-c            # Run C unit tests (kickstart + PMC)
```

Test framework is pytest. Reports go to `test-reports/`.

## Code Formatting

```bash
ruff check src/          # Lint Python
ruff format src/         # Format Python
ant code-format-python   # Format all Python code via ruff (Ant wrapper)
ant code-format-java     # Format all Java code (AOSP style via google-java-format)
```

- **Python**: ruff (check + format), configured in `.pre-commit-config.yaml`
- **Java**: google-java-format 1.7, AOSP style. Applies to `java_src/**/*.java` and `test/junit/**/*.java`

## Architecture

```
Java (Planner)     — Workflow planning, mapping, site selection, code generation
Python (Tools)     — CLI tools, workflow API, monitoring, dashboard, statistics
C/C++ (Execution)  — Job wrappers (kickstart), clustering, test job generation (keg)
```

## Source Locations

### Java — `java_src/`

Main planner: `java_src/edu/isi/pegasus/planner/` with these packages:

| Package | Purpose |
|---------|---------|
| `catalog/` | Site, Replica, Transformation, Work catalogs |
| `classes/` | Core domain models |
| `client/` | Client-facing Java APIs |
| `cluster/` | Job clustering/aggregation |
| `code/` | Code generation (GridStart, submit scripts) |
| `common/` | Shared utilities, credentials, logging |
| `dax/` | DAX parsing and handling |
| `estimate/` | Resource estimation |
| `mapper/` | Output, staging, submit mapping |
| `namespace/` | Job specification namespaces |
| `parser/` | DAX and config parsing |
| `partitioner/` | Workflow partitioning |
| `refiner/` | Cleanup, directory creation refinement |
| `selector/` | Site/replica/transformation selection |
| `transfer/` | Data transfer management |

Other Java packages: `edu.isi.pegasus.common.*`, `edu.isi.pegasus.aws.batch.*`, `org.griphyn.vdl.*`

### Python — `src/Pegasus/`

Single unified package (merged from former `pegasus-api`, `pegasus-common`, `pegasus-python`, `pegasus-worker`):

| Module | What it provides |
|--------|-----------------|
| `api/` | Workflow definition API (`Workflow`, `Job`, `File`, `Site`, catalogs) |
| `braindump.py`, `yaml.py`, `json.py` | Shared utilities (braindump, YAML/JSON handling) |
| `cli/` | CLI tools (Click subcommands), monitoring daemon |
| `db/` | Database layer |
| `service/` | Dashboard web service |
| `monitoring/` | Workflow monitoring |
| `transfer/`, `s3.py` | Worker-side data transfer |

### C/C++ — `c_src/`

| Directory | Language | Purpose |
|-----------|----------|---------|
| `kickstart/` | C | Job execution wrapper, metadata capture, checksumming |
| `cluster/` | C | Groups multiple jobs into clustered execution |
| `keg/` | C++ | Synthetic job generator for testing |
| `mpi-cluster/` | C++ | MPI-based distributed job clustering |

### Package Data — `src/Pegasus/data/`

Runtime data bundled with the Python package:

| Directory | Contents |
|-----------|----------|
| `java/` | JAR dependencies (planner, AWS, etc.) |
| `schema/` | XML/XSD/YAML schemas |
| `sh/` | Shell helpers (java.sh, pegasus-lite-*.sh) |
| `notification/` | Notification scripts |
| `htcondor/` | HTCondor configuration |
| `share/` | pegasus-configure-glite |

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
