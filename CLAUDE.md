# Pegasus WMS — Developer Guide

Pegasus Workflow Management System (v5.2.0-dev). Multi-language scientific workflow system: Java planner, Python CLI/API/monitoring, C/C++ execution wrappers.

## Build System

Dual build system: `pip install .` for Python+Java+C unified build, Ant for legacy dist packaging.

### pip-based build (primary)

```bash
pip install .           # Build and install everything (Python + Java + C)
pip install -e .        # Editable/development install
PEGASUS_NO_JAVA=1 pip install .  # Skip Java compilation
PEGASUS_NO_C=1 pip install .     # Skip C compilation
```

Project defined in `pyproject.toml` (setuptools backend). Custom build commands in `setup.py` compile Java and C during install.

### Ant-based build (legacy dist packaging)

```bash
ant dist              # Build binary distribution (no docs)
ant dist-release      # Build binary distribution with documentation
ant compile           # Compile all code (Java + C + externals)
ant compile-java      # Compile Java only
ant compile-c         # Compile C tools (kickstart, cluster, keg)
ant clean             # Remove all build artifacts
```

Java source/target level: 1.8. Version defined in `build.properties`.

## CLI

All CLI entry points are Click subcommands under the top-level `pegasus` command:

```bash
pegasus --help          # Show all subcommands
pegasus plan            # Run the planner (Java)
pegasus status          # Check workflow status
pegasus analyzer        # Analyze workflow failures
pegasus config --bin    # Query installation paths
```

Legacy `pegasus-<tool>` commands still work via `[project.scripts]` entries in `pyproject.toml`.

CLI source: `src/Pegasus/cli/main.py` (lazy-loaded subcommand group).

## Testing

```bash
tox                   # Run Python tests (all Python versions)
tox -e py310          # Run Python tests with specific Python
tox -e lint           # Run linting (ruff)
ant test              # Run ALL tests (builds dist first, then python → java → c → transfer)
ant test-python       # Run Python tests via tox
ant test-java         # Run Java JUnit tests
ant test-c            # Run C unit tests (kickstart + PMC)
```

Test framework is pytest. Reports go to `test-reports/`.

## Code Formatting

```bash
ant code-format-python   # Format all Python code (ruff)
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
