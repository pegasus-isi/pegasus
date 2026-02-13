# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-mpi-cluster` (PMC) — MPI-based workflow execution engine for DAGs. Rank 0 acts as master (parses DAG, schedules tasks), ranks 1..N are workers (execute tasks via fork/exec). Part of the Pegasus WMS C/C++ toolchain.

## Build Commands

```bash
make                    # Build pegasus-mpi-cluster (requires mpicxx in PATH)
make test               # Build + run unit tests + cppcheck + integration tests (test/test.sh)
make clean              # Remove .o files, test binaries, version.h, depends.mk
make distclean          # clean + remove pegasus-mpi-cluster binary
make install            # Install to $(PEGASUS_HOME)/bin (or override with prefix=)

# From repo root
ant compile-pegasus-mpi-cluster   # Build via ant
ant test-pmc                      # Run PMC tests via ant
```

The compiler is `mpicxx` by default. Override with `CXX=<path-to-mpi-compiler> make`.

## Running Tests

Unit tests are standalone C++ executables (no test framework):
```bash
./test-strlib           # String library tests
./test-dag              # DAG parsing tests
./test-engine           # Workflow engine tests
./test-fdcache          # File descriptor cache tests
./test-log              # Logging tests
./test-protocol         # MPI protocol tests
./test-scheduler        # Task scheduler tests
./test-tools            # Utility function tests
```

Integration tests run via `test/test.sh` using `mpiexec`. The script defines individual test functions (test_run_diamond, test_rescue_file, test_memory_limit, etc.) that exercise the full master-worker flow with test DAGs in `test/`.

## Running the Program

```bash
mpirun -n <nprocs> pegasus-mpi-cluster [options] <dagfile>
```

Minimum 2 processes (1 master + 1 worker). See `--help` for all options.

## Architecture

**Master-Worker MPI model:**
- `pegasus-mpi-cluster.cpp` — Entry point, argument parsing, dispatches to master or worker based on MPI rank
- `master.cpp/h` — Rank 0: registers workers, schedules tasks from a priority queue, collects results, manages Host/Slot resource tracking
- `worker.cpp/h` — Ranks 1..N: receives tasks, executes via fork/exec (TaskHandler), reports results back; supports host scripts, CPU affinity, I/O forwarding
- `engine.cpp/h` — Workflow orchestration: dependency tracking, `next_ready_task()`/`mark_task_finished()`, rescue file management
- `dag.cpp/h` — DAG file parser, Task data model (name, args, memory, cpus, priority, dependencies, forwards)
- `protocol.cpp/h` — Binary message types: COMMAND, RESULT, SHUTDOWN, REGISTRATION, HOSTRANK, IODATA
- `mpicomm.cpp/h` — MPI send/recv wrapper with non-blocking polling and timeout support

**Supporting modules:**
- `fdcache.cpp/h` — LRU file descriptor cache to avoid exhaustion
- `log.cpp/h` — Log levels: FATAL, ERROR, WARN, INFO, DEBUG, TRACE
- `tools.cpp/h` — System utilities (hostname, memory/CPU detection, path handling, affinity)
- `strlib.cpp/h` — String splitting, trimming, argument parsing
- `failure.cpp/h` — Exception wrapper that triggers MPI_Abort on uncaught errors
- `config.cpp/h` — Global configuration (CPU affinity settings)

**DAG file format** (subset of Condor DAGMan):
```
TASK <name> <command> [args...]
EDGE <parent> <child>
PRIORITY <task> <value>
MEMORY <task> <MB>
CPUS <task> <count>
```

## Platform Notes

- **Linux**: Auto-detects libnuma for NUMA memory affinity (`-DHAS_LIBNUMA`)
- **macOS**: Code-signs binary with "Pegasus Development" identity
- **Compiler flags**: `-g -Wall -ansi` (C++98/ANSI compliant)
- **Optional Makefile flags**: `-DSYNC_RESCUE` / `-DSYNC_IODATA` for fsync protection (adds ~10ms overhead per write); gprof (`-pg`) and gperftools (`-lprofiler`) profiling support (commented out in Makefile)
- `version.h` is auto-generated from `../../release-tools/getversion --header`
- `depends.mk` is auto-generated for header dependency tracking
