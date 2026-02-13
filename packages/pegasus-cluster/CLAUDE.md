# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

pegasus-cluster is a C utility that reads a list of shell commands (from stdin or a file) and executes them in parallel up to a configurable CPU limit. It is the non-MPI job clustering tool in Pegasus WMS — used to group multiple small jobs into a single clustered execution for efficiency.

## Build Commands

```bash
make                  # Build pegasus-cluster binary
make install          # Install to $(prefix)/bin (default: ../../../bin)
make check            # Run functional tests (check.sh)
make clean            # Remove .o files and dependency tracking
make distclean        # Remove .o files and built binaries
```

From the top-level Pegasus repository:

```bash
ant compile-pegasus-cluster   # Build via Ant
ant test-c                    # Run all C tests (kickstart + PMC)
```

## Source Architecture

The codebase is ~8 C source files with matching headers:

| Module | Role |
|--------|------|
| `pegasus-cluster.c` | Main entry point: CLI parsing, event loop, fork/wait orchestration |
| `job.c/h` | Job slot data structure (`Job`, `Jobs`) and slot management |
| `parser.c/h` | Shell-style command line parsing via finite state automaton (handles quoting/escaping) |
| `mysystem.c/h` | Signal management (SIGINT/SIGQUIT/SIGCHLD) and child process execution |
| `report.c/h` | Progress reporting with POSIX file locking; handles kickstart-wrapped commands |
| `tools.c/h` | Atomic I/O helpers, timestamp formatting (ISO 8601), timing utilities |
| `statinfo.c/h` | Executable search via PATH lookup |
| `try-cpus.c` | Standalone utility to detect processor count |

**Execution flow:** Parse CLI → run optional `SEQEXEC_SETUP` → read commands line-by-line → fork up to N parallel children → wait and report → run optional `SEQEXEC_CLEANUP` → exit with aggregated status.

## Key Environment Variables

- `SEQEXEC_PROGRESS_REPORT` — Progress file path
- `SEQEXEC_CPUS` — Number of CPUs to use
- `SEQEXEC_SETUP` — Command to run before all tasks
- `SEQEXEC_CLEANUP` — Command to run after all tasks

## Exit Code Behavior

- **Default:** Returns 0 if all jobs succeed, 5 if any fail
- **`-e` flag:** Always returns 0 (legacy mode)
- **`-f` flag:** Returns 5 on first failure and stops processing
- **`-S <code>`:** Treat specific non-zero exit code as success

## Code Conventions

- C99 (`-std=gnu99`), compiled with `-Wall -O2 -ggdb`
- K&R brace style
- snake_case for variables, PascalCase for typedef'd structs (`Job`, `Jobs`, `Signals`)
- Module-prefixed functions: `job_done()`, `jobs_init()`, `jobs_first_slot()`
- Comment style: `/* paramtr/returns */` blocks for function documentation
- Atomic writes via `writen()` for signal safety
