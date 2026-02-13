# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

**Standalone (from this directory):**
```bash
make                    # Build pegasus-kickstart binary
make clean              # Remove object files
make distclean          # Full clean including binary
```

**From parent repo root (`pegasus/`):**
```bash
ant compile-pegasus-kickstart   # Compile kickstart only
ant compile-c                   # Compile all C tools
ant test-kickstart              # Run kickstart tests (builds first, sets PEGASUS_BIN_DIR)
ant test-c                      # Run all C tests (kickstart + PMC)
```

The `ant test-kickstart` target is the standard way to run tests — it builds the full distribution first and sets the required `PEGASUS_BIN_DIR` environment variable automatically.

## Testing

Tests are shell-based integration tests in `test/test.sh`. Each test function invokes `pegasus-kickstart` and validates the YAML output. Tests require `PEGASUS_BIN_DIR` pointing to a built Pegasus `bin/` directory (needed for `pegasus-integrity` and `yaml-validator`).

```bash
# Preferred: from repo root
ant test-kickstart

# Manual: from this directory (after ant dist)
cd test && PEGASUS_BIN_DIR=../../../dist/pegasus-*/bin ./test.sh
```

Some tests are Linux-only (ptrace-based tracing: `lotsofprocs_trace`, `lotsofprocs_trace_buffer`).

## Architecture

Pegasus-kickstart wraps job execution to capture metadata (timing, resource usage, file states, checksums) and produces YAML output. It supports pre/post/setup/cleanup jobs around the main application.

### Execution Flow (pegasus-kickstart.c)

1. Parse CLI arguments → set up `AppInfo` struct
2. Change to working directory (`-w`/`-W`)
3. Stat initial files (`-S` option)
4. Execute setup → prejob → **main job** → postjob → cleanup (each via `mysystem.c` fork/exec)
5. Stat final files (`-s` option)
6. Write YAML report to logfile

### Key Modules

| Module | Purpose |
|--------|---------|
| `pegasus-kickstart.c` | Entry point, CLI parsing, orchestrates execution flow |
| `appinfo.c/h` | Top-level `AppInfo` struct — holds entire execution context |
| `jobinfo.c/h` | Per-job metadata (setup/pre/main/post/cleanup) |
| `mysystem.c/h` | Fork/exec/wait with signal handling and timeout |
| `procinfo.c/h` | Per-process metrics via ptrace (Linux), resource tracking |
| `statinfo.c/h` | File stat() capture and YAML formatting |
| `machine/` | Platform abstraction — `basic.c` (common), `linux.c`, `darwin.c` |
| `interpose.c` | `LD_PRELOAD` library for I/O interception (Linux x86_64 only) |
| `checksum.c` + `sha2.c` | SHA-256 file integrity verification |
| `parse.c` | Argument parsing and expansion |
| `utils.c` | YAML quoting, time formatting helpers |

### Platform Abstraction

`machine.c` uses function pointers (ctor/show/dtor) to dispatch to platform-specific implementations:
- **Linux**: reads `/proc/` filesystem for CPU, memory, load, boot time
- **Darwin**: uses `sysctl` calls for system info

Conditional compilation via `-DLINUX` or `-DDARWIN` (auto-detected by Makefile from `uname -s`).

### Conditional Features

- **libinterpose.so**: Built only on Linux x86_64 (non-musl). Interposes libc I/O calls via `LD_PRELOAD`.
- **PAPI support**: Enabled if `libpapi.so` and `papi.h` are found (Linux x86_64). Adds CPU cache/instruction counters.
- **ptrace tracing** (`-t` flag): Linux-only process tracing for detailed per-process I/O and syscall metrics.
- **LFS**: Large File Support flags added automatically on Linux.

## Compiler Settings

- `gcc -Wall -O2 -ggdb -std=gnu99`
- GCC >= 14 adds `-fpie`
- Links `-lm`

## Output Format

YAML v3.0 schema containing: workflow/job identifiers, timestamps, machine info, resource usage (rusage), process hierarchy, file stat data with optional SHA-256 checksums, and captured stdout/stderr.
