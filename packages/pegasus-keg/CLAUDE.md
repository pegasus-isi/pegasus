# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is pegasus-keg?

Kanonical Executable for Grids — a synthetic C++ job generator for testing Pegasus workflows. It stands in for real application binaries in DAGs, allowing workflow execution tracing and debugging without running actual computations.

## Build Commands

```bash
make                    # Build pegasus-keg (skipped on musl/Alpine)
make pegasus-keg        # Build standard executable
make pegasus-mpi-keg    # Build MPI-enabled variant (requires mpicc)
make install            # Install to $PEGASUS_HOME/bin (set prefix= to override)
make clean              # Remove object files and version.h
make distclean          # Remove object files and built binary
```

From the top-level Pegasus repo:
```bash
ant compile-c           # Builds keg along with kickstart and cluster
ant test-c              # Runs C test suite
```

## Running Tests

```bash
make test               # Runs: ./pegasus-keg -o /dev/fd/1
make test-mpi           # Runs: mpiexec -n 2 ./pegasus-mpi-keg -o /dev/fd/1
```

There is no unit test suite — the test target simply runs the binary and checks it doesn't crash.

## Source Architecture

Single main source file with platform-specific modules:

| File | Purpose |
|------|---------|
| `pegasus-keg.cc` | All core logic: arg parsing, I/O, CPU spin (Julia set fractal), memory allocation, network/system info |
| `darwin.cc/hh` | macOS system info (sysctl, Mach kernel APIs, getmntinfo) |
| `linux.cc/hh` | Linux system info (/proc, sysinfo, statvfs) |
| `gnukfreebsd.cc/hh` | GNU/kFreeBSD support (partial — some stats unavailable) |
| `basic.cc/hh` | Platform-independent utilities (`smart_units` byte formatting) |
| `genversion.sh` | Generates `version.h` from git commit info at build time |

## Key Design Decisions

- **Avoids libstdc++ linking**: Uses `malloc`/`free` and a custom `DirtyVector` class instead of STL containers and `new`/`delete`, keeping the binary lightweight.
- **Platform detection via Makefile**: `uname -s` sets `-DDARWIN`, `-DLINUX`, or `-DGNUKFREEBSD`; the `MACHINE_SPECIFIC` macro selects the right headers.
- **MPI variant**: Same source (`pegasus-keg.cc`) compiled with `-DWITH_MPI` and linked via `mpicc`.
- **Skipped on musl libc**: Build is a no-op on Alpine Linux (detected via `gcc -dumpmachine | grep musl`).

## Exit Codes

- **0**: Success
- **1**: Failed to open input file
- **2**: Failed to open output/log file
- **3**: Time budget exceeded (I/O exceeded specified spin/sleep time)
