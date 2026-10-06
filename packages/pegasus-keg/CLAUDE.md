# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is pegasus-keg?

Kanonical Executable for Grids — a synthetic job generator for testing Pegasus workflows. It stands in for real application binaries in DAGs, allowing workflow execution tracing and debugging without running actual computations.

It is a Go port of the original C++ `pegasus-keg.cc` (kept in `mpi/` only for the MPI variant): same flags, same output format, same exit codes. It builds as a single static `CGO_ENABLED=0` binary with no module dependencies.

## Build Commands

```bash
go build ./cmd/pegasus-keg     # Build locally
go test ./...                  # Unit tests
go vet ./...
```

From the top-level Pegasus repo:

```bash
make build-go                                         # Build all Go tools via CMake
cmake --build _cmake_build --target pegasus-keg-go    # Build keg only
make test-go                                          # Run all Go modules' tests
```

The MPI variant is C++ and not part of the CMake build:

```bash
make -C mpi pegasus-mpi-keg    # Requires mpicc
make -C mpi test               # Runs: mpiexec -n 2 ./pegasus-mpi-keg -o /dev/fd/1
```

## Source Architecture

| File                       | Purpose                                                                |
| -------------------------- | ---------------------------------------------------------------------- |
| `cmd/pegasus-keg/main.go`  | Entry point → `keg.Run`                                                |
| `internal/keg/args.go`     | Hand-rolled argument state machine (multi-value `-i a b c`), help text |
| `internal/keg/keg.go`      | Phases: read inputs, write outputs, spin, sleep, append log            |
| `internal/keg/spin.go`     | CPU burn via random Julia set iterations                               |
| `internal/keg/identify.go` | Primary IPv4 + hostname line (`IP addr and hostname: ...`)             |
| `mpi/pegasus-keg.cc`       | Original C++ source, built only as `pegasus-mpi-keg` (`-DWITH_MPI`)    |

Tests replace the `now`, `sleepFor`, `spinFor`, `identity`, `lookupAddr` and `lookupHost` package variables with a virtual clock, a fixed identity line and fake name lookups. `testdata/help.golden` is the C++ keg's help output.

## Behavior Notes

- `-e`, `-p`, `-C` are accepted but have no effect (the C++ keg stopped printing them in 2014); kept so existing workflows' arguments still parse.
- `-P` prefix is written once at the start of each output file, not per input line.
- Memory model matches the C++ keg: inputs are read (single copy) into the `-m` mmap region when they fit, otherwise into a heap buffer that replaces it, so peak usage is max(`-m`, inputs). A failed `-m` allocation is reported and keg carries on.
- Fixed relative to the C++ keg (intentional differences):
  - `-s` combined with `-t`/`-T` no longer wraps around to a near-infinite sleep.
  - stdin (`-i -`) is read once; repeated `-o -` all write to stdout (C++ closed stdout after the first).
  - `fn=<n>` without a unit no longer drops the last digit.
  - `-m` memory never leaks into outputs (C++ wrote leftover bytes, e.g. a stray `Z`, or read past its buffer).
  - Input files are copied byte for byte (C++ truncated at NUL bytes).
  - Output write/close errors exit 2 (C++ ignored them); input open errors are reported once.
  - Link-local addresses are not preferred as the primary IP; on macOS the interface list is read correctly.
  - Numbers parse like C `strtoul` (whitespace, `+`, saturation), except a `-` sign yields 0 instead of wrapping.
- With `CGO_ENABLED=0`, host lookups use Go's resolver (files + DNS), not NSS modules; with no usable address the queried hostname is printed rather than its canonical name.

## Exit Codes

- **0**: Success
- **1**: Failed to create an output file's parent directory
- **2**: Failed to open an input or output file
- **3**: Time budget exceeded (I/O exceeded specified spin/sleep time)
