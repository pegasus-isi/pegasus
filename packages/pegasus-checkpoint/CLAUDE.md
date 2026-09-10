# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-checkpoint` is a Go rewrite of the Python `pegasus-checkpoint` tool (formerly `packages/pegasus-worker/src/Pegasus/cli/pegasus-checkpoint.py`). It periodically (or on `SIGUSR1`) archives a set of pattern-matched files/directories in the job's CWD into `pegasus.checkpoint.tar.gz` and stages it back to the workflow's staging site by calling out to the `pegasus-transfer` binary. Same CLI flags (`-p/--pattern`, `-i/--interval`, `-d/--debug`, `-l/--log-to-file`), same archive/PID-file/URL-file names.

Ships as a single static (`CGO_ENABLED=0`) binary — no Go runtime dependency on compute nodes. This is its own Go module, independent of `packages/pegasus-transfer` (see the project's decision record: fully separate modules per tool, small duplication accepted over cross-module coupling).

## Build and Test

```bash
cd packages/pegasus-checkpoint
go build ./...
go vet ./...
go test ./...
gofmt -l .          # should print nothing

# From the repo root:
make build-go        # builds pegasus-transfer AND pegasus-checkpoint
make clean-go
```

No third-party dependencies (stdlib only), so no `go.sum`.

## Architecture

```
cmd/pegasus-checkpoint/    CLI entry point: flag parsing, logging, signal/interval wiring, worker loop
internal/checkpoint/       File-pattern matching, tar.gz archiving
```

The worker loop waits on a 1-buffered channel acting as a coalescing "event" (mirrors Python's `threading.Event`): a `SIGUSR1` handler goroutine and an optional periodic-interval goroutine both signal it; the main loop drains one notification, matches patterns against the CWD, archives, and calls out to `pegasus-transfer -m 3 -n 8 -f <url-file> -s` via `PATH` — a fire-and-forget callout with no error handling beyond logging, matching the Python original's character.

### Known deviations from the Python original

- `--log-to-file` is a plain append, not `RotatingFileHandler`-style rotation — a deliberate simplification for this short-lived per-job process (see the plan of record).
- The `.tar.gz` output is **not** byte-for-byte identical to Python's `tarfile` output (e.g. `uname`/`gname` are not populated). This is fine: the checkpoint archive is opaque to the rest of Pegasus — nothing parses its tar headers, it's just staged out and later extracted by hand — so a standard, valid tar.gz from Go's stdlib is sufficient. Symlinks are preserved as symlinks (not dereferenced), matching `tarfile.add(recursive=True)`'s default.
- `re.fullmatch`-style pattern matching is emulated by anchoring each user pattern as `^(?:pattern)$` before compiling with Go's `regexp` (RE2 syntax, not Python's `re` — patterns using PCRE-only constructs would need adjusting, though none exist in the test/e2e corpus).
- Preserves one Python quirk deliberately: an explicit `-i 0` passes validation (nonnegative) but does **not** start a periodic notifier, matching `if args.interval:` treating `0` as falsy in the original.
