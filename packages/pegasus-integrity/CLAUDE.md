# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-integrity` is a Go rewrite of the Python `pegasus-integrity` tool (formerly `packages/pegasus-worker/src/Pegasus/cli/pegasus-integrity.py`). It generates SHA-256 checksums (usually invoked by `pegasus-kickstart`) and verifies them against `.meta` files in the current working directory (usually invoked by PegasusLite). Same CLI flags (`--generate`, `--generate-yaml`, `--generate-fullstat-yaml`, `--verify`, `--print-timings`, `--debug`), same `;;;`-separated multi-file and `lfn=pfn` conventions, same `$KICKSTART_INTEGRITY_DATA`/`$PEGASUS_MULTIPART_DIR` env var contracts.

Ships as a single static (`CGO_ENABLED=0`) binary — no Go runtime dependency on compute nodes, and (see below) no external `openssl`/`sha256sum` dependency either. Its own Go module, independent of `packages/pegasus-transfer` and `packages/pegasus-checkpoint` (fully separate modules per tool, per the project's decision record — including duplicating `packages/pegasus-transfer/internal/integrity`'s fullstat-YAML generator rather than importing it).

## Build and Test

```bash
cd packages/pegasus-integrity
go build ./...
go vet ./...
go test ./...
gofmt -l .          # should print nothing

# From the repo root:
make build-go        # builds pegasus-transfer, pegasus-checkpoint, AND pegasus-integrity
make clean-go
```

No third-party dependencies (stdlib only), so no `go.sum`.

## Architecture

```
cmd/pegasus-integrity/     CLI entry point: flag parsing, dispatch, stdin/file/meta glob handling
internal/integrity/        SHA-256 + YAML generation, .meta parsing, verify comparison, multipart stats
testdata/                  Fixtures ported from the Python test suite (data.1/data.2/data.meta)
```

### Known deviations from the Python original

- **SHA-256 is computed natively** via `crypto/sha256`, not by shelling out to `openssl`/`sha256sum` on `PATH`. Checksum values are identical either way; this just removes an external-tool dependency and, as a side effect, the shell-quoting concerns the Python version needed `shlex.quote()` for.
- **`--generate-xmls` and `--generate-fullstat-xmls` are gone.** They were already dead code in the Python original — accepted by the option parser and counted toward its "exactly one flag" validation, but never handled by any branch of `main()`; passing either was a silent no-op that exited 0. Nothing in kickstart/PegasusLite/the planner ever passed them. Dropped from the manpage too.
- Two other confirmed-dead code paths from the Python original were not ported at all (not behavior changes — they were unreachable): a `json_object_decoder` that referenced `Transfer`/`Mkdir`/`Remove` classes not defined anywhere in that file (never wired into `json.load` as an `object_hook`), and a `backticks()` helper calling `subprocess` without ever importing it.
- `check_integrity`'s exact Python quirks are preserved deliberately (see doc comments on `CheckResult`/`CheckIntegrity` in `internal/integrity/integrity.go`): a filename with no matching `.meta` entry is a distinct "no counters touched, no `--print-timings` YAML row" path, separate from a checksum *comparison* failing (which is always counted/YAML-eligible) — and a checksum computation failure doesn't short-circuit the comparison, it flows through with the digest rendered as the literal string `"None"`, matching Python's `str(None)` stringification when `generate_sha256()` returns `None`.
