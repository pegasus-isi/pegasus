# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-transfer` is a Go rewrite of the Python `pegasus-transfer` tool (formerly `packages/pegasus-worker/src/Pegasus/transfer.py`), with `pegasus-s3`'s S3 support merged in natively. It is a **strict drop-in replacement**: same executable name, CLI flags (`-f -m -n -s -d`), JSON input contract (stdin or `-f file`), exit codes, and credential file conventions as the tool it replaces. See `[[go-transfer-rewrite-plan]]` in project memory for the full decision record this was built against.

Ships as a single static (`CGO_ENABLED=0`) binary — no Go runtime dependency on compute nodes.

## Build and Test

```bash
cd packages/pegasus-transfer
go build ./...
go vet ./...
go test ./...
gofmt -l .          # should print nothing

# From the repo root:
make build-go        # CMake-driven build (also usable standalone)
make clean-go
```

No vendoring — modules resolve from the network at build time (see the project's decision record). A from-scratch build requires outbound network access.

## Architecture

```
cmd/pegasus-transfer/   CLI entry point: flag parsing, handler registry wiring, engine invocation
internal/model/         PegasusURL, Transfer/Mkdir/Remove types, JSON input parsing
internal/creds/         Credentials/S3 config file loading (INI), permission checks, per-site env resolution
internal/engine/        Grouping, dispatch, retry/backoff loop (the frozen retry-semantics contract)
internal/handler/       One Handler per protocol (17 total)
internal/integrity/     In-process sha256 checksum generation (pegasus-integrity-compatible output)
internal/executil/      argv-based command runner for callout handlers (never shell=True)
internal/s3uri/         s3://user@site/bucket/key URI parsing
internal/stats/         Per-transfer stats accumulation ($PEGASUS_MULTIPART_DIR)
```

### Handlers: native vs. callout

Native (no external tool, per the project's decision to internalize the "simple" protocols):
file, symlink, moveto, http(s), webdav, S3 (aws-sdk-go-v2).

Callout (argv-based via `internal/executil`, never a shell string):
ftp, gsiftp/sshftp (gfal-copy/globus-url-copy), gfal (root/srm/gsidavs), scp, gsiscp, irods, hpss (htar), gs (gcloud storage), osdf/pelican/stash, globus online (`pegasus-globus-online` — deliberately kept as a callout, not internalized), docker, singularity.

Several callout handlers (scp, gsiscp, gridftp, hpss) intentionally do **not** replicate transfer.py's multi-file batching into a single command invocation — see the `SIMPLIFICATION`/`REDUCED FIDELITY` doc comments on each for what was traded off and why (a throughput optimization, not a correctness requirement; every handler here processes one entry at a time). HPSS in particular needs validation against a real endpoint before production use.

### What was NOT ported

- The v1 line-based input format (planner only emits JSON).
- The Panorama real-time monitoring POST (`KICKSTART_MON_ENDPOINT_URL`).
- `PEGASUS_TRANSFER_ERROR_RATE` fault injection (tests use `httptest`/fakes instead).
- `pegasus-s3` as a standalone command — retired outright; its config format (`s3cfg`/`credentials.conf`) and `s3://user@site/bucket/key` URL scheme live on inside `internal/handler/s3.go` and `internal/s3uri/`.

## Testing Notes

- Table-driven unit tests cover JSON parsing quirks (planner's ignored `id`/`attributes` fields, `"recursive": "True"` as a JSON string), URL parsing, credential resolution, retry/grouping (via fake handlers), and the native handlers (file/symlink/moveto against a temp dir, http/webdav against `httptest` servers).
- No differential Go-vs-Python test harness (explicitly decided against — see decision record).
- S3 handler tests against a real/MinIO endpoint are not yet wired up in this repo; that was planned to run in CI (see decision record's acceptance bar).
- Deletion of the old `transfer.py`/`s3.py` and their console-script wrappers is gated on the `test/core/016-pegasus-transfer` e2e suite passing with this binary — do not delete them preemptively.
