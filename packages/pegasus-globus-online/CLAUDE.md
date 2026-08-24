# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`pegasus-globus-online` is a Go rewrite of the Python `pegasus-globus-online`/`pegasus-globus-online-init` tools (formerly `packages/pegasus-worker/src/Pegasus/cli/pegasus-globus-online*.py`). Unlike `pegasus-checkpoint`/`pegasus-integrity` (mechanical ports), this one is a genuine rebuild: the Python originals used the `globus-sdk` Python package; this rewrite talks directly to the [Globus Auth](https://docs.globus.org/api/auth/) and [Globus Transfer](https://docs.globus.org/api/transfer/overview/) REST APIs via `golang.org/x/oauth2` (the only third-party dependency — chosen specifically for its built-in PKCE support, which the OAuth2 authorization-code flow both tools use requires) plus stdlib `net/http`/`encoding/json`.

`pegasus-globus-online` is invoked by `pegasus-transfer`'s `GlobusOnlineHandler` (`packages/pegasus-transfer/internal/handler/globusonline.go`) as a callout, exactly as it invoked the Python tool before — the JSON spec-file contract (`--mkdir`/`--transfer`/`--remove`, `--file`) is unchanged and must stay byte-for-byte compatible. `pegasus-globus-online-init` is run manually by a user before a workflow starts, to populate `~/.pegasus/globus.conf`.

Ships as two static (`CGO_ENABLED=0`) binaries from one Go module — see the project's decision record for why `-init` shares a module with the main tool (they share the REST/OAuth client and config code) while each *other* worker tool got its own fully separate module.

## Build and Test

```bash
cd packages/pegasus-globus-online
go build ./...
go vet ./...
go test ./...
gofmt -l .          # should print nothing

# From the repo root:
make build-go        # builds all four Go worker tools
make clean-go
```

Requires network access to resolve `golang.org/x/oauth2` on a from-scratch build (same as `pegasus-transfer`'s AWS SDK dependency).

## Architecture

```
cmd/pegasus-globus-online/       CLI: --mkdir/--transfer/--remove dispatch, JSON spec parsing, SIGINT/SIGTERM cancellation
cmd/pegasus-globus-online-init/  CLI: -p/-e/-c/-d flag parsing (hand-rolled nargs="*"), PKCE flow, globus.conf write
internal/globusapi/              Globus Auth OAuth2/PKCE client + Transfer API v0.10 REST client
internal/globusconf/             ~/.pegasus/globus.conf [oauth] section INI read/write
```

`internal/globusapi` is intentionally not a general-purpose Globus SDK — it implements exactly the operations these two tools need: scope-string construction (`scopes.go`), the Native App authorization-code+PKCE flow and token refresh (`oauth.go`), and the Transfer API calls `mkdir`/`transfer`/`remove` require — submission-ID fetch, `operation_ls`/`operation_mkdir`, transfer/delete task submission, task-status polling, error-event listing, task cancellation (`transfer.go`).

### Known deviations from the Python original

- **Endpoint auto-activation dropped entirely.** The Python original called `endpoint_autoactivate()` before every operation — a Globus Connect Server v4 (legacy) concept that GCSv5+ (the current, exclusively-deployed architecture) doesn't use or need; v5 collections authorize via the OAuth consent scopes acquired at `-init` time instead. This is a deliberate behavior change, not an oversight: a legacy GCSv4 endpoint that genuinely required activation would now fail differently (a Transfer API auth/permission error, not an "endpoint requires manual activation" message) — flagged explicitly in the plan of record, not expected to affect anyone since GCSv4 is being phased out globally.
- **`pegasus-globus-online-init` keeps the manual copy/paste flow** (print an authorize URL, prompt for the pasted code) rather than switching to a local-redirect-listener flow — deliberate, since it needs no open ports and works identically over a pure SSH session.
- No differential Go-vs-Python test harness, matching the project's stated position for the other Go rewrites in this repo.

### Validation status

No live Globus endpoint or test credentials are available in the sandbox this was built in. What *was* validated:

- Full `go test ./...` coverage of everything mockable: OAuth2/PKCE authorize-URL construction, code exchange (including the "token arrives in `other_tokens`" case for dependent GCS scopes), refresh-token renewal, scope-string construction/ordering, and every Transfer API call (`mkdir`/`ls`/`transfer`/`delete`/task status/error events/cancel/the `wait_for_task` poll loop including its "ignore benign mkdir-exists races" logic) against `httptest` fakes.
- A **real, live round-trip** against production Globus Auth (`https://auth.globus.org`) using a manually-built binary and a bogus authorization code: the authorize URL (including the full dependent-scope syntax from `--collections`/`--endpoints`/`--domains`) was accepted and evaluated by the real server, failing only at the final `invalid_grant` step (expected, since no real login occurred) — strong evidence the request shape itself is correct.

**What still needs a real environment before this can be fully trusted in production**: an actual user login + token exchange (someone needs to run `pegasus-globus-online-init` for real, once, and confirm `globus.conf` is written correctly and usable), and a real end-to-end `mkdir`/`transfer`/`remove` against a live Globus Transfer endpoint (ideally via the existing `pegasus-transfer` callout path, i.e. a workflow using `go://` URLs). Neither `pegasus-globus-online` nor `pegasus-globus-online-init` has an e2e suite under `test/core/` (unlike `pegasus-checkpoint`'s `032-*` or `pegasus-integrity`'s `043-integrity-*`) — this is the tool's only acceptance gate, so treat this as higher-risk than the other three Go ports until someone runs it for real.
