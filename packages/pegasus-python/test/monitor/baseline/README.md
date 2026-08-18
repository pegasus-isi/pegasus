# `workflow-monitor` baseline

## Pinned source

The porting baseline is canonical `main` at
`358d7ff22231882000e3741121a2ac92745831ef`. It is the merged complete monitor
implementation (748 tests recorded on that revision), without the unmerged
plugin-only surface.

The source project is `/Users/stealey/GitHub/pegasusai/workflow-monitor` and is
Apache-2.0.  The source manifest records exact provenance and the branch-line
comparison that selected this revision.

`fix/review-findings` at `b2f44e8` is tree-identical to `main` but is no longer
canonical. `monitord-plugin-adapter` at
`3ae923351023178cf7bcc2c52eaf1c3ab3946745` adds a plugin, `--condor-poll`, and
12 plugin tests (760 total); its ClassAd hardening is review input, not
baseline behavior. `monitord-plugin-live` at
`5e772ce1c3b22ded607ef62ecca063ba3b6823e3` is an older unmerged experiment
containing `stampede_stream.py` and `--source live`. Neither plugin
architecture is carried into Pegasus: decision 6 uses a `jobstate.log`
overlay and makes the plugin path a non-goal.

## Attribution and absorption policy

Both repositories are Apache-2.0; no third-party license conflict is known.
When a later owner ports or substantially adapts a mapped source module, that
owner applies the normal Pegasus Apache-2.0/USC-ISI file header and records any
retained notices required by Apache-2.0.  WP0 deliberately does not pre-header
future production files.  The source-to-destination map is in
`source-manifest.yaml`.

The source package incorrectly declares `build>=1.4.0` and `pip>=26.0.1` as
runtime dependencies.  They are build tooling, not monitor runtime behavior;
do not absorb either dependency.  Core runtime needs are already supplied by
Pegasus (`rich` and `PyYAML`); HTCondor remains optional.

## Behavioral record

The adapter checkout behavioral suite was executed on 2026-08-17:

```text
UV_CACHE_DIR=/private/tmp/pegasus-monitor-uv-cache uv run pytest -q
760 passed, 1 warning in 1.26s
```

The warning is only pytest's attempt to update the source checkout's
`.pytest_cache`, which is intentionally read-only.  A first `uv run` attempt
without `UV_CACHE_DIR` was blocked because the sandbox disallows writes under
`/Users/stealey/.cache/uv`; moving the cache to `/private/tmp` resolved it.

The exact canonical commit was archived to `/private/tmp` and executed with the
existing local workflow-monitor virtual environment while forcing the archived
`src/` directory onto `PYTHONPATH`:

```text
PYTHONPATH=/private/tmp/workflow-monitor-review.vrys8O/main/src \
  /Users/stealey/GitHub/pegasusai/workflow-monitor/.venv/bin/python -m pytest -q
748 passed in 1.28s
```

An earlier `uv run --with pytest` attempt could not download the uncached
`markdown-it-py==4.0.0` dependency because the sandbox has no DNS. Reusing the
already-installed local environment avoided network access and verified the
canonical suite directly. The 760 passing adapter run remains supporting—not
canonical—evidence.

## Golden behavior and deliberate divergences

The JSONL fixtures preserve the baseline event vocabulary (`workflow_start`,
`jobs_init`, `workflow_state`, `job_state`, HTCondor records, and
`workflow_end`) and are intentionally synthetic/minimal.  Baseline `--once`
means one DB snapshot plus one optional queue poll, then static terminal-safe
rendering and exit.  The corresponding v1 Pegasus behavior remains a
one-shot effective snapshot, but it is DB-authoritative and may show a
post-attachment `jobstate.log` overlay.

Do not freeze these source bugs as contracts:

* the source event logger scans an entire existing JSONL file on resume;
* its event identity is timestamp-centric with a local dedup set, rather than
  a durable reconciliation identity;
* the optional diagnostics sidecar is not replayed; and
* a held → released → held job in one stall window is not diagnosed again.

WP7 replaces the stream with versioned DB-confirmed checkpoints and gap
recovery.  WP6a emits structured diagnostics only; it does not retain the
sidecar.  These are intentional plan divergences, not regressions.

Further deliberate divergences are subprocess-only Condor observation, exact
Stampede DB row transition identities, last-good DB snapshots on source
failure, and one canonical diagnostic stream (WP6a result codes, then WP7
`diagnostic_result` records).

## Fixtures

`fixtures/baseline/fixtures.yaml` is the authoritative inventory.  Every
record is synthetic, contains no real hostname, user, path, credential, or
workflow UUID, and is Apache-2.0 under the Pegasus project.  The files are
small JSONL goldens for parser/replay/normalization tests—not SQLite schema or
live scheduler fixtures, which belong to the owning later WPs.
