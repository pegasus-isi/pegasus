---
name: run-tests
description: Trigger and wait for Pegasus CI pipelines on GitLab (scitech-gitlab.isi.edu) — run unit tests, or run e2e/workflow tests. Use when asked to run CI, run e2e tests, run the pipeline, trigger a build, run unit tests on a branch, or wait for CI to pass/fail. e2e (type=workflow) takes hours; run it minimally.
allowed-tools: Bash(glab pipeline run:*), Bash(glab api:*), Bash(glab auth status:*), Bash(jq:*), Bash(.claude/skills/run-tests/glab-pipeline.sh:*), Bash(./glab-pipeline.sh:*)
---

# Run Pegasus CI pipelines (unit / e2e)

The driver is **`.claude/skills/run-tests/glab-pipeline.sh`** (paths below are
relative to the repo root). It triggers a GitLab CI pipeline via `glab`, polls it
to completion, and on failure dumps the trace of every failed job.

```
.claude/skills/run-tests/glab-pipeline.sh [BRANCH] [TYPE]
```

- `BRANCH` — branch/ref to run on. Default: `master`.
- `TYPE` — `workflow` runs the **e2e/workflow** suite (sets pipeline variable
  `CI_PIPELINE_NAME=workflow`). **Anything else or omitted runs unit tests only.**

Exit code: `0` = pipeline succeeded, `1` = failed/canceled/skipped (traces of
failed jobs printed to stdout).

> **e2e (`TYPE=workflow`) takes HOURS. Run it minimally** — only when explicitly
> asked for e2e/workflow tests. Default to unit tests otherwise.

## Prerequisites

- `glab` authenticated to **scitech-gitlab.isi.edu** (the script targets repo
  `pegasus/pegasus`, project id 49 — resolved against the configured host, NOT
  gitlab.com). Check: `glab auth status`.
- `jq` on PATH.

```bash
# macOS
brew install glab jq
glab auth login --hostname scitech-gitlab.isi.edu   # only if not already logged in
```

## Run (agent path)

The script **blocks** while polling (`sleep 10` loop). Unit pipelines finish in
minutes; e2e runs for hours. Launch it in the **background** so you're notified
on exit instead of holding the turn.

Unit tests (default, fast):

```bash
.claude/skills/run-tests/glab-pipeline.sh master
```

e2e / workflow tests (slow — hours — only when explicitly requested):

```bash
.claude/skills/run-tests/glab-pipeline.sh master workflow
```

When launching via the Bash tool, set `run_in_background: true` for e2e (and for
unit runs you don't want to wait on). The script prints `Pipeline ID: <id>` early
— note it. On failure it prints a `FAILED JOB <id>` banner per job followed by
its full trace.

### Check a run without re-triggering

To inspect an already-running/finished pipeline (e.g. the one you just launched)
without firing a new one:

```bash
PID=4677   # from the script's "Pipeline ID:" line
glab api -R pegasus/pegasus projects/:id/pipelines/$PID | jq -r '.status'
# failed-job traces:
glab api -R pegasus/pegasus --paginate projects/:id/pipelines/$PID/jobs \
  | jq -r '.[] | select(.status=="failed") | .id' \
  | while read -r J; do echo "== job $J =="; glab api -R pegasus/pegasus /projects/:id/jobs/$J/trace; done
```

## Gotchas

- **e2e is hours-long.** Never trigger `workflow` to "just check" — it consumes
  shared CI runners for hours. Use unit (no `TYPE`) for routine checks; reserve
  `workflow` for explicit e2e requests.
- **Wrong GitLab host = silent confusion.** `glab` must be authed to
  `scitech-gitlab.isi.edu`. If it's only logged into gitlab.com, `-R pegasus/pegasus`
  resolves to the wrong place. Verify with `glab auth status`.
- **Mutating `glab api` is blocked by policy.** `allowed-tools` permits only
  `glab pipeline run`, `glab api` (reads), and `glab auth status`; so the skill
  can read pipelines/jobs/traces but never POST/PUT/DELETE. If you genuinely need
  a write, run it yourself outside the skill.
- **Traces only dump on `failed`.** On `canceled`/`cancelled`/`skipped` the
  script exits 1 with no traces — re-check the pipeline in the UI/API if so.
- **`TYPE` arg under `set -u`:** the script uses `set -euo pipefail`. The type
  arg is read as `${2:-}` so omitting it is safe (`./glab-pipeline.sh master`
  works, i.e. `.claude/skills/run-tests/glab-pipeline.sh master`). If you see
  `line 4: 2: unbound variable`, the script was reverted to
  `${2}` — change it back to `${2:-}`.

## Troubleshooting

- `unbound variable` at line 4 → script has `TYPE="${2}"`; must be `${2:-}`.
- `glab: command not found` / API 401 → `glab auth status`, re-login to
  scitech-gitlab.isi.edu.
- Pipeline ID comes back empty → the `glab pipeline run` output format changed;
  inspect raw output: `glab pipeline run -R pegasus/pegasus --branch master`.
