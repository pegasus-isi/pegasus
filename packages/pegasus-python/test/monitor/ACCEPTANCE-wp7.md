# pegasus-monitor WP7 fast-follow acceptance evidence

This file is the reproducible acceptance index for the extended
`pegasus-monitor` release. The minimum native v1 release remains the immutable
`feature/pegasus-monitor-v1` baseline at `751ffc7d1`. WP7 and WP9b are a stacked
fast-follow and do not replace or supersede v1.

## Candidate composition

The locally tested code candidate is `88b23627c`, composed from:

- v1 baseline `751ffc7d1`;
- WP7 branch through upstream commit `45d4a583c`, including the replay memory
  fix at `d074c2ce0`, the acceptance-document formatting fix at `dd492c836`,
  and top-level checkpoint detection hardening at `45d4a583c`;
- WP8 fast-follow documentation, represented in WP9b by `a89ad5271`;
- WP9b integration and scale gates in `7104b802f`, with the final WP7
  hardening cherry-picked as `88b23627c`.

The final documentation-only evidence commit may advance the branch tip. Use
`git rev-parse HEAD` when publishing retained evidence. Do not submit or merge
the fast-follow until the Linux/FABRIC gates below are complete.

## Local release commands

Run from `packages/pegasus-python` with Python 3.10 or another supported
project interpreter:

```bash
PYTHONPATH=src:../pegasus-common/src \
python -m pytest \
  test/monitor/test_event_log.py \
  test/monitor/test_event_log_golden.py \
  test/monitor/test_replay.py \
  test/monitor/test_server.py \
  test/monitor/test_remote.py \
  test/monitor/test_cli_wp7.py \
  test/monitor/integration/test_wp7_fast_follow.py -q

PYTHONPATH=src:../pegasus-common/src \
python -m pytest test/monitor -q

PEGASUS_MONITOR_RUN_SCALE=full \
PYTHONPATH=src:../pegasus-common/src \
python -m pytest test/monitor/scale -q
```

For a retained WP7 JSON report, invoke `run_subprocess_gate()` from
`test/monitor/scale/wp7_runner.py` with `PROFILES["full"]` and a dedicated
workspace. The report records environment, fixture hash, cardinalities,
checkpoint sizes, timings, CPU, RSS, and all fixed budgets.

Generate the fast-follow manpage from the repository root with:

```bash
make -C doc/sphinx man \
  SPHINXBUILD="$PWD/packages/pegasus-python/.tox/docs/bin/sphinx-build"
test -s doc/sphinx/_build/man/pegasus-monitor.1
```

## Local results, 2026-08-18

| Gate                                               | Result                                                            |
| -------------------------------------------------- | ----------------------------------------------------------------- |
| Focused codec/replay/server/remote/CLI/integration | 147 passed                                                        |
| Complete monitor suite                             | 567 passed, 2 expected extended-profile skips                     |
| Consolidated full scale suite                      | 7 passed in 261.57 seconds                                        |
| Targeted Sphinx manpage build                      | `pegasus-monitor.1` generated; no unfiltered warning gate failure |
| Pre-commit non-Java hooks and whitespace           | passed; `git diff --check` clean                                  |

The all-files pre-commit run reached the Java formatter, but this macOS host's
Java 8 runtime rejects the formatter's required `--add-exports` option. No Java
files differ from v1. A modern-JDK GitLab or build host must complete that
unchanged all-files hook.

The repository-wide documentation build continues to report inherited
Sphinx/OpenAPI/table and LaTeX issues outside the three WP8-owned files. The
targeted manpage succeeds. `ant dist-doc` remains a submission gate unless the
Pegasus maintainers explicitly waive those unrelated failures.

## WP7 full-cardinality measurements

The retained local report used 100,000 jobs, 100,000 attempts, 1,000,000
Stampede transitions, and 1,000,000 synthetic stream records.

| Measurement                       |                Observed |             Limit | Result   |
| --------------------------------- | ----------------------: | ----------------: | -------- |
| Periodic checkpoint record        |       115,688,451 bytes | 268,435,456 bytes | pass     |
| Local/checkpoint process peak RSS |       575,733,760 bytes |             1 GiB | pass     |
| Replay process peak RSS           |       879,083,520 bytes |             1 GiB | pass     |
| Average CPU                       | 0.9988396 logical cores |         below 1.0 | pass     |
| Snapshot load                     |        3.457892 seconds |        diagnostic | recorded |
| Checkpoint serialization          |       30.669937 seconds |        diagnostic | recorded |
| Replay                            |       18.490366 seconds |        diagnostic | recorded |

The five-minute periodic checkpoint interval limits ordinary recovery loss
while avoiding a full 115.7 MB checkpoint on every refresh. Disk-guard resume,
stream replacement, and finalization bypass the periodic wait and emit the
required authoritative checkpoint immediately. Linux/FABRIC validation must
confirm that checkpoint I/O does not exceed the inherited workflow-impact
budget on the real submit filesystem.

Local retained evidence:

- directory, relative to the primary repository:
  `.local-evidence/pegasus-monitor/wp9b-local-20260818`;
- report: `wp7-scale-report.json`;
- report SHA-256:
  `a24dc19dd07cd7ca745f69ab6d87a25f1c51b679867c2680173553ffdf939ec1`;
- fixture SHA-256:
  `a74d6846278fea2f97e798630d60ac9d06f4fdbfc2d669f36c34fb8850fef88d`;
- environment: CPython 3.10.6, macOS 26.5.2 arm64, 12 logical CPUs.

## Acceptance matrix

| Acceptance claim                                                             | Reproducible evidence                                                                                                                             |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Canonical schema-v1 bytes and shared corpus                                  | `test_event_log.py`, `test_event_log_golden.py`, and `fixtures/event_log/schema-v1-golden.jsonl`                                                  |
| Initial, periodic, recovery, replacement, and final checkpoints              | `test_event_log.py`, `test_replay.py`, and `integration/test_wp7_fast_follow.py`                                                                  |
| Gaps, missing sequences, torn records, unsupported versions, and replacement | `test_replay.py`, `test_remote.py`, and the WP9b integration suite                                                                                |
| Replay does not invoke workflow sources                                      | `test_replay.py` source-import guards and CLI replay tests                                                                                        |
| Diagnostic round-trip, redaction, provenance, and no state mutation          | `test_event_log.py`, `test_cli_wp7.py`, and diagnostic golden cases                                                                               |
| Secure output and server lifecycle                                           | `test_event_log.py` and `test_server.py` cover `0600`, no-follow paths, singleton ownership, identity checks, and cleanup                         |
| SSH validation, bounded output, timeout cleanup, reconnect, and replacement  | `test_remote.py`; WP9b integration passes server-produced JSONL through the real remote reader with a bounded fake SSH transport                  |
| Local, replay, and remote final-state/statistics equivalence                 | `integration/test_wp7_fast_follow.py` checks authoritative snapshot signatures and rendered final statistics                                      |
| Server restart                                                               | the WP9b integration suite runs two foreground server lifecycles, verifies stream replacement, and then replays and remotely reads the new stream |
| Disk-guard recovery                                                          | the WP9b integration suite forces low-space pause and verifies one gap followed by an authoritative recovery checkpoint                           |
| 100k jobs/1M records, CPU, and RSS                                           | `scale/test_wp7_scale.py` and retained `wp7-scale-report.json`                                                                                    |
| Inherited latency, DB transaction, writer impact, CPU, and RSS budgets       | `scale/test_scale_gate.py`; included in the consolidated full scale command                                                                       |
| Manpage and user documentation                                               | WP8-owned Sphinx sources plus the targeted manpage command above                                                                                  |

## Linux/FABRIC evidence still required

Local acceptance is complete. Extended-release acceptance remains pending on a
real Pegasus/HTCondor submit node. The FABRIC run must use an isolated checkout
and virtual environment, copied workflow fixtures, and unique workflow names.
It must not replace system Pegasus, change HTCondor configuration, or mutate
the original fixtures.

Record all commands, versions, measurements, logs, and checksums for:

- real-process `--serve`, SSH `--remote`, and `--replay` final-state/statistics
  equivalence;
- monitor/server restart and stream replacement;
- safe disk-guard activation and recovery on copied data;
- SIGINT/SIGTERM termination with no remaining helper process and no effect on
  workflow execution;
- checkpoint size/cadence and the with/without-monitor workflow-impact budget;
- Linux CPU, RSS, DB transaction, latency, and scale results;
- exact candidate SHA, remote test directory, installed test tooling, and both
  local and remote worktree status.

Route any discovered defect to its owning work package, rerun the affected
gate, and update this document before submission. Do not start another feature
package or merge the fast-follow while this section is pending.
