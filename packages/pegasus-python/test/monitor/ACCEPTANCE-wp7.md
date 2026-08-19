# pegasus-monitor WP7 fast-follow acceptance evidence

This file is the reproducible acceptance index for the extended
`pegasus-monitor` release. The minimum native v1 release remains the immutable
`feature/pegasus-monitor-v1` baseline at `751ffc7d1`. WP7 and WP9b are a stacked
fast-follow and do not replace or supersede v1.

## Candidate composition

The final code candidate is
`71723fc412d8abbbc31b2e3cb605ec2668beccbb`, composed from:

- v1 baseline `751ffc7d1`;
- WP7 branch through upstream commit `45d4a583c`, including the replay memory
  fix at `d074c2ce0`, the acceptance-document formatting fix at `dd492c836`,
  and top-level checkpoint detection hardening at `45d4a583c`;
- WP8 fast-follow documentation, represented in WP9b by `a89ad5271`;
- WP9b integration and scale gates in `7104b802f`, with the final WP7
  hardening cherry-picked as `88b23627c`;
- acceptance evidence recorded at `60f2d5a71`; and
- detached-server startup timeout hardening at `71723fc41`, which raises the
  default startup deadline from 10 to 60 seconds after the FABRIC host measured
  32 seconds to readiness.

The final documentation-only evidence commit may advance the branch tip. Use
`git rev-parse HEAD` when publishing retained evidence. Functional local and
FABRIC acceptance is complete. Do not submit or merge the fast-follow until an
actual GitLab pipeline passes and the Pegasus maintainers explicitly waive the
inherited repository-wide `ant dist-doc` failures described below.

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

PYTHONPATH=src:../pegasus-common/src \
python -m pytest test/monitor/test_server.py -q

PYTHONPATH=src:../pegasus-common/src \
python -m pytest \
  test/monitor/test_server.py \
  test/monitor/test_cli_wp7.py \
  test/monitor/integration/test_wp7_fast_follow.py -q
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

## Local and FABRIC results, 2026-08-18 through 2026-08-19

| Candidate   | Environment | Gate                                               | Result                                 |
| ----------- | ----------- | -------------------------------------------------- | -------------------------------------- |
| `60f2d5a71` | FABRIC      | Focused codec/replay/server/remote/CLI/integration | 147 passed                             |
| `60f2d5a71` | FABRIC      | Complete monitor suite                             | 567 passed, 2 expected skips           |
| `60f2d5a71` | FABRIC      | Consolidated full scale suite                      | 7 passed in 359.83 seconds             |
| `71723fc41` | local       | Server startup-timeout regression suite            | 48 passed                              |
| `71723fc41` | local       | Affected server/CLI/integration suite              | 68 passed                              |
| `71723fc41` | local       | Complete monitor suite                             | 568 passed, 2 expected skips           |
| `71723fc41` | FABRIC      | Affected server/CLI/integration suite              | 68 passed                              |
| `71723fc41` | FABRIC      | Focused disk-guard recovery                        | 1 passed                               |
| `60f2d5a71` | local       | Targeted Sphinx manpage build                      | passed; unchanged by `71723fc41`       |
| `71723fc41` | local       | Exact GitLab lint command in CI-equivalent image   | passed with Python 3.14 and OpenJDK 25 |

The exact GitLab lint environment was reproduced locally in `python:3.14` with
OpenJDK 25, and
`pre-commit run --show-diff-on-failure --color=always --all-files` passed,
including the Java formatter. The actual GitLab pipeline remains unexecuted:
the available `glab` configuration has no authentication entry for
`scitech-gitlab.isi.edu`. A real project pipeline remains a release gate.

The repository-wide documentation build continues to report inherited
Sphinx/OpenAPI/table and LaTeX issues outside the three WP8-owned files. The
targeted manpage succeeds. `ant dist-doc` therefore requires an explicit
Pegasus maintainer waiver before submission.

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
required authoritative checkpoint immediately. FABRIC validation confirmed
that the full-scale checkpoint and inherited workflow-impact budgets pass.

Local retained evidence:

- directory, relative to the primary repository:
  `.local-evidence/pegasus-monitor/wp9b-local-20260818`;
- report: `wp7-scale-report.json`;
- report SHA-256:
  `a24dc19dd07cd7ca745f69ab6d87a25f1c51b679867c2680173553ffdf939ec1`;
- fixture SHA-256:
  `a74d6846278fea2f97e798630d60ac9d06f4fdbfc2d669f36c34fb8850fef88d`;
- environment: CPython 3.10.6, macOS 26.5.2 arm64, 12 logical CPUs.

FABRIC retained measurements used 100,000 jobs, 100,000 attempts, 1,000,000
Stampede transitions, and 1,000,000 synthetic stream records:

| Measurement                       |               Observed |             Limit | Result |
| --------------------------------- | ---------------------: | ----------------: | ------ |
| Periodic checkpoint record        |      115,688,446 bytes | 268,435,456 bytes | pass   |
| Local/checkpoint process peak RSS |      515,375,104 bytes |             1 GiB | pass   |
| Replay process peak RSS           |      792,805,376 bytes |             1 GiB | pass   |
| Average CPU                       | 0.999386 logical cores |         below 1.0 | pass   |
| Live display p95                  |       0.879914 seconds |        1.0 second | pass   |
| Stampede transaction maximum      |       1.789540 seconds |       2.0 seconds | pass   |
| WAL writer median impact          |              0.000598% |              2.0% | pass   |
| Rollback-journal median impact    |             -0.000149% |              2.0% | pass   |

The real Pegasus/HTCondor comparison also completed successfully: the baseline
workflow took 192 seconds from `DAGMAN_STARTED` to `DAGMAN_FINISHED`, while the
monitored workflow took 187 seconds, an observed impact of -2.604% for this
single pair.

## Acceptance matrix

| Acceptance claim                                                             | Reproducible evidence                                                                                                                       |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Canonical schema-v1 bytes and shared corpus                                  | `test_event_log.py`, `test_event_log_golden.py`, and `fixtures/event_log/schema-v1-golden.jsonl`                                            |
| Initial, periodic, recovery, replacement, and final checkpoints              | `test_event_log.py`, `test_replay.py`, and `integration/test_wp7_fast_follow.py`                                                            |
| Gaps, missing sequences, torn records, unsupported versions, and replacement | `test_replay.py`, `test_remote.py`, and the WP9b integration suite                                                                          |
| Replay does not invoke workflow sources                                      | `test_replay.py` source-import guards and CLI replay tests                                                                                  |
| Diagnostic round-trip, redaction, provenance, and no state mutation          | `test_event_log.py`, `test_cli_wp7.py`, and diagnostic golden cases                                                                         |
| Secure output and server lifecycle                                           | `test_event_log.py` and `test_server.py` cover `0600`, no-follow paths, singleton ownership, identity checks, and cleanup                   |
| SSH validation, bounded output, timeout cleanup, reconnect, and replacement  | unit/integration coverage plus a real SSH `--remote --once`; live remote SIGINT left no monitor or SSH helper process                       |
| Local, replay, and remote final-state/statistics equivalence                 | real FABRIC views agree on SUCCESS 11/11 and the final statistics recorded below                                                            |
| Server restart                                                               | detached serve and stop passed; two generations used distinct stream IDs and the second stream ended with an authoritative final checkpoint |
| Disk-guard recovery                                                          | integration coverage plus the focused FABRIC recovery test passed                                                                           |
| 100k jobs/1M records, CPU, and RSS                                           | `scale/test_wp7_scale.py` and retained `wp7-scale-report.json`                                                                              |
| Inherited latency, DB transaction, writer impact, CPU, and RSS budgets       | `scale/test_scale_gate.py`; included in the consolidated full scale command                                                                 |
| Slow detached-server bootstrap                                               | `71723fc41`; local 48/68/full-suite passes and the FABRIC affected suite passed 68 tests                                                    |
| Manpage and user documentation                                               | targeted manpage passes; inherited repository-wide `ant dist-doc` failures require a maintainer waiver                                      |

## Linux/FABRIC results, 2026-08-19

The full FABRIC run used `60f2d5a71`. It exposed a real 32-second detached
server readiness delay, which exceeded the former 10-second startup deadline.
Commit `71723fc41` raises that deadline to 60 seconds and adds regression
coverage. Because that is the only code change, the affected 68-test suite and
the focused disk-guard gate were rerun on FABRIC at `71723fc41`; the full local
monitor suite was also rerun.

Real-process acceptance passed:

- detached `--serve`, stop, and restart, including two stream generations;
- SSH `--remote --once` and live remote SIGINT cleanup;
- stopping the monitor without affecting the HTCondor workflow;
- focused disk-guard recovery;
- an authoritative `reason: "final"` checkpoint; and
- local, replay, and remote final views reporting `SUCCESS`, 11/11 terminal,
  four compute jobs, 00:04:41 wall time, 00:04:21 compute time, 0.93
  parallelism, 3.5 MiB peak RSS, and authoritative final status.

The first and second restart streams have distinct IDs:
`3aff0d52-56a7-4d8f-8b78-5c4043204d11` and
`57ab0dce-0bbc-457c-b236-620edd49cec0`. The second ends at sequence 12 with
`reason: "final"`.

Environment and retained evidence:

- Ubuntu 22.04, Linux 5.15 x86_64, Python 3.10.12, SQLite 3.37.2, system
  Pegasus 5.1.2, and HTCondor 25.12.2;
- candidate checkout:
  `/home/ubuntu/pegasus-monitor-wp9b-test-60f2d5a71`;
- isolated virtual environment:
  `/home/ubuntu/pegasus-monitor-wp9b-venv-60f2d5a71`;
- remote evidence:
  `/home/ubuntu/pegasus-monitor-wp9b-evidence-60f2d5a71`;
- external copied fixtures:
  `/home/ubuntu/pegasus-monitor-wp9b-workflows-71723fc41` and
  `/home/ubuntu/pegasus-monitor-wp9b-workflows-from-checkout-71723fc41`;
- local retained copy:
  `.local-evidence/pegasus-monitor/wp9b-release-20260819/fabric-71723fc41/pegasus-monitor-wp9b-evidence-60f2d5a71`;
- final real SSH transcript:
  `.local-evidence/pegasus-monitor/wp9b-release-20260819/final-remote-once.typescript`,
  SHA-256
  `bccfe9215566d52b4f7d90063693b0dc6804c336cf7e8d68126cf4e010c8e684`;
- final non-workspace manifest: `SHA256SUMS-final-71723fc41`, SHA-256
  `d5ceb25ce1bf3a4e1fe48563613147360a0938b6f84356eef78305ca24e68452`;
- retained targeted and repository-wide documentation logs:
  `.local-evidence/pegasus-monitor/wp9b-release-20260819/build-gates`;
- installed test tooling includes pytest 9.1.1 and pytest-cov 7.1.0; the
  candidate packages were installed in editable mode in the isolated venv;
- the local WP9b worktree and remote checkout have no tracked or untracked file
  changes. The remote checkout's bundle-backed `origin` remains one commit
  behind, but its checked-out HEAD is the verified `71723fc41` candidate.

## Release status

Functional local and FABRIC acceptance is complete for `71723fc41`. An
evidence-only documentation commit may advance and be pushed on
`feature/pegasus-monitor-wp9b`. Submission or merge remains blocked pending:

1. an actual GitLab pipeline on `scitech-gitlab.isi.edu`; and
1. an explicit Pegasus maintainer waiver for the inherited repository-wide
   `ant dist-doc` failures.

WP9b remains a stacked fast-follow extended release. It does not replace or
supersede the immutable v1 baseline.
