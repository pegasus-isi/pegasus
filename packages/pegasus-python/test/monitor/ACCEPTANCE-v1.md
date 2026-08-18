# pegasus-monitor v1 acceptance evidence

This file is the reproducible acceptance index for the minimum native
`pegasus-monitor` release. Large scale inputs and host-specific JSON evidence
are generated during the run and are not committed.

## Release command

Run the fixed release profile from `packages/pegasus-python`:

```bash
PEGASUS_MONITOR_RUN_SCALE=full \
PYTHONPATH=src:../pegasus-common/src \
python -m pytest test/monitor/scale -q
```

For a retained machine-readable report, run:

```bash
PEGASUS_MONITOR_RUN_SCALE=full \
PYTHONPATH=src:../pegasus-common/src \
python test/monitor/scale/runner.py \
  --mode full --output /tmp/pegasus-monitor-scale.json
```

After a failure, repeat only append-to-display stages against that generated
fixture:

```bash
python test/monitor/scale/runner.py --mode full \
  --workspace /tmp/pegasus-monitor-scale-fixture \
  --fixture-sha256 <hash-from-report> --live-only
```

`PEGASUS_MONITOR_RUN_SCALE=default` runs a 10k-job intermediate profile.
Without the variable, normal pytest runs execute a generated 250-job smoke and
skip the extended profile.

## Scale and performance evidence matrix

| Acceptance claim | Reproducible evidence |
| --- | --- |
| 100k jobs and 100k attempts | `observed.database_jobs` and `observed.database_attempts` equal 100,000 in full mode |
| 1M authoritative transitions | The runner executes `SELECT COUNT(*) FROM jobstate`; `observed.actual_database_transitions` must equal 1,000,000 |
| Existing 1M-line tail plus live burst | The runner counts source-file newlines and requires `observed.actual_tail_lines_before_burst` to equal 1,000,000 before `LiveEventTail` attaches, then appends and observes 1,000 lines at 1,000 lines/second |
| Post-boundary display latency | Each observed burst batch is reconciled, published, and rendered through the production `Console` + `Live(auto_refresh=False)` path with a guarded manual refresh; `live_burst.display_latency_seconds.p95` must be below 1 second. `display_path` and `display_auto_refresh` prove the selected path, while per-stage poll, ingest, publication, and display samples localize failures |
| Forty Stampede samples | `stampede_refresh_seconds` retains all 40 end-to-end refresh samples; `stampede_transaction_seconds` separately retains the longest exact SQLite read-transaction interval from each refresh. Both include raw samples plus min, median, p95, and max |
| DB transaction budget | Exact `stampede_transaction_seconds` p95 must be below 500 ms and max below 2 seconds; end-to-end refresh time remains diagnostic evidence and is not substituted for transaction-open duration |
| Monitor CPU and memory | Fixture generation occurs in the parent; the separately spawned monitor worker records process CPU, average logical cores, and peak RSS. Average must be below one core and peak below 1 GiB |
| Workflow writer impact | WAL and rollback-journal modes each run one warmup and nine phased, paired baseline/with-monitor trials at the production two-second DB cadence. Each full-mode median makespan impact must be below 2% |
| Publication/display/stats/diagnostics over 100k jobs | Full mode runs three samples of each probe over the same 100k-job snapshot, exercises the same bounded-row production Live refresh, and records a descending hotspot list |
| Reproducibility | JSON records Python/platform/CPU/SQLite environment, profile, validated fixture SHA-256, raw samples, quantiles, and source call counts |

The writer subprocess only inserts into the fixture-only `writer_probe` table.
The monitor uses the production read-only `StampedeReader`; tests never write
through monitor code or invoke the monitord plugin system.
The worker validates the manifest plus the actual database and tail SHA-256
before copying both inputs to disposable runtime files. Live and writer phases
therefore cannot invalidate the reusable base fixture or its recorded hash.

Each journal-mode record also includes a separately labelled
`tight_loop_stress` collision trial. It intentionally refreshes without the
production cadence and is diagnostic evidence, not the release makespan result.
Production trials pace fixture writes across a four-second workflow window in
full mode, ensuring that each trial overlaps the two-second monitor cadence.

## Fixed budgets

The JSON `budgets` object is authoritative. Full acceptance fails when any of
these conditions is false:

- live display latency p95 is below 1 second;
- monitor CPU averages below one logical core;
- monitor peak RSS is below 1 GiB;
- exact Stampede read-transaction duration has p95 below 500 ms and no sample
  above 2 seconds;
- median writer makespan impact is below 2% independently for WAL and DELETE
  journal modes.

Writer-impact thresholds are measured but not asserted in fast/default modes,
where short trials are dominated by host scheduling noise. All other budgets
remain active in every profile.

## Integration and fault evidence

The companion WP9a integration suite owns DB lock/replacement, tail gap and
replacement, broken/hung Condor, clean termination, helper-process cleanup,
read-only filesystem, and source-cadence evidence. A v1 handoff is complete
only when that suite and this full scale gate both pass on the same candidate
commit, and their commands plus retained JSON/log locations are recorded in the
release issue.
