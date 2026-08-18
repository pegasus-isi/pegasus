# pegasus-monitor — execution-ready implementation plan

Date: 2026-08-17

Status: canonical plan, approved for implementation fanout — owner and
implementation decisions 1-17 recorded 2026-08-17 (section 20)

## 1. Outcome

Pegasus will ship a native command:

```bash
pegasus-monitor <submit_dir>
```

It will provide the useful behavior of the current `workflow-monitor` project
from an installed Pegasus distribution: a live TUI, one-shot status,
diagnostics, why-idle analysis, optional JSONL recording/server mode, replay,
and remote viewing.

The command is an independent, observational process. It may attach after a
workflow has started, requires no special planning property, and does not run
inside `pegasus-monitord`.

The canonical data flow is:

```text
                       durable history and metadata
                    +--------------------------------+
                    | read-only Stampede DB          |
                    | authoritative after reconcile  |
                    +---------------+----------------+
                                    |
                                    v
jobstate.log --> live transition --> Reconciler --> effective snapshot --> UI/output
  tail only        overlay              ^
                                         |
                         optional read-only HTCondor enrichment
```

The central design decision is:

> `jobstate.log` supplies low-latency transitions observed after attachment;
> the read-only Stampede database supplies the complete durable state and
> pre-attachment history, and remains authoritative once it catches up.

This uses standard output already produced by `pegasus-monitord`; it does not
use the monitord plugin host, a `wfevents` plugin, a socket sink, or any new code
in the monitord event-dispatch path. This supersedes the 2026-08-07 working
decision to feed the monitor from a plugin-written `wfevents.jsonl` tail; the
reversal is confirmed as owner decision 6 in section 20 (recorded 2026-08-17).

The inventory in
`/Users/stealey/GitHub/pegasusai/workflow-monitor/DATA_SOURCES.md` is the
functional baseline. The workflow-monitor repository carries several unmerged
branches; WP0 pins the exact baseline branch/commit. The safety,
source-authority, and reconciliation rules in this plan take precedence where
the existing implementation differs.

## 2. Architectural decisions and invariants

1. **Stampede is authoritative.** Durable workflow/job state, attempts, exact
   transition identity, task/invocation metadata, stdout/stderr locations,
   site, timing, and resource measurements come from read-only Stampede queries.
2. **The live file is an overlay.** `jobstate.log` may temporarily advance the
   displayed state ahead of Stampede. It never replaces historical DB data and
   never becomes the durable source of truth.
3. **Attachment has a precise boundary.** Bytes appended after the monitor arms
   the tail are live overlay candidates. Data at or before that boundary is
   obtained from Stampede, even if Stampede needs another polling interval to
   expose it.
4. **No monitord plugin dependency.** No plugin entry point, plugin property,
   plugin queue, or plugin lifecycle is required for any mode.
5. **No workflow control.** The monitor never submits, holds, releases,
   removes, edits, reprioritizes, or otherwise mutates a workflow or job.
6. **HTCondor is optional enrichment.** Queue, history, pool, priority, and
   negotiator information may explain current scheduler behavior, but does not
   define Pegasus workflow progress.
7. **Failures preserve known-good state.** An unavailable source produces an
   explicit health state and last-good age; it does not clear the display or
   synthesize an empty workflow.
8. **Observation must not impede execution.** Reads are bounded, DB
   transactions are short, HTCondor calls are time-bounded subprocesses, and no
   monitor activity occurs on the monitord/DAGMan execution thread.
9. **Exact workflow scope is the default.** Every DB and HTCondor workflow query
   is constrained to the selected `wf_uuid`. Version 1 has no subworkflow
   aggregation mode; tree aggregation is a follow-on feature.

## 3. Why the hybrid flow is feasible

The existing Pegasus implementation already provides the required ordering:

- `jobstate.log` is opened in unbuffered binary mode in
  `Pegasus/monitoring/workflow.py`.
- A complete job transition line is written to `jobstate.log` before the
  corresponding Stampede event is dispatched.
- DAGMan start/finish lines are also written before workflow-state dispatch.
- The standard file contains timestamp, executable job ID, state, status or
  scheduler ID, site, walltime value, and `job_submit_seq`.
- Stampede preserves the stronger durable row identity
  `(job_instance_id, state, timestamp, jobstate_submit_seq)` and contains data
  that is not present in `jobstate.log`.

The file therefore closes the normal DB-lag window without duplicating the
complex `dagman.out` parser. The monitor must not parse `dagman.out` directly.

This is an eventually consistent overlay, not lossless change-data capture. A
transition written just before the EOF attachment boundary can still be absent
from the first DB snapshot because monitord may batch DB writes. Periodic DB
reconciliation is the correctness mechanism for that cut race.

Important limitation: the seven-field `jobstate.log` job line does not contain
Stampede's `jobstate_submit_seq`. File offsets are provisional source identities;
reconciliation must map ordered file events to DB transitions. The file must not
be presented as an exact replacement for the DB event ledger.

## 4. Source contracts and precedence

| Source | Role | Authority | Failure behavior |
|---|---|---|---|
| `braindump.yml` | Locate run, UUIDs, DAG, submit paths, properties, DB | Identity/configuration | Target is invalid if identity cannot be resolved |
| Stampede DB | Durable history, roster, attempts, state and metadata | Authoritative | Use last-good snapshot; show `DB STALE` |
| `jobstate.log` | Post-attachment workflow/job transitions | Provisional overlay | Continue DB-only; show `TAIL?` or `TAIL GAP` |
| `condor_q` | Current scheduler status and hold/resource details | Optional enrichment | Retain last-good; show `SCHEDD?`; back off |
| `condor_history` | Completed scheduler metrics | Optional enrichment | Retain prior bounded cache |
| `condor_status` | Pool capacity/activity | Optional enrichment | Retain briefly, then show `POOL?` |
| priority/negotiator | Why-idle evidence | On-demand enrichment | Return partial diagnosis explicitly |
| kickstart output | Failure details and invocation evidence | On-demand diagnostic | Fall back to DB exit/hold evidence |

Source precedence for effective job state is:

1. Start with the latest authoritative Stampede job instance and transition.
2. Apply unreconciled `jobstate.log` events for that exact job and
   `job_submit_seq` in file order.
3. Attach HTCondor fields without changing the Pegasus state.
4. When Stampede contains the same or a later transition, retire the overlay.

Statistics operate on one effective state per Pegasus job. They must not count a
DB job and its live overlay as two jobs.

## 5. Attachment and steady-state protocol

### 5.1 Locate the workflow

`WorkflowLocator` accepts a submit directory, `braindump.yml`, or a workflow
base directory. It must:

- deserialize with `Pegasus.braindump.load` (it accepts an open file object,
  not a path; mirror the `utils.slurp_braindb` usage);
- preserve the braindump-recorded `submit_dir`/`basedir` verbatim and compute
  any local remapping in the monitor itself — the Pegasus `Braindump` model has
  no remap fields, and `rundir` is only the run-number basename; follow
  workflow-monitor's `--remap-submit-dir {auto,always,never}` semantics;
- resolve `wf_uuid`, `root_wf_uuid`, DAG name, properties, and `jobstate.log`;
- derive the workflow DB location — it is not stored in braindump: use
  `pegasus.catalog.workflow.url`, then `pegasus.monitord.output`, else the
  default `<submit_dir>/<dag stem>.stampede.db` of the top-level workflow
  (walk parent directories until `root_wf_uuid == wf_uuid`);
- select the latest run deterministically when given a workflow base directory:
  consider valid top-level braindumps (`wf_uuid == root_wf_uuid`), prefer the
  highest numeric `rundir` matching `runNNNN`, use the braindump planning
  timestamp only to break a numeric tie, and reject any remaining ambiguity. A
  sole valid nonstandard candidate is accepted; never use recursive
  lexicographic "last path" selection;
- find the top-level DB for a subworkflow while retaining the selected
  subworkflow UUID as the query scope;
- reject an ambiguous or mismatched workflow identity rather than guessing.

The default live-file path is the locally mapped submit directory plus the
braindump `jsd` value; the planner writes `jsd` as the literal name
`jobstate.log`, and monitord does not consult it when writing. Monitord's
`--job` and `--output-dir` overrides are never recorded in braindump (with
`--output-dir` the real file is `<output_dir>/<wf_uuid>-jobstate.log`), so the
CLI also provides `--jobstate-path PATH`. If the default path is absent and no
override is given, degrade to DB-only rather than searching arbitrary files.

### 5.2 Arm the live tail before DB bootstrap

For live mode, `LiveEventTail` is armed before the first DB snapshot:

1. Open the selected workflow's `jobstate.log` read-only if it exists.
2. Use `fstat()` on the open descriptor to record a generation identity and the
   current EOF byte offset.
3. Read a bounded window of at most one maximum line immediately before EOF. If
   EOF follows a newline, use EOF as the attachment boundary. If EOF is in the
   middle of a line, move the boundary to the preceding newline and seed the
   partial-line buffer with the straddling prefix. If no preceding newline is
   found within the maximum, report an overlong straddling line, discard that
   prefix, use EOF as the boundary, and rely on DB catchup.
4. Begin buffering complete lines appended after the boundary and preserve the
   seeded/incomplete trailing line until the remaining bytes arrive.
5. If the file is absent, enter `TAIL WAITING` and continue with DB bootstrap.

The generation identity includes `(st_dev, st_ino)` plus an internal generation
counter. The tailer also retains a small hash/byte anchor immediately before the
cursor so an in-place truncate-and-regrow that exceeds the old file size is not
mistaken for a normal append.

No historical file scan is required on normal attachment. The bounded backward
read exists only to avoid splitting one in-progress line. Events before the
aligned boundary belong to the DB bootstrap path. Events at or after the
boundary belong to the overlay path and are deduplicated during initial merge.

### 5.3 Bootstrap Stampede

After the tail is armed, `StampedeReader` loads one consistent snapshot in a
short read transaction:

- selected workflow and workflow-state history;
- one `JobRecord` per Pegasus `job` row;
- all attempts needed to identify the current attempt;
- latest state plus its exact DB identity;
- task count/transformation summary without duplicating clustered jobs;
- site, stdout/stderr paths, timings, exit status, and `maxrss` excluding
  pre/post-script pseudo-invocations;
- a bounded recent transition window used for initial reconciliation.

The tailer continues buffering while this transaction runs. The transaction is
closed before rendering, waiting, or invoking HTCondor.

"Complete" here means complete for the current rendered workflow/job state and
its required metadata. The monitor must not delay first render by eagerly
loading an unbounded transition ledger. Modes that need full historical events
page them from Stampede after the initial snapshot while continuing live
observation.

### 5.4 Initial merge and publication

The coordinator then:

1. installs the DB snapshot as the authoritative base;
2. maps buffered tail events to DB jobs by
   `(wf_uuid, exec_job_id, job_submit_seq)`;
3. suppresses any buffered event already reflected in the DB transition suffix;
4. applies the remaining events as a provisional ordered overlay;
5. creates a bounded provisional job only if a live event precedes the DB job
   row, then replaces it when the DB row appears;
6. publishes the first effective snapshot.

The display distinguishes `DB` and `LIVE/PENDING` provenance. A terminal state
seen only in the overlay may be displayed immediately, but final workflow
statistics and clean completion require Stampede confirmation. A
`DAGMAN_FINISHED` or `MONITORD_FINISHED` line triggers a final DB reconciliation;
it does not terminate the monitor by itself. If the DB remains permanently
unavailable, show `DB FAILED / LIVE UNCONFIRMED` and do not claim an
authoritative final result.

### 5.5 Steady state

WP4's coordinator owns all clocks, task creation, cadence, and cancellation for
three independent source loops coordinated through immutable snapshots or
message passing. WP3 exposes a bounded nonblocking tail `poll()` API and creates
no thread or task. WP5 exposes bounded HTCondor query/provider APIs and owns
query caches and backoff state, but creates no scheduler or background task.

| Loop | Default | Responsibility |
|---|---:|---|
| File tail | 250 ms, adaptive to 1 s idle | Read bounded appended bytes and emit parsed events |
| Stampede refresh | 2 s | Refresh authoritative state and reconcile overlays |
| HTCondor observer | source-specific | Refresh optional enrichment without blocking DB/UI |

Only the coordinator mutates in-memory monitor state. Rendering consumes a
snapshot and never reads files, queries a database, or waits on a subprocess.

## 6. Live event format and parser contract

### 6.1 Job lines

The existing line format is:

```text
<epoch_seconds> <job_id> <state> <value> <site> <walltime> <job_submit_seq>
```

Parse from the left with a strict seven-field contract. The current producer
emits job IDs and values without spaces. Reject malformed lines individually;
do not terminate the tail loop.

`value` is overloaded by the producer with precedence exit/status value, then
scheduler ID, then `-` (the submit-directory documentation claiming it is
always the Condor ID is wrong). The timestamp is epoch seconds rendered with
`:.0f` — a rounded float, not ISO time. `walltime` is the requested maximum
from the job's `globusrsl`, in seconds, and is normally `-`. `job_submit_seq`
is a workflow-global monotonic counter persisted across monitord restarts —
unique per attempt, but not contiguous per job. Preserve the raw values and
derive typed fields only through state-aware parsing.

Timestamp reconciliation uses producer-equivalent formatting, not truncation:
format the authoritative DB timestamp with the same `:.0f` rule and compare
that canonical integer string with the file field. Retain the full six-decimal
DB timestamp for authoritative ordering and output. WP1 freezes this conversion
and tests values on both sides of half-second boundaries before WP4 begins.

`TailJobEvent` contains at least:

- `workflow_uuid` from the selected tail context;
- `source_generation`, `start_offset`, and `end_offset`;
- `observed_at_monotonic`;
- `event_timestamp`;
- `exec_job_id`, `state`, and `job_submit_seq`;
- raw `value`, `site`, and `walltime` values plus state-aware typed projections;
- normalized state used for DB matching;
- the original bounded line for diagnostics.

The provisional identity is `(source_generation, start_offset)`. It is stable
only within one observed file generation.

Normalize known representational differences for matching, including
`JOB_HELD_REASON` to Stampede's `JOB_HELD`. Preserve the original state for
diagnostics.

### 6.2 Internal lines

Parse these separately from job lines:

```text
<timestamp> INTERNAL *** MONITORD_STARTED ***
<timestamp> INTERNAL *** MONITORD_FINISHED <status> ***
<timestamp> INTERNAL *** DAGMAN_STARTED <cluster> ***
<timestamp> INTERNAL *** DAGMAN_FINISHED <status> ***
```

`DAGMAN_STARTED` and `DAGMAN_FINISHED` are provisional workflow transitions and
reconcile to Stampede workflow state. Of the file markers, only
`DAGMAN_FINISHED` carries a provisional workflow outcome. `MONITORD_STARTED` and
`MONITORD_FINISHED` describe source health/lifecycle; they do not by themselves
define workflow success or failure.

Producer formatting details the parser must tolerate: MONITORD lines format
their integers with `:d` while DAGMAN lines use `:.0f`, and the DAGMan cluster
and status fields carry no format spec, so a literal `None` can appear when
unset. `DAGMAN_FINISHED` also has a second write site: on two consecutive START
events monitord synthesizes a `DAGMAN_FINISHED` with an unknown-failure status
before the new start (PM-723).

### 6.3 Bounded reading

- Read appended data in bounded chunks.
- Enforce a maximum line size and report/drop an overlong line.
- Bound pending lines and bytes. On overflow, mark `TAIL GAP`, force an
  immediate DB refresh, and resume at the current EOF after establishing a new
  generation boundary.
- Do not persist raw arguments or sensitive content beyond what the existing
  seven-field file contains.
- Never write, lock, truncate, rotate, or rename `jobstate.log`.
- Do not classify elapsed time without a new line as tail failure. A workflow
  may be legitimately idle; tail health is based on readability, generation
  continuity, parse/gap state, and corroborating DB/monitord evidence.

## 7. Reconciliation contract

### 7.1 Identities

Stampede transition row identity:

```text
(job_instance_id, state, timestamp, jobstate_submit_seq)
```

Tail transition identity:

```text
(source_generation, byte_offset)
```

Semantic match key:

```text
(exec_job_id, job_submit_seq, integer_timestamp, normalized_state)
```

The semantic key is matched as an ordered multiset, not a set. Repeated
same-second transitions must retain occurrence order and multiplicity.

Because normal attachment begins at EOF, the monitor cannot infer the
producer's absolute per-attempt `_job_state_seq` from post-attachment lines
alone. It must not fabricate one. File offsets order the provisional overlay;
Stampede supplies the authoritative sequence during reconciliation.

### 7.2 Per-instance watermarks

For every DB-backed job instance, retain the highest reconciled
`jobstate_submit_seq` plus the set of full transition row identities at that
sequence. Multiple DB rows may legitimately share one sequence. When pending
overlays exist, query only the bounded transition suffix needed for those
instances. Do not rescan the full event history every two seconds.

Group DB transitions by `jobstate_submit_seq`. Reconcile the longest ordered
prefix of pending tail events that appears in the new DB groups after
normalization. One tail event may correspond to more than one DB row at the
same sequence because monitord emits an extra terminal event at that sequence
(for example the `JOB_ABORTED` and failed-submit paths also send a job-end
event; the DB loader only maps states, it does not fabricate rows). Once
confirmed, replace provisional data with the authoritative DB row/group and
remove that prefix from the overlay.

The normalization/equivalence table must cover at least:

- `JOB_HELD_REASON` in the file to DB `JOB_HELD`;
- the held pair: on HTCondor >= 8.3.3 the file carries both a `JOB_HELD` line
  and a `JOB_HELD_REASON` line while Stampede receives exactly one `JOB_HELD`
  row (dispatched from the reason-bearing event). Keep the plain file
  `JOB_HELD` provisionally visible as the held state. When its matching
  `JOB_HELD_REASON` arrives and the DB confirms `JOB_HELD`, fold the pair into
  that one authoritative row and retire both file events. If the reason-bearing
  line never arrives, retain the provisional held state until a later
  authoritative DB transition explicitly supersedes it or a full DB
  rebootstrap resolves it; do not discard it merely because it has no direct DB
  counterpart;
- `PRE_SCRIPT_FAILURE`/`POST_SCRIPT_FAILURE` to DB `*_FAILED`;
- causal plus synthetic terminal groups such as `JOB_ABORTED` + `JOB_FAILURE`
  or `PRE_SCRIPT_FAILED` + `JOB_FAILURE` at one sequence;
- file-only intermediate events that may be superseded by a later confirmed DB
  state.

For authoritative current-state selection, order primarily by
`jobstate_submit_seq`, then timestamp. When multiple rows share both, use an
explicit reviewed lifecycle/state-precedence table and `state` as the final
deterministic tie-breaker. Terminal outcome rows such as `JOB_SUCCESS` and
`JOB_FAILURE` outrank their causal/intermediate rows at the same sequence.

A DB transition that is demonstrably later than a pending overlay may supersede
it even when the representations differ, but this rule must be encoded as an
explicit state-order/mapping rule and covered by tests. Unknown state pairs are
not guessed; retain the overlay until an exact match, attempt supersession, or a
full DB rebootstrap. Pending events are never retired merely because a TTL
expired; buffer limits trigger visible resynchronization instead.

### 7.3 Workflow-level reconciliation

Workflow tail events use provisional identity
`(source_generation, byte_offset)`. Stampede workflow-state row identity is
`(wf_id, state, timestamp)`, with `restart_count`, `status`, and `reason` as
authoritative fields.

Normalize `DAGMAN_STARTED` to `WORKFLOW_STARTED` and `DAGMAN_FINISHED` to
`WORKFLOW_TERMINATED`. Starting from the DB's latest confirmed restart count,
file order supplies provisional restart ordering: a start opens the next
restart epoch and a finish closes the current epoch. Reconcile by workflow UUID,
normalized state, producer-canonical timestamp, restart epoch, and end status
when present. Workflow matching uses the same `:.0f` DB-timestamp conversion as
job matching because DAGMan file markers are rounded by the same producer;
authoritative ordering retains the full DB timestamp.

Authoritative workflow ordering is `(restart_count, phase, timestamp, state)`,
where start precedes termination within one restart. Tail byte offset resolves
same-time provisional order. Duplicate equivalent file markers do not create
additional workflow epochs unless Stampede confirms a new restart count.

Because `restart_count` is not part of the current `workflowstate` primary key,
Stampede cannot represent two rows with the same workflow, state, and timestamp
even if they came from distinct rapid restarts. If the tail exposes that schema
collision, retain the extra restart as `LIVE/UNCONFIRMED`, report a DB identity
conflict, and do not invent an authoritative row.

Retirement and completion follow the same authority rule as jobs: remove the
workflow overlay after DB confirmation or a DB-confirmed later restart. Never
finalize from `MONITORD_FINISHED`; a DB-confirmed `WORKFLOW_TERMINATED` status is
required.

### 7.4 Bootstrap race cases

The implementation must deliberately handle all three cases:

1. **File event after boundary, absent from first DB snapshot:** display it as
   `LIVE/PENDING`, then retire it when DB catches up.
2. **File event after boundary, already in first DB snapshot:** suppress the
   duplicate during initial merge.
3. **Event immediately before boundary, not yet in first DB snapshot:** it is
   not a live overlay event; the next DB refresh supplies it. While attaching,
   the UI may show `DB CATCHUP` until the DB snapshot advances.

Case 3 is a bounded attachment race, not permanent data loss. The monitor must
not claim that EOF attachment can make an event written before the boundary
visible sooner than the DB exposes it.

### 7.5 File replacement and truncation

On inode replacement, size regression, anchor mismatch, deletion/recreation, or
decode corruption suggesting replacement:

1. for a normal inode rotation, drain complete readable lines from the old open
   descriptor to its EOF;
2. stop applying new events from the old generation;
3. retain already displayed pending events until DB reconciliation or reset;
4. open the new generation and establish its current EOF as a new boundary;
5. trigger an immediate full DB refresh;
6. show `TAIL REATTACHING` until the new base is installed.

Do not replay the replacement file from byte zero during normal live operation;
monitord recovery/replay may have regenerated complete history. Stampede is the
safe backfill path.

If a previously absent file is newly created after attachment, arm it from byte
zero only while it is still empty. If content already exists, establish EOF as
the boundary and trigger DB catchup.

### 7.6 DB replacement and rollback

Track DB file identity and schema/workflow identity. If the SQLite file is
replaced, shrinks unexpectedly, loses the selected workflow, or reports a
transition watermark behind the last-good snapshot, discard reconciliation
watermarks, preserve the last-good display, and perform a full rebootstrap.

Every provisional overlay is tagged with the DB generation against which it was
composed. During rebootstrap, quarantine old-generation overlays and do not
apply them to the new DB base. Revalidate them by workflow/attempt identity and
confirmed transition groups; retire reflected events and reapply only events
that are demonstrably later than the new base. If that relationship cannot be
established, discard the quarantined overlay, show `TAIL RESYNC`, and continue
from a newly armed boundary.

Never merge snapshots from different workflow UUIDs or DB generations.

## 8. Read-only Stampede implementation

Version 1 supports the local SQLite workflow database. PostgreSQL/MySQL URLs are
detected and reported as unsupported rather than mapped to a guessed file.

Requirements:

- reuse Pegasus path/property resolution rules without calling a mutating
  connection helper;
- never reuse `Pegasus.db.base_loader.BaseLoader` (it hardcodes `create=True`)
  and never pass `backup=True` (it rotates and chmods the live DB file); even
  the default `connection.connect()` raises `DBAdminError` on a schema-version
  mismatch, and SQLAlchemy's `engine.connect()` creates an empty SQLite file
  for a missing path — stat the file first and handle version skew explicitly;
- open SQLite using URI `mode=ro`, `uri=True`, and `PRAGMA query_only=ON`;
- do not use `immutable=1` because the DB changes during execution;
- set a small busy timeout and keep transactions short;
- resolve `wf_id` from the selected `wf_uuid` and filter every query by it;
- build workflow state, job records, and recent transitions in one consistent
  transaction;
- retain last-good data on lock, I/O, replacement, or malformed-row errors;
- expose DB generation, snapshot time, and stale age in source health;
- in live mode, wait clearly if the DB does not yet exist; in `--once`, return a
  non-zero error.

Query correctness:

- exactly one `JobRecord` per Pegasus `job` row;
- aggregate task count and transformations for clustered jobs;
- select the current attempt by `job_submit_seq`;
- within one job instance, order transitions deterministically by
  `(jobstate_submit_seq, timestamp, state_precedence, state)`;
- for a cross-instance recent-event feed, order by timestamp first, then stable
  workflow/job/instance identity, sequence, precedence, and state;
- carry the full transition row identity
  `(job_instance_id, state, timestamp, jobstate_submit_seq)` through models and
  JSONL;
- compute end time only from terminal main-job states;
- decode raw POSIX wait status with existing Pegasus behavior;
- select maximum invocation `maxrss` excluding pre/post-script
  pseudo-invocations (`task_submit_seq` -1/-2; real invocations count upward
  from 1 — there is no "wrapper" flag in the schema);
- scope root and subworkflow records independently.

## 9. HTCondor observer safety contract

HTCondor observation remains independent of the live-file/DB coordinator.

### 9.1 Scope and allowlist

Use Pegasus ClassAds:

```text
pegasus_wf_uuid      =?= <selected wf_uuid>
pegasus_root_wf_uuid =?= <root_wf_uuid>   # explicit tree scope only
```

Only these read operations are permitted:

- `condor_q -json -attributes ... -constraint ...`
- `condor_history -json -attributes ... -constraint ... -match ...`
- `condor_status -json -attributes ...`
- `condor_userprio -long` on demand
- `condor_status -negotiator -json -attributes ...` on demand

Never call mutation commands or binding equivalents.

### 9.2 Isolation

- Use argument arrays and `shell=False`.
- Give each process a hard deadline and a separate process group; kill the
  complete group on timeout.
- Allow only one HTCondor subprocess in flight per monitor process.
- Use a copied per-process environment for credentials/configuration; do not
  mutate `os.environ`, `htcondor.param`, or other process-global state.
- Bound stdout/stderr and history match counts.
- Distinguish valid empty results from missing command, timeout, authentication,
  authorization, daemon-unreachable, and parse failures.

### 9.3 Cadence

| Source | Default cadence | Gate |
|---|---:|---|
| `condor_q` | 5 s | exponential backoff on failure |
| `condor_history` | 30 s | attach, terminal-count change, or final refresh |
| `condor_status` | 30 s | queued/held jobs or visible pool panel |
| priority/negotiator | on demand | `--why-idle` or confirmed stall |

Back off with jitter to a five-minute ceiling. A valid empty queue is success.
HTCondor delays or failures must not slow the file tail, DB refresh, or render
loop.

## 10. Workflow scope and hierarchical workflows

Default behavior monitors exactly one `wf_uuid`:

- read that workflow's own `jobstate.log`;
- query the top-level Stampede DB but filter to that `wf_uuid`;
- constrain queue/history to `pegasus_wf_uuid`;
- never merge jobs solely because they share `root_wf_uuid`.

`--include-subworkflows` is deferred to a follow-on release (owner decision 2).
When it lands, it uses a `WorkflowTreeCoordinator` containing one independent
context per workflow UUID:

- one locator result and tail generation per workflow directory;
- one scoped DB snapshot per workflow, potentially from the same top-level DB
  transaction/read connection;
- separate job identities and reconciliation watermarks;
- explicit parent/subworkflow links from Stampede;
- root UUID only for hierarchy and optional HTCondor query scope.

Tree mode must not weaken exact-workflow correctness when it is implemented.
Single-workflow monitoring is the version 1 feature.

## 11. User-facing modes

| Mode | Invocation | Release | Behavior |
|---|---|---|---|
| Live TUI | `pegasus-monitor RUN` | v1 | DB bootstrap + live tail + optional HTCondor |
| One shot | `pegasus-monitor --once RUN` | v1 | one consistent DB snapshot + optional queue; deterministic exit |
| No Condor | `pegasus-monitor --no-condor RUN` | v1 | still uses live tail in live mode; no HTCondor process |
| No live overlay | `pegasus-monitor --no-live-events RUN` | v1 | Stampede polling only |
| Custom live file | `pegasus-monitor --jobstate-path FILE RUN` | v1 | explicit jobstate path for monitord override cases |
| Why idle | `pegasus-monitor --why-idle RUN` | v1 | DB + bounded queue/pool + on-demand priority evidence |
| Diagnostics | `pegasus-monitor --diagnose RUN` | v1 | live sources + bounded kickstart parsing rendered without a sidecar |
| Event log | `pegasus-monitor --log [FILE] RUN` | WP7 fast-follow | versioned monitor output JSONL |
| Server | `pegasus-monitor --serve RUN` | WP7 fast-follow | headless coordinator + JSONL + singleton lock |
| Replay | `pegasus-monitor --replay FILE` | WP7 fast-follow | monitor JSONL only; no workflow source calls |
| Remote | `pegasus-monitor --remote HOST:FILE` | WP7 fast-follow | incremental SSH read of server JSONL |

`--no-condor` is a hard guarantee that no HTCondor executable or binding is
invoked. `--no-live-events` is a diagnostic/fallback switch and does not disable
DB polling.

The complete v1 flag surface is: target, `--version`/`-V`, `--interval`/`-i`,
`--all-jobs`/`-a`, `--[no-]sort-by-activity`, `--events`/`-e`, `--once`,
`--why-idle`, `--remap-submit-dir`, `--diagnose`, `--no-condor`,
`--no-live-events`, `--jobstate-path`, `--schedd`, `--collector`, `--token`,
`--cert`, `--key`, and `--password-file`. The plugin-era `--condor-poll` and
`--no-condor-poll` flags are removed; neither is equivalent to the new
hard-disable `--no-condor`. WP7 adds
`--log`, `--replay`, `--speed`, `--min-free-mb`, `--max-log-mb`, `--serve`,
`--serve-foreground`, `--stop-server`, `--remote`, `--sync-interval`,
`--ssh-config`, and `--ssh-identity`; v1 does not expose placeholder forms of
those flags.

## 12. Output, replay, server, and remote contracts

JSONL is written by `pegasus-monitor --log` or `--serve`, not by monitord.

Every stream begins with:

- `schema_version`;
- unique `stream_id`;
- selected `wf_uuid` and `root_wf_uuid`;
- monitor/version/source metadata.

The header is immediately followed by a full DB-confirmed checkpoint record
containing the workflow snapshot, effective DB-confirmed job roster/state, and
the authoritative transition identities needed to continue applying records.
Emit another checkpoint after logging resumes from a disk-guard gap, after a
stream restart or replacement, at authoritative workflow completion, and every
five minutes by default. WP9b may tune the periodic cadence from measured
checkpoint size/impact, but it must retain a documented maximum recovery window.
A gap marks incremental state incomplete until the next checkpoint. Replay and
remote discard incomplete post-gap reconstruction, resume from that checkpoint,
and never query Stampede themselves.

Every record contains a monotonic `sequence`. In schema version 1, canonical
workflow/job transition records are emitted only after Stampede confirmation
and include the DB identity. The TUI may be ahead of the JSONL stream while a
tail event is pending. Source-health records may report pending-overlay counts,
but replay/remote consumers never have to reverse a provisional state. A future
schema may add provisional/correction events only through an explicit version
change.

When `--diagnose` is active, WP7 also persists versioned `diagnostic_result`
records in the canonical JSONL stream. They contain the snapshot epoch, target
job/workflow identity, diagnostic code/severity, human-readable summary, and
explicit DB/HTCondor/kickstart evidence provenance. They are derived evidence,
not authoritative transitions, never change replayed job state, and must redact
credential values and unbounded command/output content. Replay may render them
alongside the checkpoint/snapshot to which they refer.

Retain the functional event vocabulary from `DATA_SOURCES.md`, with a schema
review before freezing version 1. Writer, replay, and remote readers share one
model/codec and one golden corpus.

Operational requirements:

- file mode `0600` and safe symlink handling;
- free-space floor, optional maximum size, hysteresis, and gap markers;
- exclusive `--serve` lock plus atomic PID metadata;
- byte-offset incremental remote reads;
- SSH target validation, `ssh --`, safely quoted remote path, and output caps;
- stream replacement detected by `stream_id`, not size alone; the consumer
  resets incremental state and resumes at the replacement stream's checkpoint;
- replay never invokes DB, file-tail, kickstart, or HTCondor sources.

## 13. Package and module layout

Create `packages/pegasus-python/src/Pegasus/monitor/`:

```text
Pegasus/monitor/
  __init__.py          lightweight; no Rich or source side effects
  models.py            immutable source and effective snapshot models
  locator.py           braindump/run/DB/jobstate resolution
  stampede.py          side-effect-free read-only DB reader
  live_events.py       jobstate parser, tail generations, bounded buffering
  reconcile.py         DB/tail identity mapping, watermarks, overlay retirement
  coordinator.py       startup protocol and independent source scheduling
  condor.py            safe command builder, runner, caches, backoff
  stats.py             derived statistics from effective snapshots
  diagnostics.py       kickstart and failure analysis
  stall_detector.py    guarded stall evidence
  why_idle.py          optional scheduler explanation
  display.py           Rich TUI and one-shot rendering
  event_log.py         versioned JSONL writer/codec
  replay.py            shared-codec replay
  remote.py            hardened incremental SSH consumer
  server.py            headless coordinator and singleton lifecycle
  cli.py               argparse parser and main(argv=None) -> int
```

CLI integration:

1. Add `pegasus-monitor` to `release-tools/update-python-tools`.
2. Add `packages/pegasus-python/src/Pegasus/cli/pegasus-monitor.py` with the
   standard `PEGASUS_PYTHONPATH` preamble, ending in `sys.exit(main())` to
   preserve exit codes (precedent: `pegasus-cwl-converter.py`; most existing
   shims call bare `main()`, and none uses `raise SystemExit`).
3. Add the wheel console-script entry (owner decision 5). It is the project's
   first such entry and benefits pip installs only: the Ant build installs
   with `pip install --target`, which never generates console scripts:

   ```toml
   [project.scripts]
   pegasus-monitor = "Pegasus.monitor.cli:main"
   ```

Use `argparse`, matching workflow-monitor's existing parser. Click is
undeclared yet vendored (`click==8.4.1` in `src/requirements.txt`) and used by
five existing shims including `pegasus-status`; argparse is chosen to keep the
ported CLI unchanged, not because Click is unavailable — do not add a new
declared dependency for it. Existing `rich` and `PyYAML` dependencies are
sufficient for the core feature.

## 14. Work packages for agent fanout

Model guidance below is for task routing, not a runtime dependency. Tiers are
provider-neutral; the orchestrator maps them to whatever models its harness
offers. For Codex: reasoning tier -> `gpt-5.6-sol`, implementation tier ->
`gpt-5.6-terra`, utility tier -> `gpt-5.6-luna`. For Claude Code: reasoning tier
-> Opus/Fable, implementation tier -> Sonnet, utility tier -> Haiku.

- **reasoning tier, high/xhigh effort:** cross-source correctness, concurrency,
  database semantics, security boundaries, and integration debugging.
- **implementation tier, medium/high effort:** well-specified module
  implementation, UI, codecs, diagnostics, and substantial unit-test work.
- **utility tier, medium effort:** mechanical packaging, documentation wiring,
  fixture conversion, and focused low-risk tests.

### WP0 — Baseline, attribution, and fixture inventory

Suggested model: implementation tier, medium

Owned files:

- `packages/pegasus-python/test/monitor/baseline/README.md`;
- `packages/pegasus-python/test/monitor/baseline/source-manifest.yaml`;
- `packages/pegasus-python/test/monitor/baseline/capability-map.yaml`;
- `packages/pegasus-python/test/monitor/fixtures/baseline/**`.

Deliverables:

- pin the exact workflow-monitor baseline branch/commit — the repo carries
  several unmerged lines (the checkout is `monitord-plugin-adapter`, while
  `stampede_stream.py` exists only on `monitord-plugin-live`, and
  `get_events_since` has a signature drift between lines);
- record the approved attribution policy and source-to-destination mapping
  (owner decisions 1 and 14): each later production-file owner applies the
  Pegasus tree's standard Apache-2.0/USC-ISI header while porting absorbed
  workflow-monitor code. Record the remaining absorption cleanups (`build` and
  `pip` wrongly listed as runtime dependencies; both repos are Apache-2.0, so
  no license conflict);
- map each `DATA_SOURCES.md` capability to a work package;
- run/record the existing workflow-monitor behavioral suite;
- collect sanitized fixtures for normal, failed, held, retried, clustered,
  nested, and completed workflows;
- record golden one-shot and JSONL behavior without freezing known bugs.

Dependencies: none

Done when: the team has a traceability matrix, a pinned baseline commit, and
fixture license/provenance.

### WP1 — Shared models and source contracts

Suggested model: reasoning tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/__init__.py`;
- `packages/pegasus-python/src/Pegasus/monitor/models.py`;
- `packages/pegasus-python/test/monitor/test_models.py`;
- `packages/pegasus-python/test/monitor/contracts/**`.

Deliverables:

- define workflow/job/transition/source-health immutable models;
- define DB and tail identities, provenance, generations, and snapshot epochs;
- define normalizers, DB equivalence groups, and deterministic state precedence;
- define coordinator input/output protocols;
- freeze a DB-confirmed JSONL v1 contract only after reconciliation semantics
  are executable in tests.

Dependencies: WP0 capability map

Done when: WP2, WP3, WP4, and WP5 can implement against stable typed contracts.

### WP2 — Locator and read-only Stampede reader

Suggested model: reasoning tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/locator.py`;
- `packages/pegasus-python/src/Pegasus/monitor/stampede.py`;
- `packages/pegasus-python/test/monitor/test_locator.py`;
- `packages/pegasus-python/test/monitor/test_stampede.py`;
- `packages/pegasus-python/test/monitor/fixtures/stampede/**`.

Deliverables:

- safe run/braindump/path resolution, including `--jobstate-path` and
  subworkflow top-level DB lookup;
- read-only SQLite connection and short consistent snapshot transaction;
- clustered-task aggregation and exact workflow scoping;
- current attempts, full transition row IDs, deterministic current-state
  selection, and reconciliation suffix queries;
- last-good cache, DB generation detection, and stale health states.

Dependencies: WP1

Done when: the `WorkflowLocator` and `StampedeReader` APIs match authoritative
SQL for all DB fixtures, enforce read-only behavior, and do not impede
concurrent writes. Command-level `--once --no-condor` verification belongs to
WP9a after WP4 and WP6b exist.

### WP3 — LiveEventTail and parser

Suggested model: reasoning tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/live_events.py`;
- `packages/pegasus-python/test/monitor/test_live_events.py`;
- `packages/pegasus-python/test/monitor/fixtures/live_events/**`.

Deliverables:

- strict job/internal line parser;
- newline-aligned EOF attachment boundary, bounded backward prefix read, and a
  bounded nonblocking `poll()` API with no internally created thread/task;
- partial-line handling, malformed/overlong-line recovery;
- inode/size/anchor generation detection;
- missing/create/delete/rotate/truncate/regrow handling;
- bounded buffer and explicit gap state.

Dependencies: WP1

Done when: deterministic file tests cover every lifecycle and race listed in
section 16 without DB or UI code.

### WP4 — Reconciler and coordinator

Suggested model: reasoning tier, xhigh

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/reconcile.py`;
- `packages/pegasus-python/src/Pegasus/monitor/coordinator.py`;
- `packages/pegasus-python/test/monitor/test_reconcile.py`;
- `packages/pegasus-python/test/monitor/test_coordinator.py`.

Deliverables:

- exact startup ordering: arm tail, bootstrap DB, merge buffer, publish;
- ordered multiset reconciliation and per-instance DB watermarks;
- DB transition-group equivalence for synthetic rows sharing a sequence;
- workflow restart-epoch reconciliation and DB-generation overlay quarantine;
- provisional job handling and no-double-count effective snapshots;
- sole ownership of file/DB/Condor clocks, task creation, cadence, cancellation,
  and immutable render snapshots;
- source replacement/rebootstrap logic and clean cancellation.

Dependencies: WP1, WP2, WP3; use a fake Condor provider until WP5 lands

Done when: a deterministic simulation proves every attachment ordering and no
optional source can block DB/tail progress.

### WP5 — Safe HTCondor observer

Suggested model: reasoning tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/condor.py`;
- `packages/pegasus-python/test/monitor/test_condor.py`;
- `packages/pegasus-python/test/monitor/fixtures/condor/**`.

Deliverables:

- read-only command allowlist and UUID-safe constraints;
- subprocess process-group deadlines and output limits;
- copied environment and credential/config isolation;
- one-in-flight query guard, last-good caches, and backoff state; WP5 creates no
  scheduler/task and WP4 decides when the provider is polled;
- queue/history/pool normalization and on-demand priority/negotiator evidence.

Dependencies: WP1; can proceed in parallel with WP2/WP3

Done when: fake executables prove load limits, cleanup, isolation, empty-success,
and all failure classifications.

### WP6a — Statistics and diagnostics

Suggested model: implementation tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/stats.py`;
- `packages/pegasus-python/src/Pegasus/monitor/diagnostics.py`;
- `packages/pegasus-python/src/Pegasus/monitor/stall_detector.py`;
- `packages/pegasus-python/src/Pegasus/monitor/why_idle.py`;
- `packages/pegasus-python/test/monitor/test_stats.py`;
- `packages/pegasus-python/test/monitor/test_diagnostics.py`;
- `packages/pegasus-python/test/monitor/test_stall_detector.py`;
- `packages/pegasus-python/test/monitor/test_why_idle.py`.

Deliverables:

- port/adapt workflow-monitor analysis;
- compute statistics from one effective job state with provenance indicators;
- produce structured diagnostics and why-idle results without rendering or
  writing the workflow-monitor `diagnostics-events.jsonl` sidecar;
- define presentation-neutral result models consumed by WP6b;
- complete analysis with HTCondor absent or partially unavailable.

Dependencies: WP4 snapshot contract, WP5 provider contract

Done when: fixture-based statistics/diagnostic results match the pinned
workflow-monitor baseline where intended, document deliberate divergences, and
make no UI/source calls.

### WP6b — TUI and v1 CLI

Suggested model: implementation tier, high

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/display.py`;
- `packages/pegasus-python/src/Pegasus/monitor/cli.py`;
- `packages/pegasus-python/test/monitor/test_display.py`;
- `packages/pegasus-python/test/monitor/test_cli.py`.

Deliverables:

- source-health badges, last-good ages, terminal-safe `--once`, live TUI, and
  final summary;
- render WP6a diagnostics and why-idle results without source calls in the
  render path;
- argparse wiring for exactly the v1 flag surface in section 11 and stable
  process exit codes;
- reconcile behavior against
  `/Users/stealey/GitHub/pegasusai/workflow-monitor/DATA_SOURCES.md` section 11
  and the complete argparse inventory in
  `/Users/stealey/GitHub/pegasusai/workflow-monitor/src/workflow_monitor/cli.py`;
- omit the recorded WP7 flags rather than exposing placeholders, and remove the
  plugin-specific `--condor-poll` flag;
- complete operation with HTCondor absent and with live tail disabled.

Dependencies: WP4 snapshot contract, WP6a result contract

Done when: golden rendering, CLI-contract, and non-TTY tests pass without source
calls in the render path.

### WP7 — JSONL, replay, server, and remote

Suggested model: reasoning tier, high throughout

Owned files:

- `packages/pegasus-python/src/Pegasus/monitor/event_log.py`;
- `packages/pegasus-python/src/Pegasus/monitor/replay.py`;
- `packages/pegasus-python/src/Pegasus/monitor/server.py`;
- `packages/pegasus-python/src/Pegasus/monitor/remote.py`;
- scoped fast-follow changes to
  `packages/pegasus-python/src/Pegasus/monitor/cli.py`;
- `packages/pegasus-python/test/monitor/test_event_log.py`;
- `packages/pegasus-python/test/monitor/test_replay.py`;
- `packages/pegasus-python/test/monitor/test_server.py`;
- `packages/pegasus-python/test/monitor/test_remote.py`;
- `packages/pegasus-python/test/monitor/test_cli_wp7.py`.

Deliverables:

- versioned DB-confirmed stream/sequence schema and shared codec;
- initial/periodic/recovery/final DB-confirmed checkpoints and deterministic
  recovery at the next checkpoint after a gap or stream replacement;
- versioned `diagnostic_result` records for WP6a results, with provenance,
  redaction, replay rendering, and no state-changing semantics;
- disk guard, secure files, singleton server lifecycle;
- deterministic replay using the same effective snapshot models;
- offset-based hardened SSH remote reader;
- golden corpus shared by every producer/consumer.

Dependencies: WP1, WP4, WP6b; after the v1 tag/merge, WP7 receives scoped
ownership of `cli.py` to add only the section 11 fast-follow flags. It ships
immediately after the first native live release with the WP8 fast-follow docs
update and its own WP9b gate (owner decisions 3 and 13).

Done when: local, replay, and remote converge to equivalent DB-confirmed
snapshots/final state; torn lines are tolerated; gaps, restarts, and replacements
discard incomplete incremental state and recover at the next checkpoint.
Remote/replay are not required to mirror a still-pending local tail overlay in
schema version 1.

### WP8 — Packaging and documentation

Suggested model: utility tier, medium

Owned files:

- `packages/pegasus-python/src/Pegasus/cli/pegasus-monitor.py`;
- `packages/pegasus-python/pyproject.toml`;
- `packages/pegasus-python/tox.ini`;
- `.gitlab-ci.yml`;
- `release-tools/update-python-tools`;
- `doc/sphinx/manpages/pegasus-monitor.rst`;
- `doc/sphinx/reference-guide/cli.rst`;
- `doc/sphinx/user-guide/monitoring-debugging-stats.rst`;
- `doc/sphinx/release-notes/release_6.0.x.md` and `doc/sphinx/index.rst`.

Deliverables:

- native Ant distribution wrapper and required wheel console script;
- `pegasus-monitor` manpage and monitoring guide;
- source precedence, health badges, DB scope, cadence, credentials, and safety
  guarantees documented;
- document the coexistence of `pegasus-monitor` with the untouched
  `pegasus-status --watch` (owner decision 7): the existing tool is not
  modified or deprecated, and the guide presents `pegasus-monitor` as the
  richer live view;
- user-facing branding changed from workflow-monitor to pegasus-monitor.

Dependencies: stable v1 CLI from WP6b; server/remote documentation is a scoped
fast-follow update coordinated with WP7

Done when: `ant dist-doc` succeeds;
`doc/sphinx/manpages/pegasus-monitor.rst` is built and the generated/installed
`pegasus-monitor` manpage exists;
`doc/sphinx/reference-guide/cli.rst` contains its explicit toctree entry; and
built-distribution and wheel invocations preserve exit codes.

### WP9a — Minimum-release integration, scale, and fault-injection gate

Suggested model: reasoning tier, xhigh

Owned files:

- `packages/pegasus-python/test/monitor/integration/**`;
- `packages/pegasus-python/test/monitor/scale/**`;
- `packages/pegasus-python/test/monitor/ACCEPTANCE-v1.md`.

Deliverables:

- integrate WP2-WP6b and WP8;
- run 100k-job/1M-event scale, burst-latency, and bounded-memory tests;
- inject DB locks/replacement, tail replacement/gaps, and hung/broken Condor
  commands;
- measure DB transaction duration, source call cadence, CPU, and memory;
- enforce the fixed performance budgets in section 16.5;
- verify monitor termination leaves no helpers and does not affect workflow
  processes;
- produce the v1 acceptance evidence matrix;
- route defects back to the owning WP, wait for its isolated patch/merge, and
  rerun the affected gate rather than silently fixing another WP's files.

Dependencies: WP2-WP6b, WP8

Done when: every minimum-release acceptance check has reproducible evidence.

### WP9b — Fast-follow integration and recovery gate

Suggested model: reasoning tier, xhigh

Owned files:

- fast-follow additions under `packages/pegasus-python/test/monitor/integration/**`;
- fast-follow additions under `packages/pegasus-python/test/monitor/scale/**`;
- `packages/pegasus-python/test/monitor/ACCEPTANCE-wp7.md`.

Deliverables:

- integrate WP7 plus its scoped WP6b CLI and WP8 documentation updates;
- verify checkpoint size/cadence, gap recovery, stream replacement, replay,
  server, remote SSH, security, and final-state equivalence;
- verify the fast-follow does not violate the section 16.5 CPU, memory, DB, or
  workflow-impact budgets;
- inject disk-guard gaps, torn records, process restarts, and stream replacement;
- route defects to their owning WP and rerun affected gates.

Dependencies: WP7 and the WP8 fast-follow documentation update

Done when: every extended-release acceptance check has reproducible evidence.

## 15. Fanout sequence and ownership rules

The fanout gate is satisfied: all owner and implementation decisions are
recorded in section 20 (2026-08-17). Implementation fanout may begin.

### Parallel waves

```text
Wave A: WP0 --> WP1
                  |
Wave B:       +---+----------+-------------+
              v              v             v
             WP2            WP3           WP5
              +-------+------+-------------+
                      v
Wave C:              WP4 --> WP6a --> WP6b
                                      |
Wave D (v1):                         WP8 --> WP9a

After v1:             WP7 + WP8 fast-follow docs --> WP9b
```

WP0 research can overlap early WP1 drafting, but source/model interfaces must be
approved before WP2-WP5 merge. WP9a gates the minimum release without WP7. WP7
then receives scoped `cli.py` ownership, ships with the WP8 fast-follow
documentation update, and passes WP9b.

Every implementation WP uses a dedicated branch/worktree. The integration lead
merges completed work against the frozen WP1 contract and prevents agents from
editing another WP's files. `pegasus-monitor.md` is a temporary development
artifact: track it on the feature branch while agents need a stable reference,
then rewrite/squash it out of the submitted branch history and restore the local
`.git/info/exclude` entry before opening the PR.

### Agent handoff rules

- One architecture/integration lead owns `models.py` contracts and approves
  cross-package changes.
- Assign modules with non-overlapping file ownership. Shared fixture changes are
  coordinated through the integration lead.
- Each agent receives this plan plus its work-package section, dependencies,
  owned files, and exact done criteria.
- Every porting agent also receives the WP0 pinned workflow-monitor source
  commit and the frozen WP1 interface commit.
- Each agent returns: files changed, interface deviations, tests run, measured
  results, unresolved risks, and recommended follow-up.
- Contract changes require notifying every dependent work package before merge.
- Prefer fakes and deterministic clocks for source/concurrency tests; reserve
  live HTCondor and end-to-end workflows for the integration gate.
- Do not make WP6b/UI code compensate for source correctness defects. Fix source
  or reconciliation behavior at its owning layer.

Suggested fanout prompt shape:

```text
Implement WP<N> from pegasus-monitor.md.
Own only: <files/modules>.
Port from workflow-monitor baseline commit <wp0-id>.
Honor interfaces from WP1 at commit <id>.
Run: <focused tests>.
Return: changed files, test evidence, contract deviations, risks.
Do not edit: <other agents' modules>.
```

## 16. Verification plan

### 16.1 Live attachment and reconciliation

- Event appended after boundary and before first DB query; DB initially lags.
- DB confirmation delayed for at least the normal monitord batch interval.
- Event appended during the DB transaction.
- Buffered event already present in the first DB snapshot.
- Event written immediately before boundary and exposed by a later DB poll.
- Multiple distinct transitions at the same integer timestamp.
- DB timestamps around half-second boundaries use producer-equivalent `:.0f`
  canonicalization for file matching while retaining full precision for DB
  ordering; truncation is explicitly rejected.
- Workflow/DAGMan timestamps use the same half-second canonicalization and retain
  full DB precision for authoritative restart ordering.
- Repeated identical semantic transitions retain multiplicity.
- Multiple DB rows sharing one `jobstate_submit_seq`, including a synthetic
  `JOB_FAILURE`, select one deterministic authoritative current state.
- Retry of the same executable job with a new `job_submit_seq`.
- A held job on HTCondor >= 8.3.3 first exposes the plain `JOB_HELD` line as a
  provisional visible held state. When `JOB_HELD_REASON` and the one DB
  `JOB_HELD` row arrive, the pair folds into that authoritative row and both
  file events retire without stalling the pending prefix. A fixture where the
  reason line never arrives retains the provisional held state until a later
  authoritative DB transition supersedes it.
- Provisional event arrives before the DB job/instance row.
- DB advances beyond a pending event with a documented normalization rule.
- Overlay retirement never duplicates job counts, event output, or statistics.

### 16.2 Tail robustness

- Missing at startup, later creation empty, later creation already populated.
- Partial line across reads and torn final line at shutdown.
- Malformed UTF-8, malformed fields, unknown state, and overlong line.
- Normal append, inode rotation, deletion/recreation, size regression.
- In-place truncate and regrow past the previous cursor detected by anchor.
- Rapid replacement while DB bootstrap is running.
- Buffer/byte limit overflow produces `TAIL GAP` and DB recovery.
- Source stops without `MONITORD_FINISHED`; DB continues.
- `DAGMAN_FINISHED`/`MONITORD_FINISHED` arrives before the final DB flush.
- Rapid repeated DAGMan restarts, including a same-state/same-timestamp case
  that the current workflowstate primary key cannot represent twice.
- A legitimately idle workflow produces no tail lines for a long interval;
  silence alone is never classified as tail failure or workflow stall.

### 16.3 Stampede correctness and safety

- Normal, failed, held, evicted, aborted, retried, and same-time transitions.
- Clustered jobs with many task rows remain one Pegasus job.
- Root/subworkflow UUIDs never contaminate one another.
- Custom SQLite path, container remap, absent DB, locked DB, replacement,
  rollback, schema mismatch, and malformed rows.
- PostgreSQL/MySQL URLs fail with an explicit unsupported-backend result and do
  not trigger a guessed SQLite path or external connection attempt.
- Deterministic latest-run selection, ambiguity/mismatch rejection,
  subworkflow top-level DB lookup, recorded/local path remapping, default `jsd`,
  and explicit `--jobstate-path` resolution.
- Concurrent writer proves reads are short and monitord commits continue.
- Read-only enforcement proves no DB file/table/pragma mutation.
- A `0444` DB/log inside a non-writable submit directory produces no sidecar,
  lock, chmod, migration, journal-mode change, or source-file metadata change
  attributable to the monitor.
- Rollback-journal and WAL databases both remain observable under contention.

### 16.4 HTCondor safety

- Every queue/history command includes an exact Pegasus UUID constraint.
- Missing binary, empty result, non-zero exit, auth failure, timeout, malformed
  JSON, oversized output, and recovery.
- Timeout kills the complete process group.
- Only one command is in flight and cadence counters remain within limits.
- Parent environment/configuration is unchanged.
- Hung history/pool work never freezes DB/tail/render refresh.
- No mutation executable can be constructed by the command builder.

### 16.5 UI, output, security, and scale

- TTY, narrow terminal, redirected one-shot, and clean interrupt.
- Provenance and source-health badges with accurate stale ages.
- 100k jobs and at least 1M state lines without quadratic rescans or unbounded
  memory; burst tests cover 500-1,000 appended lines/second.
- Under the reference burst benchmark, post-boundary live-event display latency
  must have p95 below one second.
- Shared JSONL golden corpus across writer/replay/remote.
- Initial, periodic, post-gap, replacement, and final checkpoints reconstruct
  the same DB-confirmed snapshot.
- Diagnostic records round-trip with snapshot identity and evidence provenance,
  redact bounded sensitive content, render in replay, and never alter state.
- Missing sequence, unsupported version, torn line, restart, and replacement;
  post-gap state remains incomplete only until the next checkpoint.
- Double server start rejected; files are `0600`; unsafe symlinks rejected.
- SSH target/path injection and reconnect cases.
- Importing non-UI modules does not load Rich or start source activity.
- V1 parser exposes exactly the section 11 v1 flags, rejects WP7 flags rather
  than advertising placeholders, and has no `--condor-poll` option. WP7 tests
  add its recorded fast-follow flags without altering v1 semantics.
- The with/without-monitor benchmark must keep median workflow makespan impact
  below 2%.
- During the 100k-job/1M-event benchmark, monitor CPU must average below one
  logical core and peak RSS must remain below 1 GiB.
- Stampede refresh transactions must have p95 duration below 500 ms and no
  transaction may remain open longer than 2 seconds.

### 16.6 Packaging

- Focused unit suite under `packages/pegasus-python/test/monitor/`.
- `ant test-python` remains green.
- The feature's release support contract follows the classifiers in
  `packages/pegasus-python/pyproject.toml`: Python 3.10 through 3.14. Although
  `packages/pegasus-python/tox.ini` also lists `py315`, treat Python 3.15 as
  aspirational until project metadata and CI explicitly declare and support it.
- Dedicated CI jobs provision and run Python 3.10 and 3.14; a missing
  interpreter fails the job rather than being silently skipped. The current CI
  runs a single `python:3.14` image with `tox-uv` provisioning and
  `skip_missing_interpreters = True`, so this gate requires a tox/CI
  configuration change, not just new jobs.
- The package-wide coverage gate (`--cov-fail-under` in tox.ini) remains
  satisfied with the new `Pegasus/monitor/` package included.
- `ant dist` produces a working `bin/pegasus-monitor`.
- `ant dist-doc` (the `tox -e docs` chain) builds HTML/man output including the
  `pegasus-monitor` manpage. Note `ant dist-release` does not build
  documentation — its dependency chain never reaches the doc targets, and CI
  builds docs as a separate job. The manpage requires both
  `doc/sphinx/manpages/pegasus-monitor.rst` (auto-discovered by `conf.py`) and
  an explicit toctree entry in `doc/sphinx/reference-guide/cli.rst`.
- Wheel installation produces the console script (owner decision 5: the
  `[project.scripts]` entry is adopted).
- Wrapper and wheel preserve non-zero CLI return codes.
- No monitord plugin entry point or property is installed or documented.

## 17. Degradation behavior

| Failure | Required behavior |
|---|---|
| `jobstate.log` absent | DB-only live view; keep looking; show `TAIL WAITING` |
| Tail is silent but readable/stable | no failure inference; report last-event age as informational only |
| Tail malformed line | Skip line, count/report error, continue |
| Tail rotation/truncation | Re-arm at new EOF, refresh DB, show `TAIL REATTACHING` |
| Tail buffer overflow | Mark `TAIL GAP`, force DB catchup, bound memory |
| DB not created | live mode waits; `--once` errors |
| DB locked/read failure | keep last-good base plus safe pending overlay; show `DB STALE` |
| DB output permanently unavailable/disabled | bounded live view with `DB FAILED / LIVE UNCONFIRMED`; no authoritative completion |
| DB replaced/rolled back | freeze last-good, discard watermarks, rebootstrap |
| `condor_q` unavailable | DB/tail view continues; `SCHEDD?`; backoff |
| history unavailable | retain bounded prior history; omit efficiency additions |
| collector unavailable | retain briefly, then `POOL?` |
| why-idle source unavailable | partial evidence with explicit unknowns |
| kickstart unavailable | DB/hold/exit-code diagnosis only |
| JSONL disk guard active | TUI continues; logging pauses with a gap marker and emits a DB-confirmed checkpoint immediately after safe resume |

No optional-source failure may terminate the TUI. `Ctrl-C` or killing
`pegasus-monitor` must not alter DAGMan, monitord, the schedd, or workflow jobs.

## 18. Release gates and acceptance checks

### Minimum native release

Required: WP0-WP6b, WP8, and WP9a for live/once modes.

1. `pegasus-monitor RUN` attaches without prior configuration, arms the tail,
   and renders a DB-backed snapshot within one DB interval.
2. A job transition written after attachment appears from `jobstate.log` before
   a deliberately delayed DB commit, then reconciles without duplicate state or
   count changes.
3. Complete pre-attachment history and metadata come from Stampede even when
   the monitor starts late.
4. Missing/rotated/truncated `jobstate.log` degrades to DB-only and recovers
   without restarting the monitor.
5. `--no-condor` invokes no HTCondor code and still supplies Pegasus state,
   timing, site, attempts, exit status, and requested kickstart diagnostics.
6. HTCondor enrichment is UUID-scoped, bounded, isolated, and unable to block
   core monitoring.
7. Clustered and nested fixtures match authoritative SQL job counts/state
   histograms.
8. Killing the monitor leaves no helper process and does not affect workflow
   execution.
9. The Ant executable behaves correctly and preserves exit codes.
10. The latency, workflow-impact, CPU, memory, and DB-transaction budgets in
    section 16.5 all pass as release-blocking gates.

### Extended release

Required additionally: WP7, its scoped WP8 documentation update, and WP9b.

1. `--serve` + `--remote` and `--replay` converge to the local DB-confirmed
   snapshot and reproduce final statistics; they need not reproduce a
   still-pending local tail overlay in schema version 1.
2. Stream gaps, restarts, replacement, torn records, and unsupported versions
   are detected explicitly; reconstruction resumes from the next DB-confirmed
   checkpoint after a recoverable gap or replacement.
3. Output lifecycle and SSH handling meet the security requirements.

## 19. Explicit non-goals

- No monitord plugin, plugin queue, `plugins://` endpoint, wfevents writer, or
  socket EventSink.
- No second `dagman.out` parser.
- No job/pool mutation or automated remediation.
- No claim of PostgreSQL/MySQL workflow DB support in version 1.
- No claim that `jobstate.log` has Stampede's exact event identity.
- No background daemon unless the user explicitly invokes `--serve`.
- No special workflow property or re-planning requirement.

## 20. Owner and implementation decisions (recorded 2026-08-17)

All seventeen decisions are recorded; the section 15 fanout gate is satisfied.

1. **Attribution — USC/ISI headers.** Absorbed workflow-monitor code is
   re-headered to the Pegasus tree's standard Apache-2.0/USC-ISI convention
   and treated as an upstream contribution.
2. **`--include-subworkflows` — deferred to a follow-on.** Version 1 monitors
   exactly one `wf_uuid`; tree mode ships later (section 10).
3. **WP7 — immediately after version 1.** The first release is the
   live/once/diagnose/why-idle feature set (WP0-WP6b, WP8, WP9a); WP7 follows
   as a fast-follow with WP9b.
4. **SQLite-only Stampede attach for version 1 — confirmed.**
   PostgreSQL/MySQL workflow-DB URLs report an explicit unsupported-backend
   error and are never mapped to a guessed file.
5. **`[project.scripts]` — add the entry.** The project's first console-script
   entry ships with `pegasus-monitor`; the Ant wrapper path is unaffected
   (section 13).
6. **Live-event architecture reversal — confirmed.** The `jobstate.log` tail
   overlay replaces the 2026-08-07 plugin-written `wfevents.jsonl` decision;
   the monitord plugin path remains a non-goal (section 19).
7. **`pegasus-status --watch` — coexist.** `pegasus-status` is not modified or
   deprecated; documentation presents `pegasus-monitor` as the richer live
   view, leaving supersession as a possible later decision once the tool has
   field time.
8. **CLI release boundary — working flags only.** Version 1 exposes the exact
   live/once/diagnostic/HTCondor flag set in section 11. WP7 flags are absent
   until the fast-follow; the plugin-specific `--condor-poll` flag is removed.
9. **Scheduling ownership — WP4.** WP4 alone owns clocks, tasks, cadence, and
   cancellation. WP3 and WP5 expose bounded polling/provider APIs and create no
   background schedulers.
10. **JSONL gap recovery — checkpoints.** Streams contain initial, periodic,
    recovery, and final DB-confirmed checkpoints. Replay/remote recover at the
    next checkpoint after a gap or replacement without querying Stampede.
11. **Diagnostic persistence — no v1 sidecar.** Version 1 renders `--diagnose`
    results but does not write `diagnostics-events.jsonl`; persistence arrives
    with WP7's output system.
12. **WP6 — split.** WP6a owns analysis and structured diagnostic results;
    WP6b owns presentation and the v1 CLI and depends on WP6a.
13. **Fast-follow ownership and gates — explicit.** After v1, WP7 receives
    scoped `cli.py` ownership, coordinates its documentation update with WP8,
    and passes a separate WP9b gate. WP9a gates v1 without WP7.
14. **Attribution execution — owner applies.** WP0 records provenance, header
    policy, and source mapping; each later production-file owner applies the
    approved headers to the code it ports.
15. **Workflow-base selection — numeric run ordering.** Prefer valid top-level
    braindumps, select the highest numeric `runNNNN`, use planning timestamp only
    as a tie-breaker, and reject unresolved ambiguity.
16. **Development isolation and plan lifecycle — worktrees and clean PR.** Each
    parallel WP uses a dedicated branch/worktree. The plan is temporarily
    tracked for stable development references, then removed from the final PR
    diff and commit history and restored to `.git/info/exclude` before submission.
17. **Performance budgets — fixed release gates.** Post-boundary live-event
    display latency must have p95 below one second; median workflow makespan
    impact must remain below 2%; monitor CPU must average below one logical core;
    peak RSS must remain below 1 GiB at 100k jobs/1M events; Stampede refresh
    transactions must have p95 below 500 ms and no transaction may exceed 2
    seconds.
