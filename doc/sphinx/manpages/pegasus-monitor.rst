.. _cli-pegasus-monitor:

===============
pegasus-monitor
===============

1
pegasus-monitor
Monitor and diagnose one Pegasus workflow

   ::

      pegasus-monitor [options] [TARGET]


Description
===========

**pegasus-monitor** provides a live terminal view or a deterministic one-shot
report for one Pegasus workflow. ``TARGET`` may be a submit directory, a
workflow base directory, or a ``braindump.yml`` file. If it is omitted, the
current directory is used. For a workflow base directory, the latest numeric
run is selected.

The monitor first obtains complete authoritative workflow state from the
read-only Stampede SQLite database. In live mode, new ``jobstate.log`` records
are shown as a provisional overlay until Stampede confirms them. Optional,
UUID-scoped HTCondor queries enrich the view but never define Pegasus job or
workflow state. The monitor does not use the pegasus-monitord plugin system and
does not modify the workflow, its database, DAGMan, or HTCondor jobs.

Version 1 monitors exactly one workflow UUID. Subworkflows are not included
unless separately selected as ``TARGET``.


Options
========

**-h**; **--help**
   Print help and exit.

**-V**; **--version**
   Print the installed version and exit.

**-i** *SECONDS*; **--interval** *SECONDS*
   Set the Stampede refresh interval. The default is 2 seconds. The value must
   be positive and finite.

**-a**; **--all-jobs**
   Include infrastructure jobs as well as compute jobs.

**--sort-by-activity**; **--no-sort-by-activity**
   Enable or disable sorting running and recently active jobs first. Activity
   sorting is enabled by default.

**-e** *N*; **--events** *N*
   Display at most *N* recent events. The default is 15.

**--once**
   Print one non-interactive, authoritative Stampede snapshot and exit. This
   mode returns nonzero if the database is unavailable.

**--why-idle**
   Produce a one-shot report with bounded queue, pool, priority, and negotiator
   evidence explaining idle jobs. Missing optional evidence is reported as
   unknown rather than treated as workflow failure. Actionable verdicts appear
   before pool statistics, and one-shot reports do not use the live display
   line limit. Fresh queue observations also explain held jobs while the
   recorded Pegasus lifecycle is catching up. A fresh idle queue observation
   is included even if the recorded state still says held or unsubmitted.
   Evicted jobs are queued, not held; same-time release events take precedence
   over eviction events. The job table labels the recorded lifecycle and
   scheduler observation separately.

**--remap-submit-dir** {**auto**, **always**, **never**}
   Control remapping of paths recorded at planning time onto the local
   ``braindump.yml`` location. The default is **auto**.

**--diagnose**
   Add bounded failure, hold, kickstart, and stall diagnostics. Diagnostic
   source failures degrade the report instead of changing workflow state. When
   event logging is active, redacted diagnostic results are also recorded as
   derived evidence; they never change replayed workflow or job state. Fresh
   queue observations can report a hold or release before the job log does.
   Evidence is matched to the current job attempt. Final failure reports retain
   a preceding hold reason and distinguish the application exit code, when
   Kickstart output is available, from the recorded job exit code. One-shot
   diagnostics query history as well as the queue, so a restarted monitor can
   recover hold reasons retained by HTCondor. Manual and unclassified holds
   include their redacted reason.

   When the current attempt's PegasusLite stderr contains an explicit exit
   summary, diagnostics report the tool exit code separately from the
   PegasusLite wrapper, Kickstart process, and recorded job exit codes. Reads
   remain bounded to 1 MiB and reject unsafe paths and symlinks. These results
   use the ``pegasus_lite`` diagnostic evidence source in event streams; use
   the updated monitor when replaying or remotely reading such streams.

**--json**
   Write one JSON document to standard output instead of the text report. It
   requires ``--once``, ``--diagnose``, or ``--why-idle``, always runs one
   shot even on a terminal, and cannot be combined with ``--serve``,
   ``--serve-foreground``, or ``--stop-server``. See `JSON Output`_.

**--log** [*PATH*]
   Write canonical schema-version-1 JSONL while monitoring locally. If *PATH*
   is omitted, the file is ``SUBMIT_DIR/workflow-events.jsonl``. Because the
   path is optional, place ``TARGET`` before a pathless ``--log`` invocation,
   for example ``pegasus-monitor TARGET --log``.

**--replay** *PATH*
   Replay a canonical monitor JSONL file without querying Stampede,
   ``jobstate.log``, kickstart output, or HTCondor. Replay requires a terminal
   unless ``--once`` is also specified.

**--speed** *MULTIPLIER*
   Set replay speed. The default is 1.0; 0 disables replay delays. The value
   must be finite and nonnegative.

**--no-condor**
   Disable all HTCondor executable and binding use. Stampede and live event
   monitoring continue normally.

**--no-live-events**
   Disable the ``jobstate.log`` overlay and poll Stampede only.

**--jobstate-path** *PATH*
   Use an explicit live event file instead of the resolved ``jobstate.log``.

**--min-free-mb** *MB*
   Pause event-log writes when the target filesystem would fall below this
   free-space floor. The default is 200 MB; 0 disables the floor. Local
   monitoring continues while logging is paused.

**--max-log-mb** *MB*
   Set a hard maximum event-log size. The default is unlimited. The value must
   be positive when specified; reaching the limit pauses logging without
   stopping the monitor.

**--serve**
   Launch a detached, headless monitor that writes canonical JSONL. One server
   may own a given event-log path at a time.

**--serve-foreground**
   Run the headless server in the foreground. This form is intended for
   process supervisors and troubleshooting; most users should choose
   ``--serve`` when a background server is desired.

**--stop-server** [*PID_FILE*]
   Stop a server after verifying its recorded process identity. With no
   *PID_FILE*, the default server for ``TARGET`` is selected. Put ``TARGET``
   before the option, for example ``pegasus-monitor TARGET --stop-server``.
   For a server started with a custom ``--log``, pass the same ``--log``, or
   supply the adjacent hidden PID file explicitly.

**--remote** *USER@HOST:PATH*
   Read and display canonical server JSONL incrementally over bounded SSH.
   Remote live display requires a terminal; ``--once`` reads through the
   current end of file and prints the latest complete DB-confirmed state.

**--sync-interval** *SECONDS*
   Set the remote synchronization interval. The default is 5 seconds. The
   value must be positive and finite.

**--ssh-config** *PATH*
   Use an explicit local SSH configuration file for ``--remote``.

**--ssh-identity** *PATH*
   Use an explicit local SSH identity file for ``--remote``.

**--schedd** *NAME*
   Select the HTCondor schedd for optional observations.

**--collector** *HOST[:PORT]*
   Select the HTCondor collector for optional observations.

**--token** *DIRECTORY*
   Use an HTCondor IDTOKEN directory for monitor subprocesses.

**--cert** *PATH*; **--key** *PATH*
   Use an X.509 certificate and private key for monitor subprocesses.

**--password-file** *PATH*
   Use an HTCondor password file for monitor subprocesses.


Source Health and Degradation
=============================

The source panel reports states such as ``HEALTHY``, ``WAITING``, ``STALE``,
``GAP``, ``REATTACHING``, ``RESYNC``, ``UNAVAILABLE``, and ``DISABLED``, with
the last-good age when available. ``DB FAILED / LIVE UNCONFIRMED`` means live
events may still be visible but no authoritative completion or final counts can
be asserted. Tail gaps and rotation trigger a Stampede catch-up and reattach.
An unavailable HTCondor command, daemon, credential, or network path leaves the
Pegasus database/tail view running with explicit unknown source health.

The monitor supports a local SQLite Stampede database only. PostgreSQL and
MySQL workflow database URLs are detected and reported as unsupported. SQLite
is opened read-only with short queries; the monitor creates no database,
sidecar, lock, migration, journal-mode change, or plugin output.

Queue and history observations are constrained to the selected
``pegasus_wf_uuid``. Pool and priority observations are bounded and used only
when relevant or requested. Commands have deadlines, output limits, serialized
execution, and failure backoff. Credentials are passed through a copied child
environment and credential values are redacted from diagnostics.


Event Log and Recovery
======================

``--log`` and ``--serve`` produce monitor JSONL; pegasus-monitord does not
produce this stream. Every stream begins with a header containing its unique
``stream_id`` and selected workflow identity, immediately followed by a full
DB-confirmed checkpoint. Records have monotonically increasing sequence
numbers. Workflow and job transitions enter the canonical stream only after
Stampede confirmation, so the local TUI may temporarily be ahead of the log
while it displays a provisional ``jobstate.log`` overlay. New and updated job
attempts (submissions, retries, exit codes, timing) are written as incremental
``job_attempt`` records ahead of the transitions that refer to them, so the
stream grows with the number of changes rather than the size of the workflow.

Additional DB-confirmed checkpoints are written periodically, every five
minutes by default, and after recovery, database replacement or structural
change, and authoritative workflow completion. A disk, size, or writer gap
invalidates subsequent incremental reconstruction until the next checkpoint.
When safe writing resumes, the writer records the gap and a recovery
checkpoint. A replaced output file starts a new stream with a new
``stream_id`` and complete checkpoint.

Event logs, server metadata, and singleton state use restrictive permissions
and reject unsafe symlink or non-regular-file targets. An existing event-log
target is replaced only when it contains a valid pegasus-monitor stream header.
The CLI also rejects output paths that overlap the located workflow database
and its sidecars, live event log, braindump, properties, or root DAG file before
monitoring starts.
Logging failures and capacity guards do not modify or stop the workflow.
If the initial checkpoint cannot be written, the command reports the logging
failure on stderr and exits nonzero. A size cap stops recording with a
``size_limit`` gap record; 1 KiB of the cap is reserved for this marker.
A cap too small for the initial checkpoint and marker fails before creating
the log. Temporary free-space shortages after startup still pause logging
until space recovers.

An orderly recorder shutdown, including ``--stop-server`` while a workflow is
running, writes a checkpoint with reason ``stopped``. This closes the recording
without declaring the workflow complete. A completed workflow uses reason
``final``. Replay and remote consumers report stopped streams and size-limit
gaps explicitly; reaching the end of a mid-run stream does not imply completion.


Replay, Server, and Remote Modes
================================

Replay and remote consumers use the same typed schema and renderer. They
tolerate an incomplete trailing line, reject unsupported schemas, and discard
incomplete post-gap state until a checkpoint restores authority. Stream
replacement is detected by ``stream_id``, not file size. Replay never opens
workflow sources. With redirected output, ``--replay``, ``--remote``, and
``--diagnose`` automatically produce one snapshot, as ``--once`` does.
On a TTY these options retain their interactive behavior. Ordinary live
monitoring still requires a TTY, and ``--why-idle`` remains one-shot. Remote mode validates the SSH target and path, invokes SSH
without a local shell, bounds each read and captured error output, and resumes
by byte offset.

``--serve`` creates atomic hidden PID metadata and a singleton lock adjacent to
the selected JSONL path. The default files are
``.workflow-events.jsonl.pid`` and ``.workflow-events.jsonl.lock`` in the
submit directory. A custom log path has correspondingly named adjacent files.
``--stop-server`` verifies process-birth identity before signaling a recorded
PID and does not signal a reused or mismatched PID.

``--replay``, ``--remote``, ``--serve``, ``--serve-foreground``, and
``--stop-server`` are mutually exclusive. ``--log`` cannot be combined with
``--replay``, ``--remote``, or ``--stop-server`` *PID_FILE*. Server modes cannot be
combined with ``--once`` or ``--why-idle``. ``--ssh-config`` and
``--ssh-identity`` require ``--remote``.


Removal and Idle Evidence
=========================

Completed local snapshots and server runs query HTCondor history unless
``--no-condor`` is set. Removed jobs show ``REMOVED`` in the Scheduler column,
with the recorded Pegasus state retained separately. They are excluded from
active running, queued, and held counts. Removal diagnostics include the
scheduler's removal reason while preserving any recorded failure cause.
The workflow banner reports ``REMOVED`` when the selected DAGMan was removed;
removal of child jobs alone does not establish workflow removal.

Why Idle treats a current attempt with a scheduler ID as submitted even before
its submit event arrives. It reports numeric resource fit on free slots
without claiming that ClassAd Requirements or policy match. For previously
started jobs idle for at least five minutes despite available capacity, it
suggests inspecting ``condor_q -better-analyze`` and considering a targeted
hold/release to retry scheduling.


Runtime Reporting
=================

Completed job runtimes use the current attempt's recorded main-invocation
durations (summed for clustered tasks), excluding held and queued time. Until
those records arrive, the monitor measures from the latest execution start.
For running jobs, Runtime estimates the current execution only; held and idle
jobs show ``-`` instead of advancing a wall clock. These durations are preserved
in event streams and replay.


JSON Output
===========

``--json`` replaces the text report of a one-shot mode with exactly one JSON
document on standard output: a single line of ASCII, terminated by a newline,
with no terminal control sequences. Warnings and the usual error text still go
to standard error. A document is written on failure too, so standard output is
always valid JSON.

The document is a separate contract from the ``--log`` event stream. It
carries ``format`` (``pegasus-monitor-json``) and ``format_version`` rather
than the stream's ``schema_version``, and it includes HTCondor evidence,
diagnostic suggestions, the why-idle analysis, and a stall assessment, none of
which the event stream records. Adding a key or an enumeration value keeps
``format_version``; removing or renaming a key, or changing its type or
meaning, increments it. Consumers should ignore keys they do not recognize.

Every document has the same top-level keys:

``format``, ``format_version``, ``monitor_version``
   Contract identity and the Pegasus version that wrote the document.

``generated_at_epoch``, ``generated_at``, ``observed_at_epoch``
   When the document was written (epoch seconds and ISO 8601 UTC) and the
   clock reading of the snapshot it describes.

``modes``, ``origin``, ``target``
   The requested modes in the order ``once``, ``diagnose``, ``why_idle``; the
   origin ``live``, ``replay``, or ``remote``; and the target as given. A
   ``usage_error`` document carries them too when the arguments parsed and
   only their combination was rejected; ``modes`` names one-shot modes only,
   so it is empty for ``--json --serve``. When the arguments did not parse,
   ``target`` is null.

``exit_code``, ``error``
   The process exit status and, exactly when it is nonzero, an object with a
   stable ``code`` and a human-readable ``message``. Otherwise ``error`` is
   null.

``workflow``
   ``wf_uuid``, ``root_wf_uuid``, ``submit_dir``, label, owner, and DAG name.
   ``is_sub_workflow`` is true when the target is itself a subworkflow; the
   label, owner, DAG name, and submit directory are then the subworkflow's
   own, not the root's.
   ``sub_workflows`` counts and names the subworkflow jobs of this workflow;
   ``aggregated`` is always false because their jobs are not included.

``sources``
   One entry each for ``stampede``, ``live_tail``, ``condor_queue``,
   ``condor_history``, ``condor_pool``, ``condor_priority``, and
   ``condor_negotiator``, with its health ``state``, ``error_code``, and
   ``queried``. The state ``disabled`` means the source was turned off, for
   example by ``--no-condor``; ``not_queried`` means this mode did not need
   it. An empty result from a source that is not ``healthy`` means the monitor
   could not look, not that there was nothing to find.

``summary``
   Workflow state, status, start and end times, completion, removal reason,
   and ``job_counts`` by lifecycle. Null without an authoritative snapshot.
   ``job_counts`` is what Stampede recorded, as ``job_counts_source`` says:
   it trails HTCondor by the time pegasus-monitord takes to record a state,
   and keeps the last recorded state of a job that was removed, so it can
   show no held job beside a hold finding, or a queued job in a workflow that
   has ended. ``observed_job_counts`` corrects for both. When ``sources``
   shows that ``condor_queue`` or ``condor_history`` was queried, an
   unfinished job is counted under the lifecycle the queue shows; and a job
   that HTCondor removed, or that was unfinished when its workflow ended, is
   counted as ``removed``.

``stats``
   The statistics of the text report. Unknown values are omitted.

``stall``
   The one-shot stall assessment described below.

``snapshot``, ``diagnostics``, ``why_idle``
   The sections of ``--once``, ``--diagnose``, and ``--why-idle``. A section
   whose mode was not requested is null. A requested section that could not be
   produced is ``{"available": false, "reason": ...}``.

``stream``
   For replay and remote origins, the stream's ``stop_reason`` and whether the
   recorder stopped it while the workflow may still have been running.

``analysis_errors``
   Names of analyses that failed, as in the text report.

``--once`` fills ``snapshot`` with the workflow record, every job with all of
its attempts, and the most recent events. It uses the same serialization as
checkpoints in the event stream and adds ``lifecycle`` and ``is_compute`` to
each job. Every job is listed regardless of ``--all-jobs``; ``--events``
limits ``events``, and ``events_omitted`` counts the rest.

::

   $ pegasus-monitor --once --json run0001
   {"format":"pegasus-monitor-json","format_version":1,"modes":["once"],
    "origin":"live","exit_code":0,"error":null,
    "workflow":{"wf_uuid":"...","root_wf_uuid":"...","submit_dir":"/w/run0001",
                "sub_workflows":{"count":0,"aggregated":false,...},...},
    "sources":[{"source":"stampede","state":"healthy","queried":true,...},...],
    "summary":{"workflow_state":"WORKFLOW_STARTED","workflow_status":null,
               "job_counts":{"total":4,"queued":1,"held":1,"failed":1,...},...},
    "snapshot":{"workflow":{...},
                "jobs":[{"exec_job_id":"analyze_ID0000002","lifecycle":"held",
                         "is_compute":true,"attempts":[{...}],...},...],
                "events":[...],"events_omitted":0},
    "stats":{...},"stall":{...},"diagnostics":null,"why_idle":null,...}

``--diagnose`` fills ``diagnostics.findings``. Each finding has a ``target``
(workflow and ``exec_job_id``), a ``category`` of ``held`` or ``failed``, a
``severity``, a stable ``code``, a ``summary``, ``suggestions``, and the
bounded, redacted ``evidence`` behind it. Each evidence item names the
``source`` it was read from. The hold reason comes from ``condor_queue`` when
the queue was queried and from ``stampede`` otherwise, so ``--no-condor``
classifies a hold from the reason pegasus-monitord recorded; evidence never
cites a source that was not queried. Hold codes are ``user_hold``,
``output_missing``, ``output_transfer_failed``, ``input_transfer_failed``,
``memory_exceeded``, ``disk_exceeded``, ``walltime_exceeded``,
``credential_failure``, ``shadow_exception``, ``dns_failure``,
``container_failure``, ``policy_hold``, and ``held_unknown``. Failure codes are
``exit_code_<N>``, ``exit_code_unknown``, and ``job_removed``.

::

   $ pegasus-monitor --diagnose --json run0001
   {"format":"pegasus-monitor-json","format_version":1,"modes":["diagnose"],...,
    "snapshot":null,
    "diagnostics":{"available":true,"reason":null,"origin":"analysis","errors":[],
      "findings":[{"target":{"workflow":{...},"exec_job_id":"analyze_ID0000002",
                             "job_provenance":"db_confirmed"},
                   "category":"held","severity":"warning","code":"memory_exceeded",
                   "summary":"Job exceeded its memory limit: ...",
                   "suggestions":["Increase the job memory request if the observed use is expected.",
                                  "Inspect the executable for unexpectedly high memory growth."],
                   "evidence":[{"source":"condor_queue","code":"hold_reason",
                                "payload":{"hold_reason_code":34,...}},...]}]},...}

``--why-idle`` fills ``why_idle`` with structured fields: the idle jobs,
``pool`` capacity, ``resource_fit.mismatches`` for jobs that fit no available
slot, the owner's fair-share ``priority``, the last ``negotiator`` cycle, the
``findings`` and ``suggestions`` of the text report, and the ``sources`` the
analysis could use. ``findings`` and ``suggestions`` hold every such line of
the text report, including the one it prints for each resource mismatch, with
repeated suggestions listed once. ``pool.total_slots`` counts slots as
``condor_status`` lists them, a partitionable slot included;
``pool.idle_slots`` counts unclaimed slots, a partitionable slot only while
it has CPUs left to hand out. A partitionable slot whose CPUs are all handed
out is therefore not counted idle, although ``condor_status`` lists it as
Unclaimed and Idle. ClassAd expressions such as ``requirements``
are given as the expression text. ``idle_jobs`` and ``mismatches`` list at most 1000 entries
each, with the remainder counted in ``idle_jobs_omitted`` and
``mismatches_omitted``.

::

   $ pegasus-monitor --why-idle --json run0001
   {"format":"pegasus-monitor-json","format_version":1,"modes":["why_idle"],...,
    "why_idle":{"available":true,"reason":null,"queued_jobs":1,"unsubmitted_jobs":1,
      "idle_jobs":[{"exec_job_id":"preprocess_ID0000001","state":"queued",...}],
      "pool":{"machines":2,"total_cpus":12,"idle_cpus":6,...},
      "resource_fit":{"mismatches":[{"exec_job_id":"preprocess_ID0000001",
                                     "requested_cpus":64,
                                     "reason":"exceeds_single_slot_capacity",...}],
                      "mismatches_omitted":0},
      "priority":{"effective_priority":12.5,"better_priority_users":1,...},
      "negotiator":{"last_cycle_seconds":4.5,"last_cycle_matches":12},
      "findings":[...],"suggestions":[...],
      "sources":[{"kind":"queue","disposition":"healthy",...},...]},...}

The modes may be combined in one invocation. With ``--replay`` or ``--remote``
the document is built from the recorded stream: ``diagnostics.origin`` is
``event_log`` and each finding's ``category`` and ``suggestions`` are null
because the stream does not record them, and ``why_idle`` is unavailable
because the stream holds no scheduler evidence.

**Stall assessment.** The live stall detector is stateful: it times silence on
a monotonic clock and confirms a condition over two refreshes, so a single
sample can never report a confirmed stall. One-shot JSON therefore reports a
separate, stateless assessment. It takes the newest state change recorded for
the workflow or any job and measures its age against the local wall clock.
``stall.method`` is ``snapshot_timestamps`` and ``stall.confirmed`` is always
false. For replay and remote origins it describes the recorded snapshot as of
``observed_at_epoch``, not the present.

pegasus-monitord records state seconds to minutes after HTCondor acts, so the
recorded timestamps alone make a workflow look quiet when it is not. For a
live workflow the assessment therefore also reads the modification times of
the workflow's own ``<dag>.dagman.out`` and ``<dag>.nodes.log`` in its submit
directory, a sub-workflow's in its own directory. DAGMan writes the first as
it submits jobs, reads their events, and is removed; the schedd writes job
events to the second as they happen, whether or not DAGMan is alive.
``stall.last_progress_epoch`` and ``seconds_since_progress`` remain the
recorded state change. ``stall.last_activity_epoch`` is the later of that and
the newest of the two files, ``seconds_since_activity`` its age, and
``activity_source`` names which it was: ``stampede``, ``dagman_out``, or
``nodes_log``. Every time threshold below is applied to
``seconds_since_activity``. A file that is missing or cannot be read is
ignored, and with neither file the assessment uses the recorded timestamps
alone.

The result depends on the monitoring host's clock agreeing with the recorded
timestamps and, where the submit directory is on a shared filesystem, with
the file server's clock. A file time later than the local clock is treated as
the present, so a file server that runs ahead delays a stall verdict by the
difference between the clocks; it does not produce a negative age.

Because DAGMan writes ``<dag>.dagman.out`` a few seconds after the event it
reports, a stall is first reported that much later than the recorded
timestamps alone would report it. HTCondor also documents that DAGMan, after
``DAGMAN_PENDING_REPORT_INTERVAL`` seconds (600 by default) without a job
event, writes a report of its pending nodes to that file. Each such report
restarts the quiet period: a workflow already reported as ``idle_too_long``
or ``no_semantic_progress`` reads ``within_thresholds`` again for
``no_progress_seconds`` after it, and reaches ``idle_too_long`` again after
``idle_threshold_seconds``.

The assessment counts jobs as ``observed_job_counts`` does, so a hold that
only the HTCondor queue knows of yet is seen at once. ``stall.job_counts``
remains the recorded counts and ``stall.observed_job_counts`` the ones the
verdict used. A job is *in flight* when it is in a pre script, queued,
running, held, or in a post script, and *unfinished* until it has succeeded
or failed.

``stall.scheduler`` describes the queue result the verdict used:

``queue_queried``
   A healthy queue result was available. The fields below are null without
   one.

``queue_complete``
   The result did not reach its row limit. When false, rows may be missing
   and the absence of a job proves nothing.

``dagman_in_queue``, ``dagman_cluster_id``
   Whether the queue shows the workflow's own DAGMan job, and its cluster. For
   a top-level workflow that is the scheduler-universe job that no DAGMan
   submitted. A sub-workflow's DAGMan is a job of its parent workflow: it is
   the job that the sub-workflow's queued jobs name as their DAGMan or, when
   none is queued, the DAGMan job started in the sub-workflow's submit
   directory. ``dagman_in_queue`` is null when the result is incomplete and
   does not show the job.

``dagman_is_wrapper``
   The DAGMan job is ``pegasus-dagman``, which runs ``condor_dagman`` as a
   child process and can stay in the queue after ``condor_dagman`` has died.
   When true, ``dagman_in_queue`` shows that the wrapper is alive, not
   DAGMan. It is false for a sub-workflow, whose queue job is
   ``condor_dagman`` itself.

``jobs_in_queue``
   How many jobs of the workflow the queue shows. A running sub-workflow
   counts as one job of its parent. The DAGMan jobs found for a sub-workflow
   are not its jobs and are counted nowhere else in the document.

``recorded_jobs_missing``
   How many jobs are recorded as queued, running or held but are not in a
   complete queue result. Jobs in pre or post scripts are not in the queue
   and are never counted. Null when the result is incomplete.

The conditions are applied in this order:

``no_scheduler_activity``
   Jobs are unfinished, ``seconds_since_activity`` has reached
   ``no_progress_seconds``, and one of the following holds. This is a
   workflow whose DAGMan has died, is held, or has stopped submitting, as
   opposed to one that is slow. The live detector does not report this kind.

   * A queried queue shows neither the workflow's DAGMan job nor any job of
     the workflow. ``scheduler.dagman_in_queue`` is false.
   * A complete queue result shows the DAGMan job but no job of the workflow,
     although ``recorded_jobs_missing`` jobs are recorded as queued, running
     or held. ``dagman_in_queue`` is true, ``jobs_in_queue`` is 0, and
     ``summary`` names the DAGMan job. With ``dagman_is_wrapper`` true this
     is ``condor_dagman`` killed behind its wrapper. An incomplete or
     unhealthy queue result never satisfies this condition, and neither does
     a monitord that is merely behind, because a working DAGMan keeps writing
     its files.
   * No job is in flight.

   Two cases are not detected. While a job orphaned by a dead DAGMan is
   still in the queue the workflow reads ``jobs_running``, which one sample
   cannot tell from a long job; it is reported ``no_progress_seconds`` after
   that job's last event. A workflow whose only jobs in flight are recorded
   in pre or post scripts reads ``within_thresholds`` for as long as that is
   recorded, whether or not DAGMan is alive.

``all_held``
   At least one job is in flight and every job in flight is held. Jobs still
   unsubmitted behind the held ones do not prevent it. No time threshold
   applies.

``jobs_running`` (not stalled)
   A job is running. One sample cannot tell a long job from a stuck one, so a
   running job is never reported as a stall, however long it has run, and the
   live detector's ``running_plateau`` is not reported here.
   ``seconds_since_progress`` still gives the age of the last state change.

``idle_too_long``, ``no_semantic_progress``
   Jobs are queued and none is running, with ``seconds_since_activity`` at
   ``idle_threshold_seconds`` or ``no_progress_seconds`` respectively. The
   conditions and thresholds are the live detector's.

``stall.assessed`` separates "not stalled" from "not assessed". When it is
false, ``stall.stalled`` is null and ``stall.reason`` is
``authoritative_snapshot_unavailable``, ``stampede_not_healthy``, or
``progress_time_unavailable``. When it is true and ``stalled`` is false, the
reason is ``workflow_complete``, ``no_jobs_submitted``, ``startup_grace``,
``jobs_running``, or ``within_thresholds``. When ``stalled`` is true,
``reason`` and ``kind`` both name the condition.

Error codes and their exit statuses:

``usage_error`` (2)
   Invalid arguments or mode combination. The usage text is also printed to
   standard error.

``workflow_location_error`` (1)
   The target could not be resolved to one workflow run.

``no_authoritative_snapshot`` (1)
   No Stampede snapshot could be read. ``workflow`` and ``sources`` are still
   filled, so the document shows which source failed.

``no_complete_checkpoint`` (1), ``invalid_event_log`` (1)
   A replayed or remote stream had no usable checkpoint, or was malformed.

``io_error`` (1), ``cleanup_failed`` (1), ``monitor_error`` (1)
   A file or source could not be read, a source failed to close, or another
   runtime failure occurred.

``interrupted`` (1)
   The command was interrupted or terminated before it had a result. This
   differs from the text modes, which return 0 on a clean interrupt: a
   document without a result is reported as an error.


Return Value
============

The command returns 0 after a successful one-shot report, authoritative live
completion, successful server launch or stop, help/version request, or clean
interrupt. It returns 1 for target resolution, source initialization, replay,
remote, or server runtime errors, non-TTY live invocation, or an unavailable
authoritative checkpoint or Stampede snapshot in ``--once`` mode. Argument
errors and invalid mode combinations return the standard argparse status 2.

With ``--json`` the same statuses apply and are repeated in the document's
``exit_code``, with one exception: an interrupt before any result returns 1.
See `JSON Output`_ for the error codes.


Examples
========

::

   pegasus-monitor /workflows/montage/run0001
   pegasus-monitor --once --no-condor /workflows/montage/run0001
   pegasus-monitor --diagnose /workflows/montage/run0001
   pegasus-monitor --why-idle --collector collector.example.org run0001
   pegasus-monitor --diagnose --json /workflows/montage/run0001
   pegasus-monitor --once --diagnose --why-idle --json run0001 > report.json
   pegasus-monitor /workflows/montage/run0001 --log
   pegasus-monitor --serve --diagnose /workflows/montage/run0001
   pegasus-monitor --remote user@submit.example:/workflows/montage/run0001/workflow-events.jsonl
   pegasus-monitor --remote user@submit.example:/workflows/montage/run0001/workflow-events.jsonl --once
   pegasus-monitor --replay workflow-events.jsonl --speed 0
   pegasus-monitor /workflows/montage/run0001 --stop-server
   pegasus-monitor --stop-server /logs/.montage-events.jsonl.pid


Related Commands
================

``pegasus-status --watch`` remains supported, unchanged, and not deprecated.
It provides the established queue and DAG progress view. **pegasus-monitor** is
the richer single-workflow live view, with hybrid Stampede/live-event state,
source health, diagnostics, and optional bounded scheduler evidence.
