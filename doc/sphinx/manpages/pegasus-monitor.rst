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
   unknown rather than treated as workflow failure.

**--remap-submit-dir** {**auto**, **always**, **never**}
   Control remapping of paths recorded at planning time onto the local
   ``braindump.yml`` location. The default is **auto**.

**--diagnose**
   Add bounded failure, hold, kickstart, and stall diagnostics. Diagnostic
   source failures degrade the report instead of changing workflow state. When
   event logging is active, redacted diagnostic results are also recorded as
   derived evidence; they never change replayed workflow or job state.

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
   Supply the adjacent hidden PID file explicitly for a server using a custom
   log path.

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
while it displays a provisional ``jobstate.log`` overlay.

Additional DB-confirmed checkpoints are written periodically, every five
minutes by default, and after recovery, database replacement or structural
change, and authoritative workflow completion. A disk, size, or writer gap
invalidates subsequent incremental reconstruction until the next checkpoint.
When safe writing resumes, the writer records the gap and a recovery
checkpoint. A replaced output file starts a new stream with a new
``stream_id`` and complete checkpoint.

Event logs, server metadata, and singleton state use restrictive permissions
and reject unsafe symlink or non-regular-file targets. Logging failures and
capacity guards do not modify or stop the workflow.


Replay, Server, and Remote Modes
================================

Replay and remote consumers use the same typed schema and renderer. They
tolerate an incomplete trailing line, reject unsupported schemas, and discard
incomplete post-gap state until a checkpoint restores authority. Stream
replacement is detected by ``stream_id``, not file size. Replay never opens
workflow sources. Remote mode validates the SSH target and path, invokes SSH
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
``--replay``, ``--remote``, or ``--stop-server``. Server modes cannot be
combined with ``--once`` or ``--why-idle``. ``--ssh-config`` and
``--ssh-identity`` require ``--remote``.


Return Value
============

The command returns 0 after a successful one-shot report, authoritative live
completion, successful server launch or stop, help/version request, or clean
interrupt. It returns 1 for target resolution, source initialization, replay,
remote, or server runtime errors, non-TTY live invocation, or an unavailable
authoritative checkpoint or Stampede snapshot in ``--once`` mode. Argument
errors and invalid mode combinations return the standard argparse status 2.


Examples
========

::

   pegasus-monitor /workflows/montage/run0001
   pegasus-monitor --once --no-condor /workflows/montage/run0001
   pegasus-monitor --diagnose /workflows/montage/run0001
   pegasus-monitor --why-idle --collector collector.example.org run0001
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
