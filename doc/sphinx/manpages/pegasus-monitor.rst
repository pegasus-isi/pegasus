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
   source failures degrade the report instead of changing workflow state.

**--no-condor**
   Disable all HTCondor executable and binding use. Stampede and live event
   monitoring continue normally.

**--no-live-events**
   Disable the ``jobstate.log`` overlay and poll Stampede only.

**--jobstate-path** *PATH*
   Use an explicit live event file instead of the resolved ``jobstate.log``.

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


Return Value
============

The command returns 0 after a successful one-shot report, authoritative live
completion, help/version request, or clean interrupt. It returns 1 for target
resolution, source initialization, runtime errors, non-TTY live invocation, or
an unavailable authoritative Stampede snapshot in ``--once`` mode. Argument
errors return the standard argparse status 2.


Examples
========

::

   pegasus-monitor /workflows/montage/run0001
   pegasus-monitor --once --no-condor /workflows/montage/run0001
   pegasus-monitor --diagnose /workflows/montage/run0001
   pegasus-monitor --why-idle --collector collector.example.org run0001


Related Commands
================

``pegasus-status --watch`` remains supported, unchanged, and not deprecated.
It provides the established queue and DAG progress view. **pegasus-monitor** is
the richer single-workflow live view, with hybrid Stampede/live-event state,
source health, diagnostics, and optional bounded scheduler evidence.
