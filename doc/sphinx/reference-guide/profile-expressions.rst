
.. _profile-expressions:

===================
Profile Expressions
===================

Pegasus supports **profile expressions** — Python expressions that are
evaluated when a job fails and is about to be retried. These expressions
allow you to dynamically change resource requirements (memory, cores, GPUs,
runtime, queue, project, etc.) based on the actual execution metrics
collected from the previous attempt, enabling smarter retry strategies
without manual intervention.

.. note::

   Profile expressions are evaluated only on job failure, before the job
   is resubmitted by DAGMan. They have no effect on the first attempt.

Overview
========

When a job fails, Pegasus runs ``pegasus-exitcode`` to determine whether
the job should be retried. If profile expressions have been declared for
the job, ``pegasus-exitcode`` evaluates them against runtime metrics
collected from the failed attempt (via kickstart records and PegasusLite
output) and rewrites the relevant entries in the HTCondor submit file
before the job is resubmitted.

The workflow for expression-based retry looks like this:

1. A job fails.
2. ``pegasus-exitcode`` is invoked by the post-script.
3. Kickstart and PegasusLite output from the failed run is parsed into a
   *symbol table* of runtime variables.
4. Each profile expression is evaluated using the symbol table.
5. Matching entries in the HTCondor submit file are updated in-place.
6. DAGMan retries the job using the updated submit file.

Specifying Profile Expressions
===============================

Expression-based profiles are declared alongside their corresponding
plain profiles. For each supported resource profile key, a corresponding
``*_expr`` variant exists. You set the expression as a Python string on
the job or transformation.

Python API
----------

Use ``add_pegasus_profile()`` with the ``*_expr`` keyword arguments:

.. code-block:: python

   from Pegasus.api import *

   j = Job("myapp")
   j.add_pegasus_profile(
       memory="1 GB",
       memory_expr="pegasus_memory_mb * 2 if job_retry > 0 else pegasus_memory_mb",
       runtime="3600",
       runtime_expr="pegasus_job_runtime * 2 if job_retry > 0 else pegasus_job_runtime",
   )

YAML Workflow
-------------

In a YAML workflow file the ``*.expr`` suffix is appended to the profile
key name:

.. code-block:: yaml

   jobs:
     - type: job
       name: myapp
       id: ID0000001
       arguments: []
       profiles:
         pegasus:
           memory: "1024"
           memory.expr: "pegasus_memory_mb * 2 if job_retry > 0 else pegasus_memory_mb"
           runtime: "3600"
           runtime.expr: "pegasus_job_runtime * 2 if job_retry > 0 else pegasus_job_runtime"

Supported Expression Profile Keys
----------------------------------

The following ``pegasus`` namespace profile keys have a corresponding
``*_expr`` variant:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Profile Key
     - Expression Key
     - Description
   * - ``runtime``
     - ``runtime.expr``
     - Expected runtime in seconds
   * - ``memory``
     - ``memory.expr``
     - Memory requested (e.g. ``"2 GB"``)
   * - ``diskspace``
     - ``diskspace.expr``
     - Disk space requested (e.g. ``"10 GB"``)
   * - ``cores``
     - ``cores.expr``
     - Number of CPU cores
   * - ``gpus``
     - ``gpus.expr``
     - Number of GPUs
   * - ``queue``
     - ``queue.expr``
     - Batch queue name
   * - ``project``
     - ``project.expr``
     - Allocation/project name for the batch system
   * - ``glite.arguments``
     - ``glite.arguments.expr``
     - Extra arguments passed to BLAHP/glite

Expression Syntax
=================

Expressions are standard Python expressions. They are evaluated using
Python's built-in ``eval()``, so any valid Python expression is
supported. The full symbol table (see :ref:`expression-variables` below)
is available as local variables inside the expression.

Common patterns include:

**Ternary (conditional) expressions** — the most common pattern:

.. code-block:: python

   # Double memory on every retry
   "pegasus_memory_mb * 2 if job_retry > 0 else pegasus_memory_mb"

   # Triple memory after the second retry
   "pegasus_memory_mb * 3 if job_retry >= 2 else pegasus_memory_mb * 2"

   # Switch to a longer queue once the job has run for more than an hour
   '"long" if duration > 3600 else "short"'

   # Increase runtime estimate proportionally to actual observed runtime
   "int(duration * 1.5) if job_retry > 0 else pegasus_job_runtime"

**Arithmetic expressions**:

.. code-block:: python

   # Add one extra core per retry
   "pegasus_cores + job_retry"

   # Scale memory by actual RSS observed
   "max(pegasus_memory_mb, int(maxrss * 1.25))"

**String expressions** (for queue/project):

.. code-block:: python

   # Use "debug" queue if the job ran at all, otherwise "long"
   '"long" if duration > 0 else "debug"'

.. note::

   Expressions that produce string values for resource profiles (e.g.
   ``memory``) should include the unit suffix, e.g. ``"2048 MB"`` or
   ``"2 GB"``, matching the format accepted by the corresponding plain
   profile key. For numeric profiles like ``cores`` and ``gpus``, return
   a plain integer.

.. _expression-variables:

Available Variables
===================

The symbol table available to expressions contains variables derived
from three sources:
* Pegasus ClassAd values written to the submit file
* runtime data recorded by kickstart, and
* execution information captured by PegasusLite.

The convenience class ``ExprVar`` in ``Pegasus.api.mixins`` lists all
available variable names with their descriptions:

.. code-block:: python

   from Pegasus.api.mixins import ExprVar

   # ExprVar attributes are the variable name strings for use in expressions
   # e.g. ExprVar.job_retry == "job_retry"

Workflow and Job Identity Variables
------------------------------------

These variables are set by the Pegasus planner when the submit file is
generated. They are available in all expression evaluations.

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Variable
     - Type
     - Description
   * - ``pegasus_generator``
     - ``str``
     - Generator used to create the workflow (default: ``"Pegasus"``)
   * - ``pegasus_root_wf_uuid``
     - ``str``
     - UUID of the root workflow
   * - ``pegasus_wf_uuid``
     - ``str``
     - UUID of the current (sub-)workflow
   * - ``pegasus_version``
     - ``str``
     - Pegasus version string
   * - ``pegasus_wf_name``
     - ``str``
     - Workflow name
   * - ``pegasus_wf_xformation``
     - ``str``
     - Transformation in ``namespace::name:version`` format
   * - ``pegasus_wf_dax_job_id``
     - ``str``
     - ID of the associated compute job in the input workflow
   * - ``pegasus_job_class``
     - ``int``
     - Integer job class identifier
   * - ``pegasus_site``
     - ``str``
     - Name of the site the job ran on

Requested Resource Variables
------------------------------

These reflect the resource requirements that were set for the *previous*
attempt — i.e. the values that just failed. They are useful as a
baseline for scaling expressions.

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Variable
     - Type
     - Description
   * - ``pegasus_job_runtime``
     - ``int``
     - Requested runtime in seconds
   * - ``pegasus_cores``
     - ``int``
     - Number of cores requested
   * - ``pegasus_gpus``
     - ``int``
     - Number of GPUs requested
   * - ``pegasus_memory_mb``
     - ``int``
     - Memory requested in megabytes
   * - ``pegasus_diskspace_mb``
     - ``int``
     - Disk space requested in megabytes
   * - ``pegasus_cluster_size``
     - ``int``
     - Number of jobs in the cluster (set only for clustered jobs)

Runtime Execution Variables
-----------------------------

These variables are populated from the actual execution record of the
failed attempt. ``job_runtime`` comes from PegasusLite output; the
remaining variables come from kickstart records.

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Variable
     - Type
     - Description
   * - ``job_retry``
     - ``int``
     - Current retry number (``0`` on the first attempt, ``1`` on the
       first retry, etc.)
   * - ``job_runtime``
     - ``int``
     - Actual wall-clock runtime in seconds as logged by PegasusLite.
       Falls back to the kickstart ``duration`` if PegasusLite output
       is not available.
   * - ``exitcode``
     - ``int``
     - Exit code of the job as recorded in the kickstart record
   * - ``duration``
     - ``int``
     - Job duration in seconds from the kickstart record
   * - ``user``
     - ``str``
     - Username under which the job ran
   * - ``hostaddr``
     - ``str``
     - Host address of the node where the job ran
   * - ``maxrss``
     - ``int``
     - Maximum resident set size (memory) used by the job
   * - ``total_ip_size_mb``
     - ``int``
     - Total size of declared input files in megabytes
   * - ``total_op_size_mb``
     - ``int``
     - Total size of declared output files in megabytes

.. note::

   Runtime execution variables (``job_runtime``, ``exitcode``,
   ``duration``, ``maxrss``, etc.) require a kickstart record to be
   present in the job's ``.out`` file. If kickstart is not used, or the
   job failed before producing output, these variables may be missing
   from the symbol table, causing the expression to raise a
   ``NameError`` which is logged and the expression is skipped.

Examples
========

Double Memory on Retry
-----------------------

A common cause of job failure on HPC systems is running out of memory.
This example doubles the memory allocation on each retry, up to a
reasonable cap:

.. code-block:: python

   from Pegasus.api import *

   j = Job("simulate")
   j.add_pegasus_profile(
       memory="2 GB",
       # Double memory each retry; cap at 8 GiB (8192 MB)
       memory_expr="min(pegasus_memory_mb * 2, 8192)",
   )

Switch Queue Based on Runtime
------------------------------

Some batch systems route short jobs to a ``debug`` queue and longer jobs
to a normal queue. If a job fails because it exceeded the debug queue
walltime, this expression switches to the longer queue on retry:

.. code-block:: python

   from Pegasus.api import *

   j = Job("long_analysis")
   j.add_pegasus_profile(
       queue="debug",
       # If the job ran for more than 30 minutes, move to the long queue
       queue_expr='"long" if duration > 1800 else "debug"',
       runtime="1800",
       # On retry, request twice the observed runtime
       runtime_expr="int(duration * 2) if job_retry > 0 else pegasus_job_runtime",
   )

Scale Resources Based on Input Size
-------------------------------------

When input file sizes are highly variable, it may be appropriate to
scale memory based on observed input:

.. code-block:: python

   from Pegasus.api import *

   j = Job("process_data")
   j.add_pegasus_profile(
       memory="4 GB",
       # Request 2x the total input size in MB, with a 1 GB floor
       memory_expr="max(int(total_ip_size_mb * 2), 1024)",
   )

Increase Cores Per Retry
-------------------------

For jobs that can benefit from more parallelism when they fail:

.. code-block:: python

   from Pegasus.api import *

   j = Job("parallel_job")
   j.add_pegasus_profile(
       cores=4,
       # Add one extra core per retry, up to 16
       cores_expr="min(pegasus_cores + 1, 16)",
   )

Using ExprVar for Readable Expressions
-----------------------------------------

The ``ExprVar`` class provides named constants for all variables to
avoid spelling mistakes and enable IDE autocompletion:

.. code-block:: python

   from Pegasus.api import *
   from Pegasus.api.mixins import ExprVar

   j = Job("myapp")

   # Build expressions using ExprVar constants as variable name strings
   mem_expr = (
       f"str({ExprVar.pegasus_memory_mb} * 2) + ' MB' "
       f"if {ExprVar.job_retry} > 0 "
       f"else str({ExprVar.pegasus_memory_mb}) + ' MB'"
   )
   # mem_expr == "str(pegasus_memory_mb * 2) + ' MB' if job_retry > 0 else str(pegasus_memory_mb) + ' MB'"

   j.add_pegasus_profile(
       memory="2 GiB",
       memory_expr=mem_expr,
   )

Relationship to HTCondor ClassAd Expressions
=============================================

Pegasus profile expressions are distinct from HTCondor ClassAd
expressions, which use HTCondor's own expression language. Both
mechanisms can be used to adjust resource requirements on retry, but
they operate differently:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * -
     - **Pegasus Profile Expressions**
     - **HTCondor ClassAd Expressions**
   * - Language
     - Python
     - HTCondor ClassAd language
   * - Evaluated by
     - ``pegasus-exitcode`` (post-script)
     - HTCondor schedd / startd
   * - When applied
     - After job failure, before resubmission
     - Dynamically at match-time (no resubmission needed)
   * - Variables available
     - All ``ExprVar`` runtime metrics
     - HTCondor ClassAd attributes (e.g. ``DAGNodeRetry``)
   * - How to declare
     - ``add_pegasus_profile(memory_expr=...)``
     - ``add_condor_profile(request_memory=...)`` with a ClassAd expr

An example of the HTCondor ClassAd approach for comparison (set in the
site catalog):

.. code-block:: yaml

   profiles:
     condor:
       request_memory: "ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, 1024, 4096)"

This uses HTCondor's built-in ``ifthenelse`` function and the
``DAGNodeRetry`` attribute set by DAGMan. It is simpler for
memory-only cases but does not have access to the richer set of
runtime metrics available to Pegasus profile expressions.

For most use cases involving adaptive resource scaling on retry, Pegasus
profile expressions are recommended because they provide access to
actual observed execution metrics such as ``maxrss``, ``duration``, and
``total_ip_size_mb``.

Implementation Notes
====================

- Profile expression keys ending in ``.expr`` are written to the
  HTCondor submit file as plain submit-file variables (e.g.
  ``pegasus_memory_expr = "..."``) rather than as ClassAd attributes.
  This keeps them invisible to HTCondor but accessible to
  ``pegasus-exitcode`` during post-processing.

- Expression evaluation requires the ``PythonSed`` package to be
  installed on the submit host. If ``PythonSed`` is not available,
  ``pegasus-exitcode`` will log a warning and skip expression
  application.

- Expressions are only applied when the job fails (i.e. when
  ``pegasus-exitcode`` determines the job should be retried).
  Successful jobs are not subject to expression evaluation.

- The ``pegasus-exitcode`` flag ``-U`` (``--update-submit-file``)
  controls whether expression application is attempted. Pegasus sets
  this flag automatically in generated post-scripts.

- If an expression raises an error during evaluation (e.g. a variable
  is missing from the symbol table), a warning is logged and that
  expression is skipped; the remaining expressions are still applied.
