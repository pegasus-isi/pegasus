.. _nersc-sfapi-submission:

=============================
NERSC SFAPI Remote Submission
=============================

Overview
========

The SFAPI integration enables Pegasus/HTCondor to submit jobs to the
`Perlmutter <https://docs.nersc.gov/systems/perlmutter/>`_ supercomputer at NERSC without
a shared filesystem between the submit host and the remote cluster.

The integration is implemented as a BLAHP (Batch Local ASCII Helper Protocol) plugin — a
set of shell scripts that sit between HTCondor's Grid Universe and the remote batch system.
The `NERSC Superfacility API (SFAPI) <https://github.com/NERSC/sfapi_client>`_ Python client
handles all communication with Perlmutter: file upload/download, job submission, status
queries, and cancellation.

Since there is **no shared filesystem** between the submit host and Perlmutter, all files are
transferred explicitly:

- **Upload** (submit time): executable, input sandbox files, and stdin — via NERSC DTNs
  (Data Transfer Nodes).
- **Download** (job completion): stdout, stderr, and output sandbox files — via the SFAPI
  download API.

Architecture
============

.. code-block:: text

    HTCondor / Pegasus submit host
            |
            |  Grid Universe (batch_system = sfapi)
            v
       blahpd daemon
            |
       |────|──────────────────|
       │  BLAHP Shell Scripts  │
       │  sfapi_submit.sh      │  ──> sfapi_helpers.py submit
       │  sfapi_status.sh      │  ──> sfapi_helpers.py status / download
       │  sfapi_cancel.sh      │  ──> sfapi_helpers.py cancel
       │  sfapi_ping.sh        │  ──> sfapi_helpers.py status --type resource
       |───────────────────────|
                    |
             NERSC SFAPI (HTTPS)
                    |
            |───────|────────|
            │   Perlmutter   │
            │  /pscratch/…   │
            │  Slurm queue   │
            |────────────────|

The BLAHP scripts are installed alongside other batch plugin scripts in the Pegasus
``glite/`` directory. They are selected by HTCondor when ``batch_system = sfapi`` is set in
the site catalog or job ClassAd.

Components
==========

``sfapi_helpers.py``
--------------------

The Python module that wraps the ``sfapi_client`` library. All BLAHP shell scripts invoke it
as a CLI tool. It exposes four subcommands and a set of helper functions.

Credentials
^^^^^^^^^^^

Credentials are read from ``~/.superfacility/`` by ``load_sflapi_client_secret()``:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - File
     - Purpose
   * - ``~/.superfacility/clientid.txt``
     - SFAPI client ID (plain text, whitespace-stripped)
   * - ``~/.superfacility/priv_key.jwk``
     - JWK-encoded RSA private key (JSON Web Key format)

The function sets module-level globals ``client_id`` and ``client_secret`` that all helper
functions use when constructing an ``sfapi_client.Client`` instance. File permissions on
``~/.superfacility/`` are enforced to ``0600`` at load time.

Key Functions
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Function
     - Description
   * - ``load_sflapi_client_secret()``
     - Reads credentials from ``~/.superfacility/``, sets module globals, returns
       ``(client_id, client_secret)``
   * - ``submit_remote_slurm_job(job_name, job_script, input_files)``
     - Creates remote scratch directory, uploads input files, prepends
       ``#SBATCH --output/--error/--chdir`` directives, submits the job; returns
       ``(job_id, remote_stdout, remote_stderr)``
   * - ``create_remote_blahp_directory(name)``
     - Creates ``$PSCRATCH/.blah/<name>`` via NERSC DTNs; returns the absolute remote path
   * - ``upload_file(directory, file)``
     - Uploads a local file to a remote directory on Perlmutter via NERSC DTNs
   * - ``download_file(source, destination)``
     - Downloads a remote file to a local path; raises ``SfApiHelperError`` if the remote
       path does not exist or is not a file
   * - ``download_job_outputs(blahp_job_id)``
     - Reads the jobstate file for the given blahp job ID and downloads all listed remote
       files to their local destination paths
   * - ``check_nersc_status(resource_name)``
     - Checks a NERSC resource status via the public SFAPI; raises ``RuntimeError`` if not
       active
   * - ``check_job_status(jobid)``
     - Prints ``"Job <id> state: <state>"`` for the given Slurm job ID
   * - ``cancel_job(job_id)``
     - Cancels a job by bare numeric Slurm ID or blahp ID (``sfapi/<date>/<jobid>``)

CLI Subcommands
^^^^^^^^^^^^^^^

.. code-block:: console

    python3 sfapi_helpers.py submit  -n NAME -i FILE1,FILE2,... -s SCRIPT_PATH
    python3 sfapi_helpers.py status  -t resource|job -v VALUE
    python3 sfapi_helpers.py download BLAHP_JOB_ID
    python3 sfapi_helpers.py cancel  JOB_ID

``sfapi_setup.sh``
------------------

Sourced by every BLAHP script before any Python invocation. Activates the
``~/superfacility-env`` Python virtual environment, which must contain the ``sfapi_client``
package. If the virtual environment is missing, the script prints installation instructions
and exits non-zero.

.. code-block:: bash

    . `dirname $0`/sfapi_setup.sh

``sfapi_submit.sh``
-------------------

The main submission script, called by ``blahpd`` when a new HTCondor job arrives.

Flow
^^^^

1. **Parse blahp options** via ``bls_parse_submit_options`` (from
   ``blah_common_submit_functions.sh``).

2. **Set up temp files** via ``bls_setup_temp_files``. The full ``bls_setup_all_files`` is
   deliberately avoided because stdin/stdout/stderr are handled remotely via the SFAPI, not
   through a local sandbox.

3. **Build the SBATCH script** in the temp file:

   - Standard SBATCH directives: ``-A`` (project/account), ``-t`` (runtime),
     ``--mem`` (memory), ``-p`` (partition/queue), ``-N``/``-n`` (nodes/tasks),
     ``--exclusive``, ``--gres`` (GPU/MIC resources).
   - Local submit attributes appended via ``bls_set_up_local_and_extra_args``, sourcing
     ``sfapi_local_submit_attributes.sh``.
   - HTCondor job environment variables injected by ``bls_set_job_env``. As a side effect,
     if ``_CONDOR_TRANSFER_EXECUTABLE=false`` is found in the environment, ``bls_opt_stgcmd``
     is set to ``"no"`` to suppress executable staging.

   .. note::

       ``#SBATCH --output``, ``--error``, and ``--chdir`` are **not** written here — they
       are prepended by ``submit_remote_slurm_job()`` in ``sfapi_helpers.py`` once the
       remote directory path is known.

4. **Collect input files**:

   - If ``bls_opt_stgcmd == "yes"``: resolve the executable path against ``bls_opt_workdir``
     and add it to the upload list.
   - Read ``$bls_opt_inputflstring`` (the blahp ``transfer_input_files`` list) and add each
     file, resolving relative paths against ``bls_opt_workdir``.
   - If stdin is specified and is not ``/dev/null``, resolve and add it too.

5. **Collect output files** from ``$bls_opt_outputflstring``, again resolving relative paths.

6. **Submit** the job:

   .. code-block:: bash

       python3 sfapi_helpers.py submit \
           --job-name    "$bls_tmp_name" \
           --input-files "$sfapi_input_files_csv" \
           --script      "$bls_tmp_file"

7. **Parse the result line** from stdout:

   .. code-block:: text

       SFAPI_RESULT:<jobid>:<remote_stdout>:<remote_stderr>

8. **Write the jobstate file** at ``~/.blah/sfapi_jobs/<date>_<jobID>``:

   .. code-block:: text

       # type::<local file on submit host>:<remote file to retrieve via sfapi>
       stdout::/abs/path/to/local.out:/pscratch/.../job.out
       stderr::/abs/path/to/local.err:/pscratch/.../job.err
       output::/abs/path/to/local/result.dat:/pscratch/.../result.dat

   Output files are assumed to land in ``dirname(remote_stdout)`` under their local basename.

9. **Output the blahp job ID** for ``blahpd``:

   .. code-block:: text

       BLAHP_JOBID_PREFIXsfapi/<YYYYMMDD>/<jobID>

``transfer_executable`` Handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

BLAHP does not support the ``transfer_executable`` submit attribute natively. The integration
uses a convention: set ``_CONDOR_TRANSFER_EXECUTABLE=false`` as an HTCondor environment
variable (or via the Pegasus ``HTCondor_SFAPI`` style class). The ``bls_set_job_env``
function detects this string and sets ``bls_opt_stgcmd="no"``, preventing executable upload.

``sfapi_status.sh``
-------------------

Called by ``blahpd`` to poll job state. Accepts one or more blahp job IDs on the command
line.

Flow (per job)
^^^^^^^^^^^^^^

1. Strip the path prefix: ``sfapi/YYYYMMDD/JOBID`` → ``JOBID``.
2. Query the remote state:

   .. code-block:: bash

       python3 sfapi_helpers.py status --type job --value $JOBID

3. Parse the ``"Job <id> state: <state>"`` line from output.
4. Map the Slurm/SFAPI state to a blahp status code:

   .. list-table::
      :header-rows: 1
      :widths: 45 15 40

      * - SFAPI / Slurm State
        - blahp Code
        - Meaning
      * - ``PENDING``, ``CONFIGURING``
        - 1
        - Idle / Queued
      * - ``RUNNING``, ``COMPLETING``, ``STOPPED``, ``SUSPENDED``
        - 2
        - Running
      * - ``CANCELLED``
        - 3
        - Cancelled
      * - ``COMPLETED``, ``FAILED``, ``BOOT_FAIL``, ``NODE_FAIL``,
          ``PREEMPTED``, ``SPECIAL_EXIT``, ``TIMEOUT``
        - 4
        - Done / Failed
      * - (unknown)
        - 1
        - Treated as queued

5. When status is **4 (Done)**:

   - Call ``python3 sfapi_helpers.py download sfapi/<date>/<jobID>`` to retrieve all output
     files listed in the jobstate file.
   - Delete the jobstate file ``~/.blah/sfapi_jobs/<date>_<jobID>``.

6. Output the blahp result line:

   .. code-block:: text

       0[BatchJobId="JOBID";JobStatus=N;ExitCode=0;]

   On error:

   .. code-block:: text

       1Error: <message>

``sfapi_cancel.sh``
-------------------

Called by ``blahpd`` to cancel one or more jobs. Output format mirrors ``slurm_cancel.sh``.

Flow (per job)
^^^^^^^^^^^^^^

1. Call ``python3 sfapi_helpers.py cancel "$job"``.
2. If the output contains
   ``sfapi_client.exceptions.SfApiError: Job not found:``, treat the result as success — the
   job has already completed or never existed.
3. Output (single job):

   .. code-block:: text

        0 No\ error

   Output (multiple jobs, zero-indexed):

   .. code-block:: text

       .0 0 No\ error
       .1 0 No\ error

``sfapi_ping.sh``
-----------------

Called by ``blahpd`` to check whether the backend is available before submitting jobs.
Invokes ``sfapi_helpers.py status --type resource --value <resource>`` and maps the result
to blahp ping convention: ``"0 No error"`` if active, ``"1 SFAPI status error: ..."``
otherwise. Always exits with code 0.

The resource name defaults to ``perlmutter`` and can be overridden in ``blah.config`` via
the ``sfapi_resource`` variable.

``sfapi_local_submit_attributes.sh``
-------------------------------------

A helper script sourced during submission that translates HTCondor job attributes
into ``#SBATCH`` directives. It is invoked via ``bls_set_up_local_and_extra_args``.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Environment Variable (ClassAd)
     - ``#SBATCH`` Directive
   * - ``NODES``
     - ``--nodes=N``
   * - ``CORES``
     - ``--ntasks=N``
   * - ``GPUS``
     - ``--gpus=N``
   * - ``WALLTIME``
     - ``--time=HH:MM:SS``
   * - ``PER_PROCESS_MEMORY``
     - ``--mem-per-cpu=N``
   * - ``TOTAL_MEMORY``
     - ``--mem=N``
   * - ``JOBNAME``
     - ``--job-name NAME``
   * - ``PROJECT``
     - ``--account PROJECT``
   * - ``EXTRA_ARGUMENTS``
     - Verbatim ``#SBATCH ...`` (appended last)

Job Lifecycle
=============

.. code-block:: text

    submit host                              Perlmutter (NERSC)
    ─────────────────────────────────────   ──────────────────────────────────
    sfapi_submit.sh
      |_ sfapi_helpers.py submit
           ├─ create_remote_blahp_directory  ──> mkdir $PSCRATCH/.blah/<name>
           ├─ upload_file (per input file)   ──> DTN upload
           ├─ prepend SBATCH output/error/chdir
           └─ perlmutter.submit_job()        ──> Slurm job queued
      |_ write ~/.blah/sfapi_jobs/<date>_<id>
      |_ output: BLAHP_JOBID_PREFIXsfapi/<date>/<id>

    [polling interval]

    sfapi_status.sh sfapi/<date>/<id>
      |_ sfapi_helpers.py status --type job  ──> query Slurm state
      |_ map state → blahp code (1/2/3/4)
      |_ if blahp_status == 4:
           |_ sfapi_helpers.py download       ──> download stdout/stderr/outputs
           |_ rm ~/.blah/sfapi_jobs/<date>_<id>
      |_ output: 0[BatchJobId=...;JobStatus=N;ExitCode=0;]

    [on user request or workflow abort]

    sfapi_cancel.sh sfapi/<date>/<id>
      |_ sfapi_helpers.py cancel             ──> job.cancel()
      |_ output:  0 No\ error

Remote Directory Structure on Perlmutter
=========================================

Each submitted job gets its own working directory under the user's scratch filesystem:

.. code-block:: text

    /pscratch/sd/<first_char>/<username>/.blah/<job_name>/
        |── <job_name>.sh          ← SBATCH script (submitted via SFAPI)
        |── <executable>           ← uploaded if transfer_executable=true
        |── <stdin_file>           ← uploaded if stdin != /dev/null
        |── <input_file_1>         ← from transfer_input_files
        |── ...
        |── <job_name>.out         ← Slurm stdout → downloaded to local stdout path
        |── <job_name>.err         ← Slurm stderr → downloaded to local stderr path

The ``create_remote_blahp_directory`` function resolves the scratch path dynamically by
calling ``client.user()`` to obtain the NERSC username for the authenticated client.

Jobstate File
=============

The jobstate file is the bridge between submission and retrieval. It is written by
``sfapi_submit.sh`` at ``~/.blah/sfapi_jobs/<YYYYMMDD>_<jobID>`` and read by
``sfapi_helpers.py download_job_outputs()`` when the job finishes.

**Format:**

.. code-block:: text

    # type::<local file on submit host>:<remote file to retrieve via sfapi>
    stdout::/abs/local/path/job.out:/pscratch/sd/v/vahi/.blah/bl_ABC123/bl_ABC123.out
    stderr::/abs/local/path/job.err:/pscratch/sd/v/vahi/.blah/bl_ABC123/bl_ABC123.err
    output::/abs/local/path/result.dat:/pscratch/sd/v/vahi/.blah/bl_ABC123/result.dat

Each data line has the form ``<type>::<local_path>:<remote_path>``. Lines beginning with
``#`` and blank lines are ignored. The file is deleted by ``sfapi_status.sh`` after all
outputs have been successfully downloaded.

Blahp Job ID Format
===================

.. code-block:: text

    sfapi/<YYYYMMDD>/<numeric_slurm_jobid>

    Example:  sfapi/20260511/52833290

The date component is the submission date (``date +%Y%m%d``). It is used by
``sfapi_status.sh`` to reconstruct the jobstate file path:

.. code-block:: text

    sfapi/20260511/52833290  →  ~/.blah/sfapi_jobs/20260511_52833290

Setup Requirements
==================

On the Submit Host
------------------

1. Python 3.10 or later virtual environment at ``~/superfacility-env`` containing the
   ``sfapi_client`` package:

   .. code-block:: console

       $ python3 -m venv ~/superfacility-env
       $ source ~/superfacility-env/bin/activate
       (superfacility-env) $ pip install sfapi_client

2. SFAPI credentials registered at `https://iris.nersc.gov <https://iris.nersc.gov>`_ with
   the required compute scopes, stored at:

   .. code-block:: text

       ~/.superfacility/clientid.txt      ← SFAPI client ID string
       ~/.superfacility/priv_key.jwk      ← JWK-format RSA private key (JSON)

3. HTCondor configured with a Grid Universe site entry pointing to this plugin (``batch_system
   = sfapi``).

Pegasus HTCondor Style Class
-----------------------------

Pegasus uses an ``Glite`` style class to inject the following into submitted jobs:

- ``batch_system = sfapi`` — selects this BLAHP plugin.
- ``_CONDOR_TRANSFER_EXECUTABLE=false`` in the job environment — suppresses executable
  upload when the executable is pre-installed on Perlmutter or managed by another mechanism.

Development History
===================

The capability was implemented as part of
`Pegasus issue #2186 <https://github.com/pegasus-isi/pegasus/issues/2186>`_:

