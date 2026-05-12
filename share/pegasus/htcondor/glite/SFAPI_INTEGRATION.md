# HTCondor/Pegasus SFAPI Integration: Remote Slurm Submission via NERSC Superfacility API

## Overview

This document describes the SFAPI integration that enables Pegasus/HTCondor to submit
jobs to the Perlmutter supercomputer at NERSC without a shared filesystem between the submit
host and the remote cluster.

The integration is implemented as a BLAHP (Batch Local ASCII Helper Protocol) plugin — a set
of shell scripts that sit between HTCondor's Grid Universe and the remote batch system.
The NERSC Superfacility API (SFAPI) Python client handles all communication with Perlmutter:
file upload/download, job submission, status queries, and cancellation.

---

## Architecture

```
HTCondor / Pegasus submit host
        │
        │  Grid Universe (batch_system = sfapi)
        ▼
   blahpd daemon
        │
   ┌────┴──────────────────┐
   │  BLAHP Shell Scripts  │
   │  sfapi_submit.sh      │  ──► sfapi_helpers.py submit
   │  sfapi_status.sh      │  ──► sfapi_helpers.py status / download
   │  sfapi_cancel.sh      │  ──► sfapi_helpers.py cancel
   │  sfapi_ping.sh        │  ──► sfapi_helpers.py status --type resource
   └───────────────────────┘
                │
         NERSC SFAPI (HTTPS)
                │
        ┌───────┴────────┐
        │   Perlmutter   │
        │  /pscratch/…   │
        │  Slurm queue   │
        └────────────────┘
```

Since there is **no shared filesystem** between the submit host and Perlmutter, all files are
transferred explicitly:

- **Upload** (submit time): executable, input sandbox files, and stdin — via NERSC DTNs (Data Transfer Nodes).
- **Download** (job completion): stdout, stderr, and output sandbox files — via the SFAPI download API.

---

## Components

### `sfapi_helpers.py`

The Python module that wraps the `sfapi_client` library. All BLAHP shell scripts invoke it
as a CLI tool. It exposes four subcommands and a set of helper functions.

#### Credentials

Credentials are read from `~/.superfacility/` by `load_sflapi_client_secret()`:

| File | Purpose |
|------|---------|
| `~/.superfacility/clientid.txt` | SFAPI client ID (plain text, stripped of whitespace) |
| `~/.superfacility/priv_key.jwk` | JWK-encoded RSA private key (JSON Web Key) |

The function sets module-level globals `client_id` and `client_secret` that all helper
functions use when constructing an `sfapi_client.Client` instance.

#### Key Functions

| Function | Description |
|----------|-------------|
| `load_sflapi_client_secret()` | Reads credentials, sets module globals, returns `(client_id, client_secret)` |
| `submit_remote_slurm_job(job_name, job_script, input_files)` | Creates remote scratch dir, uploads files, prepends `#SBATCH --output/--error/--chdir`, submits job; returns `(job_id, remote_stdout, remote_stderr)` |
| `create_remote_blahp_directory(name)` | Creates `$PSCRATCH/.blah/<name>` via DTNs, returns its absolute path |
| `upload_file(directory, file)` | Uploads a local file to a remote directory via NERSC DTNs |
| `download_file(source, destination)` | Downloads a remote file to a local path; raises `SfApiHelperError` if the remote path does not exist |
| `download_job_outputs(blahp_job_id)` | Reads the jobstate file for the given blahp job ID and downloads all listed remote files to their local destinations |
| `check_nersc_status(resource_name)` | Checks resource status via public SFAPI; raises `RuntimeError` if not active |
| `check_job_status(jobid)` | Prints `"Job <id> state: <state>"` for the given Slurm job ID |
| `cancel_job(job_id)` | Cancels a job by bare numeric ID or blahp ID (`sfapi/<date>/<jobid>`) |

#### CLI Subcommands

```
python3 sfapi_helpers.py submit  -n NAME -i FILE1,FILE2,... -s SCRIPT_PATH
python3 sfapi_helpers.py status  -t resource|job -v VALUE
python3 sfapi_helpers.py download BLAHP_JOB_ID
python3 sfapi_helpers.py cancel  JOB_ID
```

---

### `sfapi_setup.sh`

Sourced by every BLAHP script before any Python invocation. Activates the `~/superfacility-env`
Python virtual environment which contains the `sfapi_client` package. If the venv is missing,
it prints installation instructions and exits non-zero.

```bash
. `dirname $0`/sfapi_setup.sh
```

---

### `sfapi_submit.sh`

The main submission script. Called by `blahpd` when a new job arrives.

#### Flow

1. **Parse blahp options** via `bls_parse_submit_options` (from `blah_common_submit_functions.sh`).
2. **Set up temp files** via `bls_setup_temp_files` (deliberately avoids `bls_setup_all_files`
   because stdin/stdout/stderr are handled remotely, not via a local sandbox).
3. **Build the SBATCH script** in `$bls_tmp_file`:
   - Standard SBATCH directives: `-A` (project), `-t` (runtime), `--mem`, `-p` (partition/queue),
     `-N/-n` (nodes/tasks), `--exclusive`, `--gres` (GPU/MIC).
   - Local submit attributes appended via `bls_set_up_local_and_extra_args`.
   - Environment variables injected by `bls_set_job_env`. As a side effect, if
     `_CONDOR_TRANSFER_EXECUTABLE=false` is found in the environment, `bls_opt_stgcmd` is set
     to `"no"` to suppress executable staging.
   - **Note**: `#SBATCH --output`, `--error`, and `--chdir` are NOT written here — they are
     prepended by `sfapi_helpers.py submit_remote_slurm_job()`.
4. **Collect input files**:
   - If `bls_opt_stgcmd == "yes"`: resolve the executable path and add it to `sfapi_input_files`.
   - Read `$bls_opt_inputflstring` (the blahp transfer_input_files list) and add each file.
   - If stdin is not `/dev/null`, add it too.
5. **Collect output files** from `$bls_opt_outputflstring`.
6. **Submit** by calling:
   ```bash
   python3 sfapi_helpers.py submit \
       --job-name    "$bls_tmp_name" \
       --input-files "$sfapi_input_files_csv" \
       --script      "$bls_tmp_file"
   ```
7. **Parse the result line** `SFAPI_RESULT:<jobid>:<remote_stdout>:<remote_stderr>`.
8. **Write the jobstate file** at `~/.blah/sfapi_jobs/<date>_<jobID>`:
   ```
   # type::<local file on submit host>:<remote file to retrieve via sfapi>
   stdout::/abs/path/to/local.out:/pscratch/.../remote.out
   stderr::/abs/path/to/local.err:/pscratch/.../remote.err
   output::/abs/path/to/local/outfile:/pscratch/.../outfile
   ```
   Output files are assumed to land in `dirname(remote_stdout)` under their local basename.
9. **Output the blahp job ID**: `BLAHP_JOBID_PREFIXsfapi/<YYYYMMDD>/<jobID>`

#### `transfer_executable` Handling

BLAHP does not support `transfer_executable` natively. The integration uses a convention:
set `_CONDOR_TRANSFER_EXECUTABLE=false` as an HTCondor environment variable (or via the
Pegasus `HTCondor_SFAPI` style class). `bls_set_job_env` detects this string and sets
`bls_opt_stgcmd="no"`, preventing executable upload.

---

### `sfapi_status.sh`

Called by `blahpd` to poll job state. Accepts one or more blahp job IDs.

#### Flow (per job)

1. Strip path prefix: `sfapi/YYYYMMDD/JOBID` → `JOBID`.
2. Query: `python3 sfapi_helpers.py status --type job --value $JOBID`
3. Parse `"Job <id> state: <state>"` from output.
4. Map Slurm/SFAPI state to blahp status code via `map_sfapi_state_to_blahp`:

| SFAPI / Slurm State | blahp Code | Meaning |
|---------------------|-----------|---------|
| PENDING, CONFIGURING | 1 | Idle/Queued |
| RUNNING, COMPLETING, STOPPED, SUSPENDED | 2 | Running |
| CANCELLED | 3 | Cancelled |
| COMPLETED, FAILED, BOOT_FAIL, NODE_FAIL, PREEMPTED, SPECIAL_EXIT, TIMEOUT | 4 | Done/Failed |
| (unknown) | 1 | Treated as queued |

5. When status is **4 (Done)**:
   - Call `python3 sfapi_helpers.py download sfapi/<date>/<jobID>` to retrieve all output files.
   - Delete the jobstate file `~/.blah/sfapi_jobs/<date>_<jobID>`.
6. Output blahp result line:
   ```
   0[BatchJobId="JOBID";JobStatus=N;ExitCode=0;]
   ```
   On error: `1Error: <message>`

---

### `sfapi_cancel.sh`

Called by `blahpd` to cancel one or more jobs. Mirrors `slurm_cancel.sh` output format.

#### Flow (per job)

1. Call `python3 sfapi_helpers.py cancel "$job"`.
2. If output contains `sfapi_client.exceptions.SfApiError: Job not found:`, treat as success
   (job already completed or never existed).
3. Output:
   - Single job: `" 0 No\ error"` or `" $retcode <escaped error>"`
   - Multiple jobs: `".N 0 No\ error"` or `".N $retcode <escaped error>"`

---

### `sfapi_ping.sh`

Called by `blahpd` to check whether the backend is available. Returns `"0 No error"` if
Perlmutter is active, `"1 SFAPI status error: ..."` otherwise. Always exits 0 (blahp convention).

The resource name defaults to `"perlmutter"` and can be overridden in `blah.config` via
`sfapi_resource`.

---

### `sfapi_local_submit_attributes.sh`

A helper script that translates HTCondor job ClassAd attributes into `#SBATCH` directives.
Sourced by `bls_set_up_local_and_extra_args` in the submission flow.

| Variable in remote_ce_requirements | `#SBATCH` Directive |
|------------------------------------|-------------------|
| `NODES`                            | `--nodes=N` |
| `CORES`                            | `--ntasks=N` |
| `GPUS`                             | `--gpus=N` |
| `WALLTIME`                         | `--time=HH:MM:SS` |
| `PER_PROCESS_MEMORY`               | `--mem-per-cpu=N` |
| `TOTAL_MEMORY`                     | `--mem=N` |
| `JOBNAME`                          | `--job-name NAME` |
| `PROJECT`                          | `--account PROJECT` |
| `EXTRA_ARGUMENTS`                  | verbatim `#SBATCH ...` |

---

## Job Lifecycle

```
submit host                              Perlmutter (NERSC)
─────────────────────────────────────   ────────────────────────────────
sfapi_submit.sh
  └─ sfapi_helpers.py submit
       ├─ create_remote_blahp_directory  ──► mkdir $PSCRATCH/.blah/<name>
       ├─ upload_file (per input file)   ──► DTN upload
       ├─ prepend SBATCH directives
       └─ perlmutter.submit_job()        ──► Slurm job queued
  └─ write ~/.blah/sfapi_jobs/<date>_<id>
  └─ output: BLAHP_JOBID_PREFIXsfapi/<date>/<id>

[polling interval]

sfapi_status.sh sfapi/<date>/<id>
  └─ sfapi_helpers.py status --type job  ──► query Slurm state
  └─ map state → blahp code (1/2/3/4)
  └─ if blahp_status == 4:
       └─ sfapi_helpers.py download       ──► download stdout, stderr, outputs
       └─ rm ~/.blah/sfapi_jobs/<date>_<id>
  └─ output: 0[BatchJobId=...;JobStatus=N;ExitCode=0;]

[on user request or workflow abort]

sfapi_cancel.sh sfapi/<date>/<id>
  └─ sfapi_helpers.py cancel             ──► job.cancel()
  └─ output: " 0 No\ error"
```

---

## Remote Directory Structure on Perlmutter

```
/pscratch/sd/<first_char>/<username>/.blah/<job_name>/
    ├── <job_name>.sh          ← SBATCH job script (uploaded by SFAPI submit API)
    ├── <executable>           ← uploaded if transfer_executable=true
    ├── <stdin_file>           ← uploaded if stdin ≠ /dev/null
    ├── <input_file_1>         ← from transfer_input_files
    ├── ...
    ├── <job_name>.out         ← Slurm stdout (→ downloaded to local stdout path)
    └── <job_name>.err         ← Slurm stderr (→ downloaded to local stderr path)
```

---

## Jobstate File Format

`~/.blah/sfapi_jobs/<YYYYMMDD>_<jobID>`:

```
# type::<local file on submit host>:<remote file to retrieve via sfapi>
stdout::/abs/local/path/job.out:/pscratch/sd/v/vahi/.blah/bl_ABC123/bl_ABC123.out
stderr::/abs/local/path/job.err:/pscratch/sd/v/vahi/.blah/bl_ABC123/bl_ABC123.err
output::/abs/local/path/result.dat:/pscratch/sd/v/vahi/.blah/bl_ABC123/result.dat
```

Each line has the form `<type>::<local_path>:<remote_path>`. Lines starting with `#` and
blank lines are ignored. The file is deleted by `sfapi_status.sh` after a successful download.

---

## Blahp Job ID Format

```
sfapi/<YYYYMMDD>/<numeric_slurm_jobid>

Example: sfapi/20260511/52833290
```

The date component is the submission date (`date +%Y%m%d`). It is used to reconstruct the
jobstate file path: `sfapi/20260511/52833290 → ~/.blah/sfapi_jobs/20260511_52833290`.

---

## Gaps / TODO in Implementation

- no way to specify what resource behind sfapi to submit jobs to. hardcoded to perlmutter
- sfapi_status.sh : The job should go on hold when the download of outputs fails . 
                    Need to figure out the return code in that case.
- sfapi_submit.sh:  transfer_output_remap and transfer_input_remap are not implemented
- sfapi_submit.sh:  transfer_executable functionality is implemented via setting of an 
                    environment variable _CONDOR_TRANSFER_EXECUTABLE in job environment
- job directory cleanup at NERSC: The job directory created where the remote_stdout and stderr
                    is pulled down (e.g. /pscratch/sd/v/vahi/.blah/*) are not deleted
                    as sfapi_client does not support it.


## Debugging the setup

Largely the methodology listed in the Pegasus documentation at
https://pegasus.isi.edu/documentation/user-guide/deployment-scenarios.html#debugging-job-submissions-to-local-hpc 
should work. 

One thing to keep in mind, is that in the debug directory, you will also
see the jobstate file copied from ~/.blah/sfapi_jobs . That has the
paths on the remote side where the job stdout and stderr is placed.

## Setup Requirements

### On the submit host

1. Python 3.10+ virtual environment at `~/superfacility-env` with `sfapi_client` installed:
   ```bash
   python3 -m venv ~/superfacility-env
   source ~/superfacility-env/bin/activate
   pip install sfapi_client
   ```

2. SFAPI credentials at `~/.superfacility/`:
   ```
   ~/.superfacility/clientid.txt      ← SFAPI client ID string
   ~/.superfacility/priv_key.jwk      ← JWK-format RSA private key (JSON)
   ```
   The SFAPI client registered at https://iris.nersc.gov must have the appropriate scopes
   (compute:perlmutter, compute:dtns, etc.).

3. HTCondor `blah.config` must reference the sfapi plugin. The `sfapi_resource` variable
   can override the default NERSC resource name (`perlmutter`).

### Pegasus HTCondor Style Class

Pegasus uses a `HTCondor_SFAPI` style class to inject:
- `batch_system = sfapi` — selects this BLAHP plugin
- `_CONDOR_TRANSFER_EXECUTABLE=false` in the job environment — suppresses executable upload
  when the executable is already available on Perlmutter or managed separately

---

 