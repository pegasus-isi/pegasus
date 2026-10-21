"""
pegasus-runtime-predictor — Pegasus runtime prediction CLI.

Installed as the pegasus-runtime-predictor console script by the pegasus-wms
wheel. The libraries it needs are installed with the runtime-prediction extra:
    pip install "pegasus-wms[runtime-prediction]"

The prediction itself is implemented by Pegasus.runtime.predictor.predict.
The standalone script at bin/pegasus-runtime-predictor delegates here so both
source installs and pip installs use the same code path.

Usage (called automatically as a DAGMan SCRIPT PRE by the pegasus-plan wrapper):
    pegasus-runtime-predictor --job-sub FILE [--level N] [--lof FILE] [--meta FILES]
                              [-l LOG] <workflow.yml> <output_dir>
"""

import datetime
import fcntl
import io
import json
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout

import click


def _write_log(log_file: str, log: dict) -> None:
    """
    Append the log record for this invocation as a single JSON line to the
    log file — safe for parallel SCRIPT PRE calls.
    """
    with open(log_file, "a", encoding="utf8") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            fh.write(json.dumps(log, ensure_ascii=False) + "\n")
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _run(workflow_yml, output_dir, job_sub, target_level, lof_file, meta_files):
    """
    Run the prediction, printing any error instead of raising it.

    :return: the exitcode, 0 on success and 1 on error
    """
    # the libraries for runtime prediction are in an optional extra
    try:
        from Pegasus.runtime.predictor import PredictorError, predict
    except ImportError as e:
        print(
            f"[pegasus-runtime-predictor] ERROR: Runtime prediction is not available "
            f'({e}). Install it with: pip install "pegasus-wms[runtime-prediction]"',
            file=sys.stderr,
        )
        return 1

    try:
        predict(
            workflow_yml,
            output_dir,
            job_sub,
            target_level,
            lof_file=lof_file,
            meta_files=meta_files,
        )
    except PredictorError as exc:
        print(f"[pegasus-runtime-predictor] ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"[pegasus-runtime-predictor] ERROR: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1
    return 0


def _split_comma_list(ctx, param, value):
    """Click callback that splits a comma separated option value into a list."""
    if value is None:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


@click.command(options_metavar="<options>")
@click.option(
    "--level",
    "target_level",
    type=int,
    metavar="<N>",
    help="DAG level of the job, recorded in the prescript timings",
)
@click.option(
    "--job-sub",
    "job_sub",
    required=True,
    type=click.Path(dir_okay=False),
    metavar="<file>",
    help="Condor submit file of the job invoking the prescript, which is patched",
)
@click.option(
    "--lof",
    "lof_file",
    type=click.Path(dir_okay=False),
    metavar="<file>",
    help="LOF file listing the input files of the job",
)
@click.option(
    "--meta",
    "meta_files",
    callback=_split_comma_list,
    metavar="<file1,file2,...>",
    help="Comma separated .meta files containing the sizes of the input files of the job",
)
@click.option(
    "-l",
    "--log",
    "log_file",
    type=click.Path(dir_okay=False),
    metavar="<file>",
    help="Common log file to which the stdout/stderr of the invocation is "
    "appended as a JSON record",
)
@click.argument("workflow-yml", type=click.Path(dir_okay=False))
@click.argument("output-dir", type=click.Path(file_okay=False))
def main(
    workflow_yml, output_dir, target_level, job_sub, lof_file, meta_files, log_file
):
    """pegasus-runtime-predictor predicts the runtime of a job in a workflow.

    Called automatically as a DAGMan PRE script for a job, to predict the
    runtime of the job, or of the tasks of a clustered job, and patch it into
    the condor submit file of the job. On error, it exits with a non zero
    exitcode. With -l, the stdout/stderr of the invocation is
    appended as a JSON record to the log file instead.
    """
    args = (workflow_yml, output_dir, job_sub, target_level, lof_file, meta_files)
    if not log_file:
        sys.exit(_run(*args))

    # capture stdout/stderr of the invocation to record in the log file
    log = {
        "name": job_sub,
        "timestamp": datetime.datetime.now().isoformat(),
        "level": target_level,
    }

    std_out, std_err = io.StringIO(), io.StringIO()
    start = time.time()
    with redirect_stdout(std_out), redirect_stderr(std_err):
        exitcode = _run(*args)

    log["exitcode"] = exitcode
    # in seconds
    log["duration"] = round(time.time() - start, 3)
    log["std_out"] = std_out.getvalue()
    log["std_err"] = std_err.getvalue()

    try:
        _write_log(log_file, log)
    except OSError as e:
        print(
            f"[pegasus-runtime-predictor] Unable to write to log file {log_file}: {e}",
            file=sys.stderr,
        )
        print(json.dumps(log), file=sys.stderr)

    sys.exit(exitcode)


if __name__ == "__main__":
    main()
