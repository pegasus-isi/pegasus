"""
pegasus-runtime-predictor — Pegasus runtime prediction CLI.

Installed as a console script via:
    pip install "pegasus-wms[runtime-prediction]"

This module is the canonical implementation.  The standalone script at
bin/pegasus-runtime-predictor delegates here so both source installs and
pip installs use the same code path.

Usage (called automatically as a DAGMan SCRIPT PRE by the pegasus-plan wrapper):
    pegasus-runtime-predictor --job-sub FILE [--level N] [--lof FILE] [--meta FILES]
                              <workflow.yml> <output_dir>
"""

import fcntl
import os
import sys
import time
import traceback
from pathlib import Path

import click
import yaml

from Pegasus.api import File, Job, Namespace, Workflow
from Pegasus.monitoring.metadata import Metadata
from Pegasus.runtime.predictor import (
    RuntimePredictionConfig,
    WorkflowRuntimePredictor,
    cluster_task_ids,
    parse_sub_file,
    patch_sub_file,
)


def _load_jobs_from_yaml(path: str, job_ids, inputs: bool = True) -> Workflow:
    """
    Load only the jobs with the given ids from the workflow YAML file. The
    input files of the jobs are only loaded if inputs is True.
    """
    with open(path) as f:
        data = yaml.safe_load(f)

    wf_name = data.get("name", "workflow")
    wf = Workflow(wf_name, infer_dependencies=False)

    for jdata in data.get("jobs", []):
        jid = jdata.get("id", "")
        if jid not in job_ids:
            continue
        trans = jdata.get("name", jdata.get("transformation", ""))
        job = Job(trans, _id=jid)

        for u in jdata.get("uses", []):
            fi = File(u["lfn"])
            if u.get("type") == "input":
                if not inputs:
                    continue
                if u.get("size"):
                    fi.size = int(u["size"])
                job.add_inputs(fi)
            elif u.get("type") == "output":
                job.add_outputs(fi)

        for key, val in jdata.get("profiles", {}).get("condor", {}).items():
            job.add_profiles(Namespace.CONDOR, **{key: str(val)})

        wf.add_jobs(job)

    return wf


def _read_lof(lof_file: str) -> list:
    """Return the lfns listed in a LOF file, that has one lfn=... entry per line."""
    lfns = []
    with open(lof_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                lfns.append(line.split("=", 1)[0])
    return lfns


def _read_meta_sizes(meta_files) -> dict:
    """Return the sizes in bytes of the files in the .meta files, indexed by lfn."""
    sizes = {}
    for meta_file in meta_files:
        try:
            metadata_list = Metadata.load(meta_file)
        except (OSError, ValueError) as e:
            print(
                f"[pegasus-runtime-predictor] Unable to read meta file {meta_file}: {e}",
                file=sys.stderr,
            )
            continue
        for m in metadata_list:
            size = m.get_attribute_value("size")
            if m.get_type() == "file" and size is not None:
                sizes[m.get_id()] = int(size)
    return sizes


def _warn_no_size(lfn: str, job_id: str) -> None:
    """Warn that an input file of a job has no size in the .meta files."""
    print(
        f"[pegasus-runtime-predictor] WARNING: No size found in meta files for input "
        f"{lfn} of job {job_id}",
        file=sys.stderr,
    )


def _append_prescript_timing(
    output_dir: str, job_id: str, duration_s: float, level
) -> None:
    """Append one row to prescript_timings.csv — safe for parallel SCRIPT PRE calls."""
    timing_csv = os.path.join(output_dir, "prescript_timings.csv")
    header_needed = not os.path.exists(timing_csv) or os.path.getsize(timing_csv) == 0
    with open(timing_csv, "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            if header_needed:
                fh.write("job_id,dag_level,prescript_duration_s\n")
            fh.write(
                f"{job_id},{level if level is not None else ''},{duration_s:.3f}\n"
            )
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def predict(
    workflow_yml,
    output_dir,
    job_sub,
    target_level=None,
    lof_file=None,
    meta_files=None,
):
    """
    Computes the runtime prediction for the job whose condor submit file is
    passed, and patches it into the submit file. For a clustered job, the
    predictions for its constituent tasks are computed and aggregated.

    :param workflow_yml: path to the workflow YAML file
    :param output_dir: directory to write the timings to
    :param job_sub: path to the condor submit file of the job to be patched
    :param target_level: the DAG level of the job, recorded in the timings
    :param lof_file: LOF file listing the lfns of the input files of the job
    :param meta_files: list of .meta files containing the sizes of the input
                       files of the job
    """
    _start_time = time.time()

    output_dir = str(Path(output_dir).resolve())
    os.makedirs(output_dir, exist_ok=True)

    dax_job_id, sub_info = parse_sub_file(Path(job_sub))
    if sub_info is None:
        print(
            f"[pegasus-runtime-predictor] Unable to read {job_sub} — skipping prediction",
            file=sys.stderr,
        )
        return

    # a clustered job has no dax job id of its own. predict for its tasks
    cluster_in_file = sub_info.get("cluster_in_file")
    if cluster_in_file:
        job_ids = cluster_task_ids(cluster_in_file)
        print(f"[pegasus-runtime-predictor] Cluster detected: {job_sub}")
        print(f"[pegasus-runtime-predictor]   .in file:        {cluster_in_file}")
        print(f"[pegasus-runtime-predictor]   task count:      {len(job_ids)}")
    else:
        job_ids = [dax_job_id] if dax_job_id else []

    if not job_ids:
        print(
            f"[pegasus-runtime-predictor] No job id found in {job_sub} — skipping prediction",
            file=sys.stderr,
        )
        return

    # label for the job in the log and the timings
    caller_job_id = dax_job_id or job_sub

    print(f"[pegasus-runtime-predictor] Reading workflow: {workflow_yml}")
    # the LOF lists the inputs of a clustered job as a whole, and not per
    # task. so the inputs of the tasks are loaded from the workflow
    wf = _load_jobs_from_yaml(workflow_yml, job_ids, inputs=bool(cluster_in_file))
    missing = [jid for jid in job_ids if jid not in wf.jobs]
    if missing:
        print(
            f"[pegasus-runtime-predictor] Job(s) not found in workflow: {', '.join(missing)}",
            file=sys.stderr,
        )

    sizes = _read_meta_sizes(meta_files or [])
    print(
        f"[pegasus-runtime-predictor] Found {len(sizes)} file size(s) in "
        f"{len(meta_files or [])} meta file(s)"
    )
    if cluster_in_file:
        # files produced by other tasks in the cluster have no .meta entry yet
        # and their sizes are estimated
        produced = {fo.lfn for job in wf.jobs.values() for fo in job.get_outputs()}
        for job in wf.jobs.values():
            for fi in job.get_inputs():
                if fi.lfn in sizes:
                    fi.size = sizes[fi.lfn]
                elif fi.lfn not in produced:
                    _warn_no_size(fi.lfn, job._id)
    else:
        lfns = _read_lof(lof_file) if lof_file else []
        if not lfns:
            print(
                f"[pegasus-runtime-predictor] No input files found for {caller_job_id}",
                file=sys.stderr,
            )
        for lfn in lfns:
            if lfn not in sizes:
                _warn_no_size(lfn, caller_job_id)
        for job in wf.jobs.values():
            job.add_inputs(*[File(lfn, size=sizes.get(lfn)) for lfn in lfns])

    cfg = RuntimePredictionConfig(enabled=True, output_dir=output_dir)
    predictor = WorkflowRuntimePredictor(cfg)

    # the resources in the submit file are for the job itself, and not the
    # constituent tasks of a clustered job
    sub_resources = {}
    if dax_job_id:
        sub_resources[dax_job_id] = {
            k: v for k, v in sub_info.items() if k != "sub_path"
        }

    _pred_start = time.time()
    results = predictor.predict_jobs(wf, sub_resources=sub_resources)
    _pred_duration = time.time() - _pred_start
    print(
        f"[pegasus-runtime-predictor] {len(results)} job(s) predicted in {_pred_duration:.3f}s"
    )

    pred = None
    if results and cluster_in_file:
        # Clustered job — aggregate predictions for all constituent tasks
        pred = {
            "predicted_runtime_s": sum(p["predicted_runtime_s"] for p in results),
            "upper_bound_s": sum(p["upper_bound_s"] for p in results),
            "lower_bound_s": sum(p["lower_bound_s"] for p in results),
            "status": "CLUSTERED",
        }
        print(
            f"[pegasus-runtime-predictor] Cluster {caller_job_id}: "
            f"{len(results)}/{len(job_ids)} tasks aggregated, "
            f"total={pred['predicted_runtime_s']:.1f}s"
        )
    elif results:
        pred = results[0]

    if pred:
        if patch_sub_file(sub_info["sub_path"], pred):
            print(f"[pegasus-runtime-predictor] Patched .sub for {caller_job_id}")
        else:
            print(
                f"[pegasus-runtime-predictor] Could not patch .sub for {caller_job_id}",
                file=sys.stderr,
            )
    else:
        print(f"[pegasus-runtime-predictor] No prediction found for {caller_job_id}")

    # ── Record how long this prescript invocation took ─────────────────────
    prescript_duration_s = time.time() - _start_time
    _append_prescript_timing(
        output_dir, caller_job_id, prescript_duration_s, target_level
    )
    print(
        f"[pegasus-runtime-predictor] Prescript duration for {caller_job_id}: "
        f"{prescript_duration_s:.3f}s"
    )


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
@click.argument("workflow-yml", type=click.Path(dir_okay=False))
@click.argument("output-dir", type=click.Path(file_okay=False))
def main(workflow_yml, output_dir, target_level, job_sub, lof_file, meta_files):
    """pegasus-runtime-predictor predicts the runtime of a job in a workflow.

    Called automatically as a DAGMan PRE script for a job, to predict the
    runtime of the job, or of the tasks of a clustered job, and patch it into
    the condor submit file of the job. On error, it exits 0, so that the
    workflow continues.
    """
    try:
        predict(
            workflow_yml,
            output_dir,
            job_sub,
            target_level,
            lof_file=lof_file,
            meta_files=meta_files,
        )
    except Exception as exc:
        print(f"[pegasus-runtime-predictor] ERROR: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(0)  # exit 0 so the workflow continues


if __name__ == "__main__":
    main()
