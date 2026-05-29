#!/usr/bin/env python3
"""
pegasus-runtime-predictor — Pegasus runtime prediction CLI.

Installed as a console script via:
    pip install "pegasus-wms[runtime-prediction]"

This module is the canonical implementation.  The standalone script at
bin/pegasus-runtime-predictor delegates here so both source installs and
pip installs use the same code path.

Usage (called automatically as a DAGMan SCRIPT PRE by the pegasus-plan wrapper):
    pegasus-runtime-predictor <workflow.yml> <output_dir> [--level=N] [--job-id=ID]
"""

import json
import os
import sys
import time
import traceback
from pathlib import Path

import yaml

from Pegasus.api import File, Job, Namespace, Workflow
from Pegasus.runtime_predictor import (
    RuntimePredictionConfig,
    WorkflowRuntimePredictor,
    find_submit_dir,
    patch_sub_file,
    read_meta_sizes,
    scan_sub_files,
)


def _load_workflow_from_yaml(path: str) -> Workflow:
    with open(path) as f:
        data = yaml.safe_load(f)

    wf_name = data.get("name", "workflow")
    wf = Workflow(wf_name, infer_dependencies=False)

    for jdata in data.get("jobs", []):
        jid   = jdata.get("id", "")
        trans = jdata.get("name", jdata.get("transformation", ""))
        job   = Job(trans, _id=jid)

        for u in jdata.get("uses", []):
            fi = File(u["lfn"])
            if u.get("type") == "input":
                if u.get("size"):
                    fi.size = int(u["size"])
                job.add_inputs(fi)
            elif u.get("type") == "output":
                job.add_outputs(fi)

        for key, val in jdata.get("profiles", {}).get("condor", {}).items():
            job.add_profiles(Namespace.CONDOR, **{key: str(val)})

        wf.add_jobs(job)

    for dep in data.get("dependencies", []):
        parent_id = dep.get("id")
        for child_id in dep.get("children", []):
            wf.add_dependency(parent_id, children=[child_id])

    return wf


def _scan_actual_file_sizes(output_dir: str, wf: Workflow) -> dict:
    """Scan output_dir and nearby scratch dirs for actual file sizes on disk."""
    all_lfns = set()
    for job in wf.jobs.values():
        for fi in (job.get_inputs() if hasattr(job, "get_inputs") else []):
            all_lfns.add(fi.lfn)
        for fi in (job.get_outputs() if hasattr(job, "get_outputs") else []):
            all_lfns.add(fi.lfn)

    size_map = {}
    search_roots = [Path(output_dir)]
    p = Path(output_dir).parent
    for _ in range(4):
        for candidate in (p / "scratch", p / "data", p):
            if candidate.is_dir() and candidate != Path(output_dir):
                search_roots.append(candidate)
        p = p.parent

    for root in search_roots:
        try:
            for path in root.rglob("*"):
                if path.is_file() and path.name in all_lfns:
                    size_map[path.name] = path.stat().st_size
        except (PermissionError, OSError):
            continue

    return size_map


def main():
    if len(sys.argv) < 3:
        print(
            f"Usage: {sys.argv[0]} <workflow.yml> <output_dir> [--level=N] [--job-id=ID]",
            file=sys.stderr,
        )
        sys.exit(1)

    workflow_yml  = sys.argv[1]
    output_dir    = sys.argv[2]
    target_level  = None
    caller_job_id = None

    for arg in sys.argv[3:]:
        if arg.startswith("--level="):
            try:
                target_level = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg.startswith("--job-id="):
            caller_job_id = arg.split("=", 1)[1]

    os.makedirs(output_dir, exist_ok=True)

    suffix    = f"_L{target_level}" if target_level is not None else ""
    json_path = os.path.join(output_dir, f"runtime_predictions{suffix}.json")
    csv_path  = os.path.join(output_dir, f"runtime_predictions{suffix}.csv")

    # ── Step 1: compute predictions (only if not already done for this level) ─
    # When many parallel jobs fire their prescript simultaneously, only the
    # FIRST process computes; the rest wait for the JSON and go straight to
    # patching their own .sub file.  Atomic rename ensures no partial reads.
    if not os.path.exists(json_path):
        print(f"[pegasus-runtime-predictor] Reading workflow: {workflow_yml}")
        wf = _load_workflow_from_yaml(workflow_yml)

        actual_sizes = _scan_actual_file_sizes(output_dir, wf)
        if actual_sizes:
            print(f"[pegasus-runtime-predictor] Found {len(actual_sizes)} actual file size(s) on disk")
            for job in wf.jobs.values():
                for fi in (job.get_inputs() if hasattr(job, "get_inputs") else []):
                    if fi.lfn in actual_sizes:
                        fi.size = actual_sizes[fi.lfn]
                for fi in (job.get_outputs() if hasattr(job, "get_outputs") else []):
                    if fi.lfn in actual_sizes:
                        fi.size = actual_sizes[fi.lfn]

        cfg       = RuntimePredictionConfig(enabled=True, output_dir=output_dir)
        predictor = WorkflowRuntimePredictor(cfg)

        submit_dir = find_submit_dir(workflow_yml)
        sub_scan   = {}
        if submit_dir:
            sub_scan = scan_sub_files(submit_dir)
            print(f"[pegasus-runtime-predictor] Scanned {len(sub_scan)} .sub file(s) from {submit_dir}")
        else:
            print("[pegasus-runtime-predictor] Submit dir not found — using YAML profile values")

        sub_resources = {
            jid: {k: v for k, v in info.items() if k != "sub_path"}
            for jid, info in sub_scan.items()
        }

        results = predictor.predict(wf, sub_resources=sub_resources)

        if target_level is not None and results:
            level_results = [r for r in results if r.get("dag_level") == target_level]
            print(f"[pegasus-runtime-predictor] Level {target_level}: {len(level_results)} job(s) predicted")
        else:
            level_results = results
            print(f"[pegasus-runtime-predictor] All levels: {len(results)} job(s) predicted")

        import pandas as pd

        tmp_json = json_path + ".tmp"
        tmp_csv  = csv_path  + ".tmp"
        pd.DataFrame(level_results).to_csv(tmp_csv, index=False)
        with open(tmp_json, "w") as fh:
            json.dump(
                {"level": target_level, "total_jobs": len(level_results), "predictions": level_results},
                fh, indent=2,
            )
        os.replace(tmp_csv,  csv_path)
        os.replace(tmp_json, json_path)
        print(f"[pegasus-runtime-predictor] Done -> {json_path}")
    else:
        print(f"[pegasus-runtime-predictor] Level {target_level} predictions already exist — skipping computation")

    # ── Step 2: wait for JSON, then patch only THIS job's .sub file ────────
    deadline = time.time() + 60
    while not os.path.exists(json_path) and time.time() < deadline:
        time.sleep(0.5)

    if not os.path.exists(json_path):
        print(f"[pegasus-runtime-predictor] Timed out waiting for {json_path} — skipping patch",
              file=sys.stderr)
        return

    if caller_job_id:
        with open(json_path) as fh:
            data = json.load(fh)
        pred = next(
            (p for p in data.get("predictions", []) if p.get("job_id") == caller_job_id),
            None,
        )
        if pred:
            submit_dir = find_submit_dir(workflow_yml)
            if submit_dir:
                sub_scan = scan_sub_files(submit_dir)
                sub_info = sub_scan.get(caller_job_id)
                if sub_info and sub_info.get("sub_path"):
                    if patch_sub_file(sub_info["sub_path"], pred):
                        print(f"[pegasus-runtime-predictor] Patched .sub for {caller_job_id}")
                    else:
                        print(f"[pegasus-runtime-predictor] Could not patch .sub for {caller_job_id}",
                              file=sys.stderr)
        else:
            print(f"[pegasus-runtime-predictor] No prediction found for {caller_job_id}")


def _run():
    """Entry point with error handling and fallback JSON output."""
    try:
        main()
    except Exception as exc:
        print(f"[pegasus-runtime-predictor] ERROR: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

        _output_dir = sys.argv[2] if len(sys.argv) >= 3 else "."
        _level = None
        for _a in sys.argv[3:]:
            if _a.startswith("--level="):
                try:
                    _level = int(_a.split("=", 1)[1])
                except ValueError:
                    pass
        _suffix = f"_L{_level}" if _level is not None else ""
        try:
            os.makedirs(_output_dir, exist_ok=True)
            _fallback = {"level": _level, "total_jobs": 0, "predictions": [], "error": str(exc)}
            _json_path = os.path.join(_output_dir, f"runtime_predictions{_suffix}.json")
            _csv_path  = os.path.join(_output_dir, f"runtime_predictions{_suffix}.csv")
            with open(_json_path, "w") as _f:
                json.dump(_fallback, _f, indent=2)
            with open(_csv_path, "w") as _f:
                _f.write("job_id,transformation,dag_level,status,"
                         "predicted_runtime_s,lower_bound_s,upper_bound_s\n")
            print(f"[pegasus-runtime-predictor] Wrote fallback outputs to {_output_dir}", file=sys.stderr)
        except Exception:
            pass

        sys.exit(0)  # exit 0 so the workflow continues


if __name__ == "__main__":
    _run()
