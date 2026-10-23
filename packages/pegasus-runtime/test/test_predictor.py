"""
Tests for Pegasus.runtime.predictor

Pure-function tests run without torch.
ModelContext tests are skipped when torch is absent. The whole module is
skipped when numpy or pandas are absent, e.g. on Python 3.13+ where the
numpy<2.0 required by the runtime-prediction extra has no wheels.
"""

import json
import os
import pickle
import subprocess
import time
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from Pegasus.runtime import predictor
from Pegasus.runtime.predictor import (
    CONDOR_STATUS_ATTRIBUTES,
    CONDOR_STATUS_CACHE_FILE,
    CONDOR_STATUS_CACHE_MAX_AGE,
    PredictorError,
    RuntimePredictionConfig,
    WorkflowRuntimePredictor,
    _bytes_bin,
    _condor_status_json,
    _get_condor_slot,
    _job_from_sub,
    _load_jobs_from_yaml,
    _read_lof,
    _read_meta_sizes,
    _site,
    _split_tc_name,
    _synth,
    cluster_task_ids,
    parse_sub_file,
    patch_sub_file,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

try:
    import torch  # noqa: F401
    _TORCH = True
except ImportError:
    _TORCH = False

requires_torch = pytest.mark.skipif(not _TORCH, reason="torch not installed")


# ---------------------------------------------------------------------------
# RuntimePredictionConfig
# ---------------------------------------------------------------------------

class TestRuntimePredictionConfig:
    def test_defaults(self):
        cfg = RuntimePredictionConfig()
        assert cfg.enabled is True
        assert cfg.model_path is None
        assert cfg.output_dir is None
        assert cfg.condor_status_cache is None

    def test_from_properties_enabled_false(self):
        props = {"pegasus.runtime.prediction.enable": "false"}
        cfg = RuntimePredictionConfig.from_properties(props)
        assert cfg.enabled is False

    def test_from_properties_model_path(self):
        props = {"pegasus.runtime.prediction.model.path": "/tmp/model.pkl"}
        cfg = RuntimePredictionConfig.from_properties(props)
        assert cfg.model_path == "/tmp/model.pkl"

    def test_from_properties_numeric_overrides(self):
        props = {
            "pegasus.runtime.prediction.sparse.threshold": "5",
            "pegasus.runtime.prediction.interval.low":     "0.1",
            "pegasus.runtime.prediction.interval.high":    "0.9",
            "pegasus.runtime.prediction.slot.cpu.count":   "8",
            "pegasus.runtime.prediction.slot.cpu.speed":   "3200.0",
            "pegasus.runtime.prediction.slot.ram":         "16777216",
        }
        cfg = RuntimePredictionConfig.from_properties(props)
        assert cfg.sparse_threshold == 5
        assert cfg.q_lo == pytest.approx(0.1)
        assert cfg.q_hi == pytest.approx(0.9)
        assert cfg.slot_cpu_count == 8
        assert cfg.slot_cpu_speed == pytest.approx(3200.0)
        assert cfg.slot_ram == pytest.approx(16777216.0)

    def test_from_properties_empty_dict(self):
        cfg = RuntimePredictionConfig.from_properties({})
        assert cfg.enabled is True
        assert cfg.model_path is None


# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------

class TestSiteEncoding:
    @pytest.mark.parametrize("hostname,expected", [
        ("isi.edu",        0),
        ("wsu.edu",        1),
        ("hellbender.wustl.edu", 1),
        ("anvil.rcac.purdue.edu", 2),
        ("stampede2.tacc.utexas.edu", 3),
        ("expanse.sdsc.edu", 4),
        ("osg-ce.example.org", 5),
        ("unknown",        5),
    ])
    def test_known_sites(self, hostname, expected):
        assert _site(hostname) == expected


class TestBytesbin:
    @pytest.mark.parametrize("b,expected", [
        (0,       0),
        (-1,      0),
        (100,     1),
        (50_000,  2),
        (500_000, 3),
        (5_000_000, 4),
        (50_000_000, 5),
        (500_000_000, 6),
    ])
    def test_bins(self, b, expected):
        assert _bytes_bin(b) == expected


class TestSynth:
    def test_splits_on_dash(self):
        assert _synth("preprocess-0") == "preprocess"

    def test_splits_on_underscore(self):
        assert _synth("findrange_1") == "findrange"

    def test_no_separator(self):
        assert _synth("myjob") == "myjob"

    def test_none_returns_unknown(self):
        assert _synth(None) == "unknown"


# ---------------------------------------------------------------------------
# parse_sub_file / cluster_task_ids
# ---------------------------------------------------------------------------

def _write_sub(path: Path, dax_job_id="ID0000001", cluster_size=1,
               xformation="pegasus::preprocess:4.0"):
    """Write a condor submit file the way the Pegasus planner does."""
    path.write_text(
        "universe = vanilla\n"
        "periodic_remove = (JobStatus == 5) && "
        "((CurrentTime - EnteredCurrentStatus) > 1800)\n"
        "request_cpus = $(pegasus_cores)\n"
        "request_memory = $(pegasus_memory_mb) MB\n"
        "pegasus_cores = 2\n"
        "pegasus_gpus = 0\n"
        "pegasus_memory_mb = 2048\n"
        "pegasus_diskspace_mb = 5120\n"
        f'+pegasus_wf_xformation = "{xformation}"\n'
        f'+pegasus_wf_dax_job_id = "{dax_job_id}"\n'
        f"+pegasus_cluster_size = {cluster_size}\n"
        "queue\n"
    )
    return path


def _write_cluster_in(path: Path, tasks):
    """Write a cluster .in file, with a monitord #@ line per task."""
    lines = []
    for i, (xformation, job_id) in enumerate(tasks, start=1):
        lines.append(f"#@ {i} {xformation} {job_id} ")
        lines.append(f"pegasus-kickstart -n {xformation} -N {job_id} ./{job_id}")
    path.write_text("\n".join(lines) + "\n")
    return path


class TestParseSubFile:
    def test_job(self, tmp_path):
        sub = _write_sub(tmp_path / "preprocess_ID0000001.sub")
        dax_job_id, info = parse_sub_file(sub)
        assert dax_job_id == "ID0000001"
        assert info["sub_path"] == str(sub)
        assert info["transformation"] == "pegasus::preprocess:4.0"
        assert info["cluster_size"] == 1
        assert info["cluster_in_file"] is None
        # the values of the pegasus_ keys, not the $(...) macros
        assert info["pegasus_cores"] == "2"
        assert info["pegasus_gpus"] == "0"
        assert info["pegasus_memory_mb"] == "2048"
        assert info["pegasus_diskspace_mb"] == "5120"
        assert info["request_cpus"] is None
        assert info["request_memory_mb"] is None

    def test_clustered_job(self, tmp_path):
        sub = _write_sub(
            tmp_path / "merge_findrange-4_0_PID2_ID1.sub",
            dax_job_id="null",
            cluster_size=2,
            xformation="pegasus::seqexec",
        )
        dax_job_id, info = parse_sub_file(sub)
        assert dax_job_id is None
        assert info["cluster_size"] == 2
        assert info["cluster_in_file"] == str(tmp_path / "merge_findrange-4_0_PID2_ID1.in")

    def test_merge_job_with_single_task_is_not_clustered(self, tmp_path):
        sub = _write_sub(tmp_path / "merge_findrange-4_0_PID2_ID1.sub", cluster_size=1)
        _, info = parse_sub_file(sub)
        assert info["cluster_in_file"] is None

    def test_cluster_size_without_merge_prefix_is_not_clustered(self, tmp_path):
        sub = _write_sub(tmp_path / "findrange_ID0000002.sub", cluster_size=2)
        _, info = parse_sub_file(sub)
        assert info["cluster_in_file"] is None

    def test_missing_file(self, tmp_path):
        assert parse_sub_file(tmp_path / "missing.sub") == (None, None)


class TestClusterTaskIds:
    def test_task_ids(self, tmp_path):
        in_file = _write_cluster_in(
            tmp_path / "merge.in",
            [("pegasus::findrange:4.0", "ID0000002"),
             ("pegasus::findrange:4.0", "ID0000003")],
        )
        assert cluster_task_ids(str(in_file)) == ["ID0000002", "ID0000003"]

    def test_missing_file(self, tmp_path):
        assert cluster_task_ids(str(tmp_path / "missing.in")) == []


# ---------------------------------------------------------------------------
# patch_sub_file
# ---------------------------------------------------------------------------

def _prediction_ads(content: str):
    return [l for l in content.splitlines() if l.startswith("+pegasus_predict")]


class TestPatchSubFile:
    def _make_sub(self, path: Path):
        path.write_text(
            "executable = /bin/myjob\n"
            "arguments  = input.txt\n"
            "periodic_remove = (JobStatus == 5)\n"
            "queue\n"
        )

    def test_injects_classads(self, tmp_path):
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        ok = patch_sub_file(str(sub), {
            "predicted_runtime_s": 120,
            "lower_bound_s":       60,
            "upper_bound_s":       240,
            "status":              "NORMAL",
        })
        assert ok
        content = sub.read_text()
        assert "+pegasus_predicted_runtime" in content
        assert "+pegasus_predicted_runtime_low" in content
        assert "+pegasus_predicted_runtime_high" in content
        assert '+pegasus_prediction_status      = "NORMAL"' in content

    def test_updates_periodic_remove(self, tmp_path):
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        patch_sub_file(str(sub), {
            "predicted_runtime_s": 60,
            "upper_bound_s":       120,
            "status":              "NORMAL",
        })
        content = sub.read_text()
        assert "periodic_remove" in content
        # Our timeout condition is appended
        assert "CurrentTime - EnteredCurrentStatus" in content

    def test_returns_false_for_missing_file(self, tmp_path):
        ok = patch_sub_file(str(tmp_path / "missing.sub"), {"predicted_runtime_s": 10})
        assert ok is False

    def test_returns_false_without_queue(self, tmp_path):
        sub = tmp_path / "job.sub"
        sub.write_text("executable = /bin/myjob\n")
        assert patch_sub_file(str(sub), {"predicted_runtime_s": 10}) is False
        assert sub.read_text() == "executable = /bin/myjob\n"

    def test_ads_before_queue(self, tmp_path):
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        patch_sub_file(str(sub), {"predicted_runtime_s": 30, "status": "SPARSE"})
        lines = sub.read_text().splitlines()
        queue_idx = next(i for i, l in enumerate(lines) if l.strip().lower().startswith("queue"))
        predicted_idx = next(i for i, l in enumerate(lines) if "+pegasus_predicted_runtime " in l)
        assert predicted_idx < queue_idx

    def test_periodic_remove_expression(self, tmp_path):
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        # timeout is 3x the upper bound, with a minimum of an hour
        patch_sub_file(str(sub), {"predicted_runtime_s": 60, "upper_bound_s": 2000})
        assert (
            "periodic_remove = ((JobStatus == 5)) || ((JobStatus == 2) && "
            "((CurrentTime - EnteredCurrentStatus) > 6000))"
        ) in sub.read_text().splitlines()

    def test_idempotent(self, tmp_path):
        # the prescript is rerun on job retries, patching the same file again
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        for upper in (240, 240, 2000):
            assert patch_sub_file(str(sub), {
                "predicted_runtime_s": 120,
                "lower_bound_s":       60,
                "upper_bound_s":       upper,
                "status":              "NORMAL",
            })
        content = sub.read_text()
        periodic_remove = [l for l in content.splitlines() if l.startswith("periodic_remove")]
        assert periodic_remove == [
            "periodic_remove = ((JobStatus == 5)) || ((JobStatus == 2) && "
            "((CurrentTime - EnteredCurrentStatus) > 6000))"
        ]
        # one set of ads, with the values from the last patch
        assert _prediction_ads(content) == [
            "+pegasus_predicted_runtime      = 120",
            "+pegasus_predicted_runtime_low  = 60",
            "+pegasus_predicted_runtime_high = 2000",
            '+pegasus_prediction_status      = "NORMAL"',
        ]
        assert content.count("queue") == 1


# ---------------------------------------------------------------------------
# condor_status cache and slot
# ---------------------------------------------------------------------------

_SLOTS = [
    {"Name": "slot1@h1", "Machine": "h1", "Cpus": 8, "ProcessorSpeed": 3000,
     "TotalSlotMemory": 16384},
]


class _FakeCondorStatus:
    """Stands in for subprocess.check_output, recording the commands run."""

    def __init__(self, slots=None, fail=False):
        self.slots = _SLOTS if slots is None else slots
        self.fail = fail
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        if self.fail:
            raise subprocess.CalledProcessError(1, cmd)
        return json.dumps(self.slots).encode()


@pytest.fixture
def condor_status(monkeypatch):
    fake = _FakeCondorStatus()
    monkeypatch.setattr(predictor.subprocess, "check_output", fake)
    return fake


def _make_stale(path: Path):
    old = time.time() - 2 * CONDOR_STATUS_CACHE_MAX_AGE
    os.utime(path, (old, old))


class TestCondorStatusJson:
    def test_queries_only_attributes_used(self, tmp_path, condor_status):
        _condor_status_json(str(tmp_path / "cache.json"))
        assert condor_status.calls == [
            ["condor_status", "-json", "-attributes", ",".join(CONDOR_STATUS_ATTRIBUTES)]
        ]

    def test_writes_cache(self, tmp_path, condor_status):
        cache = tmp_path / "cache.json"
        assert _condor_status_json(str(cache)) == _SLOTS
        assert json.loads(cache.read_text()) == _SLOTS
        # no temporary files are left behind
        assert os.listdir(tmp_path) == ["cache.json"]

    def test_uses_fresh_cache(self, tmp_path, condor_status):
        cache = tmp_path / "cache.json"
        _condor_status_json(str(cache))
        assert _condor_status_json(str(cache)) == _SLOTS
        assert len(condor_status.calls) == 1

    def test_refreshes_stale_cache(self, tmp_path, condor_status):
        cache = tmp_path / "cache.json"
        cache.write_text(json.dumps([{"Name": "old"}]))
        _make_stale(cache)
        assert _condor_status_json(str(cache)) == _SLOTS
        assert len(condor_status.calls) == 1
        assert json.loads(cache.read_text()) == _SLOTS

    def test_stale_cache_when_condor_status_fails(self, tmp_path, condor_status):
        cache = tmp_path / "cache.json"
        cache.write_text(json.dumps([{"Name": "old"}]))
        _make_stale(cache)
        condor_status.fail = True
        assert _condor_status_json(str(cache)) == [{"Name": "old"}]

    def test_none_when_condor_status_fails(self, tmp_path, condor_status):
        condor_status.fail = True
        assert _condor_status_json(str(tmp_path / "cache.json")) is None

    def test_default_cache_file_in_current_directory(
        self, tmp_path, monkeypatch, condor_status
    ):
        monkeypatch.chdir(tmp_path)
        _condor_status_json()
        assert (tmp_path / CONDOR_STATUS_CACHE_FILE).exists()


class TestGetCondorSlot:
    def test_highest_speed_slot(self, tmp_path, condor_status):
        condor_status.slots = [
            {"Name": "slot1@slow", "Machine": "slow", "Cpus": 4, "ProcessorSpeed": 2000},
            {"Name": "slot1@fast", "Machine": "fast", "Cpus": 8, "ProcessorSpeed": 3000,
             "TotalSlotMemory": 16384},
        ]
        slot = _get_condor_slot(str(tmp_path / "cache.json"))
        assert slot == {
            "name":      "slot1@fast",
            "cpu_count": 8,
            "cpu_speed": 3000.0,
            "ram":       16384 * 1024.0,
            "hostname":  "fast",
        }

    def test_skips_dynamic_slots(self, tmp_path, condor_status):
        condor_status.slots = [
            {"Name": "slot1_1@h1", "Machine": "h1", "ProcessorSpeed": 9000},
            {"Name": "slot1@h1", "Machine": "h1", "ProcessorSpeed": 2000},
        ]
        assert _get_condor_slot(str(tmp_path / "cache.json"))["name"] == "slot1@h1"

    def test_cpu_speed_from_kflops(self, tmp_path, condor_status):
        condor_status.slots = [{"Name": "slot1@h1", "Machine": "h1", "KFlops": 2_500_000}]
        assert _get_condor_slot(str(tmp_path / "cache.json"))["cpu_speed"] == 2500.0

    def test_defaults_without_condor(self, tmp_path, condor_status):
        condor_status.fail = True
        assert _get_condor_slot(str(tmp_path / "cache.json")) == predictor._DEFAULT_SLOT


# ---------------------------------------------------------------------------
# Inputs of the job: submit file, LOF, .meta files and workflow YAML
# ---------------------------------------------------------------------------

class TestSplitTcName:
    @pytest.mark.parametrize("tc_name,expected", [
        ("pegasus::preprocess:4.0", ("pegasus", "preprocess", "4.0")),
        ("pegasus::preprocess",     ("pegasus", "preprocess", None)),
        ("preprocess:4.0",          (None, "preprocess", "4.0")),
        ("preprocess",              (None, "preprocess", None)),
    ])
    def test_split(self, tc_name, expected):
        assert _split_tc_name(tc_name) == expected


class TestJobFromSub:
    def test_job(self, tmp_path):
        _, info = parse_sub_file(_write_sub(tmp_path / "preprocess_ID0000001.sub"))
        job = _job_from_sub("ID0000001", info)
        assert job._id == "ID0000001"
        assert (job.namespace, job.transformation, job.version) == (
            "pegasus", "preprocess", "4.0"
        )
        assert dict(job.profiles["condor"]) == {
            "request_cpus":   "2",
            "request_gpus":   "0",
            "request_memory": "2048",
            # condor defaults to KiB for request_disk
            "request_disk":   "5120 MB",
        }

    def test_no_transformation(self, capsys):
        job = _job_from_sub("ID0000001", {"sub_path": "job.sub"})
        assert job.transformation == "unknown"
        assert "No transformation found in job.sub" in capsys.readouterr().err


class TestReadLof:
    def test_lfns(self, tmp_path):
        lof = tmp_path / "job.in.lof"
        lof.write_text("# comment\nf.a=f.a\n\nf.b=f.b\n")
        assert _read_lof(str(lof)) == ["f.a", "f.b"]


def _write_meta(path: Path, sizes: dict):
    path.write_text(json.dumps([
        {"_id": lfn, "_type": "file", "_attributes": {"size": str(size)}}
        for lfn, size in sizes.items()
    ]))
    return path


class TestReadMetaSizes:
    def test_sizes(self, tmp_path):
        a = _write_meta(tmp_path / "a.meta", {"f.a": 100})
        b = _write_meta(tmp_path / "b.meta", {"f.b": 200})
        assert _read_meta_sizes([str(a), str(b)]) == {"f.a": 100, "f.b": 200}

    def test_entries_without_size(self, tmp_path):
        meta = tmp_path / "job.meta"
        meta.write_text(json.dumps([{"_id": "f.a", "_type": "file", "_attributes": {}}]))
        assert _read_meta_sizes([str(meta)]) == {}

    def test_unreadable_meta_file(self, tmp_path, capsys):
        bad = tmp_path / "bad.meta"
        bad.write_text("not json")
        assert _read_meta_sizes([str(bad), str(tmp_path / "missing.meta")]) == {}
        err = capsys.readouterr().err
        assert "Unable to read meta file" in err
        assert "missing.meta" in err


def _write_workflow(path: Path):
    """Write a workflow YAML with the two findrange tasks of a cluster."""
    path.write_text(json.dumps({
        "name": "diamond",
        "jobs": [
            {
                "id": job_id,
                "name": "findrange",
                "uses": [
                    {"lfn": lfn_in, "type": "input", "size": 1000},
                    {"lfn": lfn_out, "type": "output"},
                ],
                "profiles": {"condor": {"request_cpus": 1}},
            }
            for job_id, lfn_in, lfn_out in [
                ("ID0000002", "f.b1", "f.c1"),
                ("ID0000003", "f.b2", "f.c2"),
            ]
        ] + [{"id": "ID0000004", "name": "analyze", "uses": []}],
    }))
    return path


class TestLoadJobsFromYaml:
    def test_loads_only_given_jobs(self, tmp_path):
        wf = _load_jobs_from_yaml(
            str(_write_workflow(tmp_path / "workflow.yml")), ["ID0000002", "ID0000003"]
        )
        assert sorted(wf.jobs) == ["ID0000002", "ID0000003"]
        job = wf.jobs["ID0000002"]
        assert [(f.lfn, f.size) for f in job.get_inputs()] == [("f.b1", 1000)]
        assert [f.lfn for f in job.get_outputs()] == ["f.c1"]
        assert dict(job.profiles["condor"]) == {"request_cpus": "1"}


# ---------------------------------------------------------------------------
# WorkflowRuntimePredictor.predict, with the model mocked out
# ---------------------------------------------------------------------------

def _result(job_id, runtime):
    return {
        "job_id":              job_id,
        "predicted_runtime_s": runtime,
        "lower_bound_s":       runtime / 2,
        "upper_bound_s":       runtime * 2,
        "status":              "NORMAL",
    }


class _PredictorWithoutModel(WorkflowRuntimePredictor):
    """
    Predicts a fixed runtime for each job, without loading a model, and
    records what predict_job is called with.
    """

    def __init__(self, runtime=100.0):
        self._cfg = RuntimePredictionConfig(enabled=True)
        self.runtime = runtime
        self.calls = []

    def predict_job(self, workflow, level, sub_resources=None, slot=None):
        self.calls.append({"workflow": workflow, "level": level,
                           "sub_resources": sub_resources})
        return [_result(job_id, self.runtime) for job_id in sorted(workflow.jobs)]


class TestPredict:
    def test_job(self, tmp_path, capsys):
        sub = _write_sub(tmp_path / "preprocess_ID0000001.sub")
        lof = tmp_path / "preprocess_ID0000001.in.lof"
        lof.write_text("f.a=f.a\nf.b=f.b\n")
        meta = _write_meta(tmp_path / "stage_in.meta", {"f.a": 100})

        p = _PredictorWithoutModel(runtime=100.0)
        p.predict("workflow.yml", str(sub), target_level=1,
                  lof_file=str(lof), meta_files=[str(meta)])

        call, = p.calls
        assert call["level"] == 1
        job = call["workflow"].jobs["ID0000001"]
        assert job.transformation == "preprocess"
        # inputs from the LOF, with the sizes from the .meta files
        assert sorted((f.lfn, f.size) for f in job.get_inputs()) == [
            ("f.a", 100), ("f.b", None)
        ]
        assert call["sub_resources"]["ID0000001"]["pegasus_cores"] == "2"
        assert "WARNING: No size found in meta files for input f.b of job ID0000001" \
            in capsys.readouterr().err

        assert _prediction_ads(sub.read_text())[0] == "+pegasus_predicted_runtime      = 100"

    def test_clustered_job(self, tmp_path):
        sub = _write_sub(tmp_path / "merge_findrange-4_0_PID2_ID1.sub",
                         dax_job_id="null", cluster_size=2,
                         xformation="pegasus::seqexec")
        _write_cluster_in(tmp_path / "merge_findrange-4_0_PID2_ID1.in",
                          [("pegasus::findrange:4.0", "ID0000002"),
                           ("pegasus::findrange:4.0", "ID0000003")])
        workflow = _write_workflow(tmp_path / "workflow.yml")
        meta = _write_meta(tmp_path / "preprocess.meta", {"f.b1": 500})

        p = _PredictorWithoutModel(runtime=100.0)
        p.predict(str(workflow), str(sub), meta_files=[str(meta)])

        call, = p.calls
        # the tasks of the cluster are predicted, with the sizes from the .meta
        # files overriding the ones in the workflow
        assert sorted(call["workflow"].jobs) == ["ID0000002", "ID0000003"]
        job = call["workflow"].jobs["ID0000002"]
        assert [(f.lfn, f.size) for f in job.get_inputs()] == [("f.b1", 500)]
        # the resources in the submit file are for the cluster, not its tasks
        assert call["sub_resources"] == {}

        # the predictions of the tasks are summed up
        assert _prediction_ads(sub.read_text()) == [
            "+pegasus_predicted_runtime      = 200",
            "+pegasus_predicted_runtime_low  = 100",
            "+pegasus_predicted_runtime_high = 400",
            '+pegasus_prediction_status      = "CLUSTERED"',
        ]

    def test_missing_submit_file(self, tmp_path):
        with pytest.raises(PredictorError, match="Unable to read submit file"):
            _PredictorWithoutModel().predict("workflow.yml", str(tmp_path / "missing.sub"))

    def test_no_job_id(self, tmp_path):
        sub = _write_sub(tmp_path / "stage_in_local_0.sub", dax_job_id="null")
        with pytest.raises(PredictorError, match="No job id found"):
            _PredictorWithoutModel().predict("workflow.yml", str(sub))

    def test_cluster_without_in_file(self, tmp_path):
        sub = _write_sub(tmp_path / "merge_findrange-4_0_PID2_ID1.sub",
                         dax_job_id="null", cluster_size=2)
        with pytest.raises(PredictorError, match="No job id found"):
            _PredictorWithoutModel().predict("workflow.yml", str(sub))

    def test_no_prediction(self, tmp_path):
        sub = _write_sub(tmp_path / "preprocess_ID0000001.sub")
        p = _PredictorWithoutModel()
        p.predict_job = lambda *args, **kwargs: []
        with pytest.raises(PredictorError, match="No prediction found for ID0000001"):
            p.predict("workflow.yml", str(sub))


# ---------------------------------------------------------------------------
# ModelContext + WorkflowRuntimePredictor (torch required)
# ---------------------------------------------------------------------------

@requires_torch
class TestModelContextMocked:
    """Load a mocked .pkl without a real trained model."""

    def _make_pkl(self, tmp_path) -> str:
        import torch
        import torch.nn as nn
        from sklearn.preprocessing import StandardScaler
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.decomposition import TruncatedSVD

        # Minimal arts dict matching what predictor.py expects
        vec = TfidfVectorizer(max_features=5)
        vec.fit(["preprocess findrange analyze"])
        n_svd = 1  # with a tiny vocab, TruncatedSVD reliably yields 1 component
        svd = TruncatedSVD(n_components=n_svd)
        svd.fit(vec.transform(["preprocess findrange analyze"]))

        input_dim = 9 + n_svd  # 9 numeric cols + SVD dims
        scaler = StandardScaler()
        scaler.fit(np.zeros((1, input_dim)))

        arts = {
            "name_vec":     vec,
            "name_svd":     svd,
            "trans_map":    {"preprocess": 10.0},
            "synth_map":    {"preprocess": 10.0},
            "bucket_map":   {},
            "global_median": 10.0,
            "res_med":      {},
            "output_ratio": {},
            "trans_counts": {"preprocess": 100},
            "smart_rule":   {},
        }

        from Pegasus.runtime.predictor import PegasusOracle
        model = PegasusOracle(input_dim=input_dim, latent_dim=4)

        payload = {
            "scaler":      scaler,
            "numeric_cols": [
                "log_anchor_runtime", "log_cpu_power", "log_ram",
                "log_input_bytes", "log_io_intensity", "log_compute_intensity",
                "memory_pressure", "site_encoded", "log_input_files_count",
            ],
            "arts":        arts,
            "q_lo":        0.05,
            "q_hi":        0.95,
            "rule_map":    {},
            "lat_dim":     4,
            "input_dim":   input_dim,
            "sparse_thr":  10,
            "model_state": model.state_dict(),
        }

        pkl_path = str(tmp_path / "model.pkl")
        with open(pkl_path, "wb") as f:
            pickle.dump(payload, f)
        return pkl_path

    def test_load_model(self, tmp_path):
        from Pegasus.runtime.predictor import ModelContext
        ctx = ModelContext(self._make_pkl(tmp_path))
        assert ctx.input_dim == 10  # 9 numeric + 1 SVD component
        assert ctx.lat_dim == 4
        assert ctx.sparse_thr == 10

    def test_predict_returns_arrays(self, tmp_path):
        from Pegasus.runtime.predictor import ModelContext
        ctx = ModelContext(self._make_pkl(tmp_path))
        df = pd.DataFrame([{
            "job_id":            "job0",
            "transformation":    "preprocess",
            "cpu_count":         4,
            "request_cpus":      1,
            "cpu_speed":         2600.0,
            "ram":               8_388_608.0,
            "input_bytes_total": 1_000_000.0,
            "input_files_count": 2,
            "hostname":          "isi.edu",
        }])
        p_med, p_low, p_high, statuses, anomaly, conf, _, _ = ctx.predict(df)
        assert len(p_med) == 1
        assert statuses[0] in ("NORMAL", "SPARSE", "ZERO_SHOT", "RULE_BASED")
        # confidence is NaN with a single-sample batch (std=0); just check it's a float
        assert isinstance(float(conf[0]), float)
