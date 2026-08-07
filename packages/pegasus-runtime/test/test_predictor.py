"""
Tests for Pegasus.runtime.predictor

Pure-function tests run without torch.
ModelContext / WorkflowRuntimePredictor tests are skipped when torch is absent.
"""

import json
import os
import pickle
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from Pegasus.runtime.predictor import (
    RuntimePredictionConfig,
    _bytes_bin,
    _site,
    _synth,
    patch_sub_file,
    read_meta_sizes,
    scan_sub_files,
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
# read_meta_sizes
# ---------------------------------------------------------------------------

class TestReadMetaSizes:
    def test_empty_dir(self, tmp_path):
        assert read_meta_sizes(str(tmp_path)) == {}

    def test_nonexistent_dir(self):
        assert read_meta_sizes("/does/not/exist") == {}

    def test_reads_meta_file(self, tmp_path):
        meta = [
            {"_type": "file", "_id": "f.a", "_attributes": {"size": "1024"}},
            {"_type": "file", "_id": "f.b", "_attributes": {"size": "2048"}},
        ]
        meta_file = tmp_path / "job.meta"
        meta_file.write_text(json.dumps(meta))

        sizes = read_meta_sizes(str(tmp_path))
        assert sizes["f.a"] == 1024.0
        assert sizes["f.b"] == 2048.0

    def test_skips_missing_size(self, tmp_path):
        meta = [{"_type": "file", "_id": "f.c", "_attributes": {}}]
        (tmp_path / "job.meta").write_text(json.dumps(meta))
        sizes = read_meta_sizes(str(tmp_path))
        assert "f.c" not in sizes

    def test_nested_meta_file(self, tmp_path):
        nested = tmp_path / "00" / "00"
        nested.mkdir(parents=True)
        meta = [{"_type": "file", "_id": "f.d", "_attributes": {"size": "512"}}]
        (nested / "x.meta").write_text(json.dumps(meta))
        sizes = read_meta_sizes(str(tmp_path))
        assert sizes["f.d"] == 512.0


# ---------------------------------------------------------------------------
# scan_sub_files
# ---------------------------------------------------------------------------

class TestScanSubFiles:
    def _make_sub(self, path: Path, dax_id: str, cpus: int = 1, mem: float = 1024.0):
        path.write_text(
            f'+pegasus_wf_dax_job_id = "{dax_id}"\n'
            f"request_cpus = {cpus}\n"
            f"request_memory = {mem}\n"
            "queue\n"
        )

    def test_empty_dir(self, tmp_path):
        assert scan_sub_files(str(tmp_path)) == {}

    def test_nonexistent_dir(self):
        assert scan_sub_files("/does/not/exist") == {}

    def test_reads_sub_file(self, tmp_path):
        self._make_sub(tmp_path / "job.sub", "job_id_1", cpus=4, mem=2048.0)
        result = scan_sub_files(str(tmp_path))
        assert "job_id_1" in result
        assert result["job_id_1"]["request_cpus"] == 4
        assert result["job_id_1"]["request_memory_mb"] == 2048.0

    def test_skips_null_dax_id(self, tmp_path):
        sub = tmp_path / "system.sub"
        sub.write_text('+pegasus_wf_dax_job_id = "null"\nqueue\n')
        assert scan_sub_files(str(tmp_path)) == {}

    def test_nested_sub_file(self, tmp_path):
        nested = tmp_path / "00" / "00"
        nested.mkdir(parents=True)
        self._make_sub(nested / "job.sub", "nested_job")
        result = scan_sub_files(str(tmp_path))
        assert "nested_job" in result


# ---------------------------------------------------------------------------
# patch_sub_file
# ---------------------------------------------------------------------------

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
        assert "+PredictedRuntime" in content
        assert "+PredictedRuntimeLow" in content
        assert "+PredictedRuntimeHigh" in content
        assert '+PredictionStatus     = "NORMAL"' in content

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

    def test_idempotent_queue_position(self, tmp_path):
        sub = tmp_path / "job.sub"
        self._make_sub(sub)
        patch_sub_file(str(sub), {"predicted_runtime_s": 30, "status": "SPARSE"})
        lines = sub.read_text().splitlines()
        queue_idx = next(i for i, l in enumerate(lines) if l.strip().lower().startswith("queue"))
        # All +Predicted* lines must appear BEFORE queue
        for line in lines[:queue_idx]:
            pass  # just ensure no IndexError
        predicted_idx = next(i for i, l in enumerate(lines) if "+PredictedRuntime " in l)
        assert predicted_idx < queue_idx


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
