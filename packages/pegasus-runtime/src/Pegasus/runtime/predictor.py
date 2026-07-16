"""
PegasusOracle — Embedded Workflow Runtime Predictor

Loads a trained PegasusOracle model (.pkl) and predicts runtimes for ALL
jobs in a workflow in a single batch pass — no external API server required.

Output written to the workflow output directory:
  runtime_predictions.csv   — one row per job
  runtime_predictions.json  — same data + summary (status counts, slot info)

Architecture matches pegasus_oracle.py exactly so the same .pkl file is portable.
"""

import json
import math
import os
import pickle
import re
import subprocess
import warnings
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Default model location: <package>/models/pegasus_oracle_model.pkl
# Drop the trained .pkl there and leave model_path unset — it is found automatically.
_MODELS_DIR    = Path(__file__).parent / "models"
_DEFAULT_MODEL = _MODELS_DIR / "pegasus_oracle_model.pkl"


# ─────────────────────────────────────────────
# 0. CONFIGURATION
# ─────────────────────────────────────────────

@dataclass
class RuntimePredictionConfig:
    """
    Configuration for the workflow-level runtime predictor.

    All fields are optional — unset values fall back to the model's own
    defaults or to condor_status discovery.

    Can be built manually or loaded from Pegasus properties::

        cfg = RuntimePredictionConfig.from_properties(props)

    Pegasus property keys
    ---------------------
    pegasus.runtime.prediction.enable          : "true" / "false"
    pegasus.runtime.prediction.model.path      : /path/to/pegasus_oracle_model.pkl
    pegasus.runtime.prediction.output.dir      : /path/to/output/dir
    pegasus.runtime.prediction.sparse.threshold: integer  (default: model value)
    pegasus.runtime.prediction.interval.low    : float 0–1 (default: 0.05)
    pegasus.runtime.prediction.interval.high   : float 0–1 (default: 0.95)
    pegasus.runtime.prediction.slot.hostname   : override condor_status hostname
    pegasus.runtime.prediction.slot.cpu.count  : override cpu count
    pegasus.runtime.prediction.slot.cpu.speed  : override cpu speed (MHz)
    pegasus.runtime.prediction.slot.ram        : override RAM (kB)
    """

    # core
    enabled:          bool            = True
    model_path:       Optional[str]   = None
    output_dir:       Optional[str]   = None

    # model overrides (None = use value from loaded .pkl)
    sparse_threshold: Optional[int]   = None
    q_lo:             Optional[float] = None   # e.g. 0.05
    q_hi:             Optional[float] = None   # e.g. 0.95

    # condor slot overrides (None = query condor_status)
    slot_hostname:    Optional[str]   = None
    slot_cpu_count:   Optional[int]   = None
    slot_cpu_speed:   Optional[float] = None   # MHz
    slot_ram:         Optional[float] = None   # kB

    @classmethod
    def from_properties(cls, props) -> "RuntimePredictionConfig":
        """
        Build a config from a Pegasus :py:class:`~Pegasus.api.properties.Properties`
        object or any dict-like mapping of pegasus property keys to values.

        :param props: Properties object or dict with pegasus property keys
        :return: RuntimePredictionConfig
        """
        def _get(key, default=None):
            try:
                return props[key]
            except (KeyError, TypeError):
                return default

        def _float(key):
            v = _get(key)
            return float(v) if v is not None else None

        def _int(key):
            v = _get(key)
            return int(v) if v is not None else None

        def _bool(key, default=True):
            v = _get(key)
            if v is None:
                return default
            return str(v).lower() in ("true", "1", "yes")

        return cls(
            enabled          = _bool("pegasus.runtime.prediction.enable"),
            model_path       = _get("pegasus.runtime.prediction.model.path"),
            output_dir       = _get("pegasus.runtime.prediction.output.dir"),
            sparse_threshold = _int("pegasus.runtime.prediction.sparse.threshold"),
            q_lo             = _float("pegasus.runtime.prediction.interval.low"),
            q_hi             = _float("pegasus.runtime.prediction.interval.high"),
            slot_hostname    = _get("pegasus.runtime.prediction.slot.hostname"),
            slot_cpu_count   = _int("pegasus.runtime.prediction.slot.cpu.count"),
            slot_cpu_speed   = _float("pegasus.runtime.prediction.slot.cpu.speed"),
            slot_ram         = _float("pegasus.runtime.prediction.slot.ram"),
        )


# ─────────────────────────────────────────────
# 1. NEURAL ARCHITECTURE  (must match pegasus_oracle.py)
# ─────────────────────────────────────────────

try:
    import torch
    import torch.nn as nn

    class Mish(nn.Module):
        def forward(self, x):
            return x * torch.tanh(nn.functional.softplus(x))

    class ResBlock(nn.Module):
        def __init__(self, dim):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(dim, dim), nn.LayerNorm(dim), Mish(),
                nn.Linear(dim, dim), nn.LayerNorm(dim),
            )
        def forward(self, x): return x + self.net(x)

    class PegasusOracle(nn.Module):
        def __init__(self, input_dim: int, latent_dim: int = 8):
            super().__init__()
            H = 512
            self.backbone = nn.Sequential(
                nn.Linear(input_dim, H), nn.LayerNorm(H), Mish(), nn.Dropout(0.10),
                ResBlock(H), ResBlock(H), ResBlock(H), ResBlock(H),
                nn.Linear(H, 256), Mish(), nn.Dropout(0.05),
                nn.Linear(256, 128), Mish(),
            )
            self.median_head = nn.Linear(128, 1)
            self.low_offset  = nn.Linear(128, 1)
            self.high_offset = nn.Linear(128, 1)
            self.enc = nn.Sequential(
                nn.Linear(input_dim, 128), Mish(),
                nn.Linear(128, 64),        Mish(),
            )
            self.fc_mu     = nn.Linear(64, latent_dim)
            self.fc_logvar = nn.Linear(64, latent_dim)
            self.dec = nn.Sequential(
                nn.Linear(latent_dim, 64),  Mish(),
                nn.Linear(64, 128),         Mish(),
                nn.Linear(128, input_dim),
            )

        def encode(self, x):
            h = self.enc(x)
            return self.fc_mu(h), self.fc_logvar(h)

        def reparameterize(self, mu, logvar):
            return mu + torch.randn_like(mu) * torch.exp(0.5 * logvar)

        def forward(self, x):
            f   = self.backbone(x)
            med = self.median_head(f)
            low = med - nn.functional.softplus(self.low_offset(f))
            hig = med + nn.functional.softplus(self.high_offset(f))
            mu, lv = self.encode(x)
            z      = self.reparameterize(mu, lv)
            recon  = self.dec(z)
            return (med, low, hig), recon, mu, lv

    _TORCH_AVAILABLE = True

except ImportError:
    _TORCH_AVAILABLE = False


# ─────────────────────────────────────────────
# 2. FEATURE ENGINEERING  (must match pegasus_oracle.py)
# ─────────────────────────────────────────────

NUMERIC_COLS = [
    "log_anchor_runtime", "log_cpu_power", "log_ram",
    "log_input_bytes", "log_io_intensity", "log_compute_intensity",
    "memory_pressure", "site_encoded", "log_input_files_count",
]

RULE_BASED = {
    "system::chmod", "pegasus::transfer", "sleep",
    "pegasus::cleanup", "pegasus::dirmanager",
    "pegasus::checkpoint:4.0", "preprocess", "mkdir",
    "merge", "hello", "world",
}


def _synth(j):
    return re.split(r"[-_]", str(j))[0] if j and not pd.isna(j) else "unknown"


def _site(h):
    """
    Map a hostname or Pegasus site name to the integer encoding used during training.
    0=ISI, 1=WSU/Hellbender, 2=Anvil/Purdue, 3=Stampede/TACC, 4=Expanse/SDSC, 5=other.
    Extended to cover dominant hostname patterns in the training data.
    """
    h = str(h).lower()
    for k, v in [
        ("isi",        0),
        ("wsu",        1), ("hellbender",  1), ("wustl",      1),
        ("anvil",      2), ("purdue",      2),
        ("stampede",   3), ("tacc",        3), ("frontera",   3),
        ("expanse",    4), ("sdsc",        4),
    ]:
        if k in h:
            return v
    return 5  # OSG / unknown — largest bucket in training (73 % of rows)


def _bytes_bin(b):
    if b <= 0:   return 0
    if b < 1e4:  return 1
    if b < 1e5:  return 2
    if b < 1e6:  return 3
    if b < 1e7:  return 4
    if b < 1e8:  return 5
    return 6


def _bucket_key(row):
    return (
        _bytes_bin(row.get("input_bytes_total", 0)),
        int(row.get("cpu_count", 1)),
        _site(str(row.get("hostname", ""))),
    )


def _transform_names(transformations, vec, svd):
    names = [str(t) if t else "unknown" for t in transformations]
    return svd.transform(vec.transform(names)).astype(np.float32)


def _make_features(df: pd.DataFrame, a: dict):
    df = df.copy()
    df["synth"] = df["job_id"].apply(_synth) if "job_id" in df.columns else "unknown"
    for col, mm in a.get("res_med", {}).items():
        if col in df.columns:
            df[col] = df[col].replace(0, np.nan).fillna(
                df["synth"].map(mm)).fillna(a["global_median"])
    t           = (df["transformation"].map(a.get("trans_map", {}))
                   if "transformation" in df.columns
                   else pd.Series(np.nan, index=df.index))
    s           = df["synth"].map(a.get("synth_map", {}))
    df["_bkey"] = df.apply(_bucket_key, axis=1)
    b_fallback  = df["_bkey"].map(a.get("bucket_map", {}))
    df["anchor_runtime"]        = t.fillna(s).fillna(b_fallback).fillna(
                                    a.get("global_median", 0))
    df["log_anchor_runtime"]    = np.log1p(df["anchor_runtime"])
    df["cpu_power"]             = df.get("cpu_count", 1) * df.get("cpu_speed", 1000)
    df["log_cpu_power"]         = np.log1p(df["cpu_power"])
    df["log_ram"]               = np.log1p(df.get("ram", 0))
    df["log_input_bytes"]       = np.log1p(df.get("input_bytes_total", 0))
    df["io_intensity"]          = (df.get("input_bytes_total", 0) /
                                   (df.get("input_files_count", 0) + 1))
    df["log_io_intensity"]      = np.log1p(df["io_intensity"])
    df["compute_intensity"]     = (df.get("input_bytes_total", 0) /
                                   (df["cpu_power"] + 1e-6))
    df["log_compute_intensity"] = np.log1p(df["compute_intensity"])
    # memory_pressure uses request_cpus (what the job asked for) so it reflects
    # the per-CPU memory allocation, not the machine total.
    req_cpus_col = df["request_cpus"] if "request_cpus" in df.columns else df.get("cpu_count", 1)
    df["memory_pressure"]       = df.get("ram", 0) / (req_cpus_col + 1e-6)
    df["site_encoded"]          = (df["hostname"].apply(_site)
                                   if "hostname" in df.columns else 0)
    df["log_input_files_count"] = np.log1p(df.get("input_files_count", 0))
    name_emb = _transform_names(
        df.get("transformation", pd.Series(["unknown"] * len(df))),
        a["name_vec"], a["name_svd"])
    return df.fillna(0), name_emb


def _assemble_X(df_eng, name_emb, numeric_cols):
    return np.concatenate(
        [df_eng[numeric_cols].values.astype(np.float32), name_emb], axis=1)


# ─────────────────────────────────────────────
# 3. CONDOR SLOT  (from extract_features.py)
# ─────────────────────────────────────────────

_DEFAULT_SLOT = {
    "name":      "unknown",
    "cpu_count": 4,
    "cpu_speed": 2600.0,
    "ram":       8_388_608.0,   # 8 GB in kB
    "hostname":  "unknown",
}


def _get_condor_slot() -> dict:
    """
    Query condor_status -json and return the highest-speed slot.
    Falls back silently to defaults when condor is not available.
    """
    try:
        out       = subprocess.check_output(
            ["condor_status", "-json"], stderr=subprocess.DEVNULL, timeout=15)
        slots_raw = json.loads(out)
    except Exception:
        return dict(_DEFAULT_SLOT)

    best = None
    for s in slots_raw:
        name = str(s.get("Name", s.get("Machine", "unknown")))
        if re.search(r"slot\d+_\d+", name):
            continue  # skip dynamic sub-slots

        cpu_speed = float(s.get("ProcessorSpeed", 0))
        if cpu_speed == 0:
            kflops    = float(s.get("KFlops", 0))
            cpu_speed = round(kflops / 1_000, 1) if kflops > 0 else 2_600.0

        slot = {
            "name":      name,
            "cpu_count": int(s.get("Cpus", s.get("TotalSlotCpus", 1))),
            "cpu_speed": cpu_speed,
            "ram":       float(s.get("TotalSlotMemory",
                                     s.get("Memory", 8_192))) * 1_024,
            "hostname":  str(s.get("Machine", name)),
        }
        if best is None or slot["cpu_speed"] > best["cpu_speed"]:
            best = slot

    return best or dict(_DEFAULT_SLOT)


# ─────────────────────────────────────────────
# 4. MODEL CONTEXT  (inference only, matches api.py)
# ─────────────────────────────────────────────

class ModelContext:
    def __init__(self, path: str):
        if not _TORCH_AVAILABLE:
            raise ImportError(
                "Runtime prediction requires PyTorch. "
                "Install with: pip install torch"
            )
        with open(path, "rb") as f:
            payload = pickle.load(f)

        self.scaler       = payload["scaler"]
        self.numeric_cols = payload["numeric_cols"]
        self.arts         = payload["arts"]
        self.q_lo         = payload["q_lo"]
        self.q_hi         = payload["q_hi"]
        self.rule_map     = payload["rule_map"]
        self.lat_dim      = payload["lat_dim"]
        self.input_dim    = payload["input_dim"]
        self.sparse_thr   = payload.get("sparse_thr", 10)
        self.device       = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu")

        self.model = PegasusOracle(self.input_dim, latent_dim=self.lat_dim)
        self.model.load_state_dict(payload["model_state"])
        self.model.to(self.device).eval()

        self.known_trans  = set(self.arts["trans_map"].keys())
        self.trans_counts = self.arts.get("trans_counts", {})
        self.smart_rule   = self.arts.get("smart_rule", {})

        # Reverse lookup: bare name (no ns:: or :version) → best canonical name.
        # "best" = highest training sample count when multiple canonical names
        # share the same bare name (e.g. "diamond::findrange:4.0" and
        # "pegasus::findrange:4.0" both strip to "findrange").
        # Built once at load time so _resolve() is O(1) per job.
        self._bare_to_canonical: dict = {}
        for cname in self.known_trans:
            bare = re.sub(r":[^:]+$", "", re.sub(r"^[^:]+::", "", cname))
            if (bare not in self._bare_to_canonical or
                    self.trans_counts.get(cname, 0) >
                    self.trans_counts.get(self._bare_to_canonical[bare], 0)):
                self._bare_to_canonical[bare] = cname

    def _resolve_name(self, t: str) -> str:
        """
        Resolve a raw transformation name to its canonical training-data name.
        Four-step lookup (each step only runs if the previous one misses):
          1. Exact match in known_trans / rule_map / smart_rule
          2. Strip namespace prefix  e.g. "diamond::findrange:4.0" → "findrange:4.0"
          3. Strip version suffix    e.g. "findrange:4.0" → "findrange"
          4. Reverse bare-name map   e.g. "preprocess" → "diamond::preprocess:4.0"
        Returns the original name unchanged if nothing matches (ZERO_SHOT).
        """
        if t in self.known_trans or t in self.rule_map or t in self.smart_rule:
            return t
        base = re.sub(r"^[^:]+::", "", t)
        if base in self.known_trans or base in self.rule_map or base in self.smart_rule:
            return base
        base2 = re.sub(r":[^:]+$", "", base)
        if base2 in self.known_trans or base2 in self.rule_map or base2 in self.smart_rule:
            return base2
        bare = re.sub(r":[^:]+$", "", re.sub(r"^[^:]+::", "", t))
        canonical = self._bare_to_canonical.get(bare)
        if canonical:
            return canonical
        return t

    def predict(self, df: pd.DataFrame):
        # Resolve transformation names to canonical training names before feature
        # engineering so that anchor_runtime and name embeddings both benefit.
        if "transformation" in df.columns:
            df = df.copy()
            df["transformation"] = df["transformation"].apply(self._resolve_name)
        df_eng, name_emb = _make_features(df, self.arts)
        X = self.scaler.transform(_assemble_X(df_eng, name_emb, self.numeric_cols))
        Xt = torch.FloatTensor(X).to(self.device)
        with torch.no_grad():
            (pm, pl, ph), recon, mu, lv = self.model(Xt)

        p_med  = np.expm1(pm.cpu().numpy().flatten())
        p_low  = np.clip(np.expm1(pl.cpu().numpy().flatten()), 0, None)
        p_high = np.expm1(ph.cpu().numpy().flatten())

        # ── VAE interpretation ─────────────────────────────────────────────
        # Per-feature squared reconstruction error: shape (N, input_dim).
        # Tells us WHICH feature is most responsible for the anomaly.
        per_feat_err = ((Xt - recon) ** 2).cpu().numpy()   # (N, input_dim)
        recon_err    = per_feat_err.mean(axis=1)            # (N,) — overall error

        # KL divergence per job — how unusual is the latent encoding.
        kl = (0.5 * (mu ** 2 + lv.exp() - lv - 1).sum(dim=1)).cpu().numpy()

        # Anomaly score [0,1]: sigmoid-normalised combination of recon error + KL.
        raw_anomaly   = recon_err + 0.1 * kl
        anomaly_score = 1.0 / (1.0 + np.exp(-2.0 * (raw_anomaly - raw_anomaly.mean()
                                                       ) / (raw_anomaly.std() + 1e-8)))

        vae_confidence = np.clip(1.0 - anomaly_score, 0.0, 1.0)
        latent_mu      = mu.cpu().numpy()   # (N, lat_dim)

        # ── Anomaly type: which feature group drives the reconstruction error ──
        # Map feature index → human-readable anomaly category.
        # Indices 0-8 = numeric_cols; indices 9+ = name-embedding dims.
        _FEAT_TYPE = {
            0: "unknown_transformation",   # log_anchor_runtime  — no training history
            1: "unusual_cpu",              # log_cpu_power
            2: "unusual_memory",           # log_ram
            3: "unusual_data_size",        # log_input_bytes
            4: "unusual_io_pattern",       # log_io_intensity
            5: "unusual_compute_pattern",  # log_compute_intensity
            6: "unusual_memory_cpu_ratio", # memory_pressure
            7: "unusual_site",             # site_encoded
            8: "unusual_file_count",       # log_input_files_count
        }
        _NAME_EMB_TYPE = "unknown_transformation_name"  # dims 9+ = TF-IDF name embedding

        def _anomaly_types(feat_errors: np.ndarray) -> list:
            """
            Return a sorted list of (anomaly_type, contribution_pct) for the
            top contributing feature groups, highest first.
            Only groups contributing > 5 % of total error are included.
            """
            total = feat_errors.sum() + 1e-12
            # Group name-embedding dims (9+) as one bucket
            scores: dict = {}
            for idx, err in enumerate(feat_errors):
                key = _FEAT_TYPE.get(idx, _NAME_EMB_TYPE)
                scores[key] = scores.get(key, 0.0) + err
            ranked = sorted(scores.items(), key=lambda x: -x[1])
            return [
                {"type": k, "contribution_pct": round(float(100.0 * v / total), 1)}
                for k, v in ranked
                if v / total > 0.05
            ]

        anomaly_types = [_anomaly_types(per_feat_err[i]) for i in range(len(per_feat_err))]

        # Widen the prediction interval for high-anomaly jobs (up to 2×).
        interval_scale = 1.0 + anomaly_score
        p_mid   = (p_low + p_high) / 2.0
        half    = (p_high - p_low) / 2.0
        p_low   = np.maximum(0, p_mid - half * interval_scale)
        p_high  = p_mid + half * interval_scale

        # transformation column was already resolved by _resolve_name above;
        # re-resolve here only to determine the status label per row.
        statuses = []
        for i, row in enumerate(df.itertuples(index=False)):
            trans = getattr(row, "transformation", "")
            n_train = self.trans_counts.get(trans, 0)
            if trans in self.rule_map:
                rv        = self.rule_map[trans]
                p_med[i]  = rv
                p_low[i]  = rv * 0.5
                p_high[i] = rv * 2.0
                statuses.append("RULE_BASED")
            elif n_train < self.sparse_thr and trans in self.smart_rule:
                sr        = self.smart_rule[trans]
                p_med[i]  = sr["median"]
                p_low[i]  = sr["low"]
                p_high[i] = sr["high"]
                statuses.append("SPARSE")
            elif trans not in self.known_trans:
                statuses.append("ZERO_SHOT")
            else:
                statuses.append("NORMAL")

        return p_med, p_low, p_high, statuses, anomaly_score, vae_confidence, latent_mu, anomaly_types


# ─────────────────────────────────────────────
# 5. FEATURE EXTRACTION FROM WORKFLOW OBJECT
# ─────────────────────────────────────────────

def _job_execution_site(job) -> Optional[str]:
    """
    Extract the execution site name from a job's profiles.
    Checks selector.execution.site and pegasus.execution.site in that order.
    Returns None if not set — caller falls back to slot hostname.
    """
    for ns_key, profs in job.profiles.items():
        ns_val = ns_key.value if hasattr(ns_key, "value") else str(ns_key)
        if ns_val in ("selector", "pegasus"):
            for k, v in (profs.items() if hasattr(profs, "items") else profs):
                if str(k).lower() in ("execution.site", "executionsite"):
                    return str(v)
    return None


def _condor_profiles(job) -> dict:
    """Return the Condor-namespace profile dict from a job, key-agnostic."""
    for ns_key, profs in job.profiles.items():
        ns_val = ns_key.value if hasattr(ns_key, "value") else str(ns_key)
        if ns_val == "condor":
            return dict(profs)
    return {}


def _full_trans_name(job) -> str:
    """Build the canonical transformation name: [namespace::]name[:version]."""
    name = getattr(job, "transformation", None) or "unknown"
    ns   = getattr(job, "namespace",      None)
    ver  = getattr(job, "version",        None)
    if ns:
        name = f"{ns}::{name}"
    if ver:
        name = f"{name}:{ver}"
    return name


def _build_dag_levels(workflow) -> List[List]:
    """
    Topological sort of workflow jobs into levels.

    Level 0 = source jobs (no inputs from other jobs).
    Level N = jobs whose all upstream jobs are in levels < N.

    Returns a list of levels, each level is a list of (job_id, job) tuples.
    """
    # Map: file lfn → job_id that produces it
    producer = {}
    for jid, job in workflow.jobs.items():
        for f in job.get_outputs() if hasattr(job, "get_outputs") else []:
            producer[f.lfn] = jid

    # Build dependency map: job_id → set of job_ids it depends on
    deps = {jid: set() for jid in workflow.jobs}
    for jid, job in workflow.jobs.items():
        for f in job.get_inputs() if hasattr(job, "get_inputs") else []:
            parent = producer.get(f.lfn)
            if parent and parent != jid:
                deps[jid].add(parent)

    # Kahn's algorithm
    levels   = []
    assigned = set()
    remaining = set(workflow.jobs.keys())

    while remaining:
        level = [
            jid for jid in remaining
            if deps[jid].issubset(assigned)
        ]
        if not level:
            # cycle or unresolved — add remaining as one last level
            level = list(remaining)
        levels.append([(jid, workflow.jobs[jid]) for jid in level])
        for jid in level:
            assigned.add(jid)
            remaining.discard(jid)

    return levels


def read_meta_sizes(submit_dir: str) -> dict:
    """
    Scan all Pegasus ``.meta`` files under *submit_dir* and return a
    mapping of **LFN → size in bytes** (float).

    Pegasus writes one ``.meta`` file per job after it completes, e.g.::

        run00XX/00/00/preprocess_0.meta

    Each file is a JSON array of records::

        [
          {
            "_type": "file",
            "_id": "f.b1",
            "_attributes": {"size": "1048576", "checksum.type": "sha256", ...}
          },
          ...
        ]

    The ``_id`` field is the LFN; ``_attributes.size`` is the file size in
    bytes (stored as a string by Pegasus).  This function merges all
    discovered records into one dict so ``_extract_features`` can seed
    ``file_size_map`` with actual post-execution sizes rather than the
    static ``File.size`` values from ``workflow.yml``.

    Returns an empty dict if *submit_dir* does not exist or no ``.meta``
    files are found.
    """
    sizes: dict = {}
    root = Path(submit_dir)
    if not root.is_dir():
        return sizes

    for meta_path in root.rglob("*.meta"):
        try:
            records = json.loads(meta_path.read_text())
            if not isinstance(records, list):
                continue
            for rec in records:
                lfn  = rec.get("_id", "")
                attrs = rec.get("_attributes", {})
                raw_size = attrs.get("size", "")
                if lfn and raw_size:
                    try:
                        sizes[lfn] = float(raw_size)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            continue

    return sizes


def scan_sub_files(submit_dir: str) -> dict:
    """
    Single-pass scan of all HTCondor .sub files in *submit_dir*.

    For each file that belongs to a user job (has a non-null
    ``+pegasus_wf_dax_job_id`` ClassAd) this extracts:

    - ``request_cpus``    — CPUs requested in the .sub file
    - ``request_memory_mb`` — memory requested in the .sub file (MB)
    - ``sub_path``        — absolute path to the .sub file (for later patching)

    Infrastructure jobs (stage-in, cleanup, create-dir, etc.) have
    ``+pegasus_wf_dax_job_id = "null"`` and are skipped.

    Returns
    -------
    dict
        ``{dax_job_id: {"request_cpus": int|None,
                        "request_memory_mb": float|None,
                        "sub_path": str}}``
    """
    result  = {}
    sub_dir = Path(submit_dir)
    if not sub_dir.is_dir():
        return result

    for sub_path in sub_dir.rglob("*.sub"):  # recursive — finds jobs in 00/00/
        dax_job_id = None
        req_cpus   = None
        req_memory = None
        try:
            for line in sub_path.read_text().splitlines():
                line = line.strip()
                m = re.match(
                    r'\+pegasus_wf_dax_job_id\s*=\s*"?([^"\s]+)"?', line, re.IGNORECASE
                )
                if m and m.group(1).lower() != "null":
                    dax_job_id = m.group(1)
                m = re.match(r'request_cpus\s*=\s*(\d+)', line, re.IGNORECASE)
                if m:
                    req_cpus = int(m.group(1))
                m = re.match(r'request_memory\s*=\s*([\d.]+)', line, re.IGNORECASE)
                if m:
                    req_memory = float(m.group(1))
        except OSError:
            continue

        if dax_job_id:
            result[dax_job_id] = {
                "request_cpus":      req_cpus,
                "request_memory_mb": req_memory,
                "sub_path":          str(sub_path),
            }

    return result


# Keep old name as alias so existing code doesn't break
def parse_sub_resources(submit_dir: str) -> dict:
    """Alias for :func:`scan_sub_files` — returns resource dict only."""
    return {
        jid: {k: v for k, v in info.items() if k != "sub_path"}
        for jid, info in scan_sub_files(submit_dir).items()
    }


def patch_sub_file(sub_path: str, prediction: dict) -> bool:
    """
    Inject predicted runtime ClassAds into a .sub file **in-place**.

    Called after prediction, before DAGMan submits the job.  Adds:

    - ``+PredictedRuntime``     — median predicted seconds
    - ``+PredictedRuntimeLow``  — lower-bound seconds
    - ``+PredictedRuntimeHigh`` — upper-bound seconds
    - ``+PredictionStatus``     — NORMAL / SPARSE / ZERO_SHOT / RULE_BASED
    - Updates ``periodic_remove`` to also kill jobs running > 3× upper bound
      (safety net; keeps the existing held-job removal condition).

    Returns True on success, False if the file could not be patched.
    """
    path = Path(sub_path)
    if not path.is_file():
        return False

    predicted_s  = max(1, int(prediction.get("predicted_runtime_s", 0)))
    upper_s      = max(predicted_s, int(prediction.get("upper_bound_s",  predicted_s * 2)))
    lower_s      = max(0,           int(prediction.get("lower_bound_s",  0)))
    status       = str(prediction.get("status", "UNKNOWN"))
    # Kill if running longer than 3× upper bound (minimum 1 h)
    timeout_s    = max(upper_s * 3, 3_600)

    try:
        lines = path.read_text().splitlines()

        # Find the queue line (always present in Pegasus .sub files)
        queue_idx = next(
            (i for i, l in enumerate(lines) if l.strip().lower().startswith("queue")),
            None,
        )
        if queue_idx is None:
            return False

        # Update periodic_remove in the block before queue
        before = []
        for line in lines[:queue_idx]:
            if re.match(r'periodic_remove\s*=', line.strip(), re.IGNORECASE):
                existing = line.split("=", 1)[1].strip()
                line = (
                    f"periodic_remove = ({existing}) || "
                    f"((JobStatus == 2) && "
                    f"((CurrentTime - EnteredCurrentStatus) > {timeout_s}))"
                )
            before.append(line)

        our_ads = [
            f"+PredictedRuntime     = {predicted_s}",
            f"+PredictedRuntimeLow  = {lower_s}",
            f"+PredictedRuntimeHigh = {upper_s}",
            f'+PredictionStatus     = "{status}"',
        ]

        # Result: classads → our ads → queue → comment block (original tail)
        result = before + our_ads + lines[queue_idx:]
        path.write_text("\n".join(result) + "\n")
        return True

    except OSError:
        return False


def find_submit_dir(workflow_yml: str) -> str | None:
    """
    Locate the Pegasus submit ROOT for the **current** workflow run.

    Uses ``$PEGASUS_WF_UUID`` (always present in the job environment) to
    match the exact braindump.yml for this run — avoids returning an older
    run's submit directory when multiple runs exist side-by-side.

    Tries in order:
    1. ``$PEGASUS_SUBMIT_DIR`` / ``$_PEGASUS_SUBMIT_DIR`` env vars.
    2. Walk up from CWD looking for ``braindump.yml`` matching the UUID.
    3. Search under the workflow YAML directory, UUID-matched first,
       then any braindump as fallback.
    4. ``submit/`` subtree under the workflow YAML directory.
    """
    wf_uuid = os.environ.get("PEGASUS_WF_UUID", "")

    def _check_braindump(path: Path) -> str | None:
        """Return submit dir from braindump if UUID matches (or as fallback)."""
        try:
            import yaml as _yaml
            data = _yaml.safe_load(path.read_text()) or {}
            sd = data.get("submit_dir") or str(path.parent)
            if Path(sd).is_dir():
                return sd
        except Exception:
            if path.parent.is_dir():
                return str(path.parent)
        return None

    # 1. Explicit Pegasus env var
    for var in ("PEGASUS_SUBMIT_DIR", "_PEGASUS_SUBMIT_DIR"):
        sd = os.environ.get(var)
        if sd and Path(sd).is_dir():
            return sd

    # 2. Walk up from CWD — find braindump with matching UUID first
    for candidate in [Path.cwd()] + list(Path.cwd().parents)[:8]:
        bd = candidate / "braindump.yml"
        if bd.is_file():
            try:
                import yaml as _yaml
                data = _yaml.safe_load(bd.read_text()) or {}
                if not wf_uuid or data.get("wf_uuid") == wf_uuid:
                    sd = _check_braindump(bd)
                    if sd:
                        return sd
            except Exception:
                pass

    # 3. Search under the workflow YAML directory
    wf_dir = Path(workflow_yml).resolve().parent
    all_braindumps = []
    for search_root in [wf_dir] + list(wf_dir.parents)[:2]:
        all_braindumps += list(search_root.rglob("braindump.yml"))

    # UUID-matched pass first
    if wf_uuid:
        for bd in all_braindumps:
            try:
                import yaml as _yaml
                data = _yaml.safe_load(bd.read_text()) or {}
                if data.get("wf_uuid") == wf_uuid:
                    sd = _check_braindump(bd)
                    if sd:
                        return sd
            except Exception:
                pass

    # Fallback: most recently modified braindump (= current run)
    for bd in sorted(all_braindumps, key=lambda p: p.stat().st_mtime, reverse=True):
        sd = _check_braindump(bd)
        if sd:
            return sd

    # 4. submit/ subtree
    submit_sub = wf_dir / "submit"
    if submit_sub.is_dir():
        return str(submit_sub)

    return None


def _extract_features(
    workflow,
    slot: dict,
    arts: dict,
    sub_resources: Optional[dict] = None,
    submit_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build one feature row per job in the workflow using DAG-level traversal.

    Walks the DAG level by level (topological order):
      - Level 0 (source jobs): uses File.size directly — sizes are known
      - Level N (middle/sink): input sizes are propagated from upstream outputs
        using the output_ratio per transformation stored in arts, so middle
        jobs get a realistic input_bytes_total instead of 0

    ``sub_resources`` (optional) provides request_cpus / request_memory from
    HTCondor .sub files, keyed by DAX job ID.

    cpu_count semantics: the training data records the actual machine's CPU count
    from kickstart output (e.g. a 64-CPU node shows cpu_count=64 regardless of
    how many CPUs the job requested).  We therefore always use slot["cpu_count"]
    (the machine total from condor_status) for the model's cpu_count feature.
    request_cpus is only used to derive memory_pressure (ram / request_cpus),
    which captures the per-CPU memory allocation requested by the job.

    SubWorkflow and Pegasus/system auxiliary jobs are skipped at every level.
    """
    sub_resources = sub_resources or {}

    # file lfn → size in bytes.
    # Priority (highest wins):
    #   1. Actual size from .meta files written by completed jobs
    #   2. Static File.size declared in workflow.yml
    # .meta files are written by Pegasus after each job completes, so for
    # level N predictors (N ≥ 1) the level N-1 outputs already have .meta
    # files with exact sizes — far more accurate than workflow.yml estimates.
    file_size_map: dict = {}

    # Seed with static YAML sizes first (lowest priority)
    for jid, job in workflow.jobs.items():
        for f in (job.get_inputs() if hasattr(job, "get_inputs") else []):
            if hasattr(f, "size") and f.size:
                file_size_map[f.lfn] = float(f.size)
        for f in (job.get_outputs() if hasattr(job, "get_outputs") else []):
            if hasattr(f, "size") and f.size:
                file_size_map[f.lfn] = float(f.size)

    # Override with real sizes from .meta files (highest priority)
    if submit_dir:
        meta_sizes = read_meta_sizes(submit_dir)
        file_size_map.update(meta_sizes)  # overwrites YAML sizes where known

    output_ratio = arts.get("output_ratio", {})

    rows      = []
    level_map = {}  # job_id → level index (for reporting)

    for level_idx, level in enumerate(_build_dag_levels(workflow)):
        for jid, job in level:
            if not hasattr(job, "transformation"):
                continue  # SubWorkflow
            ns = getattr(job, "namespace", None)
            if ns in ("pegasus", "system"):
                continue  # aux job

            condor  = _condor_profiles(job)
            sub_res = sub_resources.get(jid, {})

            # cpu_count: always the machine total (matches training-time kickstart data)
            machine_cpus = slot["cpu_count"]

            # request_cpus: what the job asked for (used for memory_pressure only)
            req_cpus = (
                sub_res.get("request_cpus")
                or int(condor.get("request_cpus", 0))
                or machine_cpus
            )

            req_mem_mb = (
                sub_res.get("request_memory_mb")
                or float(condor.get("request_memory", 0))
            )
            ram_kb = req_mem_mb * 1_024 if req_mem_mb > 0 else slot["ram"]

            # site: prefer named execution site from job profiles over hostname
            exec_site = _job_execution_site(job) or slot["hostname"]

            inputs            = job.get_inputs() if hasattr(job, "get_inputs") else []
            input_files_count = len(inputs)
            input_bytes_total = sum(
                file_size_map.get(f.lfn, 0.0) for f in inputs
            )

            rows.append({
                "job_id":            jid,
                "transformation":    _full_trans_name(job),
                "dag_level":         level_idx,
                "cpu_count":         machine_cpus,
                "request_cpus":      req_cpus,
                "cpu_speed":         slot["cpu_speed"],
                "ram":               ram_kb,
                "input_bytes_total": float(input_bytes_total),
                "input_files_count": input_files_count,
                "hostname":          exec_site,
            })
            level_map[jid] = level_idx

            # Propagate estimated output sizes to downstream jobs
            trans      = _full_trans_name(job)
            ratio      = output_ratio.get(trans, 1.0)
            est_output = input_bytes_total * ratio
            for f in (job.get_outputs() if hasattr(job, "get_outputs") else []):
                if f.lfn not in file_size_map:
                    file_size_map[f.lfn] = est_output

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─────────────────────────────────────────────
# 6. WORKFLOW-LEVEL PREDICTOR
# ─────────────────────────────────────────────

class WorkflowRuntimePredictor:
    """
    Workflow-level runtime predictor.

    Loads the PegasusOracle model once, runs a single batch inference
    pass over every job in the workflow (source, middle, and sink), and
    writes two output files to the workflow output directory.

    Usage — explicit config::

        cfg = RuntimePredictionConfig(
            model_path  = "pegasus_oracle_model.pkl",
            output_dir  = "/submit/dir",
            q_lo        = 0.10,
            q_hi        = 0.90,
        )
        predictor = WorkflowRuntimePredictor(cfg)
        results   = predictor.predict(workflow)

    Usage — from pegasus.properties::

        cfg = RuntimePredictionConfig.from_properties(props)
        WorkflowRuntimePredictor(cfg).predict(workflow)

    Output files:
        runtime_predictions.csv   — tabular results, one row per job
        runtime_predictions.json  — same data + summary stats and slot info
    """

    def __init__(self, config: RuntimePredictionConfig):
        """
        :param config: :py:class:`RuntimePredictionConfig` instance
        :raises ValueError: if config.model_path is not set
        :raises FileNotFoundError: if the model file does not exist
        :raises ImportError: if PyTorch or scikit-learn are not installed
        """
        if not config.model_path:
            if _DEFAULT_MODEL.exists():
                config.model_path = str(_DEFAULT_MODEL)
            else:
                raise ValueError(
                    "No model found. Either set pegasus.runtime.prediction.model.path "
                    "in pegasus.properties, or place pegasus_oracle_model.pkl in: "
                    f"{_MODELS_DIR}"
                )
        if not os.path.isfile(config.model_path):
            raise FileNotFoundError(
                f"Model not found: {config.model_path}. "
                "Train the model via pegasus_oracle.py and place the .pkl in "
                f"{_MODELS_DIR} or set pegasus.runtime.prediction.model.path."
            )
        self._cfg        = config
        self._ctx        = ModelContext(config.model_path)

        # Apply config overrides to the loaded model context
        if config.sparse_threshold is not None:
            self._ctx.sparse_thr = config.sparse_threshold
        if config.q_lo is not None:
            self._ctx.q_lo = config.q_lo
        if config.q_hi is not None:
            self._ctx.q_hi = config.q_hi

    def predict(self, workflow, sub_resources: Optional[dict] = None) -> List[dict]:
        """
        Predict runtimes for all jobs in *workflow* and write output files.

        Output directory is taken from ``config.output_dir``; if not set it
        falls back to the workflow's submit dir (``workflow._submit_dir``) or
        the current working directory.

        :param workflow: a Pegasus Workflow object
        :param sub_resources: optional dict from :func:`parse_sub_resources` —
            ``{dax_job_id: {"request_cpus": int, "request_memory_mb": float}}``.
            When provided, overrides YAML profile values for cpu_count and ram,
            matching what was used at training time (values from .sub files).
        :return: list of prediction dicts (one per job, same content as CSV rows)
        """
        if not self._cfg.enabled:
            return []

        output_dir = (
            self._cfg.output_dir
            or getattr(workflow, "_submit_dir", None)
            or str(Path.cwd())
        )
        output_dir = str(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        # ── Step 1: resolve compute slot ──────────────────────────────────
        slot = _get_condor_slot()
        # Apply any slot overrides from config
        if self._cfg.slot_hostname  is not None: slot["hostname"]  = self._cfg.slot_hostname
        if self._cfg.slot_cpu_count is not None: slot["cpu_count"] = self._cfg.slot_cpu_count
        if self._cfg.slot_cpu_speed is not None: slot["cpu_speed"] = self._cfg.slot_cpu_speed
        if self._cfg.slot_ram       is not None: slot["ram"]       = self._cfg.slot_ram

        # ── Step 2: build feature matrix level-by-level, then collect all ───
        df = _extract_features(workflow, slot, self._ctx.arts,
                               sub_resources=sub_resources,
                               submit_dir=output_dir)
        if df.empty:
            return []

        # ── Step 3: single batch forward pass + VAE interpretation ───────────
        p_med, p_low, p_high, statuses, anomaly, confidence, latent_mu, anomaly_types = \
            self._ctx.predict(df)
        ip_pct = int(round((self._ctx.q_hi - self._ctx.q_lo) * 100))

        # ── Step 4: assemble result rows ───────────────────────────────────
        results = []
        for i, row in enumerate(df.itertuples(index=False)):
            results.append({
                "job_id":              row.job_id,
                "transformation":      row.transformation,
                "dag_level":           int(row.dag_level),
                "status":              statuses[i],
                "predicted_runtime_s": round(float(p_med[i]), 1),
                "lower_bound_s":       round(float(p_low[i]),  1),
                "upper_bound_s":       round(float(p_high[i]), 1),
                "interval_pct":        ip_pct,
                "min_wall_time_mins":  max(1, math.ceil(float(p_low[i])  / 60)),
                "max_wall_time_mins":  max(1, math.ceil(float(p_high[i]) / 60)),
                "input_files_count":   int(row.input_files_count),
                "input_bytes_total":   float(row.input_bytes_total),
                "cpu_count":           int(row.cpu_count),
                "hostname":            row.hostname,
                # VAE interpretation
                "vae_anomaly_score":   round(float(anomaly[i]),    4),
                "vae_confidence":      round(float(confidence[i]), 4),
                "vae_anomaly_types":   anomaly_types[i],
                "vae_latent_mu":       [round(float(v), 4) for v in latent_mu[i]],
            })

        # ── Step 5: write CSV ──────────────────────────────────────────────
        csv_path = os.path.join(output_dir, "runtime_predictions.csv")
        pd.DataFrame(results).to_csv(csv_path, index=False)

        # ── Step 6: write JSON with summary ───────────────────────────────
        n_levels = int(df["dag_level"].max()) + 1 if not df.empty else 0
        summary = {
            "workflow_name":  getattr(workflow, "name", "unknown"),
            "total_jobs":     len(results),
            "dag_levels":     n_levels,
            "status_counts":  dict(Counter(statuses)),
            "interval_pct":   ip_pct,
            "model_path":     self._cfg.model_path,
            "slot_used":      slot,
            "config": {
                "sparse_threshold": self._ctx.sparse_thr,
                "q_lo":             self._ctx.q_lo,
                "q_hi":             self._ctx.q_hi,
            },
            "predictions":    results,
        }
        json_path = os.path.join(output_dir, "runtime_predictions.json")
        with open(json_path, "w") as f:
            json.dump(summary, f, indent=2)

        # ── Step 7: inject native Pegasus job into the DAG ────────────────
        # Mirrors stage-in/stage-out: runs on local site before all root jobs,
        # produces runtime_predictions.json and .csv as DAG outputs.
        self._inject_into_dag(workflow, output_dir)

        return results

    def _inject_into_dag(self, workflow, output_dir: str) -> None:
        """
        Inject ONE ``pegasus-runtime-predictor`` job per DAG level.

        Like stage-in (one per level), each prediction job:
        - Runs BEFORE all user jobs at that level
        - Runs AFTER all user jobs at the previous level
        - Outputs level-specific runtime_predictions_L{N}.json/.csv

        DAG shape after injection:
            [pred-L0] → [user-L0-jobs]
                              ↓
                        [pred-L1] → [user-L1-jobs]
                                          ↓
                                    [pred-L2] → [user-L2-jobs]
        """
        import logging as _log
        _logger = _log.getLogger(__name__)

        import shutil as _shutil
        from Pegasus.api import Job, Namespace, Transformation, TransformationCatalog, TransformationSite

        # ── Inject TC entry so Pegasus can map the predictor to any site ──────
        # Resolve binary path: prefer PATH lookup, fall back to location
        # relative to this module (works for both installed and source layouts).
        _pred_bin = _shutil.which("pegasus-runtime-predictor") or str(
            Path(__file__).resolve().parent.parent.parent.parent.parent
            / "bin" / "pegasus-runtime-predictor"
        )
        if workflow.transformation_catalog is None:
            workflow.transformation_catalog = TransformationCatalog()
        _existing_tc = {
            t.name
            for t in getattr(workflow.transformation_catalog, "transformations", {}).values()
        }
        if "pegasus-runtime-predictor" not in _existing_tc:
            _pred_trans = Transformation("pegasus-runtime-predictor")
            _pred_trans.add_sites(
                TransformationSite("local",      _pred_bin, is_stageable=False),
                TransformationSite("condorpool", _pred_bin, is_stageable=False)
                    .add_profiles(Namespace.CONDOR, universe="local"),
            )
            workflow.transformation_catalog.add_transformations(_pred_trans)

        # ── Also patch the external tc.yml in the workflow directory ──────────
        # Pegasus may read an external tc.yml (from pegasus.properties or CWD)
        # that overrides the inline TC. Write our entry there too so Pegasus
        # always finds condorpool regardless of which TC source it prefers.
        _pred_tc_entry = {
            "name": "pegasus-runtime-predictor",
            "sites": [
                {"name": "local",      "pfn": _pred_bin, "type": "installed"},
                {"name": "condorpool", "pfn": _pred_bin, "type": "installed",
                 "profiles": {"condor": {"universe": "local"}}},
            ],
        }
        for _tc_candidate in (
            Path(output_dir).parent / "tc.yml",
            Path(output_dir).parent / "tc.yaml",
            Path("tc.yml"),
            Path("tc.yaml"),
        ):
            if not _tc_candidate.exists():
                continue
            try:
                import yaml as _yaml
                _tc_data = _yaml.safe_load(_tc_candidate.read_text()) or {}
                _trans = _tc_data.get("transformations", [])
                if not any(t.get("name") == "pegasus-runtime-predictor" for t in _trans):
                    _trans.append(_pred_tc_entry)
                    _tc_data["transformations"] = _trans
                    _tc_candidate.write_text(
                        _yaml.dump(_tc_data, default_flow_style=False, sort_keys=False)
                    )
                    _logger.info(f"[runtime-predictor] Patched TC entry into {_tc_candidate}")
            except Exception as _e:
                _logger.warning(f"[runtime-predictor] Could not patch {_tc_candidate}: {_e}")
            break  # only patch the first tc.yml found

        # Idempotent: remove any existing predictor jobs before re-injecting.
        # Prevents duplicate output-file errors if this method is called more
        # than once (e.g. from a user script that calls predict() twice).
        pred_job_ids = [
            jid for jid, j in list(workflow.jobs.items())
            if getattr(j, "transformation", "") == "pegasus-runtime-predictor"
        ]
        for jid in pred_job_ids:
            workflow.jobs.pop(jid, None)
        if pred_job_ids:
            _logger.info(
                f"[runtime-predictor] Removed {len(pred_job_ids)} stale predictor job(s)"
            )

        levels = _build_dag_levels(workflow)
        if not levels:
            _logger.warning("[runtime-predictor] DAG is empty — skipping")
            return

        prev_pred_job = None
        for level_idx, level in enumerate(levels):
            level_job_ids = [jid for jid, _ in level]

            # Do NOT declare output files as Pegasus LFNs — the predictor writes
            # them locally as side-effects and they never need to be staged.
            # Declaring them caused "duplicate output file" planning errors when
            # two code paths both tried to inject predictor jobs.
            pred_job = (
                Job("pegasus-runtime-predictor")
                .add_args("workflow.yml", output_dir, f"--level={level_idx}")
                .add_profiles(Namespace.PEGASUS, key="job.type", value="auxillary")
                .add_profiles(Namespace.PEGASUS, key="label",    value=f"runtime-prediction-L{level_idx}")
                .add_profiles(Namespace.SELECTOR, key="execution.site", value="local")
                .add_profiles(Namespace.CONDOR,   key="universe",       value="local")
            )

            workflow.add_jobs(pred_job)
            _logger.info(f"[runtime-predictor] Level {level_idx}: added {pred_job._id}")

            # pred-LN → user jobs at level N
            for jid in level_job_ids:
                workflow.add_dependency(pred_job, children=[workflow.jobs[jid]])

            # user jobs at level N-1 → pred-LN  (so pred-LN runs between levels)
            if prev_pred_job is not None:
                workflow.add_dependency(prev_pred_job, children=[pred_job])

            prev_pred_job = pred_job
