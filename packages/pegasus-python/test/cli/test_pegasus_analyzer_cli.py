"""Characterization tests for the pegasus-analyzer CLI.

These tests lock the *data content* of the analyzer report independent of its
presentation. They are written against the current plain-text renderer and must
keep passing after the output is migrated to Rich/Markdown -- a failure means a
datum was dropped or renamed, not that the formatting changed.

Assertions therefore only check for data substrings (job names, counts, paths,
exit codes, stderr snippets) that appear in *any* reasonable rendering, never on
banners, alignment, or separators.
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

pegasus_analyzer_pkg = __import__(
    "Pegasus.cli.pegasus-analyzer", fromlist=["pegasus_analyzer"]
)
pegasus_analyzer = pegasus_analyzer_pkg.pegasus_analyzer

# Realistic Stampede-DB sample submit dirs live in the sibling pegasus-common
# package. Reference them via the repo checkout rather than duplicating ~700KB
# binary DBs into this package.
SAMPLES_DIR = (
    Path(__file__).resolve().parents[3]
    / "pegasus-common"
    / "test"
    / "client"
    / "analyzer_samples_dir"
)

pytestmark = pytest.mark.skipif(
    not SAMPLES_DIR.is_dir(),
    reason=f"analyzer sample submit dirs not found at {SAMPLES_DIR}",
)

FAILURE_WF = "process_wf_failure"
HELD_WF = "sample_wf_held"
SUCCESS_WF = "process_wf_success"

# Known data in process_wf_failure (mirrors test_analyzer.py expectations).
FAILURE_WF_UUID = "f84f05fc-a8d0-42b5-bac5-52d6f41a77e3"
FAILED_JOB = "ls_ID0000001"


def run(*args):
    """Invoke the CLI with AI analysis disabled (no network)."""
    result = CliRunner().invoke(pegasus_analyzer, ["--ai", "false", *args])
    assert result.exit_code == 0, result.output
    return result.output


def test_failure_report_contains_failed_job_data():
    out = run(str(SAMPLES_DIR / FAILURE_WF))

    # Failed job identity and metadata.
    assert FAILED_JOB in out
    assert "POST_SCRIPT_FAILED" in out
    assert "condorpool" in out
    # Task details.
    assert "/usr/bin/ls" in out
    assert "exitcode" in out.lower()
    assert "2" in out
    # Captured task stderr (kickstart) must be surfaced. (The job-level
    # PegasusLite stderr is intentionally suppressed when kickstart stderr
    # exists, so we assert on the kickstart content that IS shown.)
    assert "invalid option" in out
    # Workflow identity reported on failure.
    assert FAILURE_WF_UUID in out


def test_failure_report_summary_counts():
    out = run(str(SAMPLES_DIR / FAILURE_WF))
    # Section presence (case-insensitive; banner vs heading agnostic).
    assert "summary" in out.lower()
    # Submit directory recorded in braindump must appear.
    assert "run0001" in out


def test_held_report_contains_held_job_data():
    out = run(str(SAMPLES_DIR / HELD_WF))
    assert "chebi_drug_loader_ID0000001" in out
    assert "Transfer output files failure" in out


def test_summary_mode_omits_job_details():
    out = run("-S", str(SAMPLES_DIR / FAILURE_WF))
    assert "summary" in out.lower()
    # Workflow identity still reported, but per-job details are suppressed.
    assert FAILURE_WF_UUID in out
    assert FAILED_JOB not in out
    assert "invalid option" not in out


def test_success_report_has_no_failed_section():
    out = run(str(SAMPLES_DIR / SUCCESS_WF))
    assert "summary" in out.lower()
    assert "Workflow failed" not in out


def test_json_mode_is_structured_and_parseable():
    import json

    out = run("-j", str(SAMPLES_DIR / FAILURE_WF))
    # stdout may be prefixed by log warnings (e.g. a missing properties file in
    # the sample); the JSON document starts at the first brace.
    data = json.loads(out[out.index("{") :])
    assert data["root_wf_uuid"] == FAILURE_WF_UUID
    root = data["workflows"]["root"]
    assert root["wf_status"] == "failure"
    assert root["jobs"]["failed"] == 1
    assert FAILED_JOB in root["jobs"]["job_details"]["failed_jobs_details"]
