"""Real-source tests for ``pegasus-monitor --json`` one-shot documents."""

##
#  Copyright 2026 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from .conftest import create_run

from Pegasus.monitor import cli
from Pegasus.monitor.condor import CondorObserver, CondorObserverConfig
from Pegasus.monitor.json_output import JSON_OUTPUT_FORMAT, JSON_OUTPUT_VERSION
from Pegasus.monitor.models import ClockSample, SchedulerQueryKind

if TYPE_CHECKING:
    import pytest

FIXTURES = Path(__file__).parents[1] / "fixtures"
CONDOR = FIXTURES / "condor"
GOLDEN = FIXTURES / "json_output"
CONTRACT = Path(__file__).parents[1] / "contracts" / "json-output-v1.json"
CLOCK = ClockSample(epoch=1000.0, monotonic=50.0)
UUIDS = {"pegasus_wf_uuid": "root-uuid", "pegasus_root_wf_uuid": "root-uuid"}
QUEUE = [
    {
        "ClusterId": 1000,
        "ProcId": 0,
        "JobStatus": 1,
        "DAGNodeName": "cluster_ID0000001",
        "Owner": "integration-user",
        "QDate": 110,
        "RequestCpus": 64,
        "RequestMemory": 4096,
        "RequestDisk": 102400,
        "RequestGpus": 0,
        **UUIDS,
    },
    {
        "ClusterId": 1001,
        "ProcId": 0,
        "JobStatus": 5,
        "DAGNodeName": "held_ID0000002",
        "Owner": "integration-user",
        "HoldReason": "Job has gone over memory limit of 4096 megabytes",
        "HoldReasonCode": 34,
        "RequestCpus": 1,
        "RequestMemory": 4096,
        **UUIDS,
    },
]
PRIORITY = """\
Name = "integration-user@example.org"
EffectivePriority = 12.5
RealPriority = 10.0
ResourcesUsed = 4

Name = "other@example.org"
EffectivePriority = 8.0
RealPriority = 7.0
ResourcesUsed = 10
"""


class _Output(io.StringIO):
    def __init__(self, *, tty: bool = False) -> None:
        super().__init__()
        self._tty = tty
        self.width = 100

    def isatty(self) -> bool:
        return self._tty


class _SchedulerByKind:
    """Send each query kind to a fake HTCondor that returns that kind's fixture."""

    def __init__(self, observers: dict[SchedulerQueryKind, CondorObserver]) -> None:
        self._observers = observers

    def query(self, request: object) -> object:
        return self._observers[request.kind].query(request)


def _fake_bin(tmp_path: Path) -> Path:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    for name in ("condor_q", "condor_history", "condor_status", "condor_userprio"):
        target = binary_dir / name
        shutil.copyfile(CONDOR / "fake_condor.py", target)
        target.chmod(0o755)
    return binary_dir


def _fake_scheduler(tmp_path: Path) -> _SchedulerByKind:
    binary_dir = _fake_bin(tmp_path)
    payloads = tmp_path / "payloads"
    payloads.mkdir()
    (payloads / "queue.json").write_text(json.dumps(QUEUE), encoding="utf-8")
    (payloads / "history.json").write_text("[]", encoding="utf-8")
    (payloads / "priority.txt").write_text(PRIORITY, encoding="utf-8")
    files = {
        SchedulerQueryKind.QUEUE: payloads / "queue.json",
        SchedulerQueryKind.HISTORY: payloads / "history.json",
        SchedulerQueryKind.POOL: CONDOR / "pool.json",
        SchedulerQueryKind.PRIORITY: payloads / "priority.txt",
        SchedulerQueryKind.NEGOTIATOR: CONDOR / "negotiator.json",
    }
    return _SchedulerByKind(
        {
            kind: CondorObserver(
                CondorObserverConfig(
                    environment={
                        "PATH": f"{binary_dir}:{os.environ.get('PATH', '')}",
                        "FAKE_CONDOR_PAYLOAD_FILE": str(path),
                    }
                ),
                jitter=lambda _delay: 0.0,
            )
            for kind, path in files.items()
        }
    )


def _runtime(scheduler: object | None = None) -> object:
    """The production runtime on a fixed clock, optionally with a fake HTCondor."""

    runtime = cli._load_runtime()
    changes = {
        "coordinator_type": partial(runtime.coordinator_type, clock=lambda: CLOCK)
    }
    if scheduler is not None:
        changes["scheduler_factory"] = lambda **_values: scheduler
    return replace(runtime, **changes)


def _problem_run(tmp_path: Path) -> Path:
    """A running workflow with queued, held, failed and sub-workflow jobs."""

    run = tmp_path / "run0001"
    database, _tail = create_run(run, terminated=False)
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            INSERT INTO job VALUES (20, 1, 'held_ID0000002', 'compute', 1);
            INSERT INTO job_instance VALUES
                (200, 20, 2, '1001.0', 'local', NULL, NULL, NULL);
            INSERT INTO jobstate VALUES (200, 'SUBMIT', 111, 1, NULL);
            INSERT INTO jobstate VALUES (200, 'JOB_HELD', 125, 2, 'memory');
            INSERT INTO task VALUES (3, 1, 20, 'example::held');
            INSERT INTO job VALUES (40, 1, 'failed_ID0000003', 'compute', 1);
            INSERT INTO job_instance VALUES
                (400, 40, 3, '1003.0', 'local', NULL, NULL, 1);
            INSERT INTO jobstate VALUES (400, 'SUBMIT', 112, 1, NULL);
            INSERT INTO jobstate VALUES (400, 'EXECUTE', 113, 2, NULL);
            INSERT INTO jobstate VALUES (400, 'JOB_FAILURE', 130, 3, NULL);
            INSERT INTO task VALUES (5, 1, 40, 'example::failed');
            INSERT INTO job VALUES (50, 1, 'subwf_ID0000004', 'dax', 1);
            """
        )
    return run


def _monitor(
    *arguments: str, runtime: object | None = None, tty: bool = False
) -> tuple[int, dict, str]:
    """Run the CLI in process and require stdout to be one JSON document."""

    stdout = _Output(tty=tty)
    stderr = io.StringIO()
    code = cli.main(list(arguments), runtime=runtime, stdout=stdout, stderr=stderr)
    text = stdout.getvalue()
    assert text.endswith("\n")
    assert text.count("\n") == 1
    assert "\x1b" not in text
    return code, json.loads(text), stderr.getvalue()


def _normalized(document: dict, run: Path) -> dict:
    """Replace values that legitimately differ between machines and runs."""

    text = json.dumps(document)
    for path in {str(run), str(run.resolve())}:
        text = text.replace(path, "<SUBMIT_DIR>")
    result = json.loads(text)
    result["monitor_version"] = "<VERSION>"
    result["generated_at_epoch"] = "<NOW>"
    result["generated_at"] = "<NOW>"
    snapshot = result.get("snapshot")
    if snapshot is not None:
        for name in ("db_generation", "tail_generation"):
            snapshot[name]["device"] = "<DEVICE>"
            snapshot[name]["inode"] = "<INODE>"
    return result


def _assert_golden(name: str, document: dict, run: Path) -> None:
    path = GOLDEN / f"{name}.json"
    actual = _normalized(document, run)
    if os.environ.get("PEGASUS_MONITOR_UPDATE_GOLDEN") == "1":
        path.write_text(json.dumps(actual, indent=2) + "\n", encoding="utf-8")
    assert actual == json.loads(path.read_text(encoding="utf-8"))


def _source(document: dict, name: str) -> dict:
    return next(item for item in document["sources"] if item["source"] == name)


def test_once_json_matches_golden(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    code, document, stderr = _monitor(
        "--once",
        "--json",
        str(run),
        runtime=_runtime(_fake_scheduler(tmp_path)),
    )

    assert (code, stderr) == (0, "")
    assert document["modes"] == ["once"]
    assert document["diagnostics"] is None
    assert document["why_idle"] is None
    jobs = {job["exec_job_id"]: job for job in document["snapshot"]["jobs"]}
    assert jobs["held_ID0000002"]["lifecycle"] == "held"
    assert jobs["failed_ID0000003"]["lifecycle"] == "failed"
    assert jobs["subwf_ID0000004"]["is_compute"] is False
    assert document["summary"]["job_counts"]["total"] == 4
    assert document["stats"]["total_jobs"] == 4
    _assert_golden("once", document, run)


def test_diagnose_json_matches_golden_and_carries_suggestions(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    code, document, _stderr = _monitor(
        "--diagnose",
        "--json",
        str(run),
        runtime=_runtime(_fake_scheduler(tmp_path)),
    )

    assert code == 0
    assert document["modes"] == ["diagnose"]
    assert document["snapshot"] is None
    findings = {
        item["target"]["exec_job_id"]: item
        for item in document["diagnostics"]["findings"]
    }
    held = findings["held_ID0000002"]
    assert held["category"] == "held"
    assert held["code"] == "memory_exceeded"
    assert held["severity"] in {"info", "warning", "error"}
    assert held["suggestions"]
    assert held["target"]["workflow"] == {
        "wf_uuid": "root-uuid",
        "root_wf_uuid": "root-uuid",
    }
    assert findings["failed_ID0000003"]["category"] == "failed"
    assert findings["failed_ID0000003"]["suggestions"]
    assert _source(document, "condor_history")["queried"] is True
    _assert_golden("diagnose", document, run)


def test_why_idle_json_matches_golden_with_structured_analysis(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    code, document, _stderr = _monitor(
        "--why-idle",
        "--json",
        str(run),
        runtime=_runtime(_fake_scheduler(tmp_path)),
    )

    assert code == 0
    assert document["modes"] == ["why_idle"]
    analysis = document["why_idle"]
    assert analysis["available"] is True
    assert analysis["pool"]["total_cpus"] > 0
    assert analysis["priority"]["better_priority_users"] == 1
    assert analysis["negotiator"]["last_cycle_matches"] == 12
    assert [item["exec_job_id"] for item in analysis["resource_fit"]["mismatches"]] == [
        "cluster_ID0000001"
    ]
    assert {item["kind"]: item["disposition"] for item in analysis["sources"]} == {
        "queue": "healthy",
        "pool": "healthy",
        "priority": "healthy",
        "negotiator": "healthy",
    }
    _assert_golden("why_idle", document, run)


def test_documents_match_the_contract_file(tmp_path: Path) -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    run = _problem_run(tmp_path)

    _code, document, _stderr = _monitor(
        "--once",
        "--diagnose",
        "--why-idle",
        "--json",
        str(run),
        runtime=_runtime(_fake_scheduler(tmp_path)),
    )

    assert contract["format"] == document["format"] == JSON_OUTPUT_FORMAT
    assert contract["format_version"] == document["format_version"]
    assert contract["format_version"] == JSON_OUTPUT_VERSION
    sections = contract["sections"]
    assert list(document) == sections["envelope"]
    assert list(document["workflow"]) == sections["workflow"]
    assert list(document["workflow"]["sub_workflows"]) == sections["sub_workflows"]
    assert list(document["sources"][0]) == sections["source"]
    assert [item["source"] for item in document["sources"]] == contract["sources"]
    assert list(document["summary"]) == sections["summary"]
    assert list(document["summary"]["job_counts"]) == sections["job_counts"]
    assert list(document["snapshot"]) == sections["snapshot"]
    assert set(sections["snapshot_job_additions"]) <= set(
        document["snapshot"]["jobs"][0]
    )
    assert list(document["diagnostics"]) == sections["diagnostics"]
    assert list(document["diagnostics"]["findings"][0]) == sections["finding"]
    assert list(document["stall"]) == sections["stall"]
    assert list(document["why_idle"]) == sections["why_idle"]
    assert list(document["stall"]["job_counts"]) == sections["stall_job_counts"]
    assert list(document["stall"]["thresholds"]) == sections["stall_thresholds"]
    assert list(document["stall"]["scheduler"]) == sections["stall_scheduler"]
    for counts in (
        document["summary"]["observed_job_counts"],
        document["stall"]["observed_job_counts"],
    ):
        assert list(counts) == sections["observed_job_counts"]
    assert document["summary"]["job_counts_source"] == "stampede"
    assert list(document["why_idle"]["pool"]) == sections["why_idle_pool"]
    assert list(document["why_idle"]["priority"]) == sections["why_idle_priority"]
    assert document["stall"]["reason"] in contract["stall_reasons"]["stalled"]
    assert document["stall"]["method"] == contract["stall_method"]
    assert document["stall"]["activity_source"] in contract["stall_activity_sources"]
    for finding in document["diagnostics"]["findings"]:
        assert finding["category"] in contract["finding_categories"]
        assert finding["severity"] in contract["finding_severities"]
        assert list(finding["target"]) == sections["finding_target"]
    for source in document["sources"]:
        assert source["state"] in contract["source_states"]


def test_stall_quiet_time_runs_from_the_workflows_dagman_files(tmp_path: Path) -> None:
    # Stampede's newest record is at 130 and the fixed clock reads 1000. The
    # queue shows one idle job, so on Stampede's clock this is idle_too_long.
    run = _problem_run(tmp_path)
    _code, document, _stderr = _monitor(
        "--once", "--json", "--no-condor", str(run), runtime=_runtime()
    )
    dag = document["workflow"]["dag_name"]
    scheduler = _fake_scheduler(tmp_path)

    def stall() -> dict:
        _code, document, _stderr = _monitor(
            "--once", "--json", str(run), runtime=_runtime(scheduler)
        )
        return document["stall"]

    before = stall()
    (run / f"{dag}.dagman.out").write_text("DAGMan\n", encoding="utf-8")
    os.utime(run / f"{dag}.dagman.out", (950.0, 950.0))
    (run / f"{dag}.nodes.log").write_text("005 (1000.000.000)\n", encoding="utf-8")
    os.utime(run / f"{dag}.nodes.log", (990.0, 990.0))
    after = stall()

    assert (before["stalled"], before["kind"]) == (True, "idle_too_long")
    assert before["last_activity_epoch"] == before["last_progress_epoch"] == 130.0
    assert before["activity_source"] == "stampede"
    assert (after["stalled"], after["reason"]) == (False, "within_thresholds")
    assert after["last_progress_epoch"] == 130.0
    assert after["last_activity_epoch"] == 990.0
    assert after["seconds_since_activity"] == 10.0
    assert after["activity_source"] == "nodes_log"


def test_contract_names_every_hold_code_and_error_code() -> None:
    from Pegasus.monitor import json_output
    from Pegasus.monitor.diagnostics import _HOLD_PATTERNS
    from Pegasus.monitor.stall_detector import StallKind

    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))

    assert contract["hold_codes"] == [
        *(pattern[1] for pattern in _HOLD_PATTERNS),
        "held_unknown",
    ]
    assert set(contract["error_codes"]) == {
        value for name, value in vars(json_output).items() if name.startswith("ERROR_")
    }
    assert contract["stall_kinds"] == [kind.value for kind in StallKind]
    reasons = contract["stall_reasons"]["stalled"]
    assert set(reasons) == set(contract["stall_kinds"]) - set(
        contract["stall_kinds_live_only"]
    )
    assert set(contract["stall_kinds_one_shot_only"]) <= set(reasons)


def test_no_condor_reports_disabled_sources_not_absent_findings(
    tmp_path: Path,
) -> None:
    run = _problem_run(tmp_path)

    code, document, _stderr = _monitor(
        "--diagnose", "--why-idle", "--json", "--no-condor", str(run)
    )

    assert code == 0
    for name in (
        "condor_queue",
        "condor_history",
        "condor_pool",
        "condor_priority",
        "condor_negotiator",
    ):
        source = _source(document, name)
        assert (source["state"], source["queried"]) == ("disabled", False)
    assert _source(document, "stampede")["state"] == "healthy"
    assert {item["disposition"] for item in document["why_idle"]["sources"]} == {
        "disabled"
    }


def test_once_alone_marks_unconsulted_scheduler_sources(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    _code, document, _stderr = _monitor(
        "--once", "--json", str(run), runtime=_runtime(_fake_scheduler(tmp_path))
    )

    assert _source(document, "condor_queue")["state"] == "healthy"
    assert _source(document, "condor_pool")["state"] == "not_queried"
    assert _source(document, "condor_pool")["queried"] is False


def test_missing_condor_binaries_are_reported_in_source_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _problem_run(tmp_path)
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))

    code, document, _stderr = _monitor(
        "--diagnose", "--why-idle", "--json", str(run), runtime=_runtime()
    )

    assert code == 0
    assert document["error"] is None
    for name in ("condor_queue", "condor_history", "condor_pool"):
        source = _source(document, name)
        assert source["queried"] is True
        assert source["state"] not in {"healthy", "disabled", "not_queried"}
        assert source["error_code"]
    assert document["diagnostics"]["available"] is True
    assert document["why_idle"]["pool"]["total_cpus"] is None


def test_missing_database_is_a_structured_error(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, _tail = create_run(run, terminated=False)
    database.unlink()

    code, document, stderr = _monitor(
        "--once", "--diagnose", "--json", "--no-condor", str(run)
    )

    assert code == 1
    assert document["exit_code"] == 1
    assert document["error"]["code"] == "no_authoritative_snapshot"
    assert "authoritative Stampede snapshot" in stderr
    assert document["workflow"]["wf_uuid"] == "root-uuid"
    assert document["workflow"]["sub_workflows"] is None
    assert document["summary"] is None
    assert document["snapshot"] is None
    assert _source(document, "stampede")["state"] != "healthy"
    assert document["stall"]["assessed"] is False
    assert document["diagnostics"]["available"] is False
    assert not database.exists()


def test_unknown_target_is_a_structured_error(tmp_path: Path) -> None:
    code, document, stderr = _monitor("--once", "--json", str(tmp_path / "absent"))

    assert code == 1
    assert document["error"]["code"] == "workflow_location_error"
    assert str(tmp_path / "absent") in document["error"]["message"]
    assert document["target"] == str(tmp_path / "absent")
    assert document["workflow"] is None
    assert document["sources"] == []
    assert "workflow path does not exist" in stderr


def test_sub_workflows_are_reported_as_not_aggregated(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    _code, document, _stderr = _monitor("--once", "--json", "--no-condor", str(run))

    assert document["workflow"]["is_sub_workflow"] is False
    assert document["workflow"]["sub_workflows"] == {
        "count": 1,
        "aggregated": False,
        "exec_job_ids": ["subwf_ID0000004"],
        "exec_job_ids_omitted": 0,
    }


def test_json_forces_one_shot_on_a_terminal(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)

    code, document, _stderr = _monitor(
        "--diagnose", "--json", "--no-condor", str(run), tty=True
    )

    assert code == 0
    assert document["modes"] == ["diagnose"]


def test_replay_once_json_uses_recorded_diagnostics(tmp_path: Path) -> None:
    run = _problem_run(tmp_path)
    log = tmp_path / "events.jsonl"
    live_code, live, _stderr = _monitor(
        "--once",
        "--diagnose",
        "--json",
        "--log",
        str(log),
        str(run),
        runtime=_runtime(_fake_scheduler(tmp_path)),
    )

    code, document, stderr = _monitor(
        "--replay", str(log), "--once", "--diagnose", "--why-idle", "--json"
    )

    assert (live_code, code, stderr) == (0, 0, "")
    assert document["origin"] == "replay"
    assert document["target"] == str(log)
    assert document["workflow"]["wf_uuid"] == "root-uuid"
    assert document["summary"]["job_counts"] == live["summary"]["job_counts"]
    # A one-shot recorder stops its stream without a final checkpoint.
    assert document["stream"] == {"stop_reason": "stopped", "stopped_by_recorder": True}
    diagnostics = document["diagnostics"]
    assert diagnostics["origin"] == "event_log"
    assert {item["code"] for item in diagnostics["findings"]} == {
        item["code"] for item in live["diagnostics"]["findings"]
    }
    assert all(item["suggestions"] is None for item in diagnostics["findings"])
    assert document["why_idle"] == {
        "available": False,
        "reason": "scheduler_evidence_not_recorded",
    }
    assert _source(document, "condor_queue")["state"] == "disabled"


def test_replay_without_checkpoint_is_a_structured_error(tmp_path: Path) -> None:
    log = tmp_path / "events.jsonl"
    log.write_text("", encoding="utf-8")

    code, document, stderr = _monitor("--replay", str(log), "--once", "--json")

    assert code == 1
    assert document["origin"] == "replay"
    assert document["error"]["code"] in {"invalid_event_log", "no_complete_checkpoint"}
    assert stderr.startswith("pegasus-monitor: ")


def test_no_condor_classifies_a_hold_from_the_reason_stampede_recorded(
    tmp_path: Path,
) -> None:
    # Trial sample diagnose-held-nocondor.json: held_unknown, with evidence
    # citing condor_queue although that source was disabled.
    run = _problem_run(tmp_path)

    code, document, _stderr = _monitor(
        "--diagnose", "--why-idle", "--json", "--no-condor", str(run)
    )

    assert code == 0
    unqueried = {
        source["source"] for source in document["sources"] if not source["queried"]
    }
    assert "condor_queue" in unqueried
    findings = {
        item["target"]["exec_job_id"]: item
        for item in document["diagnostics"]["findings"]
    }
    held = findings["held_ID0000002"]
    # The fixture's recorded reason is the bare word "memory", which matches
    # no hold pattern; the point is that the reason is found and attributed.
    assert held["code"] == "held_unknown"
    assert held["summary"] == "Job is held by HTCondor: memory"
    assert held["evidence"][1] == {
        "source": "stampede",
        "code": "hold_reason",
        "payload": {"hold_reason_code": None, "reason_excerpt": "memory"},
    }
    for finding in findings.values():
        assert not {item["source"] for item in finding["evidence"]} & unqueried
    assert (
        "held_ID0000002: HELD by HTCondor; memory" in document["why_idle"]["findings"]
    )
    assert document["stall"]["scheduler"] == {
        "queue_queried": False,
        "dagman_in_queue": None,
        "jobs_in_queue": None,
        "queue_complete": None,
        "dagman_cluster_id": None,
        "dagman_is_wrapper": None,
        "recorded_jobs_missing": None,
    }
