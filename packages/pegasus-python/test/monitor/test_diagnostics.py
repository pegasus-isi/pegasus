"""Structured diagnostics, secure kickstart parsing, and live dedup tests."""

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

from dataclasses import replace
from decimal import Decimal
from time import monotonic

import pytest

from Pegasus.monitor.coordinator import CoordinatorSnapshot
from Pegasus.monitor.diagnostics import (
    DiagnosticCategory,
    DiagnosticFinding,
    DiagnosticsEngine,
    KickstartReadStatus,
    collect_diagnostics,
    parse_kickstart_output,
    redact_excerpt,
)
from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DBJobTransition,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    DiagnosticSeverity,
    EffectiveSnapshot,
    FrozenPayload,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    Provenance,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    WorkflowIdentity,
    WorkflowSnapshot,
)
from Pegasus.monitor.stall_detector import StallDetector, StallDetectorConfig

WORKFLOW = WorkflowIdentity("wf-diagnostics", "wf-diagnostics")


def _job(
    state: str, *, timestamp: str = "10", stdout_path: str | None = None
) -> JobSnapshot:
    identity = JobAttemptIdentity(1, 2, 3)
    transition = DBJobTransition(
        WORKFLOW,
        "job",
        3,
        DBTransitionIdentity(2, state, Decimal(timestamp), int(Decimal(timestamp))),
    )
    return JobSnapshot(
        WORKFLOW,
        1,
        "job",
        "compute",
        1,
        (),
        (
            JobAttempt(
                identity,
                scheduler_id="10.0",
                raw_wait_status=256,
                stdout_path=stdout_path,
            ),
        ),
        identity,
        state,
        Decimal(timestamp),
        transition,
        Provenance.DB_CONFIRMED,
    )


def _effective(job: JobSnapshot) -> EffectiveSnapshot:
    workflow_transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", Decimal("1")),
        0,
    )
    return EffectiveSnapshot(
        SnapshotEpoch(1),
        WorkflowSnapshot(
            WORKFLOW,
            1,
            "WORKFLOW_STARTED",
            None,
            0,
            Decimal("1"),
            None,
            workflow_transition,
        ),
        (job,),
        DatabaseGeneration(1, 2, 3),
        None,
        20.0,
        20.0,
    )


def _indexed_job(index: int, state: str = "JOB_HELD") -> JobSnapshot:
    identity = JobAttemptIdentity(index, 10000 + index, index)
    timestamp = Decimal(index)
    name = f"job{index}"
    transition = DBJobTransition(
        WORKFLOW,
        name,
        index,
        DBTransitionIdentity(10000 + index, state, timestamp, index),
    )
    return JobSnapshot(
        WORKFLOW,
        index,
        name,
        "compute",
        1,
        (),
        (JobAttempt(identity),),
        identity,
        state,
        timestamp,
        transition,
        Provenance.DB_CONFIRMED,
    )


def _effective_many(jobs: tuple[JobSnapshot, ...]) -> EffectiveSnapshot:
    base = _effective(jobs[0])
    return replace(base, jobs=jobs)


def _publication(sequence: int, job: JobSnapshot) -> CoordinatorSnapshot:
    effective = replace(_effective(job), epoch=SnapshotEpoch(sequence))
    return CoordinatorSnapshot(
        sequence,
        ClockSample(float(sequence), float(sequence)),
        effective,
        (),
        (),
        0,
        sequence,
        None,
        True,
        False,
    )


def _queue(reason: str) -> SchedulerQueryResult:
    request = SchedulerQueryRequest(
        WORKFLOW,
        SchedulerQueryKind.QUEUE,
        ClockSample(20.0, 20.0),
        5.0,
        10,
    )
    return SchedulerQueryResult(
        request,
        SourceHealth(SourceName.CONDOR_QUEUE, HealthState.HEALTHY, 20.0),
        0.0,
        (
            SchedulerEvidence(
                SchedulerQueryKind.QUEUE,
                FrozenPayload.from_mapping({"ClusterId": 10, "ProcId": 0}),
                FrozenPayload.from_mapping(
                    {"DAGNodeName": "job", "HoldReason": reason}
                ),
            ),
        ),
    )


def _write_kickstart(path, *, stderr: str = "boom", exit_code: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            (
                "- mainjob:",
                "    status:",
                f"      regular_exitcode: {exit_code}",
                "    executable:",
                "      file_name: /bin/example",
                "    argument_vector: [--input, data.txt]",
                "    duration: 2.5",
                "  transformation: example::task",
                "  files:",
                "    stdout:",
                "      data: ok",
                "    stderr:",
                f"      data: {stderr!r}",
            )
        )
    )


def test_parse_kickstart_valid_bounded_record(tmp_path) -> None:
    path = tmp_path / "00" / "00" / "job.out.000"
    _write_kickstart(path, stderr="Expected local file does not exist: /tmp/input")
    result = parse_kickstart_output(tmp_path, "job")
    assert result.status is KickstartReadStatus.AVAILABLE
    assert result.info is not None
    assert result.info.exit_code == 1
    assert result.info.stderr_analysis is not None
    assert result.info.stderr_analysis.missing_files == ("/tmp/input",)


@pytest.mark.parametrize("path", ["../secret", "/etc/passwd", "a/../../secret"])
def test_parse_kickstart_rejects_path_escape(tmp_path, path: str) -> None:
    result = parse_kickstart_output(tmp_path, "job", path)
    assert result.status is KickstartReadStatus.REJECTED
    assert result.error_code == "unsafe_path"


def test_parse_kickstart_rejects_symlink(tmp_path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside.yml"
    outside.write_text("[]")
    link = tmp_path / "job.out.000"
    link.symlink_to(outside)
    result = parse_kickstart_output(tmp_path, "job", "job.out.000")
    assert result.status is KickstartReadStatus.REJECTED
    assert result.error_code == "symlink"


def test_parse_kickstart_rejects_symlinked_parent(tmp_path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "linked").symlink_to(real, target_is_directory=True)
    (real / "job.out.000").write_text("{}")
    result = parse_kickstart_output(tmp_path, "job", "linked/job.out.000")
    assert result.status is KickstartReadStatus.REJECTED
    assert result.error_code == "symlink_parent"


def test_parse_kickstart_enforces_size_and_yaml_alias_limits(tmp_path) -> None:
    large = tmp_path / "large.out.000"
    large.write_text("x" * 20)
    assert (
        parse_kickstart_output(
            tmp_path, "job", "large.out.000", maximum_bytes=10
        ).status
        is KickstartReadStatus.TOO_LARGE
    )
    aliased = tmp_path / "alias.out.000"
    aliased.write_text("mainjob: &job {}\ncopy: *job\n")
    result = parse_kickstart_output(tmp_path, "job", "alias.out.000")
    assert result.status is KickstartReadStatus.MALFORMED


def test_redaction_precedes_excerpt_bound() -> None:
    value = "password=super-secret Bearer abcdef " + "x" * 1000
    excerpt = redact_excerpt(value, 80)
    assert "super-secret" not in excerpt
    assert "abcdef" not in excerpt
    assert len(excerpt) == 80


@pytest.mark.parametrize(
    ("value", "secret"),
    [
        ("Authorization: Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"),
        ("AWS_SECRET_ACCESS_KEY=aws-secret-value", "aws-secret-value"),
        ("API_KEY=api-secret-value", "api-secret-value"),
        ("ACCESS_TOKEN: access-token-value", "access-token-value"),
    ],
)
def test_redaction_covers_common_credential_forms(value: str, secret: str) -> None:
    excerpt = redact_excerpt(f"prefix {value} suffix", 512)
    assert secret not in excerpt
    assert "<redacted>" in excerpt


def test_redaction_is_linear_for_long_non_secret_text() -> None:
    started = monotonic()
    excerpt = redact_excerpt("a" * 100_000, 512)
    elapsed = monotonic() - started
    assert len(excerpt) == 512
    assert elapsed < 0.25


def test_held_diagnostic_has_structured_condor_evidence() -> None:
    finding = collect_diagnostics(
        _effective(_job("JOB_HELD")),
        (_queue("Job exceeded memory limit token=do-not-show"),),
    )[0]
    assert finding.category is DiagnosticCategory.HELD
    assert finding.severity is DiagnosticSeverity.WARNING
    assert finding.code == "memory_exceeded"
    assert {item.source for item in finding.evidence} == {
        SourceName.STAMPEDE,
        SourceName.CONDOR_QUEUE,
    }
    assert "do-not-show" not in str(finding.evidence[1].payload.to_json_dict())


@pytest.mark.parametrize(
    ("reason", "code"),
    [
        ("Transfer output files failure: No such file", "output_missing"),
        ("Transfer output files failure", "output_transfer_failed"),
        ("Transfer input files failure", "input_transfer_failed"),
        ("MEMORY_EXCEEDED", "memory_exceeded"),
        ("DISK_EXCEEDED", "disk_exceeded"),
        ("TIME_EXCEEDED", "walltime_exceeded"),
        ("credential expired", "credential_failure"),
        ("SHADOW_EXCEPTION", "shadow_exception"),
        ("DNS cannot resolve", "dns_failure"),
        ("apptainer failed", "container_failure"),
        ("periodic hold policy", "policy_hold"),
    ],
)
def test_hold_reason_inventory(reason: str, code: str) -> None:
    finding = collect_diagnostics(_effective(_job("JOB_HELD")), (_queue(reason),))[0]
    assert finding.code == code


def test_failed_diagnostic_uses_current_attempt_and_kickstart(tmp_path) -> None:
    path = tmp_path / "result.out.000"
    _write_kickstart(
        path,
        stderr="Starting transfers - attempt 3; Some transfers failed",
        exit_code=127,
    )
    finding = collect_diagnostics(
        _effective(_job("JOB_FAILURE", stdout_path="result.out.000")),
        submit_dir=tmp_path,
    )[0]
    assert finding.category is DiagnosticCategory.FAILED
    assert finding.code == "transfer_failed"
    assert finding.severity is DiagnosticSeverity.ERROR
    assert {item.source for item in finding.evidence} == {
        SourceName.STAMPEDE,
        SourceName.KICKSTART,
    }
    assert not (tmp_path / "diagnostics-events.jsonl").exists()


@pytest.mark.parametrize(
    ("stderr", "code"),
    [
        ("integrity verification failed", "integrity_failed"),
        ("Permission denied while opening: /tmp/data", "permission_denied"),
        ("No space left on device", "no_space"),
        ("Connection timed out", "network_failure"),
    ],
)
def test_kickstart_failure_pattern_inventory(tmp_path, stderr: str, code: str) -> None:
    path = tmp_path / "result.out.000"
    _write_kickstart(path, stderr=stderr)
    finding = collect_diagnostics(
        _effective(_job("JOB_FAILURE", stdout_path="result.out.000")),
        submit_dir=tmp_path,
    )[0]
    assert finding.code == code


def test_engine_retains_active_finding_without_reparsing(monkeypatch, tmp_path) -> None:
    path = tmp_path / "result.out.000"
    _write_kickstart(path)
    engine = DiagnosticsEngine(
        StallDetector(StallDetectorConfig(startup_grace_seconds=9999))
    )
    job = _job("JOB_FAILURE", stdout_path="result.out.000")
    first = engine.analyze(_publication(1, job), submit_dir=tmp_path)
    assert len(first.findings) == len(first.new_findings) == 1

    def fail_if_reparsed(*args, **kwargs):
        raise AssertionError("kickstart file was reparsed")

    monkeypatch.setattr(
        "Pegasus.monitor.diagnostics.parse_kickstart_output", fail_if_reparsed
    )
    second = engine.analyze(_publication(2, job), submit_dir=tmp_path)
    assert second.findings == first.findings
    assert second.new_findings == ()


def test_engine_rediagnoses_held_after_release_and_rehold() -> None:
    engine = DiagnosticsEngine(
        StallDetector(StallDetectorConfig(startup_grace_seconds=9999))
    )
    held = _job("JOB_HELD", timestamp="10")
    first = engine.analyze(_publication(1, held))
    assert len(first.new_findings) == 1
    released = _job("JOB_RELEASED", timestamp="11")
    middle = engine.analyze(_publication(2, released))
    assert middle.findings == ()
    reheld = _job("JOB_HELD", timestamp="12")
    last = engine.analyze(_publication(3, reheld))
    assert len(last.new_findings) == 1


def test_engine_problem_lookup_is_linear_at_scale(monkeypatch) -> None:
    jobs = tuple(_indexed_job(index) for index in range(1, 8001))
    effective = _effective_many(jobs)
    publication = CoordinatorSnapshot(
        1,
        ClockSample(1.0, 1.0),
        effective,
        (),
        (),
        0,
        1,
        None,
        True,
        False,
    )
    findings = tuple(
        DiagnosticFinding(
            job.exec_job_id,
            DiagnosticCategory.HELD,
            DiagnosticSeverity.WARNING,
            "held_unknown",
            "held",
            (),
            job.provenance.value,
            (),
        )
        for job in jobs
    )
    monkeypatch.setattr(
        "Pegasus.monitor.diagnostics.collect_diagnostics",
        lambda *args, **kwargs: findings,
    )
    engine = DiagnosticsEngine(
        StallDetector(StallDetectorConfig(startup_grace_seconds=9999))
    )
    started = monotonic()
    batch = engine.analyze(publication)
    elapsed = monotonic() - started
    assert len(batch.new_findings) == len(jobs)
    assert elapsed < 1.0
