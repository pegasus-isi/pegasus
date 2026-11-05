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
from time import thread_time

import pytest

from Pegasus.monitor.coordinator import CoordinatorSnapshot
from Pegasus.monitor.diagnostics import (
    DiagnosticCategory,
    DiagnosticFinding,
    DiagnosticsEngine,
    KickstartReadStatus,
    collect_diagnostics,
    parse_kickstart_output,
    parse_pegasuslite_stderr,
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
        sequence=sequence,
        clock=ClockSample(float(sequence), float(sequence)),
        effective=effective,
        source_health=(),
        scheduler_results=(),
        pending_tail_events=0,
        unconfirmed_tail_events=(),
        last_tail_event_age=None,
        semantic_progress=sequence,
        latest_effective_event=None,
        has_authoritative_base=True,
        authoritative_complete=False,
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


def test_redaction_strips_terminal_control_sequences() -> None:
    value = "missing \x1b]0;pwned\x07file \x1b[2J\x9b31mred\x00 end"
    excerpt = redact_excerpt(value, 512)
    assert not any(ord(c) < 0x20 or 0x7F <= ord(c) <= 0x9F for c in excerpt)
    assert excerpt == "missing ]0;pwned file [2J 31mred end"


def test_redaction_is_linear_for_long_non_secret_text() -> None:
    started = thread_time()
    excerpt = redact_excerpt("a" * 100_000, 512)
    elapsed = thread_time() - started
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


def test_engine_refreshes_late_hold_reason_without_duplicate_findings() -> None:
    engine = DiagnosticsEngine()
    job = _job("JOB_HELD")
    first = engine.analyze(_publication(1, job))
    assert first.findings[0].code == "held_unknown"
    publication = replace(
        _publication(2, job), scheduler_results=(_queue("Memory limit exceeded"),)
    )
    updated = engine.analyze(publication)
    assert updated.findings[0].code == "memory_exceeded"
    assert updated.new_findings == updated.findings
    repeated = engine.analyze(replace(publication, sequence=3))
    assert repeated.findings == updated.findings
    assert repeated.new_findings == ()


@pytest.mark.parametrize("initial_file", [None, "- mainjob: ["])
def test_engine_retries_incomplete_kickstart_with_backoff(
    tmp_path, monkeypatch, initial_file
) -> None:
    path = tmp_path / "result.out.000"
    if initial_file is not None:
        path.write_text(initial_file)
    calls = []

    def parse(*args, **kwargs):
        calls.append(args)
        return parse_kickstart_output(*args, **kwargs)

    monkeypatch.setattr("Pegasus.monitor.diagnostics.parse_kickstart_output", parse)
    engine = DiagnosticsEngine()
    job = _job("JOB_FAILURE", stdout_path=path.name)
    first = engine.analyze(_publication(1, job), submit_dir=tmp_path)
    assert first.findings[0].code == "exit_code_1"
    for sequence in (2, 3, 4, 5):
        assert (
            engine.analyze(
                _publication(sequence, job), submit_dir=tmp_path
            ).new_findings
            == ()
        )
    assert len(calls) == 1
    retry = engine.analyze(_publication(11, job), submit_dir=tmp_path)
    assert retry.new_findings == ()
    assert len(calls) == 2

    _write_kickstart(path, stderr="No space left on device")
    recovered = engine.analyze(_publication(12, job), submit_dir=tmp_path)
    assert recovered.findings[0].code == "no_space"
    assert recovered.new_findings == recovered.findings
    assert len(calls) == 3
    assert engine.analyze(_publication(20, job), submit_dir=tmp_path).new_findings == ()
    assert len(calls) == 3


def test_engine_refreshes_changed_attempt_evidence_without_state_change() -> None:
    engine = DiagnosticsEngine()
    job = _job("JOB_FAILURE")
    first = engine.analyze(_publication(1, job))
    assert first.findings[0].code == "exit_code_1"
    changed = replace(job, attempts=(replace(job.attempts[0], raw_wait_status=512),))
    updated = engine.analyze(_publication(2, changed))
    assert updated.findings[0].code == "exit_code_2"
    assert updated.new_findings == updated.findings


def _engine_analyze_cpu_seconds(monkeypatch, count: int) -> float:
    jobs = tuple(_indexed_job(index) for index in range(1, count + 1))
    effective = _effective_many(jobs)
    publication = CoordinatorSnapshot(
        sequence=1,
        clock=ClockSample(1.0, 1.0),
        effective=effective,
        source_health=(),
        scheduler_results=(),
        pending_tail_events=0,
        unconfirmed_tail_events=(),
        last_tail_event_age=None,
        semantic_progress=1,
        latest_effective_event=None,
        has_authoritative_base=True,
        authoritative_complete=False,
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
    started = thread_time()
    batch = engine.analyze(publication)
    elapsed = thread_time() - started
    assert len(batch.new_findings) == len(jobs)
    return elapsed


def test_engine_problem_lookup_is_linear_at_scale(monkeypatch) -> None:
    # Compare growth rather than a fixed budget: line-tracing coverage on
    # Python < 3.12 roughly doubles CPU time but cancels out of the ratio.
    # Quadrupling the jobs costs ~4x when linear and ~16x when quadratic.
    small = min(_engine_analyze_cpu_seconds(monkeypatch, 2000) for _ in range(2))
    large = _engine_analyze_cpu_seconds(monkeypatch, 8000)
    assert large < 8 * max(small, 0.005)


@pytest.mark.parametrize("missing", [(), (_queue(""),)])
def test_missing_queue_reason_does_not_downgrade_specific_finding(missing):
    engine = DiagnosticsEngine()
    job = _job("JOB_HELD")
    known = engine.analyze(
        replace(
            _publication(1, job), scheduler_results=(_queue("Memory limit exceeded"),)
        )
    )
    for sequence, results in [(2, missing), (3, (_queue("Memory limit exceeded"),))]:
        batch = engine.analyze(
            replace(_publication(sequence, job), scheduler_results=results)
        )
        assert batch.findings == known.findings
        assert batch.new_findings == ()
    released = engine.analyze(_publication(4, _job("JOB_RELEASED")))
    assert released.findings == ()
    assert (
        engine.analyze(_publication(5, _job("JOB_HELD", timestamp="12")))
        .findings[0]
        .code
        == "held_unknown"
    )


def test_unavailable_submit_directory_is_not_retried(monkeypatch):
    from Pegasus.monitor import diagnostics

    calls = []
    real = diagnostics.collect_diagnostics

    def collect(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(diagnostics, "collect_diagnostics", collect)
    engine = DiagnosticsEngine()
    for sequence in (1, 10, 100, 1000):
        engine.analyze(_publication(sequence, _job("JOB_FAILURE")))
    assert len(calls) == 1
    assert engine._retry_after == {}


def test_kickstart_retries_are_bounded_and_staggered(tmp_path, monkeypatch):
    calls = []

    def parse(*args, **kwargs):
        calls.append(args)
        return parse_kickstart_output(*args, **kwargs)

    monkeypatch.setattr("Pegasus.monitor.diagnostics.parse_kickstart_output", parse)
    engine = DiagnosticsEngine()
    job = _job("JOB_FAILURE", stdout_path="missing")
    engine.analyze(_publication(1, job), submit_dir=tmp_path)
    deadlines = []
    now = 1
    while engine._retry_after:
        deadline = next(iter(engine._retry_after.values()))
        deadlines.append(deadline - now)
        now = deadline
        publication = replace(_publication(2, job), clock=ClockSample(now, now))
        engine.analyze(publication, submit_dir=tmp_path)
    assert len(calls) == 6
    assert all(b >= 2 * a - 1e-8 for a, b in zip(deadlines, deadlines[1:]))
    engine.analyze(_publication(10000, job), submit_dir=tmp_path)
    assert len(calls) == 6
    _write_kickstart(tmp_path / "missing", stderr="No space left on device")
    assert (
        engine.analyze(_publication(10001, job), submit_dir=tmp_path).findings[0].code
        == "no_space"
    )
    deadlines = []
    for name in ("job1", "job2", "job3"):
        engine = DiagnosticsEngine()
        engine.analyze(
            _publication(
                1,
                replace(
                    job,
                    exec_job_id=name,
                    transition=replace(job.transition, exec_job_id=name),
                ),
            ),
            submit_dir=tmp_path / name,
        )
        deadlines.append(next(iter(engine._retry_after.values())))
    assert len(set(deadlines)) == 3


@pytest.mark.parametrize("replacement", [False, True])
def test_parseable_kickstart_changes_refresh_cached_finding(tmp_path, replacement):
    path = tmp_path / "result.out.000"
    _write_kickstart(path)
    engine = DiagnosticsEngine()
    job = _job("JOB_FAILURE", stdout_path=path.name)
    engine.analyze(_publication(1, job), submit_dir=tmp_path)
    target = tmp_path / "replacement" if replacement else path
    _write_kickstart(target, stderr="No space left on device")
    if replacement:
        target.replace(path)
    batch = engine.analyze(_publication(2, job), submit_dir=tmp_path)
    assert batch.findings[0].code == "no_space"
    assert batch.new_findings == batch.findings


def _scheduler_observation(
    status=5, *, kind=SchedulerQueryKind.QUEUE, health=HealthState.HEALTHY, cluster=10
):
    result = _queue("Transfer output files failure: No such file token=secret-value")
    source = (
        SourceName.CONDOR_QUEUE
        if kind is SchedulerQueryKind.QUEUE
        else SourceName.CONDOR_HISTORY
    )
    return replace(
        result,
        request=replace(result.request, kind=kind),
        health=SourceHealth(source, health, 20.0),
        evidence=(
            SchedulerEvidence(
                kind,
                FrozenPayload.from_mapping({"ClusterId": cluster, "ProcId": 0}),
                FrozenPayload.from_mapping(
                    {
                        "DAGNodeName": "job",
                        "JobStatus": status,
                        "HoldReason": "Transfer output files failure: No such file token=secret-value",
                    }
                ),
            ),
        ),
    )


def test_queue_hold_is_visible_before_log_and_clears_on_release():
    from Pegasus.monitor.why_idle import analyze_why_idle

    job = _job("EXECUTE")
    engine = DiagnosticsEngine()
    held = replace(_publication(1, job), scheduler_results=(_scheduler_observation(),))
    batch = engine.analyze(held)
    assert batch.findings[0].code == "output_missing"
    assert "secret-value" not in str(batch.findings)
    assert "HELD" in analyze_why_idle(held).findings[0]
    assert held.effective.jobs[0].state == "EXECUTE"
    released = replace(
        _publication(2, job), scheduler_results=(_scheduler_observation(2),)
    )
    assert not engine.analyze(released).findings
    assert not any("HELD" in finding for finding in analyze_why_idle(released).findings)
    assert engine.analyze(replace(held, sequence=3)).new_findings


@pytest.mark.parametrize(
    "health", [HealthState.STALE, HealthState.DISABLED, HealthState.UNAVAILABLE]
)
def test_unhealthy_queue_does_not_invent_active_hold(health):
    snapshot = replace(
        _publication(1, _job("EXECUTE")),
        scheduler_results=(_scheduler_observation(health=health),),
    )
    assert not DiagnosticsEngine().analyze(snapshot).findings


def test_hold_evidence_does_not_cross_attempt_or_workflow():
    job = _job("EXECUTE")
    wrong_attempt = replace(
        _publication(1, job), scheduler_results=(_scheduler_observation(cluster=9),)
    )
    assert not DiagnosticsEngine().analyze(wrong_attempt).findings
    result = _scheduler_observation()
    result = replace(
        result,
        request=replace(result.request, workflow=WorkflowIdentity("other", "other")),
    )
    assert not collect_diagnostics(_effective(job), (result,))


def test_hold_cause_survives_abort_and_restart(tmp_path):
    engine = DiagnosticsEngine()
    held = replace(
        _publication(1, _job("EXECUTE")), scheduler_results=(_scheduler_observation(),)
    )
    engine.analyze(held)
    _write_kickstart(tmp_path / "job.out.000", exit_code=42)
    failed = _publication(2, _job("JOB_ABORTED"))
    finding = engine.analyze(failed, submit_dir=tmp_path).findings[0]
    assert finding.code == "output_missing"
    assert "preceding hold" in finding.summary
    assert "application exit code 42" in finding.summary
    assert "recorded job exit code 1" in finding.summary
    assert "secret-value" not in str(finding)
    restarted = replace(
        failed,
        scheduler_results=(_scheduler_observation(3, kind=SchedulerQueryKind.HISTORY),),
    )
    again = DiagnosticsEngine().analyze(restarted, submit_dir=tmp_path).findings[0]
    assert again.code == "output_missing"
    assert "application exit code 42" in again.summary
    app_evidence = next(
        item for item in again.evidence if item.source is SourceName.KICKSTART
    )
    assert app_evidence.payload.to_json_dict()["application_exit_code"] == 42


def test_flat_kickstart_fallback_remains_bounded(tmp_path):
    _write_kickstart(tmp_path / "job.out.000", exit_code=42)
    assert parse_kickstart_output(tmp_path, "job").info.exit_code == 42
    assert (
        parse_kickstart_output(tmp_path, "job", maximum_bytes=10).status
        is KickstartReadStatus.TOO_LARGE
    )
    (tmp_path / "job.out.000").unlink()
    (tmp_path / "job.out.000").symlink_to(tmp_path / "outside")
    assert (
        parse_kickstart_output(tmp_path, "job").status is KickstartReadStatus.REJECTED
    )


def test_flat_kickstart_update_invalidates_cached_diagnosis(tmp_path):
    engine = DiagnosticsEngine()
    publication = _publication(1, _job("JOB_FAILURE"))
    _write_kickstart(tmp_path / "job.out.000", exit_code=42)
    assert (
        engine.analyze(publication, submit_dir=tmp_path).findings[0].code
        == "exit_code_42"
    )
    _write_kickstart(tmp_path / "job.out.000", exit_code=127)
    assert (
        engine.analyze(publication, submit_dir=tmp_path).findings[0].code
        == "exit_code_127"
    )


def test_fresh_release_overrides_delayed_held_log():
    publication = replace(
        _publication(1, _job("JOB_HELD")),
        scheduler_results=(_scheduler_observation(2),),
    )
    assert not DiagnosticsEngine().analyze(publication).findings


def test_previous_attempt_cause_does_not_follow_retry(tmp_path):
    engine = DiagnosticsEngine()
    engine.analyze(
        replace(
            _publication(1, _job("JOB_HELD")),
            scheduler_results=(_scheduler_observation(),),
        )
    )
    job = _job("JOB_ABORTED")
    new_identity = JobAttemptIdentity(1, 3, 4)
    job = replace(
        job,
        attempts=(
            *job.attempts,
            JobAttempt(new_identity, scheduler_id="11.0", exit_code=2),
        ),
        current_attempt=new_identity,
        transition=replace(
            job.transition,
            job_submit_seq=4,
            identity=replace(job.transition.identity, job_instance_id=3),
        ),
    )
    _write_kickstart(tmp_path / "job.out.000", exit_code=42)
    finding = engine.analyze(_publication(2, job), submit_dir=tmp_path).findings[0]
    assert finding.code == "exit_code_2"
    assert "application exit code 42" not in finding.summary


def _lite_job(stderr_path="job.err.000", stdout_path="job.out.000"):
    job = _job("JOB_ABORTED", stdout_path=stdout_path)
    return replace(job, attempts=(replace(job.attempts[0], stderr_path=stderr_path),))


def test_pegasuslite_exit_is_distinct_from_kickstart_and_recorded_status(tmp_path):
    _write_kickstart(tmp_path / "job.out.000", exit_code=0)
    with (tmp_path / "job.out.000").open("a") as stream:
        stream.write(
            "\n  start: 2026-10-23T00:38:42.864+00:00\n  date: 2026-10-23\n---------------pegasus-multipart\n- integrity_verification_attempts: []\n"
        )
    (tmp_path / "job.err.000").write_text(
        "password=do-not-show\nPegasusLite: exitcode 71, tool originally exited with exitcode 3\n"
    )
    finding = collect_diagnostics(
        _effective(_lite_job()), (_scheduler_observation(),), submit_dir=tmp_path
    )[0]
    assert finding.code == "output_missing"
    assert (
        sum("pegasus-analyzer" in suggestion for suggestion in finding.suggestions) == 1
    )
    assert "tool exit code 3" in finding.summary
    assert "Kickstart process exit code 0" in finding.summary
    assert "recorded job exit code 1" in finding.summary
    assert "do-not-show" not in str(finding)
    evidence = next(e for e in finding.evidence if e.source is SourceName.PEGASUS_LITE)
    assert evidence.payload.to_json_dict()["tool_exit_code"] == 3


@pytest.mark.parametrize(
    "text",
    [
        "tool originally exited with exitcode 3",
        "PegasusLite: exitcode 71, tool originally exited with exitcode 999",
        "PegasusLite: exitcode 71, tool originally exited with exitcode 3\nPegasusLite: exitcode 71, tool originally exited with exitcode 4",
    ],
)
def test_pegasuslite_requires_unambiguous_producer_summary(tmp_path, text):
    (tmp_path / "job.err.000").write_text(text)
    assert parse_pegasuslite_stderr(tmp_path, _lite_job()).tool_exit_code is None


@pytest.mark.parametrize("path", ["../secret", "/etc/passwd", "a/../../secret"])
def test_pegasuslite_rejects_unsafe_paths(tmp_path, path):
    assert (
        parse_pegasuslite_stderr(tmp_path, _lite_job(stderr_path=path)).status
        is KickstartReadStatus.REJECTED
    )


def test_pegasuslite_rejects_symlinks_and_oversized_files(tmp_path):
    path = tmp_path / "job.err.000"
    path.symlink_to(tmp_path / "outside")
    assert (
        parse_pegasuslite_stderr(tmp_path, _lite_job()).status
        is KickstartReadStatus.REJECTED
    )
    path.unlink()
    path.write_bytes(b"x" * (1024 * 1024 + 1))
    assert (
        parse_pegasuslite_stderr(tmp_path, _lite_job()).status
        is KickstartReadStatus.TOO_LARGE
    )


def test_late_pegasuslite_stderr_refreshes_diagnosis(tmp_path):
    _write_kickstart(tmp_path / "job.out.000", exit_code=0)
    engine = DiagnosticsEngine()
    publication = _publication(1, _lite_job())
    assert (
        "tool exit code 3"
        not in engine.analyze(publication, submit_dir=tmp_path).findings[0].summary
    )
    (tmp_path / "job.err.000").write_text(
        "PegasusLite: exitcode 71, tool originally exited with exitcode 3\n"
    )
    assert (
        "tool exit code 3"
        in engine.analyze(publication, submit_dir=tmp_path).findings[0].summary
    )


def test_pegasuslite_uses_recorded_retry_path(tmp_path):
    (tmp_path / "job.err.000").write_text(
        "PegasusLite: exitcode 71, tool originally exited with exitcode 3\n"
    )
    (tmp_path / "job.err.001").write_text(
        "PegasusLite: exitcode 72, tool originally exited with exitcode 7\n"
    )
    result = parse_pegasuslite_stderr(tmp_path, _lite_job("job.err.001", "job.out.001"))
    assert result.tool_exit_code == 7


def test_manual_and_unknown_hold_reasons_are_visible_and_redacted():
    for reason, code in [
        ("via condor_hold (by user ubuntu)", "user_hold"),
        ("site maintenance token=secret", "held_unknown"),
    ]:
        finding = collect_diagnostics(_effective(_job("JOB_HELD")), (_queue(reason),))[
            0
        ]
        assert finding.code == code
        assert reason.split(" token=")[0] in finding.summary
        assert "secret" not in finding.summary


def test_kickstart_accepts_pegasuslite_multipart_metadata(tmp_path):
    path = tmp_path / "job.out.000"
    _write_kickstart(path, exit_code=0)
    with path.open("a") as output:
        output.write(
            "\n---------------pegasus-multipart\n- integrity_verification_attempts: []\n"
        )
    result = parse_kickstart_output(tmp_path, "job", "job.out.000")
    assert result.status is KickstartReadStatus.AVAILABLE
    assert result.info.exit_code == 0


def test_manual_hold_code_overrides_custom_reason_and_refreshes_cached_finding():
    reason = "Pause for memory policy investigation"
    result = _queue(reason)
    job = _job("JOB_HELD")
    publication = replace(_publication(1, job), scheduler_results=(result,))
    engine = DiagnosticsEngine()
    assert engine.analyze(publication).findings[0].code == "policy_hold"
    evidence = result.evidence[0]
    coded = replace(
        result,
        evidence=(
            replace(
                evidence,
                payload=FrozenPayload.from_mapping(
                    {**evidence.payload.to_json_dict(), "HoldReasonCode": 1}
                ),
            ),
        ),
    )
    manual = replace(publication, scheduler_results=(coded,))
    finding = engine.analyze(manual).findings[0]
    assert finding.code == "user_hold"
    assert reason in finding.summary
    assert finding.evidence[-1].payload.to_json_dict()["hold_reason_code"] == 1
    assert collect_diagnostics(_effective(job), (coded,))[0].code == "user_hold"
    failed = engine.analyze(_publication(2, _job("JOB_FAILURE"))).findings[0]
    assert failed.code == "user_hold"
    assert "preceding hold: Job was held by a user" in failed.summary


def test_hold_code_does_not_join_previous_retry():
    result = _queue("maintenance")
    evidence = result.evidence[0]
    result = replace(
        result,
        evidence=(
            replace(
                evidence,
                target=FrozenPayload.from_mapping({"ClusterId": 9, "ProcId": 0}),
                payload=FrozenPayload.from_mapping(
                    {**evidence.payload.to_json_dict(), "HoldReasonCode": 1}
                ),
            ),
        ),
    )
    assert (
        collect_diagnostics(_effective(_job("JOB_HELD")), (result,))[0].code
        == "held_unknown"
    )


@pytest.mark.parametrize("state", ["JOB_HELD", "SUBMIT", "EXECUTE"])
def test_removed_job_replaces_stale_hold_or_queue_diagnostic(state):
    result = _scheduler_observation(3, kind=SchedulerQueryKind.HISTORY)
    evidence = result.evidence[0]
    result = replace(
        result,
        evidence=(
            replace(
                evidence,
                payload=FrozenPayload.from_mapping(
                    {
                        **evidence.payload.to_json_dict(),
                        "RemoveReason": "Removed by user alice",
                    }
                ),
            ),
        ),
    )
    engine = DiagnosticsEngine()
    engine.analyze(
        replace(
            _publication(1, _job("JOB_HELD")), scheduler_results=(_queue("manual"),)
        )
    )
    publication = replace(_publication(2, _job(state)), scheduler_results=(result,))
    finding = engine.analyze(publication).findings[0]
    assert finding.code == "job_removed"
    assert "Removed by user alice" in finding.summary
    assert collect_diagnostics(publication)[0] == finding


def test_removal_does_not_join_old_retry():
    result = _scheduler_observation(3, kind=SchedulerQueryKind.HISTORY, cluster=9)
    assert collect_diagnostics(_effective(_job("SUBMIT")), (result,)) == ()


def test_kickstart_open_is_nonblocking_during_fifo_swap(tmp_path, monkeypatch):
    import os

    from Pegasus.monitor import diagnostics

    path = tmp_path / "job.out"
    path.write_text("regular file")
    real_open = os.open

    def swap_before_open(candidate, flags):
        # Assert before opening, so the vulnerable implementation cannot hang.
        assert flags & os.O_NONBLOCK
        path.unlink()
        os.mkfifo(path)
        return real_open(candidate, flags)

    monkeypatch.setattr(diagnostics.os, "open", swap_before_open)
    result = parse_kickstart_output(tmp_path, "job", path.name)
    assert result.status is KickstartReadStatus.REJECTED
    assert result.error_code == "file_changed"
