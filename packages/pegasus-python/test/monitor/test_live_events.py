"""Deterministic parser and lifecycle tests for the native live-event tail."""

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

import os
from pathlib import Path

import pytest

from Pegasus.monitor import live_events
from Pegasus.monitor.live_events import (
    LiveEventParseError,
    LiveEventTail,
    parse_live_event_line,
)
from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    HealthState,
    TailGeneration,
    TailJobEvent,
    TailPollRequest,
    TailSourceEvent,
    TailSourceMarker,
    TailWorkflowEvent,
    WorkflowIdentity,
)

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
DB_GENERATION = DatabaseGeneration(2, 10, 20)
GENERATION = TailGeneration(3, 11, 21)
FIXTURES = Path(__file__).parent / "fixtures" / "live_events"


def request(
    epoch: float = 100.0,
    monotonic: float = 500.0,
    *,
    max_bytes: int = 1024 * 1024,
    max_lines: int = 100,
) -> TailPollRequest:
    return TailPollRequest(
        WORKFLOW,
        DB_GENERATION,
        ClockSample(epoch, monotonic),
        max_bytes,
        max_lines,
    )


def parse(line: str, *, start: int = 0):
    return parse_live_event_line(
        line,
        workflow=WORKFLOW,
        source_generation=GENERATION,
        start_offset=start,
        end_offset=start + len(line.encode()) + 1,
        observed_at_monotonic=500.0,
        base_db_generation=DB_GENERATION,
    )


def append(path: Path, data: str | bytes) -> None:
    encoded = data.encode() if isinstance(data, str) else data
    with path.open("ab", buffering=0) as stream:
        stream.write(encoded)


def job_line(
    timestamp: int,
    state: str = "EXECUTE",
    value: str = "123.0",
    seq: int = 1,
    job: str = "compute_ID0000001",
) -> str:
    return f"{timestamp} {job} {state} {value} local 3600 {seq}\n"


def test_parse_job_lines_preserves_raw_fields_and_state_aware_value() -> None:
    submit = parse("100 compute_ID0000001 SUBMIT 123.0 local 3600 7")
    success = parse("101 compute_ID0000001 JOB_SUCCESS 0 local - 7", start=60)
    held = parse("102 compute_ID0000001 JOB_HELD_REASON - - - 7", start=120)

    assert isinstance(submit, TailJobEvent)
    assert submit.scheduler_id == "123.0"
    assert submit.status is None
    assert submit.walltime_seconds == 3600
    assert submit.raw_value == "123.0"
    assert submit.raw_site == "local"
    assert submit.raw_walltime == "3600"
    assert submit.identity.source_generation == GENERATION
    assert submit.base_db_generation == DB_GENERATION
    assert success.status == 0
    assert success.scheduler_id is None
    assert success.walltime_seconds is None
    assert held.normalized_state == "JOB_HELD"
    assert held.status is None and held.scheduler_id is None


def test_unknown_job_state_is_preserved_without_guessing() -> None:
    event = parse("100 compute_ID0000001 FUTURE_STATE 12 local - 7")
    assert isinstance(event, TailJobEvent)
    assert event.state == "FUTURE_STATE"
    assert event.normalized_state == "FUTURE_STATE"
    assert event.scheduler_id == "12"


@pytest.mark.parametrize(
    "line",
    [
        "100 too few",
        "100  compute_ID0000001 EXECUTE 1 local - 7",
        "100\tcompute_ID0000001\tEXECUTE\t1\tlocal\t-\t7",
        "100.0 compute_ID0000001 EXECUTE 1 local - 7",
        "100 compute_ID0000001 JOB_SUCCESS nope local - 7",
        "100 compute_ID0000001 EXECUTE 1 local - -",
        "100 compute_ID0000001 EXECUTE 1 local nope 7",
        "100 compute_ID0000001 EXECUTE None local - 7",
        "100 compute_ID0000001 EXECUTE 1 None - 7",
        "100 compute_ID0000001 EXECUTE 1 local None 7",
    ],
)
def test_strict_seven_field_job_parser_rejects_malformed_lines(line: str) -> None:
    with pytest.raises(LiveEventParseError):
        parse(line)


@pytest.mark.parametrize(
    ("line", "event_type", "status", "cluster"),
    [
        (
            "100 INTERNAL *** MONITORD_STARTED ***",
            TailSourceEvent,
            None,
            None,
        ),
        (
            "101 INTERNAL *** MONITORD_FINISHED 0 ***",
            TailSourceEvent,
            0,
            None,
        ),
        (
            "102 INTERNAL *** DAGMAN_STARTED 123.0 ***",
            TailWorkflowEvent,
            None,
            "123.0",
        ),
        (
            "103 INTERNAL *** DAGMAN_STARTED None ***",
            TailWorkflowEvent,
            None,
            None,
        ),
        (
            "104 INTERNAL *** DAGMAN_FINISHED 1 ***",
            TailWorkflowEvent,
            1,
            None,
        ),
        (
            "105 INTERNAL *** DAGMAN_FINISHED None ***",
            TailWorkflowEvent,
            None,
            None,
        ),
    ],
)
def test_internal_markers_are_parsed_separately(
    line: str, event_type: type, status: int | None, cluster: str | None
) -> None:
    event = parse(line)
    assert isinstance(event, event_type)
    assert event.status == status
    assert getattr(event, "dagman_cluster", None) == cluster


def test_source_markers_do_not_become_workflow_transitions() -> None:
    started = parse("100 INTERNAL *** MONITORD_STARTED ***")
    finished = parse("101 INTERNAL *** MONITORD_FINISHED 1 ***")
    assert isinstance(started, TailSourceEvent)
    assert started.marker is TailSourceMarker.MONITORD_STARTED
    assert isinstance(finished, TailSourceEvent)
    assert finished.marker is TailSourceMarker.MONITORD_FINISHED


@pytest.mark.parametrize(
    "line",
    [
        "100 INTERNAL *** MONITORD_STARTED 0 ***",
        "100 INTERNAL *** MONITORD_FINISHED ***",
        "100 INTERNAL *** MONITORD_FINISHED None ***",
        "100 INTERNAL *** MONITORD_FINISHED - ***",
        "100 INTERNAL *** DAGMAN_STARTED ***",
        "100 INTERNAL *** DAGMAN_STARTED - ***",
        "100 INTERNAL *** DAGMAN_FINISHED nope ***",
        "100 INTERNAL *** DAGMAN_FINISHED - ***",
        "100 INTERNAL *** UNKNOWN 0 ***",
    ],
)
def test_internal_marker_shape_is_strict(line: str) -> None:
    with pytest.raises(LiveEventParseError):
        parse(line)


def test_fixture_corpus_has_expected_valid_and_invalid_lines() -> None:
    valid = (FIXTURES / "valid.log").read_text().splitlines()
    malformed = (FIXTURES / "malformed.log").read_text().splitlines()
    assert (
        len([parse(line, start=index * 100) for index, line in enumerate(valid)]) == 6
    )
    for line in malformed:
        with pytest.raises(LiveEventParseError):
            parse(line)


def test_attachment_starts_at_newline_aligned_eof_and_skips_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    historical = job_line(90)
    path.write_text(historical)
    tail = LiveEventTail(path)
    append(path, job_line(100, "JOB_SUCCESS", "0"))

    result = tail.poll(request())

    assert [event.state for event in result.job_events] == ["JOB_SUCCESS"]
    assert result.job_events[0].identity.start_offset == len(historical.encode())
    assert result.bytes_read == len(job_line(100, "JOB_SUCCESS", "0").encode())
    assert result.health.state is HealthState.HEALTHY


def test_attachment_seeds_one_straddling_partial_line(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    history = job_line(90)
    prefix = "100 compute_ID0000001 JOB_SUCCESS"
    path.write_text(history + prefix)
    tail = LiveEventTail(path)
    suffix = " 0 local 3600 1\n"
    append(path, suffix)

    result = tail.poll(request())

    assert len(result.job_events) == 1
    assert result.job_events[0].identity.start_offset == len(history.encode())
    assert result.bytes_read == len(suffix.encode())


@pytest.mark.parametrize("historical", ["", "x\n"])
def test_attachment_accepts_exact_maximum_partial_line(
    tmp_path: Path, historical: str
) -> None:
    path = tmp_path / "jobstate.log"
    partial = job_line(100).rstrip("\n")
    path.write_text(historical + partial)
    tail = LiveEventTail(
        path,
        max_line_bytes=len(partial.encode()),
        max_buffer_bytes=len(partial.encode()) * 2,
    )
    append(path, "\n")

    result = tail.poll(request())

    assert len(result.job_events) == 1
    assert result.job_events[0].identity.start_offset == len(historical.encode())


def test_attachment_discards_prefix_without_bounded_newline(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.write_bytes(b"x" * 80)
    tail = LiveEventTail(path, max_line_bytes=32, max_buffer_bytes=64)

    result = tail.poll(request())

    assert [gap.reason for gap in result.gaps] == ["overlong_attachment_line"]
    assert result.health.state is HealthState.GAP
    assert result.job_events == ()


def test_partial_line_is_preserved_across_bounded_polls(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    line = job_line(100)
    split = len(line) // 2
    append(path, line[:split])

    first = tail.poll(request(max_bytes=split))
    append(path, line[split:])
    second = tail.poll(request(101.0, 501.0))

    assert first.job_events == ()
    assert first.bytes_read == split
    assert first.health.pending_count == 1
    assert len(second.job_events) == 1
    assert second.job_events[0].identity.start_offset == 0


def test_torn_final_line_never_becomes_an_event_on_close(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(path, "100 compute_ID0000001 EXECUTE")

    partial = tail.poll(request())
    tail.close()
    closed = tail.poll(request(101.0, 501.0))

    assert partial.job_events == ()
    assert partial.health.pending_count == 1
    assert closed.job_events == ()
    assert closed.health.state is HealthState.UNAVAILABLE


def test_byte_and_line_limits_are_never_exceeded(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    first_line = job_line(100)
    second_line = job_line(101, "JOB_SUCCESS", "0")
    append(path, first_line + second_line)

    first = tail.poll(request(max_bytes=len(first_line) + 5, max_lines=1))
    second = tail.poll(request(101.0, 501.0, max_bytes=1024, max_lines=1))

    assert first.bytes_read <= first.request.max_bytes
    assert first.lines_read == 1
    assert [event.state for event in first.job_events] == ["EXECUTE"]
    assert second.bytes_read <= second.request.max_bytes
    assert second.lines_read == 1
    assert [event.state for event in second.job_events] == ["JOB_SUCCESS"]


def test_malformed_utf8_and_fields_are_skipped_without_stopping_tail(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(path, b"\xff\xfe\n100 bad\n" + job_line(101).encode())

    result = tail.poll(request())

    assert len(result.job_events) == 1
    assert result.lines_read == 3
    assert result.health.state is HealthState.DEGRADED
    assert result.health.consecutive_failures == 2


def test_overlong_line_is_dropped_and_following_event_survives(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path, max_line_bytes=64, max_buffer_bytes=256)
    append(path, b"x" * 80 + b"\n" + job_line(101).encode())

    result = tail.poll(request())

    assert len(result.job_events) == 1
    assert result.lines_read == 2
    assert result.health.state is HealthState.DEGRADED
    assert result.health.consecutive_failures == 1


def test_missing_at_start_waits_then_arms_empty_creation_from_zero(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    tail = LiveEventTail(path)
    waiting = tail.poll(request())
    path.touch()
    created = tail.poll(request(101.0, 501.0))
    append(path, job_line(102))
    event = tail.poll(request(102.0, 502.0))

    assert waiting.health.state is HealthState.WAITING
    assert created.generation is not None
    assert created.gaps == ()
    assert event.job_events[0].identity.start_offset == 0


def test_missing_then_populated_creation_skips_existing_content_and_reports_gap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    tail = LiveEventTail(path)
    path.write_text(job_line(100))

    attached = tail.poll(request())
    append(path, job_line(101, "JOB_SUCCESS", "0"))
    live = tail.poll(request(101.0, 501.0))

    assert attached.job_events == ()
    assert [gap.reason for gap in attached.gaps] == ["created_with_existing_content"]
    assert attached.health.state is HealthState.REATTACHING
    assert [event.state for event in live.job_events] == ["JOB_SUCCESS"]


def test_inode_rotation_drains_old_descriptor_then_arms_new_eof(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    rotated = tmp_path / "jobstate.log.000"
    path.touch()
    tail = LiveEventTail(path)
    old_generation = tail.generation
    append(path, job_line(100))
    path.rename(rotated)
    path.write_text(job_line(90, job="historical"))

    drained = tail.poll(request())
    reattached = tail.poll(request(101.0, 501.0))
    new_generation = tail.generation
    append(path, job_line(102, "JOB_SUCCESS", "0"))
    live = tail.poll(request(102.0, 502.0))

    assert [event.state for event in drained.job_events] == ["EXECUTE"]
    assert drained.generation == old_generation
    assert drained.health.state is HealthState.REATTACHING
    assert [gap.reason for gap in drained.gaps] == ["source_replaced"]
    assert reattached.health.state is HealthState.REATTACHING
    assert new_generation is not None and new_generation != old_generation
    assert live.generation == new_generation
    assert [event.state for event in live.job_events] == ["JOB_SUCCESS"]


def test_rotation_drain_waits_for_stable_eof_and_captures_late_old_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    rotated = tmp_path / "jobstate.log.000"
    path.touch()
    tail = LiveEventTail(path)
    old_generation = tail.generation
    append(path, job_line(100))
    path.rename(rotated)
    path.write_text(job_line(90, job="historical"))

    detected = tail.poll(request())
    append(rotated, job_line(101, "JOB_SUCCESS", "0"))
    late = tail.poll(request(101.0, 501.0))
    stable = tail.poll(request(102.0, 502.0))

    assert [event.state for event in detected.job_events] == ["EXECUTE"]
    assert [event.state for event in late.job_events] == ["JOB_SUCCESS"]
    assert detected.generation == late.generation == old_generation
    assert tail.generation is not None and tail.generation != old_generation
    assert stable.health.state is HealthState.REATTACHING


def test_rotation_drain_lowers_target_when_old_inode_shrinks(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    rotated = tmp_path / "jobstate.log.000"
    path.touch()
    tail = LiveEventTail(path)
    old_generation = tail.generation
    first_line = job_line(100)
    append(path, first_line + job_line(101, "JOB_SUCCESS", "0"))
    path.rename(rotated)
    path.write_text(job_line(90, job="historical"))

    detected = tail.poll(request(max_bytes=1))
    rotated.write_text(first_line)
    truncated = tail.poll(request(101.0, 501.0))
    stable = tail.poll(request(102.0, 502.0))

    assert detected.generation == old_generation
    assert [gap.reason for gap in detected.gaps] == ["source_replaced"]
    assert [event.state for event in truncated.job_events] == ["EXECUTE"]
    assert [gap.reason for gap in truncated.gaps] == ["replacement_source_truncated"]
    assert truncated.health.state is HealthState.GAP
    assert stable.health.state is HealthState.REATTACHING
    assert tail.generation is not None and tail.generation != old_generation


def test_deletion_drains_old_source_and_waits_for_recreation(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(path, job_line(100))
    path.unlink()

    deleted = tail.poll(request())
    path.touch()
    recreated = tail.poll(request(101.0, 501.0))

    assert len(deleted.job_events) == 1
    assert [gap.reason for gap in deleted.gaps] == ["source_deleted"]
    assert deleted.health.state is HealthState.REATTACHING
    assert recreated.generation is not None


def test_size_regression_establishes_a_new_generation_at_current_eof(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(path, job_line(100))
    first = tail.poll(request())
    old_generation = first.generation
    path.write_bytes(b"")

    truncated = tail.poll(request(101.0, 501.0))
    append(path, job_line(102))
    live = tail.poll(request(102.0, 502.0))

    assert [gap.reason for gap in truncated.gaps] == ["source_truncated"]
    assert truncated.generation != old_generation
    assert truncated.health.state is HealthState.REATTACHING
    assert live.job_events[0].identity.start_offset == 0


def test_truncate_and_regrow_past_cursor_is_detected_by_anchor(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path, anchor_bytes=16)
    append(path, job_line(100))
    old = tail.poll(request()).generation
    replacement_history = job_line(90, job="replacement") * 3
    path.write_text(replacement_history)

    regrown = tail.poll(request(101.0, 501.0))
    append(path, job_line(102))
    live = tail.poll(request(102.0, 502.0))

    assert [gap.reason for gap in regrown.gaps] == ["source_regrew_after_truncate"]
    assert regrown.generation != old
    assert regrown.job_events == ()
    assert live.job_events[0].identity.start_offset == len(replacement_history.encode())


def test_buffer_overflow_produces_explicit_gap_and_resumes_at_eof(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path, max_line_bytes=64, max_buffer_bytes=128)
    append(path, job_line(100) + job_line(101) + job_line(102))

    overflow = tail.poll(request(max_bytes=1024, max_lines=100))
    append(path, job_line(103, "JOB_SUCCESS", "0"))
    live = tail.poll(request(101.0, 501.0))

    assert [gap.reason for gap in overflow.gaps] == ["buffer_overflow"]
    assert overflow.health.state is HealthState.GAP
    assert overflow.job_events == ()
    assert [event.state for event in live.job_events] == ["JOB_SUCCESS"]


def test_overflow_after_an_event_keeps_result_on_the_event_generation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path, max_line_bytes=64, max_buffer_bytes=128)
    append(path, job_line(100) + job_line(101))
    first = tail.poll(request(max_lines=1))
    old_generation = first.generation
    append(path, job_line(102) + job_line(103) + job_line(104))

    overflow = tail.poll(request(101.0, 501.0))
    armed_generation = tail.generation

    assert [event.event_timestamp for event in overflow.job_events] == [101]
    assert overflow.generation == old_generation
    assert overflow.job_events[0].identity.source_generation == old_generation
    assert [gap.reason for gap in overflow.gaps] == ["buffer_overflow"]
    assert armed_generation is not None and armed_generation != old_generation


def test_rapid_dagman_restarts_keep_order_and_multiplicity(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(
        path,
        "100 INTERNAL *** DAGMAN_STARTED 10 ***\n"
        "100 INTERNAL *** DAGMAN_FINISHED 1 ***\n"
        "100 INTERNAL *** DAGMAN_STARTED 11 ***\n"
        "100 INTERNAL *** DAGMAN_FINISHED None ***\n",
    )

    result = tail.poll(request())

    assert [event.marker for event in result.workflow_events] == [
        "DAGMAN_STARTED",
        "DAGMAN_FINISHED",
        "DAGMAN_STARTED",
        "DAGMAN_FINISHED",
    ]
    assert len({event.identity for event in result.workflow_events}) == 4


def test_terminal_markers_are_visible_before_any_db_confirmation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    append(
        path,
        "100 INTERNAL *** DAGMAN_FINISHED 0 ***\n"
        "101 INTERNAL *** MONITORD_FINISHED 0 ***\n",
    )

    result = tail.poll(request())

    assert result.workflow_events[0].status == 0
    assert result.source_events[0].marker is TailSourceMarker.MONITORD_FINISHED
    assert all(
        event.base_db_generation == DB_GENERATION
        for event in result.workflow_events + result.source_events
    )


def test_long_silence_is_healthy_and_does_not_create_events(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)

    first = tail.poll(request())
    much_later = tail.poll(request(100000.0, 100000.0))

    assert first.health.state is HealthState.HEALTHY
    assert much_later.health.state is HealthState.HEALTHY
    assert much_later.lines_read == 0
    assert much_later.job_events == ()


def test_non_regular_source_degrades_without_mutation(tmp_path: Path) -> None:
    source = tmp_path / "jobstate.log"
    source.mkdir()
    before = source.stat()
    tail = LiveEventTail(source)

    result = tail.poll(request())
    after = source.stat()

    assert result.health.state is HealthState.UNAVAILABLE
    assert result.health.error_code == "tail_io_error"
    assert (after.st_mode, after.st_mtime_ns, after.st_size) == (
        before.st_mode,
        before.st_mtime_ns,
        before.st_size,
    )


def test_transient_fstat_error_is_health_and_last_success_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    healthy = tail.poll(request())
    real_fstat = live_events.os.fstat
    calls = 0

    def fail_once(fd: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError(5, "injected fstat failure")
        return real_fstat(fd)

    monkeypatch.setattr(live_events.os, "fstat", fail_once)
    failed = tail.poll(request(101.0, 501.0))
    recovered = tail.poll(request(102.0, 502.0))

    assert healthy.health.last_success_epoch == 100.0
    assert failed.health.state is HealthState.UNAVAILABLE
    assert failed.health.last_success_epoch == 100.0
    assert recovered.health.state is HealthState.HEALTHY
    assert recovered.health.error_code is None
    assert recovered.health.last_success_epoch == 102.0


def test_transient_stat_and_read_errors_do_not_escape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    real_stat = live_events.os.stat
    stat_calls = 0

    def fail_stat_once(target):
        nonlocal stat_calls
        stat_calls += 1
        if stat_calls == 1:
            raise OSError(5, "injected stat failure")
        return real_stat(target)

    monkeypatch.setattr(live_events.os, "stat", fail_stat_once)
    stat_failed = tail.poll(request())
    assert stat_failed.health.state is HealthState.UNAVAILABLE

    monkeypatch.setattr(live_events.os, "stat", real_stat)
    append(path, job_line(101))
    real_read = live_events.os.read
    read_calls = 0

    def fail_read_once(fd: int, count: int):
        nonlocal read_calls
        read_calls += 1
        if read_calls == 1:
            raise OSError(5, "injected read failure")
        return real_read(fd, count)

    monkeypatch.setattr(live_events.os, "read", fail_read_once)
    read_failed = tail.poll(request(101.0, 501.0))
    recovered = tail.poll(request(102.0, 502.0))

    assert read_failed.health.state is HealthState.UNAVAILABLE
    assert [event.state for event in recovered.job_events] == ["EXECUTE"]
    assert recovered.health.state is HealthState.HEALTHY


def test_transient_open_and_attachment_pread_errors_recover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.write_text(job_line(90))
    real_open = live_events.os.open

    def fail_open(*_args, **_kwargs):
        raise OSError(5, "injected open failure")

    monkeypatch.setattr(live_events.os, "open", fail_open)
    tail = LiveEventTail(path)
    unavailable = tail.poll(request())
    assert unavailable.health.state is HealthState.UNAVAILABLE

    monkeypatch.setattr(live_events.os, "open", real_open)
    real_pread = live_events.os.pread
    pread_calls = 0

    def fail_pread_once(fd: int, count: int, offset: int):
        nonlocal pread_calls
        pread_calls += 1
        if pread_calls == 1:
            raise OSError(5, "injected attachment pread failure")
        return real_pread(fd, count, offset)

    monkeypatch.setattr(live_events.os, "pread", fail_pread_once)
    still_unavailable = tail.poll(request(101.0, 501.0))
    recovered = tail.poll(request(102.0, 502.0))
    healthy = tail.poll(request(103.0, 503.0))

    assert still_unavailable.health.state is HealthState.UNAVAILABLE
    assert recovered.health.state is HealthState.REATTACHING
    assert recovered.health.error_code == "tail_reattaching"
    assert healthy.health.state is HealthState.HEALTHY


def test_anchor_probe_error_recovers_through_visible_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.write_text(job_line(90))
    tail = LiveEventTail(path)
    initial = tail.poll(request())
    real_pread = live_events.os.pread
    calls = 0

    def fail_once(fd: int, count: int, offset: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError(5, "injected anchor probe failure")
        return real_pread(fd, count, offset)

    monkeypatch.setattr(live_events.os, "pread", fail_once)
    unavailable = tail.poll(request(101.0, 501.0))
    recovered_gap = tail.poll(request(102.0, 502.0))
    healthy = tail.poll(request(103.0, 503.0))

    assert unavailable.health.state is HealthState.UNAVAILABLE
    assert unavailable.health.last_success_epoch == initial.health.last_success_epoch
    assert [gap.reason for gap in recovered_gap.gaps] == ["source_anchor_probe_gap"]
    assert recovered_gap.health.state is HealthState.GAP
    assert recovered_gap.health.last_success_epoch == initial.health.last_success_epoch
    assert healthy.health.state is HealthState.HEALTHY
    assert healthy.health.last_success_epoch == 103.0


def test_short_anchor_probe_forces_visible_continuity_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.write_text(job_line(90))
    tail = LiveEventTail(path)
    old_generation = tail.generation
    real_pread = live_events.os.pread
    calls = 0

    def short_once(fd: int, count: int, offset: int):
        nonlocal calls
        calls += 1
        if calls == 1 and count:
            return b""
        return real_pread(fd, count, offset)

    monkeypatch.setattr(live_events.os, "pread", short_once)
    result = tail.poll(request())

    assert [gap.reason for gap in result.gaps] == ["source_anchor_probe_gap"]
    assert result.health.state is HealthState.GAP
    assert tail.generation is not None and tail.generation != old_generation


def test_overflow_and_drain_fstat_errors_are_reported_and_recover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path, max_line_bytes=64, max_buffer_bytes=128)
    append(path, job_line(100) * 3)
    real_fstat = live_events.os.fstat
    calls = 0

    def fail_second(fd: int):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError(5, "injected overflow fstat failure")
        return real_fstat(fd)

    monkeypatch.setattr(live_events.os, "fstat", fail_second)
    unavailable = tail.poll(request())
    overflow = tail.poll(request(101.0, 501.0))

    assert unavailable.health.state is HealthState.UNAVAILABLE
    assert len(overflow.job_events) == 3
    assert overflow.health.state is HealthState.HEALTHY

    monkeypatch.setattr(live_events.os, "fstat", real_fstat)
    rotated = tmp_path / "jobstate.log.000"
    path.rename(rotated)
    path.touch()
    detected = tail.poll(request(102.0, 502.0))
    drain_calls = 0

    def fail_drain_once(fd: int):
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            raise OSError(5, "injected drain fstat failure")
        return real_fstat(fd)

    monkeypatch.setattr(live_events.os, "fstat", fail_drain_once)
    drain_failed = tail.poll(request(103.0, 503.0))
    drain_recovered = tail.poll(request(104.0, 504.0))

    assert detected.health.state is HealthState.REATTACHING
    assert drain_failed.health.state is HealthState.UNAVAILABLE
    assert drain_recovered.health.state is HealthState.REATTACHING


def test_tail_never_rebinds_to_another_workflow(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    tail.poll(request())
    other = TailPollRequest(
        WorkflowIdentity("wf-other", "wf-root"),
        DB_GENERATION,
        ClockSample(101.0, 501.0),
        100,
        10,
    )

    with pytest.raises(ValueError, match="cannot be rebound"):
        tail.poll(other)


def test_close_is_idempotent_and_poll_reports_unavailable(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.touch()
    tail = LiveEventTail(path)
    tail.close()
    tail.close()

    result = tail.poll(request())

    assert result.health.state is HealthState.UNAVAILABLE
    assert result.health.error_code == "tail_closed"


def test_read_only_tail_does_not_change_source_metadata(tmp_path: Path) -> None:
    path = tmp_path / "jobstate.log"
    path.write_text(job_line(90))
    os.chmod(path, 0o444)
    before = path.stat()
    tail = LiveEventTail(path)
    result = tail.poll(request())
    after = path.stat()

    assert result.health.state is HealthState.HEALTHY
    assert (after.st_mode, after.st_mtime_ns, after.st_size) == (
        before.st_mode,
        before.st_mtime_ns,
        before.st_size,
    )
