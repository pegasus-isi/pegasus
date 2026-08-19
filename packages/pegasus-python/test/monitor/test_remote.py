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

import json
import os
import sys
import time
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from Pegasus.monitor.event_log import EventRecord, decode_json_line
from Pegasus.monitor.models import (
    CheckpointRecord,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    GapReason,
    GapRecord,
    JobTransitionRecord,
    SnapshotEpoch,
    StreamHeader,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionWatermark,
)
from Pegasus.monitor.remote import (
    CommandResult,
    InvalidRemoteLocation,
    RemoteCommandError,
    RemoteCursor,
    RemoteJSONLReader,
    RemoteLocation,
    RemoteProtocolError,
    run_bounded_command,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any


WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")


def _snapshot() -> DatabaseSnapshot:
    transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(1, "WORKFLOW_STARTED", Decimal("1")),
        0,
        0,
    )
    workflow = WorkflowSnapshot(
        WORKFLOW,
        1,
        "WORKFLOW_STARTED",
        0,
        0,
        Decimal("1"),
        None,
        transition,
    )
    return DatabaseSnapshot(
        SnapshotEpoch(1),
        DatabaseGeneration(1, 10, 20),
        1.0,
        workflow,
        (),
        (),
        (transition,),
        (),
        WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 1, 0), (transition.identity,)
        ),
    )


def _line(record_type: str, sequence: int, stream_id: str, **values: Any) -> bytes:
    if record_type == "header":
        record: EventRecord = StreamHeader(sequence, stream_id, WORKFLOW, 1.0, "6.0")
    elif record_type == "checkpoint":
        record = CheckpointRecord(sequence, stream_id, 1.0, _snapshot(), "test")
    elif record_type == "job_transition":
        transition = DBJobTransition(
            WORKFLOW,
            "compute_ID0001",
            1,
            DBTransitionIdentity(10, "EXECUTE", Decimal("2"), sequence),
        )
        record = JobTransitionRecord(
            sequence, stream_id, SnapshotEpoch(1), 2.0, transition
        )
    elif record_type == "gap":
        record = GapRecord(
            sequence,
            stream_id,
            2.0,
            GapReason.DISK_GUARD,
            max(1, sequence - 1),
            max(1, sequence - 1),
        )
    else:  # pragma: no cover - the test corpus is deliberately closed
        raise AssertionError(f"unsupported test record: {record_type}")
    payload = record.to_json_dict()
    payload.update(values)
    return json.dumps(payload, separators=(",", ":")).encode() + b"\n"


def _stream(stream_id: str, *records: bytes) -> bytes:
    return _line("header", 0, stream_id) + b"".join(records)


class FakeSSH:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[tuple[str, ...], int, int, float]] = []
        self.failures: list[CommandResult] = []

    def __call__(
        self,
        argv: Sequence[str],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout: float,
    ) -> CommandResult:
        command = tuple(argv)
        self.calls.append((command, max_stdout_bytes, max_stderr_bytes, timeout))
        if self.failures:
            return self.failures.pop(0)
        skip = int(
            next(value for value in command if value.startswith("skip=")).split("=", 1)[
                1
            ]
        )
        count = int(
            next(value for value in command if value.startswith("count=")).split(
                "=", 1
            )[1]
        )
        return CommandResult(0, self.content[skip : skip + count], b"")


class ReplacingSSH(FakeSSH):
    """Return the old header once, then ranges from a replacement stream."""

    def __init__(self, before: bytes, after: bytes) -> None:
        super().__init__(before)
        self.after = after

    def __call__(
        self,
        argv: Sequence[str],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout: float,
    ) -> CommandResult:
        if self.calls:
            self.content = self.after
        return super().__call__(
            argv,
            max_stdout_bytes=max_stdout_bytes,
            max_stderr_bytes=max_stderr_bytes,
            timeout=timeout,
        )


@pytest.mark.parametrize(
    ("spec", "target", "path"),
    [
        ("submit.example:/tmp/monitor.jsonl", "submit.example", "/tmp/monitor.jsonl"),
        ("alice@submit-1:run/events.jsonl", "alice@submit-1", "run/events.jsonl"),
        ("[2001:db8::1]:/tmp/events", "[2001:db8::1]", "/tmp/events"),
        ("alice@[2001:db8::7]:/tmp/events", "alice@[2001:db8::7]", "/tmp/events"),
    ],
)
def test_remote_location_accepts_supported_targets(
    spec: str, target: str, path: str
) -> None:
    assert RemoteLocation.parse(spec) == RemoteLocation(target, path)


@pytest.mark.parametrize(
    "spec",
    [
        "",
        "host-only",
        ":/tmp/events",
        "-oProxyCommand=evil:/tmp/events",
        "user@@host:/tmp/events",
        "user name@host:/tmp/events",
        "host name:/tmp/events",
        "2001:db8::1:/tmp/events",
        "[not-ipv6]:/tmp/events",
        "[2001:db8::1]/tmp/events",
        "host:",
        "host:-oProxyCommand=evil",
        "host:/tmp/events\ncommand",
        "host:/tmp/\x00events",
    ],
)
def test_remote_location_rejects_injection_and_malformed_values(spec: str) -> None:
    with pytest.raises(InvalidRemoteLocation):
        RemoteLocation.parse(spec)


def test_ssh_argv_is_auditable_bounded_and_quotes_only_remote_path() -> None:
    fake = FakeSSH(_stream("stream-a"))
    reader = RemoteJSONLReader(
        "alice@host:/tmp/a b;$(touch nope).jsonl",
        ssh_config="/tmp/ssh config",
        ssh_identity="/tmp/id key",
        command_runner=fake,
    )

    argv = reader.build_ssh_argv(17, 41)

    assert argv[:2] == ("ssh", "-T")
    assert ("-F", "/tmp/ssh config") == argv[argv.index("-F") : argv.index("-F") + 2]
    assert ("-i", "/tmp/id key") == argv[argv.index("-i") : argv.index("-i") + 2]
    assert argv[argv.index("--") :] == (
        "--",
        "alice@host",
        "dd",
        "if='/tmp/a b;$(touch nope).jsonl'",
        "bs=1",
        "skip=17",
        "count=41",
    )


@pytest.mark.parametrize("keyword", ["ssh_config", "ssh_identity"])
def test_ssh_option_paths_reject_option_injection(keyword: str) -> None:
    with pytest.raises(ValueError, match="unsafe"):
        RemoteJSONLReader("host:/tmp/events", **{keyword: "-oProxyCommand=evil"})


def test_incremental_read_retries_torn_line_from_last_complete_offset() -> None:
    checkpoint = _line("checkpoint", 1, "stream-a")
    transition = _line("job_transition", 2, "stream-a")
    complete = _stream("stream-a", checkpoint)
    fake = FakeSSH(complete + transition[:-3])
    reader = RemoteJSONLReader(
        "host:/tmp/events", chunk_bytes=4096, command_runner=fake
    )

    first = reader.read()

    assert [record.to_json_dict()["record_type"] for record in first.records] == [
        "header",
        "checkpoint",
    ]
    assert first.cursor.offset == len(complete)
    assert first.cursor.awaiting_checkpoint is False
    assert first.bytes_read > first.bytes_consumed
    assert first.at_eof is True

    fake.content += transition[-3:]
    second = reader.read(first.cursor)

    assert [record.sequence for record in second.records] == [2]
    assert second.cursor.offset == len(complete + transition)
    data_call = fake.calls[-1][0]
    assert f"skip={len(complete)}" in data_call


def test_torn_header_is_tolerated_without_advancing_cursor() -> None:
    fake = FakeSSH(_line("header", 0, "stream-a")[:-1])
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    cursor = RemoteCursor(offset=22, stream_id="old")

    result = reader.read(cursor)

    assert result.records == ()
    assert result.cursor is cursor
    assert result.bytes_consumed == 0


def test_checkpoint_larger_than_transport_chunk_is_read_progressively() -> None:
    content = _stream(
        "stream-a",
        _line("checkpoint", 1, "stream-a", padding="x" * 4096),
    )
    fake = FakeSSH(content)
    reader = RemoteJSONLReader(
        "host:/tmp/events",
        chunk_bytes=128,
        max_record_bytes=8192,
        command_runner=fake,
    )

    first = reader.read()
    assert [record.sequence for record in first.records] == [0]

    second = reader.read(first.cursor)
    assert [record.sequence for record in second.records] == [1]
    assert len(fake.calls) > 4


def test_record_larger_than_configured_limit_is_rejected() -> None:
    content = _stream(
        "stream-a",
        _line("checkpoint", 1, "stream-a", padding="x" * 4096),
    )
    fake = FakeSSH(content)
    reader = RemoteJSONLReader(
        "host:/tmp/events",
        chunk_bytes=128,
        max_record_bytes=1024,
        command_runner=fake,
    )
    cursor = reader.read().cursor

    with pytest.raises(RemoteProtocolError, match="record byte limit"):
        reader.read(cursor)


def test_reconnect_after_ssh_failure_reuses_unchanged_cursor() -> None:
    content = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    cursor = reader.read().cursor
    fake.content += _line("job_transition", 2, "stream-a")
    fake.failures.extend(
        [
            CommandResult(0, fake.content, b""),
            CommandResult(255, b"", b"connection reset"),
        ]
    )

    with pytest.raises(RemoteCommandError, match="connection reset"):
        reader.read(cursor)

    assert cursor.offset == len(content)
    assert cursor.stream_id == "stream-a"
    result = reader.read(cursor)
    assert [record.sequence for record in result.records] == [2]


def test_replacement_is_detected_by_header_stream_id_and_resets_offset() -> None:
    stream_a = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(stream_a)
    reader = RemoteJSONLReader(
        "host:/tmp/events", chunk_bytes=4096, command_runner=fake
    )
    first = reader.read()
    old_offset = first.cursor.offset

    stream_b = _stream(
        "stream-b",
        _line("checkpoint", 1, "stream-b"),
        _line("job_transition", 2, "stream-b", padding="x" * len(stream_a)),
    )
    assert len(stream_b) > old_offset
    fake.content = stream_b
    replaced = reader.read(first.cursor)

    assert replaced.stream_replaced is True
    assert replaced.cursor.stream_id == "stream-b"
    assert replaced.cursor.offset == len(stream_b)
    assert replaced.cursor.last_sequence == 2
    assert isinstance(replaced.records[0], StreamHeader)
    assert "skip=0" in fake.calls[-1][0]


def test_replacement_between_header_probe_and_data_read_is_retried_once() -> None:
    stream_a = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    initial = FakeSSH(stream_a)
    reader = RemoteJSONLReader(
        "host:/tmp/events", chunk_bytes=4096, command_runner=initial
    )
    cursor = reader.read().cursor

    stream_b = _stream(
        "stream-b",
        _line("checkpoint", 1, "stream-b"),
        _line("job_transition", 2, "stream-b"),
    )
    assert len(_stream("stream-b", _line("checkpoint", 1, "stream-b"))) == len(stream_a)
    replacing = ReplacingSSH(stream_a, stream_b)
    reader.command_runner = replacing

    result = reader.read(cursor)

    assert result.stream_replaced is True
    assert result.cursor.stream_id == "stream-b"
    assert result.cursor.last_sequence == 2
    assert [record.sequence for record in result.records] == [0, 1, 2]
    assert len(replacing.calls) == 4


def test_large_torn_replacement_checkpoint_restarts_from_header() -> None:
    stream_a = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(stream_a)
    reader = RemoteJSONLReader(
        "host:/tmp/events",
        chunk_bytes=128,
        max_record_bytes=8192,
        command_runner=fake,
    )
    cursor = reader.read().cursor
    cursor = reader.read(cursor).cursor
    assert cursor.last_sequence == 1

    checkpoint = _line("checkpoint", 1, "stream-b", padding="x" * 4096)
    complete_replacement = _stream("stream-b", checkpoint)
    fake.content = complete_replacement[:-7]

    replacement = reader.read(cursor)
    assert replacement.stream_replaced is True
    assert [record.sequence for record in replacement.records] == [0]
    assert replacement.cursor.last_sequence == 0
    checkpoint_offset = replacement.cursor.offset

    torn = reader.read(replacement.cursor)
    assert torn.records == ()
    assert torn.cursor.offset == checkpoint_offset
    assert torn.bytes_read > 128

    fake.content = complete_replacement
    recovered = reader.read(torn.cursor)
    assert [record.sequence for record in recovered.records] == [1]
    assert recovered.cursor.last_sequence == 1
    assert recovered.cursor.awaiting_checkpoint is False


def test_same_stream_id_does_not_reset_when_file_size_changes() -> None:
    content = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    first = reader.read()
    fake.content += _line("job_transition", 2, "stream-a")

    second = reader.read(first.cursor)

    assert second.stream_replaced is False
    assert [record.sequence for record in second.records] == [2]


def test_gap_requires_checkpoint_until_a_checkpoint_record_arrives() -> None:
    content = _stream(
        "stream-a",
        _line("checkpoint", 1, "stream-a"),
        _line("gap", 2, "stream-a"),
    )
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    first = reader.read()
    assert first.cursor.awaiting_checkpoint is True

    fake.content += _line("job_transition", 3, "stream-a")
    second = reader.read(first.cursor)
    assert second.cursor.awaiting_checkpoint is True

    fake.content += _line("checkpoint", 4, "stream-a")
    third = reader.read(second.cursor)
    assert third.cursor.awaiting_checkpoint is False


def test_unreported_sequence_hole_requires_checkpoint() -> None:
    content = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    first = reader.read()
    assert first.cursor.last_sequence == 1

    fake.content += _line("job_transition", 3, "stream-a")
    missing = reader.read(first.cursor)
    assert missing.cursor.last_sequence == 3
    assert missing.cursor.awaiting_checkpoint is True

    fake.content += _line("checkpoint", 4, "stream-a")
    recovered = reader.read(missing.cursor)
    assert recovered.cursor.last_sequence == 4
    assert recovered.cursor.awaiting_checkpoint is False


def test_explicit_gap_range_advances_sequence_and_requires_checkpoint() -> None:
    content = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", command_runner=fake)
    first = reader.read()

    fake.content += _line(
        "gap",
        4,
        "stream-a",
        first_missing_sequence=2,
        last_missing_sequence=3,
    )
    gap = reader.read(first.cursor)
    assert gap.cursor.last_sequence == 4
    assert gap.cursor.awaiting_checkpoint is True

    fake.content += _line("checkpoint", 5, "stream-a")
    recovered = reader.read(gap.cursor)
    assert recovered.cursor.last_sequence == 5
    assert recovered.cursor.awaiting_checkpoint is False


def test_complete_invalid_json_fails_but_output_is_bounded() -> None:
    content = _stream("stream-a") + b"not-json\n"
    fake = FakeSSH(content)
    reader = RemoteJSONLReader("host:/tmp/events", chunk_bytes=512, command_runner=fake)

    with pytest.raises(RemoteProtocolError, match="invalid complete JSONL"):
        reader.read()

    assert all(
        call[1] in {reader.header_bytes, reader.chunk_bytes} for call in fake.calls
    )


def test_runner_output_larger_than_requested_is_rejected() -> None:
    content = _stream("stream-a")

    def overflowing_runner(
        argv: Sequence[str],
        *,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        timeout: float,
    ) -> CommandResult:
        del argv, max_stderr_bytes, timeout
        return CommandResult(0, content + b"x" * max_stdout_bytes, b"")

    reader = RemoteJSONLReader("host:/tmp/events", command_runner=overflowing_runner)
    with pytest.raises(RemoteCommandError, match="exceeded"):
        reader.read()


def test_custom_line_decoder_is_used_for_header_and_records() -> None:
    content = _stream("stream-a", _line("checkpoint", 1, "stream-a"))
    fake = FakeSSH(content)
    decoded: list[bytes] = []

    def decode_line(line: bytes) -> EventRecord:
        decoded.append(line)
        return decode_json_line(line)

    reader = RemoteJSONLReader(
        "host:/tmp/events", command_runner=fake, decode_line=decode_line
    )
    result = reader.read()

    assert len(result.records) == 2
    assert len(decoded) == 3  # header probe, then header and checkpoint chunk records


@pytest.mark.skipif(not hasattr(os, "killpg"), reason="requires POSIX process groups")
def test_bounded_runner_terminates_descendant_process_group_on_timeout(
    tmp_path,
) -> None:
    process_group_file = tmp_path / "process-group"
    term_marker = tmp_path / "term-received"
    script = """
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

marker = Path(sys.argv[2])

def handle_term(*_args):
    marker.write_text("term")

signal.signal(signal.SIGTERM, handle_term)
child = subprocess.Popen(["sleep", "30"])
Path(sys.argv[1]).write_text(f"{os.getpgrp()} {child.pid}")
while True:
    time.sleep(1)
"""

    with pytest.raises(RemoteCommandError, match="timed out"):
        run_bounded_command(
            (sys.executable, "-c", script, str(process_group_file), str(term_marker)),
            max_stdout_bytes=128,
            max_stderr_bytes=128,
            timeout=0.25,
        )

    process_group, _child = map(int, process_group_file.read_text().split())
    assert term_marker.read_text() == "term"
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            break
        time.sleep(0.02)
    else:
        pytest.fail("timed-out SSH descendant process group survived cleanup")
