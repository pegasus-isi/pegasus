"""Shared schema-v1 JSONL golden corpus tests."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from Pegasus.monitor.event_log import decode_json_line, encode_record
from Pegasus.monitor.models import GapRecord, JobTransitionRecord, Provenance
from Pegasus.monitor.remote import CommandResult, RemoteJSONLReader
from Pegasus.monitor.replay import replay_records

if TYPE_CHECKING:
    from collections.abc import Sequence


FIXTURE = Path(__file__).parent / "fixtures" / "event_log" / "schema-v1-golden.jsonl"


class BoundedFakeRunner:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[tuple[str, ...], int, int, float]] = []

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
        skip = int(
            next(item for item in command if item.startswith("skip=")).split("=", 1)[1]
        )
        count = int(
            next(item for item in command if item.startswith("count=")).split("=", 1)[1]
        )
        return CommandResult(0, self.content[skip : skip + count], b"")


def test_every_complete_fixture_line_is_canonical() -> None:
    for line in FIXTURE.read_bytes().splitlines(keepends=True):
        record = decode_json_line(line)
        assert encode_record(record) == line


def test_replay_reaches_complete_final_snapshot_and_recovers_gap() -> None:
    records = tuple(
        decode_json_line(line)
        for line in FIXTURE.read_bytes().splitlines(keepends=True)
    )
    result = replay_records(records)

    assert result.complete
    assert result.awaiting_checkpoint is False
    assert result.ignored_records == 1
    assert result.snapshot is not None
    assert result.snapshot.epoch.value == 4
    assert result.snapshot.jobs[0].state == "JOB_SUCCESS"
    assert result.snapshot.jobs[0].provenance is Provenance.DB_CONFIRMED
    assert any(isinstance(record, GapRecord) for record in records)
    assert any(
        isinstance(record, JobTransitionRecord)
        and record.transition.identity.state == "JOB_HELD"
        for record in records
    )


def test_remote_reader_consumes_same_fixture_with_bounded_runner() -> None:
    content = FIXTURE.read_bytes()
    fake = BoundedFakeRunner(content)
    reader = RemoteJSONLReader("host:/tmp/schema-v1-golden.jsonl", command_runner=fake)

    result = reader.read()

    assert result.records
    assert result.records[-1].to_json_dict()["record_type"] == "checkpoint"
    assert result.cursor.stream_id == "golden-stream"
    assert result.cursor.offset == len(content)
    assert result.cursor.awaiting_checkpoint is False
    assert result.bytes_consumed == len(content)
    assert len(fake.calls) == 2
