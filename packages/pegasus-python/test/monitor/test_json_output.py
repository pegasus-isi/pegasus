"""Unit tests for the ``pegasus-monitor --json`` document writer."""

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
from types import SimpleNamespace

import pytest

from Pegasus.monitor import json_output
from Pegasus.monitor.event_log import EventLogFormatError
from Pegasus.monitor.json_output import JsonEmitter, envelope
from Pegasus.monitor.locator import WorkflowLocationError
from Pegasus.monitor.models import JSONL_V1_SCHEMA_VERSION


def _emitter() -> tuple[JsonEmitter, io.StringIO]:
    stream = io.StringIO()
    base = envelope(
        monitor_version="6.0.0", modes=("once",), origin="live", target="run0001"
    )
    return JsonEmitter(stream, base), stream


def test_envelope_is_versioned_separately_from_the_jsonl_log() -> None:
    document = envelope(monitor_version="6.0.0", modes=(), origin="live", target=None)

    assert document["format"] == "pegasus-monitor-json"
    assert document["format_version"] == json_output.JSON_OUTPUT_VERSION
    assert "schema_version" not in document
    assert JSONL_V1_SCHEMA_VERSION == 1  # the log's own, unrelated version


def test_emitter_writes_one_compact_ascii_line_once() -> None:
    emitter, stream = _emitter()
    emitter.document = {**emitter.base, "summary": {"label": "caf\u00e9 \x1b[31m"}}

    assert emitter.emit(0, now_epoch=1700000000.5) == 0
    assert emitter.emit(1) == 0

    text = stream.getvalue()
    assert text.count("\n") == 1
    assert text.isascii() and "\x1b" not in text
    document = json.loads(text)
    assert document["generated_at_epoch"] == 1700000000.5
    assert document["generated_at"] == "2023-11-14T22:13:20.500Z"
    assert (document["exit_code"], document["error"]) == (0, None)
    assert document["summary"]["label"] == "caf\u00e9 \x1b[31m"


@pytest.mark.parametrize(
    ("failure", "exit_code", "expected_code", "expected_exit"),
    [
        (None, 1, "monitor_error", 1),
        (("io_error", "disk"), 0, "io_error", 1),
        (("usage_error", "bad flag"), 2, "usage_error", 2),
    ],
)
def test_error_is_present_exactly_when_the_exit_code_is_nonzero(
    failure, exit_code, expected_code, expected_exit
) -> None:
    emitter, stream = _emitter()
    emitter.document = dict(emitter.base)
    if failure is not None:
        emitter.fail(*failure)

    assert emitter.emit(exit_code) == expected_exit

    document = json.loads(stream.getvalue())
    assert document["exit_code"] == expected_exit
    assert document["error"]["code"] == expected_code


def test_no_result_and_no_failure_is_reported_as_interrupted() -> None:
    emitter, stream = _emitter()

    assert emitter.emit(0) == 1

    document = json.loads(stream.getvalue())
    assert document["error"]["code"] == "interrupted"
    assert document["modes"] == ["once"]
    assert document["workflow"] is None


def test_first_failure_names_the_cause() -> None:
    emitter, stream = _emitter()
    emitter.fail("workflow_location_error", "missing")
    emitter.fail("cleanup_failed", "later")
    emitter.emit(1)

    assert json.loads(stream.getvalue())["error"] == {
        "code": "workflow_location_error",
        "message": "missing",
    }


def test_non_finite_values_cannot_produce_invalid_json() -> None:
    assert json_output._finite({"a": float("nan"), "b": [float("inf"), 1.5]}) == {
        "a": None,
        "b": [None, 1.5],
    }
    emitter, _stream = _emitter()
    emitter.document = {**emitter.base, "stats": {"wall_time": float("nan")}}
    with pytest.raises(ValueError):
        emitter.emit(0)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (WorkflowLocationError("no run"), "workflow_location_error"),
        (json_output.NoCompleteCheckpoint("incomplete"), "no_complete_checkpoint"),
        (EventLogFormatError("bad header"), "invalid_event_log"),
        (PermissionError("denied"), "io_error"),
        (RuntimeError("other"), "monitor_error"),
    ],
)
def test_error_codes_are_stable_per_failure_class(error, code) -> None:
    assert json_output.error_code(error) == code


def test_requested_modes_keep_a_fixed_order() -> None:
    args = SimpleNamespace(why_idle=True, once=True, diagnose=False)

    assert json_output.requested_modes(args) == ("once", "why_idle")


def test_long_lists_are_bounded_with_an_omitted_count() -> None:
    analysis = SimpleNamespace(
        to_json_dict=lambda: {
            "idle_jobs": [{"exec_job_id": str(index)} for index in range(1005)],
            "resource_fit": {"mismatches": []},
            "findings": ["token=abc123 is waiting"],
        }
    )

    document = json_output._why_idle(analysis)

    assert len(document["idle_jobs"]) == json_output.MAX_IDLE_JOBS
    assert document["idle_jobs_omitted"] == 5
    assert document["findings"] == ["token=<redacted> is waiting"]
