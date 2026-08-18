"""Contract and safety tests for the native read-only HTCondor observer."""

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
import shutil
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from Pegasus.monitor.condor import (
    CondorObserver,
    CondorObserverConfig,
    build_workflow_constraint,
)
from Pegasus.monitor.models import (
    ClockSample,
    HealthState,
    SchedulerProvider,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SourceName,
    WorkflowIdentity,
)

FIXTURES = Path(__file__).parent / "fixtures" / "condor"
WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")


@pytest.fixture
def fake_bin(tmp_path: Path) -> Path:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    source = FIXTURES / "fake_condor.py"
    for name in ("condor_q", "condor_history", "condor_status", "condor_userprio"):
        target = binary_dir / name
        shutil.copyfile(source, target)
        target.chmod(0o755)
    return binary_dir


@pytest.fixture
def mode_file(tmp_path: Path) -> Path:
    path = tmp_path / "mode"
    path.write_text("success", encoding="utf-8")
    return path


def request(
    kind: SchedulerQueryKind,
    *,
    workflow: WorkflowIdentity = WORKFLOW,
    epoch: float = 100.0,
    monotonic: float = 500.0,
    timeout: float = 5.0,
    limit: int = 20,
) -> SchedulerQueryRequest:
    return SchedulerQueryRequest(
        workflow=workflow,
        kind=kind,
        clock=ClockSample(epoch=epoch, monotonic=monotonic),
        timeout_seconds=timeout,
        result_limit=limit,
    )


def observer(
    fake_bin: Path,
    *,
    payload: Path | None = None,
    mode: str = "success",
    mode_file: Path | None = None,
    log: Path | None = None,
    config: CondorObserverConfig | None = None,
    **kwargs: object,
) -> CondorObserver:
    environment = {
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "FAKE_CONDOR_MODE": mode,
    }
    if payload is not None:
        environment["FAKE_CONDOR_PAYLOAD_FILE"] = str(payload)
    if mode_file is not None:
        environment["FAKE_CONDOR_MODE_FILE"] = str(mode_file)
    if log is not None:
        environment["FAKE_CONDOR_LOG"] = str(log)
    base = config or CondorObserverConfig()
    configured = replace(base, environment={**base.environment, **environment})
    return CondorObserver(configured, jitter=lambda _delay: 0.0, **kwargs)


def payload_dict(result: object) -> dict[str, object]:
    return result.to_json_dict()  # type: ignore[no-any-return, union-attr]


def read_log(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_provider_implements_frozen_scheduler_protocol(fake_bin: Path) -> None:
    provider = observer(fake_bin, payload=FIXTURES / "queue.json")
    assert isinstance(provider, SchedulerProvider)


def test_constraint_is_exact_and_classad_quoted() -> None:
    workflow = WorkflowIdentity('wf" || Owner =!= "safe', "root\\value")

    assert build_workflow_constraint(workflow) == (
        'pegasus_wf_uuid =?= "wf\\" || Owner =!= \\"safe"'
    )
    assert build_workflow_constraint(workflow, tree_scope=True) == (
        'pegasus_root_wf_uuid =?= "root\\\\value"'
    )


@pytest.mark.parametrize(
    ("kind", "payload_name", "executable", "required_flags"),
    [
        (
            SchedulerQueryKind.QUEUE,
            "queue.json",
            "condor_q",
            ("-json", "-attributes", "-constraint", "-limit", "-pool"),
        ),
        (
            SchedulerQueryKind.HISTORY,
            "history.json",
            "condor_history",
            ("-json", "-attributes", "-constraint", "-match", "-pool"),
        ),
        (
            SchedulerQueryKind.POOL,
            "pool.json",
            "condor_status",
            ("-json", "-attributes", "-pool"),
        ),
        (
            SchedulerQueryKind.PRIORITY,
            "priority.txt",
            "condor_userprio",
            ("-long", "-pool"),
        ),
        (
            SchedulerQueryKind.NEGOTIATOR,
            "negotiator.json",
            "condor_status",
            ("-negotiator", "-json", "-attributes", "-pool"),
        ),
    ],
)
def test_every_query_kind_uses_only_reviewed_read_arguments(
    fake_bin: Path,
    tmp_path: Path,
    kind: SchedulerQueryKind,
    payload_name: str,
    executable: str,
    required_flags: tuple[str, ...],
) -> None:
    log = tmp_path / "commands.jsonl"
    config = CondorObserverConfig(
        schedd="schedd.example.org",
        collector="collector.example.org:9618",
    )
    provider = observer(
        fake_bin,
        payload=FIXTURES / payload_name,
        log=log,
        config=config,
    )

    result = provider.query(request(kind, limit=7))

    assert result.health.state is HealthState.HEALTHY
    command = read_log(log)[0]
    assert command["executable"] == executable
    argv = command["argv"]
    assert isinstance(argv, list)
    assert all(flag in argv for flag in required_flags)
    joined = " ".join(str(value) for value in argv)
    assert not any(
        mutation in joined
        for mutation in ("condor_rm", "condor_hold", "condor_release", "-set")
    )
    if kind in {SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY}:
        constraint = argv[argv.index("-constraint") + 1]
        assert constraint == 'pegasus_wf_uuid =?= "wf-selected"'
        assert "pegasus_root_wf_uuid =?=" not in constraint
    else:
        assert "-constraint" not in argv


def test_tree_scope_must_be_explicit(fake_bin: Path, tmp_path: Path) -> None:
    log = tmp_path / "commands.jsonl"
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        log=log,
        config=CondorObserverConfig(tree_scope=True),
    )

    provider.query(request(SchedulerQueryKind.QUEUE))

    argv = read_log(log)[0]["argv"]
    assert isinstance(argv, list)
    constraint = argv[argv.index("-constraint") + 1]
    assert constraint == 'pegasus_root_wf_uuid =?= "wf-root"'


@pytest.mark.parametrize("scope_value", [None, "wf-foreign"])
def test_queue_rejects_missing_or_foreign_workflow_scope(
    fake_bin: Path, tmp_path: Path, scope_value: str | None
) -> None:
    payload = tmp_path / "foreign.json"
    row: dict[str, object] = {"ClusterId": 77, "ProcId": 0, "JobStatus": 1}
    if scope_value is not None:
        row["pegasus_wf_uuid"] = scope_value
    payload.write_text(json.dumps([row]), encoding="utf-8")
    provider = observer(fake_bin, payload=payload)

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.error_code == "scope_mismatch"
    assert result.evidence == ()


def test_history_scope_validation_is_case_insensitive(
    fake_bin: Path, tmp_path: Path
) -> None:
    payload = tmp_path / "history.json"
    payload.write_text(
        json.dumps(
            [
                {
                    "ClusterId": 78,
                    "ProcId": 0,
                    "PEGASUS_WF_UUID": "wf-selected",
                }
            ]
        ),
        encoding="utf-8",
    )
    provider = observer(fake_bin, payload=payload)

    result = provider.query(request(SchedulerQueryKind.HISTORY))

    assert result.health.state is HealthState.HEALTHY
    assert len(result.evidence) == 1


def test_tree_scope_validates_root_uuid_not_selected_uuid(
    fake_bin: Path, tmp_path: Path
) -> None:
    payload = tmp_path / "tree.json"
    payload.write_text(
        json.dumps(
            [
                {
                    "ClusterId": 79,
                    "ProcId": 0,
                    "pegasus_wf_uuid": "wf-child",
                    "pegasus_root_wf_uuid": "wf-root",
                }
            ]
        ),
        encoding="utf-8",
    )
    provider = observer(
        fake_bin,
        payload=payload,
        config=CondorObserverConfig(tree_scope=True),
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.state is HealthState.HEALTHY
    assert len(result.evidence) == 1


def test_endpoint_values_reject_option_or_shell_injection() -> None:
    with pytest.raises(ValueError, match="unsafe HTCondor schedd"):
        CondorObserverConfig(schedd="--name evil")
    with pytest.raises(ValueError, match="unsafe HTCondor collector"):
        CondorObserverConfig(collector="pool;condor_rm -all")


def test_queue_normalization_and_summary(fake_bin: Path) -> None:
    provider = observer(fake_bin, payload=FIXTURES / "queue.json")

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.source is SourceName.CONDOR_QUEUE
    assert result.health.checked_at_epoch == 100.0
    assert result.health.last_success_epoch == 100.0
    assert result.backoff_seconds == 0.0
    assert len(result.evidence) == 2
    first = result.evidence[0]
    assert first.kind is SchedulerQueryKind.QUEUE
    assert first.target.to_json_dict() == {
        "ClusterId": 41,
        "DAGNodeName": "preprocess_ID0000001",
        "ProcId": 0,
    }
    assert payload_dict(result.summary) == {
        "requested_cpus": 3,
        "requested_gpus": 1,
        "requested_memory_mb": 12288,
        "status_counts": {
            "completed": 0,
            "held": 1,
            "idle": 1,
            "other": 0,
            "removed": 0,
            "running": 0,
            "suspended": 0,
            "transfer_output": 0,
        },
        "total_jobs": 2,
    }


def test_history_normalization_aggregates_portable_metrics(fake_bin: Path) -> None:
    provider = observer(fake_bin, payload=FIXTURES / "history.json")

    result = provider.query(request(SchedulerQueryKind.HISTORY))

    assert len(result.evidence) == 2
    assert payload_dict(result.summary) == {
        "bytes_received": 600,
        "bytes_sent": 400,
        "remote_sys_cpu_seconds": 2.5,
        "remote_user_cpu_seconds": 25.5,
        "remote_wall_seconds": 30.5,
        "restarts": 2,
        "total_jobs": 2,
    }


def test_pool_normalization_matches_partitionable_slot_behavior(fake_bin: Path) -> None:
    provider = observer(fake_bin, payload=FIXTURES / "pool.json")

    result = provider.query(request(SchedulerQueryKind.POOL))

    assert len(result.evidence) == 3
    assert payload_dict(result.summary) == {
        "claimed_slots": 1,
        "idle_cpus": 6,
        "idle_gpus": 2,
        "idle_memory_mb": 12288,
        "idle_slots": 1,
        "load_avg": 1.5,
        "machines": 2,
        "os_arch": "LINUX/AARCH64, LINUX/X86_64",
        "other_slots": 0,
        "total_cpus": 12,
        "total_disk_kb": 800000,
        "total_gpus": 2,
        "total_memory_mb": 24576,
        "total_slots": 2,
    }


def test_pool_uses_partitionable_flags_and_parent_link_without_double_counting(
    fake_bin: Path, tmp_path: Path
) -> None:
    payload = tmp_path / "flagged-pool.json"
    payload.write_text(
        json.dumps(
            [
                {
                    "Name": "slot1@host-c",
                    "Machine": "host-c",
                    "PartitionableSlot": "true",
                    "DynamicSlot": "false",
                    "Cpus": 4,
                    "Memory": 8000,
                    "TotalSlotCpus": 8,
                    "TotalSlotMemory": 16000,
                },
                {
                    "Name": "slot1_1@host-c",
                    "ParentSlotName": "slot1@host-c",
                    "DynamicSlot": True,
                    "Cpus": 4,
                    "Memory": 8000,
                    "State": "Claimed",
                    "Activity": "Busy",
                },
                {
                    "Name": "slot-static@host-c",
                    "Machine": "host-c",
                    "SlotType": "Static",
                    "Cpus": 2,
                    "Memory": 2000,
                    "State": "Unclaimed",
                    "Activity": "Idle",
                },
            ]
        ),
        encoding="utf-8",
    )
    provider = observer(fake_bin, payload=payload)

    result = provider.query(request(SchedulerQueryKind.POOL))
    summary = payload_dict(result.summary)

    assert summary["machines"] == 1
    assert summary["total_slots"] == 2
    assert summary["claimed_slots"] == 1
    assert summary["idle_slots"] == 1
    assert summary["total_cpus"] == 10
    assert summary["idle_cpus"] == 6
    assert summary["total_memory_mb"] == 18000
    assert summary["idle_memory_mb"] == 10000


def test_priority_and_negotiator_normalization(fake_bin: Path) -> None:
    priority = observer(fake_bin, payload=FIXTURES / "priority.txt").query(
        request(SchedulerQueryKind.PRIORITY)
    )
    negotiator = observer(fake_bin, payload=FIXTURES / "negotiator.json").query(
        request(SchedulerQueryKind.NEGOTIATOR)
    )

    assert len(priority.evidence) == 2
    assert priority.evidence[0].target.to_json_dict() == {"Name": "pegasus@example.org"}
    assert payload_dict(priority.summary) == {
        "best_effective_priority": 8.0,
        "users": 2,
        "worst_effective_priority": 12.5,
    }
    assert len(negotiator.evidence) == 1
    assert payload_dict(negotiator.summary) == {
        "ads": 1,
        "last_cycle_duration_seconds": 4.5,
        "last_cycle_matches": 12,
    }


def test_result_limit_bounds_evidence_even_if_fake_ignores_command_limit(
    fake_bin: Path,
) -> None:
    provider = observer(fake_bin, payload=FIXTURES / "queue.json")
    result = provider.query(request(SchedulerQueryKind.QUEUE, limit=1))

    assert len(result.evidence) == 1


@pytest.mark.parametrize("kind", list(SchedulerQueryKind))
def test_empty_output_is_a_healthy_success(
    fake_bin: Path, kind: SchedulerQueryKind
) -> None:
    provider = observer(fake_bin, mode="empty")

    result = provider.query(request(kind))

    assert result.health.state is HealthState.HEALTHY
    assert result.health.consecutive_failures == 0
    assert result.evidence == ()
    assert result.backoff_seconds == 0.0


def test_empty_queue_replaces_last_good_instead_of_looking_failed(
    fake_bin: Path, mode_file: Path
) -> None:
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        mode_file=mode_file,
    )
    assert len(provider.query(request(SchedulerQueryKind.QUEUE)).evidence) == 2

    mode_file.write_text("empty", encoding="utf-8")
    result = provider.query(request(SchedulerQueryKind.QUEUE, epoch=101.0))

    assert result.health.state is HealthState.HEALTHY
    assert result.evidence == ()
    assert payload_dict(result.summary)["total_jobs"] == 0


def test_history_cache_merges_by_cluster_and_proc_and_survives_empty(
    fake_bin: Path, mode_file: Path, tmp_path: Path
) -> None:
    payload = tmp_path / "history.json"
    shutil.copyfile(FIXTURES / "history.json", payload)
    provider = observer(fake_bin, payload=payload, mode_file=mode_file)
    first = provider.query(request(SchedulerQueryKind.HISTORY))
    assert len(first.evidence) == 2

    payload.write_text(
        json.dumps(
            [
                {
                    "ClusterId": 40,
                    "ProcId": 2,
                    "DAGNodeName": "sibling",
                    "RemoteWallClockTime": 1.0,
                    "pegasus_wf_uuid": "wf-selected",
                }
            ]
        ),
        encoding="utf-8",
    )
    merged = provider.query(request(SchedulerQueryKind.HISTORY, epoch=101.0))
    assert len(merged.evidence) == 3

    mode_file.write_text("empty", encoding="utf-8")
    empty = provider.query(request(SchedulerQueryKind.HISTORY, epoch=102.0))
    assert empty.health.state is HealthState.HEALTHY
    assert len(empty.evidence) == 3


def test_last_good_cache_is_returned_stale_on_failure(
    fake_bin: Path, mode_file: Path
) -> None:
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        mode_file=mode_file,
    )
    good = provider.query(request(SchedulerQueryKind.QUEUE, epoch=100.0))
    mode_file.write_text("nonzero", encoding="utf-8")

    failed = provider.query(request(SchedulerQueryKind.QUEUE, epoch=112.0))

    assert failed.evidence == good.evidence
    assert failed.summary == good.summary
    assert failed.health.state is HealthState.STALE
    assert failed.health.last_success_epoch == 100.0
    assert failed.health.last_good_age is not None
    assert failed.health.last_good_age.seconds == 12.0
    assert failed.health.error_code == "command_failed"


def test_cache_and_backoff_are_isolated_by_effective_workflow_scope(
    fake_bin: Path, mode_file: Path
) -> None:
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        mode_file=mode_file,
    )
    selected = provider.query(request(SchedulerQueryKind.QUEUE, epoch=100.0))
    assert len(selected.evidence) == 2
    mode_file.write_text("nonzero", encoding="utf-8")

    selected_failure = provider.query(request(SchedulerQueryKind.QUEUE, epoch=101.0))
    other_failure = provider.query(
        request(
            SchedulerQueryKind.QUEUE,
            workflow=WorkflowIdentity("wf-other", "root-other"),
            epoch=102.0,
        )
    )

    assert selected_failure.health.state is HealthState.STALE
    assert selected_failure.evidence == selected.evidence
    assert selected_failure.backoff_seconds == 5.0
    assert other_failure.health.state is HealthState.UNAVAILABLE
    assert other_failure.evidence == ()
    assert other_failure.backoff_seconds == 5.0


def test_exponential_backoff_is_state_only_and_resets_on_recovery(
    fake_bin: Path, mode_file: Path
) -> None:
    mode_file.write_text("nonzero", encoding="utf-8")
    provider = observer(fake_bin, mode_file=mode_file)

    first = provider.query(request(SchedulerQueryKind.QUEUE, epoch=10.0))
    second = provider.query(request(SchedulerQueryKind.QUEUE, epoch=9999.0))
    mode_file.write_text("empty", encoding="utf-8")
    recovered = provider.query(request(SchedulerQueryKind.QUEUE, epoch=1.0))

    assert first.backoff_seconds == 5.0
    assert second.backoff_seconds == 10.0
    assert second.health.consecutive_failures == 2
    assert recovered.backoff_seconds == 0.0
    assert recovered.health.consecutive_failures == 0


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("AUTHENTICATION failed for token", "authentication"),
        ("not authorized to read the queue", "authorization"),
        ("failed to connect to schedd", "daemon_unreachable"),
        ("unexpected exit", "command_failed"),
    ],
)
def test_nonzero_failures_are_classified(
    fake_bin: Path, message: str, expected: str
) -> None:
    provider = observer(
        fake_bin,
        mode="nonzero",
        config=CondorObserverConfig(
            environment={"FAKE_CONDOR_ERROR": message, "FAKE_CONDOR_EXIT": "3"}
        ),
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.state is HealthState.UNAVAILABLE
    assert result.health.error_code == expected
    assert result.backoff_seconds == 5.0


def test_missing_command_is_classified_without_fallback(tmp_path: Path) -> None:
    provider = CondorObserver(
        CondorObserverConfig(environment={"PATH": str(tmp_path)}),
        jitter=lambda _delay: 0.0,
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.error_code == "missing_command"
    assert result.health.state is HealthState.UNAVAILABLE


@pytest.mark.parametrize("mode", ["malformed", "bad_shape"])
def test_malformed_json_is_a_parse_failure(fake_bin: Path, mode: str) -> None:
    provider = observer(fake_bin, mode=mode)

    result = provider.query(request(SchedulerQueryKind.POOL))

    assert result.health.error_code == "parse_error"
    assert result.health.state is HealthState.UNAVAILABLE


@pytest.mark.parametrize(
    "content",
    [
        "this is not a ClassAd",
        'Name = "unterminated',
        'Name = "bad\\qescape"',
        'Name = "valid" trailing',
        " = 12",
        'Name = "valid"\ntrailing garbage',
        'Name = "valid"\nEffectivePriority = twelve',
        'Name = "valid"\nEffectivePriority = 12.3oops',
        'Name = "valid"\nEffectivePriority = NaN',
    ],
)
def test_malformed_priority_classads_are_a_parse_failure(
    fake_bin: Path, tmp_path: Path, content: str
) -> None:
    payload = tmp_path / "bad-priority.txt"
    payload.write_text(content, encoding="utf-8")
    provider = observer(fake_bin, payload=payload)

    result = provider.query(request(SchedulerQueryKind.PRIORITY))

    assert result.health.error_code == "parse_error"


def test_stdout_is_hard_bounded_and_process_is_stopped(fake_bin: Path) -> None:
    provider = observer(fake_bin, mode="oversized", stdout_limit=1024)

    result = provider.query(request(SchedulerQueryKind.QUEUE, timeout=2.0))

    assert result.health.error_code == "output_limit"
    assert result.health.state is HealthState.UNAVAILABLE


def test_stderr_is_hard_bounded_and_process_is_stopped(fake_bin: Path) -> None:
    provider = observer(fake_bin, mode="oversized_stderr", stderr_limit=1024)

    result = provider.query(request(SchedulerQueryKind.QUEUE, timeout=2.0))

    assert result.health.error_code == "output_limit"
    assert result.health.state is HealthState.UNAVAILABLE


def test_timeout_kills_the_complete_process_group(
    fake_bin: Path, tmp_path: Path
) -> None:
    started = tmp_path / "started"
    child_pid = tmp_path / "child.pid"
    child_marker = tmp_path / "child.terminated"
    provider = observer(
        fake_bin,
        mode="hang",
        config=CondorObserverConfig(
            environment={
                "FAKE_CONDOR_STARTED": str(started),
                "FAKE_CONDOR_CHILD_PID": str(child_pid),
                "FAKE_CONDOR_CHILD_MARKER": str(child_marker),
            }
        ),
    )

    began = time.monotonic()
    result = provider.query(request(SchedulerQueryKind.QUEUE, timeout=0.4))
    elapsed = time.monotonic() - began

    assert started.exists()
    assert child_pid.exists()
    assert result.health.error_code == "timeout"
    assert elapsed < 2.0
    assert child_marker.read_text(encoding="utf-8") == "terminated"


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_base_exception_reaps_helper_and_descendant(
    fake_bin: Path,
    tmp_path: Path,
    interrupt_type: type[BaseException],
) -> None:
    started = tmp_path / "started"
    child_pid = tmp_path / "child.pid"
    child_marker = tmp_path / "child.terminated"
    child_ready = tmp_path / "child.ready"
    readings = 0

    def interrupting_monotonic() -> float:
        nonlocal readings
        readings += 1
        if readings == 1:
            return 0.0
        deadline = time.monotonic() + 2.0
        while not child_ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        raise interrupt_type()

    provider = observer(
        fake_bin,
        mode="hang",
        monotonic=interrupting_monotonic,
        config=CondorObserverConfig(
            environment={
                "FAKE_CONDOR_STARTED": str(started),
                "FAKE_CONDOR_CHILD_PID": str(child_pid),
                "FAKE_CONDOR_CHILD_MARKER": str(child_marker),
                "FAKE_CONDOR_CHILD_READY": str(child_ready),
            }
        ),
    )

    with pytest.raises(interrupt_type):
        provider.query(request(SchedulerQueryKind.QUEUE, timeout=10.0))

    assert started.exists()
    assert child_pid.exists()
    assert child_marker.read_text(encoding="utf-8") == "terminated"
    assert provider.cancel() is False


def test_concurrent_cancel_reaps_helper_and_descendant(
    fake_bin: Path, tmp_path: Path
) -> None:
    started = tmp_path / "started"
    child_pid = tmp_path / "child.pid"
    child_marker = tmp_path / "child.terminated"
    child_ready = tmp_path / "child.ready"
    provider = observer(
        fake_bin,
        mode="hang",
        config=CondorObserverConfig(
            environment={
                "FAKE_CONDOR_STARTED": str(started),
                "FAKE_CONDOR_CHILD_PID": str(child_pid),
                "FAKE_CONDOR_CHILD_MARKER": str(child_marker),
                "FAKE_CONDOR_CHILD_READY": str(child_ready),
            }
        ),
    )
    results: list[object] = []
    worker = threading.Thread(
        target=lambda: results.append(
            provider.query(request(SchedulerQueryKind.QUEUE, timeout=10.0))
        )
    )
    worker.start()
    deadline = time.monotonic() + 2.0
    while not child_ready.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert child_ready.exists()

    assert provider.cancel() is True
    worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert len(results) == 1
    cancelled = results[0]
    assert cancelled.health.error_code == "cancelled"  # type: ignore[union-attr]
    assert cancelled.health.state is HealthState.DEGRADED  # type: ignore[union-attr]
    assert cancelled.backoff_seconds == 0.0  # type: ignore[union-attr]
    assert child_marker.read_text(encoding="utf-8") == "terminated"
    provider.close()
    assert provider.cancel() is False


def test_one_in_flight_guard_returns_immediately_without_second_process(
    fake_bin: Path, tmp_path: Path
) -> None:
    started = tmp_path / "started"
    log = tmp_path / "commands.jsonl"
    provider = observer(
        fake_bin,
        mode="hang",
        log=log,
        config=CondorObserverConfig(environment={"FAKE_CONDOR_STARTED": str(started)}),
    )
    result_holder: list[object] = []
    worker = threading.Thread(
        target=lambda: result_holder.append(
            provider.query(request(SchedulerQueryKind.QUEUE, timeout=0.5))
        )
    )
    worker.start()
    deadline = time.monotonic() + 2.0
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started.exists()

    began = time.monotonic()
    concurrent = provider.query(request(SchedulerQueryKind.HISTORY, timeout=1.0))
    elapsed = time.monotonic() - began
    worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert elapsed < 0.2
    assert concurrent.health.error_code == "query_in_flight"
    assert concurrent.health.state is HealthState.DEGRADED
    assert concurrent.backoff_seconds == 0.0
    assert len(read_log(log)) == 1


def test_child_environment_is_minimal_isolated_and_parent_unchanged(
    fake_bin: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = tmp_path / "environment.jsonl"
    monkeypatch.setenv("SECRET_PARENT", "must-not-leak")
    monkeypatch.delenv("CONDOR_CONFIG", raising=False)
    config = CondorObserverConfig(
        token="/private/token-dir",
        cert="/private/cert.pem",
        key="/private/key.pem",
        password_file="/private/password",
        condor_config="/private/condor_config",
    )
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        log=log,
        config=config,
    )

    provider.query(request(SchedulerQueryKind.QUEUE))

    child = read_log(log)[0]["environment"]
    assert child == {
        "SECRET_PARENT": None,
        "CONDOR_CONFIG": "/private/condor_config",
        "_CONDOR_SEC_TOKEN_DIRECTORY": "/private/token-dir",
        "X509_USER_CERT": "/private/cert.pem",
        "X509_USER_KEY": "/private/key.pem",
        "_CONDOR_PASSWORD_FILE": "/private/password",
    }
    assert os.environ["SECRET_PARENT"] == "must-not-leak"
    assert "CONDOR_CONFIG" not in os.environ


def test_credential_values_are_redacted_from_failure_detail(fake_bin: Path) -> None:
    provider = observer(
        fake_bin,
        mode="nonzero",
        config=CondorObserverConfig(
            token="/secret/token",
            environment={"FAKE_CONDOR_ERROR": "bad token /secret/token"},
        ),
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.detail is not None
    assert "/secret/token" not in result.health.detail
    assert "<redacted>" in result.health.detail


def test_secret_is_redacted_before_health_detail_truncation(fake_bin: Path) -> None:
    secret = "TOP-SECRET-BOUNDARY-VALUE"
    provider = observer(
        fake_bin,
        mode="nonzero",
        config=CondorObserverConfig(
            token=secret,
            environment={"FAKE_CONDOR_ERROR": ("x" * 380) + secret + "tail"},
        ),
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.detail is not None
    assert secret not in result.health.detail
    assert "TOP-SECRET" not in result.health.detail
    assert "<redacted>" in result.health.detail


def test_whitespace_bearing_secret_is_redacted_before_normalization(
    fake_bin: Path,
) -> None:
    secret = "token  with\tconsecutive   whitespace"
    provider = observer(
        fake_bin,
        mode="nonzero",
        config=CondorObserverConfig(
            token=secret,
            environment={"FAKE_CONDOR_ERROR": f"authentication failed for {secret}"},
        ),
    )

    result = provider.query(request(SchedulerQueryKind.QUEUE))

    assert result.health.detail is not None
    assert secret not in result.health.detail
    assert "token with consecutive whitespace" not in result.health.detail
    assert "<redacted>" in result.health.detail


def test_deadline_clock_is_mechanical_not_a_semantic_timestamp(
    fake_bin: Path,
) -> None:
    readings = iter((1_000_000.0, 1_000_000.01, 1_000_000.02, 1_000_000.03))
    provider = observer(
        fake_bin,
        payload=FIXTURES / "queue.json",
        monotonic=lambda: next(readings),
    )
    source_request = request(
        SchedulerQueryKind.QUEUE,
        epoch=123.25,
        monotonic=-9000.0,
    )

    result = provider.query(source_request)

    assert result.request is source_request
    assert result.health.checked_at_epoch == 123.25
    assert result.health.last_success_epoch == 123.25
    assert result.backoff_seconds == 0.0


def test_payload_strings_and_cache_are_bounded(fake_bin: Path, tmp_path: Path) -> None:
    payload = tmp_path / "large-values.json"
    payload.write_text(
        json.dumps(
            [
                {
                    "ClusterId": index,
                    "ProcId": 0,
                    "DAGNodeName": "x" * 10000,
                    "pegasus_wf_uuid": "wf-selected",
                }
                for index in range(6)
            ]
        ),
        encoding="utf-8",
    )
    provider = observer(fake_bin, payload=payload, cache_limit=3)

    good = provider.query(request(SchedulerQueryKind.QUEUE, limit=10))

    assert len(good.evidence) == 3
    node = good.evidence[0].payload.to_json_dict()["DAGNodeName"]
    assert isinstance(node, str)
    assert len(node) == 4096


def test_observer_configuration_copies_environment_mapping() -> None:
    source = {"EXAMPLE": "first"}
    config = CondorObserverConfig(environment=source)
    source["EXAMPLE"] = "changed"

    assert config.environment["EXAMPLE"] == "first"
    with pytest.raises(TypeError):
        config.environment["EXAMPLE"] = "nope"  # type: ignore[index]
