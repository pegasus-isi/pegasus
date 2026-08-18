"""Integration gates for optional HTCondor failures during core progress."""

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

import asyncio
import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

from conftest import ROOT, append_line, insert_state, job_line

from Pegasus.monitor.condor import CondorObserver, CondorObserverConfig
from Pegasus.monitor.coordinator import CoordinatorConfig, MonitorCoordinator
from Pegasus.monitor.live_events import LiveEventTail
from Pegasus.monitor.models import HealthState, SchedulerQueryKind
from Pegasus.monitor.stampede import StampedeReader

FAKE = Path(__file__).parents[1] / "fixtures" / "condor" / "fake_condor.py"


def make_observer(
    tmp_path: Path,
    mode_file: Path,
    *,
    token: str | None = None,
    **environment: str,
) -> CondorObserver:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir(exist_ok=True)
    source = FAKE.read_text(encoding="utf-8").split("\n", 1)[1]
    leader_probe = """\
#!/usr/bin/env python3
import os
from pathlib import Path

leader_pid_path = os.environ.get("FAKE_CONDOR_LEADER_PID")
if leader_pid_path:
    Path(leader_pid_path).write_text(str(os.getpid()), encoding="utf-8")
"""
    for name in ("condor_q", "condor_history", "condor_status", "condor_userprio"):
        target = binary_dir / name
        target.write_text(leader_probe + source, encoding="utf-8")
        target.chmod(0o755)
    values = {
        "PATH": f"{binary_dir}:{os.environ.get('PATH', '')}",
        "FAKE_CONDOR_MODE_FILE": str(mode_file),
        **environment,
    }
    config = replace(CondorObserverConfig(token=token), environment=values)
    return CondorObserver(
        config,
        stdout_limit=4096,
        stderr_limit=1024,
        jitter=lambda _delay: 0.0,
    )


def wait_for(path: Path, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def wait_for_terminated(pid: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:  # pragma: no cover - foreign-UID platform
            pass
        time.sleep(0.01)
    raise AssertionError(f"process {pid} survived observer cancellation")


def test_hung_condor_does_not_block_real_tail_or_database_progress(
    workflow_database: Path, tmp_path: Path
) -> None:
    log = tmp_path / "jobstate.log"
    log.write_text("100 INTERNAL *** MONITORD_STARTED ***\n", encoding="utf-8")
    mode = tmp_path / "mode"
    mode.write_text("hang", encoding="utf-8")
    started = tmp_path / "started"
    leader_pid_path = tmp_path / "leader.pid"
    child_pid_path = tmp_path / "child.pid"
    child_ready = tmp_path / "child.ready"
    child_marker = tmp_path / "child.terminated"
    observer = make_observer(
        tmp_path,
        mode,
        FAKE_CONDOR_STARTED=str(started),
        FAKE_CONDOR_LEADER_PID=str(leader_pid_path),
        FAKE_CONDOR_CHILD_PID=str(child_pid_path),
        FAKE_CONDOR_CHILD_READY=str(child_ready),
        FAKE_CONDOR_CHILD_MARKER=str(child_marker),
    )
    sentinel = subprocess.Popen(
        (sys.executable, "-c", "import time; time.sleep(30)"),
        start_new_session=True,
    )

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            StampedeReader(workflow_database, ROOT),
            tail=LiveEventTail(log),
            scheduler=observer,
            config=CoordinatorConfig(scheduler_intervals=(), scheduler_timeout=30.0),
        )
        await coordinator.bootstrap()
        query = asyncio.create_task(
            coordinator.poll_scheduler_once(SchedulerQueryKind.QUEUE)
        )
        try:
            await asyncio.to_thread(wait_for, started)
            await asyncio.to_thread(wait_for, leader_pid_path)
            await asyncio.to_thread(wait_for, child_pid_path)
            await asyncio.to_thread(wait_for, child_ready)
            append_line(log, job_line(121, "EXECUTE"))
            tail_result = await asyncio.wait_for(
                coordinator.poll_tail_once(), timeout=0.5
            )
            assert tail_result.job_events
            insert_state(workflow_database, 100, "EXECUTE", 121, 2)
            db_result = await asyncio.wait_for(
                coordinator.refresh_database_once(), timeout=0.5
            )
            assert db_result.snapshot is not None
            assert coordinator.latest is not None
            assert coordinator.latest.effective is not None
            assert coordinator.latest.effective.jobs[0].state == "EXECUTE"
        finally:
            await coordinator.close()
            await asyncio.wait_for(query, timeout=2.0)

    try:
        asyncio.run(exercise())
        wait_for(child_marker)
        leader_pid = int(leader_pid_path.read_text(encoding="utf-8"))
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        wait_for_terminated(leader_pid)
        wait_for_terminated(child_pid)
        assert child_marker.read_text(encoding="utf-8") == "terminated"
        assert sentinel.poll() is None
    finally:
        if sentinel.poll() is None:
            sentinel.terminate()
        sentinel.wait(timeout=2.0)


def test_condor_oversize_and_auth_failure_recover_without_secret_disclosure(
    tmp_path: Path,
) -> None:
    mode = tmp_path / "mode"
    mode.write_text("oversized", encoding="utf-8")
    secret = "monitor-super-secret-token"
    payload = tmp_path / "queue.json"
    payload.write_text(
        json.dumps(
            [
                {
                    "ClusterId": 42,
                    "ProcId": 0,
                    "JobStatus": 1,
                    "pegasus_wf_uuid": ROOT.wf_uuid,
                }
            ]
        ),
        encoding="utf-8",
    )
    observer = make_observer(
        tmp_path,
        mode,
        token=secret,
        FAKE_CONDOR_ERROR=f"authentication failed for {secret}",
        FAKE_CONDOR_PAYLOAD_FILE=str(payload),
    )

    from Pegasus.monitor.models import ClockSample, SchedulerQueryRequest

    def request(epoch: float) -> SchedulerQueryRequest:
        return SchedulerQueryRequest(
            ROOT,
            SchedulerQueryKind.QUEUE,
            ClockSample(epoch, epoch),
            1.0,
            20,
        )

    oversized = observer.query(request(1.0))
    assert oversized.health.error_code == "output_limit"
    assert oversized.backoff_seconds == 5.0

    mode.write_text("nonzero", encoding="utf-8")
    failed = observer.query(request(2.0))
    detail = failed.health.detail or ""
    assert failed.health.state is HealthState.UNAVAILABLE
    assert secret not in detail
    assert failed.backoff_seconds == 10.0
    assert failed.health.consecutive_failures == 2

    mode.write_text("success", encoding="utf-8")
    recovered = observer.query(request(3.0))
    assert recovered.health.state is HealthState.HEALTHY
    assert recovered.health.consecutive_failures == 0
    assert recovered.backoff_seconds == 0.0
    assert len(recovered.evidence) == 1

    mode.write_text("malformed", encoding="utf-8")
    reset_failure = observer.query(request(4.0))
    assert reset_failure.health.consecutive_failures == 1
    assert reset_failure.backoff_seconds == 5.0
    observer.close()
