"""Subprocess-level acceptance tests for native pegasus-monitor modes."""

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
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import create_run

PACKAGE_SRC = Path(__file__).parents[3] / "src"
COMMON_SRC = Path(__file__).parents[4] / "pegasus-common" / "src"


def environment() -> dict[str, str]:
    current = os.environ.get("PYTHONPATH")
    paths = current.split(os.pathsep) if current else []
    paths.extend((str(PACKAGE_SRC), str(COMMON_SRC)))
    return {**os.environ, "PYTHONPATH": os.pathsep.join(paths)}


def run_monitor(*arguments: str, timeout: float = 8.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "Pegasus.monitor.cli", *arguments],
        text=True,
        capture_output=True,
        env=environment(),
        timeout=timeout,
        check=False,
    )


@pytest.mark.parametrize("extra", [(), ("--no-live-events",)])
def test_redirected_once_and_source_disable_modes(tmp_path: Path, extra) -> None:
    run = tmp_path / "run0001"
    database, tail = create_run(run)
    db_before = database.stat()
    before = tail.stat()
    names_before = {path.name for path in run.iterdir()}

    result = run_monitor("--once", "--no-condor", *extra, str(run))

    assert result.returncode == 0, result.stderr
    assert "integration-diamond" in result.stdout
    assert "WORKFLOW_TERMINATED" in result.stdout
    after = tail.stat()
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
    assert database.stat().st_mtime_ns == db_before.st_mtime_ns
    assert {path.name for path in run.iterdir()} == names_before


def test_missing_database_once_fails_without_creating_it(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, _tail = create_run(run)
    database.unlink()

    result = run_monitor("--once", "--no-condor", str(run))

    assert result.returncode == 1
    assert "--once requires an authoritative Stampede snapshot" in result.stderr
    assert not database.exists()


def test_redirected_live_requires_tty_before_source_activity(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, tail = create_run(run)
    db_before = database.stat()
    tail_before = tail.stat()

    result = run_monitor("--no-condor", str(run))

    assert result.returncode == 1
    assert "interactive live mode requires a TTY" in result.stderr
    assert database.stat().st_mtime_ns == db_before.st_mtime_ns
    assert tail.stat().st_mtime_ns == tail_before.st_mtime_ns


@pytest.mark.skipif(not hasattr(os, "openpty"), reason="PTY requires POSIX")
def test_tail_only_live_process_exits_cleanly_on_sigint(tmp_path: Path) -> None:
    run = tmp_path / "run0001"
    database, _tail = create_run(run, terminated=False)
    database.unlink()
    master, slave = os.openpty()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "Pegasus.monitor.cli",
            "--no-condor",
            "--interval",
            "0.1",
            str(run),
        ],
        stdin=subprocess.DEVNULL,
        stdout=slave,
        stderr=slave,
        env={**environment(), "TERM": "xterm-256color"},
        close_fds=True,
    )
    os.close(slave)
    try:
        deadline = time.monotonic() + 4.0
        output = bytearray()
        while time.monotonic() < deadline and process.poll() is None:
            readable, _, _ = select.select([master], [], [], 0.1)
            if not readable:
                continue
            try:
                chunk = os.read(master, 4096)
            except OSError:
                break
            if chunk:
                output.extend(chunk)
                if b"TAIL" in output or b"Stampede" in output:
                    break
        assert b"TAIL" in output or b"Stampede" in output, (
            f"process={process.poll()} output={output.decode(errors='replace')!r}"
        )
        process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline and process.poll() is None:
            readable, _, _ = select.select([master], [], [], 0.1)
            if not readable:
                continue
            try:
                chunk = os.read(master, 4096)
            except OSError:
                continue
            if chunk:
                output.extend(chunk)
        returncode = process.poll()
        assert returncode is not None, output.decode(errors="replace")
        assert returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2.0)
        os.close(master)
