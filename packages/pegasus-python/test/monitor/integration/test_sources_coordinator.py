"""Cross-component live-tail, Stampede, and coordinator acceptance tests."""

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
import os
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from conftest import (
    CHILD,
    ROOT,
    append_line,
    create_database,
    insert_state,
    job_line,
)

from Pegasus.monitor.coordinator import CoordinatorConfig, MonitorCoordinator
from Pegasus.monitor.live_events import LiveEventTail
from Pegasus.monitor.models import DBRefreshMode, HealthState, Provenance, SourceName
from Pegasus.monitor.stampede import StampedeReader


def health(snapshot, source: SourceName):
    return next(item for item in snapshot.source_health if item.source is source)


def test_real_attachment_race_is_live_then_retires_to_stampede(
    workflow_database: Path, tmp_path: Path
) -> None:
    log = tmp_path / "jobstate.log"
    log.write_text("100 INTERNAL *** MONITORD_STARTED ***\n", encoding="utf-8")
    reader = StampedeReader(workflow_database, ROOT)

    class AppendDuringFirstRefresh:
        appended = False

        def refresh(self, request):
            if not self.appended:
                append_line(log, job_line(121, "EXECUTE"))
                self.appended = True
            return reader.refresh(request)

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            AppendDuringFirstRefresh(),
            tail=LiveEventTail(log),
            config=CoordinatorConfig(scheduler_intervals=()),
        )
        try:
            raced = await coordinator.bootstrap()
            assert raced.effective is not None
            assert raced.effective.jobs[0].state == "EXECUTE"
            assert raced.effective.jobs[0].provenance is Provenance.DB_WITH_TAIL_OVERLAY
            assert raced.pending_tail_events == 1

            insert_state(workflow_database, 100, "EXECUTE", 121, 2)
            await coordinator.refresh_database_once()
            confirmed = coordinator.latest
            assert confirmed is not None and confirmed.effective is not None
            assert confirmed.effective.jobs[0].state == "EXECUTE"
            assert confirmed.effective.jobs[0].provenance is Provenance.DB_CONFIRMED
            assert confirmed.pending_tail_events == 0
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_late_attachment_skips_old_file_events_and_uses_database_authority(
    workflow_database: Path, tmp_path: Path
) -> None:
    insert_state(workflow_database, 100, "EXECUTE", 121, 2)
    log = tmp_path / "jobstate.log"
    log.write_text(
        "100 INTERNAL *** MONITORD_STARTED ***\n"
        + job_line(110, "SUBMIT")
        + job_line(121, "EXECUTE"),
        encoding="utf-8",
    )

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            StampedeReader(workflow_database, ROOT),
            tail=LiveEventTail(log),
            config=CoordinatorConfig(scheduler_intervals=()),
        )
        try:
            snapshot = await coordinator.bootstrap()
            assert snapshot.effective is not None
            assert snapshot.effective.jobs[0].state == "EXECUTE"
            assert snapshot.effective.jobs[0].provenance is Provenance.DB_CONFIRMED
            assert snapshot.pending_tail_events == 0
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_real_held_pair_folds_into_one_authoritative_database_transition(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "held.db", include_held=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "DELETE FROM jobstate WHERE job_instance_id=200 AND state='JOB_HELD'"
        )
    log = tmp_path / "jobstate.log"
    log.write_text("100 INTERNAL *** MONITORD_STARTED ***\n", encoding="utf-8")

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            StampedeReader(database, ROOT),
            tail=LiveEventTail(log),
            config=CoordinatorConfig(scheduler_intervals=()),
        )
        try:
            await coordinator.bootstrap()
            append_line(
                log,
                job_line(
                    125,
                    "JOB_HELD",
                    job="held_ID0000002",
                    value="-",
                    submit_sequence=2,
                ),
            )
            append_line(
                log,
                job_line(
                    125,
                    "JOB_HELD_REASON",
                    job="held_ID0000002",
                    value="-",
                    submit_sequence=2,
                ),
            )
            provisional = await coordinator.poll_tail_once()
            assert [event.state for event in provisional.job_events] == [
                "JOB_HELD",
                "JOB_HELD_REASON",
            ]
            assert coordinator.latest is not None
            assert coordinator.latest.pending_tail_events == 2
            assert coordinator.latest.effective is not None
            held = next(
                job
                for job in coordinator.latest.effective.jobs
                if job.exec_job_id == "held_ID0000002"
            )
            assert held.state == "JOB_HELD"

            insert_state(database, 200, "JOB_HELD", 125, 2, "operator hold")
            await coordinator.refresh_database_once()
            assert coordinator.latest is not None
            assert coordinator.latest.pending_tail_events == 0
            assert coordinator.latest.effective is not None
            held = next(
                job
                for job in coordinator.latest.effective.jobs
                if job.exec_job_id == "held_ID0000002"
            )
            assert held.provenance is Provenance.DB_CONFIRMED
            assert held.transition is not None
            assert held.transition.reason == "operator hold"
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_tail_missing_creation_rotation_truncation_and_factory_recovery(
    workflow_database: Path, tmp_path: Path
) -> None:
    log = tmp_path / "jobstate.log"
    tails: list[LiveEventTail] = []

    def factory() -> LiveEventTail:
        tail = LiveEventTail(log, max_line_bytes=128, max_buffer_bytes=256)
        tails.append(tail)
        return tail

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            StampedeReader(workflow_database, ROOT),
            tail_factory=factory,
            config=CoordinatorConfig(
                scheduler_intervals=(), tail_max_bytes=128, tail_max_lines=2
            ),
        )
        try:
            waiting = await coordinator.bootstrap()
            assert health(waiting, SourceName.LIVE_TAIL).state is HealthState.WAITING

            log.write_text("", encoding="utf-8")
            await coordinator.poll_tail_once()
            append_line(log, job_line(121, "EXECUTE"))
            observed = await coordinator.poll_tail_once()
            assert [event.state for event in observed.job_events] == ["EXECUTE"]

            replacement = tmp_path / "replacement.log"
            replacement.write_text(job_line(130, "JOB_SUCCESS", value="0"))
            os.replace(replacement, log)
            rotated = await coordinator.poll_tail_once()
            assert rotated.gaps
            assert len(tails) == 1
            rebuilt = await coordinator.refresh_database_once()
            assert rebuilt.snapshot is not None
            # A replaced path is drained to a stable EOF before the provider
            # moves to the new inode. The replacement's pre-attachment line is
            # intentionally not replayed.
            await coordinator.poll_tail_once()
            await coordinator.poll_tail_once()

            # The tail has moved to the replacement inode. A subsequent append
            # after the forced DB bootstrap proves contiguous recovery.
            append_line(log, job_line(131, "POST_SCRIPT_STARTED", value="-"))
            recovered = await coordinator.poll_tail_once()
            assert [event.state for event in recovered.job_events] == [
                "POST_SCRIPT_STARTED"
            ]

            with log.open("r+b", buffering=0) as stream:
                stream.truncate(0)
                stream.write(job_line(140, "JOB_SUCCESS", value="0").encode())
            truncated = await coordinator.poll_tail_once()
            assert truncated.gaps
            assert len(tails) == 1
            assert (await coordinator.refresh_database_once()).snapshot is not None
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_tail_buffer_overflow_rearms_forces_full_refresh_and_recovers(
    workflow_database: Path, tmp_path: Path
) -> None:
    log = tmp_path / "jobstate.log"
    log.write_text("100 INTERNAL *** MONITORD_STARTED ***\n", encoding="utf-8")
    reader = StampedeReader(workflow_database, ROOT)
    requests = []
    tails: list[LiveEventTail] = []

    class RecordingStampede:
        def refresh(self, request):
            requests.append(request)
            return reader.refresh(request)

    def factory() -> LiveEventTail:
        tail = LiveEventTail(log, max_line_bytes=64, max_buffer_bytes=80)
        tails.append(tail)
        return tail

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            RecordingStampede(),
            tail_factory=factory,
            config=CoordinatorConfig(
                scheduler_intervals=(), tail_max_bytes=128, tail_max_lines=8
            ),
        )
        try:
            await coordinator.bootstrap()
            assert requests[-1].mode is DBRefreshMode.FULL_REBOOTSTRAP
            assert len(tails) == 1
            initial_generation = tails[0].generation

            append_line(log, "x" * 100 + "\n")
            overflow = await coordinator.poll_tail_once()
            assert [gap.reason for gap in overflow.gaps] == ["buffer_overflow"]
            assert overflow.health.state is HealthState.GAP
            assert len(tails) == 1
            assert tails[0].generation != initial_generation
            assert tails[0].attachment_offset == log.stat().st_size
            assert coordinator.latest is not None
            assert (
                health(coordinator.latest, SourceName.LIVE_TAIL).state
                is HealthState.GAP
            )

            rebuilt = await coordinator.refresh_database_once()
            assert rebuilt.snapshot is not None
            assert requests[-1].mode is DBRefreshMode.FULL_REBOOTSTRAP

            append_line(log, job_line(121, "EXECUTE"))
            recovered = await coordinator.poll_tail_once()
            assert not recovered.gaps
            assert recovered.health.state is HealthState.HEALTHY
            assert [event.state for event in recovered.job_events] == ["EXECUTE"]
            assert coordinator.latest is not None
            assert coordinator.latest.pending_tail_events == 1
            assert coordinator.latest.effective is not None
            assert coordinator.latest.effective.jobs[0].state == "EXECUTE"
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_db_lock_replacement_and_rollback_keep_last_good_until_rebootstrap(
    workflow_database: Path, tmp_path: Path
) -> None:
    reader = StampedeReader(workflow_database, ROOT, busy_timeout_seconds=0.01)

    async def exercise() -> None:
        coordinator = MonitorCoordinator(
            ROOT,
            reader,
            config=CoordinatorConfig(scheduler_intervals=()),
        )
        try:
            first = await coordinator.bootstrap()
            assert first.effective is not None
            first_state = first.effective.jobs[0].state

            lock = sqlite3.connect(workflow_database)
            try:
                lock.execute("BEGIN EXCLUSIVE")
                lock.execute("UPDATE workflow SET dag_file_name = dag_file_name")
                locked = await coordinator.refresh_database_once()
            finally:
                lock.rollback()
                lock.close()
            assert locked.health.error_code == "database_busy"
            assert coordinator.latest is not None
            assert coordinator.latest.effective is not None
            assert coordinator.latest.effective.jobs[0].state == first_state
            assert (
                health(coordinator.latest, SourceName.STAMPEDE).state
                is HealthState.STALE
            )

            replacement = create_database(tmp_path / "replacement.db")
            insert_state(replacement, 100, "EXECUTE", 121, 2)
            os.replace(replacement, workflow_database)
            replaced = await coordinator.refresh_database_once()
            assert replaced.health.state is HealthState.RESYNC
            assert replaced.health.error_code == "database_generation_changed"
            assert coordinator.latest is not None
            assert coordinator.latest.effective is not None
            assert coordinator.latest.effective.jobs[0].state == first_state

            rebuilt = await coordinator.refresh_database_once()
            assert rebuilt.snapshot is not None
            assert coordinator.latest is not None
            assert coordinator.latest.effective is not None
            assert coordinator.latest.effective.jobs[0].state == "EXECUTE"

            with sqlite3.connect(workflow_database) as connection:
                connection.execute(
                    "DELETE FROM jobstate WHERE job_instance_id=100 AND state='EXECUTE'"
                )
            rollback = await coordinator.refresh_database_once()
            assert rollback.health.state is HealthState.RESYNC
            assert rollback.health.error_code == "transition_watermark_rollback"
            recovered = await coordinator.refresh_database_once()
            assert recovered.snapshot is not None
            assert recovered.snapshot.jobs[0].state == "SUBMIT"
        finally:
            await coordinator.close()

    asyncio.run(exercise())


def test_authoritative_counts_preserve_clusters_retries_holds_and_scope(
    tmp_path: Path,
) -> None:
    database = create_database(
        tmp_path / "scope.db",
        include_child=True,
        include_retry=True,
        include_held=True,
    )

    async def bootstrap(workflow):
        coordinator = MonitorCoordinator(
            workflow,
            StampedeReader(database, workflow),
            config=CoordinatorConfig(scheduler_intervals=()),
        )
        try:
            return await coordinator.bootstrap()
        finally:
            await coordinator.close()

    root = asyncio.run(bootstrap(ROOT))
    child = asyncio.run(bootstrap(CHILD))
    assert root.effective is not None and child.effective is not None
    assert len(root.effective.jobs) == 2
    cluster = next(job for job in root.effective.jobs if job.task_count == 2)
    held = next(job for job in root.effective.jobs if job.state == "JOB_HELD")
    assert len(cluster.attempts) == 2
    assert cluster.current_attempt.job_submit_seq == 4
    assert cluster.state == "JOB_SUCCESS"
    assert held.transition is not None and held.transition.reason == "operator hold"
    assert [job.exec_job_id for job in child.effective.jobs] == ["child_job"]
