"""Coordinator ordering, isolation, clock, and cancellation tests."""

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
import threading
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal

import pytest

from Pegasus.monitor.coordinator import (
    CoordinatorConfig,
    MonitorCoordinator,
)
from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBRefreshResult,
    DBTransitionIdentity,
    DBWorkflowTransition,
    DBWorkflowTransitionIdentity,
    FrozenPayload,
    HealthState,
    JobAttempt,
    JobAttemptIdentity,
    JobSnapshot,
    JobTransitionWatermark,
    Provenance,
    SchedulerEvidence,
    SchedulerQueryKind,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    TailGeneration,
    TailJobEvent,
    TailPollRequest,
    TailPollResult,
    TailSourceEvent,
    TailSourceMarker,
    TailTransitionIdentity,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionWatermark,
)
from Pegasus.monitor.reconcile import Reconciler

WORKFLOW = WorkflowIdentity("wf-selected", "wf-root")
DB_GENERATION = DatabaseGeneration(1, 10, 20)
TAIL_GENERATION = TailGeneration(1, 10, 30)


class FakeClock:
    def __init__(self, epoch: float = 100.0, monotonic: float = 50.0) -> None:
        self.epoch = epoch
        self.monotonic = monotonic

    def __call__(self) -> ClockSample:
        return ClockSample(self.epoch, self.monotonic)

    def advance(self, seconds: float) -> None:
        self.epoch += seconds
        self.monotonic += seconds


def transition(state: str, timestamp: str, sequence: int) -> DBJobTransition:
    return DBJobTransition(
        WORKFLOW,
        "compute_ID0000001",
        3,
        DBTransitionIdentity(7, state, Decimal(timestamp), sequence),
    )


def database(
    transitions: tuple[DBJobTransition, ...],
    *,
    generation: DatabaseGeneration = DB_GENERATION,
) -> DatabaseSnapshot:
    wf_transition = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(5, "WORKFLOW_STARTED", Decimal("90")),
        0,
    )
    current = max(transitions, key=lambda item: item.authoritative_sort_key)
    attempt_id = JobAttemptIdentity(2, 7, 3)
    job = JobSnapshot(
        WORKFLOW,
        2,
        "compute_ID0000001",
        "compute",
        1,
        ("example::compute",),
        (JobAttempt(attempt_id, scheduler_id="41.0"),),
        attempt_id,
        current.identity.state,
        current.identity.timestamp,
        current,
        Provenance.DB_CONFIRMED,
    )
    highest = max(item.identity.jobstate_submit_seq for item in transitions)
    return DatabaseSnapshot(
        SnapshotEpoch(1),
        generation,
        100.0,
        WorkflowSnapshot(
            WORKFLOW,
            5,
            "WORKFLOW_STARTED",
            None,
            0,
            Decimal("90"),
            None,
            wf_transition,
        ),
        (job,),
        tuple(sorted(transitions, key=lambda item: item.recent_event_sort_key)),
        (wf_transition,),
        (
            JobTransitionWatermark(
                7,
                highest,
                tuple(
                    item.identity
                    for item in transitions
                    if item.identity.jobstate_submit_seq == highest
                ),
            ),
        ),
        WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 5, 0), (wf_transition.identity,)
        ),
    )


def terminated_database(
    transitions: tuple[DBJobTransition, ...],
) -> DatabaseSnapshot:
    snapshot = database(transitions)
    started = snapshot.workflow.transition
    assert started is not None
    terminated = DBWorkflowTransition(
        WORKFLOW,
        DBWorkflowTransitionIdentity(5, "WORKFLOW_TERMINATED", Decimal("110")),
        0,
        0,
    )
    return replace(
        snapshot,
        workflow=WorkflowSnapshot(
            WORKFLOW,
            5,
            "WORKFLOW_TERMINATED",
            0,
            0,
            started.identity.timestamp,
            terminated.identity.timestamp,
            terminated,
        ),
        recent_workflow_transitions=(started, terminated),
        workflow_watermark=WorkflowTransitionWatermark(
            WorkflowRestartIdentity(WORKFLOW, 5, 0),
            (started.identity, terminated.identity),
        ),
    )


class FakeStampede:
    def __init__(
        self,
        snapshot: DatabaseSnapshot | None,
        calls: list[str],
        *,
        during_refresh: Callable[[], None] | None = None,
        failures: int = 0,
    ) -> None:
        self.snapshot = snapshot
        self.calls = calls
        self.during_refresh = during_refresh
        self.requests = []
        self.failures = failures

    def refresh(self, request):
        self.calls.append("db")
        self.requests.append(request)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("database boom")
        if self.during_refresh is not None:
            self.during_refresh()
            self.during_refresh = None
        snapshot = (
            replace(
                self.snapshot,
                epoch=request.next_epoch,
                snapshot_at_epoch=request.clock.epoch,
            )
            if self.snapshot is not None
            else None
        )
        state = HealthState.HEALTHY if snapshot is not None else HealthState.WAITING
        return DBRefreshResult(
            request,
            snapshot,
            SourceHealth(SourceName.STAMPEDE, state, request.clock.epoch),
            snapshot.generation if snapshot is not None else None,
        )


TailBuilder = Callable[[TailPollRequest], tuple[TailJobEvent, ...]]


class FakeTail:
    def __init__(
        self,
        calls: list[str],
        builders: list[TailBuilder] | None = None,
        *,
        generation: TailGeneration = TAIL_GENERATION,
        failures: int = 0,
    ) -> None:
        self.calls = calls
        self.builders = builders or []
        self.generation = generation
        self.closed = 0
        self.polls = 0
        self.failures = failures

    def append(self, builder: TailBuilder) -> None:
        self.builders.append(builder)

    def poll(self, request: TailPollRequest) -> TailPollResult:
        self.calls.append("tail")
        self.polls += 1
        if self.failures:
            self.failures -= 1
            raise RuntimeError("tail boom")
        events = self.builders.pop(0)(request) if self.builders else ()
        return TailPollResult(
            request,
            events,
            (),
            (),
            (),
            SourceHealth(
                SourceName.LIVE_TAIL, HealthState.HEALTHY, request.clock.epoch
            ),
            self.generation,
            sum(len(item.original_line) for item in events),
            len(events),
        )

    def close(self) -> None:
        self.closed += 1


def tail_builder(
    state: str,
    timestamp: str,
    offset: int,
) -> TailBuilder:
    def build(request: TailPollRequest) -> tuple[TailJobEvent, ...]:
        line = f"{timestamp} compute_ID0000001 {state} - local - 3"
        return (
            TailJobEvent(
                WORKFLOW,
                TailTransitionIdentity(TAIL_GENERATION, offset),
                request.base_db_generation,
                offset + len(line),
                request.clock.monotonic,
                Decimal(timestamp),
                "compute_ID0000001",
                state,
                3,
                "-",
                "local",
                "-",
                line,
            ),
        )

    return build


def empty_builder(_request: TailPollRequest) -> tuple[TailJobEvent, ...]:
    return ()


class BlockingScheduler:
    def __init__(self, *, failures: int = 0) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled = 0
        self.closed = 0
        self.failures = failures

    def query(self, request):
        self.started.set()
        if self.failures:
            self.failures -= 1
            raise RuntimeError("scheduler boom")
        self.release.wait(5.0)
        evidence = SchedulerEvidence(
            request.kind,
            FrozenPayload.from_mapping(
                {"ClusterId": 41, "DAGNodeName": "other_ID0000009", "ProcId": 0}
            ),
            FrozenPayload.from_mapping(
                {
                    "ClusterId": 41,
                    "DAGNodeName": "other_ID0000009",
                    "JobStatus": 2,
                    "ProcId": 0,
                }
            ),
        )
        source = {
            SchedulerQueryKind.QUEUE: SourceName.CONDOR_QUEUE,
            SchedulerQueryKind.HISTORY: SourceName.CONDOR_HISTORY,
            SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
            SchedulerQueryKind.PRIORITY: SourceName.CONDOR_PRIORITY,
            SchedulerQueryKind.NEGOTIATOR: SourceName.CONDOR_NEGOTIATOR,
        }[request.kind]
        return SchedulerQueryResult(
            request,
            SourceHealth(source, HealthState.HEALTHY, request.clock.epoch),
            0.0,
            (evidence,),
        )

    def cancel(self) -> None:
        self.cancelled += 1
        self.release.set()

    def close(self) -> None:
        self.closed += 1
        self.release.set()


def run(coro):
    return asyncio.run(coro)


def test_bootstrap_order_captures_event_appended_during_db_transaction() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        tail = FakeTail(calls, [empty_builder])

        def append_during_refresh() -> None:
            tail.append(tail_builder("EXECUTE", "101", 10))

        stampede = FakeStampede(
            database((transition("SUBMIT", "100", 1),)),
            calls,
            during_refresh=append_during_refresh,
        )
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail, clock=clock)

        publication = await coordinator.bootstrap()

        assert calls == ["tail", "db", "tail"]
        assert publication.effective is not None
        assert publication.effective.jobs[0].state == "EXECUTE"
        assert publication.effective.pending_overlay_count == 1
        await coordinator.close()

    run(scenario())


def test_bootstrap_suppresses_buffered_event_already_in_first_db_snapshot() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls, [tail_builder("EXECUTE", "101", 10)])
        stampede = FakeStampede(
            database(
                (
                    transition("SUBMIT", "100", 1),
                    transition("EXECUTE", "101", 2),
                )
            ),
            calls,
        )
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail)

        publication = await coordinator.bootstrap()

        assert publication.effective is not None
        assert publication.effective.pending_overlay_count == 0
        await coordinator.close()

    run(scenario())


def test_pre_boundary_event_arrives_only_from_later_database_refresh() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail)
        first = await coordinator.bootstrap()
        assert first.effective is not None
        assert first.effective.jobs[0].state == "SUBMIT"

        stampede.snapshot = database(
            (
                transition("SUBMIT", "100", 1),
                transition("EXECUTE", "101", 2),
            )
        )
        await coordinator.refresh_database_once()
        assert coordinator.latest is not None
        assert coordinator.latest.effective is not None
        assert coordinator.latest.effective.jobs[0].state == "EXECUTE"
        assert coordinator.latest.effective.pending_overlay_count == 0
        await coordinator.close()

    run(scenario())


def test_database_waiting_publication_does_not_fabricate_workflow_identity() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        tail = FakeTail(calls, [tail_builder("SUBMIT", "101", 10)])
        stampede = FakeStampede(None, calls)
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail, clock=clock)

        publication = await coordinator.bootstrap()

        assert publication.effective is None
        assert not publication.authoritative
        assert publication.pending_tail_events == 1
        assert len(publication.unconfirmed_tail_events) == 1
        assert publication.unconfirmed_tail_events[0].event.exec_job_id == (
            "compute_ID0000001"
        )
        assert publication.last_tail_event_age is not None
        assert publication.last_tail_event_age.seconds == 0.0
        assert any(
            item.source is SourceName.STAMPEDE and item.state is HealthState.WAITING
            for item in publication.source_health
        )
        await coordinator.close()

    run(scenario())


def test_pre_database_preview_is_stable_then_clears_on_database_arrival() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        tail = FakeTail(calls, [tail_builder("EXECUTE", "101", 10)])
        stampede = FakeStampede(None, calls)
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail, clock=clock)

        first = await coordinator.bootstrap()
        assert len(first.unconfirmed_tail_events) == 1
        preview = first.unconfirmed_tail_events[0]

        clock.advance(3.0)
        await coordinator.refresh_database_once()
        second = coordinator.latest
        assert second is not None
        assert second.unconfirmed_tail_events == first.unconfirmed_tail_events
        assert second.semantic_progress == first.semantic_progress
        assert second.last_tail_event_age is not None
        assert second.last_tail_event_age.seconds == 3.0

        stampede.snapshot = database(
            (
                transition("SUBMIT", "100", 1),
                transition("EXECUTE", "101", 2),
            )
        )
        await coordinator.refresh_database_once()
        confirmed = coordinator.latest
        assert confirmed is not None
        assert confirmed.effective is not None
        assert confirmed.unconfirmed_tail_events == ()
        assert confirmed.last_tail_event_age is None
        matching = [
            event
            for event in confirmed.effective.events
            if isinstance(event.event, DBJobTransition)
            and event.event.normalized_state == "EXECUTE"
        ]
        assert len(matching) == 1
        assert matching[0].order == preview.order
        await coordinator.close()

    run(scenario())


def test_pre_database_tail_event_age_advances_during_readable_silence() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        tail = FakeTail(calls, [tail_builder("EXECUTE", "101", 10)])
        coordinator = MonitorCoordinator(
            WORKFLOW, FakeStampede(None, calls), tail=tail, clock=clock
        )
        await coordinator.bootstrap()

        clock.advance(5.0)
        await coordinator.poll_tail_once()
        publication = coordinator.latest

        assert publication is not None
        assert publication.last_tail_event_age is not None
        assert publication.last_tail_event_age.seconds == 5.0
        assert any(
            item.source is SourceName.LIVE_TAIL and item.state is HealthState.HEALTHY
            for item in publication.source_health
        )
        await coordinator.close()

    run(scenario())


def test_pre_database_malformed_or_empty_tail_does_not_invent_event_age() -> None:
    class UnparsedTail(FakeTail):
        def poll(self, request: TailPollRequest) -> TailPollResult:
            self.calls.append("tail")
            self.polls += 1
            return TailPollResult(
                request,
                (),
                (),
                (),
                (),
                SourceHealth(
                    SourceName.LIVE_TAIL, HealthState.HEALTHY, request.clock.epoch
                ),
                self.generation,
                12,
                1,
            )

    async def scenario() -> None:
        calls: list[str] = []
        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(None, calls),
            tail=UnparsedTail(calls),
            clock=FakeClock(),
        )

        publication = await coordinator.bootstrap()

        assert publication.unconfirmed_tail_events == ()
        assert publication.last_tail_event_age is None
        await coordinator.close()

        empty = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(None, calls),
            tail=FakeTail(calls),
            clock=FakeClock(),
        )
        empty_publication = await empty.bootstrap()
        assert empty_publication.last_tail_event_age is None
        await empty.close()

        disabled = MonitorCoordinator(
            WORKFLOW, FakeStampede(None, calls), clock=FakeClock()
        )
        disabled_publication = await disabled.bootstrap()
        assert disabled_publication.last_tail_event_age is None
        await disabled.close()

    run(scenario())


def test_pre_database_overflow_withholds_preview_and_event_age() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(
            calls,
            [
                lambda request: (
                    tail_builder("EXECUTE", "101", 10)(request)
                    + tail_builder("JOB_SUCCESS", "102", 30)(request)
                )
            ],
        )
        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(None, calls),
            tail=tail,
            reconciler=Reconciler(WORKFLOW, max_pending_events=1),
            clock=FakeClock(),
        )

        publication = await coordinator.bootstrap()

        assert publication.effective is None
        assert publication.unconfirmed_tail_events == ()
        assert publication.last_tail_event_age is None
        assert any(
            item.source is SourceName.LIVE_TAIL and item.state is HealthState.GAP
            for item in publication.source_health
        )
        await coordinator.close()

    run(scenario())


def test_pre_database_source_marker_updates_age_without_fabricating_preview() -> None:
    class SourceTail(FakeTail):
        def poll(self, request: TailPollRequest) -> TailPollResult:
            self.calls.append("tail")
            self.polls += 1
            if self.polls > 1:
                return TailPollResult(
                    request,
                    (),
                    (),
                    (),
                    (),
                    SourceHealth(
                        SourceName.LIVE_TAIL,
                        HealthState.HEALTHY,
                        request.clock.epoch,
                    ),
                    self.generation,
                    0,
                    0,
                )
            line = "101 INTERNAL *** MONITORD_STARTED ***"
            source = TailSourceEvent(
                WORKFLOW,
                TailTransitionIdentity(self.generation, 10),
                request.base_db_generation,
                10 + len(line),
                request.clock.monotonic,
                Decimal("101"),
                TailSourceMarker.MONITORD_STARTED,
                None,
                line,
            )
            return TailPollResult(
                request,
                (),
                (),
                (source,),
                (),
                SourceHealth(
                    SourceName.LIVE_TAIL, HealthState.HEALTHY, request.clock.epoch
                ),
                self.generation,
                len(line),
                1,
            )

    async def scenario() -> None:
        calls: list[str] = []
        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(None, calls),
            tail=SourceTail(calls),
            clock=FakeClock(),
        )
        publication = await coordinator.bootstrap()

        assert publication.unconfirmed_tail_events == ()
        assert publication.last_tail_event_age is not None
        assert publication.last_tail_event_age.seconds == 0.0
        await coordinator.close()

    run(scenario())


def test_stale_refresh_retains_db_confirmed_authoritative_completion() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        stampede = FakeStampede(
            terminated_database((transition("JOB_SUCCESS", "105", 2),)), calls
        )
        coordinator = MonitorCoordinator(WORKFLOW, stampede)

        terminal = await coordinator.bootstrap()
        assert terminal.authoritative_complete

        stampede.failures = 1
        result = await coordinator.refresh_database_once()
        stale = coordinator.latest

        assert result.snapshot is None
        assert result.health.state is HealthState.STALE
        assert stale is not None
        assert stale.authoritative_complete
        assert stale.effective is not None
        assert terminal.effective is not None
        assert stale.effective.workflow == terminal.effective.workflow
        assert stale.effective.jobs == terminal.effective.jobs
        assert stale.effective.events == terminal.effective.events
        assert stale.effective.db_generation == terminal.effective.db_generation
        assert any(
            item.source is SourceName.STAMPEDE and item.state is HealthState.STALE
            for item in stale.source_health
        )
        await coordinator.close()

    run(scenario())


def test_hung_optional_scheduler_does_not_block_tail_or_database() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        scheduler = BlockingScheduler()
        coordinator = MonitorCoordinator(
            WORKFLOW, stampede, tail=tail, scheduler=scheduler
        )
        await coordinator.bootstrap()
        query = asyncio.create_task(
            coordinator.poll_scheduler_once(SchedulerQueryKind.QUEUE)
        )
        await asyncio.to_thread(scheduler.started.wait, 1.0)

        tail.append(tail_builder("EXECUTE", "101", 10))
        await asyncio.wait_for(coordinator.poll_tail_once(), timeout=0.5)
        await asyncio.wait_for(coordinator.refresh_database_once(), timeout=0.5)
        assert not query.done()

        scheduler.release.set()
        await query
        assert coordinator.latest is not None
        assert coordinator.latest.scheduler_results
        assert coordinator.latest.effective is not None
        scheduler_payload = coordinator.latest.effective.jobs[
            0
        ].scheduler.to_json_dict()
        assert scheduler_payload["queue"]["JobStatus"] == 2
        await coordinator.close()

    run(scenario())


def test_refresh_only_publication_does_not_advance_semantic_progress() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail, clock=clock)
        first = await coordinator.bootstrap()
        clock.advance(2.0)
        await coordinator.refresh_database_once()
        second = coordinator.latest
        assert second is not None

        assert second.sequence > first.sequence
        assert second.semantic_progress == first.semantic_progress
        assert second.latest_effective_event == first.latest_effective_event
        await coordinator.close()

    run(scenario())


def test_overlay_overflow_recreates_tail_at_eof_before_full_catchup() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        first_tail = FakeTail(
            calls,
            [
                lambda request: (
                    tail_builder("EXECUTE", "101", 10)(request)
                    + tail_builder("JOB_SUCCESS", "102", 30)(request)
                )
            ],
        )
        replacements: list[FakeTail] = []

        def factory() -> FakeTail:
            replacement = FakeTail(calls, generation=TailGeneration(2, 10, 31))
            replacements.append(replacement)
            return replacement

        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        coordinator = MonitorCoordinator(
            WORKFLOW,
            stampede,
            tail=first_tail,
            tail_factory=factory,
            reconciler=Reconciler(WORKFLOW, max_pending_events=1),
        )

        publication = await coordinator.bootstrap()

        assert first_tail.closed == 1
        assert len(replacements) == 1
        assert stampede.requests[0].mode.value == "full_rebootstrap"
        assert publication.effective is not None
        await coordinator.close()
        assert replacements[0].closed == 1

    run(scenario())


def test_start_owns_three_loops_and_close_cleans_capabilities_idempotently() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        scheduler = BlockingScheduler()
        sleep_gate = asyncio.Event()

        async def sleeping(_delay: float) -> None:
            await sleep_gate.wait()

        coordinator = MonitorCoordinator(
            WORKFLOW,
            stampede,
            tail=tail,
            scheduler=scheduler,
            sleep=sleeping,
            config=CoordinatorConfig(
                scheduler_intervals=((SchedulerQueryKind.QUEUE, 2.0),)
            ),
        )
        await coordinator.start()
        await asyncio.sleep(0)
        names = {task.get_name() for task in coordinator._tasks}
        assert names == {
            "pegasus-monitor-tail",
            "pegasus-monitor-stampede",
            "pegasus-monitor-htcondor",
        }

        await coordinator.close()
        await coordinator.close()
        assert not coordinator._tasks
        assert scheduler.cancelled == 1
        assert scheduler.closed == 1
        assert tail.closed == 1

    run(scenario())


def test_all_provider_requests_use_coordinator_clock_samples() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock(500.0, 700.0)
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        scheduler = BlockingScheduler()
        scheduler.release.set()
        coordinator = MonitorCoordinator(
            WORKFLOW,
            stampede,
            tail=tail,
            scheduler=scheduler,
            clock=clock,
        )
        await coordinator.bootstrap()
        await coordinator.poll_scheduler_once(SchedulerQueryKind.QUEUE)

        assert all(request.clock.epoch == 500.0 for request in stampede.requests)
        assert coordinator.latest is not None
        assert coordinator.latest.clock.epoch == 500.0
        assert all(
            item.health.checked_at_epoch == 500.0
            for item in coordinator.latest.scheduler_results
        )
        await coordinator.close()

    run(scenario())


def test_provider_exceptions_are_visible_and_recover_without_losing_base() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        scheduler = BlockingScheduler(failures=1)
        coordinator = MonitorCoordinator(
            WORKFLOW, stampede, tail=tail, scheduler=scheduler
        )
        await coordinator.bootstrap()

        tail.failures = 1
        tail_failure = await coordinator.poll_tail_once()
        assert tail_failure.health.error_code == "tail_provider_exception"
        assert coordinator.latest is not None
        assert coordinator.latest.effective is not None

        stampede.failures = 1
        db_failure = await coordinator.refresh_database_once()
        assert db_failure.health.state is HealthState.STALE
        assert db_failure.health.error_code == "database_provider_exception"
        assert coordinator.latest is not None
        assert coordinator.latest.effective is not None

        scheduler_failure = await coordinator.poll_scheduler_once(
            SchedulerQueryKind.QUEUE
        )
        assert scheduler_failure is not None
        assert scheduler_failure.health.error_code == "scheduler_provider_exception"
        scheduler.release.set()
        recovered = await coordinator.poll_scheduler_once(SchedulerQueryKind.QUEUE)
        assert recovered is not None
        assert recovered.health.state is HealthState.HEALTHY
        await coordinator.close()

    run(scenario())


def test_start_after_close_fails_and_failed_bootstrap_does_not_poison_start() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        clock = FakeClock()
        failures = 1

        def flaky_clock() -> ClockSample:
            nonlocal failures
            if failures:
                failures -= 1
                raise RuntimeError("clock boom")
            return clock()

        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(database((transition("SUBMIT", "100", 1),)), calls),
            tail=FakeTail(calls),
            clock=flaky_clock,
        )
        with pytest.raises(RuntimeError, match="clock boom"):
            await coordinator.start()
        assert not coordinator._started

        await coordinator.start()
        assert coordinator._started
        await coordinator.close()
        with pytest.raises(RuntimeError, match="closed coordinator"):
            await coordinator.start()

    run(scenario())


def test_blocked_async_publisher_never_blocks_source_publication() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def publisher(_publication) -> None:
            started.set()
            await release.wait()

        tail = FakeTail(calls)
        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(database((transition("SUBMIT", "100", 1),)), calls),
            tail=tail,
            publisher=publisher,
            config=CoordinatorConfig(publisher_queue_limit=2),
        )
        initial = await coordinator.start()
        await asyncio.wait_for(started.wait(), 0.5)

        tail.append(tail_builder("EXECUTE", "101", 10))
        await asyncio.wait_for(coordinator.poll_tail_once(), 0.5)
        assert coordinator.latest is not None
        assert coordinator.latest.sequence > initial.sequence
        assert coordinator.latest.effective is not None
        assert coordinator.latest.effective.jobs[0].state == "EXECUTE"
        await coordinator.close()
        assert not coordinator._tasks

    run(scenario())


def test_publisher_and_unexpected_loop_errors_are_observed_and_visible() -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def failing_publisher(_publication) -> None:
            raise RuntimeError("publisher boom")

        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(database((transition("SUBMIT", "100", 1),)), calls),
            tail=FakeTail(calls),
            publisher=failing_publisher,
            config=CoordinatorConfig(loop_retry_interval=0.001),
        )
        await coordinator.start()
        for _ in range(20):
            if coordinator.latest is not None and coordinator.latest.publisher_failures:
                break
            await asyncio.sleep(0.001)
        assert coordinator.latest is not None
        assert coordinator.latest.publisher_failures >= 1
        assert "publisher boom" in (coordinator.latest.publisher_error or "")

        attempts = 0
        hold = asyncio.Event()

        async def flaky_loop() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("loop boom")
            await hold.wait()

        coordinator._create_supervised(flaky_loop, "test-supervised-loop")
        for _ in range(20):
            if attempts >= 2:
                break
            await asyncio.sleep(0.001)
        assert attempts >= 2
        assert coordinator.latest is not None
        assert (
            coordinator.latest.coordinator_errors.to_json_dict()["test-supervised-loop"]
            == "RuntimeError: loop boom"
        )
        await coordinator.close()

    run(scenario())


def test_database_thread_does_not_block_tail_and_live_tail_is_optional() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        stampede = FakeStampede(database((transition("SUBMIT", "100", 1),)), calls)
        coordinator = MonitorCoordinator(WORKFLOW, stampede, tail=tail)
        await coordinator.bootstrap()

        started = threading.Event()
        release = threading.Event()
        original_refresh = stampede.refresh

        def blocked_refresh(request):
            started.set()
            release.wait(2.0)
            return original_refresh(request)

        stampede.refresh = blocked_refresh
        refresh = asyncio.create_task(coordinator.refresh_database_once())
        await asyncio.to_thread(started.wait, 0.5)
        tail.append(tail_builder("EXECUTE", "101", 10))
        await asyncio.wait_for(coordinator.poll_tail_once(), 0.5)
        release.set()
        await refresh
        await coordinator.close()

        db_only = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(database((transition("SUBMIT", "100", 1),)), []),
        )
        publication = await db_only.bootstrap()
        assert publication.effective is not None
        assert any(
            health.source is SourceName.LIVE_TAIL
            and health.state is HealthState.DISABLED
            for health in publication.source_health
        )
        await db_only.close()

    run(scenario())


def test_ordinary_tail_event_does_not_force_database_wakeup() -> None:
    async def scenario() -> None:
        calls: list[str] = []
        tail = FakeTail(calls)
        coordinator = MonitorCoordinator(
            WORKFLOW,
            FakeStampede(database((transition("SUBMIT", "100", 1),)), calls),
            tail=tail,
        )
        await coordinator.bootstrap()
        assert not coordinator._database_wakeup.is_set()
        tail.append(tail_builder("EXECUTE", "101", 10))
        await coordinator.poll_tail_once()
        assert not coordinator._database_wakeup.is_set()
        await coordinator.close()

    run(scenario())
