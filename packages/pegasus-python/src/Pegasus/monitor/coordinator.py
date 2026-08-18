"""Scheduling and publication coordinator for native workflow monitoring.

All cadence, task creation, clock sampling, and cancellation lives here.
Providers remain bounded synchronous components.  HTCondor queries run outside
the core event-loop path and are serialized so a slow optional source cannot
delay tail or Stampede progress.
"""

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
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import TypeAlias

from Pegasus.monitor.models import (
    ClockSample,
    DBRefreshMode,
    DBRefreshRequest,
    DBRefreshResult,
    EffectiveEvent,
    EffectiveSnapshot,
    FrozenPayload,
    HealthState,
    LiveTailProvider,
    Provenance,
    SchedulerProvider,
    SchedulerQueryKind,
    SchedulerQueryRequest,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    StampedeSnapshotProvider,
    TailPollRequest,
    TailPollResult,
    WorkflowIdentity,
)
from Pegasus.monitor.reconcile import Reconciler

Clock: TypeAlias = Callable[[], ClockSample]
Sleep: TypeAlias = Callable[[float], Awaitable[None]]
TailFactory: TypeAlias = Callable[[], LiveTailProvider]
Publisher: TypeAlias = Callable[["CoordinatorSnapshot"], object]


def system_clock() -> ClockSample:
    return ClockSample(time.time(), time.monotonic())


_SCHEDULER_SOURCE = {
    SchedulerQueryKind.QUEUE: SourceName.CONDOR_QUEUE,
    SchedulerQueryKind.HISTORY: SourceName.CONDOR_HISTORY,
    SchedulerQueryKind.POOL: SourceName.CONDOR_POOL,
    SchedulerQueryKind.PRIORITY: SourceName.CONDOR_PRIORITY,
    SchedulerQueryKind.NEGOTIATOR: SourceName.CONDOR_NEGOTIATOR,
}


def _exception_detail(error: BaseException) -> str:
    return f"{error.__class__.__name__}: {error}"[-512:]


@dataclass(frozen=True, slots=True)
class CoordinatorConfig:
    tail_active_interval: float = 0.25
    tail_idle_interval: float = 1.0
    database_interval: float = 2.0
    tail_max_bytes: int = 256 * 1024
    tail_max_lines: int = 2048
    recent_transition_limit: int = 256
    recent_workflow_transition_limit: int = 64
    scheduler_timeout: float = 5.0
    scheduler_result_limit: int = 4096
    publisher_queue_limit: int = 64
    loop_retry_interval: float = 1.0
    scheduler_intervals: tuple[tuple[SchedulerQueryKind, float], ...] = field(
        default_factory=lambda: (
            (SchedulerQueryKind.QUEUE, 5.0),
            (SchedulerQueryKind.HISTORY, 30.0),
            (SchedulerQueryKind.POOL, 30.0),
        )
    )

    def __post_init__(self) -> None:
        positive = (
            self.tail_active_interval,
            self.tail_idle_interval,
            self.database_interval,
            self.scheduler_timeout,
            self.loop_retry_interval,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("coordinator intervals and timeout must be positive")
        if self.tail_max_bytes <= 0 or self.tail_max_lines <= 0:
            raise ValueError("tail poll bounds must be positive")
        if (
            self.recent_transition_limit <= 0
            or self.recent_workflow_transition_limit <= 0
            or self.scheduler_result_limit <= 0
            or self.publisher_queue_limit <= 0
        ):
            raise ValueError("coordinator result limits must be positive")
        kinds = [kind for kind, _ in self.scheduler_intervals]
        if len(set(kinds)) != len(kinds):
            raise ValueError("scheduler cadence kinds must be unique")
        if any(interval <= 0 for _, interval in self.scheduler_intervals):
            raise ValueError("scheduler cadence intervals must be positive")


@dataclass(frozen=True, slots=True)
class CoordinatorSnapshot:
    """One immutable publication, including pre-DB and global-source state."""

    sequence: int
    clock: ClockSample
    effective: EffectiveSnapshot | None
    source_health: tuple[SourceHealth, ...]
    scheduler_results: tuple[SchedulerQueryResult, ...]
    pending_tail_events: int
    semantic_progress: int
    latest_effective_event: EffectiveEvent | None
    has_authoritative_base: bool
    authoritative_complete: bool
    publisher_failures: int = 0
    publisher_error: str | None = None
    coordinator_errors: FrozenPayload = field(default_factory=FrozenPayload)

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            raise ValueError("coordinator publication sequence must be positive")
        if self.pending_tail_events < 0:
            raise ValueError("pending tail count must be non-negative")
        if self.semantic_progress < 0:
            raise ValueError("semantic progress must be non-negative")
        if self.publisher_failures < 0:
            raise ValueError("publisher failure count must be non-negative")
        if (
            self.latest_effective_event is not None
            and self.latest_effective_event.order > self.semantic_progress
        ):
            raise ValueError("latest event cannot exceed semantic progress")
        if self.has_authoritative_base != (self.effective is not None):
            raise ValueError("authoritative base flag must match effective snapshot")
        if self.authoritative_complete and not self.has_authoritative_base:
            raise ValueError("authoritative completion requires a database base")

    @property
    def authoritative(self) -> bool:
        """Backward-compatible alias for authoritative workflow completion."""

        return self.authoritative_complete


class MonitorCoordinator:
    """Own the monitor's independent source loops and immutable publications."""

    def __init__(
        self,
        workflow: WorkflowIdentity,
        stampede: StampedeSnapshotProvider,
        *,
        tail: LiveTailProvider | None = None,
        tail_factory: TailFactory | None = None,
        scheduler: SchedulerProvider | None = None,
        reconciler: Reconciler | None = None,
        config: CoordinatorConfig | None = None,
        clock: Clock = system_clock,
        sleep: Sleep = asyncio.sleep,
        publisher: Publisher | None = None,
    ) -> None:
        self.workflow = workflow
        self.stampede = stampede
        self._tail_factory = tail_factory
        # Constructing LiveEventTail arms it immediately, before bootstrap.
        self.tail = (
            tail
            if tail is not None
            else tail_factory()
            if tail_factory is not None
            else None
        )
        self.scheduler = scheduler
        self.reconciler = reconciler or Reconciler(workflow)
        self.config = config or CoordinatorConfig()
        self._clock = clock
        self._sleep = sleep
        self._publisher = publisher
        self._publication_sequence = 0
        self._effective_epoch = 0
        self._latest: CoordinatorSnapshot | None = None
        self._scheduler_results: dict[SchedulerQueryKind, SchedulerQueryResult] = {}
        self._publisher_queue: asyncio.Queue[CoordinatorSnapshot] = asyncio.Queue(
            maxsize=self.config.publisher_queue_limit
        )
        self._publisher_failures = 0
        self._publisher_error: str | None = None
        self._coordinator_errors: dict[str, str] = {}
        self._tasks: set[asyncio.Task[object]] = set()
        self._stop_event = asyncio.Event()
        self._database_wakeup = asyncio.Event()
        self._scheduler_wakeup = asyncio.Event()
        self._last_terminal_count = 0
        self._final_history_requested = False
        self._started = False
        self._closed = False
        self._publish_lock = asyncio.Lock()
        self._database_refresh_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()

    @property
    def latest(self) -> CoordinatorSnapshot | None:
        return self._latest

    def _sample_clock(self) -> ClockSample:
        sample = self._clock()
        if not isinstance(sample, ClockSample):
            raise TypeError("coordinator clock must return ClockSample")
        return sample

    def _next_epoch(self) -> SnapshotEpoch:
        self._effective_epoch += 1
        return SnapshotEpoch(self._effective_epoch)

    async def bootstrap(self) -> CoordinatorSnapshot:
        """Arm/bind tail, bootstrap DB, drain the race window, then publish."""

        if self._closed:
            raise RuntimeError("coordinator is closed")
        # The concrete tail is armed on construction.  This first bounded poll
        # binds its workflow and captures appends before the DB transaction.
        if self.tail is not None:
            await self.poll_tail_once(publish=False)
        else:
            clock = self._sample_clock()
            self.reconciler.update_health(
                SourceHealth(
                    SourceName.LIVE_TAIL,
                    HealthState.DISABLED,
                    clock.epoch,
                    error_code="live_tail_disabled",
                )
            )
        await self.refresh_database_once(
            requested_mode=DBRefreshMode.FULL_REBOOTSTRAP, publish=False
        )
        # Drain lines appended while the DB transaction was in progress.
        if self.tail is not None and not (
            self.reconciler.tail_rearm_required and self._tail_factory is None
        ):
            await self.poll_tail_once(publish=False)
        return await self.publish()

    async def poll_tail_once(self, *, publish: bool = True) -> TailPollResult:
        if self.tail is None:
            raise RuntimeError("live tail is disabled")
        if self.reconciler.tail_rearm_required and self._tail_factory is None:
            raise RuntimeError(
                "tail overlay gap requires an injected factory before polling resumes"
            )
        request = TailPollRequest(
            workflow=self.workflow,
            base_db_generation=self.reconciler.database_generation,
            clock=self._sample_clock(),
            max_bytes=self.config.tail_max_bytes,
            max_lines=self.config.tail_max_lines,
        )
        try:
            result = self.tail.poll(request)
        except Exception as error:
            result = TailPollResult(
                request=request,
                job_events=(),
                workflow_events=(),
                source_events=(),
                gaps=(),
                health=SourceHealth(
                    SourceName.LIVE_TAIL,
                    HealthState.DEGRADED,
                    request.clock.epoch,
                    consecutive_failures=1,
                    error_code="tail_provider_exception",
                    detail=_exception_detail(error),
                ),
                generation=self.reconciler.tail_generation,
                bytes_read=0,
                lines_read=0,
            )
        urgent = self.reconciler.ingest_tail(result)
        if self.reconciler.tail_rearm_required:
            self._rearm_tail()
        if urgent:
            self._database_wakeup.set()
        if publish:
            await self.publish()
        return result

    def _rearm_tail(self) -> None:
        if self._tail_factory is None:
            # Stop consuming at a known gap.  Resuming the same descriptor
            # would silently pretend the dropped prefix was observed.
            return
        self._call_capability(self.tail, "close")
        self.tail = self._tail_factory()
        self.reconciler.mark_tail_rearmed()

    async def refresh_database_once(
        self,
        *,
        requested_mode: DBRefreshMode | None = None,
        publish: bool = True,
    ) -> DBRefreshResult:
        async with self._database_refresh_lock:
            return await self._refresh_database_once(
                requested_mode=requested_mode, publish=publish
            )

    async def _refresh_database_once(
        self,
        *,
        requested_mode: DBRefreshMode | None,
        publish: bool,
    ) -> DBRefreshResult:
        mode = requested_mode or self.reconciler.refresh_mode()
        cursor = self.reconciler.reconciliation_cursor()
        if mode is not DBRefreshMode.BOUNDED_SUFFIX:
            cursor_job_watermarks = ()
            cursor_job_keys = ()
            cursor_workflow = None
        else:
            cursor_job_watermarks = cursor.job_watermarks
            cursor_job_keys = cursor.provisional_job_keys
            cursor_workflow = cursor.workflow_watermark
            if not (
                cursor_job_watermarks or cursor_job_keys or cursor_workflow is not None
            ):
                # A tail source/lifecycle wakeup may request an immediate DB
                # poll without a state overlay.
                mode = DBRefreshMode.CURRENT_SNAPSHOT

        request = DBRefreshRequest(
            workflow=self.workflow,
            next_epoch=SnapshotEpoch(self._effective_epoch + 1),
            mode=mode,
            clock=self._sample_clock(),
            prior_generation=(
                None
                if mode is DBRefreshMode.FULL_REBOOTSTRAP
                else self.reconciler.database_generation
            ),
            pending_job_watermarks=cursor_job_watermarks,
            pending_job_keys=cursor_job_keys,
            pending_workflow_watermark=cursor_workflow,
            recent_transition_limit=self.config.recent_transition_limit,
            recent_workflow_transition_limit=self.config.recent_workflow_transition_limit,
        )
        try:
            result = await asyncio.to_thread(self.stampede.refresh, request)
        except Exception as error:
            result = DBRefreshResult(
                request=request,
                snapshot=None,
                health=SourceHealth(
                    SourceName.STAMPEDE,
                    (
                        HealthState.STALE
                        if self.reconciler.database is not None
                        else HealthState.WAITING
                    ),
                    request.clock.epoch,
                    consecutive_failures=1,
                    error_code="database_provider_exception",
                    detail=_exception_detail(error),
                ),
                generation=self.reconciler.database_generation,
            )
        self.reconciler.apply_database(result)
        if publish:
            await self.publish()
        return result

    async def poll_scheduler_once(
        self, kind: SchedulerQueryKind, *, publish: bool = True
    ) -> SchedulerQueryResult | None:
        if self.scheduler is None:
            return None
        request = SchedulerQueryRequest(
            workflow=self.workflow,
            kind=kind,
            clock=self._sample_clock(),
            timeout_seconds=self.config.scheduler_timeout,
            result_limit=self.config.scheduler_result_limit,
        )
        # The concrete provider is synchronous and process-bounded.  Moving it
        # off the coordinator event loop keeps DB/tail/render progress intact.
        try:
            result = await asyncio.to_thread(self.scheduler.query, request)
        except Exception as error:
            result = SchedulerQueryResult(
                request=request,
                health=SourceHealth(
                    _SCHEDULER_SOURCE[kind],
                    HealthState.UNAVAILABLE,
                    request.clock.epoch,
                    consecutive_failures=1,
                    error_code="scheduler_provider_exception",
                    detail=_exception_detail(error),
                ),
                backoff_seconds=min(self.config.loop_retry_interval * 2.0, 300.0),
            )
        if self._stop_event.is_set():
            return result
        self._scheduler_results[kind] = result
        self.reconciler.update_scheduler(result)
        if publish:
            await self.publish()
        return result

    async def publish(self) -> CoordinatorSnapshot:
        async with self._publish_lock:
            clock = self._sample_clock()
            effective = self.reconciler.build_snapshot(self._next_epoch(), clock)
            self._publication_sequence += 1
            source_health = (
                effective.source_health
                if effective is not None
                else self.reconciler.source_health
            )
            publication = CoordinatorSnapshot(
                sequence=self._publication_sequence,
                clock=clock,
                effective=effective,
                source_health=source_health,
                scheduler_results=tuple(
                    self._scheduler_results[kind]
                    for kind in sorted(self._scheduler_results, key=lambda x: x.value)
                ),
                pending_tail_events=self.reconciler.buffered_count,
                semantic_progress=self.reconciler.semantic_progress,
                latest_effective_event=(
                    max(effective.events, key=lambda item: item.order)
                    if effective is not None and effective.events
                    else None
                ),
                has_authoritative_base=effective is not None,
                authoritative_complete=(
                    effective is not None
                    and effective.workflow.provenance is Provenance.DB_CONFIRMED
                    and effective.workflow.state == "WORKFLOW_TERMINATED"
                    and effective.workflow.status is not None
                    and effective.pending_overlay_count == 0
                ),
                publisher_failures=self._publisher_failures,
                publisher_error=self._publisher_error,
                coordinator_errors=FrozenPayload.from_mapping(self._coordinator_errors),
            )
            self._latest = publication
            if effective is not None:
                terminal_count = sum(
                    job.lifecycle.value in {"succeeded", "failed"}
                    for job in effective.jobs
                )
                if terminal_count != self._last_terminal_count:
                    self._last_terminal_count = terminal_count
                    self._scheduler_wakeup.set()
                if (
                    publication.authoritative_complete
                    and not self._final_history_requested
                ):
                    self._final_history_requested = True
                    self._scheduler_wakeup.set()
            if self._publisher is not None:
                self._enqueue_publication(publication)
            return publication

    def _enqueue_publication(self, publication: CoordinatorSnapshot) -> None:
        if self._publisher_queue.full():
            self._publisher_queue.get_nowait()
            self._publisher_queue.task_done()
        self._publisher_queue.put_nowait(publication)

    async def _publisher_loop(self) -> None:
        while not self._stop_event.is_set():
            publication = await self._publisher_queue.get()
            try:
                if self._publisher is None:
                    continue
                if inspect.iscoroutinefunction(self._publisher):
                    await self._publisher(publication)  # type: ignore[misc]
                else:
                    returned = await asyncio.to_thread(self._publisher, publication)
                    if inspect.isawaitable(returned):
                        await returned
                self._publisher_error = None
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._publisher_failures += 1
                self._publisher_error = _exception_detail(error)
                if self._latest is not None:
                    self._latest = replace(
                        self._latest,
                        publisher_failures=self._publisher_failures,
                        publisher_error=self._publisher_error,
                    )
            finally:
                self._publisher_queue.task_done()

    def _record_coordinator_error(self, name: str, error: BaseException) -> None:
        self._coordinator_errors[name] = _exception_detail(error)
        if self._latest is not None:
            self._latest = replace(
                self._latest,
                coordinator_errors=FrozenPayload.from_mapping(self._coordinator_errors),
            )

    async def start(self) -> CoordinatorSnapshot:
        async with self._start_lock:
            if self._closed:
                raise RuntimeError("cannot start a closed coordinator")
            if self._started:
                if self._latest is None:  # pragma: no cover - invariant guard
                    raise RuntimeError("started coordinator has no publication")
                return self._latest
            initial = await self.bootstrap()
            self._started = True
            if self.tail is not None:
                self._create_supervised(self._tail_loop, "pegasus-monitor-tail")
            self._create_supervised(self._database_loop, "pegasus-monitor-stampede")
            if self.scheduler is not None and self.config.scheduler_intervals:
                self._create_supervised(
                    self._scheduler_loop, "pegasus-monitor-htcondor"
                )
            if self._publisher is not None:
                self._create_supervised(
                    self._publisher_loop, "pegasus-monitor-publisher"
                )
            return initial

    def _create_supervised(
        self, factory: Callable[[], Awaitable[object]], name: str
    ) -> None:
        task = asyncio.create_task(self._supervise(factory, name), name=name)
        self._tasks.add(task)
        task.add_done_callback(self._observe_task)

    async def _supervise(
        self, factory: Callable[[], Awaitable[object]], name: str
    ) -> None:
        while not self._stop_event.is_set():
            try:
                await factory()
                if not self._stop_event.is_set():
                    raise RuntimeError("source loop exited unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._record_coordinator_error(name, error)
                await asyncio.sleep(self.config.loop_retry_interval)

    def _observe_task(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None and not self._closed:
            self._record_coordinator_error(task.get_name(), error)

    async def _tail_loop(self) -> None:
        interval = self.config.tail_active_interval
        while not self._stop_event.is_set():
            await self._sleep(interval)
            if self._stop_event.is_set():
                return
            if self.reconciler.tail_rearm_required and self._tail_factory is None:
                interval = self.config.tail_idle_interval
                continue
            result = await self.poll_tail_once()
            interval = (
                self.config.tail_active_interval
                if result.bytes_read or result.lines_read
                else self.config.tail_idle_interval
            )

    async def _database_loop(self) -> None:
        while not self._stop_event.is_set():
            sleep_task = asyncio.create_task(self._sleep(self.config.database_interval))
            wake_task = asyncio.create_task(self._database_wakeup.wait())
            waits = {sleep_task, wake_task}
            try:
                done, pending = await asyncio.wait(
                    waits, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                pending = {task for task in waits if not task.done()}
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            if self._stop_event.is_set():
                return
            if wake_task in done:
                self._database_wakeup.clear()
            await self.refresh_database_once()

    async def _scheduler_loop(self) -> None:
        intervals = dict(self.config.scheduler_intervals)
        due = {kind: self._sample_clock().monotonic for kind in intervals}
        while not self._stop_event.is_set():
            now = self._sample_clock().monotonic
            kind = min(due, key=due.__getitem__)
            delay = max(0.0, due[kind] - now)
            if delay:
                sleep_task = asyncio.create_task(self._sleep(delay))
                wake_task = asyncio.create_task(self._scheduler_wakeup.wait())
                waits = {sleep_task, wake_task}
                try:
                    done, pending = await asyncio.wait(
                        waits, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    pending = {task for task in waits if not task.done()}
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                if wake_task in done:
                    self._scheduler_wakeup.clear()
                    if SchedulerQueryKind.HISTORY in intervals:
                        kind = SchedulerQueryKind.HISTORY
            if self._stop_event.is_set():
                return
            if kind is SchedulerQueryKind.POOL and not self._pool_observation_needed():
                due[kind] = self._sample_clock().monotonic + intervals[kind]
                continue
            result = await self.poll_scheduler_once(kind)
            backoff = result.backoff_seconds if result is not None else 0.0
            due[kind] = self._sample_clock().monotonic + max(intervals[kind], backoff)

    def _pool_observation_needed(self) -> bool:
        effective = self._latest.effective if self._latest is not None else None
        if effective is None:
            return False
        return any(job.lifecycle.value in {"queued", "held"} for job in effective.jobs)

    async def run(self) -> None:
        await self.start()
        await self._stop_event.wait()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        # Stop the concrete process group before cancelling the task awaiting
        # its executor future.  Provider cleanup is capability-based because
        # the frozen generic protocol intentionally has no lifecycle methods.
        self._call_capability(self.scheduler, "cancel")
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._call_capability(self.scheduler, "close")
        self._call_capability(self.tail, "close")

    async def __aenter__(self) -> MonitorCoordinator:
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    @staticmethod
    def _call_capability(target: object | None, name: str) -> None:
        if target is None:
            return
        operation = getattr(target, name, None)
        if callable(operation):
            operation()
